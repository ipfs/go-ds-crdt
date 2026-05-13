package crdt

import (
	"context"
	"slices"
	"sync"
)

// syncLockState gates DAG-traversal sessions for MerkleCRDT.LockSync /
// LockSyncDAG. A session is one (handleNext per-DAG goroutine) batch
// or one repairDAG per-head iteration; each session calls gateAndEnter
// on entry and leave on exit.
//
// Each engageFull / engageDAG call issues a unique uint64 id. The lock
// is engaged iff at least one id is still held. releaseFull /
// releaseDAG are keyed on that id and are idempotent: re-releasing an
// already-released id is a no-op, so independent callers cannot
// interfere with each other's holds.
//
// All methods are safe to call on a nil receiver: a nil syncLockState
// behaves as if no lock is ever engaged. Plain Datastore (constructed
// via New) leaves the field nil; only NewMerkleCRDT installs a real
// syncLockState.
type syncLockState struct {
	closeOnce sync.Once
	closed    chan struct{}

	mu     sync.Mutex
	cond   *sync.Cond
	nextID uint64

	fullHandles map[uint64]struct{}            // unique ids holding the full lock
	dagHandles  map[string]map[uint64]struct{} // unique ids holding per-DAG locks

	activeDAG map[string]int
}

func newSyncLockState() *syncLockState {
	s := &syncLockState{
		closed:      make(chan struct{}),
		fullHandles: make(map[uint64]struct{}),
		dagHandles:  make(map[string]map[uint64]struct{}),
		activeDAG:   make(map[string]int),
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Close signals all waitOn callers to abort with ErrClosed. Idempotent
// and nil-safe.
func (s *syncLockState) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		close(s.closed)
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
	})
}

// gateAndEnter atomically checks whether dagName is locked. Returns true if
// the caller entered the protected region (counters were incremented; the
// caller must call leave when finished). Returns false if the lock is engaged,
// in which case the caller must skip its work and not call leave.
//
// Polarity mirrors sync.(*Mutex).TryLock: true means "got in".
func (s *syncLockState) gateAndEnter(dagName string) bool {
	if s == nil {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.fullHandles) > 0 {
		return false
	}
	if len(s.dagHandles[dagName]) > 0 {
		return false
	}
	s.activeDAG[dagName]++
	return true
}

// leave decrements the session counter for a previously entered session. Must
// be paired with a gateAndEnter that returned true.
func (s *syncLockState) leave(dagName string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeDAG[dagName] <= 1 {
		delete(s.activeDAG, dagName)
		s.cond.Broadcast()
		return
	}
	s.activeDAG[dagName]--
}

// engageFull adds a hold on the full lock and returns its id. Returns
// 0 on a nil receiver.
func (s *syncLockState) engageFull() uint64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	s.nextID++
	id := s.nextID
	s.fullHandles[id] = struct{}{}
	s.mu.Unlock()
	return id
}

// releaseFull removes the hold with the given id. Idempotent: removing
// an absent id is a no-op.
func (s *syncLockState) releaseFull(id uint64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	delete(s.fullHandles, id)
	s.mu.Unlock()
}

// engageDAG adds a hold on dagName's lock and returns its id. Returns
// 0 on a nil receiver.
func (s *syncLockState) engageDAG(dagName string) uint64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	s.nextID++
	id := s.nextID
	if s.dagHandles[dagName] == nil {
		s.dagHandles[dagName] = make(map[uint64]struct{})
	}
	s.dagHandles[dagName][id] = struct{}{}
	s.mu.Unlock()
	return id
}

// releaseDAG removes the hold for (dagName, id). Idempotent.
func (s *syncLockState) releaseDAG(dagName string, id uint64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if h := s.dagHandles[dagName]; h != nil {
		delete(h, id)
		if len(h) == 0 {
			delete(s.dagHandles, dagName)
		}
	}
	s.mu.Unlock()
}

func (s *syncLockState) fullEngaged() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.fullHandles) > 0
}

func (s *syncLockState) dagsEngaged() []string {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	out := make([]string, 0, len(s.dagHandles))
	for n := range s.dagHandles {
		out = append(out, n)
	}
	s.mu.Unlock()
	slices.Sort(out)
	return out
}

// waitFull blocks until no DAG-traversal session is in flight, ctx is
// done, or Close is called. Returns nil on drain success, ctx.Err()
// on cancellation, or ErrClosed on Close.
func (s *syncLockState) waitFull(ctx context.Context) error {
	if s == nil {
		return nil
	}
	return s.waitOn(ctx, func() bool { return len(s.activeDAG) == 0 })
}

// waitDAG blocks until activeDAG[name] == 0, ctx is done, or Close is
// called.
func (s *syncLockState) waitDAG(ctx context.Context, name string) error {
	if s == nil {
		return nil
	}
	return s.waitOn(ctx, func() bool { return s.activeDAG[name] == 0 })
}

// waitOn implements the standard "cond.Wait with cancellation" pattern.
// A watcher goroutine broadcasts on cond when ctx is done or Close is
// called; the loop re-checks the predicate under mu.
func (s *syncLockState) waitOn(ctx context.Context, done func() bool) error {
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
		case <-s.closed:
		case <-stop:
			return
		}
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
	}()

	s.mu.Lock()
	defer s.mu.Unlock()
	for !done() {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-s.closed:
			return ErrClosed
		default:
		}
		s.cond.Wait()
	}
	return nil
}
