package crdt

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ipfs/boxo/ipld/merkledag"
	mdutils "github.com/ipfs/boxo/ipld/merkledag/test"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	ipld "github.com/ipfs/go-ipld-format"
)

func TestSyncLockState_GateAndEnter(t *testing.T) {
	t.Parallel()

	t.Run("not locked admits and increments", func(t *testing.T) {
		s := newSyncLockState()
		if locked := s.gateAndEnter("a"); locked {
			t.Fatal("expected gateAndEnter to admit when unlocked")
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.activeDAG["a"] != 1 {
			t.Errorf("activeDAG[a] = %d, want 1", s.activeDAG["a"])
		}
	})

	t.Run("full lock blocks all DAGs", func(t *testing.T) {
		s := newSyncLockState()
		s.engageFull()
		for _, name := range []string{"", "a", "b"} {
			if !s.gateAndEnter(name) {
				t.Errorf("expected gateAndEnter(%q) to block when fully locked", name)
			}
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if len(s.activeDAG) != 0 {
			t.Errorf("counters mutated on blocked entry: dags=%v", s.activeDAG)
		}
	})

	t.Run("DAG lock blocks only matching DAG", func(t *testing.T) {
		s := newSyncLockState()
		s.engageDAG("a")
		if !s.gateAndEnter("a") {
			t.Error("gateAndEnter(a) should block")
		}
		if s.gateAndEnter("b") {
			t.Error("gateAndEnter(b) should admit")
		}
	})
}

func TestSyncLockState_LeaveBalances(t *testing.T) {
	t.Parallel()
	s := newSyncLockState()

	// Two sessions on "a", one on "b".
	for _, n := range []string{"a", "a", "b"} {
		if locked := s.gateAndEnter(n); locked {
			t.Fatalf("unexpected lock for %q", n)
		}
	}
	s.leave("a")
	s.leave("a")
	s.leave("b")

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.activeDAG) != 0 {
		t.Errorf("activeDAG = %v, want empty", s.activeDAG)
	}
}

func TestSyncLockState_Accessors(t *testing.T) {
	t.Parallel()
	s := newSyncLockState()

	if s.fullEngaged() {
		t.Error("expected fullEngaged false initially")
	}
	if got := s.dagsEngaged(); len(got) != 0 {
		t.Errorf("dagsEngaged = %v, want empty", got)
	}

	id := s.engageFull()
	if !s.fullEngaged() {
		t.Error("expected fullEngaged true after engageFull")
	}
	s.releaseFull(id)
	if s.fullEngaged() {
		t.Error("expected fullEngaged false after releaseFull")
	}

	idZ := s.engageDAG("zeta")
	idA := s.engageDAG("alpha")
	idM := s.engageDAG("mu")
	got := s.dagsEngaged()
	want := []string{"alpha", "mu", "zeta"}
	if len(got) != len(want) {
		t.Fatalf("dagsEngaged len = %d, want %d (got %v)", len(got), len(want), got)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("dagsEngaged[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	s.releaseDAG("mu", idM)
	got = s.dagsEngaged()
	if len(got) != 2 || got[0] != "alpha" || got[1] != "zeta" {
		t.Errorf("after releaseDAG(mu): dagsEngaged = %v", got)
	}
	s.releaseDAG("zeta", idZ)
	s.releaseDAG("alpha", idA)
	if got := s.dagsEngaged(); len(got) != 0 {
		t.Errorf("after releasing all: dagsEngaged = %v, want empty", got)
	}
}

func TestSyncLockState_RefcountIndependence(t *testing.T) {
	// Two separate engagements on the full lock should each be tracked
	// independently. Releasing one keeps the lock engaged; releasing
	// both releases it.
	t.Parallel()
	s := newSyncLockState()

	id1 := s.engageFull()
	id2 := s.engageFull()
	if !s.fullEngaged() {
		t.Fatal("fullEngaged should be true with two holds")
	}
	s.releaseFull(id1)
	if !s.fullEngaged() {
		t.Error("fullEngaged should remain true with one hold remaining")
	}
	s.releaseFull(id1) // idempotent: same id again is a no-op
	if !s.fullEngaged() {
		t.Error("idempotent re-release of id1 must not affect id2's hold")
	}
	s.releaseFull(id2)
	if s.fullEngaged() {
		t.Error("fullEngaged should be false after releasing both ids")
	}
}

func TestSyncLockState_DAGRefcountIndependence(t *testing.T) {
	t.Parallel()
	s := newSyncLockState()

	id1 := s.engageDAG("a")
	id2 := s.engageDAG("a")
	if !slices.Equal(s.dagsEngaged(), []string{"a"}) {
		t.Fatalf("dagsEngaged = %v, want [a]", s.dagsEngaged())
	}
	s.releaseDAG("a", id1)
	if !slices.Equal(s.dagsEngaged(), []string{"a"}) {
		t.Error("DAG should remain engaged with one hold remaining")
	}
	s.releaseDAG("a", id1) // idempotent
	if !slices.Equal(s.dagsEngaged(), []string{"a"}) {
		t.Error("idempotent release must not affect id2's hold")
	}
	s.releaseDAG("a", id2)
	if got := s.dagsEngaged(); len(got) != 0 {
		t.Errorf("dagsEngaged = %v, want empty after releasing both", got)
	}
}

func TestSyncLockState_WaitFullImmediate(t *testing.T) {
	t.Parallel()
	s := newSyncLockState()

	if err := s.waitFull(t.Context()); err != nil {
		t.Errorf("waitFull on idle lock returned %v, want nil", err)
	}
	if err := s.waitDAG(t.Context(), "a"); err != nil {
		t.Errorf("waitDAG on idle lock returned %v, want nil", err)
	}
}

func TestSyncLockState_WaitFullDrains(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s := newSyncLockState()

		if locked := s.gateAndEnter("a"); locked {
			t.Fatal("gateAndEnter should admit")
		}

		done := make(chan error, 1)
		go func() { done <- s.waitFull(t.Context()) }()

		// Once all goroutines park, waitFull must still be blocked on
		// the in-flight session.
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("waitFull returned prematurely: %v", err)
		default:
		}

		s.leave("a")
		synctest.Wait()
		if err := <-done; err != nil {
			t.Errorf("waitFull returned %v, want nil", err)
		}
	})
}

func TestSyncLockState_WaitDAGDrains(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s := newSyncLockState()

		if locked := s.gateAndEnter("a"); locked {
			t.Fatal("gateAndEnter(a) should admit")
		}
		if locked := s.gateAndEnter("b"); locked {
			t.Fatal("gateAndEnter(b) should admit")
		}

		done := make(chan error, 1)
		go func() { done <- s.waitDAG(t.Context(), "a") }()

		// Leaving "b" must not unblock waitDAG("a").
		s.leave("b")
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("waitDAG(a) returned after leave(b): %v", err)
		default:
		}

		s.leave("a")
		synctest.Wait()
		if err := <-done; err != nil {
			t.Errorf("waitDAG returned %v, want nil", err)
		}
	})
}

func TestSyncLockState_WaitFullCtxCancel(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s := newSyncLockState()

		if locked := s.gateAndEnter("a"); locked {
			t.Fatal("gateAndEnter should admit")
		}
		defer s.leave("a")

		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
		defer cancel()

		if err := s.waitFull(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("waitFull err = %v, want DeadlineExceeded", err)
		}
	})
}

func TestSyncLockState_WaitClosed(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s := newSyncLockState()

		if locked := s.gateAndEnter("a"); locked {
			t.Fatal("gateAndEnter should admit")
		}
		defer s.leave("a")

		done := make(chan error, 1)
		go func() { done <- s.waitFull(t.Context()) }()

		// Ensure waitFull is parked on cond.Wait before Close.
		synctest.Wait()
		s.Close()
		synctest.Wait()

		if err := <-done; !errors.Is(err, ErrClosed) {
			t.Errorf("waitFull err = %v, want ErrClosed", err)
		}
	})
}

func TestSyncLockState_CloseIdempotent(t *testing.T) {
	t.Parallel()
	s := newSyncLockState()
	s.Close()
	s.Close() // must not panic
}

func TestSyncLockState_NilSafe(t *testing.T) {
	t.Parallel()
	var s *syncLockState

	// All methods must be safe to call on a nil receiver.
	if s.gateAndEnter("a") {
		t.Error("gateAndEnter on nil should admit (return false)")
	}
	s.leave("a")
	if id := s.engageFull(); id != 0 {
		t.Errorf("engageFull on nil returned id %d, want 0", id)
	}
	s.releaseFull(0)
	if id := s.engageDAG("x"); id != 0 {
		t.Errorf("engageDAG on nil returned id %d, want 0", id)
	}
	s.releaseDAG("x", 0)
	if s.fullEngaged() {
		t.Error("fullEngaged on nil should be false")
	}
	if got := s.dagsEngaged(); got != nil {
		t.Errorf("dagsEngaged on nil = %v, want nil", got)
	}
	if err := s.waitFull(context.Background()); err != nil {
		t.Errorf("waitFull on nil = %v, want nil", err)
	}
	if err := s.waitDAG(context.Background(), "a"); err != nil {
		t.Errorf("waitDAG on nil = %v, want nil", err)
	}
	s.Close()
}

func TestSyncLockState_WaitFullSucceedsOnCtxRace(t *testing.T) {
	// If drain completes at the same instant ctx fires, success wins:
	// the operation actually completed.
	t.Parallel()
	s := newSyncLockState()

	for i := 0; i < 1<<6; i++ {
		if locked := s.gateAndEnter("a"); locked {
			t.Fatal("gateAndEnter should admit")
		}

		ctx, cancel := context.WithCancel(t.Context())
		var wg sync.WaitGroup
		wg.Add(2)
		var got error
		go func() {
			defer wg.Done()
			got = s.waitFull(ctx)
		}()
		go func() {
			defer wg.Done()
			s.leave("a")
		}()
		cancel()
		wg.Wait()
		// Either success (drain won) or Canceled (ctx won) is acceptable.
		if got != nil && !errors.Is(got, context.Canceled) {
			t.Fatalf("iter %d: waitFull err = %v, want nil or Canceled", i, got)
		}
	}
}

// --- MerkleCRDT integration tests ----------------------------------------

// makeSyncLockReplicas builds n MerkleCRDT replicas with the sync
// lock primitive enabled. We can't use makeNReplicas because that
// helper passes the production default (EnableSyncLock=false) so the
// resulting MerkleCRDT.LockSync would be a no-op. The intervals match
// makeNReplicas for consistency; under synctest they are synthetic.
//
// Pass repair=0 to disable the periodic repair goroutine.
func makeSyncLockReplicas(t *testing.T, n int, repair time.Duration) ([]*MerkleCRDT, func()) {
	t.Helper()

	bcasts, cancelBcasts := newBroadcasters(t, n)
	bs := mdutils.Bserv()
	dagserv := merkledag.NewDAGService(bs)

	replicas := make([]*MerkleCRDT, n)
	for i := 0; i < n; i++ {
		opts := DefaultOptions()
		opts.Logger = &testLogger{
			name: fmt.Sprintf("r#%d: ", i),
			l:    DefaultOptions().Logger,
		}
		opts.RebroadcastInterval = 5 * time.Second
		opts.RepairInterval = repair
		opts.NumWorkers = 5
		opts.DAGSyncerTimeout = time.Second

		dagsync := &mockDAGSvc{
			DAGService: dagserv,
			bs:         bs.Blockstore(),
		}
		mc, err := NewMerkleCRDT(
			makeStore(t, i),
			ds.NewKey("crdttest"),
			dagsync,
			bcasts[i],
			opts,
			&MerkleCRDTOptions{EnableSyncLock: true},
		)
		if err != nil {
			t.Fatal(err)
		}
		replicas[i] = mc
	}

	closeFn := func() {
		cancelBcasts()
		for i, r := range replicas {
			if err := r.Close(); err != nil {
				t.Error(err)
			}
			if err := os.RemoveAll(storeFolder(i)); err != nil {
				t.Errorf("removing store folder for replica %d: %v", i, err)
			}
		}
	}
	return replicas, closeFn
}

func TestMerkleCRDT_SyncLock_DisabledByDefault(t *testing.T) {
	// Default MerkleCRDTOptions has EnableSyncLock=false. The lock
	// methods must still be callable but return no-op handles, and
	// SyncLocked / SyncLockedDAGs must report no engagement.
	t.Parallel()

	bcasts, cancelBcasts := newBroadcasters(t, 1)
	t.Cleanup(cancelBcasts)
	bs := mdutils.Bserv()
	dagserv := merkledag.NewDAGService(bs)

	dagsync := &mockDAGSvc{DAGService: dagserv, bs: bs.Blockstore()}
	opts := DefaultOptions()
	opts.Logger = &testLogger{name: "r#0: ", l: DefaultOptions().Logger}
	mc, err := NewMerkleCRDT(makeStore(t, 0), ds.NewKey("crdttest"), dagsync, bcasts[0], opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := mc.Close(); err != nil {
			t.Errorf("closing MerkleCRDT: %v", err)
		}
		if err := os.RemoveAll(storeFolder(0)); err != nil {
			t.Errorf("removing store folder: %v", err)
		}
	})

	// syncLock state should be nil — no allocation when disabled.
	if mc.syncLock != nil {
		t.Error("expected syncLock to be nil when EnableSyncLock is unset")
	}
	if mc.SyncLocked() {
		t.Error("SyncLocked should be false when sync lock is disabled")
	}
	if got := mc.SyncLockedDAGs(); got != nil {
		t.Errorf("SyncLockedDAGs = %v, want nil when sync lock is disabled", got)
	}

	// LockSync / LockSyncDAG return no-op handles. Unlock on them is
	// safe and SyncLocked stays false.
	lk, err := mc.LockSync(t.Context())
	if err != nil {
		t.Fatalf("LockSync: %v", err)
	}
	if mc.SyncLocked() {
		t.Error("SyncLocked should remain false even after LockSync when disabled")
	}
	lk.Unlock()

	lkD, err := mc.LockSyncDAG(t.Context(), "any")
	if err != nil {
		t.Fatalf("LockSyncDAG: %v", err)
	}
	lkD.Unlock()
}

func TestMerkleCRDT_SyncLock_HandleAPI(t *testing.T) {
	t.Parallel()
	mcs, closeFn := makeSyncLockReplicas(t, 1, 0)
	t.Cleanup(closeFn)
	mc := mcs[0]

	lk1, err := mc.LockSync(t.Context())
	if err != nil {
		t.Fatalf("LockSync: %v", err)
	}
	if !mc.SyncLocked() {
		t.Error("SyncLocked should be true after first LockSync")
	}

	// Two concurrent holders: each gets its own handle, lock stays
	// engaged as long as either is unreleased.
	lk2, err := mc.LockSync(t.Context())
	if err != nil {
		t.Fatalf("second LockSync: %v", err)
	}

	lk1.Unlock()
	if !mc.SyncLocked() {
		t.Error("SyncLocked should remain true while lk2 is held")
	}
	lk1.Unlock() // idempotent: must not affect lk2's hold
	if !mc.SyncLocked() {
		t.Error("idempotent re-Unlock of lk1 must not release lk2's hold")
	}

	lk2.Unlock()
	if mc.SyncLocked() {
		t.Error("SyncLocked should be false after both handles released")
	}

	// Nil-safe and zero-value-safe
	var nilLk *SyncLock
	nilLk.Unlock()
	(&SyncLock{}).Unlock()
}

func TestMerkleCRDT_SyncLockDAG_HandleAPI(t *testing.T) {
	t.Parallel()
	mcs, closeFn := makeSyncLockReplicas(t, 1, 0)
	t.Cleanup(closeFn)
	mc := mcs[0]

	lkA1, err := mc.LockSyncDAG(t.Context(), "a")
	if err != nil {
		t.Fatalf("LockSyncDAG(a): %v", err)
	}
	lkA2, err := mc.LockSyncDAG(t.Context(), "a")
	if err != nil {
		t.Fatalf("second LockSyncDAG(a): %v", err)
	}
	lkB, err := mc.LockSyncDAG(t.Context(), "b")
	if err != nil {
		t.Fatalf("LockSyncDAG(b): %v", err)
	}

	got := mc.SyncLockedDAGs()
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("SyncLockedDAGs = %v, want [a b]", got)
	}
	if mc.SyncLocked() {
		t.Error("SyncLocked should be false (only per-DAG locks held)")
	}

	lkA1.Unlock()
	got = mc.SyncLockedDAGs()
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("after one unlock of a: SyncLockedDAGs = %v, want [a b]", got)
	}
	lkA2.Unlock()
	got = mc.SyncLockedDAGs()
	if len(got) != 1 || got[0] != "b" {
		t.Errorf("after both a-handles unlocked: SyncLockedDAGs = %v, want [b]", got)
	}
	lkB.Unlock()
	if got = mc.SyncLockedDAGs(); len(got) != 0 {
		t.Errorf("after all unlocks: SyncLockedDAGs = %v, want empty", got)
	}
}

// TestMerkleCRDT_LockBlocksRemoteIngest exercises the basic "engaged-lock
// blocks ingest, release-lock catches up via rebroadcast" cycle for both the
// full and per-DAG lock variants.
func TestMerkleCRDT_LockBlocksRemoteIngest(t *testing.T) {
	t.Parallel()

	run := func(t *testing.T, lockFn func(*MerkleCRDT, context.Context) (*SyncLock, error)) {
		synctest.Test(t, func(t *testing.T) {
			mcs, closeFn := makeSyncLockReplicas(t, 2, 0)
			t.Cleanup(closeFn)

			ctx := t.Context()
			keyA := ds.NewKey("/before-lock")
			keyB := ds.NewKey("/under-lock")

			// Baseline: peer 0's write reaches peer 1 unimpeded.
			if err := mcs[0].Put(ctx, keyA, []byte("a")); err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
			if ok, _ := mcs[1].Has(ctx, keyA); !ok {
				t.Fatal("peer 1 did not receive keyA")
			}

			// Lock peer 1, write keyB on peer 0.
			lk, err := lockFn(mcs[1], ctx)
			if err != nil {
				t.Fatalf("lock: %v", err)
			}
			if !mcs[1].SyncLocked() && !slices.Contains(mcs[1].SyncLockedDAGs(), "") {
				t.Fatal("expected peer 1 to be sync-locked after lockFn")
			}
			if err := mcs[0].Put(ctx, keyB, []byte("b")); err != nil {
				t.Fatal(err)
			}

			// keyB must not propagate while peer 1 is sync-locked.
			// Advance synthetic time past several rebroadcast
			// intervals (5s in the test harness).
			time.Sleep(20 * time.Second)
			synctest.Wait()
			if ok, _ := mcs[1].Has(ctx, keyB); ok {
				t.Errorf("peer 1 received %q despite lock", keyB.String())
			}

			// Unlock and verify catch-up via the next rebroadcast
			// cycles.
			lk.Unlock()
			time.Sleep(20 * time.Second)
			synctest.Wait()
			if ok, _ := mcs[1].Has(ctx, keyB); !ok {
				t.Errorf("peer 1 did not catch up on %q after unlock", keyB.String())
			}
		})
	}

	t.Run("full lock", func(t *testing.T) {
		run(t, func(mc *MerkleCRDT, ctx context.Context) (*SyncLock, error) {
			return mc.LockSync(ctx)
		})
	})
	t.Run("dag lock", func(t *testing.T) {
		run(t, func(mc *MerkleCRDT, ctx context.Context) (*SyncLock, error) {
			// All Puts go to the default ("") DAG.
			return mc.LockSyncDAG(ctx, "")
		})
	})
}

// gatedDAGSvc wraps an ipld.DAGService and lets the test pause Get
// calls. Two granularities:
//   - Block() / Release(): pause every Get.
//   - BlockCID(c) / ReleaseCID(c): pause Gets only for cid c.
//
// Both gates are checked sequentially (global first, per-CID second).
// A pending Get unblocks via either gate's release or via ctx.Done.
type gatedDAGSvc struct {
	ipld.DAGService

	mu      sync.Mutex
	gate    chan struct{}             // nil = passthrough; non-nil = block until closed
	cidGate map[cid.Cid]chan struct{} // per-CID gates
}

func (g *gatedDAGSvc) Block() {
	g.mu.Lock()
	g.gate = make(chan struct{})
	g.mu.Unlock()
}

func (g *gatedDAGSvc) Release() {
	g.mu.Lock()
	if g.gate != nil {
		close(g.gate)
		g.gate = nil
	}
	g.mu.Unlock()
}

func (g *gatedDAGSvc) BlockCID(c cid.Cid) {
	g.mu.Lock()
	if g.cidGate == nil {
		g.cidGate = make(map[cid.Cid]chan struct{})
	}
	if _, ok := g.cidGate[c]; !ok {
		g.cidGate[c] = make(chan struct{})
	}
	g.mu.Unlock()
}

func (g *gatedDAGSvc) ReleaseCID(c cid.Cid) {
	g.mu.Lock()
	if ch, ok := g.cidGate[c]; ok {
		close(ch)
		delete(g.cidGate, c)
	}
	g.mu.Unlock()
}

func (g *gatedDAGSvc) Get(ctx context.Context, c cid.Cid) (ipld.Node, error) {
	g.mu.Lock()
	ch := g.gate
	cidCh := g.cidGate[c]
	g.mu.Unlock()
	if ch != nil {
		select {
		case <-ch:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if cidCh != nil {
		select {
		case <-cidCh:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return g.DAGService.Get(ctx, c)
}

// GetMany routes each fetch through Get so the gates apply.
// Otherwise sendNewJobs's GetDeltas path (which uses GetMany)
// would bypass the gating completely. The output channel is
// buffered to len(cids) so we always deliver the (node, err) pair
// for every requested CID — including the err-only result when
// ctx fires inside Get.
func (g *gatedDAGSvc) GetMany(ctx context.Context, cids []cid.Cid) <-chan *ipld.NodeOption {
	out := make(chan *ipld.NodeOption, len(cids))
	go func() {
		defer close(out)
		for _, c := range cids {
			n, err := g.Get(ctx, c)
			out <- &ipld.NodeOption{Node: n, Err: err}
		}
	}()
	return out
}

// Session implements crdt.SessionDAGService so handleBranch's session
// path uses our gated NodeGetter rather than the embedded merkledag
// session (which would bypass the gates entirely).
func (g *gatedDAGSvc) Session(ctx context.Context) ipld.NodeGetter {
	return g
}

// TestMerkleCRDT_LockDrainsInFlightSession verifies that, while a
// DAG-traversal session is in-flight (peer 0 stopped seeding mid-
// fetch), an attempt to LockSync / LockSyncDAG on peer 1 does not
// return until the session finishes. After releasing the seed gate,
// the lock returns once the in-flight branch is fully traversed.
func TestMerkleCRDT_LockDrainsInFlightSession(t *testing.T) {
	t.Parallel()

	run := func(t *testing.T, lockFn func(*MerkleCRDT, context.Context) (*SyncLock, error)) {
		synctest.Test(t, func(t *testing.T) {
			bs := mdutils.Bserv()
			dagserv := merkledag.NewDAGService(bs)
			gated := &gatedDAGSvc{DAGService: dagserv}

			bcasts, cancelBcasts := newBroadcasters(t, 2)
			t.Cleanup(cancelBcasts)

			newPeer := func(i int, dagsync ipld.DAGService) *MerkleCRDT {
				opts := DefaultOptions()
				opts.Logger = &testLogger{
					name: fmt.Sprintf("r#%d: ", i),
					l:    DefaultOptions().Logger,
				}
				opts.RebroadcastInterval = 5 * time.Second
				opts.NumWorkers = 5
				// Long DAGSyncerTimeout: this test pauses peer 1
				// inside Get and needs the cctx to outlive the
				// pause. With a 1s timeout the Get would error
				// before the test gets a chance to take the lock.
				opts.DAGSyncerTimeout = time.Hour
				mc, err := NewMerkleCRDT(
					makeStore(t, i),
					ds.NewKey("crdttest"),
					dagsync,
					bcasts[i],
					opts,
					&MerkleCRDTOptions{EnableSyncLock: true},
				)
				if err != nil {
					t.Fatal(err)
				}
				return mc
			}

			peer0 := newPeer(0, &mockDAGSvc{DAGService: dagserv, bs: bs.Blockstore()})
			peer1 := newPeer(1, gated)
			t.Cleanup(func() {
				if err := peer0.Close(); err != nil {
					t.Error(err)
				}
				if err := peer1.Close(); err != nil {
					t.Error(err)
				}
				if err := os.RemoveAll(storeFolder(0)); err != nil {
					t.Errorf("removing store folder for peer 0: %v", err)
				}
				if err := os.RemoveAll(storeFolder(1)); err != nil {
					t.Errorf("removing store folder for peer 1: %v", err)
				}
			})

			ctx := t.Context()

			// Baseline sync (gate open).
			keyA := ds.NewKey("/a")
			if err := peer0.Put(ctx, keyA, []byte("a")); err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
			if ok, _ := peer1.Has(ctx, keyA); !ok {
				t.Fatal("baseline sync failed")
			}

			// Stop peer 1 from fetching blocks; peer 0 publishes.
			gated.Block()
			keyB := ds.NewKey("/b")
			if err := peer0.Put(ctx, keyB, []byte("b")); err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
			if ok, _ := peer1.Has(ctx, keyB); ok {
				t.Error("peer 1 should not have keyB while DAGSyncer is gated")
			}

			// Try to take the lock on peer 1. The drain blocks on
			// the in-flight session that's stuck inside Get.
			// synctest.Wait() below ensures the spawned goroutine
			// has progressed through engageFull and parked on
			// cond.Wait inside waitFull before any of the
			// SyncLocked / select assertions run.
			done := make(chan struct{})
			var lk *SyncLock
			var lockErr error
			go func() {
				lk, lockErr = lockFn(peer1, ctx)
				close(done)
			}()
			synctest.Wait()
			select {
			case <-done:
				t.Fatalf("lock returned prematurely (err=%v)", lockErr)
			default:
			}
			if !peer1.SyncLocked() && !slices.Contains(peer1.SyncLockedDAGs(), "") {
				t.Error("expected lock to be engaged while drain blocks")
			}

			// Release the gate. The pending Get returns, the
			// branch is processed, the session leaves, drain
			// completes, and the lock returns.
			gated.Release()
			<-done
			if lockErr != nil {
				t.Fatalf("lock after release: %v", lockErr)
			}
			if ok, _ := peer1.Has(ctx, keyB); !ok {
				t.Error("peer 1 should have keyB once drain completes")
			}
			lk.Unlock()
		})
	}

	t.Run("full lock", func(t *testing.T) {
		run(t, func(mc *MerkleCRDT, ctx context.Context) (*SyncLock, error) {
			return mc.LockSync(ctx)
		})
	})
	t.Run("dag lock", func(t *testing.T) {
		run(t, func(mc *MerkleCRDT, ctx context.Context) (*SyncLock, error) {
			return mc.LockSyncDAG(ctx, "")
		})
	})
}

// TestMerkleCRDT_LockSync_RepairAfterFailedFetch covers recovery
// from a partially-applied chain via the natural flow (no manual
// heads.Add):
//
//  1. peer 0 publishes block0 (keyA), peer 1 syncs.
//  2. Lock peer 1.
//  3. peer 0 publishes block1 (keyB) and block2 (keyC); the
//     broadcasts arrive at peer 1 but the engaged lock causes the
//     per-DAG goroutines to skip them.
//  4. Block fetch of block1's CID specifically.
//  5. Unlock peer 1. The next rebroadcast of peer 0's heads (which
//     is just block2-head) reaches peer 1. handleBranch
//     pre-registers block2-head in peer 1's heads (#355), fetches
//     and processes block2 (set merges keyC), then queues block1
//     for the chain walk. block1 fetch BLOCKS on the gate; cctx
//     fires; dagWorker's recursive sendNewJobs path errors and
//     calls MarkDirty.
//  6. Lock peer 1 over the dirty partial state.
//  7. Release block1's gate, then peer1.Repair(ctx) while
//     locked: gateAndEnter at the top of repairDAG's loop
//     reports locked for every iteration — skipped without a
//     GetDelta or link walk. skippedDAGs=true; MarkClean is
//     suppressed. peer 1 still missing keyB; dirty bit
//     survives. (The gate state at this point is irrelevant
//     since the early skip never reaches GetDelta.)
//  8. Unlock and peer1.Repair(ctx) again: gateAndEnter admits,
//     repairDAG fetches block1 (gate open), handleBranch
//     processes it (set merges keyB), then walks to block0
//     (already processed). MarkClean clears the dirty bit.
func TestMerkleCRDT_LockSync_RepairAfterFailedFetch(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		bs := mdutils.Bserv()
		dagserv := merkledag.NewDAGService(bs)
		gated := &gatedDAGSvc{DAGService: dagserv}

		bcasts, cancelBcasts := newBroadcasters(t, 2)
		t.Cleanup(cancelBcasts)

		newPeer := func(i int, dagsync ipld.DAGService) *MerkleCRDT {
			opts := DefaultOptions()
			opts.Logger = &testLogger{
				name: fmt.Sprintf("r#%d: ", i),
				l:    DefaultOptions().Logger,
			}
			opts.RebroadcastInterval = 5 * time.Second
			opts.NumWorkers = 5
			// Short DAGSyncerTimeout: when the per-CID gate
			// blocks block1, we want cctx to fire quickly so the
			// recursive sendNewJobs errors and dagWorker calls
			// MarkDirty.
			opts.DAGSyncerTimeout = time.Second
			// Disable the periodic repair so the test fully
			// controls when repair runs.
			opts.RepairInterval = 0
			mc, err := NewMerkleCRDT(
				makeStore(t, i),
				ds.NewKey("crdttest"),
				dagsync,
				bcasts[i],
				opts,
				&MerkleCRDTOptions{EnableSyncLock: true},
			)
			if err != nil {
				t.Fatal(err)
			}
			return mc
		}

		peer0 := newPeer(0, &mockDAGSvc{DAGService: dagserv, bs: bs.Blockstore()})
		peer1 := newPeer(1, gated)
		t.Cleanup(func() {
			if err := peer0.Close(); err != nil {
				t.Error(err)
			}
			if err := peer1.Close(); err != nil {
				t.Error(err)
			}
			if err := os.RemoveAll(storeFolder(0)); err != nil {
				t.Errorf("removing store folder for peer 0: %v", err)
			}
			if err := os.RemoveAll(storeFolder(1)); err != nil {
				t.Errorf("removing store folder for peer 1: %v", err)
			}
		})

		ctx := t.Context()

		// 1. peer 0 publishes keyA; peer 1 syncs (gate open).
		keyA := ds.NewKey("/a")
		if err := peer0.Put(ctx, keyA, []byte("a")); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if ok, _ := peer1.Has(ctx, keyA); !ok {
			t.Fatal("peer 1 did not sync keyA")
		}

		// 2. Lock peer 1.
		lk1, err := peer1.LockSync(ctx)
		if err != nil {
			t.Fatalf("first LockSync: %v", err)
		}

		// 3. peer 0 publishes keyB then keyC. Both broadcasts
		//    reach peer 1 but the engaged lock causes the
		//    per-DAG goroutines to skip them.
		keyB := ds.NewKey("/b")
		keyC := ds.NewKey("/c")
		if err := peer0.Put(ctx, keyB, []byte("b")); err != nil {
			t.Fatal(err)
		}
		if err := peer0.Put(ctx, keyC, []byte("c")); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if ok, _ := peer1.Has(ctx, keyB); ok {
			t.Error("peer 1 has keyB while locked")
		}
		if ok, _ := peer1.Has(ctx, keyC); ok {
			t.Error("peer 1 has keyC while locked")
		}

		// 4. Discover block1's CID (block2's parent in the
		//    chain) so we can gate it specifically. peer 0's
		//    only head is block2-head.
		peer0Heads, _, err := peer0.heads.List(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(peer0Heads) != 1 {
			t.Fatalf("peer 0 heads = %v, want exactly one", peer0Heads)
		}
		block2Cid := peer0Heads[0].Cid
		block2Node, err := dagserv.Get(ctx, block2Cid)
		if err != nil {
			t.Fatal(err)
		}
		links := block2Node.Links()
		if len(links) != 1 {
			t.Fatalf("block2 has %d links, want 1", len(links))
		}
		block1Cid := links[0].Cid
		gated.BlockCID(block1Cid)

		// 5. Unlock peer 1 and let rebroadcast deliver
		//    block2-head. peer 0's seenHeads carries the echoes
		//    of the keyB and keyC broadcasts, so the first
		//    rebroadcast tick is a no-op (heads are seen) — only
		//    a later one actually rebroadcasts. 30s synthetic
		//    spans several ticks (5s interval ± 30%) plus the
		//    cctx fire (DAGSyncerTimeout = 1s) once block1's
		//    fetch starts.
		lk1.Unlock()
		time.Sleep(30 * time.Second)
		synctest.Wait()

		// peer 1 should have processed block2 (keyC merged),
		// failed to process block1 (gated), and the recursive
		// sendNewJobs path should have set the dirty bit.
		if ok, _ := peer1.Has(ctx, keyC); !ok {
			t.Error("expected peer 1 to have keyC after partial sync (block2 was processed)")
		}
		if ok, _ := peer1.Has(ctx, keyB); ok {
			t.Error("peer 1 should not have keyB while block1 is gated")
		}
		if !peer1.IsDirty(ctx) {
			t.Error("expected dirty bit set after failed traversal of block1")
		}

		// Sanity-check: peer 1's heads must contain block2-head, otherwise repair
		// has nothing to walk to.
		peer1Heads, _, err := peer1.heads.List(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var hasBlock2Head bool
		for _, h := range peer1Heads {
			if h.Cid.Equals(block2Cid) {
				hasBlock2Head = true
				break
			}
		}
		if !hasBlock2Head {
			t.Fatalf("peer 1 heads = %v, expected to include block2-head %s", peer1Heads, block2Cid)
		}

		// 6. Lock peer 1 over the dirty partial state.
		lk2, err := peer1.LockSync(ctx)
		if err != nil {
			t.Fatalf("second LockSync: %v", err)
		}

		// 7. Release block1's gate, then repair while locked.
		//    gateAndEnter at the top of repairDAG's loop reports
		//    locked for every head (block0-head and block2-head)
		//    and skips each iteration without ever calling
		//    GetDelta or expanding links. skippedDAGs=true;
		//    MarkClean is suppressed; peer 1 still missing keyB.
		//    Releasing block1's gate ahead of time is a no-op
		//    here because the early skip never reaches the
		//    fetch — but it keeps step 8 simple.
		gated.ReleaseCID(block1Cid)
		if err := peer1.Repair(ctx); err != nil {
			t.Fatalf("repair while locked: %v", err)
		}
		if ok, _ := peer1.Has(ctx, keyB); ok {
			t.Error("repair should not have brought keyB while the lock is engaged")
		}
		if !peer1.IsDirty(ctx) {
			t.Error("dirty bit should remain set while the lock is engaged")
		}

		// 8. Unlock and repair again. With the lock released
		//    and the gate open, repairDAG admits each iteration,
		//    handleBranch processes block1 (set merges keyB),
		//    then walks to block0 (already processed). Dirty
		//    bit cleared.
		lk2.Unlock()
		if err := peer1.Repair(ctx); err != nil {
			t.Fatalf("repair after unlock: %v", err)
		}
		if ok, _ := peer1.Has(ctx, keyB); !ok {
			t.Error("expected peer 1 to have keyB after post-unlock repair")
		}
		if peer1.IsDirty(ctx) {
			t.Error("dirty bit should be cleared after successful repair")
		}
	})
}

func TestMerkleCRDT_LockSync_RepairSkippedWhenLocked(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		mcs, closeFn := makeSyncLockReplicas(t, 1, 50*time.Millisecond)
		t.Cleanup(closeFn)
		mc := mcs[0]
		ctx := t.Context()

		// Put something so heads exist; mark dirty so the repair loop
		// is expected to act on the next tick.
		if err := mc.Put(ctx, ds.NewKey("/k"), []byte("v")); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		mc.MarkDirty(ctx)
		if !mc.IsDirty(ctx) {
			t.Fatal("expected dirty after MarkDirty")
		}

		// Engage the lock and observe that successive repair ticks
		// do not clear the dirty bit. fullEngaged() short-circuits
		// the periodic repair tick before repairDAG runs.
		lk, err := mc.LockSync(ctx)
		if err != nil {
			t.Fatalf("LockSync: %v", err)
		}
		time.Sleep(300 * time.Millisecond) // many ticks at 50ms interval
		synctest.Wait()
		if !mc.IsDirty(ctx) {
			t.Error("dirty bit was cleared while sync lock engaged")
		}

		lk.Unlock()
		time.Sleep(200 * time.Millisecond)
		synctest.Wait()
		if mc.IsDirty(ctx) {
			t.Error("dirty bit not cleared after unlock + repair tick")
		}
	})
}

func TestMerkleCRDT_LockSyncDAG_RepairLeavesDirtyOnSkip(t *testing.T) {
	// Per-DAG lock does not short-circuit the repair tick (that only
	// happens for the full lock). Instead, repairDAG runs, gates each
	// unprocessed head via gateAndEnter(head.DAGName), records that
	// at least one was skipped, and suppresses MarkClean so the dirty
	// bit survives the tick. After unlock the next tick reprocesses
	// and clears the bit.
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		mcs, closeFn := makeSyncLockReplicas(t, 1, 50*time.Millisecond)
		t.Cleanup(closeFn)
		mc := mcs[0]
		ctx := t.Context()

		if err := mc.Put(ctx, ds.NewKey("/k"), []byte("v")); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()

		// Force the head to look unprocessed so that repairDAG's
		// !isProcessed branch (the one that consults gateAndEnter)
		// fires.
		heads, _, err := mc.heads.List(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(heads) == 0 {
			t.Fatal("expected at least one head after Put")
		}
		for _, h := range heads {
			if err := mc.store.Delete(ctx, mc.processedBlockKey(h.Cid)); err != nil {
				t.Fatal(err)
			}
		}
		mc.MarkDirty(ctx)

		// Engage per-DAG lock for the default ("") DAG.
		lk, err := mc.LockSyncDAG(ctx, "")
		if err != nil {
			t.Fatalf("LockSyncDAG: %v", err)
		}
		if got := mc.SyncLockedDAGs(); len(got) != 1 || got[0] != "" {
			t.Errorf("SyncLockedDAGs = %v, want [\"\"]", got)
		}
		if mc.SyncLocked() {
			t.Error("SyncLocked should be false (only per-DAG lock held)")
		}

		if err := mc.Repair(ctx); err != nil {
			t.Fatalf("Repair: %v", err)
		}
		if !mc.IsDirty(ctx) {
			t.Error("dirty bit was cleared while LockSyncDAG was held")
		}
		// Many repair ticks fire (50ms interval, 300ms window).
		// Each must run repairDAG, hit the gate, set skippedDAGs,
		// and refrain from clearing the dirty bit.
		time.Sleep(300 * time.Millisecond)
		synctest.Wait()
		if !mc.IsDirty(ctx) {
			t.Error("dirty bit was cleared while LockSyncDAG was held")
		}

		lk.Unlock()
		time.Sleep(200 * time.Millisecond)
		synctest.Wait()
		if mc.IsDirty(ctx) {
			t.Error("dirty bit not cleared after unlock + repair tick")
		}
	})
}

func TestMerkleCRDT_CloseWhileLocked(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		mcs, closeFn := makeSyncLockReplicas(t, 1, 0)
		mc := mcs[0]

		// Hold an in-flight session manually so LockSync blocks.
		if locked := mc.syncLock.gateAndEnter("a"); locked {
			t.Fatal("gateAndEnter should admit")
		}

		done := make(chan error, 1)
		go func() {
			_, err := mc.LockSync(context.Background())
			done <- err
		}()

		// Wait for LockSync to be parked on cond.Wait.
		synctest.Wait()

		// closeFn calls Close on the Datastore, which closes the
		// syncLockState; the pending LockSync must unblock.
		closeFn()

		err := <-done
		if !errors.Is(err, ErrClosed) {
			t.Errorf("LockSync err = %v, want ErrClosed", err)
		}

		// The fake session was never released, but Close has dropped
		// the state — leave is benign on a torn-down lock.
		mc.syncLock.leave("a")
	})
}

func TestMerkleCRDT_LockSync_RollbackOnCtxCancel(t *testing.T) {
	// On ctx error, the engagement is rolled back: the caller gets
	// (nil, err) and SyncLocked reflects no contribution from the
	// failed call. (Other concurrent holders, if any, would still
	// keep it engaged.)
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		mcs, closeFn := makeSyncLockReplicas(t, 1, 0)
		t.Cleanup(closeFn)
		mc := mcs[0]

		if locked := mc.syncLock.gateAndEnter("a"); locked {
			t.Fatal("gateAndEnter should admit")
		}
		defer mc.syncLock.leave("a")

		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
		defer cancel()

		lk, err := mc.LockSync(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("LockSync err = %v, want DeadlineExceeded", err)
		}
		if lk != nil {
			t.Error("LockSync should return nil handle on error")
		}
		if mc.SyncLocked() {
			t.Error("SyncLocked should be false after rolled-back LockSync")
		}
	})
}

// TestMerkleCRDT_LockSync_MultiHeadProcessing_PrematureReturn demonstrates
// that with Options.MultiHeadProcessing=true, LockSync returns before
// in-flight DAG-traversal work for an already-admitted broadcast has
// finished — violating the godoc claim that "all in-flight DAG-traversal
// sessions have drained" upon return.
//
// Mechanics: under MultiHeadProcessing=true, the per-dagName goroutine
// in handleNext (crdt.go:489) spawns processHead in its own goroutine
// (crdt.go:537) and exits immediately. The deferred syncLock.leave fires
// while the spawned sub-goroutine is still inside handleBranch, queueing
// dagWorker jobs and mutating heads/set state. activeDAG hits 0, so
// LockSync's waitFull returns even though work is in flight.
//
// Counterpart: TestMerkleCRDT_LockDrainsInFlightSession runs the same
// scenario with MultiHeadProcessing=false (default) and passes — the
// session goroutine itself blocks on the gated Get, so leave is delayed
// until the branch is fully processed.
func TestCRDTLockSyncMultiHeadProcessingPrematureReturn(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		bs := mdutils.Bserv()
		dagserv := merkledag.NewDAGService(bs)
		gated := &gatedDAGSvc{DAGService: dagserv}

		bcasts, cancelBcasts := newBroadcasters(t, 2)
		t.Cleanup(cancelBcasts)

		newPeer := func(i int, dagsync ipld.DAGService, multiHead bool) *MerkleCRDT {
			opts := DefaultOptions()
			opts.Logger = &testLogger{
				name: fmt.Sprintf("r#%d: ", i),
				l:    DefaultOptions().Logger,
			}
			opts.RebroadcastInterval = 5 * time.Second
			opts.NumWorkers = 5
			opts.DAGSyncerTimeout = time.Hour
			opts.MultiHeadProcessing = multiHead
			mc, err := NewMerkleCRDT(
				makeStore(t, i),
				ds.NewKey("crdttest"),
				dagsync,
				bcasts[i],
				opts,
				&MerkleCRDTOptions{EnableSyncLock: true},
			)
			if err != nil {
				t.Fatal(err)
			}
			return mc
		}

		peer0 := newPeer(0, &mockDAGSvc{DAGService: dagserv, bs: bs.Blockstore()}, false)
		peer1 := newPeer(1, gated, true)
		t.Cleanup(func() {
			if err := peer0.Close(); err != nil {
				t.Error(err)
			}
			if err := peer1.Close(); err != nil {
				t.Error(err)
			}
			if err := os.RemoveAll(storeFolder(0)); err != nil {
				t.Errorf("removing store folder for peer 0: %v", err)
			}
			if err := os.RemoveAll(storeFolder(1)); err != nil {
				t.Errorf("removing store folder for peer 1: %v", err)
			}
		})

		ctx := t.Context()

		// Baseline sync (gate open). Establishes a head on peer 1 so
		// the next broadcast skips the curHeadCount==0 fresh-start
		// branch (which would do its own heads.Add directly inside
		// the per-dagName goroutine and call getPriority before the
		// for-loop spawns processHead).
		keyA := ds.NewKey("/a")
		if err := peer0.Put(ctx, keyA, []byte("a")); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if ok, _ := peer1.Has(ctx, keyA); !ok {
			t.Fatal("baseline sync failed")
		}

		// Block peer 1 from fetching new blocks; peer 0 publishes.
		gated.Block()
		keyB := ds.NewKey("/b")
		if err := peer0.Put(ctx, keyB, []byte("b")); err != nil {
			t.Fatal(err)
		}

		// synctest.Wait blocks until every goroutine in the bubble is
		// durably parked. By the time it returns:
		//   - peer 1's handleNext has received the broadcast,
		//   - the per-dagName goroutine has spawned `go processHead`
		//     and called `leave("")` on its way out (activeDAG=0),
		//   - the spawned sub-goroutine is parked inside Get on the
		//     closed gate channel.
		synctest.Wait()
		if ok, _ := peer1.Has(ctx, keyB); ok {
			t.Fatal("keyB processed despite gate closed")
		}

		// LockSync must wait for in-flight sessions to drain. Run it
		// in a goroutine so we can observe whether it returns before
		// the gated sub-goroutine has finished its work.
		lockDone := make(chan struct{})
		var lk *SyncLock
		var lockErr error
		go func() {
			lk, lockErr = peer1.LockSync(ctx)
			close(lockDone)
		}()
		synctest.Wait()

		select {
		case <-lockDone:
			// LockSync returned even though processHead is still
			// blocked on the gate. The drain guarantee is not
			// being honored. Continue the test to also show the
			// concrete state-mutation that occurs after release.
		default:
			t.Fatal("expected: LockSync returns prematurely under MultiHeadProcessing=true. " +
				"If this case fires, the issue is fixed (LockSync now waits for processHead).")
		}
		if lockErr != nil {
			t.Fatalf("LockSync: %v", lockErr)
		}

		// The lock claims to be drained. Releasing the gate should
		// have nothing to unblock — the sub-goroutine should already
		// have finished. In reality the sub-goroutine is alive,
		// processes the branch, and mutates peer 1's state while we
		// hold the lock.
		gated.Release()
		synctest.Wait()

		if ok, _ := peer1.Has(ctx, keyB); ok {
			t.Fatal("LockSync drain guarantee violated: peer 1 processed keyB " +
				"while the sync lock was held — sub-goroutine spawned by " +
				"MultiHeadProcessing escaped the session boundary")
		}
		lk.Unlock()
	})
}
