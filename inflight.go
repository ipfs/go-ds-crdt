package crdt

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"

	dshelp "github.com/ipfs/boxo/datastore/dshelp"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
)

const (
	// pendingBlocksNs holds CIDs that have been merged into the set but
	// whose originating walk has not yet finalized. Layout per CID:
	//   pending/<cid>/m       -> uvarint(walk-id)   (meta)
	//   pending/<cid>/p/<par> -> empty             (one per parent recorder)
	// Children of <cid> are not stored here — they are read from the DAG
	// block in the local blockstore at finalize time.
	pendingBlocksNs = "p"
	// pendingMetaSubKey holds the meta entry (walk-id) for a pending CID.
	pendingMetaSubKey = "m"
	// pendingParentSubKey is the prefix for parent recorders under a
	// pending CID — one entry per (pending CID, parent CID) pair.
	pendingParentSubKey = "p"
	// inflightHeadsNs holds the tips of walks that have begun but not
	// finalized. Acts as the recovery anchor: as long as an entry is
	// present, repair will re-drive a walk from the tip until the walk
	// completes and removes the entry.
	inflightHeadsNs = "i"
)

// activeTraversals is a process-local registry of in-flight walk IDs. A walk
// allocates a fresh ID on entry and registers it; on exit (success, error,
// panic) it unregisters via defer. Persisted pending tags whose ID is not
// present here are treated as stalled — the walk that wrote them died — so
// the next walk that reaches the region re-descends through it. On startup
// the set is empty, which correctly classifies every persisted tag as
// stalled.
type activeTraversals struct {
	mu  sync.RWMutex
	set map[uint64]struct{}
}

func newActiveTraversals() *activeTraversals {
	return &activeTraversals{set: make(map[uint64]struct{})}
}

func (a *activeTraversals) register(t uint64) {
	a.mu.Lock()
	a.set[t] = struct{}{}
	a.mu.Unlock()
}

func (a *activeTraversals) unregister(t uint64) {
	a.mu.Lock()
	delete(a.set, t)
	a.mu.Unlock()
}

func (a *activeTraversals) isLive(t uint64) bool {
	a.mu.RLock()
	_, ok := a.set[t]
	a.mu.RUnlock()
	return ok
}

// pendingCIDBaseKey returns the parent key under which all per-CID pending
// state lives: <namespace>/<pendingNs>/<cidHashKey>.
func (store *Datastore) pendingCIDBaseKey(c cid.Cid) ds.Key {
	return store.namespace.
		ChildString(store.opts.crdtOpts.Namespaces.PendingBlocks).
		ChildString(dshelp.MultihashToDsKey(c.Hash()).String())
}

func (store *Datastore) pendingMetaKey(c cid.Cid) ds.Key {
	return store.pendingCIDBaseKey(c).ChildString(pendingMetaSubKey)
}

func (store *Datastore) pendingParentKey(c, parent cid.Cid) ds.Key {
	return store.pendingCIDBaseKey(c).
		ChildString(pendingParentSubKey).
		ChildString(dshelp.MultihashToDsKey(parent.Hash()).String())
}

// markPending records that c has been merged into the set by walk walkID and
// that parent links to c. Must be called AFTER set.Merge commits, otherwise a
// crash window leaves a live pending tag pointing at unmerged data.
//
// If meta already exists for c under a different walk-id, behavior depends on
// whether that walk is still alive:
//   - Live peer walk: leave meta alone — both walks may reach c through the
//     merge-then-mark race window. The parent edge below is added to the
//     accumulating set.
//   - Stalled (dead) walk: refresh meta with our walk-id so peer walks see
//     a live owner and stop at this boundary. The previous walk's parent
//     edges are KEPT — they record real DAG relationships, so our finalize
//     cascade will transitively promote the dead walk's pending region
//     instead of waiting for repairPending.
//
// parent.Defined() == false signals "c is the walk's tip" — no parent entry
// is written.
//
// When c was already pending before this call (present == true), we subsume
// c's inflightHeads entry whenever an upward parent-edge chain links it to
// another anchor — either the edge we just wrote (parent.Defined()) or one
// already on disk from a (live or dead) peer walk that descended through c.
// In both cases c is reachable from whichever anchor sits above; the local
// anchor is redundant and removing it lets finalize-time cleanup fire only
// for true topmost CIDs. The remaining case — present with no upward edges
// — is c being its own previous tip (e.g. repair re-driving from a crashed
// walk that died at c); the local anchor is the only recovery point and
// must stay until finalizeWalk promotes c.
//
// The order — parent edge first, then inflight delete — is load-bearing for
// crash safety: the reverse order would leave a window where c's region
// has neither a recovery anchor nor a parent-edge link to one.
func (store *Datastore) markPending(ctx context.Context, c cid.Cid, walkID uint64, parent cid.Cid, dagName string) error {
	prevWalkID, present, err := store.pendingTag(ctx, c)
	if err != nil {
		return err
	}
	if !present || (prevWalkID != walkID && !store.activeTraversals.isLive(prevWalkID)) {
		// First walker to reach c: claim ownership by writing meta.
		// OR
		// Previous owner is dead: refresh meta with our walk-id so peer walks see
		// a live owner. Keep the old parent edges intact, so they will let our
		// cascade finalize the dead walk's region.
		if err := store.store.Put(ctx, store.pendingMetaKey(c), binary.AppendUvarint(nil, walkID)); err != nil {
			return err
		}
	}
	// Otherwise: same walk re-marking, or a live peer owns meta, leave meta
	// alone; only the parent edge below is added.
	if parent.Defined() {
		if err := store.store.Put(ctx, store.pendingParentKey(c, parent), nil); err != nil {
			return err
		}
	}
	if !present {
		return nil
	}

	subsume := parent.Defined()
	if !subsume {
		parents, err := store.listPendingParents(ctx, c)
		if err != nil {
			return err
		}
		subsume = len(parents) > 0
	}
	if subsume {
		if err := store.removeInflightHead(ctx, dagName, c); err != nil {
			return err
		}
	}
	return nil
}

// pendingTag returns the traversal-id that pended c. present=false when c is
// not in the pending namespace.
func (store *Datastore) pendingTag(ctx context.Context, c cid.Cid) (uint64, bool, error) {
	val, err := store.store.Get(ctx, store.pendingMetaKey(c))
	if errors.Is(err, ds.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	t, n := binary.Uvarint(val)
	if n <= 0 {
		return 0, true, errors.New("error decoding traversal id")
	}
	return t, true, nil
}

// listPendingParents returns every parent CID that has c recorded as a
// pending child.
func (store *Datastore) listPendingParents(ctx context.Context, c cid.Cid) ([]cid.Cid, error) {
	prefix := store.pendingCIDBaseKey(c).ChildString(pendingParentSubKey).String()
	results, err := store.store.Query(ctx, query.Query{Prefix: prefix, KeysOnly: true})
	if err != nil {
		return nil, err
	}
	defer results.Close()

	var out []cid.Cid
	for r := range results.Next() {
		if r.Error != nil {
			return nil, r.Error
		}
		rel := strings.TrimPrefix(r.Key, prefix)
		parentCid, err := dshelp.DsKeyToCidV1(ds.NewKey(rel), cid.DagProtobuf)
		if err != nil {
			return nil, fmt.Errorf("pending parent key %s: %w", r.Key, err)
		}
		out = append(out, parentCid)
	}
	return out, nil
}

// inflightHeadKey returns the datastore key for the inflight tip h. The
// schema mirrors the heads/ schema in heads.go: empty DAGName uses a single
// CID component; a named DAG nests under the dagName.
func (store *Datastore) inflightHeadKey(dagName string, c cid.Cid) ds.Key {
	base := store.namespace.ChildString(store.opts.crdtOpts.Namespaces.InflightHeads)
	hashKey := dshelp.MultihashToDsKey(c.Hash())
	if dagName == "" {
		return base.Child(hashKey)
	}
	return base.ChildString(dagName).Child(hashKey)
}

// addInflightHead records that a walk has begun from h. Stores h.Height in
// the value (h.DAGName is in the key) so the entry can be reconstructed into
// a Head during recovery.
func (store *Datastore) addInflightHead(ctx context.Context, h Head) error {
	return store.store.Put(ctx, store.inflightHeadKey(h.DAGName, h.Cid), binary.AppendUvarint(nil, h.Height))
}

func (store *Datastore) removeInflightHead(ctx context.Context, dagName string, c cid.Cid) error {
	err := store.store.Delete(ctx, store.inflightHeadKey(dagName, c))
	if errors.Is(err, ds.ErrNotFound) {
		return nil
	}
	return err
}

func (store *Datastore) hasInflightHead(ctx context.Context, dagName string, c cid.Cid) (bool, error) {
	return store.store.Has(ctx, store.inflightHeadKey(dagName, c))
}

// listInflightHeadsForDAG returns every inflight tip whose DAGName matches
// dagName. Order is unspecified.
func (store *Datastore) listInflightHeadsForDAG(ctx context.Context, dagName string) ([]Head, error) {
	all, err := store.listInflightHeads(ctx)
	if err != nil {
		return nil, err
	}
	var out []Head
	for _, h := range all {
		if h.DAGName == dagName {
			out = append(out, h)
		}
	}
	return out, nil
}

// listInflightHeads enumerates every inflight tip on disk. The order is
// unspecified. Used by repairPending to drive recovery walks.
func (store *Datastore) listInflightHeads(ctx context.Context) ([]Head, error) {
	base := store.namespace.ChildString(store.opts.crdtOpts.Namespaces.InflightHeads).String()
	q := query.Query{Prefix: base, KeysOnly: false}

	results, err := store.store.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer results.Close()

	var out []Head
	for r := range results.Next() {
		if r.Error != nil {
			return nil, r.Error
		}
		rel := ds.NewKey(strings.TrimPrefix(r.Key, base))
		ns := rel.Namespaces()
		var dagName string
		var cidKey ds.Key
		switch len(ns) {
		case 1:
			cidKey = ds.NewKey(ns[0])
		case 2:
			dagName = ns[0]
			cidKey = ds.NewKey(ns[1])
		default:
			store.logger.Errorf("bad inflight head key: %s", r.Key)
			continue
		}
		c, err := dshelp.DsKeyToCidV1(cidKey, cid.DagProtobuf)
		if err != nil {
			return nil, fmt.Errorf("inflight head key %s: %w", r.Key, err)
		}
		height, n := binary.Uvarint(r.Value)
		if n <= 0 {
			return nil, fmt.Errorf("inflight head value at %s: invalid height", r.Key)
		}
		out = append(out, Head{
			Cid:       c,
			HeadValue: HeadValue{Height: height, DAGName: dagName},
		})
	}
	return out, nil
}

// finalizeWalk drives a bottom-up topological promote from walk.frontier. The
// frontier was populated by processNode for every CID whose links were all
// classified as already-processed boundaries, these are ready to promote
// without re-reading their IPLD node.
//
// For each c popped from the queue:
//
//   - If c is already processed or no longer pending: skip.
//   - If c has any link not yet in processed: skip. Some other walk's
//     finalize will eventually promote c's missing child and re-enqueue c
//     via the parent-edge cascade, OR the walk did not reach that child
//     and the next repairPending pass will re-drive from tip to fill the
//     gap. Frontier seeds always pass this check by construction; the
//     check is meaningful for parents enqueued during the cascade.
//   - Otherwise: list c's pending parents, promote c (Put processed/c +
//     delete pending/<c>/*), enqueue the listed parents.
//
// Anchor cleanup runs only for c with no pending parents — the topmost
// CID in the joined pending region. processNode's boundary subsumption
// already cleared the anchor of any CID with a parent edge written above
// it (a live-peer tip we joined as a boundary), so by the time finalize
// reaches a CID with parents, that CID's anchor — if it ever had one —
// has been removed. The single anchor that survives is the one belonging
// to the topmost walk in the region, which we drop here.
//
// The cascade naturally crosses walk boundaries: when this walk's cascade
// reaches a CID c that another walk recorded as a boundary parent (via
// pendingParentKey), that walk's parent gets enqueued and the cascade
// continues upward through it.
//
// Memory is bounded by len(queue), worst case O(width of pending sub-DAG).
// Linear chains run at O(1).
func (store *Datastore) finalizeWalk(ctx context.Context, tip Head, walk *walkState) error {
	queue := walk.frontier
	for len(queue) > 0 {
		c := queue[len(queue)-1]
		queue = queue[:len(queue)-1]

		processed, err := store.isProcessed(ctx, c)
		if err != nil {
			return err
		}
		if processed {
			continue
		}
		_, present, err := store.pendingTag(ctx, c)
		if err != nil {
			return err
		}
		if !present {
			continue
		}

		ready, err := store.readyToPromote(ctx, c)
		if err != nil {
			return err
		}
		if !ready {
			continue
		}

		parents, err := store.listPendingParents(ctx, c)
		if err != nil {
			return err
		}
		if err := store.promote(ctx, c); err != nil {
			return err
		}
		if len(parents) == 0 {
			// c is the topmost pending CID: nothing above recorded it as a child, so
			// its anchor is the only one left for this region.
			if err := store.removeInflightHead(ctx, tip.DAGName, c); err != nil {
				return err
			}
		}
		queue = append(queue, parents...)
	}
	return nil
}

// readyToPromote returns true when every child link of c is in the processed
// namespace. For walk frontier seeds this is true by construction (processNode
// classified every link as a processed boundary); for parents reached during
// the cascade it may not yet hold when other walks' regions are still pending.
//
// The IPLD node is read from the local blockstore: it was fetched during the
// walk-down and is still in store.dagService's underlying blockstore. No
// network traffic.
func (store *Datastore) readyToPromote(ctx context.Context, c cid.Cid) (bool, error) {
	cctx, cancel := context.WithTimeout(ctx, store.opts.DAGSyncerTimeout)
	nd, err := store.dagService.Get(cctx, c)
	cancel()
	if err != nil {
		return false, fmt.Errorf("error reading %s during finalize: %w", c, err)
	}
	for _, l := range nd.Links() {
		processed, err := store.isProcessed(ctx, l.Cid)
		if err != nil {
			return false, err
		}
		if !processed {
			return false, nil
		}
	}
	return true, nil
}

// promote marks c as processed and removes all of its pending state. Parent
// recorders (pending/<c>/p/*) are queried by the caller before this is invoked
// so that parents can be enqueued for the cascade; here we just clear them.
func (store *Datastore) promote(ctx context.Context, c cid.Cid) error {
	var err error
	var write ds.Write = store.store
	batchingDs, batching := store.store.(ds.Batching)
	if batching {
		write, err = batchingDs.Batch(ctx)
		if err != nil {
			return err
		}
	}

	if err := write.Put(ctx, store.processedBlockKey(c), nil); err != nil {
		return err
	}

	// Drain the cursor into a slice and close it before issuing deletes /
	// committing the batch. Some datastores (e.g., LevelDB, Badger) do not
	// guarantee correct behavior when a read iterator is open over the same
	// prefix being mutated by a concurrent batch commit.
	prefix := store.pendingCIDBaseKey(c).String()
	results, err := store.store.Query(ctx, query.Query{Prefix: prefix, KeysOnly: true})
	if err != nil {
		return err
	}
	var keys []string
	for r := range results.Next() {
		if r.Error != nil {
			results.Close()
			return r.Error
		}
		keys = append(keys, r.Key)
	}
	if err := results.Close(); err != nil {
		return err
	}

	for _, k := range keys {
		if err := write.Delete(ctx, ds.NewKey(k)); err != nil && !errors.Is(err, ds.ErrNotFound) {
			return err
		}
	}

	if batching {
		return write.(ds.Batch).Commit(ctx)
	}
	return nil
}

// repairPending re-drives a fresh walk from every inflight tip on disk.
// Persisted pending tags appear stalled to a fresh walk (activeTraversals is
// empty at startup), so the regular walk path's stalled-pending re-descent
// self-heals the region. Tips are processed sequentially; we collect errors
// rather than bailing on the first failure so partial progress is preserved
// across multiple inflight heads.
//
// On crash recovery this also fetches blocks that the original walk failed to
// retrieve. repairDAG's body cannot reach those CIDs because they are above
// current heads and gossip will not redeliver a stalled tip.
func (store *Datastore) repairPending(ctx context.Context) error {
	tips, err := store.listInflightHeads(ctx)
	if err != nil {
		return fmt.Errorf("error listing inflight heads: %w", err)
	}
	if len(tips) == 0 {
		return nil
	}
	store.logger.Infof("repairing %d inflight tip(s)", len(tips))

	var errs []error
	for _, tip := range tips {
		select {
		case <-ctx.Done():
			errs = append(errs, ctx.Err())
			return errors.Join(errs...)
		default:
		}
		store.logger.Infof("repair: re-driving walk from %s [%s]", tip.Cid, tip.DAGName)
		if err := store.handleBranch(ctx, tip, tip.Cid); err != nil {
			store.logger.Warnf("repair: walk from %s failed: %s", tip.Cid, err)
			errs = append(errs, fmt.Errorf("inflight tip %s: %w", tip.Cid, err))
		}
	}
	return errors.Join(errs...)
}
