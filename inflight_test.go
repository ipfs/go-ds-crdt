package crdt

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/boxo/ipld/merkledag"
	mdutils "github.com/ipfs/boxo/ipld/merkledag/test"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	dssync "github.com/ipfs/go-datastore/sync"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multihash"
)

// ---------- helpers ----------

// newTestDatastore constructs a Datastore over an in-memory ds with sensible
// test defaults. The returned Datastore is auto-closed via t.Cleanup.
func newTestDatastore(t *testing.T) *Datastore {
	t.Helper()
	bs := mdutils.Bserv()
	dagserv := merkledag.NewDAGService(bs)
	dst, err := New(
		dssync.MutexWrap(ds.NewMapDatastore()),
		ds.NewKey("crdttest"),
		&mockDAGSvc{DAGService: dagserv, bs: bs.Blockstore()},
		&nullBroadcaster{},
		DefaultOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dst.Close() })
	return dst
}

// fakeCid produces a deterministic CID from a single byte seed. Useful for
// tests that want to manipulate pending/inflight namespace state directly
// without going through real DAG construction.
func fakeCid(t *testing.T, seed byte) cid.Cid {
	t.Helper()
	buf := make([]byte, 32)
	buf[0] = seed
	hash, err := multihash.Sum(buf, multihash.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	return cid.NewCidV1(cid.DagProtobuf, hash)
}

// buildChainSrc builds a linear DAG of `n` Puts on a fresh source datastore
// and returns the source dagservice plus the tip head. Each entry creates a
// new node whose first link points to the previous head.
func buildChainSrc(t *testing.T, n int) (*Datastore, *mockDAGSvc, Head) {
	t.Helper()
	bs := mdutils.Bserv()
	dagserv := merkledag.NewDAGService(bs)
	srcDag := &mockDAGSvc{DAGService: dagserv, bs: bs.Blockstore()}
	srcOpts := DefaultOptions()
	srcOpts.Logger = &testLogger{name: "src: ", l: DefaultOptions().Logger}
	src, err := New(
		dssync.MutexWrap(ds.NewMapDatastore()),
		ds.NewKey("crdttest"),
		srcDag,
		&nullBroadcaster{},
		srcOpts,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { src.Close() })
	for i := range n {
		if err := src.Put(t.Context(), ds.NewKey(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	heads, _, err := src.heads.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(heads) != 1 {
		t.Fatalf("expected 1 head after %d puts, got %d", n, len(heads))
	}
	return src, srcDag, heads[0]
}

// chainCidsTopDown returns every CID along the chain rooted at tip, in
// top-down order (tip first, deepest leaf last). Assumes a strictly linear
// chain (one link per node).
func chainCidsTopDown(t *testing.T, dagSvc ipld.DAGService, tip cid.Cid) []cid.Cid {
	t.Helper()
	var out []cid.Cid
	cur := tip
	for {
		out = append(out, cur)
		nd, err := dagSvc.Get(t.Context(), cur)
		if err != nil {
			t.Fatalf("walk chain: %v", err)
		}
		if len(nd.Links()) == 0 {
			return out
		}
		cur = nd.Links()[0].Cid
	}
}

// countKeys returns the number of keys directly under ns (one of the CRDT
// namespace strings, e.g. "p" or "i").
func countKeys(t *testing.T, dst *Datastore, ns string) int {
	t.Helper()
	prefix := dst.namespace.ChildString(ns).String()
	results, err := dst.store.Query(t.Context(), query.Query{Prefix: prefix, KeysOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer results.Close()
	rest, err := results.Rest()
	if err != nil {
		t.Fatal(err)
	}
	return len(rest)
}

// ---------- markPending ----------

func TestMarkPendingFreshCIDWritesMetaAndParent(t *testing.T) {
	dst := newTestDatastore(t)
	c := fakeCid(t, 1)
	parent := fakeCid(t, 2)

	if err := dst.markPending(t.Context(), c, 42, parent, ""); err != nil {
		t.Fatal(err)
	}

	tag, present, err := dst.pendingTag(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	if !present || tag != 42 {
		t.Fatalf("expected meta walk-id 42 present, got tag=%d present=%v", tag, present)
	}
	parents, err := dst.listPendingParents(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	if len(parents) != 1 || parents[0] != parent {
		t.Errorf("expected single parent edge to %s, got %v", parent, parents)
	}
}

func TestMarkPendingTipDoesNotWriteParentEdge(t *testing.T) {
	dst := newTestDatastore(t)
	tip := fakeCid(t, 1)

	if err := dst.markPending(t.Context(), tip, 7, cid.Undef, ""); err != nil {
		t.Fatal(err)
	}
	parents, err := dst.listPendingParents(t.Context(), tip)
	if err != nil {
		t.Fatal(err)
	}
	if len(parents) != 0 {
		t.Errorf("tip should have no parent edges, got %v", parents)
	}
}

// TestMarkPendingPresentRemovesInflightAnchor exercises the in-band
// subsumption: when a walk descends from a parent into an already-pending
// CID, markPending writes the parent edge AND removes the CID's
// inflightHeads entry (since the CID is now reachable from above through
// our walk's anchor chain).
func TestMarkPendingPresentRemovesInflightAnchor(t *testing.T) {
	dst := newTestDatastore(t)
	c := fakeCid(t, 1)
	parent := fakeCid(t, 2)

	// Seed: a peer walk's tip is at c.
	if err := dst.markPending(t.Context(), c, 100, cid.Undef, ""); err != nil {
		t.Fatal(err)
	}
	if err := dst.addInflightHead(t.Context(), Head{Cid: c, HeadValue: HeadValue{Height: 1}}); err != nil {
		t.Fatal(err)
	}

	// Our walk now descends into c from `parent`. c is already present →
	// markPending should remove its inflight anchor.
	if err := dst.markPending(t.Context(), c, 200, parent, ""); err != nil {
		t.Fatal(err)
	}

	if has, err := dst.hasInflightHead(t.Context(), "", c); err != nil {
		t.Fatal(err)
	} else if has {
		t.Errorf("inflight anchor for c should have been subsumed when descending into pending CID")
	}
	parents, err := dst.listPendingParents(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	if len(parents) != 1 || parents[0] != parent {
		t.Errorf("expected parent edge to %s, got %v", parent, parents)
	}
}

// TestMarkPendingFreshCIDLeavesAnchor: if c is in inflightHeads but NOT in
// pendingBlocks (a rare state, e.g. a crash between addInflightHead and
// markPending), our walk's first markPending sees !present and skips the
// inflight cleanup. The anchor stays for repair.
func TestMarkPendingFreshCIDLeavesAnchor(t *testing.T) {
	dst := newTestDatastore(t)
	c := fakeCid(t, 1)
	parent := fakeCid(t, 2)

	if err := dst.addInflightHead(t.Context(), Head{Cid: c, HeadValue: HeadValue{Height: 1}}); err != nil {
		t.Fatal(err)
	}

	if err := dst.markPending(t.Context(), c, 42, parent, ""); err != nil {
		t.Fatal(err)
	}
	if has, err := dst.hasInflightHead(t.Context(), "", c); err != nil {
		t.Fatal(err)
	} else if !has {
		t.Errorf("inflight anchor must remain when markPending sees !present")
	}
}

// TestMarkPendingTipDoesNotRemoveOwnAnchor: when markPending is called for
// the walk's own tip (parent.Undef) and no peer walk has recorded any
// parent edge above c, the walk's own inflight anchor must stay so
// finalizeWalk can clean it up at the right moment. This covers the
// crash-recovery shape where a previous walk died at tip c and we are
// re-driving from the same tip.
func TestMarkPendingTipDoesNotRemoveOwnAnchor(t *testing.T) {
	dst := newTestDatastore(t)
	c := fakeCid(t, 1)

	// Walk's lifecycle: addInflightHead, then markPending(c, walk, undef).
	if err := dst.addInflightHead(t.Context(), Head{Cid: c, HeadValue: HeadValue{Height: 1}}); err != nil {
		t.Fatal(err)
	}
	// First call: !present, parent undef → no removal regardless.
	if err := dst.markPending(t.Context(), c, 7, cid.Undef, ""); err != nil {
		t.Fatal(err)
	}
	// Second call (e.g. retry / repair re-walk): present=true, parent undef,
	// no peer parent edges above c → no upward chain, anchor must stay.
	if err := dst.markPending(t.Context(), c, 7, cid.Undef, ""); err != nil {
		t.Fatal(err)
	}
	if has, err := dst.hasInflightHead(t.Context(), "", c); err != nil {
		t.Fatal(err)
	} else if !has {
		t.Errorf("walk's own tip anchor must not be removed by markPending(parent=undef)")
	}
}

// TestMarkPendingTipSubsumesWhenPeerEdgesExist: when our tip c is already
// pending because a peer walk descended through it from above, peer's
// recorded parent edges keep c reachable from peer's anchor. Our local
// anchor at c is redundant — markPending must subsume it even with
// parent.Undef, otherwise it leaks (finalizeWalk's len(parents)==0 gate
// will never fire on c).
func TestMarkPendingTipSubsumesWhenPeerEdgesExist(t *testing.T) {
	dst := newTestDatastore(t)
	c := fakeCid(t, 1)
	peerParent := fakeCid(t, 2)

	// Peer walk (live) descended from peerParent into c.
	dst.activeTraversals.register(100)
	defer dst.activeTraversals.unregister(100)
	if err := dst.markPending(t.Context(), c, 100, peerParent, ""); err != nil {
		t.Fatal(err)
	}

	// Our walk now starts from c (e.g. fresh head delivered by gossip).
	// addInflightHead writes our anchor; processNode then calls
	// markPending(c, ours, undef).
	if err := dst.addInflightHead(t.Context(), Head{Cid: c, HeadValue: HeadValue{Height: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := dst.markPending(t.Context(), c, 200, cid.Undef, ""); err != nil {
		t.Fatal(err)
	}

	if has, err := dst.hasInflightHead(t.Context(), "", c); err != nil {
		t.Fatal(err)
	} else if has {
		t.Errorf("anchor at tip must be subsumed when peer-recorded parent edges exist above c")
	}

	// Live peer's meta must be untouched.
	tag, _, err := dst.pendingTag(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	if tag != 100 {
		t.Errorf("live peer's meta should not be overwritten, got tag=%d", tag)
	}
}

// TestMarkPendingTipSubsumesAfterDeadPeer: same shape as
// TestMarkPendingTipSubsumesWhenPeerEdgesExist but with a dead peer.
// markPending refreshes meta to our walk-id and keeps the old parent
// edges; the surviving edges still provide an upward chain (the dead
// walk's tip anchor sits above c), so our local anchor at c is
// subsumed.
func TestMarkPendingTipSubsumesAfterDeadPeer(t *testing.T) {
	dst := newTestDatastore(t)
	c := fakeCid(t, 1)
	peerParent := fakeCid(t, 2)

	// Dead walk-id 100 (never registered) recorded peerParent → c.
	if err := dst.markPending(t.Context(), c, 100, peerParent, ""); err != nil {
		t.Fatal(err)
	}

	if err := dst.addInflightHead(t.Context(), Head{Cid: c, HeadValue: HeadValue{Height: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := dst.markPending(t.Context(), c, 200, cid.Undef, ""); err != nil {
		t.Fatal(err)
	}

	if has, err := dst.hasInflightHead(t.Context(), "", c); err != nil {
		t.Fatal(err)
	} else if has {
		t.Errorf("anchor must be subsumed when dead peer's parent edges remain above c")
	}

	// Dead peer's tag was refreshed to ours.
	tag, _, err := dst.pendingTag(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	if tag != 200 {
		t.Errorf("expected refreshed meta walk-id 200, got tag=%d", tag)
	}
}

// TestMarkPendingStalledWalkTakeover: a dead peer's tag is refreshed and the
// peer's anchor is removed. The peer's parent edges remain so our cascade
// can still promote the peer's region.
func TestMarkPendingStalledWalkTakeover(t *testing.T) {
	dst := newTestDatastore(t)
	c := fakeCid(t, 1)
	parentA := fakeCid(t, 2)
	parentB := fakeCid(t, 3)

	// Stalled walk-id 100 (never registered) markPending'd c.
	if err := dst.markPending(t.Context(), c, 100, parentA, ""); err != nil {
		t.Fatal(err)
	}
	if err := dst.addInflightHead(t.Context(), Head{Cid: c, HeadValue: HeadValue{Height: 1}}); err != nil {
		t.Fatal(err)
	}

	// Walk-id 200 reaches c from parentB. 100 is dead → refresh meta.
	if err := dst.markPending(t.Context(), c, 200, parentB, ""); err != nil {
		t.Fatal(err)
	}

	tag, present, err := dst.pendingTag(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	if !present || tag != 200 {
		t.Errorf("expected refreshed meta walk-id 200, got tag=%d present=%v", tag, present)
	}
	parents, err := dst.listPendingParents(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	if len(parents) != 2 {
		t.Errorf("expected both parentA + parentB, got %d: %v", len(parents), parents)
	}
	if has, err := dst.hasInflightHead(t.Context(), "", c); err != nil {
		t.Fatal(err)
	} else if has {
		t.Errorf("stalled peer's anchor must be removed during takeover")
	}
}

// TestMarkPendingLivePeerLeavesMeta: when the meta-owning walk is alive,
// our markPending leaves the meta tag alone but adds our parent edge.
// The peer's anchor IS still removed (in-band subsumption: we joined the
// region via a parent edge so it now hangs off our anchor chain too).
func TestMarkPendingLivePeerLeavesMeta(t *testing.T) {
	dst := newTestDatastore(t)
	c := fakeCid(t, 1)
	parentA := fakeCid(t, 2)
	parentB := fakeCid(t, 3)

	// Walk-id 100 takes ownership and is registered as live.
	if err := dst.markPending(t.Context(), c, 100, parentA, ""); err != nil {
		t.Fatal(err)
	}
	if err := dst.addInflightHead(t.Context(), Head{Cid: c, HeadValue: HeadValue{Height: 1}}); err != nil {
		t.Fatal(err)
	}
	dst.activeTraversals.register(100)
	defer dst.activeTraversals.unregister(100)

	// Walk-id 200 reaches c. Live peer → meta unchanged.
	if err := dst.markPending(t.Context(), c, 200, parentB, ""); err != nil {
		t.Fatal(err)
	}

	tag, _, err := dst.pendingTag(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	if tag != 100 {
		t.Errorf("live peer's meta should not be overwritten, got tag=%d", tag)
	}
	parents, err := dst.listPendingParents(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	if len(parents) != 2 {
		t.Errorf("expected both parents, got %d: %v", len(parents), parents)
	}
}

// TestMarkPendingDagNamePropagates verifies the dagName parameter ends up
// in the right inflightHeads key (the CRDT supports multiple named DAGs).
func TestMarkPendingDagNamePropagates(t *testing.T) {
	dst := newTestDatastore(t)
	c := fakeCid(t, 1)
	parent := fakeCid(t, 2)

	if err := dst.markPending(t.Context(), c, 100, cid.Undef, "dagA"); err != nil {
		t.Fatal(err)
	}
	if err := dst.addInflightHead(t.Context(), Head{Cid: c, HeadValue: HeadValue{Height: 1, DAGName: "dagA"}}); err != nil {
		t.Fatal(err)
	}

	// Subsumption with the WRONG dagName must NOT remove the dagA anchor.
	if err := dst.markPending(t.Context(), c, 200, parent, "dagB"); err != nil {
		t.Fatal(err)
	}
	if has, err := dst.hasInflightHead(t.Context(), "dagA", c); err != nil {
		t.Fatal(err)
	} else if !has {
		t.Errorf("anchor under dagA must not be removed by markPending(dagName=dagB)")
	}

	// Subsumption with the right dagName removes it.
	if err := dst.markPending(t.Context(), c, 300, parent, "dagA"); err != nil {
		t.Fatal(err)
	}
	if has, err := dst.hasInflightHead(t.Context(), "dagA", c); err != nil {
		t.Fatal(err)
	} else if has {
		t.Errorf("anchor under dagA should be removed when dagName matches")
	}
}

// ---------- pendingTag round-trip ----------

func TestPendingTagRoundTrip(t *testing.T) {
	dst := newTestDatastore(t)
	c := fakeCid(t, 1)

	if _, present, err := dst.pendingTag(t.Context(), c); err != nil {
		t.Fatal(err)
	} else if present {
		t.Errorf("fresh CID should not be present")
	}

	for _, walkID := range []uint64{1, 1<<7 - 1, 1 << 32, 1<<63 - 1} {
		if err := dst.markPending(t.Context(), c, walkID, cid.Undef, ""); err != nil {
			t.Fatal(err)
		}
		got, present, err := dst.pendingTag(t.Context(), c)
		if err != nil {
			t.Fatal(err)
		}
		if !present || got != walkID {
			t.Errorf("round-trip walkID=%d failed: got tag=%d present=%v", walkID, got, present)
		}
	}
}

// ---------- inflight head namespace ----------

func TestInflightHeadRoundTrip(t *testing.T) {
	dst := newTestDatastore(t)

	c1, c2 := fakeCid(t, 1), fakeCid(t, 2)
	heads := []Head{
		{Cid: c1, HeadValue: HeadValue{Height: 5}},
		{Cid: c2, HeadValue: HeadValue{Height: 9, DAGName: "named"}},
	}
	for _, h := range heads {
		if err := dst.addInflightHead(t.Context(), h); err != nil {
			t.Fatal(err)
		}
	}

	got, err := dst.listInflightHeads(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d (%v)", len(got), got)
	}
	byCid := map[cid.Cid]Head{}
	for _, h := range got {
		byCid[h.Cid] = h
	}
	for _, want := range heads {
		got := byCid[want.Cid]
		if got.Height != want.Height || got.DAGName != want.DAGName {
			t.Errorf("inflight head mismatch: want %+v, got %+v", want, got)
		}
	}

	if err := dst.removeInflightHead(t.Context(), "", c1); err != nil {
		t.Fatal(err)
	}
	if has, err := dst.hasInflightHead(t.Context(), "", c1); err != nil {
		t.Fatal(err)
	} else if has {
		t.Errorf("c1 anchor should be gone")
	}

	// Idempotent delete.
	if err := dst.removeInflightHead(t.Context(), "", c1); err != nil {
		t.Errorf("re-delete should be a no-op, got %v", err)
	}

	// Per-DAG listing.
	named, err := dst.listInflightHeadsForDAG(t.Context(), "named")
	if err != nil {
		t.Fatal(err)
	}
	if len(named) != 1 || named[0].Cid != c2 {
		t.Errorf("expected only c2 under 'named', got %v", named)
	}
}

// ---------- walkState frontier ----------

func TestWalkStateRecordFrontierConcurrent(t *testing.T) {
	walk := newWalkState(1, fakeCid(t, 0))
	const goroutines = 16
	const each = 32
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := range goroutines {
		go func(g int) {
			defer wg.Done()
			for i := range each {
				walk.recordFrontier(fakeCid(t, byte(g*each+i)))
			}
		}(g)
	}
	wg.Wait()
	if got := len(walk.frontier); got != goroutines*each {
		t.Errorf("expected %d frontier entries, got %d", goroutines*each, got)
	}
}

// ---------- finalizeWalk: bottom-up cascade ----------

// TestFinalizeWalkEmptyFrontier: with no frontier seeds, finalize does
// nothing and the inflight anchor stays in place (the walk did no work).
func TestFinalizeWalkEmptyFrontier(t *testing.T) {
	dst := newTestDatastore(t)
	tip := Head{Cid: fakeCid(t, 1), HeadValue: HeadValue{Height: 1}}
	if err := dst.addInflightHead(t.Context(), tip); err != nil {
		t.Fatal(err)
	}
	walk := newWalkState(99, tip.Cid)

	if err := dst.finalizeWalk(t.Context(), tip, walk); err != nil {
		t.Fatal(err)
	}
	if has, err := dst.hasInflightHead(t.Context(), "", tip.Cid); err != nil {
		t.Fatal(err)
	} else if !has {
		t.Errorf("anchor must persist when finalize did no work")
	}
}

// TestFinalizeWalkChainCascade exercises the bottom-up promote: a linear
// chain of pending CIDs with parent edges from child→parent. Frontier
// seeded with the bottom; cascade walks up to the tip.
func TestFinalizeWalkChainCascade(t *testing.T) {
	src, _, tipHead := buildChainSrc(t, 6)
	cids := chainCidsTopDown(t, src.dagService, tipHead.Cid)

	dst := newTestDatastore(t)
	// Seed pending state by hand to mirror what processNode would have
	// produced. Tip has no parent edge; each subsequent CID has the prior
	// as its parent.
	const walkID = uint64(7)
	for i, c := range cids {
		var parent cid.Cid
		if i > 0 {
			parent = cids[i-1]
		}
		if err := dst.markPending(t.Context(), c, walkID, parent, ""); err != nil {
			t.Fatal(err)
		}
	}
	if err := dst.addInflightHead(t.Context(), tipHead); err != nil {
		t.Fatal(err)
	}

	// Frontier seed: the bottom (deepest) CID. Its IPLD node has no links,
	// so readyToPromote returns true.
	walk := &walkState{id: walkID, tipCid: tipHead.Cid, frontier: []cid.Cid{cids[len(cids)-1]}}
	// Re-use src's DAG service so readyToPromote can read the IPLD nodes.
	dst.dagService = src.dagService

	if err := dst.finalizeWalk(t.Context(), tipHead, walk); err != nil {
		t.Fatal(err)
	}

	for _, c := range cids {
		if processed, err := dst.isProcessed(t.Context(), c); err != nil {
			t.Fatal(err)
		} else if !processed {
			t.Errorf("CID %s should be processed after cascade", c)
		}
	}
	if pending := countKeys(t, dst, dst.opts.crdtOpts.Namespaces.PendingBlocks); pending != 0 {
		t.Errorf("pending namespace should be empty, got %d entries", pending)
	}
	if inflight := countKeys(t, dst, dst.opts.crdtOpts.Namespaces.InflightHeads); inflight != 0 {
		t.Errorf("inflight namespace should be empty, got %d entries", inflight)
	}
}

// TestFinalizeWalkAnchorOnlyAtTopmost: a CID with len(parents) > 0 must
// NOT have its anchor removed by finalizeWalk — boundary subsumption was
// supposed to clean it earlier. We seed both anchors directly to verify
// finalizeWalk respects the gate.
func TestFinalizeWalkAnchorOnlyAtTopmost(t *testing.T) {
	src, _, tipHead := buildChainSrc(t, 3)
	cids := chainCidsTopDown(t, src.dagService, tipHead.Cid)
	if len(cids) != 3 {
		t.Fatalf("expected 3 cids, got %d", len(cids))
	}

	dst := newTestDatastore(t)
	dst.dagService = src.dagService

	const walkID = uint64(11)
	// Seed: bottom and middle as pending (with bottom as middle's child),
	// tip as inflight (the topmost). Crucially we ALSO leave a stale
	// inflight anchor on `middle` to simulate buggy state — finalize must
	// NOT remove it (because middle has parents in the cascade graph).
	for i, c := range cids {
		var parent cid.Cid
		if i > 0 {
			parent = cids[i-1]
		}
		if err := dst.markPending(t.Context(), c, walkID, parent, ""); err != nil {
			t.Fatal(err)
		}
	}
	if err := dst.addInflightHead(t.Context(), tipHead); err != nil {
		t.Fatal(err)
	}
	staleMid := Head{Cid: cids[1], HeadValue: HeadValue{Height: 2}}
	if err := dst.addInflightHead(t.Context(), staleMid); err != nil {
		t.Fatal(err)
	}

	walk := &walkState{id: walkID, tipCid: tipHead.Cid, frontier: []cid.Cid{cids[2]}}
	if err := dst.finalizeWalk(t.Context(), tipHead, walk); err != nil {
		t.Fatal(err)
	}

	if has, err := dst.hasInflightHead(t.Context(), "", tipHead.Cid); err != nil {
		t.Fatal(err)
	} else if has {
		t.Errorf("topmost anchor (tip) should be removed: len(parents)==0 case")
	}
	// The middle anchor has parents (it has a parent edge from the tip),
	// so finalize must leave it alone. (The test reproduces a state that
	// processNode's boundary subsumption would normally have cleaned at
	// descent time. We just verify finalize itself does not touch it.)
	if has, err := dst.hasInflightHead(t.Context(), "", cids[1]); err != nil {
		t.Fatal(err)
	} else if !has {
		t.Errorf("middle anchor (len(parents)>0) must be left alone by finalizeWalk")
	}
}

// TestFinalizeWalkReadyCheckBlocksUnreadyParents: a parent enqueued via
// the cascade is only promoted when ALL of its IPLD children are
// processed. This test seeds a fork where one child is processed and
// another is still pending, and checks that the fork node is NOT
// promoted prematurely.
func TestFinalizeWalkReadyCheckBlocksUnreadyParents(t *testing.T) {
	// Build: fork with two children.
	bs := mdutils.Bserv()
	dagserv := merkledag.NewDAGService(bs)
	src, srcDag, tipHead := buildChainSrc(t, 3)
	_ = bs
	_ = dagserv

	dst := newTestDatastore(t)
	dst.dagService = src.dagService

	cids := chainCidsTopDown(t, srcDag, tipHead.Cid)
	tip := cids[0]
	mid := cids[1]

	// Mark only `tip` and `mid` as pending. `bottom` is left un-pending
	// (so readyToPromote(mid) sees a non-processed link → not ready).
	const walkID = uint64(13)
	if err := dst.markPending(t.Context(), tip, walkID, cid.Undef, ""); err != nil {
		t.Fatal(err)
	}
	if err := dst.markPending(t.Context(), mid, walkID, tip, ""); err != nil {
		t.Fatal(err)
	}
	// We deliberately do NOT mark `bottom` pending or processed.
	if err := dst.addInflightHead(t.Context(), tipHead); err != nil {
		t.Fatal(err)
	}

	// Frontier seed at `mid` even though it isn't ready.
	walk := &walkState{id: walkID, tipCid: tip, frontier: []cid.Cid{mid}}
	if err := dst.finalizeWalk(t.Context(), tipHead, walk); err != nil {
		t.Fatal(err)
	}

	// mid was not ready → not promoted.
	if processed, err := dst.isProcessed(t.Context(), mid); err != nil {
		t.Fatal(err)
	} else if processed {
		t.Errorf("mid promoted despite an unprocessed child")
	}
	if processed, err := dst.isProcessed(t.Context(), tip); err != nil {
		t.Fatal(err)
	} else if processed {
		t.Errorf("tip promoted while mid is still pending")
	}
	if has, err := dst.hasInflightHead(t.Context(), "", tip); err != nil {
		t.Fatal(err)
	} else if !has {
		t.Errorf("anchor must persist when finalize halts mid-cascade")
	}
}

// ---------- end-to-end walk via Put ----------

// TestPutFinalizesPendingAndAnchor exercises the full Put path. Each Put
// goes through addDAGNode → processNode → finalizeWalk. After Put returns
// the pending and inflight namespaces must be empty and the new CID must
// be processed.
func TestPutFinalizesPendingAndAnchor(t *testing.T) {
	dst := newTestDatastore(t)

	for i := range 8 {
		if err := dst.Put(t.Context(), ds.NewKey(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	if pending := countKeys(t, dst, dst.opts.crdtOpts.Namespaces.PendingBlocks); pending != 0 {
		t.Errorf("pending namespace should be empty after Puts, got %d entries", pending)
	}
	if inflight := countKeys(t, dst, dst.opts.crdtOpts.Namespaces.InflightHeads); inflight != 0 {
		t.Errorf("inflight namespace should be empty after Puts, got %d entries", inflight)
	}
}

// TestRemoteWalkFinalizesChain replicates: source builds a chain, dest
// receives the tip via handleBlock and walks the chain.
func TestRemoteWalkFinalizesChain(t *testing.T) {
	src, srcDag, tipHead := buildChainSrc(t, 5)
	_ = src

	dstOpts := DefaultOptions()
	dstOpts.Logger = &testLogger{name: "dst: ", l: DefaultOptions().Logger}
	dstOpts.DAGSyncerTimeout = 5 * time.Second
	dst, err := New(
		dssync.MutexWrap(ds.NewMapDatastore()),
		ds.NewKey("crdttest"),
		srcDag,
		&nullBroadcaster{},
		dstOpts,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dst.Close() })

	if err := dst.handleBlock(t.Context(), tipHead); err != nil {
		t.Fatalf("handleBlock: %v", err)
	}

	cids := chainCidsTopDown(t, srcDag, tipHead.Cid)
	for _, c := range cids {
		if processed, err := dst.isProcessed(t.Context(), c); err != nil {
			t.Fatal(err)
		} else if !processed {
			t.Errorf("CID %s should be processed after walk", c)
		}
	}
	if pending := countKeys(t, dst, dst.opts.crdtOpts.Namespaces.PendingBlocks); pending != 0 {
		t.Errorf("pending namespace should be empty after walk, got %d", pending)
	}
	if inflight := countKeys(t, dst, dst.opts.crdtOpts.Namespaces.InflightHeads); inflight != 0 {
		t.Errorf("inflight namespace should be empty after walk, got %d", inflight)
	}
}

// ---------- repair / recovery ----------

// TestRepairPendingFinalizesCrashedWalk: seed a "post-crash" state by
// hand (inflight anchor + pending entries with a stalled tag) and verify
// repair re-drives the walk to completion.
func TestRepairPendingFinalizesCrashedWalk(t *testing.T) {
	src, srcDag, tipHead := buildChainSrc(t, 5)
	_ = src
	cids := chainCidsTopDown(t, srcDag, tipHead.Cid)

	dstOpts := DefaultOptions()
	dstOpts.Logger = &testLogger{name: "dst: ", l: DefaultOptions().Logger}
	dstOpts.DAGSyncerTimeout = 5 * time.Second
	dstOpts.RepairInterval = 0
	dst, err := New(
		dssync.MutexWrap(ds.NewMapDatastore()),
		ds.NewKey("crdttest"),
		srcDag,
		&nullBroadcaster{},
		dstOpts,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dst.Close() })

	// Stalled walk-id 999 — never registered → looks dead to repair.
	const stalled = uint64(999)
	for i, c := range cids {
		var parent cid.Cid
		if i > 0 {
			parent = cids[i-1]
		}
		if err := dst.markPending(t.Context(), c, stalled, parent, ""); err != nil {
			t.Fatal(err)
		}
	}
	if err := dst.addInflightHead(t.Context(), tipHead); err != nil {
		t.Fatal(err)
	}

	dst.MarkDirty(t.Context())
	if err := dst.Repair(t.Context()); err != nil {
		t.Fatalf("Repair: %v", err)
	}

	for _, c := range cids {
		if processed, err := dst.isProcessed(t.Context(), c); err != nil {
			t.Fatal(err)
		} else if !processed {
			t.Errorf("CID %s should be processed after repair", c)
		}
	}
	if pending := countKeys(t, dst, dst.opts.crdtOpts.Namespaces.PendingBlocks); pending != 0 {
		t.Errorf("pending namespace should be empty after repair, got %d", pending)
	}
	if inflight := countKeys(t, dst, dst.opts.crdtOpts.Namespaces.InflightHeads); inflight != 0 {
		t.Errorf("inflight namespace should be empty after repair, got %d", inflight)
	}
}

// TestRepairAfterPartialPromote: simulate a crash mid-finalize where some
// CIDs were already promoted to processed but others remain pending.
// Repair must finish the job idempotently.
func TestRepairAfterPartialPromote(t *testing.T) {
	src, srcDag, tipHead := buildChainSrc(t, 6)
	_ = src
	cids := chainCidsTopDown(t, srcDag, tipHead.Cid)

	dstOpts := DefaultOptions()
	dstOpts.Logger = &testLogger{name: "dst: ", l: DefaultOptions().Logger}
	dstOpts.DAGSyncerTimeout = 5 * time.Second
	dstOpts.RepairInterval = 0
	dst, err := New(
		dssync.MutexWrap(ds.NewMapDatastore()),
		ds.NewKey("crdttest"),
		srcDag,
		&nullBroadcaster{},
		dstOpts,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dst.Close() })

	// Pre-promote the bottom half; leave top half pending.
	const stalled = uint64(777)
	half := len(cids) / 2
	for i := half; i < len(cids); i++ {
		if err := dst.markProcessed(t.Context(), cids[i]); err != nil {
			t.Fatal(err)
		}
	}
	for i, c := range cids[:half] {
		var parent cid.Cid
		if i > 0 {
			parent = cids[i-1]
		}
		if err := dst.markPending(t.Context(), c, stalled, parent, ""); err != nil {
			t.Fatal(err)
		}
	}
	if err := dst.addInflightHead(t.Context(), tipHead); err != nil {
		t.Fatal(err)
	}

	dst.MarkDirty(t.Context())
	if err := dst.Repair(t.Context()); err != nil {
		t.Fatalf("Repair: %v", err)
	}

	for _, c := range cids {
		if processed, err := dst.isProcessed(t.Context(), c); err != nil {
			t.Fatal(err)
		} else if !processed {
			t.Errorf("CID %s should be processed after repair", c)
		}
	}
	if pending := countKeys(t, dst, dst.opts.crdtOpts.Namespaces.PendingBlocks); pending != 0 {
		t.Errorf("pending namespace should be empty after repair, got %d", pending)
	}
	if inflight := countKeys(t, dst, dst.opts.crdtOpts.Namespaces.InflightHeads); inflight != 0 {
		t.Errorf("inflight namespace should be empty after repair, got %d", inflight)
	}
}

// ---------- activeTraversals ----------

func TestActiveTraversalsBasic(t *testing.T) {
	a := newActiveTraversals()
	if a.isLive(7) {
		t.Errorf("fresh registry should not have 7")
	}
	a.register(7)
	if !a.isLive(7) {
		t.Errorf("after register, 7 must be live")
	}
	a.unregister(7)
	if a.isLive(7) {
		t.Errorf("after unregister, 7 must not be live")
	}
}

func TestActiveTraversalsConcurrent(t *testing.T) {
	a := newActiveTraversals()
	const ids = 256
	var wg sync.WaitGroup
	wg.Add(ids)
	for i := range ids {
		go func(id uint64) {
			defer wg.Done()
			a.register(id)
			if !a.isLive(id) {
				t.Errorf("id %d not live after register", id)
			}
		}(uint64(i))
	}
	wg.Wait()
	for i := range ids {
		a.unregister(uint64(i))
	}
}

// ---------- countingDAGSvc helper for cross-test fetch counting ----------

type countingDAGSvc struct {
	ipld.DAGService
	gets atomic.Int64
}

func (c *countingDAGSvc) Get(ctx context.Context, cd cid.Cid) (ipld.Node, error) {
	c.gets.Add(1)
	return c.DAGService.Get(ctx, cd)
}

// We don't need GetMany here — finalize uses Get per pending CID.

// TestFinalizeReadsEachPendingCIDOnce: bottom-up cascade should read each
// pending CID's IPLD node at most once via readyToPromote (frontier seeds
// pass the readiness check on first inspection).
func TestFinalizeReadsEachPendingCIDOnce(t *testing.T) {
	src, srcDag, tipHead := buildChainSrc(t, 6)
	_ = src
	cids := chainCidsTopDown(t, srcDag, tipHead.Cid)

	dst := newTestDatastore(t)
	counter := &countingDAGSvc{DAGService: srcDag}
	dst.dagService = counter

	const walkID = uint64(33)
	for i, c := range cids {
		var parent cid.Cid
		if i > 0 {
			parent = cids[i-1]
		}
		if err := dst.markPending(t.Context(), c, walkID, parent, ""); err != nil {
			t.Fatal(err)
		}
	}
	if err := dst.addInflightHead(t.Context(), tipHead); err != nil {
		t.Fatal(err)
	}

	walk := &walkState{id: walkID, tipCid: tipHead.Cid, frontier: []cid.Cid{cids[len(cids)-1]}}
	if err := dst.finalizeWalk(t.Context(), tipHead, walk); err != nil {
		t.Fatal(err)
	}
	gets := counter.gets.Load()
	// readyToPromote runs Get once per popped CID. Cascade pops each
	// pending CID exactly once (frontier seed plus the parents enqueued).
	if gets != int64(len(cids)) {
		t.Errorf("expected exactly %d Get calls (one per pending CID), got %d", len(cids), gets)
	}
}

// TestFinalizeWalkPropagatesErrors: when readyToPromote cannot read an
// IPLD node, finalizeWalk surfaces the error rather than silently
// progressing or stalling.
func TestFinalizeWalkPropagatesErrors(t *testing.T) {
	dst := newTestDatastore(t)
	c := fakeCid(t, 1)
	walk := &walkState{id: 1, tipCid: c, frontier: []cid.Cid{c}}
	if err := dst.markPending(t.Context(), c, 1, cid.Undef, ""); err != nil {
		t.Fatal(err)
	}
	// readyToPromote will call dagService.Get on c, which we never added.
	if err := dst.finalizeWalk(t.Context(), Head{Cid: c}, walk); err == nil {
		t.Errorf("expected error from missing IPLD node, got nil")
	}
}

// ---------- concurrent-walk heads invariant ----------

// gatedDAGSvc gates the first GetMany call whose argument list includes
// target. It signals on `blocked` once the gate fires, and forwards the call
// only after `release` is closed. Get/GetMany for non-target CIDs pass
// through unmodified.
type gatedDAGSvc struct {
	ipld.DAGService
	target  cid.Cid
	blocked chan struct{}
	release chan struct{}
	once    sync.Once
}

func newGatedDAGSvc(svc ipld.DAGService, target cid.Cid) *gatedDAGSvc {
	return &gatedDAGSvc{
		DAGService: svc,
		target:     target,
		blocked:    make(chan struct{}),
		release:    make(chan struct{}),
	}
}

func (g *gatedDAGSvc) GetMany(ctx context.Context, cids []cid.Cid) <-chan *ipld.NodeOption {
	for _, c := range cids {
		if c == g.target {
			g.once.Do(func() { close(g.blocked) })
			select {
			case <-g.release:
			case <-ctx.Done():
				ch := make(chan *ipld.NodeOption, 1)
				ch <- &ipld.NodeOption{Err: ctx.Err()}
				close(ch)
				return ch
			}
			break
		}
	}
	return g.DAGService.GetMany(ctx, cids)
}

// TestConcurrentWalksLeaveStaleHead documents a heads-invariant violation
// when one walk's region wraps another and ordering causes the inner
// walk's heads.Replace to fire AFTER the outer walk's boundary
// classification.
//
// Topology (links point downward toward older nodes):
//
//	tipC → Z → tipA → Y → oldHead
//
// dst starts with oldHead processed and as its only head.
//
// Two concurrent walks:
//   - walk-A: handleBranch from tipA. processNode(Y) classifies oldHead as
//     a head boundary and calls heads.Replace(oldHead, tipA).
//   - walk-C: handleBranch from tipC. processNode(Z) classifies tipA as a
//     live-pending boundary. The classification calls heads.Get(tipA);
//     if walk-A has not yet fired its Replace, tipA is NOT in heads, and
//     walk-C falls into the heads.Add(tipC) branch instead of
//     heads.Replace(tipA, tipC).
//
// We force this exact ordering by gating walk-A's GetMany([Y]) so its
// processNode(Y) cannot run until walk-C has classified tipA as a
// boundary and committed heads.Add(tipC). Then we release walk-A;
// processNode(Y) runs and writes heads.Replace(oldHead, tipA).
//
// After both walks finalize, every CID in the chain is processed and
// inflightHeads is empty — but heads contains BOTH tipC and tipA, even
// though tipA is a descendant of tipC and is therefore not a real tip.
func TestConcurrentWalksLeaveStaleHead(t *testing.T) {
	src, srcDag, tipCHead := buildChainSrc(t, 5)
	_ = src
	cids := chainCidsTopDown(t, srcDag, tipCHead.Cid)
	if len(cids) != 5 {
		t.Fatalf("expected 5 CIDs in chain, got %d", len(cids))
	}
	// Top-down order from buildChainSrc: [tipC, Z, tipA, Y, oldHead].
	tipC := cids[0]
	tipA := cids[2]
	y := cids[3]
	oldHead := cids[4]
	if tipCHead.Height != 5 {
		t.Fatalf("expected tipC.Height=5, got %d", tipCHead.Height)
	}
	tipAHead := Head{Cid: tipA, HeadValue: HeadValue{Height: 3}}
	oldHeadHead := Head{Cid: oldHead, HeadValue: HeadValue{Height: 1}}

	gated := newGatedDAGSvc(srcDag, y)

	dstOpts := DefaultOptions()
	dstOpts.Logger = &testLogger{name: "dst: ", l: DefaultOptions().Logger}
	dstOpts.DAGSyncerTimeout = 30 * time.Second
	dst, err := New(
		dssync.MutexWrap(ds.NewMapDatastore()),
		ds.NewKey("crdttest"),
		gated,
		&nullBroadcaster{},
		dstOpts,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dst.Close() })

	// Steady state: oldHead is processed and is the only head.
	if err := dst.handleBlock(t.Context(), oldHeadHead); err != nil {
		t.Fatalf("seed oldHead: %v", err)
	}
	initial, _, err := dst.heads.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(initial) != 1 || initial[0].Cid != oldHead {
		t.Fatalf("expected initial heads = {oldHead}, got %v", initial)
	}

	// walk-A: enters handleBranch. processNode(tipA) returns [Y]; the
	// recursive sendNewJobs([Y]) calls GetMany([Y]) and the gate holds.
	walkADone := make(chan error, 1)
	go func() {
		walkADone <- dst.handleBranch(t.Context(), tipAHead, tipA)
	}()

	select {
	case <-gated.blocked:
	case <-time.After(10 * time.Second):
		t.Fatal("walk-A never reached the GetMany([Y]) gate")
	}

	// walk-C: runs to completion while walk-A is gated. processNode(Z)
	// finds tipA pending under live walk-A but NOT yet in heads, so
	// walk-C takes the heads.Add(tipC) branch (not heads.Replace).
	if err := dst.handleBranch(t.Context(), tipCHead, tipC); err != nil {
		t.Fatalf("walk-C handleBranch: %v", err)
	}

	// Release walk-A. processNode(Y) classifies oldHead as a head
	// boundary and calls heads.Replace(oldHead, tipA). finalizeWalk then
	// cascades up through Y → tipA → Z → tipC.
	close(gated.release)
	select {
	case err := <-walkADone:
		if err != nil {
			t.Fatalf("walk-A handleBranch: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("walk-A did not complete after gate release")
	}

	// Sanity: all CIDs processed, no leftover pending or inflight state.
	for _, c := range cids {
		processed, perr := dst.isProcessed(t.Context(), c)
		if perr != nil {
			t.Fatal(perr)
		}
		if !processed {
			t.Errorf("CID %s should be processed", c)
		}
	}
	if pending := countKeys(t, dst, dst.opts.crdtOpts.Namespaces.PendingBlocks); pending != 0 {
		t.Errorf("pending namespace should be empty, got %d", pending)
	}
	if inflight := countKeys(t, dst, dst.opts.crdtOpts.Namespaces.InflightHeads); inflight != 0 {
		t.Errorf("inflight namespace should be empty, got %d", inflight)
	}

	// Heads invariant: tipC is the only true tip; tipA is a descendant of
	// tipC and must not be in heads.
	finalHeads, _, err := dst.heads.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, h := range finalHeads {
		got = append(got, h.Cid.String())
	}
	if len(finalHeads) != 1 || finalHeads[0].Cid != tipC {
		t.Errorf("heads invariant violated: expected {tipC=%s}, got %v", tipC, got)
	}
}
