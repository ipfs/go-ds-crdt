package crdt

import (
	"testing"

	dshelp "github.com/ipfs/boxo/datastore/dshelp"
	mdutils "github.com/ipfs/boxo/ipld/merkledag/test"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	pb "github.com/ipfs/go-ds-crdt/pb"
	ipld "github.com/ipfs/go-ipld-format"
)

func newTestSet(t *testing.T, dagService ipld.DAGService, hooks setHooks) *set {
	t.Helper()
	store := dssync.MutexWrap(ds.NewMapDatastore())
	df := func() Delta { return &pbDelta{Delta: &pb.Delta{}} }
	s, err := newCRDTSet(t.Context(), store, ds.NewKey("/testset"), dagService, DefaultOptions().Logger, hooks, df)
	if err != nil {
		t.Fatalf("newCRDTSet: %v", err)
	}
	return s
}

// addElem creates a real DAG block for (key, value, prio), stores it in
// dagService, seeds the set with putElems, and returns the block key (element ID).
func addElem(t *testing.T, s *set, dagService ipld.DAGService, key string, value []byte, prio uint64) string {
	t.Helper()
	ctx := t.Context()
	d := &pbDelta{Delta: &pb.Delta{}}
	d.SetElements([]*pb.Element{{Key: key, Value: value}})
	d.SetPriority(prio)

	node, err := makeNode(d, nil)
	if err != nil {
		t.Fatalf("makeNode: %v", err)
	}
	if err := dagService.Add(ctx, node); err != nil {
		t.Fatalf("dagService.Add: %v", err)
	}
	blockKey := dshelp.MultihashToDsKey(node.Cid().Hash()).String()

	elems, err := d.GetElements()
	if err != nil {
		t.Fatalf("GetElements: %v", err)
	}
	if err := s.putElems(ctx, elems, blockKey, d); err != nil {
		t.Fatalf("putElems: %v", err)
	}
	return blockKey
}

// TestPutTombsEmpty verifies that putTombs with an empty list is a no-op.
func TestPutTombsEmpty(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	var fired bool
	s := newTestSet(t, mdutils.Mock(), setHooks{
		putHook:    func(PutEvent) { fired = true },
		deleteHook: func(DeleteEvent) { fired = true },
	})

	if err := s.putTombs(ctx, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.putTombs(ctx, []*pb.Element{}, nil); err != nil {
		t.Fatal(err)
	}
	if fired {
		t.Error("no hooks should fire for an empty tombstone list")
	}
}

// TestPutTombsFullDelete verifies tombstoning the only element for a key:
// the store is cleaned up and the appropriate hook fires.
func TestPutTombsFullDelete(t *testing.T) {
	t.Parallel()

	t.Run("store cleanup", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		dag := mdutils.Mock()
		s := newTestSet(t, dag, setHooks{})
		id := addElem(t, s, dag, "foo", []byte("hello"), 1)

		if err := s.putTombs(ctx, []*pb.Element{{Key: "foo", Id: id}}, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := s.store.Get(ctx, s.valueKey("foo")); err != ds.ErrNotFound {
			t.Errorf("value key should be deleted, got err=%v", err)
		}
		if _, err := s.store.Get(ctx, s.priorityKey("foo")); err != ds.ErrNotFound {
			t.Errorf("priority key should be deleted, got err=%v", err)
		}
		if inSet, _ := s.InSet(ctx, "foo"); inSet {
			t.Error("key should not be in set after full tombstone")
		}
	})

	t.Run("deleteHook", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		dag := mdutils.Mock()
		var keys []ds.Key
		s := newTestSet(t, dag, setHooks{deleteHook: func(e DeleteEvent) { keys = append(keys, e.Key) }})
		id := addElem(t, s, dag, "foo", []byte("hello"), 1)

		if err := s.putTombs(ctx, []*pb.Element{{Key: "foo", Id: id}}, nil); err != nil {
			t.Fatal(err)
		}
		if len(keys) != 1 || keys[0].String() != ds.NewKey("foo").String() {
			t.Errorf("deleteHook calls = %v, want [/foo]", keys)
		}
	})

	t.Run("deleteHook receives lastVal with hookLoadPreviousValue", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		dag := mdutils.Mock()
		var gotKey ds.Key
		var gotVal []byte
		s := newTestSet(t, dag, setHooks{
			deleteHook:            func(e DeleteEvent) { gotKey, gotVal = e.Key, e.LastValue },
			hookLoadPreviousValue: true,
		})
		id := addElem(t, s, dag, "foo", []byte("hello"), 1)

		if err := s.putTombs(ctx, []*pb.Element{{Key: "foo", Id: id}}, nil); err != nil {
			t.Fatal(err)
		}
		if gotKey.String() != ds.NewKey("foo").String() {
			t.Errorf("deleteHook key = %q, want /foo", gotKey)
		}
		if string(gotVal) != "hello" {
			t.Errorf("deleteHook lastVal = %q, want hello", gotVal)
		}
	})
}

// TestPutTombsNilValue verifies correct hook behavior when a key's stored value
// is nil (e.g. Put(ctx, k, nil)). A nil stored value is distinct from "key not
// found": delete hooks must fire, and deleteHook must receive nil as lastVal.
func TestPutTombsNilValue(t *testing.T) {
	t.Parallel()

	t.Run("deleteHook fires for nil value", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		dag := mdutils.Mock()
		var deleteFired bool
		s := newTestSet(t, dag, setHooks{
			deleteHook: func(DeleteEvent) { deleteFired = true },
		})
		id := addElem(t, s, dag, "foo", nil, 1)

		if err := s.putTombs(ctx, []*pb.Element{{Key: "foo", Id: id}}, nil); err != nil {
			t.Fatal(err)
		}
		if !deleteFired {
			t.Error("deleteHook must fire even when the stored value is nil")
		}
	})

	t.Run("deleteHook receives nil lastVal", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		dag := mdutils.Mock()
		var gotKey ds.Key
		var gotVal []byte
		var called bool
		s := newTestSet(t, dag, setHooks{
			deleteHook:            func(e DeleteEvent) { gotKey, gotVal, called = e.Key, e.LastValue, true },
			hookLoadPreviousValue: true,
		})
		id := addElem(t, s, dag, "foo", nil, 1)

		if err := s.putTombs(ctx, []*pb.Element{{Key: "foo", Id: id}}, nil); err != nil {
			t.Fatal(err)
		}
		if !called {
			t.Fatal("deleteHook must be called")
		}
		if gotKey.String() != ds.NewKey("foo").String() {
			t.Errorf("deleteHook key = %q, want /foo", gotKey)
		}
		if gotVal != nil {
			t.Errorf("deleteHook lastVal = %q, want nil", gotVal)
		}
	})

	t.Run("nil survivor keeps key with nil value", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		dag := mdutils.Mock()
		var deleteFired bool
		var putFired bool
		var putOldVal, putNewVal []byte
		s := newTestSet(t, dag, setHooks{
			putHook: func(e PutEvent) {
				putFired = true
				putOldVal = e.OldValue
				putNewVal = e.NewValue
			},
			deleteHook:            func(DeleteEvent) { deleteFired = true },
			hookLoadPreviousValue: true,
		})
		// "aaa" wins over nil lexicographically; store value is "aaa".
		addElem(t, s, dag, "foo", nil, 1)                  // loses
		id2 := addElem(t, s, dag, "foo", []byte("aaa"), 1) // wins
		putFired = false

		// After tombstoning id2, id1 (nil value) is still a live, non-tombstoned
		// element — the key must remain in the set with a nil value.
		// putTombs distinguishes "no survivors" from "nil-valued survivor" via
		// priority (p == 0 iff no survivor), so this takes the partial-tombstone
		// path: value is replaced with nil and putHook fires.
		if err := s.putTombs(ctx, []*pb.Element{{Key: "foo", Id: id2}}, nil); err != nil {
			t.Fatal(err)
		}
		if deleteFired {
			t.Error("deleteHook must not fire; a survivor remains")
		}
		if !putFired {
			t.Fatal("putHook must fire; value changed from aaa to nil")
		}
		if string(putOldVal) != "aaa" {
			t.Errorf("putHook OldValue = %q, want aaa", putOldVal)
		}
		if putNewVal != nil {
			t.Errorf("putHook NewValue = %q, want nil", putNewVal)
		}
		if inSet, err := s.InSet(ctx, "foo"); err != nil {
			t.Fatalf("InSet: %v", err)
		} else if !inSet {
			t.Error("key should remain in set while nil-valued survivor exists")
		}
	})
}

// TestPutTombsPartialTombstone verifies that tombstoning one of two elements
// leaves the survivor in the store and fires the right hook.
// Two elements at equal priority: "bbb" wins lexicographically over "aaa".
// Tombstoning the "aaa" block leaves "bbb" as the survivor.
func TestPutTombsPartialTombstone(t *testing.T) {
	t.Parallel()

	t.Run("surviving value kept", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		dag := mdutils.Mock()
		s := newTestSet(t, dag, setHooks{})
		id1 := addElem(t, s, dag, "foo", []byte("aaa"), 1)
		addElem(t, s, dag, "foo", []byte("bbb"), 1)

		if err := s.putTombs(ctx, []*pb.Element{{Key: "foo", Id: id1}}, nil); err != nil {
			t.Fatal(err)
		}
		val, err := s.Element(ctx, "foo")
		if err != nil {
			t.Fatalf("Element: %v", err)
		}
		if string(val) != "bbb" {
			t.Errorf("surviving value = %q, want bbb", val)
		}
	})

	t.Run("no hook when value unchanged", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		dag := mdutils.Mock()
		var putFired, deleteFired bool
		s := newTestSet(t, dag, setHooks{
			putHook:               func(PutEvent) { putFired = true },
			deleteHook:            func(DeleteEvent) { deleteFired = true },
			hookLoadPreviousValue: true,
		})
		id1 := addElem(t, s, dag, "foo", []byte("aaa"), 1) // "aaa" loses
		addElem(t, s, dag, "foo", []byte("bbb"), 1)        // "bbb" wins
		putFired = false

		// Tombstone the loser: "bbb" remains the winner, no observable change.
		if err := s.putTombs(ctx, []*pb.Element{{Key: "foo", Id: id1}}, nil); err != nil {
			t.Fatal(err)
		}
		if putFired || deleteFired {
			t.Error("no hook should fire when tombstoning a non-winning element")
		}
	})

	t.Run("putHook fires not deleteHook", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		dag := mdutils.Mock()
		var putVals [][]byte
		var deleteFired bool
		s := newTestSet(t, dag, setHooks{
			putHook:    func(e PutEvent) { putVals = append(putVals, e.NewValue) },
			deleteHook: func(DeleteEvent) { deleteFired = true },
		})
		addElem(t, s, dag, "foo", []byte("aaa"), 1)
		id2 := addElem(t, s, dag, "foo", []byte("bbb"), 1) // "bbb" wins, is current value
		putVals = nil                                      // discard calls from addElem

		// Tombstone the current winner ("bbb"). "aaa" becomes the new winner,
		// so the value changes and putHook must fire.
		if err := s.putTombs(ctx, []*pb.Element{{Key: "foo", Id: id2}}, nil); err != nil {
			t.Fatal(err)
		}
		if deleteFired {
			t.Error("deleteHook must not fire when a surviving element exists")
		}
		if len(putVals) != 1 || string(putVals[0]) != "aaa" {
			t.Errorf("putHook vals = %q, want [aaa]", putVals)
		}
	})

	t.Run("putHook receives newVal and oldVal with hookLoadPreviousValue", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		dag := mdutils.Mock()
		type call struct{ newVal, oldVal []byte }
		var calls []call
		s := newTestSet(t, dag, setHooks{
			putHook:               func(e PutEvent) { calls = append(calls, call{e.NewValue, e.OldValue}) },
			hookLoadPreviousValue: true,
		})
		addElem(t, s, dag, "foo", []byte("aaa"), 1)
		id2 := addElem(t, s, dag, "foo", []byte("bbb"), 1) // "bbb" wins, is current value
		calls = nil

		// Tombstone the current winner ("bbb"). "aaa" becomes the new winner:
		// putHook must fire with newVal="aaa", oldVal="bbb".
		if err := s.putTombs(ctx, []*pb.Element{{Key: "foo", Id: id2}}, nil); err != nil {
			t.Fatal(err)
		}
		if len(calls) != 1 {
			t.Fatalf("expected 1 putHook call, got %d", len(calls))
		}
		if string(calls[0].newVal) != "aaa" {
			t.Errorf("putHook newVal = %q, want aaa", calls[0].newVal)
		}
		if string(calls[0].oldVal) != "bbb" {
			t.Errorf("putHook oldVal = %q, want bbb", calls[0].oldVal)
		}
	})
}

// TestPutTombsMultipleKeys verifies that a single putTombs call handles
// tombstones for multiple keys independently.
func TestPutTombsMultipleKeys(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dag := mdutils.Mock()
	deleted := make(map[string][]byte)
	var putCalled bool
	s := newTestSet(t, dag, setHooks{
		deleteHook:            func(e DeleteEvent) { deleted[e.Key.String()] = e.LastValue },
		putHook:               func(PutEvent) { putCalled = true },
		hookLoadPreviousValue: true,
	})
	id1 := addElem(t, s, dag, "key1", []byte("val1"), 1)
	id2 := addElem(t, s, dag, "key2", []byte("val2"), 1)
	putCalled = false // discard calls from addElem setup

	tombs := []*pb.Element{{Key: "key1", Id: id1}, {Key: "key2", Id: id2}}
	if err := s.putTombs(ctx, tombs, nil); err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 2 {
		t.Fatalf("expected 2 deleteHook calls, got %d", len(deleted))
	}
	if string(deleted[ds.NewKey("key1").String()]) != "val1" {
		t.Errorf("key1 lastVal = %q, want val1", deleted[ds.NewKey("key1").String()])
	}
	if string(deleted[ds.NewKey("key2").String()]) != "val2" {
		t.Errorf("key2 lastVal = %q, want val2", deleted[ds.NewKey("key2").String()])
	}
	if putCalled {
		t.Error("putHook must not fire for full-delete tombstones")
	}
}

// TestPutTombsNonExistentKey verifies that tombstoning a block ID for a key
// that was never PUT does not crash: the tomb is recorded, and when
// hookLoadPreviousValue is set, no hooks fire since there was no prior value to
// report.
func TestPutTombsNonExistentKey(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	var putFired, deleteFired bool
	s := newTestSet(t, mdutils.Mock(), setHooks{
		putHook:               func(PutEvent) { putFired = true },
		deleteHook:            func(DeleteEvent) { deleteFired = true },
		hookLoadPreviousValue: true,
	})

	if err := s.putTombs(ctx, []*pb.Element{{Key: "ghost", Id: "fakeid"}}, nil); err != nil {
		t.Fatalf("putTombs: %v", err)
	}
	inTomb, err := s.inTombsKeyID(ctx, "ghost", "fakeid")
	if err != nil {
		t.Fatalf("inTombsKeyID: %v", err)
	}
	if !inTomb {
		t.Error("tomb entry should be written even for a key never PUT")
	}
	if inSet, _ := s.InSet(ctx, "ghost"); inSet {
		t.Error("key must not be in set")
	}
	if putFired {
		t.Error("putHook must not fire for a key that was never PUT")
	}
	if deleteFired {
		t.Error("deleteHook must not fire when key had no prior value")
	}
}

// TestPutTombsPrevValsFirstEncounterOnly verifies that prevVals is captured
// before any write for a key: deleteHook receives the pre-call value even
// when multiple tombs for the same key appear in one delta.
func TestPutTombsPrevValsFirstEncounterOnly(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dag := mdutils.Mock()
	var putFired bool
	var lastVals [][]byte
	s := newTestSet(t, dag, setHooks{
		putHook:               func(PutEvent) { putFired = true },
		deleteHook:            func(e DeleteEvent) { lastVals = append(lastVals, e.LastValue) },
		hookLoadPreviousValue: true,
	})
	id1 := addElem(t, s, dag, "foo", []byte("first"), 1)
	id2 := addElem(t, s, dag, "foo", []byte("second"), 2) // wins (higher prio)
	putFired = false

	tombs := []*pb.Element{{Key: "foo", Id: id1}, {Key: "foo", Id: id2}}
	if err := s.putTombs(ctx, tombs, nil); err != nil {
		t.Fatal(err)
	}
	if putFired {
		t.Error("putHook must not fire when tombstoning a key with a surviving element")
	}
	if len(lastVals) != 1 {
		t.Fatalf("expected 1 deleteHook call, got %d", len(lastVals))
	}
	if string(lastVals[0]) != "second" {
		t.Errorf("deleteHook lastVal = %q, want second", lastVals[0])
	}
}

// TestPutTombsIdempotent verifies that applying the same tombstone twice
// leaves the key absent from the set.
func TestPutTombsIdempotent(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dag := mdutils.Mock()
	var deleteFiredCount uint8
	s := newTestSet(t, dag, setHooks{
		deleteHook:            func(DeleteEvent) { deleteFiredCount++ },
		hookLoadPreviousValue: true,
	})
	id := addElem(t, s, dag, "foo", []byte("hello"), 1)
	tombs := []*pb.Element{{Key: "foo", Id: id}}

	for range 5 {
		if err := s.putTombs(ctx, tombs, nil); err != nil {
			t.Fatal(err)
		}
	}
	if inSet, _ := s.InSet(ctx, "foo"); inSet {
		t.Error("key should not be in set after re-tombstoning")
	}
	if deleteFiredCount != 1 {
		t.Errorf("deleteHook should fire only once for the same tombstone, got %d", deleteFiredCount)
	}
}

// TestPutElemsDeltaForwarded verifies that the Delta passed to putElems is
// forwarded identically to the putHook.
func TestPutElemsDeltaForwarded(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	var gotDelta Delta
	s := newTestSet(t, mdutils.Mock(), setHooks{
		putHook: func(e PutEvent) { gotDelta = e.Delta },
	})

	var prio uint64 = 42
	d := &pbDelta{Delta: &pb.Delta{DagName: "dag-test", Priority: prio}}
	if err := s.putElems(ctx, []*pb.Element{{Key: "foo", Value: []byte("v")}}, "block1", d); err != nil {
		t.Fatal(err)
	}
	if gotDelta != d {
		t.Fatalf("putHook delta pointer mismatch: got %p want %p", gotDelta, d)
	}
	if gotDelta.GetPriority() != prio {
		t.Errorf("forwarded delta priority = %d, want %d", gotDelta.GetPriority(), prio)
	}
	if gotDelta.GetDagName() != "dag-test" {
		t.Errorf("forwarded delta dagName = %q, want dag-test", gotDelta.GetDagName())
	}
}

// TestPutTombsFullDeleteDeltaForwarded verifies that a tombstone-only delta is
// forwarded to the deleteHook when a key is fully deleted.
func TestPutTombsFullDeleteDeltaForwarded(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dag := mdutils.Mock()
	var gotDelta Delta
	s := newTestSet(t, dag, setHooks{
		deleteHook: func(e DeleteEvent) { gotDelta = e.Delta },
	})
	id := addElem(t, s, dag, "foo", []byte("hello"), 1)

	tombDelta := &pbDelta{Delta: &pb.Delta{DagName: "tomb-dag"}}
	tombs := []*pb.Element{{Key: "foo", Id: id}}
	if err := s.putTombs(ctx, tombs, tombDelta); err != nil {
		t.Fatal(err)
	}
	if gotDelta != tombDelta {
		t.Fatalf("deleteHook delta pointer mismatch: got %p want %p", gotDelta, tombDelta)
	}
	if gotDelta.GetDagName() != "tomb-dag" {
		t.Errorf("forwarded delta dagName = %q, want tomb-dag", gotDelta.GetDagName())
	}
}

// TestPutTombsPartialPutDeltaForwarded verifies that when a tombstone removes
// the current winner and a surviving element takes over, the putHook
// receives the tombstone delta (the one that triggered the MV change), not
// the original put delta.
func TestPutTombsPartialPutDeltaForwarded(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dag := mdutils.Mock()
	var gotEvent PutEvent
	// Only capture the hook call triggered by putTombs; ignore the addElem puts.
	var capture bool
	s := newTestSet(t, dag, setHooks{
		putHook: func(e PutEvent) {
			if capture {
				gotEvent = e
			}
		},
		hookLoadPreviousValue: true,
	})
	// Seed two elements; "aaa" at prio 2 is the current winner.
	addElem(t, s, dag, "foo", []byte("zzz"), 1)
	id2 := addElem(t, s, dag, "foo", []byte("aaa"), 2)

	capture = true
	tombDelta := &pbDelta{Delta: &pb.Delta{DagName: "tomb-dag", Priority: 3}}
	// Tombstone the winner so "zzz" takes over — that changes the MV and fires putHook.
	if err := s.putTombs(ctx, []*pb.Element{{Key: "foo", Id: id2}}, tombDelta); err != nil {
		t.Fatal(err)
	}
	if gotEvent.Delta != tombDelta {
		t.Fatalf("partial-put putHook delta should be the tombstone delta: got %p want %p", gotEvent.Delta, tombDelta)
	}
	if string(gotEvent.NewValue) != "zzz" || string(gotEvent.OldValue) != "aaa" {
		t.Errorf("putHook values = (new=%q, old=%q), want (new=zzz, old=aaa)", gotEvent.NewValue, gotEvent.OldValue)
	}
}

// TestPutElemsPrioritiesReported verifies that PutEvent.NewPriority matches
// the delta priority for a normal put and PutEvent.OldPriority carries the
// replaced value's priority when HookLoadPreviousValue is set.
func TestPutElemsPrioritiesReported(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dag := mdutils.Mock()
	var gotEvent PutEvent
	capture := false
	s := newTestSet(t, dag, setHooks{
		putHook: func(e PutEvent) {
			if capture {
				gotEvent = e
			}
		},
		hookLoadPreviousValue: true,
	})
	var oldPrio uint64 = 3
	addElem(t, s, dag, "foo", []byte("old"), oldPrio)

	capture = true
	var newPrio uint64 = 7
	d := &pbDelta{Delta: &pb.Delta{Priority: newPrio}}
	if err := s.putElems(ctx, []*pb.Element{{Key: "foo", Value: []byte("new")}}, "block-new", d); err != nil {
		t.Fatal(err)
	}
	if gotEvent.NewPriority != newPrio {
		t.Errorf("NewPriority = %d, want %d (delta priority)", gotEvent.NewPriority, newPrio)
	}
	if gotEvent.OldPriority != oldPrio {
		t.Errorf("OldPriority = %d, want %d (prior value's priority)", gotEvent.OldPriority, oldPrio)
	}
}

// TestPutElemsOldPriorityZeroWhenNotLoaded verifies that OldPriority is 0
// when HookLoadPreviousValue is false, matching the OldValue behavior.
func TestPutElemsOldPriorityZeroWhenNotLoaded(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dag := mdutils.Mock()
	var gotEvent PutEvent
	capture := false
	s := newTestSet(t, dag, setHooks{
		putHook: func(e PutEvent) {
			if capture {
				gotEvent = e
			}
		},
	})
	addElem(t, s, dag, "foo", []byte("old"), 3)

	capture = true
	var newPrio uint64 = 7
	d := &pbDelta{Delta: &pb.Delta{Priority: newPrio}}
	if err := s.putElems(ctx, []*pb.Element{{Key: "foo", Value: []byte("new")}}, "block-new", d); err != nil {
		t.Fatal(err)
	}
	if gotEvent.NewPriority != newPrio {
		t.Errorf("NewPriority = %d, want %d", gotEvent.NewPriority, newPrio)
	}
	if gotEvent.OldPriority != 0 {
		t.Errorf("OldPriority = %d, want 0 (HookLoadPreviousValue is false)", gotEvent.OldPriority)
	}
}

// TestPutTombsPartialPutPriorities verifies that for a partial-tombstone put
// NewPriority carries the surviving element's priority (not the tombstone
// delta's priority) and OldPriority carries the previous winner's priority.
// This is the case where NewPriority cannot be inferred from Delta.
func TestPutTombsPartialPutPriorities(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dag := mdutils.Mock()
	var gotEvent PutEvent
	capture := false
	s := newTestSet(t, dag, setHooks{
		putHook: func(e PutEvent) {
			if capture {
				gotEvent = e
			}
		},
		hookLoadPreviousValue: true,
	})
	// Seed two elements; "aaa" at prio 2 is the current winner.
	addElem(t, s, dag, "foo", []byte("zzz"), 1)
	id2 := addElem(t, s, dag, "foo", []byte("aaa"), 2)

	capture = true
	// Tombstone delta priority (5) is deliberately different from the
	// surviving element priority (1) to prove they are distinct.
	tombDelta := &pbDelta{Delta: &pb.Delta{Priority: 5}}
	if err := s.putTombs(ctx, []*pb.Element{{Key: "foo", Id: id2}}, tombDelta); err != nil {
		t.Fatal(err)
	}
	if gotEvent.NewPriority != 1 {
		t.Errorf("NewPriority = %d, want 1 (surviving element's priority, not the tombstone delta's)", gotEvent.NewPriority)
	}
	if gotEvent.OldPriority != 2 {
		t.Errorf("OldPriority = %d, want 2 (tombstoned winner's priority)", gotEvent.OldPriority)
	}
}

// TestPutTombsFullDeleteLastPriority verifies that the DeleteEvent carries
// the priority of the removed value when HookLoadPreviousValue is set.
func TestPutTombsFullDeleteLastPriority(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dag := mdutils.Mock()
	var gotEvent DeleteEvent
	s := newTestSet(t, dag, setHooks{
		deleteHook:            func(e DeleteEvent) { gotEvent = e },
		hookLoadPreviousValue: true,
	})
	id := addElem(t, s, dag, "foo", []byte("hello"), 4)

	if err := s.putTombs(ctx, []*pb.Element{{Key: "foo", Id: id}}, &pbDelta{Delta: &pb.Delta{Priority: 9}}); err != nil {
		t.Fatal(err)
	}
	if gotEvent.LastPriority != 4 {
		t.Errorf("LastPriority = %d, want 4 (deleted value's priority)", gotEvent.LastPriority)
	}
}

// TestCustomDeltaTypeAssert verifies that callbacks can type-assert the Delta
// back to a concrete implementation to reach application-specific fields
func TestCustomDeltaTypeAssert(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	var gotCutsomField int64
	s := newTestSet(t, mdutils.Mock(), setHooks{
		putHook: func(e PutEvent) {
			if cd, ok := e.Delta.(*customDelta); ok {
				gotCutsomField = cd.CustomField
			}
		},
	})

	d := &customDelta{pbDelta: pbDelta{Delta: &pb.Delta{Priority: 1}}, CustomField: 1713456789}
	if err := s.putElems(ctx, []*pb.Element{{Key: "foo", Value: []byte("v")}}, "block1", d); err != nil {
		t.Fatal(err)
	}
	if gotCutsomField != 1713456789 {
		t.Errorf("type-asserted custom field = %d, want 1713456789", gotCutsomField)
	}
}

// customDelta embeds pbDelta and adds an application-specific field, mimicking
// how an external application supplies its own DeltaFactory.
type customDelta struct {
	pbDelta
	CustomField int64
}

// TestPutTombsHigherPriorityWins verifies that the higher-priority element
// survives when the lower-priority one is tombstoned.
func TestPutTombsHigherPriorityWins(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dag := mdutils.Mock()
	s := newTestSet(t, dag, setHooks{})
	id1 := addElem(t, s, dag, "foo", []byte("zzz"), 1) // high lex, low prio
	addElem(t, s, dag, "foo", []byte("aaa"), 2)        // low lex, high prio

	if err := s.putTombs(ctx, []*pb.Element{{Key: "foo", Id: id1}}, nil); err != nil {
		t.Fatal(err)
	}
	val, err := s.Element(ctx, "foo")
	if err != nil {
		t.Fatalf("Element: %v", err)
	}
	if string(val) != "aaa" {
		t.Errorf("surviving value = %q, want aaa (high-priority element)", val)
	}
}

// TestPurgeKeyBlocksDeltaNil verifies that hooks fired from the purge path
// receive a nil Delta, since no originating delta triggered the state change.
func TestPurgeKeyBlocksDeltaNil(t *testing.T) {
	t.Parallel()

	t.Run("full purge fires deleteHook with nil delta", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		dag := mdutils.Mock()
		var gotEvent DeleteEvent
		var fired bool
		s := newTestSet(t, dag, setHooks{
			deleteHook:            func(e DeleteEvent) { gotEvent = e; fired = true },
			hookLoadPreviousValue: true,
		})
		id := addElem(t, s, dag, "foo", []byte("hello"), 1)

		c := blockIDToCid(t, id)
		if err := s.purgeKeyBlocks(ctx, "foo", map[cid.Cid]struct{}{c: {}}, true, false); err != nil {
			t.Fatal(err)
		}
		if !fired {
			t.Fatal("deleteHook should fire for full purge")
		}
		if gotEvent.Delta != nil {
			t.Errorf("deleteHook delta = %v, want nil (no originating delta on purge path)", gotEvent.Delta)
		}
		if string(gotEvent.LastValue) != "hello" {
			t.Errorf("deleteHook lastValue = %q, want hello", gotEvent.LastValue)
		}
	})

	t.Run("partial purge fires putHook with nil delta", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		dag := mdutils.Mock()
		var gotEvent PutEvent
		var fired bool
		s := newTestSet(t, dag, setHooks{
			putHook: func(e PutEvent) { gotEvent = e; fired = true },
		})
		id1 := addElem(t, s, dag, "foo", []byte("zzz"), 1)
		addElem(t, s, dag, "foo", []byte("aaa"), 2)

		c := blockIDToCid(t, id1)
		if err := s.purgeKeyBlocks(ctx, "foo", map[cid.Cid]struct{}{c: {}}, true, false); err != nil {
			t.Fatal(err)
		}
		if !fired {
			t.Fatal("putHook should fire when partial purge changes the winner")
		}
		if gotEvent.Delta != nil {
			t.Errorf("putHook delta = %v, want nil (no originating delta on purge path)", gotEvent.Delta)
		}
	})
}

// TestPurgeKeyBlocksPriorities verifies that hooks fired from the purge path
// carry the correct old/new priorities when HookLoadPreviousValue is set.
func TestPurgeKeyBlocksPriorities(t *testing.T) {
	t.Parallel()

	t.Run("full purge reports LastPriority", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		dag := mdutils.Mock()
		var gotEvent DeleteEvent
		s := newTestSet(t, dag, setHooks{
			deleteHook:            func(e DeleteEvent) { gotEvent = e },
			hookLoadPreviousValue: true,
		})
		id := addElem(t, s, dag, "foo", []byte("hello"), 6)

		c := blockIDToCid(t, id)
		if err := s.purgeKeyBlocks(ctx, "foo", map[cid.Cid]struct{}{c: {}}, true, false); err != nil {
			t.Fatal(err)
		}
		if gotEvent.LastPriority != 6 {
			t.Errorf("LastPriority = %d, want 6", gotEvent.LastPriority)
		}
	})

	t.Run("partial purge reports old and new priorities", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		dag := mdutils.Mock()
		var gotEvent PutEvent
		s := newTestSet(t, dag, setHooks{
			putHook:               func(e PutEvent) { gotEvent = e },
			hookLoadPreviousValue: true,
		})
		addElem(t, s, dag, "foo", []byte("zzz"), 1)
		id2 := addElem(t, s, dag, "foo", []byte("aaa"), 2) // current winner

		c2 := blockIDToCid(t, id2)
		if err := s.purgeKeyBlocks(ctx, "foo", map[cid.Cid]struct{}{c2: {}}, true, false); err != nil {
			t.Fatal(err)
		}
		if gotEvent.OldPriority != 2 {
			t.Errorf("OldPriority = %d, want 2 (purged winner's priority)", gotEvent.OldPriority)
		}
		if gotEvent.NewPriority != 1 {
			t.Errorf("NewPriority = %d, want 1 (surviving element's priority)", gotEvent.NewPriority)
		}
	})
}

// TestPurgeKeyBlocksSuppression covers the two edge cases where purgeKeyBlocks
// needs to make a decision about whether to fire a hook:
//  1. full purge on a key that had no prior value → deleteHook must NOT fire
//     (nothing to notify — there was no value to delete)
//  2. partial purge where the surviving value equals the pre-purge value →
//     putHook must STILL fire, because a partial purge always replaces the
//     winning element and therefore changes the priority
func TestPurgeKeyBlocksSuppression(t *testing.T) {
	t.Parallel()

	t.Run("full purge on key with no prior value suppresses deleteHook", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		dag := mdutils.Mock()
		var deleteFired bool
		s := newTestSet(t, dag, setHooks{
			deleteHook:            func(DeleteEvent) { deleteFired = true },
			hookLoadPreviousValue: true,
		})
		// Create a real block for an unrelated key so we have a valid CID, but
		// never insert an element under "ghost": the value key for "ghost" is
		// absent, so hadPrior must be false.
		blockID := addElem(t, s, dag, "other", []byte("x"), 1)
		c := blockIDToCid(t, blockID)

		if err := s.purgeKeyBlocks(ctx, "ghost", map[cid.Cid]struct{}{c: {}}, true, false); err != nil {
			t.Fatal(err)
		}
		if deleteFired {
			t.Error("deleteHook must not fire when purged key had no prior value")
		}
	})

	t.Run("partial purge with unchanged value still fires putHook (priority changed)", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		dag := mdutils.Mock()
		var gotEvent PutEvent
		var fired bool
		s := newTestSet(t, dag, setHooks{
			putHook:               func(e PutEvent) { gotEvent = e; fired = true },
			hookLoadPreviousValue: true,
		})
		// Two elements with the SAME value at different priorities. The higher
		// priority (id2) currently wins. Purging id2 leaves id1 — identical
		// value, but lower priority. The priority change IS a state transition
		// and must be surfaced via the put hook.
		addElem(t, s, dag, "foo", []byte("same"), 1)
		id2 := addElem(t, s, dag, "foo", []byte("same"), 2)
		fired = false // reset: the second addElem fires putHook

		c := blockIDToCid(t, id2)
		if err := s.purgeKeyBlocks(ctx, "foo", map[cid.Cid]struct{}{c: {}}, true, false); err != nil {
			t.Fatal(err)
		}
		if !fired {
			t.Fatal("putHook must fire on partial purge, even when value is unchanged, to surface the priority change")
		}
		if gotEvent.OldPriority != 2 {
			t.Errorf("OldPriority = %d, want 2", gotEvent.OldPriority)
		}
		if gotEvent.NewPriority != 1 {
			t.Errorf("NewPriority = %d, want 1", gotEvent.NewPriority)
		}
	})
}

// blockIDToCid converts the blockKey returned by addElem (a datastore-keyed
// multihash) back into a CID, for use with purgeKeyBlocks.
func blockIDToCid(t *testing.T, blockID string) cid.Cid {
	t.Helper()
	mhash, err := dshelp.DsKeyToMultihash(ds.NewKey(blockID))
	if err != nil {
		t.Fatalf("DsKeyToMultihash: %v", err)
	}
	return cid.NewCidV1(cid.DagProtobuf, mhash)
}
