package crdt

import (
	"testing"

	dshelp "github.com/ipfs/boxo/datastore/dshelp"
	mdutils "github.com/ipfs/boxo/ipld/merkledag/test"
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
	if err := s.putElems(ctx, elems, blockKey, prio); err != nil {
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
		putHook:    func(ds.Key, []byte) { fired = true },
		deleteHook: func(ds.Key) { fired = true },
	})

	if err := s.putTombs(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.putTombs(ctx, []*pb.Element{}); err != nil {
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

		if err := s.putTombs(ctx, []*pb.Element{{Key: "foo", Id: id}}); err != nil {
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
		s := newTestSet(t, dag, setHooks{deleteHook: func(k ds.Key) { keys = append(keys, k) }})
		id := addElem(t, s, dag, "foo", []byte("hello"), 1)

		if err := s.putTombs(ctx, []*pb.Element{{Key: "foo", Id: id}}); err != nil {
			t.Fatal(err)
		}
		if len(keys) != 1 || keys[0].String() != ds.NewKey("foo").String() {
			t.Errorf("deleteHook calls = %v, want [/foo]", keys)
		}
	})

	t.Run("onDelete receives lastVal", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		dag := mdutils.Mock()
		var gotKey ds.Key
		var gotVal []byte
		s := newTestSet(t, dag, setHooks{
			onDelete: func(k ds.Key, v []byte) { gotKey, gotVal = k, v },
		})
		id := addElem(t, s, dag, "foo", []byte("hello"), 1)

		if err := s.putTombs(ctx, []*pb.Element{{Key: "foo", Id: id}}); err != nil {
			t.Fatal(err)
		}
		if gotKey.String() != ds.NewKey("foo").String() {
			t.Errorf("onDelete key = %q, want /foo", gotKey)
		}
		if string(gotVal) != "hello" {
			t.Errorf("onDelete lastVal = %q, want hello", gotVal)
		}
	})
}

// TestPutTombsNilValue verifies correct hook behavior when a key's stored value
// is nil (e.g. Put(ctx, k, nil)). A nil stored value is distinct from "key not
// found": delete hooks must fire, and onDelete must receive nil as lastVal.
func TestPutTombsNilValue(t *testing.T) {
	t.Parallel()

	t.Run("deleteHook fires for nil value", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		dag := mdutils.Mock()
		var deleteFired bool
		s := newTestSet(t, dag, setHooks{
			deleteHook: func(ds.Key) { deleteFired = true },
		})
		id := addElem(t, s, dag, "foo", nil, 1)

		if err := s.putTombs(ctx, []*pb.Element{{Key: "foo", Id: id}}); err != nil {
			t.Fatal(err)
		}
		if !deleteFired {
			t.Error("deleteHook must fire even when the stored value is nil")
		}
	})

	t.Run("onDelete receives nil lastVal", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		dag := mdutils.Mock()
		var gotKey ds.Key
		var gotVal []byte
		var called bool
		s := newTestSet(t, dag, setHooks{
			onDelete: func(k ds.Key, v []byte) { gotKey, gotVal, called = k, v, true },
		})
		id := addElem(t, s, dag, "foo", nil, 1)

		if err := s.putTombs(ctx, []*pb.Element{{Key: "foo", Id: id}}); err != nil {
			t.Fatal(err)
		}
		if !called {
			t.Fatal("onDelete must be called")
		}
		if gotKey.String() != ds.NewKey("foo").String() {
			t.Errorf("onDelete key = %q, want /foo", gotKey)
		}
		if gotVal != nil {
			t.Errorf("onDelete lastVal = %q, want nil", gotVal)
		}
	})

	t.Run("nil survivor treated as full delete", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		dag := mdutils.Mock()
		var deleteFired bool
		var deleteLastVal []byte
		var putFired bool
		s := newTestSet(t, dag, setHooks{
			onPut:    func(ds.Key, []byte, []byte) { putFired = true },
			onDelete: func(_ ds.Key, v []byte) { deleteFired = true; deleteLastVal = v },
		})
		// "aaa" wins over nil lexicographically; store value is "aaa".
		id1 := addElem(t, s, dag, "foo", nil, 1)           // loses
		id2 := addElem(t, s, dag, "foo", []byte("aaa"), 1) // wins
		putFired = false

		// NOTE: this test documents a known limitation. After tombstoning id2,
		// id1 (nil value) is still a live, non-tombstoned element — so the key
		// should logically remain in the set with a nil value and fire onPut.
		// However, findBestValue returns nil for both "no survivors" and
		// "one survivor whose value is nil", so putTombs cannot distinguish the
		// two cases. It takes the full-delete path, removes the value key from
		// the store, and fires onDelete. As a result InSet returns false even
		// though a surviving element exists. Fixing this requires findBestValue
		// to return a separate found bool alongside the value.
		if err := s.putTombs(ctx, []*pb.Element{{Key: "foo", Id: id2}}); err != nil {
			t.Fatal(err)
		}
		_ = id1
		if putFired {
			t.Error("onPut must not fire; nil survivor is treated as full delete")
		}
		if !deleteFired {
			t.Fatal("onDelete must fire when nil survivor is treated as full delete")
		}
		if string(deleteLastVal) != "aaa" {
			t.Errorf("onDelete lastVal = %q, want aaa", deleteLastVal)
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

		if err := s.putTombs(ctx, []*pb.Element{{Key: "foo", Id: id1}}); err != nil {
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
			putHook:    func(ds.Key, []byte) { putFired = true },
			deleteHook: func(ds.Key) { deleteFired = true },
		})
		id1 := addElem(t, s, dag, "foo", []byte("aaa"), 1) // "aaa" loses
		addElem(t, s, dag, "foo", []byte("bbb"), 1)        // "bbb" wins
		putFired = false

		// Tombstone the loser: "bbb" remains the winner, no observable change.
		if err := s.putTombs(ctx, []*pb.Element{{Key: "foo", Id: id1}}); err != nil {
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
			putHook:    func(_ ds.Key, v []byte) { putVals = append(putVals, v) },
			deleteHook: func(ds.Key) { deleteFired = true },
		})
		addElem(t, s, dag, "foo", []byte("aaa"), 1)
		id2 := addElem(t, s, dag, "foo", []byte("bbb"), 1) // "bbb" wins, is current value
		putVals = nil                                      // discard calls from addElem

		// Tombstone the current winner ("bbb"). "aaa" becomes the new winner,
		// so the value changes and putHook must fire.
		if err := s.putTombs(ctx, []*pb.Element{{Key: "foo", Id: id2}}); err != nil {
			t.Fatal(err)
		}
		if deleteFired {
			t.Error("deleteHook must not fire when a surviving element exists")
		}
		if len(putVals) != 1 || string(putVals[0]) != "aaa" {
			t.Errorf("putHook vals = %q, want [aaa]", putVals)
		}
	})

	t.Run("onPut receives newVal and oldVal", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		dag := mdutils.Mock()
		type call struct{ newVal, oldVal []byte }
		var calls []call
		s := newTestSet(t, dag, setHooks{
			onPut: func(_ ds.Key, n, o []byte) { calls = append(calls, call{n, o}) },
		})
		addElem(t, s, dag, "foo", []byte("aaa"), 1)
		id2 := addElem(t, s, dag, "foo", []byte("bbb"), 1) // "bbb" wins, is current value
		calls = nil

		// Tombstone the current winner ("bbb"). "aaa" becomes the new winner:
		// onPut must fire with newVal="aaa", oldVal="bbb".
		if err := s.putTombs(ctx, []*pb.Element{{Key: "foo", Id: id2}}); err != nil {
			t.Fatal(err)
		}
		if len(calls) != 1 {
			t.Fatalf("expected 1 onPut call, got %d", len(calls))
		}
		if string(calls[0].newVal) != "aaa" {
			t.Errorf("onPut newVal = %q, want aaa", calls[0].newVal)
		}
		if string(calls[0].oldVal) != "bbb" {
			t.Errorf("onPut oldVal = %q, want bbb", calls[0].oldVal)
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
	var onPutCalled bool
	s := newTestSet(t, dag, setHooks{
		onDelete: func(k ds.Key, v []byte) { deleted[k.String()] = v },
		onPut:    func(ds.Key, []byte, []byte) { onPutCalled = true },
	})
	id1 := addElem(t, s, dag, "key1", []byte("val1"), 1)
	id2 := addElem(t, s, dag, "key2", []byte("val2"), 1)
	onPutCalled = false // discard calls from addElem setup

	tombs := []*pb.Element{{Key: "key1", Id: id1}, {Key: "key2", Id: id2}}
	if err := s.putTombs(ctx, tombs); err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 2 {
		t.Fatalf("expected 2 onDelete calls, got %d", len(deleted))
	}
	if string(deleted[ds.NewKey("key1").String()]) != "val1" {
		t.Errorf("key1 lastVal = %q, want val1", deleted[ds.NewKey("key1").String()])
	}
	if string(deleted[ds.NewKey("key2").String()]) != "val2" {
		t.Errorf("key2 lastVal = %q, want val2", deleted[ds.NewKey("key2").String()])
	}
	if onPutCalled {
		t.Error("onPut must not fire for full-delete tombstones")
	}
}

// TestPutTombsNonExistentKey verifies that tombstoning a block ID for a key
// that was never PUT does not crash: the tomb is recorded, and no hooks fire
// since there was no prior value to report.
func TestPutTombsNonExistentKey(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	var putFired, deleteFired bool
	s := newTestSet(t, mdutils.Mock(), setHooks{
		onPut:    func(ds.Key, []byte, []byte) { putFired = true },
		onDelete: func(ds.Key, []byte) { deleteFired = true },
	})

	if err := s.putTombs(ctx, []*pb.Element{{Key: "ghost", Id: "fakeid"}}); err != nil {
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
		t.Error("onPut must not fire for a key that was never PUT")
	}
	if deleteFired {
		t.Error("onDelete must not fire when key had no prior value")
	}
}

// TestPutTombsPrevValsFirstEncounterOnly verifies that prevVals is captured
// before any write for a key: onDelete receives the pre-call value even when
// multiple tombs for the same key appear in one delta.
func TestPutTombsPrevValsFirstEncounterOnly(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dag := mdutils.Mock()
	var putFired bool
	var lastVals [][]byte
	s := newTestSet(t, dag, setHooks{
		putHook:  func(ds.Key, []byte) { putFired = true },
		onDelete: func(_ ds.Key, v []byte) { lastVals = append(lastVals, v) },
	})
	id1 := addElem(t, s, dag, "foo", []byte("first"), 1)
	id2 := addElem(t, s, dag, "foo", []byte("second"), 2) // wins (higher prio)
	putFired = false

	tombs := []*pb.Element{{Key: "foo", Id: id1}, {Key: "foo", Id: id2}}
	if err := s.putTombs(ctx, tombs); err != nil {
		t.Fatal(err)
	}
	if putFired {
		t.Error("putHook must not fire when tombstoning a key with a surviving element")
	}
	if len(lastVals) != 1 {
		t.Fatalf("expected 1 onDelete call, got %d", len(lastVals))
	}
	if string(lastVals[0]) != "second" {
		t.Errorf("onDelete lastVal = %q, want second", lastVals[0])
	}
}

// TestPutTombsIdempotent verifies that applying the same tombstone twice
// leaves the key absent from the set.
func TestPutTombsIdempotent(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dag := mdutils.Mock()
	var deleteFiredCount uint8
	s := newTestSet(t, dag, setHooks{deleteHook: func(ds.Key) { deleteFiredCount++ }})
	id := addElem(t, s, dag, "foo", []byte("hello"), 1)
	tombs := []*pb.Element{{Key: "foo", Id: id}}

	for range 5 {
		if err := s.putTombs(ctx, tombs); err != nil {
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

// TestPutTombsHigherPriorityWins verifies that the higher-priority element
// survives when the lower-priority one is tombstoned.
func TestPutTombsHigherPriorityWins(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dag := mdutils.Mock()
	s := newTestSet(t, dag, setHooks{})
	id1 := addElem(t, s, dag, "foo", []byte("zzz"), 1) // high lex, low prio
	addElem(t, s, dag, "foo", []byte("aaa"), 2)        // low lex, high prio

	if err := s.putTombs(ctx, []*pb.Element{{Key: "foo", Id: id1}}); err != nil {
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
