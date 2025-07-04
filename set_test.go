// crdt/set_clone_priority_test.go
package crdt

import (
	"context"
	"strings"
	"testing"

	mdutils "github.com/ipfs/boxo/ipld/merkledag/test"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-ds-crdt/pb"

	logging "github.com/ipfs/go-log/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHAMTDatastoreWithSet(t *testing.T) {
	ctx := context.Background()

	// Create a DAG service for testing
	dagService := mdutils.Mock()

	// Create a namespace for the set
	namespace := ds.NewKey("/test-set")
	logger := logging.Logger("test-set")

	// Test 1: Delta idempotency with Set using HAMTDatastore
	t.Run("DeltaIdempotency", func(t *testing.T) {
		// Create a HAMT datastore
		hamtDS, err := NewHAMTDatastore(ctx, dagService, cid.Undef)
		require.NoError(t, err, "Failed to create HAMT datastore")

		// Create a set using the HAMT datastore
		set, err := newCRDTSet(ctx, hamtDS, namespace, dagService, logger, nil, nil)
		require.NoError(t, err, "Failed to create CRDT set")

		// Create some sample deltas

		delta1 := set.Add(ctx, "key1", []byte("value1"))
		err = set.Merge(ctx, delta1, "delta1")
		require.NoError(t, err, "Failed to merge delta1")

		delta2 := set.Add(ctx, "key2", []byte("value2"))
		err = set.Merge(ctx, delta2, "delta2")
		require.NoError(t, err, "Failed to merge delta2")

		delta3, err := set.Rmv(ctx, "key1")
		require.NoError(t, err, "Failed to remove key1")

		err = set.Merge(ctx, delta3, "delta3")
		require.NoError(t, err, "Failed to merge delta3")

		// Get the root CID after first application
		rootCID1, err := hamtDS.GetRoot(ctx)
		require.NoError(t, err, "Failed to get root CID")

		// Verify the state after delta application
		// key1 should be deleted by the tombstone
		inSet, err := set.InSet(ctx, "key1")
		require.NoError(t, err, "Failed to check if key1 is in set")
		assert.False(t, inSet, "key1 should be deleted by tombstone")

		// key2 and key3 should be in the set
		inSet, err = set.InSet(ctx, "key2")
		require.NoError(t, err, "Failed to check if key2 is in set")
		assert.True(t, inSet, "key2 should be in the set")

		// Now apply the same deltas again with the SAME block IDs
		err = set.Merge(ctx, delta1, "delta1") // Using the same ID as before
		require.NoError(t, err, "Failed to re-merge delta1")

		err = set.Merge(ctx, delta2, "delta2") // Using the same ID as before
		require.NoError(t, err, "Failed to re-merge delta2")

		err = set.Merge(ctx, delta3, "delta3") // Using the same ID as before
		require.NoError(t, err, "Failed to re-merge delta2")

		// Get the root CID after second application
		rootCID2, err := hamtDS.GetRoot(ctx)
		require.NoError(t, err, "Failed to get second root CID")

		// The root CIDs should be the same (idempotency) when using the same block IDs
		assert.Equal(t, rootCID1, rootCID2, "Root CIDs should be identical after applying the same deltas with the same block IDs")

	})

	// Test 2: Creating a new Set from an existing snapshot
	t.Run("RestoreFromSnapshot", func(t *testing.T) {
		// Create a HAMT datastore
		hamtDS1, err := NewHAMTDatastore(ctx, dagService, cid.Undef)
		require.NoError(t, err, "Failed to create first HAMT datastore")

		// Create a set using the HAMT datastore
		set1, err := newCRDTSet(ctx, hamtDS1, namespace, dagService, logger, nil, nil)
		require.NoError(t, err, "Failed to create first CRDT set")

		// Add some data using Add (which returns a delta)
		delta1 := set1.Add(ctx, "key1", []byte("value1"))

		// Apply the delta
		err = set1.Merge(ctx, delta1, "delta1")
		require.NoError(t, err, "Failed to merge delta from Add")

		delta2 := set1.Add(ctx, "key2", []byte("value2"))

		// Apply the delta
		err = set1.Merge(ctx, delta2, "delta2")
		require.NoError(t, err, "Failed to merge delta from second Add")

		// Get the root CID
		rootCID, err := hamtDS1.GetRoot(ctx)
		require.NoError(t, err, "Failed to get root CID")

		// Create a new HAMT datastore with the same root CID
		hamtDS2, err := NewHAMTDatastore(ctx, dagService, rootCID)
		require.NoError(t, err, "Failed to create second HAMT datastore")

		// Create a new set using the second HAMT datastore
		set2, err := newCRDTSet(ctx, hamtDS2, namespace, dagService, logger, nil, nil)
		require.NoError(t, err, "Failed to create second CRDT set")

		// Verify the data is the same
		inSet, err := set2.InSet(ctx, "key1")
		require.NoError(t, err, "Failed to check if key1 is in second set")
		assert.True(t, inSet, "key1 should be in the second set")

		inSet, err = set2.InSet(ctx, "key2")
		require.NoError(t, err, "Failed to check if key2 is in second set")
		assert.True(t, inSet, "key2 should be in the second set")
	})

	// Test 3: Conflict resolution with Set using HAMTDatastore
	t.Run("ConflictResolution", func(t *testing.T) {
		// Create a HAMT datastore
		hamtDS, err := NewHAMTDatastore(ctx, dagService, cid.Undef)
		require.NoError(t, err, "Failed to create HAMT datastore")

		// Create a set using the HAMT datastore
		set, err := newCRDTSet(ctx, hamtDS, namespace, dagService, logger, nil, nil)
		require.NoError(t, err, "Failed to create CRDT set")

		// Create deltas with conflicting updates

		// First delta - lower priority
		delta1 := &pb.Delta{
			Elements: []*pb.Element{
				{
					Key:   "conflict",
					Value: []byte("value1"),
					Id:    "id1",
				},
			},
			Priority: 1,
		}

		// Second delta - higher priority
		delta2 := &pb.Delta{
			Elements: []*pb.Element{
				{
					Key:   "conflict",
					Value: []byte("value2"),
					Id:    "id2",
				},
			},
			Priority: 2,
		}

		// Apply deltas in reverse order to test priority handling
		err = set.Merge(ctx, delta2, "block2")
		require.NoError(t, err, "Failed to merge delta2")

		err = set.Merge(ctx, delta1, "block1")
		require.NoError(t, err, "Failed to merge delta1")

		// Get the element to verify the final value
		value, err := set.Element(ctx, "conflict")
		require.NoError(t, err, "Failed to get conflict element")
		assert.Equal(t, []byte("value2"), value, "Element should have value from higher priority delta")
	})

	// Test 5: Specific tombstone handling - undo a specific value change
	t.Run("SpecificValueTombstone", func(t *testing.T) {
		// Create a HAMT datastore
		hamtDS, err := NewHAMTDatastore(ctx, dagService, cid.Undef)
		require.NoError(t, err)
		set, err := newCRDTSet(ctx, hamtDS, namespace, dagService, logger, nil, nil)
		require.NoError(t, err)

		// 1) Add the first version
		delta1 := set.Add(ctx, "multi", []byte("v1"))
		require.NoError(t, set.Merge(ctx, delta1, "id1"))

		// 2) Add a second version at higher priority
		delta2 := set.Add(ctx, "multi", []byte("v2"))
		// bump priority on the delta itself so that set.Merge uses it:
		delta2.Priority = delta1.Priority + 1
		require.NoError(t, set.Merge(ctx, delta2, "id2"))

		// sanity: we now see "v2"
		cur, err := set.Element(ctx, "multi")
		require.NoError(t, err)
		assert.Equal(t, []byte("v2"), cur)

		// 3) Now tombstone *only* the second ID, to roll back to v1
		tomb := &pb.Delta{
			Tombstones: []*pb.Element{
				{Key: "multi", Id: "/id2"},
			},
			Priority: delta2.Priority + 1,
		}
		require.NoError(t, set.Merge(ctx, tomb, "tomb-id2"))

		// final: we should be back to "v1"
		cur, err = set.Element(ctx, "multi")
		require.NoError(t, err)
		assert.Equal(t, []byte("v1"), cur)
	})
}

func TestSetCloneFrom(t *testing.T) {
	ctx := context.Background()

	// Create two separate datastores
	srcStore := ds.NewMapDatastore()
	destStore := ds.NewMapDatastore()

	// Mock DAGService and logger (don't matter here, no DAG ops happen)
	dagService := mdutils.Mock()
	logger := logging.Logger("test")

	// Create source Set
	srcSet, err := newCRDTSet(ctx, srcStore, ds.NewKey("/test/namespace"), dagService, logger, nil, nil)
	require.NoError(t, err)

	// Create destination Set (initially empty)
	destSet, err := newCRDTSet(ctx, destStore, ds.NewKey("/test/namespace"), dagService, logger, nil, nil)
	require.NoError(t, err)

	// Add some elements into source Set
	err = srcSet.Merge(ctx, &pb.Delta{
		Elements: []*pb.Element{
			{Key: "key1", Value: []byte("value1")},
			{Key: "key2", Value: []byte("value2")},
		},
		Priority: 1,
	}, "test-id-1")
	require.NoError(t, err)

	err = srcSet.Merge(ctx, &pb.Delta{
		Tombstones: []*pb.Element{
			{Key: "key3", Id: "some-id"},
		},
		Priority: 2,
	}, "test-id-2")
	require.NoError(t, err)

	// Now Clone source -> dest
	err = destSet.CloneFrom(ctx, srcSet, 0)
	require.NoError(t, err)

	// Validate: destSet must now have the same elements
	val1, err := destSet.Element(ctx, "key1")
	require.NoError(t, err)
	assert.Equal(t, []byte("value1"), val1)

	val2, err := destSet.Element(ctx, "key2")
	require.NoError(t, err)
	assert.Equal(t, []byte("value2"), val2)

	// key3 should NOT exist (tombstoned)
	_, err = destSet.Element(ctx, "key3")
	assert.Error(t, err)
	assert.Equal(t, ds.ErrNotFound, err)
}

func TestCloneFrom_WithPriorityHint_HAMT(t *testing.T) {
	ctx := context.Background()
	namespace := ds.NewKey("/test")
	dagServ := mdutils.Mock()
	logger := logging.Logger("clone-test")

	const basePrio = uint64(10)

	// 1) Build the **base** state at priority 10
	hamtDS1, err := NewHAMTDatastore(ctx, dagServ, cid.Undef)
	require.NoError(t, err)
	baseSet, err := newCRDTSet(ctx, hamtDS1, namespace, dagServ, logger, nil, nil)
	require.NoError(t, err)

	// add k1@10
	d1 := baseSet.Add(ctx, "k1", []byte("v1"))
	d1.Priority = basePrio
	require.NoError(t, baseSet.Merge(ctx, d1, "block-base-1"))

	// add k2@10
	d2 := baseSet.Add(ctx, "k2", []byte("v2"))
	d2.Priority = basePrio
	require.NoError(t, baseSet.Merge(ctx, d2, "block-base-2"))

	// capture base root
	baseRoot, err := hamtDS1.GetRoot(ctx)
	require.NoError(t, err)

	// 2) Build the **updated** state by loading from baseRoot
	hamtDS2, err := NewHAMTDatastore(ctx, dagServ, baseRoot)
	require.NoError(t, err)
	updatedSet, err := newCRDTSet(ctx, hamtDS2, namespace, dagServ, logger, nil, nil)
	require.NoError(t, err)

	// (a) update k1 → v1b @ prio=11
	du := updatedSet.Add(ctx, "k1", []byte("v1b"))
	du.Priority = basePrio + 1
	require.NoError(t, updatedSet.Merge(ctx, du, "block-upd1"))

	// (b) tombstone k2 @ prio=12
	rt, err := updatedSet.Rmv(ctx, "k2")
	require.NoError(t, err)
	rt.Priority = basePrio + 2
	require.NoError(t, updatedSet.Merge(ctx, rt, "block-tomb2"))

	// (c) add k3 → v3 @ prio=13
	da := updatedSet.Add(ctx, "k3", []byte("v3"))
	da.Priority = basePrio + 3
	require.NoError(t, updatedSet.Merge(ctx, da, "block-add3"))

	// 3) Prepare the **destination** by loading from the same baseRoot
	hamtDS3, err := NewHAMTDatastore(ctx, dagServ, baseRoot)
	require.NoError(t, err)
	var gotPuts, gotDels []string
	dstSet, err := newCRDTSet(
		ctx, hamtDS3, namespace, dagServ, logger,
		func(k string, _ []byte) { gotPuts = append(gotPuts, k) },
		func(k string) { gotDels = append(gotDels, k) },
	)
	require.NoError(t, err)

	// 4) Clone only the prio>10 changes
	require.NoError(t, dstSet.CloneFrom(ctx, updatedSet, basePrio))

	// 5a) Final values must match updatedSet
	v1, err := dstSet.Element(ctx, "k1")
	require.NoError(t, err)
	assert.Equal(t, []byte("v1b"), v1)

	_, err = dstSet.Element(ctx, "k2")
	assert.Error(t, err, "k2 should have been tombstoned")

	v3, err := dstSet.Element(ctx, "k3")
	require.NoError(t, err)
	assert.Equal(t, []byte("v3"), v3)

	// 5b) Underlying priorities
	pr1, err := dstSet.getPriority(ctx, "k1")
	require.NoError(t, err)
	assert.Equal(t, basePrio+1, pr1, "k1 should have priority 11")

	pr3, err := dstSet.getPriority(ctx, "k3")
	require.NoError(t, err)
	assert.Equal(t, basePrio+3, pr3, "k3 should have priority 13")

	// 5c) Hooks fired exactly for k1,k3 (puts); no deletes should fire
	assert.ElementsMatch(t, []string{"k1", "k3"}, gotPuts)
	assert.Empty(t, gotDels, "no DeleteHook should be fired since k2 was already tombstoned at basePrio=10")

}

func TestCloneFrom_HookConsistency_AllScenarios(t *testing.T) {
	ctx := context.Background()
	dagServ := mdutils.Mock()
	logger := logging.Logger("clone-hooks")
	namespace := ds.NewKey("/test")

	const basePrio = uint64(10)

	// Track hooks
	putHooks := map[string][]byte{}
	var delHooks []string

	hookSet := func(k string, v []byte) { putHooks[k] = v }
	hookDel := func(k string) { delHooks = append(delHooks, k) }

	// === BASE STATE ===
	baseDS, err := NewHAMTDatastore(ctx, dagServ, cid.Undef)

	require.NoError(t, err)
	baseSet, err := newCRDTSet(ctx, dssync.MutexWrap(baseDS), namespace, dagServ, logger, nil, nil)
	require.NoError(t, err)

	merge := func(s *set, d *pb.Delta, id string) { require.NoError(t, s.Merge(ctx, d, id)) }

	// k1 → unchanged
	merge(baseSet, baseSet.Add(ctx, "k1", []byte("v1")), "id-k1")

	// k2 → deleted (but removal at prio > 10, so CloneFrom sees nothing)
	d := baseSet.Add(ctx, "k2", []byte("v2"))
	d.Priority = basePrio
	merge(baseSet, d, "id-k2")

	// k9 → present in base, will be removed in update (should fire deleteHook)
	d9base := baseSet.Add(ctx, "k9", []byte("v9"))
	d9base.Priority = basePrio + 1
	merge(baseSet, d9base, "id-k9a")

	baseRoot, err := baseDS.GetRoot(ctx)
	require.NoError(t, err)

	// === UPDATED STATE ===
	updDS, err := NewHAMTDatastore(ctx, dagServ, baseRoot)
	require.NoError(t, err)
	updSet, err := newCRDTSet(ctx, updDS, namespace, dagServ, logger, nil, nil)
	require.NoError(t, err)

	// k2 → tombstone @12 (will not fire delete hook due to filtering)
	rmv2, err := updSet.Rmv(ctx, "k2")
	require.NoError(t, err)
	rmv2.Priority = basePrio + 2
	merge(updSet, rmv2, "id-k2-rmv")

	// k3 → added
	d3 := updSet.Add(ctx, "k3", []byte("v3"))
	d3.Priority = basePrio + 3
	merge(updSet, d3, "id-k3")

	// k4 → add then tombstone (but not visible, no hook)
	d4a := updSet.Add(ctx, "k4", []byte("v4"))
	d4a.Priority = basePrio + 4
	merge(updSet, d4a, "id-k4a")
	rmv4, err := updSet.Rmv(ctx, "k4")
	require.NoError(t, err)
	rmv4.Priority = basePrio + 5
	merge(updSet, rmv4, "id-k4b")

	// k5 → removed then re-added (should fire putHook once)
	d5a := updSet.Add(ctx, "k5", []byte("v5a"))
	d5a.Priority = basePrio + 6
	merge(updSet, d5a, "id-k5a")
	rmv5, err := updSet.Rmv(ctx, "k5")
	require.NoError(t, err)
	rmv5.Priority = basePrio + 7
	merge(updSet, rmv5, "id-k5b")
	d5b := updSet.Add(ctx, "k5", []byte("v5b"))
	d5b.Priority = basePrio + 8
	merge(updSet, d5b, "id-k5c")

	// k6 → v1 (base) → v2 → tombstone v2 (should revert to v1, no hook)
	d6a := updSet.Add(ctx, "k6", []byte("v6a"))
	d6a.Priority = basePrio + 1
	merge(updSet, d6a, "id-k6a")
	d6b := updSet.Add(ctx, "k6", []byte("v6b"))
	d6b.Elements[0].Id = "id-k6b"
	d6b.Priority = basePrio + 9
	merge(updSet, d6b, "id-k6b")
	rmv6, err := updSet.Rmv(ctx, "k6")
	require.NoError(t, err)

	// Filter to only tombstone v6b
	filtered := []*pb.Element{}
	for _, e := range rmv6.Tombstones {
		if e.Id == "/id-k6b" {
			filtered = append(filtered, e)
		}
	}
	rmv6.Tombstones = filtered
	rmv6.Priority = basePrio + 10
	merge(updSet, rmv6, "id-k6c")

	// k9 → present in base, removed in update (should fire deleteHook)
	// (k9 already exists from base state, just remove it)
	rmv9, err := updSet.Rmv(ctx, "k9")
	require.NoError(t, err)
	rmv9.Priority = basePrio + 2
	merge(updSet, rmv9, "id-k9b")

	// === DEST STATE ===
	dstDS, err := NewHAMTDatastore(ctx, dagServ, baseRoot)
	require.NoError(t, err)
	dstSet, err := newCRDTSet(ctx, dstDS, namespace, dagServ, logger, hookSet, hookDel)
	require.NoError(t, err)

	require.NoError(t, dstSet.CloneFrom(ctx, updSet, basePrio))

	// === ASSERTIONS ===
	assert.Equal(t, map[string][]byte{
		"k3": []byte("v3"),
		"k5": []byte("v5b"),
		"k6": []byte("v6a"),
	}, putHooks, "unexpected putHooks")

	assert.ElementsMatch(t, []string{"k9"}, delHooks, "expected delete hook for k9")

	// confirm final state matches
	final := map[string]string{}
	q, _ := dstSet.Elements(ctx, query.Query{})
	for r := range q.Next() {
		key := strings.TrimPrefix(r.Key, "/")
		final[key] = string(r.Value)
	}
	assert.Equal(t, map[string]string{
		"k1": "v1",
		"k3": "v3",
		"k5": "v5b",
		"k6": "v6a",
	}, final, "final dstSet state")
}
