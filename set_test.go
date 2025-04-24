package crdt

import (
	"context"
	"testing"

	mdutils "github.com/ipfs/boxo/ipld/merkledag/test"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
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

	// Test 4: Tombstone handling with Set using HAMTDatastore
	t.Run("TombstoneHandling", func(t *testing.T) {
		// Create a HAMT datastore
		hamtDS, err := NewHAMTDatastore(ctx, dagService, cid.Undef)
		require.NoError(t, err, "Failed to create HAMT datastore")

		// Create a set using the HAMT datastore
		set, err := newCRDTSet(ctx, hamtDS, namespace, dagService, logger, nil, nil)
		require.NoError(t, err, "Failed to create CRDT set")

		// Add a key
		delta := set.Add(ctx, "to-delete", []byte("original"))

		err = set.Merge(ctx, delta, "add-delta")
		require.NoError(t, err, "Failed to merge Add delta")

		// Verify the key is in the set
		inSet, err := set.InSet(ctx, "to-delete")
		require.NoError(t, err, "Failed to check if to-delete is in set")
		assert.True(t, inSet, "to-delete should be in the set")

		// Create a Remove delta
		removeDelta, err := set.Rmv(ctx, "to-delete")
		require.NoError(t, err, "Failed to create rmv delta")

		// Apply the Remove delta
		err = set.Merge(ctx, removeDelta, "rmv-delta")
		require.NoError(t, err, "Failed to merge Remove delta")

		// Verify the key is no longer in the set
		inSet, err = set.InSet(ctx, "to-delete")
		require.NoError(t, err, "Failed to check if to-delete is in set after removal")
		assert.False(t, inSet, "to-delete should not be in the set after removal")

		// Try to add the key again with a lower priority
		lowerDelta := &pb.Delta{
			Elements: []*pb.Element{
				{
					Key:   "to-delete",
					Value: []byte("new-value"),
					Id:    "lower-id",
				},
			},
			Priority: removeDelta.Priority - 1, // Lower priority than the tombstone
		}

		err = set.Merge(ctx, lowerDelta, "lower-delta")
		require.NoError(t, err, "Failed to merge lower priority delta")

		// Verify the key is still not in the set
		inSet, err = set.InSet(ctx, "to-delete")
		require.NoError(t, err, "Failed to check if to-delete is in set after lower priority add")
		assert.False(t, inSet, "to-delete should not be in the set (tombstone wins)")

		// Now add with higher priority
		higherDelta := &pb.Delta{
			Elements: []*pb.Element{
				{
					Key:   "to-delete",
					Value: []byte("higher-priority"),
					Id:    "higher-id",
				},
			},
			Priority: removeDelta.Priority + 1, // Higher priority than the tombstone
		}

		err = set.Merge(ctx, higherDelta, "higher-delta")
		require.NoError(t, err, "Failed to merge higher priority delta")

		// Verify the key is now in the set
		inSet, err = set.InSet(ctx, "to-delete")
		require.NoError(t, err, "Failed to check if to-delete is in set after higher priority add")
		assert.True(t, inSet, "to-delete should be in the set (higher priority wins)")

		// Get the element to verify the value
		value, err := set.Element(ctx, "to-delete")
		require.NoError(t, err, "Failed to get to-delete element")
		assert.Equal(t, []byte("higher-priority"), value, "Element should have value from higher priority delta")
	})

	// Test 5: Specific tombstone handling - undo a specific value change
	t.Run("SpecificTombstoneHandling", func(t *testing.T) {
		// Create a HAMT datastore
		hamtDS, err := NewHAMTDatastore(ctx, dagService, cid.Undef)
		require.NoError(t, err, "Failed to create HAMT datastore")

		// Create a set using the HAMT datastore
		set, err := newCRDTSet(ctx, hamtDS, namespace, dagService, logger, nil, nil)
		require.NoError(t, err, "Failed to create CRDT set")

		// Add initial value
		initialDelta := set.Add(ctx, "multi-value", []byte("initial"))
		err = set.Merge(ctx, initialDelta, "initial-delta")
		require.NoError(t, err, "Failed to merge initial delta")

		// Add a second value with higher priority
		secondDelta := &pb.Delta{
			Elements: []*pb.Element{
				{
					Key:   "multi-value",
					Value: []byte("second"),
					Id:    "second-id",
				},
			},
			Priority: initialDelta.Priority + 1,
		}
		err = set.Merge(ctx, secondDelta, "second-delta")
		require.NoError(t, err, "Failed to merge second delta")

		// Verify the current value is "second"
		value, err := set.Element(ctx, "multi-value")
		require.NoError(t, err, "Failed to get multi-value element")
		assert.Equal(t, []byte("second"), value, "Element should have the second value")

		// Now create a tombstone specifically for the second value (not the whole key)
		tombstoneDelta := &pb.Delta{
			Tombstones: []*pb.Element{
				{
					Key: "multi-value",
					Id:  "second-id", // Specifically target the second-id
				},
			},
			Priority: secondDelta.Priority + 1, // Higher priority to ensure it wins
		}
		err = set.Merge(ctx, tombstoneDelta, "tombstone-delta")
		require.NoError(t, err, "Failed to merge tombstone delta")

		// The key should still be in the set (because we only tombstoned one value)
		inSet, err := set.InSet(ctx, "multi-value")
		require.NoError(t, err, "Failed to check if multi-value is in set")
		assert.True(t, inSet, "multi-value should still be in the set")

		// The value should now be "initial" (the only remaining value)
		value, err = set.Element(ctx, "multi-value")
		require.NoError(t, err, "Failed to get multi-value element after tombstone")
		assert.Equal(t, []byte("initial"), value, "Element should have rolled back to initial value")
	})
}
