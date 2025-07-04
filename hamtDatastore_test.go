package crdt

import (
	"context"
	"fmt"
	"strings"
	"testing"

	mdutils "github.com/ipfs/boxo/ipld/merkledag/test"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHAMTDatastore(t *testing.T) {
	ctx := context.Background()

	// Create a DAG service for testing
	dagService := mdutils.Mock()

	// Test basic datastore operations
	t.Run("BasicOperations", func(t *testing.T) {
		// Create a HAMT datastore
		hamtDS, err := NewHAMTDatastore(ctx, dagService, cid.Undef)
		require.NoError(t, err, "Failed to create HAMT datastore")

		// Test Put operation
		key1 := ds.NewKey("/test/key1")
		value1 := []byte("value1")
		err = hamtDS.Put(ctx, key1, value1)
		require.NoError(t, err, "Failed to put key1")

		// Test Get operation
		retrievedValue, err := hamtDS.Get(ctx, key1)
		require.NoError(t, err, "Failed to get key1")
		assert.Equal(t, value1, retrievedValue, "Retrieved value doesn't match original")

		// Test Has operation
		exists, err := hamtDS.Has(ctx, key1)
		require.NoError(t, err, "Failed to check if key1 exists")
		assert.True(t, exists, "key1 should exist")

		// Test GetSize operation
		size, err := hamtDS.GetSize(ctx, key1)
		require.NoError(t, err, "Failed to get size of key1")
		assert.Equal(t, len(value1), size, "Size doesn't match value length")

		// Test Delete operation
		err = hamtDS.Delete(ctx, key1)
		require.NoError(t, err, "Failed to delete key1")

		// Verify deletion
		exists, err = hamtDS.Has(ctx, key1)
		require.NoError(t, err, "Failed to check if key1 exists after deletion")
		assert.False(t, exists, "key1 should be deleted")

		// Test Get on non-existent key
		_, err = hamtDS.Get(ctx, key1)
		assert.Equal(t, ds.ErrNotFound, err, "Get on deleted key should return ErrNotFound")

		// Test GetSize on non-existent key
		_, err = hamtDS.GetSize(ctx, key1)
		assert.Equal(t, ds.ErrNotFound, err, "GetSize on deleted key should return ErrNotFound")
	})

	// Test batch operations
	t.Run("BatchOperations", func(t *testing.T) {
		// Create a HAMT datastore
		hamtDS, err := NewHAMTDatastore(ctx, dagService, cid.Undef)
		require.NoError(t, err, "Failed to create HAMT datastore")

		// Create a batch
		batch, err := hamtDS.Batch(ctx)
		require.NoError(t, err, "Failed to create batch")

		// Add operations to the batch
		key1 := ds.NewKey("/test/batch/key1")
		value1 := []byte("batch-value1")
		err = batch.Put(ctx, key1, value1)
		require.NoError(t, err, "Failed to add Put to batch")

		key2 := ds.NewKey("/test/batch/key2")
		value2 := []byte("batch-value2")
		err = batch.Put(ctx, key2, value2)
		require.NoError(t, err, "Failed to add second Put to batch")

		// Commit the batch
		err = batch.Commit(ctx)
		require.NoError(t, err, "Failed to commit batch")

		// Verify the operations were applied
		retrievedValue1, err := hamtDS.Get(ctx, key1)
		require.NoError(t, err, "Failed to get key1 after batch")
		assert.Equal(t, value1, retrievedValue1, "Retrieved value1 doesn't match original")

		retrievedValue2, err := hamtDS.Get(ctx, key2)
		require.NoError(t, err, "Failed to get key2 after batch")
		assert.Equal(t, value2, retrievedValue2, "Retrieved value2 doesn't match original")

		// Test batch delete
		batch2, err := hamtDS.Batch(ctx)
		require.NoError(t, err, "Failed to create second batch")

		err = batch2.Delete(ctx, key1)
		require.NoError(t, err, "Failed to add Delete to batch")

		err = batch2.Commit(ctx)
		require.NoError(t, err, "Failed to commit second batch")

		// Verify deletion
		exists, err := hamtDS.Has(ctx, key1)
		require.NoError(t, err, "Failed to check if key1 exists after batch deletion")
		assert.False(t, exists, "key1 should be deleted by batch")
	})

	// Test root CID management
	t.Run("RootCIDManagement", func(t *testing.T) {
		// Create a HAMT datastore
		hamtDS1, err := NewHAMTDatastore(ctx, dagService, cid.Undef)
		require.NoError(t, err, "Failed to create first HAMT datastore")

		// Add some data
		key1 := ds.NewKey("/test/root/key1")
		value1 := []byte("root-value1")
		err = hamtDS1.Put(ctx, key1, value1)
		require.NoError(t, err, "Failed to put key1")

		// Get the root CID
		rootCID, err := hamtDS1.GetRoot(ctx)
		require.NoError(t, err, "Failed to get root CID")
		assert.NotEqual(t, cid.Undef, rootCID, "Root CID should not be undefined")

		// Create a new HAMT datastore with the same root CID
		hamtDS2, err := NewHAMTDatastore(ctx, dagService, rootCID)
		require.NoError(t, err, "Failed to create second HAMT datastore from root CID")

		// Verify the data is preserved
		retrievedValue, err := hamtDS2.Get(ctx, key1)
		require.NoError(t, err, "Failed to get key1 from second datastore")
		assert.Equal(t, value1, retrievedValue, "Retrieved value doesn't match original in second datastore")

		// Add more data to the second datastore
		key2 := ds.NewKey("/test/root/key2")
		value2 := []byte("root-value2")
		err = hamtDS2.Put(ctx, key2, value2)
		require.NoError(t, err, "Failed to put key2 in second datastore")

		// Get the new root CID
		newRootCID, err := hamtDS2.GetRoot(ctx)
		require.NoError(t, err, "Failed to get new root CID")
		assert.NotEqual(t, rootCID, newRootCID, "Root CID should change after modification")

		// Create a third datastore with the new root CID
		hamtDS3, err := NewHAMTDatastore(ctx, dagService, newRootCID)
		require.NoError(t, err, "Failed to create third HAMT datastore")

		// Verify both keys are present
		retrievedValue1, err := hamtDS3.Get(ctx, key1)
		require.NoError(t, err, "Failed to get key1 from third datastore")
		assert.Equal(t, value1, retrievedValue1, "Retrieved value1 doesn't match in third datastore")

		retrievedValue2, err := hamtDS3.Get(ctx, key2)
		require.NoError(t, err, "Failed to get key2 from third datastore")
		assert.Equal(t, value2, retrievedValue2, "Retrieved value2 doesn't match in third datastore")
	})

	// Test idempotency of operations sequence
	t.Run("SequenceIdempotency", func(t *testing.T) {
		// Create a HAMT datastore
		hamtDS1, err := NewHAMTDatastore(ctx, dagService, cid.Undef)
		require.NoError(t, err, "Failed to create first HAMT datastore")

		// Add a sequence of data
		key1 := ds.NewKey("/test/sequence/key1")
		value1 := []byte("sequence-value1")
		err = hamtDS1.Put(ctx, key1, value1)
		require.NoError(t, err, "Failed to put key1")

		key2 := ds.NewKey("/test/sequence/key2")
		value2 := []byte("sequence-value2")
		err = hamtDS1.Put(ctx, key2, value2)
		require.NoError(t, err, "Failed to put key2")

		// Get the root CID after first sequence
		rootCID1, err := hamtDS1.GetRoot(ctx)
		require.NoError(t, err, "Failed to get first root CID")

		// Create a new HAMT datastore
		hamtDS2, err := NewHAMTDatastore(ctx, dagService, cid.Undef)
		require.NoError(t, err, "Failed to create second HAMT datastore")

		// Repeat the same sequence of operations
		err = hamtDS2.Put(ctx, key1, value1)
		require.NoError(t, err, "Failed to put key1 in second datastore")

		err = hamtDS2.Put(ctx, key2, value2)
		require.NoError(t, err, "Failed to put key2 in second datastore")

		// Get the root CID after second sequence
		rootCID2, err := hamtDS2.GetRoot(ctx)
		require.NoError(t, err, "Failed to get second root CID")

		// The root CIDs should be the same (idempotent sequence)
		assert.Equal(t, rootCID1, rootCID2, "Root CIDs should be identical after applying the same sequence of operations")

		// Now let's try a different sequence
		hamtDS3, err := NewHAMTDatastore(ctx, dagService, cid.Undef)
		require.NoError(t, err, "Failed to create third HAMT datastore")

		// Apply operations in reverse order
		err = hamtDS3.Put(ctx, key2, value2)
		require.NoError(t, err, "Failed to put key2 first in third datastore")

		err = hamtDS3.Put(ctx, key1, value1)
		require.NoError(t, err, "Failed to put key1 second in third datastore")

		// Get the root CID after reverse sequence
		rootCID3, err := hamtDS3.GetRoot(ctx)
		require.NoError(t, err, "Failed to get third root CID")

		// The root CIDs should still be the same (order independence)
		assert.Equal(t, rootCID1, rootCID3, "Root CIDs should be identical regardless of operation order")

		// Let's also verify that re-applying the same operations doesn't change the root
		err = hamtDS1.Put(ctx, key1, value1)
		require.NoError(t, err, "Failed to re-put key1")

		err = hamtDS1.Put(ctx, key2, value2)
		require.NoError(t, err, "Failed to re-put key2")

		// Get the root CID after re-applying
		rootCID4, err := hamtDS1.GetRoot(ctx)
		require.NoError(t, err, "Failed to get fourth root CID")

		// The root CID should not change
		assert.Equal(t, rootCID1, rootCID4, "Root CID should not change after re-applying the same operations")
	})

	// Test query functionality
	t.Run("QueryOperations", func(t *testing.T) {
		// Create a HAMT datastore
		hamtDS, err := NewHAMTDatastore(ctx, dagService, cid.Undef)
		require.NoError(t, err, "Failed to create HAMT datastore")

		// Add some data with a common prefix
		prefix := "/test/query/"
		for i := 0; i < 5; i++ {
			key := ds.NewKey(prefix + fmt.Sprintf("a%d", i))
			value := []byte{byte('a' + i)}
			err = hamtDS.Put(ctx, key, value)
			require.NoError(t, err, "Failed to put key "+key.String())
		}

		// Add some data with a different prefix
		otherPrefix := "/test/other/"
		for i := 0; i < 3; i++ {
			key := ds.NewKey(otherPrefix + fmt.Sprintf("a%d", i))
			value := []byte{byte('A' + i)}
			err = hamtDS.Put(ctx, key, value)
			require.NoError(t, err, "Failed to put key "+key.String())
		}

		// Query with the common prefix
		q := query.Query{Prefix: prefix}
		results, err := hamtDS.Query(ctx, q)
		require.NoError(t, err, "Failed to execute query")
		defer results.Close()

		// Collect and verify results
		var entries []query.Entry
		for {
			result, ok := results.NextSync()
			if !ok {
				break
			}
			require.NoError(t, result.Error, "Query result contains error")
			entries = append(entries, result.Entry)
		}

		// Verify we got the expected number of results
		assert.Equal(t, 5, len(entries), "Query should return 5 results")

		// Verify all results have the correct prefix
		for _, entry := range entries {
			assert.True(t, strings.HasPrefix(entry.Key, prefix),
				"Result key should have the query prefix: "+entry.Key)
		}
	})

	// Test idempotency of operations
	t.Run("Idempotency", func(t *testing.T) {
		// Create a HAMT datastore
		hamtDS, err := NewHAMTDatastore(ctx, dagService, cid.Undef)
		require.NoError(t, err, "Failed to create HAMT datastore")

		// Add some data
		key1 := ds.NewKey("/test/idempotent/key1")
		value1 := []byte("idempotent-value1")
		err = hamtDS.Put(ctx, key1, value1)
		require.NoError(t, err, "Failed to put key1")

		// Get the root CID after first operation
		rootCID1, err := hamtDS.GetRoot(ctx)
		require.NoError(t, err, "Failed to get first root CID")

		// Put the same data again
		err = hamtDS.Put(ctx, key1, value1)
		require.NoError(t, err, "Failed to put key1 again")

		// Get the root CID after second operation
		rootCID2, err := hamtDS.GetRoot(ctx)
		require.NoError(t, err, "Failed to get second root CID")

		// The root CIDs should be the same (idempotent operation)
		assert.Equal(t, rootCID1, rootCID2, "Root CIDs should be identical after idempotent operation")

		// Now change the value
		value1Modified := []byte("idempotent-value1-modified")
		err = hamtDS.Put(ctx, key1, value1Modified)
		require.NoError(t, err, "Failed to put modified value")

		// Get the root CID after modification
		rootCID3, err := hamtDS.GetRoot(ctx)
		require.NoError(t, err, "Failed to get third root CID")

		// The root CID should be different now
		assert.NotEqual(t, rootCID2, rootCID3, "Root CID should change after value modification")
	})
}
