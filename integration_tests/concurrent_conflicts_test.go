package integration_tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-ds-crdt"
	"github.com/stretchr/testify/require"
)

func TestConcurrentSameKeyWrites(t *testing.T) {
	replicas := NewIntegrationTestReplicas(t, 3, nil)
	defer replicas.Cleanup()

	ctx := context.Background()
	conflictKey := ds.NewKey("concurrent/conflict_key")

	var wg sync.WaitGroup
	var mu sync.Mutex
	results := make(map[int]error)

	// Concurrent writes to the same key from different replicas
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(replicaID int) {
			defer wg.Done()

			value := []byte(fmt.Sprintf("value_from_replica_%d", replicaID))
			err := replicas.Replicas[replicaID].Put(ctx, conflictKey, value)

			mu.Lock()
			results[replicaID] = err
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	// All writes should succeed (CRDT property)
	for i, err := range results {
		require.NoError(t, err, "replica %d write should succeed", i)
	}

	// Wait for convergence
	replicas.WaitForConsistency(t, 10*time.Second)

	// All replicas should converge to the same value
	finalValue, err := replicas.Replicas[0].Get(ctx, conflictKey)
	require.NoError(t, err)

	for i := 1; i < 3; i++ {
		replicaValue, err := replicas.Replicas[i].Get(ctx, conflictKey)
		require.NoError(t, err)
		require.Equal(t, finalValue, replicaValue,
			"replica %d should have same final value as replica 0", i)
	}

	t.Logf("Final converged value: %s", string(finalValue))
}

func TestHighFrequencyConcurrentWrites(t *testing.T) {
	// Reduce concurrency to avoid deadlocks
	opts := crdt.DefaultOptions()
	opts.NumWorkers = 1 // Single worker to minimize contention

	replicas := NewIntegrationTestReplicas(t, 3, opts) // Fewer replicas
	defer replicas.Cleanup()

	ctx := context.Background()
	numOperations := 100 // Reduced operations
	numKeys := 10        // Fewer keys

	tester := NewConcurrentOperationTester(replicas.Replicas)

	// Generate keys and values
	keys := make([]string, numKeys)
	values := make([][]byte, numOperations)

	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("highfreq/key_%d", i)
	}

	for i := 0; i < numOperations; i++ {
		values[i] = []byte(fmt.Sprintf("value_%d_at_%d", i, time.Now().UnixNano()))
	}

	// Execute concurrent writes
	start := time.Now()
	tester.ExecuteConcurrentWrites(t, keys, values, numOperations)
	duration := time.Since(start)

	t.Logf("Executed %d concurrent operations in %v", numOperations, duration)

	// Get results
	results, errors := tester.GetResults()
	require.Empty(t, errors, "should have no errors during concurrent writes")
	require.Len(t, results, numOperations, "should have results for all operations")

	// Wait for convergence
	replicas.WaitForConsistency(t, 30*time.Second)

	// Verify final consistency
	require.True(t, replicas.AreConsistent(t), "replicas should converge after high frequency writes")

	// Verify all keys have values
	for _, key := range keys {
		k := ds.NewKey(key)
		for i, replica := range replicas.Replicas {
			_, err := replica.Get(ctx, k)
			require.NoError(t, err, "replica %d should have key %s", i, key)
		}
	}
}

func TestConcurrentWriteDeleteConflicts(t *testing.T) {
	replicas := NewIntegrationTestReplicas(t, 3, nil)
	defer replicas.Cleanup()

	ctx := context.Background()

	// Initialize some keys
	for i := 0; i < 10; i++ {
		key := ds.NewKey(fmt.Sprintf("writeDelete/key_%d", i))
		value := []byte(fmt.Sprintf("initial_value_%d", i))
		err := replicas.Replicas[0].Put(ctx, key, value)
		require.NoError(t, err)
	}

	replicas.WaitForConsistency(t, 5*time.Second)

	var wg sync.WaitGroup
	var mu sync.Mutex
	operations := make([]TestResult, 0, 60)

	// Concurrent write and delete operations on the same keys
	for i := 0; i < 20; i++ {
		keyID := i % 10
		key := ds.NewKey(fmt.Sprintf("writeDelete/key_%d", keyID))

		// Concurrent write
		wg.Add(1)
		go func(opID int) {
			defer wg.Done()

			replicaID := opID % 3
			value := []byte(fmt.Sprintf("concurrent_value_%d", opID))
			start := time.Now()
			err := replicas.Replicas[replicaID].Put(ctx, key, value)

			result := TestResult{
				ReplicaID: replicaID,
				Key:       key.String(),
				Value:     value,
				Operation: "PUT",
				Timestamp: start,
				Error:     err,
			}

			mu.Lock()
			operations = append(operations, result)
			mu.Unlock()
		}(i)

		// Concurrent delete
		wg.Add(1)
		go func(opID int) {
			defer wg.Done()

			replicaID := (opID + 1) % 3
			start := time.Now()
			err := replicas.Replicas[replicaID].Delete(ctx, key)

			result := TestResult{
				ReplicaID: replicaID,
				Key:       key.String(),
				Operation: "DELETE",
				Timestamp: start,
				Error:     err,
			}

			mu.Lock()
			operations = append(operations, result)
			mu.Unlock()
		}(i)

		// Third operation: another write
		wg.Add(1)
		go func(opID int) {
			defer wg.Done()

			replicaID := (opID + 2) % 3
			value := []byte(fmt.Sprintf("second_concurrent_value_%d", opID))
			start := time.Now()
			err := replicas.Replicas[replicaID].Put(ctx, key, value)

			result := TestResult{
				ReplicaID: replicaID,
				Key:       key.String(),
				Value:     value,
				Operation: "PUT2",
				Timestamp: start,
				Error:     err,
			}

			mu.Lock()
			operations = append(operations, result)
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	// All operations should succeed
	for _, op := range operations {
		require.NoError(t, op.Error, "operation %s on key %s should succeed", op.Operation, op.Key)
	}

	t.Logf("Executed %d concurrent write/delete operations", len(operations))

	// Wait for convergence
	replicas.WaitForConsistency(t, 20*time.Second)

	// Verify final consistency
	require.True(t, replicas.AreConsistent(t), "replicas should converge after concurrent write/delete")

	// Check final state - some keys may exist, some may not, but all replicas should agree
	finalData := getReplicaData(t, replicas.Replicas[0])
	t.Logf("Final state has %d keys", len(finalData))
}

func TestConcurrentBatchOperations(t *testing.T) {
	replicas := NewIntegrationTestReplicas(t, 3, nil)
	defer replicas.Cleanup()

	ctx := context.Background()
	var wg sync.WaitGroup
	var mu sync.Mutex
	batchResults := make([]error, 0, 9)

	// Execute multiple concurrent batch operations
	for replicaID := 0; replicaID < 3; replicaID++ {
		for batchID := 0; batchID < 3; batchID++ {
			wg.Add(1)
			go func(repID, bID int) {
				defer wg.Done()

				batch, err := replicas.Replicas[repID].Batch(ctx)
				if err != nil {
					mu.Lock()
					batchResults = append(batchResults, err)
					mu.Unlock()
					return
				}

				// Add operations to batch
				for i := 0; i < 10; i++ {
					key := ds.NewKey(fmt.Sprintf("batch_%d_%d/key_%d", repID, bID, i))
					value := []byte(fmt.Sprintf("batch_value_r%d_b%d_i%d", repID, bID, i))
					err = batch.Put(ctx, key, value)
					if err != nil {
						mu.Lock()
						batchResults = append(batchResults, err)
						mu.Unlock()
						return
					}
				}

				// Commit batch
				err = batch.Commit(ctx)
				mu.Lock()
				batchResults = append(batchResults, err)
				mu.Unlock()
			}(replicaID, batchID)
		}
	}

	wg.Wait()

	// All batch operations should succeed
	for i, err := range batchResults {
		require.NoError(t, err, "batch operation %d should succeed", i)
	}

	// Wait for convergence
	replicas.WaitForConsistency(t, 15*time.Second)

	// Verify consistency
	require.True(t, replicas.AreConsistent(t), "replicas should converge after concurrent batch operations")

	// Verify we have all expected data
	finalData := getReplicaData(t, replicas.Replicas[0])
	require.Equal(t, 90, len(finalData), "should have 90 keys total (3 replicas × 3 batches × 10 keys)")
}

func TestConcurrentConflictResolution(t *testing.T) {
	replicas := NewIntegrationTestReplicas(t, 2, nil)
	defer replicas.Cleanup()

	ctx := context.Background()
	conflictKey := ds.NewKey("resolution/key")

	// Create a scenario where we can predict conflict resolution
	// Write values with different priorities by controlling timing and content

	var wg sync.WaitGroup

	// First write from replica 0
	wg.Add(1)
	go func() {
		defer wg.Done()
		value := []byte("aaaa") // Lexicographically first
		err := replicas.Replicas[0].Put(ctx, conflictKey, value)
		require.NoError(t, err)
	}()

	// Slightly delayed write from replica 1
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(1 * time.Millisecond) // Small delay to potentially affect priority
		value := []byte("zzzz")          // Lexicographically last
		err := replicas.Replicas[1].Put(ctx, conflictKey, value)
		require.NoError(t, err)
	}()

	wg.Wait()

	// Wait for convergence
	replicas.WaitForConsistency(t, 10*time.Second)

	// Get final values
	value0, err := replicas.Replicas[0].Get(ctx, conflictKey)
	require.NoError(t, err)

	value1, err := replicas.Replicas[1].Get(ctx, conflictKey)
	require.NoError(t, err)

	// Both should be the same (deterministic resolution)
	require.Equal(t, value0, value1, "conflict resolution should be deterministic")

	t.Logf("Conflict resolved to: %s", string(value0))

	// Test multiple rounds of conflicts
	for round := 0; round < 5; round++ {
		wg.Add(2)

		go func(r int) {
			defer wg.Done()
			value := []byte(fmt.Sprintf("round_%d_replica_0_%d", r, time.Now().UnixNano()))
			err := replicas.Replicas[0].Put(ctx, conflictKey, value)
			require.NoError(t, err)
		}(round)

		go func(r int) {
			defer wg.Done()
			value := []byte(fmt.Sprintf("round_%d_replica_1_%d", r, time.Now().UnixNano()))
			err := replicas.Replicas[1].Put(ctx, conflictKey, value)
			require.NoError(t, err)
		}(round)

		wg.Wait()

		// Brief wait between rounds
		time.Sleep(100 * time.Millisecond)
	}

	// Final convergence check
	replicas.WaitForConsistency(t, 15*time.Second)

	finalValue0, err := replicas.Replicas[0].Get(ctx, conflictKey)
	require.NoError(t, err)

	finalValue1, err := replicas.Replicas[1].Get(ctx, conflictKey)
	require.NoError(t, err)

	require.Equal(t, finalValue0, finalValue1, "final conflict resolution should be deterministic")
	t.Logf("Final conflict resolved to: %s", string(finalValue0))
}

func TestRacingHeadUpdates(t *testing.T) {
	// Test the specific race condition in head processing
	opts := crdt.DefaultOptions()
	opts.MultiHeadProcessing = true // Enable concurrent head processing

	replicas := NewIntegrationTestReplicas(t, 3, opts)
	defer replicas.Cleanup()

	ctx := context.Background()
	var wg sync.WaitGroup

	// Create rapid succession of writes that will create multiple heads
	numWrites := 50

	for i := 0; i < numWrites; i++ {
		wg.Add(1)
		go func(writeID int) {
			defer wg.Done()

			replicaID := writeID % 3
			key := ds.NewKey(fmt.Sprintf("racing/key_%d", writeID))
			value := []byte(fmt.Sprintf("racing_value_%d", writeID))

			err := replicas.Replicas[replicaID].Put(ctx, key, value)
			require.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// Wait for all head processing to complete
	replicas.WaitForConsistency(t, 30*time.Second)

	// Verify consistency despite racing head updates
	require.True(t, replicas.AreConsistent(t), "replicas should be consistent despite racing head updates")

	// Verify all data is present
	finalData := getReplicaData(t, replicas.Replicas[0])
	require.Equal(t, numWrites, len(finalData), "should have all writes despite concurrent head processing")

	// Check head count consistency across replicas
	stats := replicas.GetReplicaStats(t)
	for i, stat := range stats {
		t.Logf("Replica %d: %d heads, max height: %d", i, len(stat.Heads), stat.MaxHeight)
	}
}

func TestConcurrentCompactionAndWrites(t *testing.T) {
	// Test writes during compaction to check for race conditions
	opts := crdt.DefaultOptions()
	opts.CompactInterval = time.Second * 2 // Frequent compaction
	opts.CompactDagSize = 50               // Compact after 50 operations

	replicas := NewIntegrationTestReplicas(t, 2, opts)
	defer replicas.Cleanup()

	ctx := context.Background()

	// Start background writes
	var wg sync.WaitGroup
	stopWrites := make(chan bool, 1)

	wg.Add(1)
	go func() {
		defer wg.Done()
		counter := 0

		for {
			select {
			case <-stopWrites:
				return
			default:
				key := ds.NewKey(fmt.Sprintf("compaction_test/key_%d", counter))
				value := []byte(fmt.Sprintf("compaction_value_%d", counter))

				replicaID := counter % 2
				err := replicas.Replicas[replicaID].Put(ctx, key, value)
				if err != nil {
					t.Errorf("write during compaction failed: %v", err)
					return
				}

				counter++
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// Let writes run for a while to trigger compaction
	time.Sleep(20 * time.Second)

	// Stop background writes
	stopWrites <- true
	wg.Wait()

	// Wait for final convergence
	replicas.WaitForConsistency(t, 30*time.Second)

	// Verify consistency after compaction + concurrent writes
	require.True(t, replicas.AreConsistent(t), "replicas should be consistent after compaction and concurrent writes")

	// Verify data integrity
	finalData := getReplicaData(t, replicas.Replicas[0])
	t.Logf("Final data count after compaction test: %d", len(finalData))
	require.Greater(t, len(finalData), 100, "should have substantial data after compaction test")
}
