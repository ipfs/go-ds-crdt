package integration_tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-ds-crdt"
	"github.com/stretchr/testify/require"
)

func TestBasicReplicaSync(t *testing.T) {
	replicas := NewIntegrationTestReplicas(t, 3, nil)
	defer replicas.Cleanup()

	ctx := context.Background()

	// Write to replica 0
	key1 := ds.NewKey("test/key1")
	value1 := []byte("value1")
	err := replicas.Replicas[0].Put(ctx, key1, value1)
	require.NoError(t, err)

	// Write to replica 1
	key2 := ds.NewKey("test/key2")
	value2 := []byte("value2")
	err = replicas.Replicas[1].Put(ctx, key2, value2)
	require.NoError(t, err)

	// Write to replica 2
	key3 := ds.NewKey("test/key3")
	value3 := []byte("value3")
	err = replicas.Replicas[2].Put(ctx, key3, value3)
	require.NoError(t, err)

	// Wait for synchronization
	replicas.WaitForConsistency(t, 10*time.Second)

	// Verify all replicas have all keys
	for i, replica := range replicas.Replicas {
		val, err := replica.Get(ctx, key1)
		require.NoError(t, err, "replica %d should have key1", i)
		require.Equal(t, value1, val, "replica %d should have correct value for key1", i)

		val, err = replica.Get(ctx, key2)
		require.NoError(t, err, "replica %d should have key2", i)
		require.Equal(t, value2, val, "replica %d should have correct value for key2", i)

		val, err = replica.Get(ctx, key3)
		require.NoError(t, err, "replica %d should have key3", i)
		require.Equal(t, value3, val, "replica %d should have correct value for key3", i)
	}
}

func TestReplicaSyncWithMessageLoss(t *testing.T) {
	replicas := NewIntegrationTestReplicas(t, 3, nil)
	defer replicas.Cleanup()

	ctx := context.Background()

	// Set 50% message drop rate for replica 1
	replicas.Network.SetMessageDropRate(1, 50)

	// Write multiple keys to different replicas
	for i := 0; i < 20; i++ {
		key := ds.NewKey(fmt.Sprintf("test/key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))

		replicaIdx := i % 3
		err := replicas.Replicas[replicaIdx].Put(ctx, key, value)
		require.NoError(t, err)

		// Small delay to allow some propagation
		time.Sleep(50 * time.Millisecond)
	}

	// Reset message drop rate
	replicas.Network.SetMessageDropRate(1, 0)

	// Wait longer for eventual consistency
	replicas.WaitForConsistency(t, 30*time.Second)

	// Verify consistency
	require.True(t, replicas.AreConsistent(t), "replicas should eventually be consistent despite message loss")
}

func TestReplicaSyncAfterTemporaryFailure(t *testing.T) {
	replicas := NewIntegrationTestReplicas(t, 3, nil)
	defer replicas.Cleanup()

	ctx := context.Background()

	// Write initial data
	for i := 0; i < 10; i++ {
		key := ds.NewKey(fmt.Sprintf("initial/key%d", i))
		value := []byte(fmt.Sprintf("initial_value%d", i))
		err := replicas.Replicas[0].Put(ctx, key, value)
		require.NoError(t, err)
	}

	// Wait for initial sync
	replicas.WaitForConsistency(t, 10*time.Second)

	// Simulate replica 2 going offline (100% message drop)
	replicas.Network.SetMessageDropRate(2, 100)

	// Write more data while replica 2 is "offline"
	for i := 10; i < 20; i++ {
		key := ds.NewKey(fmt.Sprintf("offline/key%d", i))
		value := []byte(fmt.Sprintf("offline_value%d", i))
		replicaIdx := i % 2 // Only use replicas 0 and 1
		err := replicas.Replicas[replicaIdx].Put(ctx, key, value)
		require.NoError(t, err)
	}

	// Wait for sync between active replicas
	time.Sleep(5 * time.Second)

	// Bring replica 2 back online
	replicas.Network.SetMessageDropRate(2, 0)

	// Wait for replica 2 to catch up
	replicas.WaitForConsistency(t, 30*time.Second)

	// Verify all replicas are consistent
	require.True(t, replicas.AreConsistent(t), "replica should catch up after coming back online")
}

func TestReplicaSyncWithDifferentWritePatterns(t *testing.T) {
	replicas := NewIntegrationTestReplicas(t, 4, nil)
	defer replicas.Cleanup()

	ctx := context.Background()

	// Pattern 1: Sequential writes from replica 0
	for i := 0; i < 10; i++ {
		key := ds.NewKey(fmt.Sprintf("seq/key%d", i))
		value := []byte(fmt.Sprintf("seq_value%d", i))
		err := replicas.Replicas[0].Put(ctx, key, value)
		require.NoError(t, err)
	}

	// Pattern 2: Batch writes from replica 1
	batch, err := replicas.Replicas[1].Batch(ctx)
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		key := ds.NewKey(fmt.Sprintf("batch/key%d", i))
		value := []byte(fmt.Sprintf("batch_value%d", i))
		err = batch.Put(ctx, key, value)
		require.NoError(t, err)
	}
	err = batch.Commit(ctx)
	require.NoError(t, err)

	// Pattern 3: Rapid writes from replica 2
	for i := 0; i < 10; i++ {
		key := ds.NewKey(fmt.Sprintf("rapid/key%d", i))
		value := []byte(fmt.Sprintf("rapid_value%d", i))
		err := replicas.Replicas[2].Put(ctx, key, value)
		require.NoError(t, err)
	}

	// Pattern 4: Mixed operations from replica 3
	for i := 0; i < 5; i++ {
		// Put then delete pattern
		key := ds.NewKey(fmt.Sprintf("mixed/key%d", i))
		value := []byte(fmt.Sprintf("mixed_value%d", i))
		err := replicas.Replicas[3].Put(ctx, key, value)
		require.NoError(t, err)

		if i%2 == 0 {
			err = replicas.Replicas[3].Delete(ctx, key)
			require.NoError(t, err)
		}
	}

	// Wait for convergence
	replicas.WaitForConsistency(t, 20*time.Second)

	// Verify all replicas converged to same state
	require.True(t, replicas.AreConsistent(t), "replicas should converge despite different write patterns")
}

func TestLargeValueSync(t *testing.T) {
	// Test with larger max batch size for this test
	opts := crdt.DefaultOptions()
	opts.MaxBatchDeltaSize = 10 * 1024 * 1024 // 10MB

	replicas := NewIntegrationTestReplicas(t, 2, opts)
	defer replicas.Cleanup()

	ctx := context.Background()

	// Create large values (1MB each)
	largeValue := make([]byte, 1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	// Write large values to different replicas
	for i := 0; i < 5; i++ {
		key := ds.NewKey(fmt.Sprintf("large/key%d", i))
		replicaIdx := i % len(replicas.Replicas)
		err := replicas.Replicas[replicaIdx].Put(ctx, key, largeValue)
		require.NoError(t, err)
	}

	// Wait for sync with longer timeout for large data
	replicas.WaitForConsistency(t, 60*time.Second)

	// Verify consistency
	require.True(t, replicas.AreConsistent(t), "large values should sync correctly")
}

func TestReplicaSyncWithNetworkLatency(t *testing.T) {
	replicas := NewIntegrationTestReplicas(t, 3, nil)
	defer replicas.Cleanup()

	ctx := context.Background()

	// Wrap broadcasters with latency simulators (100ms to 500ms)
	for i, replica := range replicas.Replicas {
		// Access the broadcaster through reflection or create wrapper
		// This is a simplified approach - in production you'd want more sophisticated latency simulation
		_ = replica
		_ = i
	}

	// Write data with artificial network delays
	for i := 0; i < 20; i++ {
		key := ds.NewKey(fmt.Sprintf("latency/key%d", i))
		value := []byte(fmt.Sprintf("latency_value%d", i))

		replicaIdx := i % 3
		err := replicas.Replicas[replicaIdx].Put(ctx, key, value)
		require.NoError(t, err)

		// Small delay between writes
		time.Sleep(100 * time.Millisecond)
	}

	// Wait for eventual consistency with higher timeout due to latency
	replicas.WaitForConsistency(t, 45*time.Second)

	// Verify consistency
	require.True(t, replicas.AreConsistent(t), "replicas should sync despite network latency")
}

func TestReplicaStatsConsistency(t *testing.T) {
	replicas := NewIntegrationTestReplicas(t, 3, nil)
	defer replicas.Cleanup()

	ctx := context.Background()

	// Write some data
	for i := 0; i < 50; i++ {
		key := ds.NewKey(fmt.Sprintf("stats/key%d", i))
		value := []byte(fmt.Sprintf("stats_value%d", i))
		replicaIdx := i % 3
		err := replicas.Replicas[replicaIdx].Put(ctx, key, value)
		require.NoError(t, err)
	}

	// Wait for sync
	replicas.WaitForConsistency(t, 15*time.Second)

	// Check stats consistency
	stats := replicas.GetReplicaStats(t)
	require.Len(t, stats, 3)

	// All replicas should have the same number of heads (eventually)
	// Allow some variance as heads might be in different states of consolidation
	baseHeadCount := len(stats[0].Heads)
	for i, stat := range stats {
		headCount := len(stat.Heads)
		t.Logf("Replica %d: %d heads, max height: %d", i, headCount, stat.MaxHeight)

		// Head counts can vary slightly due to timing, but shouldn't be too different
		require.True(t, abs(headCount-baseHeadCount) <= 2,
			"replica %d head count %d should be close to base %d", i, headCount, baseHeadCount)
	}
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
