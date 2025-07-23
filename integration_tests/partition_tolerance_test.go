package integration_tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/stretchr/testify/require"
)

func TestNetworkPartitionTolerance(t *testing.T) {
	replicas := NewIntegrationTestReplicas(t, 5, nil)
	defer replicas.Cleanup()

	ctx := context.Background()

	// Initial data sync
	for i := 0; i < 10; i++ {
		key := ds.NewKey(fmt.Sprintf("initial/key%d", i))
		value := []byte(fmt.Sprintf("initial_value%d", i))
		err := replicas.Replicas[0].Put(ctx, key, value)
		require.NoError(t, err)
	}

	// Wait for initial sync
	replicas.WaitForConsistency(t, 10*time.Second)

	// Create partition: {0,1,2} | {3,4}
	replicas.Network.CreatePartition([]int{0, 1, 2}, []int{3, 4})

	t.Log("Network partition created: {0,1,2} | {3,4}")

	// Write to partition 1 (replicas 0,1,2)
	for i := 0; i < 10; i++ {
		key := ds.NewKey(fmt.Sprintf("partition1/key%d", i))
		value := []byte(fmt.Sprintf("partition1_value%d", i))
		replicaIdx := i % 3 // Only use replicas 0, 1, 2
		err := replicas.Replicas[replicaIdx].Put(ctx, key, value)
		require.NoError(t, err)
	}

	// Write to partition 2 (replicas 3,4)
	for i := 0; i < 10; i++ {
		key := ds.NewKey(fmt.Sprintf("partition2/key%d", i))
		value := []byte(fmt.Sprintf("partition2_value%d", i))
		replicaIdx := 3 + (i % 2) // Only use replicas 3, 4
		err := replicas.Replicas[replicaIdx].Put(ctx, key, value)
		require.NoError(t, err)
	}

	// Wait for intra-partition consistency
	time.Sleep(5 * time.Second)

	// Verify partition 1 consistency (replicas 0,1,2)
	partition1Data := getReplicaData(t, replicas.Replicas[0])
	for i := 1; i < 3; i++ {
		replicaData := getReplicaData(t, replicas.Replicas[i])
		require.Equal(t, len(partition1Data), len(replicaData),
			"replicas in partition 1 should have same number of keys")
		for key, value := range partition1Data {
			require.Equal(t, value, replicaData[key],
				"replicas in partition 1 should have same values for key %s", key)
		}
	}

	// Verify partition 2 consistency (replicas 3,4)
	partition2Data := getReplicaData(t, replicas.Replicas[3])
	replica4Data := getReplicaData(t, replicas.Replicas[4])
	require.Equal(t, len(partition2Data), len(replica4Data),
		"replicas in partition 2 should have same number of keys")
	for key, value := range partition2Data {
		require.Equal(t, value, replica4Data[key],
			"replicas in partition 2 should have same values for key %s", key)
	}

	// Verify partitions are actually separate by checking they have different content
	// Both should have initial data (10 keys) plus their partition-specific data (10 keys) = 20 keys each
	// But the partition-specific keys should be different
	require.Equal(t, 20, len(partition1Data), "partition 1 should have 20 keys (10 initial + 10 partition1)")
	require.Equal(t, 20, len(partition2Data), "partition 2 should have 20 keys (10 initial + 10 partition2)")

	// Check that partition1 has partition1-specific keys but not partition2 keys
	hasPartition1Keys := false
	hasPartition2Keys := false
	for key := range partition1Data {
		if contains(key, "partition1/") {
			hasPartition1Keys = true
		}
		if contains(key, "partition2/") {
			hasPartition2Keys = true
		}
	}
	require.True(t, hasPartition1Keys, "partition 1 should have partition1-specific keys")
	require.False(t, hasPartition2Keys, "partition 1 should NOT have partition2-specific keys")

	t.Log("Both partitions are internally consistent but different")

	// Heal the partition
	replicas.Network.HealPartition()
	t.Log("Network partition healed")

	// Wait for cross-partition synchronization
	replicas.WaitForConsistency(t, 30*time.Second)

	// Verify all replicas converged
	require.True(t, replicas.AreConsistent(t),
		"all replicas should converge after partition healing")

	// Verify we have data from both partitions
	finalData := getReplicaData(t, replicas.Replicas[0])
	hasPartition1Data := false
	hasPartition2Data := false

	for key := range finalData {
		if contains(key, "partition1") {
			hasPartition1Data = true
		}
		if contains(key, "partition2") {
			hasPartition2Data = true
		}
	}

	require.True(t, hasPartition1Data, "final state should include partition 1 data")
	require.True(t, hasPartition2Data, "final state should include partition 2 data")
}

func TestAsymmetricPartition(t *testing.T) {
	replicas := NewIntegrationTestReplicas(t, 4, nil)
	defer replicas.Cleanup()

	ctx := context.Background()

	// Create asymmetric partition: 0 can talk to 1, but 1 cannot talk to 0
	// This simulates real-world scenarios where connectivity is not bidirectional
	replicas.Network.CreateAsymmetricPartition([]int{1}, []int{0}) // 1 cannot send to 0, but 0 can send to 1

	// Write from replica 0 (can send to 1, but won't get responses)
	for i := 0; i < 5; i++ {
		key := ds.NewKey(fmt.Sprintf("from0/key%d", i))
		value := []byte(fmt.Sprintf("from0_value%d", i))
		err := replicas.Replicas[0].Put(ctx, key, value)
		require.NoError(t, err)
	}

	// Write from replica 1 (cannot send to 0)
	for i := 0; i < 5; i++ {
		key := ds.NewKey(fmt.Sprintf("from1/key%d", i))
		value := []byte(fmt.Sprintf("from1_value%d", i))
		err := replicas.Replicas[1].Put(ctx, key, value)
		require.NoError(t, err)
	}

	time.Sleep(5 * time.Second)

	// Replica 1 should have received data from replica 0
	replica1Data := getReplicaData(t, replicas.Replicas[1])
	hasDataFrom0 := false
	for key := range replica1Data {
		if contains(key, "from0") {
			hasDataFrom0 = true
			break
		}
	}
	require.True(t, hasDataFrom0, "replica 1 should receive data from replica 0")

	// Replica 0 should NOT have received data from replica 1
	replica0Data := getReplicaData(t, replicas.Replicas[0])
	hasDataFrom1 := false
	for key := range replica0Data {
		if contains(key, "from1") {
			hasDataFrom1 = true
			break
		}
	}
	require.False(t, hasDataFrom1, "replica 0 should not receive data from replica 1 due to asymmetric partition")

	// Heal partition
	replicas.Network.HealPartition()
	replicas.WaitForConsistency(t, 15*time.Second)

	// Now both should be consistent
	require.True(t, replicas.AreConsistent(t), "replicas should converge after healing asymmetric partition")
}

func TestCascadingPartition(t *testing.T) {
	replicas := NewIntegrationTestReplicas(t, 6, nil)
	defer replicas.Cleanup()

	ctx := context.Background()

	// Initial sync
	key := ds.NewKey("initial/key")
	value := []byte("initial_value")
	err := replicas.Replicas[0].Put(ctx, key, value)
	require.NoError(t, err)
	replicas.WaitForConsistency(t, 10*time.Second)

	// Create cascading partition: 0|1|2 with 3,4,5 forming their own network
	replicas.Network.CreatePartition([]int{0}, []int{1, 2, 3, 4, 5})
	replicas.Network.CreatePartition([]int{1}, []int{2, 3, 4, 5})
	replicas.Network.CreatePartition([]int{2}, []int{3, 4, 5})

	t.Log("Created cascading partition: 0|1|2|{3,4,5}")

	// Write unique data to each isolated replica
	isolatedWrites := []string{"isolated0", "isolated1", "isolated2"}
	for i := 0; i < 3; i++ {
		key := ds.NewKey(fmt.Sprintf("isolated/key_%d", i))
		value := []byte(isolatedWrites[i])
		err := replicas.Replicas[i].Put(ctx, key, value)
		require.NoError(t, err)
	}

	// Write to the connected group (3,4,5)
	for i := 0; i < 5; i++ {
		key := ds.NewKey(fmt.Sprintf("group/key%d", i))
		value := []byte(fmt.Sprintf("group_value%d", i))
		replicaIdx := 3 + (i % 3)
		err := replicas.Replicas[replicaIdx].Put(ctx, key, value)
		require.NoError(t, err)
	}

	time.Sleep(5 * time.Second)

	// Verify group 3,4,5 is internally consistent
	group345Data := getReplicaData(t, replicas.Replicas[3])
	for i := 4; i < 6; i++ {
		replicaData := getReplicaData(t, replicas.Replicas[i])
		require.Equal(t, len(group345Data), len(replicaData),
			"replicas in group should have same data count")
	}

	// Gradually heal the partition
	t.Log("Healing partition gradually...")

	// First connect 2 to the group
	replicas.Network.HealPartition()
	replicas.Network.CreatePartition([]int{0}, []int{1, 2, 3, 4, 5})
	replicas.Network.CreatePartition([]int{1}, []int{2, 3, 4, 5})
	time.Sleep(3 * time.Second)

	// Then connect 1
	replicas.Network.HealPartition()
	replicas.Network.CreatePartition([]int{0}, []int{1, 2, 3, 4, 5})
	time.Sleep(3 * time.Second)

	// Finally heal completely
	replicas.Network.HealPartition()
	replicas.WaitForConsistency(t, 20*time.Second)

	// Verify final consistency
	require.True(t, replicas.AreConsistent(t), "all replicas should converge after gradual healing")

	// Verify we have all data
	finalData := getReplicaData(t, replicas.Replicas[0])
	for i := 0; i < 3; i++ {
		expectedKey := fmt.Sprintf("/isolated/key_%d", i)
		require.Contains(t, finalData, expectedKey, "should have isolated data from replica %d", i)
	}
}

func TestPartitionWithConflictingWrites(t *testing.T) {
	replicas := NewIntegrationTestReplicas(t, 4, nil)
	defer replicas.Cleanup()

	ctx := context.Background()
	conflictKey := ds.NewKey("conflict/key")

	// Initial sync
	err := replicas.Replicas[0].Put(ctx, conflictKey, []byte("initial"))
	require.NoError(t, err)
	replicas.WaitForConsistency(t, 5*time.Second)

	// Create partition: {0,1} | {2,3}
	replicas.Network.CreatePartition([]int{0, 1}, []int{2, 3})

	// Write conflicting values to the same key in both partitions
	err = replicas.Replicas[0].Put(ctx, conflictKey, []byte("partition_A_value"))
	require.NoError(t, err)

	err = replicas.Replicas[2].Put(ctx, conflictKey, []byte("partition_B_value"))
	require.NoError(t, err)

	// Add more conflicting writes to increase complexity
	err = replicas.Replicas[1].Put(ctx, conflictKey, []byte("partition_A_value_2"))
	require.NoError(t, err)

	err = replicas.Replicas[3].Put(ctx, conflictKey, []byte("partition_B_value_2"))
	require.NoError(t, err)

	time.Sleep(3 * time.Second)

	// Verify internal partition consistency
	partition1Value, err := replicas.Replicas[0].Get(ctx, conflictKey)
	require.NoError(t, err)
	partition1Value2, err := replicas.Replicas[1].Get(ctx, conflictKey)
	require.NoError(t, err)
	require.Equal(t, partition1Value, partition1Value2, "partition 1 should be internally consistent")

	partition2Value, err := replicas.Replicas[2].Get(ctx, conflictKey)
	require.NoError(t, err)
	partition2Value2, err := replicas.Replicas[3].Get(ctx, conflictKey)
	require.NoError(t, err)
	require.Equal(t, partition2Value, partition2Value2, "partition 2 should be internally consistent")

	// Verify partitions have different values
	require.NotEqual(t, partition1Value, partition2Value, "partitions should have different values")

	t.Logf("Partition 1 value: %s", string(partition1Value))
	t.Logf("Partition 2 value: %s", string(partition2Value))

	// Heal partition
	replicas.Network.HealPartition()
	replicas.WaitForConsistency(t, 15*time.Second)

	// Verify deterministic conflict resolution
	finalValue, err := replicas.Replicas[0].Get(ctx, conflictKey)
	require.NoError(t, err)

	// All replicas should have the same final value
	for i := 1; i < 4; i++ {
		replicaValue, err := replicas.Replicas[i].Get(ctx, conflictKey)
		require.NoError(t, err)
		require.Equal(t, finalValue, replicaValue, "all replicas should have same final value after healing")
	}

	t.Logf("Final converged value: %s", string(finalValue))
}

func TestPartitionDuringBatchOperations(t *testing.T) {
	replicas := NewIntegrationTestReplicas(t, 3, nil)
	defer replicas.Cleanup()

	ctx := context.Background()

	// Start batch operation on replica 0
	batch0, err := replicas.Replicas[0].Batch(ctx)
	require.NoError(t, err)

	// Add some operations to batch
	for i := 0; i < 5; i++ {
		key := ds.NewKey(fmt.Sprintf("batch/key%d", i))
		value := []byte(fmt.Sprintf("batch_value%d", i))
		err = batch0.Put(ctx, key, value)
		require.NoError(t, err)
	}

	// Create partition before committing batch
	replicas.Network.CreatePartition([]int{0}, []int{1, 2})

	// Commit batch while partitioned
	err = batch0.Commit(ctx)
	require.NoError(t, err)

	// Write to other partition
	for i := 5; i < 10; i++ {
		key := ds.NewKey(fmt.Sprintf("other/key%d", i))
		value := []byte(fmt.Sprintf("other_value%d", i))
		err = replicas.Replicas[1].Put(ctx, key, value)
		require.NoError(t, err)
	}

	time.Sleep(3 * time.Second)

	// Heal and verify convergence
	replicas.Network.HealPartition()
	replicas.WaitForConsistency(t, 15*time.Second)

	require.True(t, replicas.AreConsistent(t), "replicas should converge after batch during partition")

	// Verify both batch and individual writes are present
	finalData := getReplicaData(t, replicas.Replicas[0])
	batchDataPresent := false
	otherDataPresent := false

	for key := range finalData {
		if contains(key, "batch") {
			batchDataPresent = true
		}
		if contains(key, "other") {
			otherDataPresent = true
		}
	}

	require.True(t, batchDataPresent, "batch data should be present after healing")
	require.True(t, otherDataPresent, "other partition data should be present after healing")
}
