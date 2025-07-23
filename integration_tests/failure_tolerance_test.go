// Package integration_tests contains failure tolerance tests for trusted internal networks.
//
// TRUST MODEL ASSUMPTIONS:
// - All replicas are controlled by the organization
// - Network access is controlled at infrastructure level
// - No cryptographic authentication between replicas (handled by network security)
// - Failures are due to infrastructure issues, not malicious actors
//
// These tests focus on realistic failure scenarios:
// - Network partitions (infrastructure failures)
// - Node crashes and recovery
// - Resource exhaustion and performance degradation
// - Hardware/software failures causing data inconsistencies
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

func TestNodeCompleteIsolation(t *testing.T) {
	// Test behavior when a replica becomes completely isolated due to infrastructure failure
	// This simulates scenarios like network switch failures, firewall misconfigurations, etc.
	replicas := NewIntegrationTestReplicas(t, 5, nil)
	defer replicas.Cleanup()

	ctx := context.Background()

	// Initial sync with all replicas
	for i := 0; i < 10; i++ {
		key := ds.NewKey(fmt.Sprintf("initial/key%d", i))
		value := []byte(fmt.Sprintf("initial_value%d", i))
		err := replicas.Replicas[0].Put(ctx, key, value)
		require.NoError(t, err)
	}

	replicas.WaitForConsistency(t, 10*time.Second)

	// Simulate complete network isolation of replica 4 (infrastructure failure)
	replicas.Network.CreatePartition([]int{4}, []int{0, 1, 2, 3})
	t.Log("Simulating complete network isolation of replica 4")

	// Healthy replicas continue operating normally
	for i := 0; i < 10; i++ {
		key := ds.NewKey(fmt.Sprintf("during_isolation/key%d", i))
		value := []byte(fmt.Sprintf("during_isolation_value%d", i))
		replicaID := i % 4 // Only use replicas 0-3
		err := replicas.Replicas[replicaID].Put(ctx, key, value)
		require.NoError(t, err)
	}

	// Isolated replica continues operating (unaware of isolation)
	for i := 0; i < 5; i++ {
		key := ds.NewKey(fmt.Sprintf("isolated_ops/key%d", i))
		value := []byte(fmt.Sprintf("isolated_value%d", i))
		err := replicas.Replicas[4].Put(ctx, key, value)
		require.NoError(t, err)
	}

	time.Sleep(5 * time.Second)

	// Verify healthy replicas maintain consistency among themselves
	for i := 0; i < 3; i++ {
		for j := i + 1; j < 4; j++ {
			replica1Data := getReplicaData(t, replicas.Replicas[i])
			replica2Data := getReplicaData(t, replicas.Replicas[j])
			require.Equal(t, len(replica1Data), len(replica2Data),
				"healthy replicas %d and %d should have same data count", i, j)
		}
	}

	// Verify isolated replica has diverged
	isolatedData := getReplicaData(t, replicas.Replicas[4])
	healthyData := getReplicaData(t, replicas.Replicas[0])
	require.NotEqual(t, len(isolatedData), len(healthyData),
		"isolated replica should have different data than healthy replicas")

	// Heal network partition (infrastructure repaired)
	t.Log("Healing network partition - infrastructure repaired")
	replicas.Network.HealPartition()
	replicas.WaitForConsistency(t, 30*time.Second)

	require.True(t, replicas.AreConsistent(t),
		"all replicas should converge after network infrastructure is repaired")
}

func TestHighFrequencyUpdatesUnderLoad(t *testing.T) {
	// Test system behavior when a replica generates high-frequency updates due to:
	// - Software bugs causing update loops
	// - Performance issues causing retry storms
	// - Application logic errors generating excessive writes
	replicas := NewIntegrationTestReplicas(t, 4, nil)
	defer replicas.Cleanup()

	ctx := context.Background()
	conflictKey := ds.NewKey("high_frequency/target_key")

	// Establish initial state
	err := replicas.Replicas[0].Put(ctx, conflictKey, []byte("initial_value"))
	require.NoError(t, err)
	replicas.WaitForConsistency(t, 5*time.Second)

	// Simulate high-frequency updates from replica 3 (e.g., application bug)
	t.Log("Simulating high-frequency updates from replica 3 due to application issue")
	updateValues := []string{
		"update_1", "update_2", "update_3", "update_4", "update_5",
	}

	// Replica 3 generates rapid updates (simulating application bug/retry storm)
	for _, value := range updateValues {
		err := replicas.Replicas[3].Put(ctx, conflictKey, []byte(value))
		require.NoError(t, err)
		time.Sleep(10 * time.Millisecond) // Very rapid updates
	}

	// Other replicas continue normal operations
	normalKey := ds.NewKey("normal/operation")
	for i := 0; i < 3; i++ {
		err := replicas.Replicas[i].Put(ctx, normalKey, []byte(fmt.Sprintf("normal_value_%d", i)))
		require.NoError(t, err)
	}

	// Wait for convergence
	replicas.WaitForConsistency(t, 20*time.Second)

	// Verify system reaches consistent state despite high-frequency updates
	require.True(t, replicas.AreConsistent(t),
		"system should reach consensus despite high-frequency updates from one replica")

	// Verify normal operations succeeded
	for i, replica := range replicas.Replicas {
		value, err := replica.Get(ctx, normalKey)
		require.NoError(t, err, "replica %d should have normal key", i)
		require.NotEmpty(t, value, "replica %d should have non-empty value for normal key", i)
	}
}

func TestInfrastructureFailureRecovery(t *testing.T) {
	// Test system recovery when multiple nodes experience infrastructure failures
	// Simulates scenarios like datacenter outages, network equipment failures, etc.
	opts := crdt.DefaultOptions()
	opts.RebroadcastInterval = 1 * time.Second // Faster rebroadcast for this test

	replicas := NewIntegrationTestReplicas(t, 6, opts)
	defer replicas.Cleanup()

	ctx := context.Background()

	// Normal operation initially
	for i := 0; i < 20; i++ {
		key := ds.NewKey(fmt.Sprintf("normal/key_%d", i))
		value := []byte(fmt.Sprintf("normal_value_%d", i))
		replicaID := i % 6
		err := replicas.Replicas[replicaID].Put(ctx, key, value)
		require.NoError(t, err)
	}

	replicas.WaitForConsistency(t, 15*time.Second)
	initialDataCount := len(getReplicaData(t, replicas.Replicas[0]))

	// Simulate infrastructure failure affecting replicas 4 and 5 (e.g., datacenter outage)
	t.Log("Simulating infrastructure failure affecting replicas 4 and 5")
	replicas.Network.CreatePartition([]int{4, 5}, []int{0, 1, 2, 3})

	// Failed infrastructure nodes continue operating in isolation (split-brain scenario)
	for i := 0; i < 15; i++ {
		key := ds.NewKey(fmt.Sprintf("isolated_datacenter/key_%d", i))
		value := []byte(fmt.Sprintf("isolated_value_%d", i))
		replicaID := 4 + (i % 2)
		err := replicas.Replicas[replicaID].Put(ctx, key, value)
		require.NoError(t, err)
	}

	// Healthy infrastructure nodes continue normal operation
	for i := 0; i < 10; i++ {
		key := ds.NewKey(fmt.Sprintf("healthy_datacenter/key_%d", i))
		value := []byte(fmt.Sprintf("healthy_value_%d", i))
		replicaID := i % 4 // Only replicas 0-3
		err := replicas.Replicas[replicaID].Put(ctx, key, value)
		require.NoError(t, err)
	}

	time.Sleep(5 * time.Second)

	// Verify healthy infrastructure maintains consistency
	healthyData := getReplicaData(t, replicas.Replicas[0])
	for i := 1; i < 4; i++ {
		replicaData := getReplicaData(t, replicas.Replicas[i])
		require.Equal(t, len(healthyData), len(replicaData),
			"healthy infrastructure replicas should maintain consistency")
	}

	// Verify isolated infrastructure has diverged
	isolatedData := getReplicaData(t, replicas.Replicas[4])
	require.NotEqual(t, len(healthyData), len(isolatedData),
		"isolated infrastructure should have diverged from healthy infrastructure")

	// Recovery: infrastructure repaired, connectivity restored
	t.Log("Infrastructure repaired - restoring connectivity")
	replicas.Network.HealPartition()
	replicas.WaitForConsistency(t, 60*time.Second)

	// System should recover and reach global consistency
	require.True(t, replicas.AreConsistent(t),
		"system should recover to consistent state after infrastructure repair")

	finalDataCount := len(getReplicaData(t, replicas.Replicas[0]))
	require.Greater(t, finalDataCount, initialDataCount,
		"system should preserve data after infrastructure recovery")
}

// TestSybilAttackResistance removed - not applicable to trusted internal networks
// In a trusted network environment, all replicas are controlled by the organization
// and cryptographic identity verification is handled at the infrastructure level.

func TestDataCorruptionRecovery(t *testing.T) {
	// Test system behavior when hardware/software failures cause data inconsistencies
	// Simulates scenarios like disk corruption, memory errors, software bugs
	replicas := NewIntegrationTestReplicas(t, 5, nil)
	defer replicas.Cleanup()

	ctx := context.Background()

	// Establish initial consistent state
	initialKeys := make([]string, 20)
	for i := 0; i < 20; i++ {
		key := ds.NewKey(fmt.Sprintf("data/key_%d", i))
		value := []byte(fmt.Sprintf("original_value_%d", i))
		initialKeys[i] = key.String()

		replicaID := i % 5
		err := replicas.Replicas[replicaID].Put(ctx, key, value)
		require.NoError(t, err)
	}

	replicas.WaitForConsistency(t, 15*time.Second)
	initialData := getReplicaData(t, replicas.Replicas[0])

	// Simulate data corruption on replica 4 (e.g., disk corruption, memory error)
	t.Log("Simulating data corruption events on replica 4")
	for i := 0; i < 10; i++ { // Corrupt half the keys
		key := ds.NewKey(fmt.Sprintf("data/key_%d", i*2))
		corruptValue := []byte(fmt.Sprintf("CORRUPTED_BY_HW_ERROR_%d", i))
		err := replicas.Replicas[4].Put(ctx, key, corruptValue)
		require.NoError(t, err)
	}

	// Healthy replicas continue normal operations
	for i := 0; i < 4; i++ {
		key := ds.NewKey(fmt.Sprintf("continued/key_%d", i))
		value := []byte(fmt.Sprintf("continued_value_%d", i))
		err := replicas.Replicas[i].Put(ctx, key, value)
		require.NoError(t, err)
	}

	// Wait for convergence
	replicas.WaitForConsistency(t, 20*time.Second)

	// System should reach consistent state despite corruption
	require.True(t, replicas.AreConsistent(t),
		"system should reach consistent state despite hardware-induced corruption")

	finalData := getReplicaData(t, replicas.Replicas[0])

	// Verify we have all expected keys (original + continued operations)
	require.Equal(t, len(initialData)+4, len(finalData),
		"should have original data plus continued operations")

	// For each potentially corrupted key, verify the system picked a deterministic value
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("/data/key_%d", i*2)
		finalValue := finalData[key]
		require.NotEmpty(t, finalValue, "corrupted key should have some final value")

		// Verify all replicas agree on this value (CRDT convergence property)
		for replicaID := 1; replicaID < 5; replicaID++ {
			replicaData := getReplicaData(t, replicas.Replicas[replicaID])
			require.Equal(t, finalValue, replicaData[key],
				"replica %d should agree on final value for potentially corrupted key %s", replicaID, key)
		}
	}
}

func TestNodeFailureAndRecovery(t *testing.T) {
	// Test system behavior when a node experiences failures and recovers
	// Simulates scenarios like server crashes, process restarts, temporary resource exhaustion
	replicas := NewIntegrationTestReplicas(t, 4, nil)
	defer replicas.Cleanup()

	ctx := context.Background()

	// Phase 1: Normal operation
	for i := 0; i < 10; i++ {
		key := ds.NewKey(fmt.Sprintf("phase1/key_%d", i))
		value := []byte(fmt.Sprintf("phase1_value_%d", i))
		replicaID := i % 4
		err := replicas.Replicas[replicaID].Put(ctx, key, value)
		require.NoError(t, err)
	}

	replicas.WaitForConsistency(t, 10*time.Second)

	// Phase 2: Replica 3 fails (server crash, network disconnection)
	t.Log("Simulating node failure - replica 3 disconnected")
	replicas.Network.CreatePartition([]int{3}, []int{0, 1, 2})

	// Failed node continues operations while isolated (unaware of failure)
	for i := 0; i < 8; i++ {
		key := ds.NewKey(fmt.Sprintf("isolated_ops/key_%d", i))
		value := []byte(fmt.Sprintf("isolated_value_%d", i))
		err := replicas.Replicas[3].Put(ctx, key, value)
		require.NoError(t, err)
	}

	// Healthy replicas continue normal operations
	for i := 0; i < 5; i++ {
		key := ds.NewKey(fmt.Sprintf("phase2/key_%d", i))
		value := []byte(fmt.Sprintf("phase2_value_%d", i))
		replicaID := i % 3 // Only healthy replicas
		err := replicas.Replicas[replicaID].Put(ctx, key, value)
		require.NoError(t, err)
	}

	time.Sleep(5 * time.Second)

	// Phase 3: Node recovery (server restarted, connectivity restored)
	t.Log("Node recovered - restoring connectivity")
	replicas.Network.HealPartition()

	// Give time for recovery sync
	time.Sleep(2 * time.Second)

	// Phase 4: Recovered node participates in normal operations
	for i := 0; i < 5; i++ {
		key := ds.NewKey(fmt.Sprintf("post_recovery/key_%d", i))
		value := []byte(fmt.Sprintf("post_recovery_value_%d", i))
		err := replicas.Replicas[3].Put(ctx, key, value) // Previously failed node
		require.NoError(t, err)
	}

	// All nodes participate in final operations
	for i := 0; i < 8; i++ {
		key := ds.NewKey(fmt.Sprintf("final_phase/key_%d", i))
		value := []byte(fmt.Sprintf("final_value_%d", i))
		replicaID := i % 4
		err := replicas.Replicas[replicaID].Put(ctx, key, value)
		require.NoError(t, err)
	}

	// Wait for full convergence
	replicas.WaitForConsistency(t, 30*time.Second)

	// Verify system reaches global consistency after recovery
	require.True(t, replicas.AreConsistent(t),
		"system should reach consistency after node failure and recovery")

	// Verify all phases of data are represented
	finalData := getReplicaData(t, replicas.Replicas[0])

	phase1Count := 0
	phase2Count := 0
	isolatedCount := 0
	postRecoveryCount := 0
	finalPhaseCount := 0

	for key := range finalData {
		if contains(key, "phase1/") {
			phase1Count++
		} else if contains(key, "phase2/") {
			phase2Count++
		} else if contains(key, "isolated_ops/") {
			isolatedCount++
		} else if contains(key, "post_recovery/") {
			postRecoveryCount++
		} else if contains(key, "final_phase/") {
			finalPhaseCount++
		}
	}

	require.Equal(t, 10, phase1Count, "should have all phase1 data")
	require.Equal(t, 5, phase2Count, "should have all phase2 data")
	require.Equal(t, 8, isolatedCount, "should have isolated operations data after recovery")
	require.Equal(t, 5, postRecoveryCount, "should have post-recovery data")
	require.Equal(t, 8, finalPhaseCount, "should have all final phase data")

	t.Logf("Final state: Phase1=%d, Phase2=%d, Isolated=%d, PostRecovery=%d, Final=%d",
		phase1Count, phase2Count, isolatedCount, postRecoveryCount, finalPhaseCount)
}
