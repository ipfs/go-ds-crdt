package crdt

import (
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-ds-crdt/pb"
	"github.com/ipfs/go-log/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// CorePeerSelector selects peers with role="core"
func CorePeerSelector(metadata map[string]string) bool {
	return metadata["role"] == "core"
}

// DurablePeerSelector selects peers marked as durable storage
func DurablePeerSelector(metadata map[string]string) bool {
	return metadata["durable"] == "true"
}

// ProductionPeerSelector selects non-spot/non-ephemeral peers
func ProductionPeerSelector(metadata map[string]string) bool {
	return metadata["spot"] != "true" && metadata["ephemeral"] != "true"
}

func TestPeerSelectors(t *testing.T) {
	testCases := []struct {
		name     string
		metadata map[string]string
		selector PeerSelector
		expected bool
	}{
		{"core peer", map[string]string{"role": "core"}, CorePeerSelector, true},
		{"non-core peer", map[string]string{"role": "read"}, CorePeerSelector, false},
		{"durable peer", map[string]string{"durable": "true"}, DurablePeerSelector, true},
		{"non-durable peer", map[string]string{"durable": "false"}, DurablePeerSelector, false},
		{"production peer", map[string]string{"spot": "false"}, ProductionPeerSelector, true},
		{"spot instance", map[string]string{"spot": "true"}, ProductionPeerSelector, false},
		{"ephemeral peer", map[string]string{"ephemeral": "true"}, ProductionPeerSelector, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.selector(tc.metadata)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestAndOrSelectors(t *testing.T) {

	t.Run("AndSelector", func(t *testing.T) {
		selector := AndSelector(CorePeerSelector, DurablePeerSelector)

		// Both conditions met
		assert.True(t, selector(map[string]string{"role": "core", "durable": "true"}))

		// Only one condition met
		assert.False(t, selector(map[string]string{"role": "core", "durable": "false"}))
		assert.False(t, selector(map[string]string{"role": "read", "durable": "true"}))

		// No conditions met
		assert.False(t, selector(map[string]string{"role": "read", "durable": "false"}))
	})

	t.Run("OrSelector", func(t *testing.T) {
		selector := OrSelector(CorePeerSelector, DurablePeerSelector)

		// Both conditions met
		assert.True(t, selector(map[string]string{"role": "core", "durable": "true"}))

		// Either condition met
		assert.True(t, selector(map[string]string{"role": "core", "durable": "false"}))
		assert.True(t, selector(map[string]string{"role": "read", "durable": "true"}))

		// No conditions met
		assert.False(t, selector(map[string]string{"role": "read", "durable": "false"}))
	})
}

func TestHeadInclusionChecker(t *testing.T) {
	// Mock DAGWalker for testing
	mockWalker := &mockDAGWalker{
		reachability: make(map[string]bool),
	}

	checker := NewHeadInclusionChecker(mockWalker, 3, 5)

	head1 := createTestCID(t, "head1")
	head2 := createTestCID(t, "head2")
	head3 := createTestCID(t, "head3")
	peer1 := createTestCID(t, "peer1")

	t.Run("empty myHeads returns true", func(t *testing.T) {
		result, err := checker.HeadsIncluded(context.Background(), []cid.Cid{}, []cid.Cid{peer1})
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("empty peerHeads returns false", func(t *testing.T) {
		result, err := checker.HeadsIncluded(context.Background(), []cid.Cid{head1}, []cid.Cid{})
		require.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("all heads included", func(t *testing.T) {
		// Set up reachability: peer1 can reach head1 and head2
		mockWalker.reachability[peer1.KeyString()+":"+head1.KeyString()] = true
		mockWalker.reachability[peer1.KeyString()+":"+head2.KeyString()] = true

		result, err := checker.HeadsIncluded(context.Background(), []cid.Cid{head1, head2}, []cid.Cid{peer1})
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("some heads not included", func(t *testing.T) {
		// peer1 can reach head1 but not head3
		mockWalker.reachability[peer1.KeyString()+":"+head1.KeyString()] = true
		mockWalker.reachability[peer1.KeyString()+":"+head3.KeyString()] = false

		result, err := checker.HeadsIncluded(context.Background(), []cid.Cid{head1, head3}, []cid.Cid{peer1})
		require.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("caching works", func(t *testing.T) {
		// First call
		result1, err := checker.HeadsIncluded(context.Background(), []cid.Cid{head1}, []cid.Cid{peer1})
		require.NoError(t, err)

		// Second call should hit cache
		result2, err := checker.HeadsIncluded(context.Background(), []cid.Cid{head1}, []cid.Cid{peer1})
		require.NoError(t, err)

		assert.Equal(t, result1, result2)
		// Verify cache was used (mockWalker could track call count)
	})

	t.Run("reset clears cache", func(t *testing.T) {
		checker.Reset()
		// After reset, cache should be empty
		assert.Len(t, checker.reachabilityCache, 0)
	})
}

// Mock DAGWalker for testing
type mockDAGWalker struct {
	reachability map[string]bool
}

func (m *mockDAGWalker) WalkReverseTopo(ctx context.Context, heads []cid.Cid, stopAt cid.Cid) ([]cid.Cid, error) {
	if len(heads) == 0 {
		return []cid.Cid{}, nil
	}

	from := heads[0]
	key := from.KeyString() + ":" + stopAt.KeyString()

	if reachable, exists := m.reachability[key]; exists && reachable {
		// Return a path that includes the stopAt CID
		return []cid.Cid{from, stopAt}, nil
	}

	// Return a path that doesn't include stopAt
	return []cid.Cid{from}, nil
}

func TestMarkLeavingPropagation(t *testing.T) {
	ctx := context.Background()
	store := sync.MutexWrap(ds.NewMapDatastore())
	key := ds.NewKey("/test/state")
	ttl := time.Hour
	mockClock := clock.NewMock()

	baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	mockClock.Set(baseTime)

	manager, err := NewStateManager(ctx, store, key, ttl, log.Logger("crdt"))
	require.NoError(t, err)
	manager.clock = mockClock

	peerID := createTestPeerID(t, "peer1")

	t.Run("mark leaving survives older broadcast", func(t *testing.T) {
		// First, establish a normal membership
		err := manager.UpdateHeads(ctx, peerID, []cid.Cid{createTestCID(t, "test")}, true)
		require.NoError(t, err)

		currentState := manager.GetState()
		normalMember := currentState.Members[peerID.String()]
		normalTTL := normalMember.BestBefore
		normalSeq := normalMember.StateSeq

		// Mark as leaving
		grace := 5 * time.Second
		err = manager.MarkLeaving(ctx, peerID, grace)
		require.NoError(t, err)

		leavingState := manager.GetState()
		leavingMember := leavingState.Members[peerID.String()]
		shortTTL := leavingMember.BestBefore
		leavingSeq := leavingMember.StateSeq

		assert.True(t, shortTTL < normalTTL, "TTL should be shorter after MarkLeaving")
		assert.True(t, leavingSeq > normalSeq, "Sequence should be incremented")

		// Simulate receiving an older broadcast with longer TTL but lower sequence
		olderBroadcast := &pb.StateBroadcast{
			Members: map[string]*pb.Participant{
				peerID.String(): {
					BestBefore: normalTTL,      // Old longer TTL
					StateSeq:   leavingSeq - 1, // Lower sequence
					Metadata:   map[string]string{"role": "core"},
				},
			},
		}

		err = manager.MergeMembers(ctx, olderBroadcast)
		require.NoError(t, err)

		// Verify that our short TTL and higher sequence are preserved
		finalState := manager.GetState()
		finalMember := finalState.Members[peerID.String()]
		assert.Equal(t, shortTTL, finalMember.BestBefore, "Short TTL should be preserved")
		assert.Equal(t, leavingSeq, finalMember.StateSeq, "Higher sequence should be preserved")
	})
}
