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
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func createTestCID(tb testing.TB, data string) cid.Cid {
	hash, err := multihash.Sum([]byte(data), multihash.SHA2_256, -1)
	require.NoError(tb, err)
	return cid.NewCidV1(cid.Raw, hash)
}

func createTestPeerID(tb testing.TB, id string) peer.ID {
	peerID, err := peer.Decode("12D3KooW" + id + "ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	if err != nil {
		// Fallback to a simpler peer ID for testing
		return peer.ID(id)
	}
	return peerID
}

func TestNewStateManager(t *testing.T) {
	ctx := context.Background()
	store := sync.MutexWrap(ds.NewMapDatastore())
	key := ds.NewKey("/test/state")
	ttl := time.Hour

	t.Run("successful initialization", func(t *testing.T) {
		manager, err := NewStateManager(ctx, store, key, ttl, log.Logger("crdt"))
		require.NoError(t, err)
		assert.NotNil(t, manager)
		assert.Equal(t, store, manager.datastore)
		assert.Equal(t, key, manager.key)
		assert.Equal(t, ttl, manager.ttl)
		assert.NotNil(t, manager.state)
		assert.NotNil(t, manager.state.Members)
	})

	t.Run("loads existing state", func(t *testing.T) {
		// Pre-populate datastore with state
		existingState := &pb.StateBroadcast{
			Members: map[string]*pb.Participant{
				"peer1": {
					BestBefore: uint64(time.Now().Add(ttl).Unix()),
					Metadata:   map[string]string{"key": "value"},
				},
			},
		}
		data, err := proto.Marshal(existingState)
		require.NoError(t, err)
		err = store.Put(ctx, key, data)
		require.NoError(t, err)

		manager, err := NewStateManager(ctx, store, key, ttl, log.Logger("crdt"))
		require.NoError(t, err)

		state := manager.GetState()
		assert.Len(t, state.Members, 1)
		assert.Contains(t, state.Members, "peer1")
		assert.Equal(t, "value", state.Members["peer1"].Metadata["key"])
	})
}

func TestStateManager_Load(t *testing.T) {
	ctx := context.Background()
	store := sync.MutexWrap(ds.NewMapDatastore())
	key := ds.NewKey("/test/state")
	ttl := time.Hour

	t.Run("load from empty datastore", func(t *testing.T) {
		manager := &StateManager{
			datastore: store,
			key:       key,
			state:     &pb.StateBroadcast{}, // Initialize state before Load()
			clock:     clock.New(),
			ttl:       ttl,
		}

		err := manager.Load(ctx)
		require.NoError(t, err)
		assert.NotNil(t, manager.state)
		assert.NotNil(t, manager.state.Members)
		assert.Len(t, manager.state.Members, 0)
	})

	t.Run("load existing state", func(t *testing.T) {
		// Setup existing state
		existingState := &pb.StateBroadcast{
			Members: map[string]*pb.Participant{
				"peer1": {BestBefore: uint64(time.Now().Add(ttl).Unix())},
			},
		}
		data, err := proto.Marshal(existingState)
		require.NoError(t, err)
		err = store.Put(ctx, key, data)
		require.NoError(t, err)

		manager := &StateManager{
			datastore: store,
			key:       key,
			state:     &pb.StateBroadcast{}, // Initialize state before Load()
			clock:     clock.New(),
			ttl:       ttl,
		}

		err = manager.Load(ctx)
		require.NoError(t, err)
		assert.Len(t, manager.state.Members, 1)
		assert.Contains(t, manager.state.Members, "peer1")
	})

	t.Run("load corrupted data", func(t *testing.T) {
		// Put invalid protobuf data
		err := store.Put(ctx, key, []byte("invalid protobuf data"))
		require.NoError(t, err)

		manager := &StateManager{
			datastore: store,
			key:       key,
			state:     &pb.StateBroadcast{}, // Initialize state before Load()
			clock:     clock.New(),
			ttl:       ttl,
		}

		err = manager.Load(ctx)
		assert.Error(t, err)
	})
}

func TestStateManager_Save(t *testing.T) {
	ctx := context.Background()
	store := sync.MutexWrap(ds.NewMapDatastore())
	key := ds.NewKey("/test/state")
	ttl := time.Hour

	manager, err := NewStateManager(ctx, store, key, ttl, log.Logger("crdt"))
	require.NoError(t, err)

	// Modify state
	manager.state.Members["test_peer"] = &pb.Participant{
		BestBefore: uint64(time.Now().Add(ttl).Unix()),
	}

	err = manager.Save(ctx)
	require.NoError(t, err)

	// Verify data was saved
	data, err := store.Get(ctx, key)
	require.NoError(t, err)

	var savedState pb.StateBroadcast
	err = proto.Unmarshal(data, &savedState)
	require.NoError(t, err)
	assert.Contains(t, savedState.Members, "test_peer")
}

func TestStateManager_GetState(t *testing.T) {
	ctx := context.Background()
	store := sync.MutexWrap(ds.NewMapDatastore())
	key := ds.NewKey("/test/state")
	ttl := time.Hour

	manager, err := NewStateManager(ctx, store, key, ttl, log.Logger("crdt"))
	require.NoError(t, err)

	// Add member to internal state
	manager.state.Members["peer1"] = &pb.Participant{
		BestBefore: uint64(time.Now().Add(ttl).Unix()),
	}

	state := manager.GetState()
	assert.Contains(t, state.Members, "peer1")

	// Verify it's a copy, not the original
	state.Members["peer2"] = &pb.Participant{}
	assert.NotContains(t, manager.state.Members, "peer2")
}

func TestStateManager_UpdateHeads(t *testing.T) {
	ctx := context.Background()
	store := sync.MutexWrap(ds.NewMapDatastore())
	key := ds.NewKey("/test/state")
	ttl := time.Hour
	mockClock := clock.NewMock()

	// Set mock clock to a reasonable time to avoid underflow issues
	baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	mockClock.Set(baseTime)

	manager, err := NewStateManager(ctx, store, key, ttl, log.Logger("crdt"))
	require.NoError(t, err)
	manager.clock = mockClock

	peerID := createTestPeerID(t, "peer1")
	testCIDs := []cid.Cid{
		createTestCID(t, "test1"),
		createTestCID(t, "test2"),
	}

	t.Run("update heads for new member", func(t *testing.T) {
		err := manager.UpdateHeads(ctx, peerID, testCIDs, true)
		require.NoError(t, err)

		state := manager.GetState()
		member := state.Members[peerID.String()]
		require.NotNil(t, member)
		assert.Len(t, member.DagHeads, 2)
		assert.Equal(t, testCIDs[0].Bytes(), member.DagHeads[0].Cid)
		assert.Equal(t, testCIDs[1].Bytes(), member.DagHeads[1].Cid)
		assert.Equal(t, uint64(mockClock.Now().Add(ttl).Unix()), member.BestBefore)
	})

	t.Run("update heads without updating TTL", func(t *testing.T) {
		originalBestBefore := manager.state.Members[peerID.String()].BestBefore
		newCIDs := []cid.Cid{createTestCID(t, "test3")}

		err := manager.UpdateHeads(ctx, peerID, newCIDs, false)
		require.NoError(t, err)

		state := manager.GetState()
		member := state.Members[peerID.String()]
		assert.Len(t, member.DagHeads, 1)
		assert.Equal(t, newCIDs[0].Bytes(), member.DagHeads[0].Cid)
		assert.Equal(t, originalBestBefore, member.BestBefore)
	})

	t.Run("callback is triggered", func(t *testing.T) {
		callbackTriggered := false
		var callbackMembers map[string]*pb.Participant

		manager.SetMembershipUpdateCallback(func(members map[string]*pb.Participant) {
			callbackTriggered = true
			callbackMembers = members
		})

		err := manager.UpdateHeads(ctx, peerID, testCIDs, true)
		require.NoError(t, err)

		// Wait a bit for goroutine to execute
		time.Sleep(10 * time.Millisecond)

		assert.True(t, callbackTriggered)
		assert.Contains(t, callbackMembers, peerID.String())
	})
}

func TestStateManager_MergeMembers(t *testing.T) {
	ctx := context.Background()
	store := sync.MutexWrap(ds.NewMapDatastore())
	key := ds.NewKey("/test/state")
	ttl := time.Hour
	mockClock := clock.NewMock()

	// Set mock clock to a reasonable time to avoid underflow issues
	baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	mockClock.Set(baseTime)

	manager, err := NewStateManager(ctx, store, key, ttl, log.Logger("crdt"))
	require.NoError(t, err)
	manager.clock = mockClock

	t.Run("merge new members", func(t *testing.T) {
		broadcast := &pb.StateBroadcast{
			Members: map[string]*pb.Participant{
				"peer1": {
					BestBefore: uint64(mockClock.Now().Add(ttl).Unix()),
					Metadata:   map[string]string{"role": "leader"},
				},
				"peer2": {
					BestBefore: uint64(mockClock.Now().Add(ttl).Unix()),
					Metadata:   map[string]string{"role": "follower"},
				},
			},
		}

		err := manager.MergeMembers(ctx, broadcast)
		require.NoError(t, err)

		state := manager.GetState()
		assert.Len(t, state.Members, 2)
		assert.Contains(t, state.Members, "peer1")
		assert.Contains(t, state.Members, "peer2")
		assert.Equal(t, "leader", state.Members["peer1"].Metadata["role"])
	})

	t.Run("merge with newer timestamps wins", func(t *testing.T) {
		// Add existing member
		manager.state.Members["peer1"] = &pb.Participant{
			BestBefore: uint64(mockClock.Now().Add(-time.Hour).Unix()), // Older
			Metadata:   map[string]string{"role": "old_leader"},
		}

		broadcast := &pb.StateBroadcast{
			Members: map[string]*pb.Participant{
				"peer1": {
					BestBefore: uint64(mockClock.Now().Add(ttl).Unix()), // Newer
					Metadata:   map[string]string{"role": "new_leader"},
				},
			},
		}

		err := manager.MergeMembers(ctx, broadcast)
		require.NoError(t, err)

		state := manager.GetState()
		assert.Equal(t, "new_leader", state.Members["peer1"].Metadata["role"])
	})

	t.Run("older timestamps are ignored", func(t *testing.T) {
		// Add existing member with newer timestamp
		manager.state.Members["peer1"] = &pb.Participant{
			BestBefore: uint64(mockClock.Now().Add(ttl).Unix()), // Newer
			Metadata:   map[string]string{"role": "current_leader"},
		}

		broadcast := &pb.StateBroadcast{
			Members: map[string]*pb.Participant{
				"peer1": {
					BestBefore: uint64(mockClock.Now().Add(-time.Hour).Unix()), // Older
					Metadata:   map[string]string{"role": "old_leader"},
				},
			},
		}

		err := manager.MergeMembers(ctx, broadcast)
		require.NoError(t, err)

		state := manager.GetState()
		assert.Equal(t, "current_leader", state.Members["peer1"].Metadata["role"])
	})

	t.Run("expired members are removed", func(t *testing.T) {
		// Add expired member
		manager.state.Members["expired_peer"] = &pb.Participant{
			BestBefore: uint64(mockClock.Now().Add(-time.Hour).Unix()), // Expired
			Metadata:   map[string]string{"role": "expired"},
		}

		broadcast := &pb.StateBroadcast{
			Members: map[string]*pb.Participant{},
		}

		err := manager.MergeMembers(ctx, broadcast)
		require.NoError(t, err)

		state := manager.GetState()
		assert.NotContains(t, state.Members, "expired_peer")
	})

	t.Run("callback is triggered", func(t *testing.T) {
		callbackTriggered := false
		manager.SetMembershipUpdateCallback(func(members map[string]*pb.Participant) {
			callbackTriggered = true
		})

		broadcast := &pb.StateBroadcast{
			Members: map[string]*pb.Participant{
				"new_peer": {
					BestBefore: uint64(mockClock.Now().Add(ttl).Unix()),
				},
			},
		}

		err := manager.MergeMembers(ctx, broadcast)
		require.NoError(t, err)

		// Wait for goroutine
		time.Sleep(10 * time.Millisecond)
		assert.True(t, callbackTriggered)
	})
}

func TestStateManager_SetSnapshot(t *testing.T) {
	ctx := context.Background()
	store := sync.MutexWrap(ds.NewMapDatastore())
	key := ds.NewKey("/test/state")
	ttl := time.Hour
	mockClock := clock.NewMock()

	// Set mock clock to a reasonable time to avoid underflow issues
	baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	mockClock.Set(baseTime)

	manager, err := NewStateManager(ctx, store, key, ttl, log.Logger("crdt"))
	require.NoError(t, err)
	manager.clock = mockClock

	peerID := createTestPeerID(t, "peer1")
	snapshotInfo := &SnapshotInfo{
		WrapperCID:   createTestCID(t, "wrapper"),
		DeltaHeadCID: createTestCID(t, "delta"),
		Height:       42,
	}

	t.Run("set snapshot for new member", func(t *testing.T) {
		err := manager.SetSnapshot(ctx, peerID, snapshotInfo)
		require.NoError(t, err)

		state := manager.GetState()
		member := state.Members[peerID.String()]
		require.NotNil(t, member)
		require.NotNil(t, member.Snapshot)

		assert.Equal(t, snapshotInfo.WrapperCID.Bytes(), member.Snapshot.SnapshotKey.Cid)
		assert.Equal(t, snapshotInfo.DeltaHeadCID.Bytes(), member.Snapshot.DagHead.Cid)
		assert.Equal(t, snapshotInfo.Height, member.Snapshot.Height)
		assert.Equal(t, uint64(mockClock.Now().Add(ttl).Unix()), member.BestBefore)
	})

	t.Run("update snapshot for existing member", func(t *testing.T) {
		// Add existing member
		manager.state.Members[peerID.String()] = &pb.Participant{
			BestBefore: uint64(mockClock.Now().Add(-time.Hour).Unix()),
			Metadata:   map[string]string{"existing": "data"},
		}

		newSnapshotInfo := &SnapshotInfo{
			WrapperCID:   createTestCID(t, "new_wrapper"),
			DeltaHeadCID: createTestCID(t, "new_delta"),
			Height:       100,
		}

		err := manager.SetSnapshot(ctx, peerID, newSnapshotInfo)
		require.NoError(t, err)

		state := manager.GetState()
		member := state.Members[peerID.String()]
		assert.Equal(t, newSnapshotInfo.WrapperCID.Bytes(), member.Snapshot.SnapshotKey.Cid)
		assert.Equal(t, newSnapshotInfo.Height, member.Snapshot.Height)
		assert.Equal(t, "data", member.Metadata["existing"]) // Existing data preserved
	})
}

func TestStateManager_SetMeta(t *testing.T) {
	ctx := context.Background()
	store := sync.MutexWrap(ds.NewMapDatastore())
	key := ds.NewKey("/test/state")
	ttl := time.Hour
	mockClock := clock.NewMock()

	manager, err := NewStateManager(ctx, store, key, ttl, log.Logger("crdt"))
	require.NoError(t, err)
	manager.clock = mockClock

	peerID := createTestPeerID(t, "peer1")

	t.Run("set metadata for new member", func(t *testing.T) {
		metadata := map[string]string{
			"role":   "leader",
			"region": "us-west",
		}

		err := manager.SetMeta(ctx, peerID, metadata)
		require.NoError(t, err)

		state := manager.GetState()
		member := state.Members[peerID.String()]
		require.NotNil(t, member)
		assert.Equal(t, "leader", member.Metadata["role"])
		assert.Equal(t, "us-west", member.Metadata["region"])
		assert.Equal(t, uint64(mockClock.Now().Add(ttl).Unix()), member.BestBefore)
	})

	t.Run("update metadata for existing member", func(t *testing.T) {
		// Add existing member
		manager.state.Members[peerID.String()] = &pb.Participant{
			BestBefore: uint64(mockClock.Now().Add(-time.Hour).Unix()),
			Metadata: map[string]string{
				"role":     "follower",
				"existing": "value",
			},
		}

		newMetadata := map[string]string{
			"role":   "leader",  // Update existing
			"region": "us-east", // Add new
		}

		err := manager.SetMeta(ctx, peerID, newMetadata)
		require.NoError(t, err)

		state := manager.GetState()
		member := state.Members[peerID.String()]
		assert.Equal(t, "leader", member.Metadata["role"])
		assert.Equal(t, "us-east", member.Metadata["region"])
		assert.Equal(t, "value", member.Metadata["existing"]) // Existing preserved
	})

	t.Run("no changes made - no save", func(t *testing.T) {
		// Set up existing member with metadata
		manager.state.Members[peerID.String()] = &pb.Participant{
			BestBefore: uint64(mockClock.Now().Add(-time.Hour).Unix()),
			Metadata: map[string]string{
				"role": "leader",
			},
		}
		originalBestBefore := manager.state.Members[peerID.String()].BestBefore

		// Try to set same metadata
		sameMetadata := map[string]string{
			"role": "leader",
		}

		err := manager.SetMeta(ctx, peerID, sameMetadata)
		require.NoError(t, err)

		// BestBefore should not be updated since no changes were made
		state := manager.GetState()
		member := state.Members[peerID.String()]
		assert.Equal(t, originalBestBefore, member.BestBefore)
	})
}

func TestStateManager_SetMembershipUpdateCallback(t *testing.T) {
	ctx := context.Background()
	store := sync.MutexWrap(ds.NewMapDatastore())
	key := ds.NewKey("/test/state")
	ttl := time.Hour

	manager, err := NewStateManager(ctx, store, key, ttl, log.Logger("crdt"))
	require.NoError(t, err)

	callbackCount := 0
	var lastMembers map[string]*pb.Participant

	callback := func(members map[string]*pb.Participant) {
		callbackCount++
		lastMembers = members
	}

	manager.SetMembershipUpdateCallback(callback)

	peerID := createTestPeerID(t, "peer1")
	err = manager.UpdateHeads(ctx, peerID, []cid.Cid{createTestCID(t, "test")}, true)
	require.NoError(t, err)

	// Wait for goroutine
	time.Sleep(10 * time.Millisecond)

	assert.Equal(t, 1, callbackCount)
	assert.Contains(t, lastMembers, peerID.String())
}

func TestStateManager_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	store := sync.MutexWrap(ds.NewMapDatastore())
	key := ds.NewKey("/test/state")
	ttl := time.Hour

	manager, err := NewStateManager(ctx, store, key, ttl, log.Logger("crdt"))
	require.NoError(t, err)

	// Test concurrent access doesn't cause race conditions
	const numGoroutines = 10
	const numOperations = 100

	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()

			peerID := createTestPeerID(t, string(rune('A'+id)))

			for j := 0; j < numOperations; j++ {
				// Mix different operations
				switch j % 4 {
				case 0:
					manager.UpdateHeads(ctx, peerID, []cid.Cid{createTestCID(t, "test")}, true)
				case 1:
					manager.SetMeta(ctx, peerID, map[string]string{"iter": string(rune('0' + j))})
				case 2:
					manager.GetState()
				case 3:
					broadcast := &pb.StateBroadcast{
						Members: map[string]*pb.Participant{
							peerID.String(): {
								BestBefore: uint64(time.Now().Add(ttl).Unix()),
								Metadata:   map[string]string{},
							},
						},
					}
					manager.MergeMembers(ctx, broadcast)
				}
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify state is consistent
	state := manager.GetState()
	assert.NotNil(t, state)
	assert.NotNil(t, state.Members)
}

func TestStateManager_ErrorHandling(t *testing.T) {
	ctx := context.Background()

	t.Run("nil snapshot info", func(t *testing.T) {
		store := sync.MutexWrap(ds.NewMapDatastore())
		key := ds.NewKey("/test/state")
		ttl := time.Hour

		manager, err := NewStateManager(ctx, store, key, ttl, log.Logger("crdt"))
		require.NoError(t, err)

		peerID := createTestPeerID(t, "peer1")

		// This should not panic, but may set nil snapshot
		err = manager.SetSnapshot(ctx, peerID, &SnapshotInfo{})
		require.NoError(t, err)
	})

	t.Run("nil broadcast", func(t *testing.T) {
		store := sync.MutexWrap(ds.NewMapDatastore())
		key := ds.NewKey("/test/state")
		ttl := time.Hour

		manager, err := NewStateManager(ctx, store, key, ttl, log.Logger("crdt"))
		require.NoError(t, err)

		// This should handle nil gracefully
		err = manager.MergeMembers(ctx, &pb.StateBroadcast{})
		require.NoError(t, err)
	})
}

// Benchmark tests
func BenchmarkStateManager_UpdateHeads(b *testing.B) {
	ctx := context.Background()
	store := sync.MutexWrap(ds.NewMapDatastore())
	key := ds.NewKey("/test/state")
	ttl := time.Hour

	manager, err := NewStateManager(ctx, store, key, ttl, log.Logger("crdt"))
	require.NoError(b, err)

	peerID := createTestPeerID(b, "peer1")
	testCIDs := []cid.Cid{createTestCID(b, "test")}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := manager.UpdateHeads(ctx, peerID, testCIDs, true)
		require.NoError(b, err)
	}
}

func BenchmarkStateManager_GetState(b *testing.B) {
	ctx := context.Background()
	store := sync.MutexWrap(ds.NewMapDatastore())
	key := ds.NewKey("/test/state")
	ttl := time.Hour

	manager, err := NewStateManager(ctx, store, key, ttl, log.Logger("crdt"))
	require.NoError(b, err)

	// Add some state
	for i := 0; i < 100; i++ {
		peerID := createTestPeerID(b, string(rune('A'+i)))
		manager.UpdateHeads(ctx, peerID, []cid.Cid{createTestCID(b, string(rune('A'+i)))}, true)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state := manager.GetState()
		_ = state
	}
}
