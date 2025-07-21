package crdt

import (
	"context"
	stdsync "sync"
	"sync/atomic"
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
		var triggered int32
		var callbackMembers map[string]*pb.Participant
		var mu stdsync.Mutex

		manager.SetMembershipUpdateCallback(func(members map[string]*pb.Participant) {
			mu.Lock()
			callbackMembers = members
			mu.Unlock()
			atomic.StoreInt32(&triggered, 1)
		})

		err := manager.UpdateHeads(ctx, peerID, testCIDs, true)
		require.NoError(t, err)

		// Wait for callback to run
		time.Sleep(10 * time.Millisecond)

		assert.Equal(t, int32(1), atomic.LoadInt32(&triggered))

		mu.Lock()
		defer mu.Unlock()
		assert.Contains(t, callbackMembers, peerID.String())
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

	var mu stdsync.Mutex
	callbackCount := 0
	var lastMembers map[string]*pb.Participant

	callback := func(members map[string]*pb.Participant) {
		mu.Lock()
		defer mu.Unlock()
		callbackCount++
		lastMembers = members
	}

	manager.SetMembershipUpdateCallback(callback)

	peerID := createTestPeerID(t, "peer1")
	err = manager.UpdateHeads(ctx, peerID, []cid.Cid{createTestCID(t, "test")}, true)
	require.NoError(t, err)

	// Wait for goroutine
	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
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
					_ = manager.UpdateHeads(ctx, peerID, []cid.Cid{createTestCID(t, "test")}, true)
				case 1:
					_ = manager.SetMeta(ctx, peerID, map[string]string{"iter": string(rune('0' + j))})
				case 2:
					_ = manager.GetState()
				case 3:
					broadcast := &pb.StateBroadcast{
						Members: map[string]*pb.Participant{
							peerID.String(): {
								BestBefore: uint64(time.Now().Add(ttl).Unix()),
								Metadata:   map[string]string{},
							},
						},
					}
					_ = manager.MergeMembers(ctx, broadcast)
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
		_ = manager.UpdateHeads(ctx, peerID, []cid.Cid{createTestCID(b, string(rune('A'+i)))}, true)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state := manager.GetState()
		_ = state
	}
}

// Additional tests for StateSeq functionality - add to statemanager_test.go

func TestStateManager_StateSeqIncrement(t *testing.T) {
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
	testCIDs := []cid.Cid{createTestCID(t, "test1")}

	t.Run("new member starts with seq 1", func(t *testing.T) {
		err := manager.UpdateHeads(ctx, peerID, testCIDs, true)
		require.NoError(t, err)

		state := manager.GetState()
		member := state.Members[peerID.String()]
		require.NotNil(t, member)
		assert.Equal(t, uint32(1), member.StateSeq)
	})

	t.Run("subsequent updates increment seq", func(t *testing.T) {
		err := manager.UpdateHeads(ctx, peerID, testCIDs, true)
		require.NoError(t, err)

		state := manager.GetState()
		member := state.Members[peerID.String()]
		assert.Equal(t, uint32(2), member.StateSeq)

		// Another update
		err = manager.UpdateHeads(ctx, peerID, testCIDs, false)
		require.NoError(t, err)

		state = manager.GetState()
		member = state.Members[peerID.String()]
		assert.Equal(t, uint32(3), member.StateSeq)
	})

	t.Run("metadata updates increment seq", func(t *testing.T) {
		currentSeq := manager.state.Members[peerID.String()].StateSeq

		err := manager.SetMeta(ctx, peerID, map[string]string{"role": "leader"})
		require.NoError(t, err)

		state := manager.GetState()
		member := state.Members[peerID.String()]
		assert.Equal(t, currentSeq+1, member.StateSeq)
	})

	t.Run("no-change metadata doesn't increment seq", func(t *testing.T) {
		currentSeq := manager.state.Members[peerID.String()].StateSeq

		// Set same metadata again
		err := manager.SetMeta(ctx, peerID, map[string]string{"role": "leader"})
		require.NoError(t, err)

		state := manager.GetState()
		member := state.Members[peerID.String()]
		assert.Equal(t, currentSeq, member.StateSeq) // Should not increment
	})
}

func TestStateManager_StateSeqWraparound(t *testing.T) {
	ctx := context.Background()
	store := sync.MutexWrap(ds.NewMapDatastore())
	key := ds.NewKey("/test/state")
	ttl := time.Hour

	manager, err := NewStateManager(ctx, store, key, ttl, log.Logger("crdt"))
	require.NoError(t, err)

	peerID := createTestPeerID(t, "peer1")

	t.Run("sequence wraps around at 2^32", func(t *testing.T) {
		// Create member with high sequence number near max uint32
		manager.state.Members[peerID.String()] = &pb.Participant{
			BestBefore: uint64(time.Now().Add(ttl).Unix()),
			StateSeq:   0xFFFFFFFF, // Max uint32
			Metadata:   make(map[string]string),
		}

		err := manager.UpdateHeads(ctx, peerID, []cid.Cid{createTestCID(t, "test")}, true)
		require.NoError(t, err)

		state := manager.GetState()
		member := state.Members[peerID.String()]
		assert.Equal(t, uint32(0), member.StateSeq) // Should wrap to 0
	})
}

func TestStateManager_MergeMembersWithStateSeq(t *testing.T) {
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

	t.Run("higher sequence number wins", func(t *testing.T) {
		// Add existing member with seq 5
		manager.state.Members["peer1"] = &pb.Participant{
			BestBefore: uint64(mockClock.Now().Add(ttl).Unix()),
			StateSeq:   5,
			Metadata:   map[string]string{"role": "old_value"},
		}

		// Incoming broadcast with seq 6 (higher)
		broadcast := &pb.StateBroadcast{
			Members: map[string]*pb.Participant{
				"peer1": {
					BestBefore: uint64(mockClock.Now().Add(ttl).Unix()),
					StateSeq:   6,
					Metadata:   map[string]string{"role": "new_value"},
				},
			},
		}

		err := manager.MergeMembers(ctx, broadcast)
		require.NoError(t, err)

		state := manager.GetState()
		assert.Equal(t, "new_value", state.Members["peer1"].Metadata["role"])
		assert.Equal(t, uint32(6), state.Members["peer1"].StateSeq)
	})

	t.Run("lower sequence number is ignored", func(t *testing.T) {
		// Add existing member with seq 10
		manager.state.Members["peer1"] = &pb.Participant{
			BestBefore: uint64(mockClock.Now().Add(ttl).Unix()),
			StateSeq:   10,
			Metadata:   map[string]string{"role": "current_value"},
		}

		// Incoming broadcast with seq 8 (lower)
		broadcast := &pb.StateBroadcast{
			Members: map[string]*pb.Participant{
				"peer1": {
					BestBefore: uint64(mockClock.Now().Add(ttl).Unix()),
					StateSeq:   8,
					Metadata:   map[string]string{"role": "old_value"},
				},
			},
		}

		err := manager.MergeMembers(ctx, broadcast)
		require.NoError(t, err)

		state := manager.GetState()
		assert.Equal(t, "current_value", state.Members["peer1"].Metadata["role"])
		assert.Equal(t, uint32(10), state.Members["peer1"].StateSeq)
	})

	t.Run("sequence wraparound handling", func(t *testing.T) {
		// Current member with seq near max
		manager.state.Members["peer1"] = &pb.Participant{
			BestBefore: uint64(mockClock.Now().Add(ttl).Unix()),
			StateSeq:   0xFFFFFFF0, // Near max uint32
			Metadata:   map[string]string{"role": "old_value"},
		}

		// Incoming with seq that wrapped around
		broadcast := &pb.StateBroadcast{
			Members: map[string]*pb.Participant{
				"peer1": {
					BestBefore: uint64(mockClock.Now().Add(ttl).Unix()),
					StateSeq:   5, // Wrapped around from near-max
					Metadata:   map[string]string{"role": "wrapped_value"},
				},
			},
		}

		err := manager.MergeMembers(ctx, broadcast)
		require.NoError(t, err)

		state := manager.GetState()
		assert.Equal(t, "wrapped_value", state.Members["peer1"].Metadata["role"])
		assert.Equal(t, uint32(5), state.Members["peer1"].StateSeq)
	})

	t.Run("sequence difference at boundary", func(t *testing.T) {
		// Current member with seq 0
		manager.state.Members["peer1"] = &pb.Participant{
			BestBefore: uint64(mockClock.Now().Add(ttl).Unix()),
			StateSeq:   0,
			Metadata:   map[string]string{"role": "current_value"},
		}

		// Incoming with seq exactly at half-boundary (should be treated as older)
		broadcast := &pb.StateBroadcast{
			Members: map[string]*pb.Participant{
				"peer1": {
					BestBefore: uint64(mockClock.Now().Add(ttl).Unix()),
					StateSeq:   0x80000000, // Exactly at the boundary
					Metadata:   map[string]string{"role": "boundary_value"},
				},
			},
		}

		err := manager.MergeMembers(ctx, broadcast)
		require.NoError(t, err)

		state := manager.GetState()
		assert.Equal(t, "current_value", state.Members["peer1"].Metadata["role"])
		assert.Equal(t, uint32(0), state.Members["peer1"].StateSeq)
	})
}

func TestStateManager_MarkLeaving(t *testing.T) {
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

	t.Run("mark leaving for new member", func(t *testing.T) {
		grace := 10 * time.Second
		err := manager.MarkLeaving(ctx, peerID, grace)
		require.NoError(t, err)

		state := manager.GetState()
		member := state.Members[peerID.String()]
		require.NotNil(t, member)

		expectedExpiry := uint64(mockClock.Now().Add(grace).Unix())
		assert.Equal(t, expectedExpiry, member.BestBefore)
		assert.Equal(t, uint32(1), member.StateSeq)
	})

	t.Run("mark leaving for existing member increments seq", func(t *testing.T) {
		// Add existing member
		manager.state.Members[peerID.String()] = &pb.Participant{
			BestBefore: uint64(mockClock.Now().Add(ttl).Unix()),
			StateSeq:   5,
			Metadata:   map[string]string{"existing": "data"},
		}

		grace := 5 * time.Second
		err := manager.MarkLeaving(ctx, peerID, grace)
		require.NoError(t, err)

		state := manager.GetState()
		member := state.Members[peerID.String()]

		expectedExpiry := uint64(mockClock.Now().Add(grace).Unix())
		assert.Equal(t, expectedExpiry, member.BestBefore)
		assert.Equal(t, uint32(6), member.StateSeq)          // Should increment
		assert.Equal(t, "data", member.Metadata["existing"]) // Preserve existing data
	})

	t.Run("mark leaving update survives merge", func(t *testing.T) {
		// Set up a member with short TTL due to MarkLeaving
		grace := 5 * time.Second
		err := manager.MarkLeaving(ctx, peerID, grace)
		require.NoError(t, err)

		currentState := manager.GetState()
		leavingMember := currentState.Members[peerID.String()]
		shortTTL := leavingMember.BestBefore
		leavingSeq := leavingMember.StateSeq

		// Simulate receiving a broadcast from another peer with the old, long TTL
		// but lower sequence number
		broadcast := &pb.StateBroadcast{
			Members: map[string]*pb.Participant{
				peerID.String(): {
					BestBefore: uint64(mockClock.Now().Add(ttl).Unix()), // Much longer TTL
					StateSeq:   leavingSeq - 1,                          // Lower sequence
					Metadata:   map[string]string{"role": "old_data"},
				},
			},
		}

		err = manager.MergeMembers(ctx, broadcast)
		require.NoError(t, err)

		// Our short TTL should be preserved because our sequence is higher
		finalState := manager.GetState()
		finalMember := finalState.Members[peerID.String()]
		assert.Equal(t, shortTTL, finalMember.BestBefore, "Short TTL should be preserved")
		assert.Equal(t, leavingSeq, finalMember.StateSeq, "Higher sequence should be preserved")
	})
}

// Test the seq32Newer function directly
func TestSeq32Newer(t *testing.T) {
	tests := []struct {
		name     string
		seqA     uint32
		seqB     uint32
		expected bool
	}{
		{"simple newer", 5, 4, true},
		{"simple older", 4, 5, false},
		{"same sequence", 5, 5, false},
		{"wraparound newer", 5, 0xFFFFFFF0, true},  // Large seqB, small seqA = newer
		{"wraparound older", 0xFFFFFFF0, 5, false}, // Going backwards
		{"boundary case - just under half", 0x7FFFFFFF, 0, true},
		{"boundary case - exactly half", 0x80000000, 0, false},
		{"boundary case - just over half", 0x80000001, 0, false},
		{"max newer", 0xFFFFFFFF, 0xFFFFFFFE, true},
		{"zero newer than max", 0, 0xFFFFFFFF, true}, // Wraparound case
		{"large gap still newer", 1000, 100, true},
		{"large gap older", 100, 1000, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := seq32Newer(tt.seqA, tt.seqB)
			assert.Equal(t, tt.expected, result,
				"seq32Newer(%d, %d) = %v, expected %v",
				tt.seqA, tt.seqB, result, tt.expected)
		})
	}
}
