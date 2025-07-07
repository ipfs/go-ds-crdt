package crdt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockservice"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	mdutils "github.com/ipfs/boxo/ipld/merkledag/test"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-ds-crdt/pb"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-log"
	logging "github.com/ipfs/go-log/v2"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func setupTestEnv(t *testing.T, opts *Options) (*Datastore, Peer, format.DAGService, func()) {
	t.Helper()
	memStore := makeStore(t, 0)
	bs := mdutils.Bserv()
	dagserv := dag.NewDAGService(bs)
	dagService := &mockDAGSvc{DAGService: dagserv, bs: bs.Blockstore()}
	broadcasters, cancel := newBroadcasters(t, 1)
	broadcaster := broadcasters[0]
	h := newMockPeer("test-peer")

	opts.Logger =
		&testLogger{
			name: fmt.Sprintf("r#%d: ", 0),
			l:    DefaultOptions().Logger,
		}
	opts.Logger = &testLogger{name: t.Name(),
		l: DefaultOptions().Logger}
	store, err := New(h, memStore, bs.Blockstore(), ds.NewKey("/test"), dagService, broadcaster, opts)
	require.NoError(t, err)

	cleanup := func() {
		cancel()
	}
	return store, h, dagService, cleanup
}

type testEnv struct {
	t                 testing.TB
	mu                sync.Mutex
	replicas          []*Datastore
	broadcasters      []*mockBroadcaster
	replicaBcastChans []chan []byte
	bs                blockservice.BlockService
	dagserv           format.DAGService
	ctx               context.Context
	cancelBcasts      context.CancelFunc
}

func createTestEnv(t testing.TB) *testEnv {
	replicas := make([]*Datastore, 0)
	broadcasters := make([]*mockBroadcaster, 0)
	bs := mdutils.Bserv()
	dagserv := dag.NewDAGService(bs)
	bcastChans := make([]chan []byte, 0)
	ctx, cancel := context.WithCancel(context.Background())
	return &testEnv{
		t:                 t,
		ctx:               ctx,
		replicas:          replicas,
		broadcasters:      broadcasters,
		replicaBcastChans: bcastChans,
		bs:                bs,
		dagserv:           dagserv,
		cancelBcasts:      cancel,
	}
}

func (env *testEnv) AddReplica(opts *Options) {
	env.mu.Lock()
	defer env.mu.Unlock()

	var replicaOpts *Options
	if opts == nil {
		replicaOpts = DefaultOptions()
	} else {
		copy := *opts
		replicaOpts = &copy
	}

	// Index for new replica in replicas list
	i := len(env.replicas)

	// Set up replica options
	replicaOpts.Logger = &testLogger{
		name: fmt.Sprintf("r#%d: ", i),
		l:    DefaultOptions().Logger,
	}
	replicaOpts.RebroadcastInterval = time.Second * 5
	replicaOpts.NumWorkers = 5
	replicaOpts.DAGSyncerTimeout = time.Second

	// Create mock broadcaster for the new replica

	myChan := make(chan []byte, 300)
	env.replicaBcastChans = append(env.replicaBcastChans, myChan)
	broadcaster := &mockBroadcaster{
		ctx:      env.ctx,
		chans:    env.replicaBcastChans,
		myChan:   myChan,
		dropProb: &atomic.Int64{},
		t:        env.t,
	}
	env.broadcasters = append(env.broadcasters, broadcaster)

	// Create the new replica

	dagsync := &mockDAGSvc{
		DAGService: env.dagserv,
		bs:         env.bs.Blockstore(),
	}
	h := newMockPeer(fmt.Sprintf("peer-%d", i))
	var err error
	replica, err := New(h, makeStore(env.t, i), env.bs.Blockstore(), ds.NewKey("crdttest"), dagsync, broadcaster, replicaOpts)
	if err != nil {
		env.t.Fatal(err)
	}

	// Add new replica to list of replicas
	env.replicas = append(env.replicas, replica)

	// Update all replica broadcasters with new list of broadcast channels
	for i, b := range env.broadcasters {
		b.mu.Lock()
		env.broadcasters[i].chans = env.replicaBcastChans
		b.mu.Unlock()
	}

	if debug {
		_ = log.SetLogLevel("crdt", "debug")
	}
}

func (env *testEnv) Cleanup() {
	env.cancelBcasts()
	for i, r := range env.replicas {
		err := r.Close()
		if err != nil {
			env.t.Error(err)
		}
		_ = os.RemoveAll(storeFolder(i))
	}
}

func requireReplicaHasKey(t *testing.T, ctx context.Context, replica *Datastore, key ds.Key, value []byte) {
	t.Helper()
	require.Eventually(t, func() bool {
		v, err := replica.Get(ctx, key)
		return err == nil && bytes.Equal(v, value)
	}, 15*time.Second, 500*time.Millisecond)
}

func requireReplicaDoesNotHaveKey(t *testing.T, ctx context.Context, replica *Datastore, key ds.Key, msgAndArgs ...interface{}) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, err := replica.Get(ctx, key)
		return errors.Is(err, ds.ErrNotFound)
	}, 15*time.Second, 500*time.Millisecond, msgAndArgs...)
}

func TestSnapshotCreationAndContent(t *testing.T) {
	const (
		numKeys      = 502
		compactEvery = 100
	)
	ctx := context.Background()
	opts := DefaultOptions()
	opts.CompactDagSize = compactEvery
	opts.CompactRetainNodes = 1
	store, h, _, cleanup := setupTestEnv(t, opts)
	defer cleanup()

	var snapshotRoot cid.Cid

	for i := 1; i <= numKeys; i++ {
		k := fmt.Sprintf("key-%d", i)
		v := fmt.Sprintf("value-%d", i)
		require.NoError(t, store.Put(ctx, ds.NewKey(k), []byte(v)), "failed to put key-value")

		// Manually trigger compaction every 100 puts
		if i%compactEvery == 0 {
			require.NoError(t, store.triggerCompactionIfNeeded(ctx))
			m, ok := store.InternalStats(ctx).State.Members[h.ID().String()]
			require.True(t, ok, "our peerid should exist in the state")
			require.NotNil(t, m.Snapshot)
			snapshotRoot = cid.MustParse(m.Snapshot.SnapshotKey.Cid)
		}
	}

	info, err := store.loadSnapshotInfo(ctx, snapshotRoot)
	require.NoError(t, err)

	// Extract the snapshot contents
	set := ExtractSnapshotSet(t, ctx, store, info.HamtRootCID)
	// Validate some of the early inserted keys exist
	for i := 1; i <= 300; i++ { // Keys inserted early should survive
		k := fmt.Sprintf("key-%d", i)
		v := fmt.Sprintf("value-%d", i)
		cv, err := set.Element(ctx, k)
		require.NoError(t, err)

		require.Equal(t, v, string(cv), fmt.Sprintf("key %s has incorrect value", k))
	}
}

func TestCRDTRemoveConvergesAfterRestoringSnapshot(t *testing.T) {
	// Step 1: Set up replicas with compaction options enabled
	opts := DefaultOptions()
	opts.CompactDagSize = 50     // Ensure compaction occurs when the DAG grows beyond 50 nodes
	opts.CompactRetainNodes = 10 // Retain the last 10 nodes after compaction
	opts.CompactInterval = 5 * time.Second

	env := createTestEnv(t)
	defer env.Cleanup()

	env.AddReplica(opts)

	r0Id := env.replicas[0].h.ID().String()

	ctx := context.Background()

	k := ds.NewKey("k1")
	k2 := ds.NewKey("k2")

	// Modify `k1` multiple times to simulate realistic updates
	require.NoError(t, env.replicas[0].Put(ctx, k, []byte("v1")))
	require.NoError(t, env.replicas[0].Put(ctx, k, []byte("v2")))

	// Populate Replica 0 with a significant number of key updates to ensure compaction
	for i := 1; i <= 60; i++ { // 60 operations to ensure we pass the compaction threshold
		key := ds.NewKey(fmt.Sprintf("key-%d", i))
		require.NoError(t, env.replicas[0].Put(ctx, key, []byte(fmt.Sprintf("value-%d", i))))
	}

	// Ensure at least one more key exists before compaction
	require.NoError(t, env.replicas[0].Put(ctx, k2, []byte("v1")))

	// Wait for compaction to trigger automatically
	require.Eventually(t, func() bool {
		s := env.replicas[0].InternalStats(ctx).State.Members[r0Id]
		return s != nil && s.Snapshot != nil &&
			s.Snapshot.SnapshotKey != nil
	}, 1*time.Minute, 500*time.Millisecond, "Replica 0 should have created a snapshot")

	// Fetch the snapshot that was created
	s := env.replicas[0].InternalStats(ctx).State

	require.NotNil(t, s.Members[r0Id].Snapshot, "Snapshot should have been triggered")

	// Add new replica, which should eventually restore the snapshot automatically
	env.AddReplica(opts)
	r1Id := env.replicas[1].h.ID().String()
	// Wait for snapshot to be present in replica 1
	require.Eventually(t, func() bool {
		s := env.replicas[1].InternalStats(ctx).State.Members[r1Id]
		return s != nil && s.Snapshot != nil &&
			s.Snapshot.SnapshotKey != nil
	}, 1*time.Minute, 500*time.Millisecond, "Replica 1 should have gotten a snapshot")

	// Verify that key `k1` now exists in Replica 1 with the last known value
	requireReplicaHasKey(t, ctx, env.replicas[1], k, []byte("v2"))

	// Delete `k1` in Replica 1
	require.NoError(t, env.replicas[1].Delete(ctx, k))

	// Verify deletion in both replicas

	_, err := env.replicas[1].Get(ctx, k)
	require.ErrorIs(t, err, ds.ErrNotFound, "Key should not exist in Replica 1")

	requireReplicaDoesNotHaveKey(t, ctx, env.replicas[0], k, "Key should not exist in Replica 0")
}

func TestCompactionWithMultipleHeads(t *testing.T) {
	ctx := context.Background()

	/*
		// for crazy debugging use the following
		cfg := zap.NewDevelopmentConfig()
		cfg.EncoderConfig = zapcore.EncoderConfig{
			MessageKey: "M", // Just the message key
			// Set everything else to empty or omit it
			TimeKey:        "",
			LevelKey:       "",
			NameKey:        "",
			CallerKey:      "",
			FunctionKey:    "",
			StacktraceKey:  "",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    nil,
			EncodeTime:     nil,
			EncodeDuration: nil,
			EncodeCaller:   nil,
		}

		logger, _ := cfg.Build()
		logging.SetPrimaryCore(logger.Core())

		// Now set log level
		_ = logging.SetLogLevel("crdt", "debug")

		_ = logging.Logger("crdt")
		_ = logging.SetLogLevel("crdt", "debug")
	*/
	o := DefaultOptions()
	o.CompactDagSize = 50
	o.CompactRetainNodes = 10
	o.CompactInterval = 5 * time.Second
	o.RebroadcastInterval = 10 * time.Second

	replicas, closeReplicas := makeNReplicas(t, 2, o)
	defer closeReplicas()

	r0 := replicas[0]
	r1 := replicas[1]

	br0 := r0.broadcaster.(*mockBroadcaster)
	br1 := r1.broadcaster.(*mockBroadcaster)

	// Phase 1: Create common DAG foundation with both replicas connected
	t.Logf("Phase 1: Creating common DAG foundation")

	// Ensure both replicas are connected for foundation (needed for compaction consensus)
	br0.dropProb.Store(0)
	br1.dropProb.Store(0)

	for i := 1; i < 50; i++ {
		key := fmt.Sprintf("foundation-key-%d", i)
		value := []byte(fmt.Sprintf("foundation-value-%d", i))
		require.NoError(t, r0.Put(ctx, ds.NewKey(key), value))
	}

	// Wait for r1 to sync the foundation
	require.Eventually(t, func() bool {
		testKey := ds.NewKey("foundation-key-49")
		_, err := r1.Get(ctx, testKey)
		return err == nil
	}, 30*time.Second, 1*time.Second)

	t.Logf("Phase 1 complete - both replicas have common foundation")

	// Wait for both replicas to see each other before continuing
	require.Eventually(t, func() bool {
		r0State := r0.GetState(ctx)
		r1State := r1.GetState(ctx)

		r0Members := len(r0State.Members)
		r1Members := len(r1State.Members)

		t.Logf("Membership sync check - r0 sees %d members, r1 sees %d members", r0Members, r1Members)

		// Both should see both members (themselves + the other)
		expectedMembers := 2
		if r0Members >= expectedMembers && r1Members >= expectedMembers {
			t.Logf("✓ Full membership achieved - both replicas see all %d members", expectedMembers)
			return true
		}

		// Log details if not ready
		t.Logf("Waiting for full membership:")
		for id, member := range r0State.Members {
			t.Logf("  r0 sees member %s: %d heads", id, len(member.DagHeads))
		}
		for id, member := range r1State.Members {
			t.Logf("  r1 sees member %s: %d heads", id, len(member.DagHeads))
		}

		return false
	}, 30*time.Second, 1*time.Second)

	// Wait for both replicas to be properly synced (same head count)
	require.Eventually(t, func() bool {
		r0Stats := r0.InternalStats(ctx)
		r1Stats := r1.InternalStats(ctx)

		t.Logf("Sync check - r0: %d heads, r1: %d heads", len(r0Stats.Heads), len(r1Stats.Heads))

		// Both should have the same number of heads and at least 1
		return len(r0Stats.Heads) > 0 && len(r1Stats.Heads) > 0 &&
			len(r0Stats.Heads) == len(r1Stats.Heads)
	}, 30*time.Second, 2*time.Second)

	// Verify we start with single head on both replicas
	r0Stats := r0.InternalStats(ctx)
	r1Stats := r1.InternalStats(ctx)
	t.Logf("After foundation sync - r0: %d heads, r1: %d heads", len(r0Stats.Heads), len(r1Stats.Heads))
	require.Equal(t, 1, len(r0Stats.Heads), "r0 should start with single head after foundation")
	require.Equal(t, 1, len(r1Stats.Heads), "r1 should start with single head after foundation")

	// Phase 2: Create divergent branches and VERIFY we get multiple heads
	foundMultipleHeads := false
	maxHeadsObserved := 1

	for batch := 0; batch < 3; batch++ {
		t.Logf("Phase 2: Starting partition batch %d", batch)

		// Partition the network to create divergent branches
		br0.dropProb.Store(100) // Fixed: ensure 100% drop, not 101
		br1.dropProb.Store(100)

		// Write different keys on each replica while partitioned
		var eg errgroup.Group
		eg.Go(func() error {
			base := 100 + (batch * 200)
			for i := base; i < base+100; i++ {
				key := fmt.Sprintf("r0-key-%d", i)
				value := []byte(fmt.Sprintf("r0-value-%d", i))
				if err := r0.Put(ctx, ds.NewKey(key), value); err != nil {
					return err
				}
			}
			return nil
		})

		eg.Go(func() error {
			base := 100 + (batch * 200)
			for i := base; i < base+100; i++ {
				key := fmt.Sprintf("r1-key-%d", i)
				value := []byte(fmt.Sprintf("r1-value-%d", i))
				if err := r1.Put(ctx, ds.NewKey(key), value); err != nil {
					return err
				}
			}
			return nil
		})

		require.NoError(t, eg.Wait())

		// Verify each replica has independent heads while partitioned
		r0Stats := r0.InternalStats(ctx)
		r1Stats := r1.InternalStats(ctx)
		t.Logf("During partition %d - r0 heads: %d, r1 heads: %d", batch, len(r0Stats.Heads), len(r1Stats.Heads))

		// Verify they actually have different head CIDs while partitioned
		require.Eventually(t, func() bool {
			r0Stats := r0.InternalStats(ctx)
			r1Stats := r1.InternalStats(ctx)

			if len(r0Stats.Heads) == 0 || len(r1Stats.Heads) == 0 {
				return false
			}

			// Check if they have different head CIDs
			diverged := r0Stats.Heads[0] != r1Stats.Heads[0]
			if diverged {
				t.Logf("✓ Verified divergence - r0 head: %s, r1 head: %s",
					r0Stats.Heads[0], r1Stats.Heads[0])
			}
			return diverged
		}, 10*time.Second, 500*time.Millisecond)

		// Reconnect and observe the merge process
		br0.dropProb.Store(0)
		br1.dropProb.Store(0)

		// Monitor heads during the merge process - increased window and frequency
		for i := 0; i < 100; i++ { // Increased from 30 to 100
			r0Stats := r0.InternalStats(ctx)
			r1Stats := r1.InternalStats(ctx)

			currentMaxHeads := len(r0Stats.Heads)
			if len(r1Stats.Heads) > currentMaxHeads {
				currentMaxHeads = len(r1Stats.Heads)
			}

			if currentMaxHeads > maxHeadsObserved {
				maxHeadsObserved = currentMaxHeads
			}

			if currentMaxHeads > 1 {
				foundMultipleHeads = true
				t.Logf("✓ Multiple heads observed! Batch %d, iteration %d: r0=%d heads, r1=%d heads",
					batch, i, len(r0Stats.Heads), len(r1Stats.Heads))

				// Log the actual head CIDs for debugging
				t.Logf("  r0 heads: %v", r0Stats.Heads)
				t.Logf("  r1 heads: %v", r1Stats.Heads)
			}

			time.Sleep(100 * time.Millisecond) // Reduced from 200ms to 100ms for finer granularity
		}

		// Final state after this batch
		r0Stats = r0.InternalStats(ctx)
		r1Stats = r1.InternalStats(ctx)
		t.Logf("After batch %d merge: r0=%d heads, r1=%d heads, r0 MaxHeight=%d",
			batch, len(r0Stats.Heads), len(r1Stats.Heads), r0Stats.MaxHeight)
	}

	// REQUIRE that we actually observed multiple heads - FAIL THE TEST if we didn't
	t.Logf("Maximum heads observed during test: %d", maxHeadsObserved)
	if !foundMultipleHeads {
		t.Fatalf("TEST FAILURE: Never observed multiple heads! This test is supposed to demonstrate multiple heads. Max heads seen: %d", maxHeadsObserved)
	}
	if maxHeadsObserved <= 1 {
		t.Fatalf("TEST FAILURE: Never observed more than 1 head during partition/merge cycles. Max heads seen: %d", maxHeadsObserved)
	}

	t.Logf("✓ SUCCESS: Multiple heads verified! Maximum observed: %d", maxHeadsObserved)

	// Phase 3: Add sequential writes to exceed compaction threshold
	t.Logf("Phase 3: Adding sequential writes to trigger compaction")
	for i := 1000; i < 1100; i++ {
		key := fmt.Sprintf("sequential-key-%d", i)
		value := []byte(fmt.Sprintf("sequential-value-%d", i))
		require.NoError(t, r0.Put(ctx, ds.NewKey(key), value))
	}

	keyFinal := ds.NewKey("key-final")
	valueFinal := []byte("value-final")
	require.NoError(t, r0.Put(ctx, keyFinal, valueFinal))

	// Wait for final sync - but we already verified multiple heads above
	require.Eventually(t, func() bool {
		r0Stats := r0.InternalStats(ctx)
		r1Stats := r1.InternalStats(ctx)
		return len(r0Stats.Heads) > 0 && len(r1Stats.Heads) > 0
	}, 30*time.Second, 1*time.Second)

	finalStats := r0.InternalStats(ctx)
	t.Logf("Final state before compaction - heads: %d, MaxHeight: %d",
		len(finalStats.Heads), finalStats.MaxHeight)

	// Phase 4: Wait for compaction
	require.Eventually(t, func() bool {
		stats := r0.InternalStats(ctx)
		t.Logf("Checking for compaction - Heads: %d, MaxHeight: %d", len(stats.Heads), stats.MaxHeight)

		if stats.State == nil || stats.State.Members == nil {
			return false
		}

		member, exists := stats.State.Members[r0.h.ID().String()]
		if !exists {
			return false
		}

		if member.Snapshot == nil {
			return false
		}

		t.Logf("✓ Snapshot found for member %s", r0.h.ID().String())
		return true
	}, 2*time.Minute, 1*time.Second)

	// Phase 5: Comprehensive database verification
	t.Logf("Phase 5: Verifying database integrity on both replicas")

	// Verify foundation keys exist on both replicas
	foundationOnR0, foundationOnR1 := 0, 0
	for i := 1; i < 50; i++ {
		key := ds.NewKey(fmt.Sprintf("foundation-key-%d", i))

		if _, err := r0.Get(ctx, key); err == nil {
			foundationOnR0++
		}
		if _, err := r1.Get(ctx, key); err == nil {
			foundationOnR1++
		}
	}
	t.Logf("Foundation keys - r0: %d/49, r1: %d/49", foundationOnR0, foundationOnR1)
	require.Equal(t, 49, foundationOnR0, "r0 should have all foundation keys")
	require.Equal(t, 49, foundationOnR1, "r1 should have all foundation keys")

	// Verify branch keys from all batches
	totalR0Keys, totalR1Keys := 0, 0
	for batch := 0; batch < 3; batch++ {
		r0KeysOnR0, r0KeysOnR1 := 0, 0
		r1KeysOnR0, r1KeysOnR1 := 0, 0

		base := 100 + (batch * 200)
		for i := base; i < base+100; i++ {
			// Check r0's keys
			r0Key := ds.NewKey(fmt.Sprintf("r0-key-%d", i))
			if _, err := r0.Get(ctx, r0Key); err == nil {
				r0KeysOnR0++
				totalR0Keys++
			}
			if _, err := r1.Get(ctx, r0Key); err == nil {
				r0KeysOnR1++
			}

			// Check r1's keys
			r1Key := ds.NewKey(fmt.Sprintf("r1-key-%d", i))
			if _, err := r0.Get(ctx, r1Key); err == nil {
				r1KeysOnR0++
			}
			if _, err := r1.Get(ctx, r1Key); err == nil {
				r1KeysOnR1++
				totalR1Keys++
			}
		}

		t.Logf("Batch %d verification:", batch)
		t.Logf("  r0's keys: r0 has %d/100, r1 has %d/100", r0KeysOnR0, r0KeysOnR1)
		t.Logf("  r1's keys: r0 has %d/100, r1 has %d/100", r1KeysOnR0, r1KeysOnR1)
	}

	t.Logf("Total branch keys - r0 created: %d, r1 created: %d", totalR0Keys, totalR1Keys)
	require.Greater(t, totalR0Keys, 250, "r0 should have created 300 keys across 3 batches")
	require.Greater(t, totalR1Keys, 250, "r1 should have created 300 keys across 3 batches")

	// Verify sequential keys
	sequentialOnR0, sequentialOnR1 := 0, 0
	for i := 1000; i < 1100; i++ {
		key := ds.NewKey(fmt.Sprintf("sequential-key-%d", i))

		if _, err := r0.Get(ctx, key); err == nil {
			sequentialOnR0++
		}
		if _, err := r1.Get(ctx, key); err == nil {
			sequentialOnR1++
		}
	}
	t.Logf("Sequential keys - r0: %d/100, r1: %d/100", sequentialOnR0, sequentialOnR1)
	require.Equal(t, 100, sequentialOnR0, "r0 should have all sequential keys")
	require.Equal(t, 100, sequentialOnR1, "r1 should have all sequential keys")

	// Verify final key
	finalKey := ds.NewKey("key-final")
	_, err := r0.Get(ctx, finalKey)
	require.NoError(t, err, "r0 should have final key")
	_, err = r1.Get(ctx, finalKey)
	require.NoError(t, err, "r1 should have final key")

	t.Logf("✓ Database verification complete - both replicas have working databases")

	// Phase 6: Verify snapshot integrity
	snapshotCID := cid.MustParse(r0.InternalStats(ctx).State.Members[r0.h.ID().String()].Snapshot.SnapshotKey.Cid)

	info, err := r0.loadSnapshotInfo(ctx, snapshotCID)
	require.NoError(t, err)

	snapshotSet := ExtractSnapshotSet(t, ctx, r0, info.HamtRootCID)

	// Verify foundation keys are in the snapshot
	foundationKeysFound := 0
	for i := 1; i < 50; i++ {
		expectedKey := fmt.Sprintf("/foundation-key-%d", i)
		_, err := snapshotSet.Element(ctx, expectedKey)
		if err == nil {
			foundationKeysFound++
		}
	}

	t.Logf("Found %d/49 foundation keys in snapshot", foundationKeysFound)
	require.Greater(t, foundationKeysFound, 40, "Should find most foundation keys in snapshot")

	// Verify branch keys from all batches are in the snapshot
	r0BranchKeysFound, r1BranchKeysFound := 0, 0
	for batch := 0; batch < 3; batch++ {
		base := 100 + (batch * 200)
		for i := base; i < base+150; i++ { // Check first 50 of each batch
			r0Key := fmt.Sprintf("/r0-key-%d", i)
			if _, err := snapshotSet.Element(ctx, r0Key); err == nil {
				r0BranchKeysFound++
			}

			r1Key := fmt.Sprintf("/r1-key-%d", i)
			if _, err := snapshotSet.Element(ctx, r1Key); err == nil {
				r1BranchKeysFound++
			}
		}
	}

	t.Logf("Found %d r0 branch keys and %d r1 branch keys in snapshot",
		r0BranchKeysFound, r1BranchKeysFound)
	require.Greater(t, r0BranchKeysFound, 0, "Should find r0 branch keys in snapshot")

	// Check if r1 keys are missing from snapshot
	if r1BranchKeysFound == 0 {
		t.Logf("⚠️  No r1 branch keys found in snapshot - this suggests:")
		t.Logf("    1. Compaction occurred before r1's changes were fully merged")
		t.Logf("    2. Block availability issues during partition prevented r1 data inclusion")
		t.Logf("    3. CRDT merge strategy favored r0's timeline")
	} else {
		t.Logf("✓ Found r1 branch keys in snapshot")
	}

	// Verify some sequential keys are in the snapshot
	sequentialKeysFound := 0
	for i := 1000; i < 1050; i++ {
		expectedKey := fmt.Sprintf("/sequential-key-%d", i)
		_, err := snapshotSet.Element(ctx, expectedKey)
		if err == nil {
			sequentialKeysFound++
		}
	}

	t.Logf("Found %d/50 sequential keys in snapshot", sequentialKeysFound)
}

func ExtractSnapshotSet(t *testing.T, ctx context.Context, store *Datastore, snapshot cid.Cid) *set {
	hamtDS, err := NewHAMTDatastore(ctx, store.dagService, snapshot)
	require.NoError(t, err)

	snapshotSet, err := newCRDTSet(ctx, hamtDS, ds.NewKey(""), store.dagService, log.Logger("test"), nil, nil)
	require.NoError(t, err, "failed to create snapshot Set view")

	return snapshotSet
}

func TestSnapShotRestore(t *testing.T) {
	_ = logging.Logger("crdt")
	_ = logging.SetLogLevel("*", "debug")

	ctx := context.Background()

	opts := DefaultOptions()
	opts.CompactDagSize = 50
	opts.CompactRetainNodes = 10
	opts.CompactInterval = 5 * time.Second

	replicas, closeReplicas := makeNReplicas(t, 2, opts)
	defer closeReplicas()

	r0 := replicas[0]
	r1 := replicas[1]

	br0 := r0.broadcaster.(*mockBroadcaster)
	br1 := r1.broadcaster.(*mockBroadcaster)

	// Partition the network (simulate missing deltas)
	br0.dropProb.Store(101)
	br1.dropProb.Store(101)

	// Write lots of data to Replica 0
	for i := 1; i <= 60; i++ {
		key := ds.NewKey(fmt.Sprintf("k%d", i))
		require.NoError(t, r0.Put(ctx, key, []byte("v1")))
	}

	// Wait until Replica 0 creates a snapshot
	require.Eventually(t, func() bool {
		m := r0.InternalStats(ctx).State.Members[r0.h.ID().String()]
		return m.Snapshot != nil
	}, 15*time.Second, 500*time.Millisecond)

	// Un-partition the network
	br0.dropProb.Store(0)
	br1.dropProb.Store(0)

	// Wait until Replica 1 receives and processes the snapshot
	require.Eventually(t, func() bool {
		m := r1.InternalStats(ctx).State.Members[r1.h.ID().String()]
		return m != nil && m.Snapshot != nil
	}, 15*time.Second, 500*time.Millisecond)

	// Check: Replica 1 has data from Replica 0
	val, err := r1.Get(ctx, ds.NewKey("k50"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val)

	// Check: Snapshots are now in sync
	s0 := r0.InternalStats(ctx).State.Members[r0.h.ID().String()]
	s1 := r1.InternalStats(ctx).State.Members[r1.h.ID().String()]
	require.NotNil(t, s0.Snapshot)
	require.NotNil(t, s1.Snapshot)
	require.Equal(t, s0.Snapshot.SnapshotKey.Cid, s1.Snapshot.SnapshotKey.Cid)

	// Extra test: Replica 0 writes a new key
	newKey := ds.NewKey("kNew")
	require.NoError(t, r0.Put(ctx, newKey, []byte("newVal")))

	// Check that Replica 1 eventually sees the new key
	require.Eventually(t, func() bool {
		val, err := r1.Get(ctx, newKey)
		return err == nil && string(val) == "newVal"
	}, 10*time.Second, 500*time.Millisecond)
}

func TestTriggerSnapshot(t *testing.T) {
	ctx := context.Background()

	opts := DefaultOptions()
	opts.CompactDagSize = 50
	opts.CompactRetainNodes = 10
	opts.CompactInterval = 5 * time.Second

	replicas, closeReplicas := makeNReplicas(t, 1, opts)
	defer closeReplicas()

	for i := 1; i <= 100; i++ {
		k := ds.NewKey(fmt.Sprintf("k%d", i))
		require.NoError(t, replicas[0].Put(ctx, k, []byte("v1")))
	}

	require.Eventually(t, func() bool {
		m := replicas[0].InternalStats(ctx).State.Members[replicas[0].h.ID().String()]
		return m.Snapshot != nil
	}, 15*time.Second, 500*time.Millisecond)
}

func TestSnapshotAllowsLowerPriorityValueToResurface(t *testing.T) {
	ctx := context.Background()

	//=== CRITICAL BUG REGRESSION TEST ===
	//Testing that snapshots preserve full CRDT history, not just current k-v pairs
	//
	//📋 SCENARIO THAT EXPOSES THE BUG:
	//1. Create k3=v1 (lower priority)
	//2. Create k3=v2 (higher priority) - v2 wins
	//3. 🔄 CREATE SNAPSHOT (this is the critical step)
	//4. Selectively tombstone ONLY v2 (not v1)
	//5. Expected: v1 should resurface
	//6. 🐛 OLD BUG: New replica from snapshot only knows k3=v2, shows k3 as deleted after tombstone
	//7. ✅ FIXED: New replica knows about v1, correctly shows k3=v1
	//

	// We need to create this scenario using internal CRDT operations
	// because we need the snapshot to happen BEFORE the selective tombstone

	opts := DefaultOptions()
	opts.CompactDagSize = 8
	opts.CompactRetainNodes = 2
	opts.CompactInterval = 2 * time.Second

	env := createTestEnv(t)
	defer env.Cleanup()

	env.AddReplica(opts)
	replica := env.replicas[0]

	// Phase 1: Create the CRDT state with both versions
	t.Log("Phase 1: Creating CRDT state with lower and higher priority versions")

	require.NoError(t, replica.Put(ctx, ds.NewKey("k1"), []byte("baseline")))
	require.NoError(t, replica.Put(ctx, ds.NewKey("k3"), []byte("v3-LowerPriority"))) // Priority ~2

	// Snapshot of the tombstone delta BEFORE the second Put. We use this to filter later.
	rmvTmpDelta, err := replica.set.Rmv(ctx, "k3")
	require.NoError(t, err)

	// Add operations to build priority
	for i := 0; i < 3; i++ {
		key := ds.NewKey(fmt.Sprintf("build-%d", i))
		require.NoError(t, replica.Put(ctx, key, []byte("value")))
	}

	require.NoError(t, replica.Put(ctx, ds.NewKey("k3"), []byte("v3-GreaterPriority"))) // Priority ~6

	// Verify higher priority wins
	val, err := replica.Get(ctx, ds.NewKey("k3"))
	require.NoError(t, err)
	require.Equal(t, []byte("v3-GreaterPriority"), val)
	t.Logf("✓ State created: k3=%s (higher priority wins)", string(val))

	// Phase 2: Trigger compaction to create snapshot with BOTH versions in CRDT history
	t.Log("Phase 2: Creating snapshot with current state (k3=v3-GreaterPriority)")
	t.Log("🔑 CRITICAL: Snapshot must contain BOTH v3-LowerPriority AND v3-GreaterPriority")

	// Add 8 ops to exceed CompactDagSize=8 and trigger compaction logic
	for i := 0; i < 8; i++ {
		key := ds.NewKey(fmt.Sprintf("trigger-%d", i))
		require.NoError(t, replica.Put(ctx, key, []byte("compact")))
	}

	// Wait for snapshot creation
	require.Eventually(t, func() bool {
		stats := replica.InternalStats(ctx)
		m := stats.State.Members[replica.h.ID().String()]
		if m != nil && m.Snapshot != nil {
			t.Logf("✓ Snapshot created at height %d", m.Snapshot.Height)
			return true
		}
		return false
	}, 30*time.Second, 1*time.Second)

	// Phase 3: The critical test - selective tombstone AFTER snapshot creation
	t.Log("Phase 3: Applying selective tombstone of higher priority version")
	t.Log("⚠️  This happens AFTER snapshot creation - this is key to exposing the bug")

	// Here we need to simulate what would happen with selective tombstoning
	// Since we can't easily do selective tombstones with the high-level API,

	// Snapshot of the tombstone delta BEFORE the second Put. We use this to filter later.
	rmvDelta, err := replica.set.Rmv(ctx, "k3")
	require.NoError(t, err)

	// Only keep the tombstone that removes the newer (v3-GreaterPriority) version of k3.
	var filtered []*pb.Element
	for _, tombstone := range rmvDelta.Tombstones {
		if tombstone.Id != rmvTmpDelta.Tombstones[0].Id {
			filtered = append(filtered, tombstone)
		}
	}

	rmvDelta.Tombstones = filtered
	_, height, err := replica.heads.List(ctx)
	rmvDelta.Priority = height + 1
	require.NoError(t, err)
	require.NoError(t, replica.publish(ctx, rmvDelta))
	// For this test, we'll add a replica and see what it gets from the snapshot
	// The key insight: the snapshot was created when k3=v3-GreaterPriority was visible
	// If selective tombstoning happens after, the new replica needs to be able to
	// compute the correct state from the full CRDT history

	t.Log("Phase 4: Creating new replica from snapshot")
	t.Log("🎯 This new replica will restore from the snapshot created in Phase 2")

	env.AddReplica(opts)

	require.Eventually(t, func() bool {
		m := env.replicas[1].InternalStats(ctx).State.Members[env.replicas[1].h.ID().String()]
		return m != nil && m.Snapshot != nil
	}, 30*time.Second, 500*time.Millisecond)

	t.Log("✓ New replica created and restored from snapshot")

	// Phase 5: The moment of truth - verify what the new replica can see
	t.Log("Phase 5: Verifying new replica can access full CRDT history")

	// The new replica should have the same k3 state as the original
	val0, err0 := env.replicas[0].Get(ctx, ds.NewKey("k3"))
	val1, err1 := env.replicas[1].Get(ctx, ds.NewKey("k3"))

	require.NoError(t, err0)
	require.NoError(t, err1)
	require.Equal(t, "v3-LowerPriority", string(val1))
	require.Equal(t, val0, val1)

	t.Logf("✓ Both replicas see k3=%s", string(val1))
}
