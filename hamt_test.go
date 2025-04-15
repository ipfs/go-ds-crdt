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
	"github.com/ipfs/boxo/ipld/unixfs/hamt"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	format "github.com/ipfs/go-ipld-format"
	log "github.com/ipfs/go-log/v2"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func setupTestEnv(t *testing.T) (*Datastore, Peer, format.DAGService, func()) {
	t.Helper()
	memStore := makeStore(t, 0)
	bs := mdutils.Bserv()
	dagserv := dag.NewDAGService(bs)
	dagService := &mockDAGSvc{DAGService: dagserv, bs: bs.Blockstore()}
	broadcasters, cancel := newBroadcasters(t, 1)
	broadcaster := broadcasters[0]
	h := newMockPeer("test-peer")

	opts := DefaultOptions()
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
	for i := range env.broadcasters {
		env.broadcasters[i].chans = env.replicaBcastChans
	}

	if debug {
		log.SetLogLevel("crdt", "debug")
	}
}

func (env *testEnv) Cleanup() {
	env.cancelBcasts()
	for i, r := range env.replicas {
		err := r.Close()
		if err != nil {
			env.t.Error(err)
		}
		os.RemoveAll(storeFolder(i))
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

func TestCompactAndTruncateDeltaDAG(t *testing.T) {
	ctx := context.Background()
	store, h, dagService, cleanup := setupTestEnv(t)
	defer cleanup()

	const (
		numKeys      = 502
		compactEvery = 100
	)
	var (
		maxID            int
		snapshotCID      cid.Cid
		lastCompactedCid cid.Cid
	)
	for i := 1; i <= numKeys; i++ {
		k := fmt.Sprintf("key-%d", i)
		v := fmt.Sprintf("value-%d", i)
		require.NoError(t, store.Put(ctx, ds.NewKey(k), []byte(v)), "failed to put key-value")

		if i%compactEvery == 0 {
			m, ok := store.InternalStats(ctx).State.Members[h.ID().String()]
			require.True(t, ok, "our peerid should exist in the state")
			lastHead := m.DagHeads[len(m.DagHeads)-1]
			_, headCID, err := cid.CidFromBytes(lastHead.Cid)
			require.NoError(t, err, "failed to parse CID")

			// Perform compaction and truncation in one step
			snapshotCID, err = store.compactAndTruncate(ctx, headCID, lastCompactedCid)
			require.NoError(t, err, "compaction and truncation failed")

			maxID = i
			lastCompactedCid = headCID
		}
	}

	// Verify the snapshot in the HAMT
	r, err := ExtractSnapshot(t, ctx, dagService, snapshotCID)
	require.NoError(t, err)
	for i := 1; i <= maxID; i++ {
		k := fmt.Sprintf("/key-%d", i)
		v := fmt.Sprintf("value-%d", i)
		require.Equal(t, v, r[k], fmt.Sprintf("key %s has incorrect value", k))
	}

	// Ensure that the head walks back and only contains the expected keys
	//m, ok := store.InternalStats().State.Members[h.ID().String()]
	//require.True(t, ok, "our peerid should exist in the state")
	//lastHead := m.DagHeads[len(m.DagHeads)-1]

	// Step 2: Perform compaction and truncation
	heads := store.InternalStats(ctx).Heads
	require.NotEmpty(t, heads, "DAG heads should not be empty")

	// Step 3: Extract DAG content after compaction
	dagContent, err := store.ExtractDAGContent(ctx, store.bs)
	require.NoError(t, err, "failed to extract DAG content")

	// Step 4: Validate DAG has been truncated (only 1 or 2 nodes should remain)
	require.Len(t, dagContent, 2, "DAG should contain only the snapshot and latest delta")

	// Step 5: Validate the remaining deltas
	require.Equal(t, DAGNodeInfo{
		Additions: map[string][]byte{
			"/key-502": []byte("value-502"),
		},
		Tombstones: nil,
	}, dagContent[502])
	require.Equal(t, DAGNodeInfo{
		Additions: map[string][]byte{
			"/key-501": []byte("value-501"),
		},
		Tombstones: nil,
	}, dagContent[501])
}

func TestCRDTRemoveConvergesAfterRestoringSnapshot(t *testing.T) {
	// Step 1: Set up replicas with compaction options enabled
	opts := DefaultOptions()
	opts.CompactDagSize = 50     // Ensure compaction occurs when the DAG grows beyond 50 nodes
	opts.CompactRetainNodes = 10 // Retain the last 10 nodes after compaction
	opts.CompactInterval = 5 * time.Second

	replicas, closeReplicas := makeNReplicas(t, 2, opts)
	defer closeReplicas()

	ctx := context.Background()

	// Step 2: Simulate a network partition by preventing message exchange
	br0 := replicas[0].broadcaster.(*mockBroadcaster)
	br0.dropProb.Store(101)

	br1 := replicas[1].broadcaster.(*mockBroadcaster)
	br1.dropProb.Store(101)

	k := ds.NewKey("k1")
	k2 := ds.NewKey("k2")

	// Step 3: Modify `k1` multiple times to simulate realistic updates
	require.NoError(t, replicas[0].Put(ctx, k, []byte("v1")))
	require.NoError(t, replicas[0].Put(ctx, k, []byte("v2")))

	// Step 4: Populate Replica 0 with a significant number of key updates to ensure compaction
	for i := 1; i <= 60; i++ { // 60 operations to ensure we pass the compaction threshold
		key := ds.NewKey(fmt.Sprintf("key-%d", i))
		require.NoError(t, replicas[0].Put(ctx, key, []byte(fmt.Sprintf("value-%d", i))))
	}

	// Ensure at least one more key exists before compaction
	require.NoError(t, replicas[0].Put(ctx, k2, []byte("v1")))

	// Step 5: Wait for compaction to trigger automatically
	require.Eventually(t, func() bool {
		return replicas[0].InternalStats(ctx).State.Snapshot != nil &&
			replicas[0].InternalStats(ctx).State.Snapshot.SnapshotKey != nil
	}, 1*time.Minute, 500*time.Millisecond, "Replica 0 should have created a snapshot")

	// Fetch the snapshot that was created
	s := replicas[0].InternalStats(ctx).State
	require.NotNil(t, s.Snapshot, "Snapshot should have been triggered")

	// Step 6: Restore the snapshot by allowing synchronization (happens automatically)
	br0.dropProb.Store(0)
	br1.dropProb.Store(0)

	// Wait for snapshot to be present in replica 1
	require.Eventually(t, func() bool {
		return replicas[1].InternalStats(ctx).State.Snapshot != nil &&
			replicas[1].InternalStats(ctx).State.Snapshot.SnapshotKey != nil
	}, 1*time.Minute, 500*time.Millisecond, "Replica 1 should have gotten a snapshot")

	// Step 7: Verify that key `k1` now exists in Replica 1 with the last known value
	requireReplicaHasKey(t, ctx, replicas[1], k, []byte("v2"))

	// Step 8: Delete `k1` in Replica 1
	require.NoError(t, replicas[1].Delete(ctx, k))

	// Step 9: Allow synchronization and verify deletion in both replicas
	time.Sleep(10 * time.Second)

	_, err := replicas[1].Get(ctx, k)
	require.ErrorIs(t, err, ds.ErrNotFound, "Key should not exist in Replica 1")

	requireReplicaDoesNotHaveKey(t, ctx, replicas[0], k, "Key should not exist in Replica 0")

	closeReplicas()
}

func TestCompactionWithMultipleHeads(t *testing.T) {
	ctx := context.Background()

	// 1) Configure compaction so it triggers automatically
	o := DefaultOptions()
	o.CompactDagSize = 50
	o.CompactRetainNodes = 10
	o.CompactInterval = 5 * time.Second

	// 2) Create 2 replicas
	replicas, closeReplicas := makeNReplicas(t, 2, o)
	defer closeReplicas()

	r0 := replicas[0]
	r1 := replicas[1]

	// 3) Perform a large concurrent load across both replicas
	var eg errgroup.Group
	eg.Go(func() error {
		for i := 1; i < 400; i++ {
			key := fmt.Sprintf("key-%d", i)
			value := []byte(fmt.Sprintf("value-%d", i))
			r := replicas[i%len(replicas)]
			if err := r.Put(ctx, ds.NewKey(key), value); err != nil {
				return err
			}
		}
		return nil
	})
	eg.Go(func() error {
		for i := 400; i < 999; i++ {
			key := fmt.Sprintf("key-%d", i)
			value := []byte(fmt.Sprintf("value-%d", i))
			r := replicas[i%len(replicas)]
			if err := r.Put(ctx, ds.NewKey(key), value); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, eg.Wait(), "concurrent puts failed")

	// 4) Wait for the replicas to converge on the large load
	//    We give them a decent window to broadcast & process deltas
	require.Eventually(t, func() bool {
		// We’ll do a quick heuristic: both replicas have > 500 heads or none.
		// In reality, you might want a more robust check, e.g. verifying a few random keys exist in both replicas.
		heads0 := r0.InternalStats(ctx).Heads
		heads1 := r1.InternalStats(ctx).Heads
		return len(heads0) > 0 && len(heads1) > 0
	}, 30*time.Second, 1*time.Second, "replicas should converge on some consistent heads")

	// 5) Insert the final key to ensure compaction is triggered
	i := 1000
	keyFinal := fmt.Sprintf("key-%d", i)
	valueFinal := []byte(fmt.Sprintf("value-%d", i))
	rFinal := replicas[i%len(replicas)]
	require.NoError(t, rFinal.Put(ctx, ds.NewKey(keyFinal), valueFinal))

	// 6) Wait for automatic compaction on r0
	require.Eventually(t, func() bool {
		return r0.InternalStats(ctx).State.Snapshot != nil &&
			r0.InternalStats(ctx).State.Snapshot.SnapshotKey != nil
	}, 1*time.Minute, 500*time.Millisecond, "replica 0 should have created a snapshot")

	// 7) Verify the snapshot. We do so on r0, but you could also confirm that r1 sees it eventually.
	snapshotCidBytes := r0.InternalStats(ctx).State.Snapshot.SnapshotKey.Cid
	require.NotEmpty(t, snapshotCidBytes, "Snapshot Key must be valid")

	snapshotCid := cid.MustParse(snapshotCidBytes)
	snapshotContents, err := ExtractSnapshot(t, ctx, r0.dagService, snapshotCid)
	require.NoError(t, err)

	// 8) We check a subset of the keys (e.g., 1..500) are present.
	//    Because compaction can happen at various times, a higher range (e.g. up to 999)
	//    might be missing keys. But at least the first 500 we expect to appear if compaction
	//    triggered after the concurrency load was stable.
	for i := 1; i < 500; i++ {
		expectedKey := fmt.Sprintf("/key-%d", i)
		_, exists := snapshotContents[expectedKey]
		require.True(t, exists, "Expected key %s to be in the snapshot", expectedKey)
	}
}

func ExtractSnapshot(t *testing.T, ctx context.Context, dagService format.DAGService, rootCID cid.Cid) (map[string]string, error) {
	hamNode, err := dagService.Get(ctx, rootCID)
	require.NoError(t, err, "failed to get HAMT node")

	hamShard, err := hamt.NewHamtFromDag(dagService, hamNode)
	require.NoError(t, err, "failed to load HAMT shard")

	return ExtractShardData(ctx, hamShard, dagService)
}

func ExtractShardData(ctx context.Context, shard *hamt.Shard, getter format.NodeGetter) (map[string]string, error) {
	result := map[string]string{}
	var mu sync.Mutex

	err := shard.ForEachLink(ctx, func(link *format.Link) error {
		node, err := link.GetNode(ctx, getter)
		if err != nil {
			return fmt.Errorf("failed to retrieve node %s: %w", link.Cid, err)
		}

		pn, ok := node.(*dag.ProtoNode)
		if !ok {
			return fmt.Errorf("unknown node type '%T'", node)
		}

		mu.Lock()
		result[link.Name] = string(pn.Data())
		mu.Unlock()
		return nil
	})
	return result, err
}

func TestSnapShotRestore(t *testing.T) {
	ctx := context.Background()

	// 1. Set up replicas with compaction options.
	opts := DefaultOptions()
	opts.CompactDagSize = 50     // force snapshot creation after enough writes
	opts.CompactRetainNodes = 10 // keep the DAG from growing too large
	opts.CompactInterval = 5 * time.Second

	replicas, closeReplicas := makeNReplicas(t, 2, opts)
	defer closeReplicas()

	// 2. Optional: Simulate network partition so that only replica 0 receives writes initially.
	br0 := replicas[0].broadcaster.(*mockBroadcaster)
	br0.dropProb.Store(101)
	br1 := replicas[1].broadcaster.(*mockBroadcaster)
	br1.dropProb.Store(101)

	// 3. Write enough keys to exceed CompactDagSize, triggering automatic compaction on replica 0.
	for i := 1; i <= 60; i++ {
		key := ds.NewKey(fmt.Sprintf("k%d", i))
		require.NoError(t, replicas[0].Put(ctx, key, []byte("v1")))
	}

	// 4. Wait until replica 0 forms a snapshot automatically.
	require.Eventually(t, func() bool {
		snap := replicas[0].InternalStats(ctx).State.Snapshot
		return snap != nil && snap.SnapshotKey != nil
	}, 15*time.Second, 500*time.Millisecond, "Replica 0 should create a snapshot")

	// 5. Reconnect the replicas so that the snapshot (and any deltas) propagate.
	br0.dropProb.Store(0)
	br1.dropProb.Store(0)

	// 6. Wait for replica 1 to receive the snapshot.
	//    We'll check if replica 1 has the same snapshot ID as replica 0 or
	//    if it sees the same keys that replica 0 wrote.
	require.Eventually(t, func() bool {
		// quick check: does replica 1 see any snapshot yet?
		return replicas[1].InternalStats(ctx).State.Snapshot != nil
	}, 15*time.Second, 500*time.Millisecond, "Replica 1 should receive the snapshot")

	// 7. At this point, both replicas should converge on the same data set from replica 0’s snapshot.
	//    Optionally verify a specific key.
	_, err := replicas[1].Get(ctx, ds.NewKey("k50"))
	require.NoError(t, err, "Replica 1 must see the data from the snapshot")

	// 8. Perform new writes on replica 0, ensuring they replicate to replica 1.
	newKey := ds.NewKey("kNew")
	require.NoError(t, replicas[0].Put(ctx, newKey, []byte("newVal")))

	// 9. Wait for synchronization of that new key on replica 1.
	require.Eventually(t, func() bool {
		val, err := replicas[1].Get(ctx, newKey)
		return err == nil && string(val) == "newVal"
	}, 10*time.Second, 500*time.Millisecond, "Replica 1 should receive the new key")

	// Both replicas now share the same snapshot + new changes.
}

func TestTriggerSnapshot(t *testing.T) {
	ctx := context.Background()
	o := DefaultOptions()
	o.CompactDagSize = 50
	o.CompactRetainNodes = 10
	o.CompactInterval = 5 * time.Second

	replicas, closeReplicas := makeNReplicas(t, 1, o)
	defer closeReplicas()

	for i := 1; i < 101; i++ {
		k := ds.NewKey(fmt.Sprintf("k%d", i))
		replicas[0].Put(ctx, k, []byte("v1"))
	}

	require.Eventually(t, func() bool {
		return replicas[0].InternalStats(ctx).State.Snapshot != nil
	}, 15*time.Second, 500*time.Millisecond)
}

func TestSnapshotAllowsLowerPriorityValueToResurface(t *testing.T) {
	ctx := context.Background()
	o := DefaultOptions()
	o.CompactDagSize = 7
	o.CompactRetainNodes = 2
	o.CompactInterval = 5 * time.Second

	env := createTestEnv(t)
	defer env.Cleanup()

	env.AddReplica(o)
	env.AddReplica(o)

	// DAG node A
	require.NoError(t, env.replicas[0].Put(ctx, ds.NewKey("k1"), []byte("v1")))

	// Wait until replica 1 also sees node A

	requireReplicaHasKey(t, ctx, env.replicas[1], ds.NewKey("k1"), []byte("v1"))

	// All peers should have a DAG height of 1 and only 1 head (key k1)
	for i := 0; i < 2; i++ {
		require.Equal(t, uint64(1), env.replicas[i].InternalStats(ctx).MaxHeight)
		require.Equal(t, 1, len(env.replicas[i].InternalStats(ctx).Heads))
	}

	br0 := env.replicas[0].broadcaster.(*mockBroadcaster)
	br1 := env.replicas[1].broadcaster.(*mockBroadcaster)

	// Turn off communication between both peers
	br0.dropProb.Store(101)
	br1.dropProb.Store(101)

	require.NoError(t, env.replicas[0].Put(ctx, ds.NewKey("k2"), []byte("v2")))                 // DAG node C
	require.NoError(t, env.replicas[0].Put(ctx, ds.NewKey("k3"), []byte("v3-GreaterPriority"))) // DAG node E

	require.NoError(t, env.replicas[1].Put(ctx, ds.NewKey("k3"), []byte("v3-LowerPriority"))) // DAG node B
	require.NoError(t, env.replicas[1].Put(ctx, ds.NewKey("k4"), []byte("v4")))               // DAG node D

	// Assert that replica 0 knows about value v3-GreaterPriority
	v, err := env.replicas[0].Get(ctx, ds.NewKey("k3"))
	require.NoError(t, err)
	require.Equal(t, []byte("v3-GreaterPriority"), v)

	// Assert that replica 1 knows about value v3-LowerPriority
	v, err = env.replicas[1].Get(ctx, ds.NewKey("k3"))
	require.NoError(t, err)
	require.Equal(t, []byte("v3-LowerPriority"), v)

	// Partially turn on communication, only from replica 0 to replica 1
	br0.dropProb.Store(0)
	br1.dropProb.Store(101)

	// Now replica 1 should see the value v3-GreaterPriority for k3, received from replica 0
	requireReplicaHasKey(t, ctx, env.replicas[1], ds.NewKey("k3"), []byte("v3-GreaterPriority"))

	// Turn off all communication again
	br0.dropProb.Store(101)

	time.Sleep(100 * time.Millisecond)

	// Delete k3 from replica 0; this should create a removal delta for the v3-GreaterPriority value only
	// (and not v3-LowerPriority, since replica 0 doesn't have DAG node B yet)
	env.replicas[0].Delete(ctx, ds.NewKey("k3"))

	// Now replica 0 should see no value for k3
	_, err = env.replicas[0].Get(ctx, ds.NewKey("k3"))
	require.Error(t, err, ds.ErrNotFound)

	// Replica 1 should still see v3-GreaterPriority, since it has not received the removal from replica 0 yet
	v, err = env.replicas[1].Get(ctx, ds.NewKey("k3"))
	require.NoError(t, err)
	require.Equal(t, []byte("v3-GreaterPriority"), v)

	// Add some more nodes to replica 0's DAG (node I)
	require.NoError(t, env.replicas[0].Put(ctx, ds.NewKey("somekey-1"), []byte("somevalue-1")))

	// Add some more nodes to replica 1's DAG (nodes F, H and J)
	require.NoError(t, env.replicas[1].Put(ctx, ds.NewKey("somekey-2"), []byte("somevalue-2")))
	require.NoError(t, env.replicas[1].Put(ctx, ds.NewKey("somekey-3"), []byte("somevalue-3")))
	require.NoError(t, env.replicas[1].Put(ctx, ds.NewKey("somekey-4"), []byte("somevalue-4")))

	// Turn on all communication
	br0.dropProb.Store(0)
	br1.dropProb.Store(0)

	// Wait for DAGs in both peers to converge
	requireReplicaHasKey(t, ctx, env.replicas[1], ds.NewKey("k3"), []byte("v3-LowerPriority"))
	requireReplicaHasKey(t, ctx, env.replicas[0], ds.NewKey("somekey-4"), []byte("somevalue-4"))

	// Add a final key to the top of the DAG
	require.NoError(t, env.replicas[0].Put(ctx, ds.NewKey("final-key"), []byte("final-value")))
	requireReplicaHasKey(t, ctx, env.replicas[1], ds.NewKey("final-key"), []byte("final-value"))

	// Wait for both DAGs to converge, at which point both should have a height of 7.
	require.Equal(t, uint64(7), env.replicas[0].InternalStats(ctx).MaxHeight)
	require.Equal(t, uint64(7), env.replicas[1].InternalStats(ctx).MaxHeight)

	// Wait for snapshot to be triggered
	require.Eventually(t, func() bool {
		return env.replicas[0].InternalStats(ctx).State.Snapshot != nil
	}, 25*time.Second, 500*time.Millisecond, "Replica 0 should generate a snapshot")

	// Since both replicas have converged, both should see the value v3-LowerPriority for k3,
	// because v3-GreaterPriority was removed by replica 0.
	requireReplicaHasKey(t, ctx, env.replicas[0], ds.NewKey("k3"), []byte("v3-LowerPriority"))
	requireReplicaHasKey(t, ctx, env.replicas[1], ds.NewKey("k3"), []byte("v3-LowerPriority"))

	// Now we add a new replica, wait for it to restore its state from the snapshot and verify
	// that it correctly contains v3-LowerPriority.

	// Add replica 2
	env.AddReplica(o)

	// Wait for it to get snapshot.
	require.Eventually(t, func() bool {
		return env.replicas[2].InternalStats(ctx).State.Snapshot != nil
	}, 25*time.Second, 500*time.Millisecond, "Replica 2 should eventually restore snapshot")

	// Wait for it to see the value v3-LowerPriority for k3
	requireReplicaHasKey(t, ctx, env.replicas[2], ds.NewKey("k3"), []byte("v3-LowerPriority"))
}
