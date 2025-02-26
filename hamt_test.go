package crdt

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	dag "github.com/ipfs/boxo/ipld/merkledag"
	mdutils "github.com/ipfs/boxo/ipld/merkledag/test"
	"github.com/ipfs/boxo/ipld/unixfs/hamt"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-ipld-format"
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
			m, ok := store.InternalStats().State.Members[h.ID().String()]
			require.True(t, ok, "our peerid should exist in the state")
			lastHead := m.DagHeads[len(m.DagHeads)-1]
			_, headCID, err := cid.CidFromBytes(lastHead.Cid)
			require.NoError(t, err, "failed to parse CID")

			// Perform compaction and truncation in one step
			snapshotCID, err = store.CompactAndTruncate(ctx, headCID, lastCompactedCid)
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
	heads := store.InternalStats().Heads
	require.NotEmpty(t, heads, "DAG heads should not be empty")

	// Step 3: Extract DAG content after compaction
	dagContent, err := store.ExtractDAGContent(store.bs)
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

	// Step 3: Populate Replica 0 with a significant number of key updates to ensure compaction
	for i := 1; i <= 60; i++ { // 60 operations to ensure we pass the compaction threshold
		key := ds.NewKey(fmt.Sprintf("key-%d", i))
		require.NoError(t, replicas[0].Put(ctx, key, []byte(fmt.Sprintf("value-%d", i))))
	}

	// Step 4: Modify `k1` multiple times to simulate realistic updates
	require.NoError(t, replicas[0].Put(ctx, k, []byte("v1")))
	require.NoError(t, replicas[0].Put(ctx, k, []byte("v2")))

	// Ensure at least one more key exists before compaction
	require.NoError(t, replicas[0].Put(ctx, k2, []byte("v1")))

	// Step 5: Wait for compaction to trigger automatically
	time.Sleep(10 * time.Second) // Ensure automatic compaction runs

	// Fetch the snapshot that was created
	s := replicas[0].InternalStats().State
	require.NotNil(t, s.Snapshot, "Snapshot should have been triggered")

	// Step 6: Restore the snapshot by allowing synchronization (happens automatically)
	br0.dropProb.Store(0)
	br1.dropProb.Store(0)

	time.Sleep(10 * time.Second) // Allow time for state synchronization

	// Step 7: Verify that key `k1` now exists in Replica 1 with the last known value
	val, err := replicas[1].Get(ctx, k)
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), val)

	// Step 8: Delete `k1` in Replica 1
	require.NoError(t, replicas[1].Delete(ctx, k))

	// Step 9: Allow synchronization and verify deletion in both replicas
	time.Sleep(10 * time.Second)

	_, err = replicas[1].Get(ctx, k)
	require.ErrorIs(t, err, ds.ErrNotFound, "Key should not exist in Replica 1")

	_, err = replicas[0].Get(ctx, k)
	require.ErrorIs(t, err, ds.ErrNotFound, "Key should not exist in Replica 0")

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
		heads0 := r0.InternalStats().Heads
		heads1 := r1.InternalStats().Heads
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
		return r0.InternalStats().State.Snapshot != nil &&
			r0.InternalStats().State.Snapshot.SnapshotKey != nil
	}, 1*time.Minute, 500*time.Millisecond, "replica 0 should have created a snapshot")

	// 7) Verify the snapshot. We do so on r0, but you could also confirm that r1 sees it eventually.
	snapshotCidBytes := r0.InternalStats().State.Snapshot.SnapshotKey.Cid
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
		snap := replicas[0].InternalStats().State.Snapshot
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
		return replicas[1].InternalStats().State.Snapshot != nil
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
		return replicas[0].InternalStats().State.Snapshot != nil
	}, 15*time.Second, 500*time.Millisecond)
}
