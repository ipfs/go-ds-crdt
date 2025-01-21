package crdt

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockstore"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	mdutils "github.com/ipfs/boxo/ipld/merkledag/test"
	"github.com/ipfs/boxo/ipld/unixfs/hamt"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	format "github.com/ipfs/go-ipld-format"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func setupTestEnv(t *testing.T) (*Datastore, Peer, blockstore.Blockstore, format.DAGService, func()) {
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
	return store, h, bs.Blockstore(), dagserv, cleanup
}

func TestCompactAndTruncateDeltaDAG(t *testing.T) {
	ctx := context.Background()
	store, h, bs, dagService, cleanup := setupTestEnv(t)
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
	r := ExtractSnapshot(ctx, dagService, snapshotCID)
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
	dagContent, err := store.ExtractDAGContent(bs)
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
	replicas, closeReplicas := makeNReplicas(t, 2, nil)
	defer closeReplicas()

	ctx := context.Background()

	// Initially, both replicas are disconnected

	br0 := replicas[0].broadcaster.(*mockBroadcaster)
	br0.dropProb.Store(101)

	br1 := replicas[1].broadcaster.(*mockBroadcaster)
	br1.dropProb.Store(101)

	k := ds.NewKey("k1")

	// Put key in replica 0
	err := replicas[0].Put(ctx, k, []byte("v1"))
	if err != nil {
		t.Fatal(err)
	}
	err = replicas[0].Put(ctx, k, []byte("v2"))
	if err != nil {
		t.Fatal(err)
	}

	m, ok := replicas[0].InternalStats().State.Members[replicas[0].h.ID().String()]
	require.True(t, ok, "our peerid should exist in the state")
	lastHead := m.DagHeads[len(m.DagHeads)-1]
	_, headCID, err := cid.CidFromBytes(lastHead.Cid)
	require.NoError(t, err, "failed to parse CID")

	// Create snapshot
	snapshotCid, err := replicas[0].CompactAndTruncate(ctx, headCID, cid.Cid{})
	require.NoError(t, err, "compaction and truncation failed")

	// Key should not exist in replica 1 at this point, since replicas are disconnected
	_, err = replicas[1].Get(ctx, k)
	require.ErrorIs(t, err, ds.ErrNotFound)

	// Restore snapshot into replica 1
	replicas[1].restoreSnapshot(replicas[1].dagService, snapshotCid, headCID, 2)

	// Key now exists in replica 1
	val, err := replicas[1].Get(ctx, k)
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), val)

	// Delete key from replica 1
	err = replicas[1].Delete(ctx, k)
	if err != nil {
		t.Fatal(err)
	}

	// At this point, before allowing the replicas to sync, the key should not be found in replica 1
	_, err = replicas[1].Get(ctx, k)
	require.ErrorIs(t, err, ds.ErrNotFound)

	// Allow replicas to sync
	br0.dropProb.Store(0)
	br1.dropProb.Store(0)

	time.Sleep(10 * time.Second)

	// The key should not be found in any replica

	_, err = replicas[1].Get(ctx, k)
	require.ErrorIs(t, err, ds.ErrNotFound)

	_, err = replicas[0].Get(ctx, k)
	require.ErrorIs(t, err, ds.ErrNotFound)

	closeReplicas()
}

func TestCompactionWithMultipleHeads(t *testing.T) {
	ctx := context.Background()
	store, _, _, dagService, cleanup := setupTestEnv(t)
	defer cleanup()

	var eg errgroup.Group

	// Run multiple concurrent Put's. This should trigger the creation of multiple heads.
	for i := 1; i < 10; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		eg.Go(func() error {
			err := store.Put(ctx, ds.NewKey(key), value)
			if err != nil {
				return fmt.Errorf("put error: %w", err)
			}
			return nil
		})
	}

	eg.Wait()

	m, ok := store.InternalStats().State.Members[store.h.ID().String()]
	require.True(t, ok, "our peerid should exist in the state")
	lastHead := m.DagHeads[len(m.DagHeads)-1]
	_, headCID, err := cid.CidFromBytes(lastHead.Cid)
	require.NoError(t, err, "failed to parse CID")

	// Create snapshot

	// TODO (fix me): We currently don't have a way to create a snapshot from multiple heads.
	// As a placeholder, we are creating the snapshot only for the last head CID in our heads list.

	snapshotCid, err := store.CompactAndTruncate(ctx, headCID, cid.Cid{})
	require.NoError(t, err, "compaction and truncation failed")

	snapshotContents := ExtractSnapshot(ctx, dagService, snapshotCid)

	// Assert that all keys are present in snapshot

	for i := 1; i < 10; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		require.Contains(t, snapshotContents, key)
		require.Equal(t, value, snapshotContents[key])
	}
}

func ExtractSnapshot(ctx context.Context, dagService format.DAGService, rootCID cid.Cid) map[string]string {
	hamNode, err := dagService.Get(ctx, rootCID)
	if err != nil {
		log.Fatalf("failed to get HAMT node: %v", err)
	}

	hamShard, err := hamt.NewHamtFromDag(dagService, hamNode)
	if err != nil {
		log.Fatalf("failed to load HAMT shard: %v", err)
	}

	r, _ := ExtractShardData(ctx, hamShard, dagService)
	return r
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
