package crdt_test

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"

	ipfslite "github.com/hsanjuan/ipfs-lite"
	bserv "github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange/offline"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs/hamt"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	crdt "github.com/ipfs/go-ds-crdt"
	format "github.com/ipfs/go-ipld-format"
	"github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func setupTestEnv(ctx context.Context, t *testing.T) (*crdt.Datastore, host.Host, blockstore.Blockstore, format.DAGService, func()) {
	t.Helper()

	memStore := dssync.MutexWrap(ds.NewMapDatastore())
	bs := blockstore.NewBlockstore(memStore)
	ex := offline.Exchange(bs)
	bserv := bserv.New(bs, ex)
	dagService := dag.NewDAGService(bserv)

	pk, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 1)
	require.NoError(t, err, "failed to generate key pair")

	listen, _ := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/45000")
	h, dht, err := ipfslite.SetupLibp2p(ctx, pk, nil, []multiaddr.Multiaddr{listen}, nil, ipfslite.Libp2pOptionsExtra...)
	require.NoError(t, err, "failed to set up libp2p")

	ps, err := pubsub.NewGossipSub(ctx, h)
	require.NoError(t, err, "failed to create pubsub")

	broadcaster, _ := crdt.NewPubSubBroadcaster(ctx, ps, "test-topic")

	opts := crdt.DefaultOptions()
	store, err := crdt.New(h, memStore, ds.NewKey("/test"), dagService, broadcaster, opts)
	require.NoError(t, err, "failed to create CRDT datastore")

	cleanup := func() {
		h.Close()
		dht.Close()
	}

	return store, h, bs, dagService, cleanup
}

func TestCompactAndTruncateDeltaDAG(t *testing.T) {
	ctx := context.Background()
	store, h, bs, dagService, cleanup := setupTestEnv(ctx, t)
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
			snapshotCID, err = store.CompactAndTruncate(ctx, bs, headCID, lastCompactedCid)
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
	require.Equal(t, crdt.DAGNodeInfo{
		Additions: map[string][]byte{
			"/key-502": []byte("value-502"),
		},
		Tombstones: nil,
	}, dagContent[502])
	require.Equal(t, crdt.DAGNodeInfo{
		Additions: map[string][]byte{
			"/key-501": []byte("value-501"),
		},
		Tombstones: nil,
	}, dagContent[501])
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
