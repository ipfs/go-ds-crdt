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

func TestCompactToHAMT(t *testing.T) {
	ctx := context.Background()
	store, _, _, dagService, cleanup := setupTestEnv(ctx, t)
	defer cleanup()

	const numKeys = 5000
	const batchSize = 100

	var b ds.Batch
	for i := 0; i < numKeys; i++ {
		k := fmt.Sprintf("key-%d", i)
		v := fmt.Sprintf("value-%d", i)
		if i%batchSize == 0 {
			if b != nil {
				require.NoError(t, b.Commit(ctx), "batch commit failed")
			}
			b, _ = store.Batch(ctx)
		}
		require.NoError(t, b.Put(ctx, ds.NewKey(k), []byte(v)), "failed to put key-value")
	}
	require.NoError(t, b.Commit(ctx), "final batch commit failed")

	rootCID, err := store.CompactToHAMT(ctx)
	require.NoError(t, err, "compaction failed")

	r := PrintSnapshot(ctx, dagService, rootCID)
	for i := 0; i < numKeys; i++ {
		k := fmt.Sprintf("/key-%d", i)
		v := fmt.Sprintf("value-%d", i)
		require.Equal(t, v, r[k], fmt.Sprintf("key %s has incorrect value", k))
	}
}

func TestRemoveDeltaDAG(t *testing.T) {
	ctx := context.Background()
	store, h, bs, _, cleanup := setupTestEnv(ctx, t)
	defer cleanup()

	const numDeltas = 10
	var (
		rootCID cid.Cid
		err     error
	)

	for i := 0; i < numDeltas; i++ {
		k := fmt.Sprintf("key-%d", i)
		v := fmt.Sprintf("value-%d", i)
		require.NoError(t, store.Put(ctx, ds.NewKey(k), []byte(v)), "failed to put key-value")

		if i == 4 {
			m, ok := store.InternalStats().State.Members[h.ID().String()]
			require.True(t, ok, "our peerid should exist in the state")
			lastHead := m.DagHeads[len(m.DagHeads)-1]
			_, rootCID, err = cid.CidFromBytes(lastHead.Cid)
			require.NoError(t, err, "failed to parse CID")
		}
	}

	require.NotNil(t, rootCID, "rootCID should not be nil")
	require.NoError(t, store.RemoveDeltaDAG(ctx, bs, rootCID), "failed to remove deltas")
}

func TestCompactToHAMTAndTruncateDeltaDag(t *testing.T) {
	ctx := context.Background()
	store, h, bs, dagService, cleanup := setupTestEnv(ctx, t)
	defer cleanup()

	const numKeys = 502
	const compactEvery = 100
	var maxID int
	var ss cid.Cid

	for i := 1; i < numKeys; i++ {
		k := fmt.Sprintf("key-%d", i)
		v := fmt.Sprintf("value-%d", i)
		require.NoError(t, store.Put(ctx, ds.NewKey(k), []byte(v)), "failed to put key-value")
		if i%compactEvery == 0 {
			m, ok := store.InternalStats().State.Members[h.ID().String()]
			require.True(t, ok, "our peerid should exist in the state")
			lastHead := m.DagHeads[len(m.DagHeads)-1]
			_, hcid, err := cid.CidFromBytes(lastHead.Cid)
			require.NoError(t, err, "failed to parse CID")

			ss, err = store.CompactToHAMT(ctx)
			require.NoError(t, err, "compaction failed")
			err = store.RemoveDeltaDAG(ctx, bs, hcid)
			if err != nil {
				require.NoError(t, err, "failed to truncate delta DAG")
			}

			maxID = i
		}
	}

	store.PrintDAG()
	r := PrintSnapshot(ctx, dagService, ss)
	for i := 1; i <= maxID; i++ {
		k := fmt.Sprintf("/key-%d", i)
		v := fmt.Sprintf("value-%d", i)
		require.Equal(t, v, r[k], fmt.Sprintf("key %s has incorrect value", k))
	}
}

func PrintSnapshot(ctx context.Context, dagService format.DAGService, rootCID cid.Cid) map[string]string {
	hamNode, err := dagService.Get(ctx, rootCID)
	if err != nil {
		log.Fatalf("failed to get HAMT node: %v", err)
	}

	hamShard, err := hamt.NewHamtFromDag(dagService, hamNode)
	if err != nil {
		log.Fatalf("failed to load HAMT shard: %v", err)
	}

	r, _ := PrintShardContent(ctx, hamShard, dagService)
	return r
}

func PrintShardContent(ctx context.Context, shard *hamt.Shard, getter format.NodeGetter) (map[string]string, error) {
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
