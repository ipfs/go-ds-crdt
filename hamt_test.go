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
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"

	"github.com/libp2p/go-libp2p-pubsub"
)

func TestCompactToHAMT(t *testing.T) {
	// Context for the operations
	ctx := context.Background()

	// In-memory datastore for testing
	memStore := ds.NewMapDatastore()

	bs := blockstore.NewBlockstore(memStore)
	ex := offline.Exchange(bs)
	bserv := bserv.New(bs, ex)

	// Mock DAG service
	dagService := dag.NewDAGService(bserv)

	pk, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 1)
	if err != nil {
		log.Fatal(err)
	}

	listen, _ := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/45000")

	h, dht, err := ipfslite.SetupLibp2p(
		ctx,
		pk,
		nil,
		[]multiaddr.Multiaddr{listen},
		nil,
		ipfslite.Libp2pOptionsExtra...,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer h.Close()
	defer dht.Close()

	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		log.Fatal(err)
	}
	broadcaster, _ := crdt.NewPubSubBroadcaster(ctx, ps, "test-topic")

	// Options for the CRDT datastore
	opts := crdt.DefaultOptions()

	// Initialize the CRDT Datastore
	namespace := ds.NewKey("/test")
	store, err := crdt.New(h, memStore, namespace, dagService, broadcaster, opts)
	if err != nil {
		log.Fatalf("Error initializing CRDT datastore: %v", err)
	}

	// Populate the CRDT Datastore with key-value pairs
	const numKeys = 5000 // A large number to ensure shard splitting
	const batchSize = 100
	var (
		b ds.Batch
	)
	for i := 0; i < numKeys; i++ {
		k := fmt.Sprintf("key-%d", i)
		v := fmt.Sprintf("value-%d", i)
		if i%batchSize == 0 {
			if b != nil {
				err := b.Commit(ctx)
				if err != nil {
					log.Fatal(err)
				}
			}
			b, err = store.Batch(ctx)
			if err != nil {
				log.Fatal(err)
			}
		}
		err = b.Put(ctx, ds.NewKey(k), []byte(v))
		if err != nil {
			log.Fatalf("Error adding key %s: %v", k, err)
		}
	}
	err = b.Commit(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Trigger compaction
	rootCID, err := store.CompactToHAMT(ctx)
	if err != nil {
		log.Fatalf("Error during compaction: %v", err)
	}

	r := PrintSnapshot(ctx, dagService, rootCID)
	require.NoError(t, err)

	for i := 0; i < numKeys; i++ {
		k := fmt.Sprintf("/key-%d", i)
		v := fmt.Sprintf("value-%d", i)

		mv, ok := r[k]
		require.True(t, ok)
		require.Equal(t, v, mv)
	}

}

func PrintSnapshot(ctx context.Context, dagService format.DAGService, rootCID cid.Cid) map[string]string {
	// Fetch the HAMT snapshot and verify its contents
	hamtNode, err := dagService.Get(ctx, rootCID)
	if err != nil {
		log.Fatalf("Error retrieving HAMT node: %v", err)
	}

	fmt.Printf("HAMT Root CID: %s\n", rootCID)
	fmt.Printf("HAMT Node: %+v\n", hamtNode)

	// TODO: Add detailed verification of the HAMT contents
	// Traverse and print HAMT contents
	hamtShard, err := hamt.NewHamtFromDag(dagService, hamtNode)
	if err != nil {
		log.Fatalf("Error loading HAMT shard: %v", err)
	}

	r, err := PrintShardContent(ctx, hamtShard, dagService)
	return r
}

func PrintShardContent(ctx context.Context, shard *hamt.Shard, getter format.NodeGetter) (map[string]string, error) {
	result := map[string]string{}
	var mu sync.Mutex

	err := shard.ForEachLink(ctx, func(link *format.Link) error {
		fmt.Printf("Name: %s, Cid: %s, Size: %d\n", link.Name, link.Cid.String(), link.Size)

		// Check if this is a sub-shard or a value node
		node, err := link.GetNode(ctx, getter)
		if err != nil {
			log.Printf("Error retrieving node %s: %v", link.Cid.String(), err)
			return nil
		}

		pn, ok := node.(*dag.ProtoNode)
		if !ok {
			return fmt.Errorf("unknown node type '%T'", node)
		}

		// Safely update the result map
		mu.Lock()
		result[link.Name] = string(pn.Data())
		mu.Unlock()

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error traversing shard: %w", err)
	}
	return result, nil
}

func TestRemoveDeltaDAG(t *testing.T) {
	// Context for the operations
	ctx := context.Background()

	// In-memory datastore for testing
	memStore := dssync.MutexWrap(ds.NewMapDatastore())

	bs := blockstore.NewBlockstore(memStore)
	ex := offline.Exchange(bs)
	bserv := bserv.New(bs, ex)

	// Mock DAG service
	dagService := dag.NewDAGService(bserv)

	pk, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 1)
	if err != nil {
		log.Fatal(err)
	}

	listen, _ := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/45000")

	h, dht, err := ipfslite.SetupLibp2p(
		ctx,
		pk,
		nil,
		[]multiaddr.Multiaddr{listen},
		nil,
		ipfslite.Libp2pOptionsExtra...,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer h.Close()
	defer dht.Close()

	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		log.Fatal(err)
	}
	broadcaster, _ := crdt.NewPubSubBroadcaster(ctx, ps, "test-topic")

	// Options for the CRDT datastore
	opts := crdt.DefaultOptions()

	// Initialize the CRDT Datastore
	namespace := ds.NewKey("/test")
	store, err := crdt.New(h, memStore, namespace, dagService, broadcaster, opts)
	if err != nil {
		log.Fatalf("Error initializing CRDT datastore: %v", err)
	}

	// Build a delta DAG 10 transactions deep
	var heads []cid.Cid
	var rootCID cid.Cid

	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("key-%d", i)
		v := fmt.Sprintf("value-%d", i)
		// Create a new delta
		err = store.Put(ctx, ds.NewKey(k), []byte(v))

		// Update the heads
		for _, h := range store.State().GetState().Members[h.ID().String()].DagHeads {
			_, c, err := cid.CidFromBytes(h.Cid)
			require.NoError(t, err)
			heads = append(heads, c)
		}

		if i == 4 {
			rootCID = heads[len(heads)-1] // Save the CID of the 5th transaction
		}
	}

	require.NotNil(t, rootCID, "5th transaction CID should not be nil")

	// Verify all nodes exist in the DAG
	for i, head := range heads {
		_, err := dagService.Get(ctx, head)
		require.NoError(t, err, "node %d should exist in the DAG", i)
	}

	// Perform cleanup from the 5th transaction to the start
	err = store.RemoveDeltaDAG(ctx, rootCID)
	require.NoError(t, err)

	// Verify nodes from the 5th transaction to the start are removed
	for i := 0; i <= 4; i++ {
		_, err := dagService.Get(ctx, heads[i])
		require.Error(t, err, "node %d should be removed from the DAG", i)
	}

	// Verify nodes from the 6th transaction onward still exist
	for i := 5; i < 10; i++ {
		_, err := dagService.Get(ctx, heads[i])
		require.NoError(t, err, "node %d should still exist in the DAG", i)
	}
}

func TestRemoveEntireDeltaDAG(t *testing.T) {
	// Context for the operations
	ctx := context.Background()

	// In-memory datastore for testing

	memStore := dssync.MutexWrap(ds.NewMapDatastore())

	bs := blockstore.NewBlockstore(memStore)
	ex := offline.Exchange(bs)
	bserv := bserv.New(bs, ex)

	// Mock DAG service
	dagService := dag.NewDAGService(bserv)

	pk, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 1)
	if err != nil {
		log.Fatal(err)
	}

	listen, _ := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/45000")

	h, dht, err := ipfslite.SetupLibp2p(
		ctx,
		pk,
		nil,
		[]multiaddr.Multiaddr{listen},
		nil,
		ipfslite.Libp2pOptionsExtra...,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer h.Close()
	defer dht.Close()

	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		log.Fatal(err)
	}
	broadcaster, _ := crdt.NewPubSubBroadcaster(ctx, ps, "test-topic")

	// Options for the CRDT datastore
	opts := crdt.DefaultOptions()

	// Initialize the CRDT Datastore
	namespace := ds.NewKey("/test")
	store, err := crdt.New(h, memStore, namespace, dagService, broadcaster, opts)
	if err != nil {
		log.Fatalf("Error initializing CRDT datastore: %v", err)
	}

	// Build a delta DAG 10 transactions deep
	var heads []cid.Cid
	var rootCID cid.Cid

	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("key-%d", i)
		v := fmt.Sprintf("value-%d", i)
		// Create a new delta
		err = store.Put(ctx, ds.NewKey(k), []byte(v))

		// Update the heads
		for _, h := range store.State().GetState().Members[h.ID().String()].DagHeads {
			_, c, err := cid.CidFromBytes(h.Cid)
			require.NoError(t, err)
			heads = append(heads, c)
		}
	}

	rootCID = heads[len(heads)-1] // Save the CID of the last transaction

	require.NotNil(t, rootCID, "5th transaction CID should not be nil")

	// Verify all nodes exist in the DAG
	for i, head := range heads {
		_, err := dagService.Get(ctx, head)
		require.NoError(t, err, "node %d should exist in the DAG", i)
	}

	// Perform cleanup from the 5th transaction to the start
	err = store.RemoveDeltaDAG(ctx, rootCID)
	require.NoError(t, err)

	// Verify all nodes are removed
	for i := 0; i < 10; i++ {
		_, err := dagService.Get(ctx, heads[i])
		require.Error(t, err, "node %d should be removed from the DAG", i)
	}
	_ = memStore
	///test/s/s/key-3/CIQHVXP46D6XGS7NQLCBEBBFGPX4E5EOFCIYX4ALKUMZFEYJ3Q35OSA
}

func TestCompactToHAMTAndTruncateDeltaDag(t *testing.T) {
	// Context for the operations
	ctx := context.Background()

	// In-memory datastore for testing
	memStore := dssync.MutexWrap(ds.NewMapDatastore())

	bs := blockstore.NewBlockstore(memStore)
	ex := offline.Exchange(bs)
	bserv := bserv.New(bs, ex)

	// Mock DAG service
	dagService := dag.NewDAGService(bserv)

	pk, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 1)
	if err != nil {
		log.Fatal(err)
	}

	listen, _ := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/45000")

	h, dht, err := ipfslite.SetupLibp2p(
		ctx,
		pk,
		nil,
		[]multiaddr.Multiaddr{listen},
		nil,
		ipfslite.Libp2pOptionsExtra...,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer h.Close()
	defer dht.Close()

	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		log.Fatal(err)
	}
	broadcaster, _ := crdt.NewPubSubBroadcaster(ctx, ps, "test-topic")

	// Options for the CRDT datastore
	opts := crdt.DefaultOptions()

	// Initialize the CRDT Datastore
	namespace := ds.NewKey("/test")
	store, err := crdt.New(h, memStore, namespace, dagService, broadcaster, opts)
	if err != nil {
		log.Fatalf("Error initializing CRDT datastore: %v", err)
	}

	// Populate the CRDT Datastore with key-value pairs
	const numKeys = 502
	const compactEvery = 100
	var maxId int

	var ss cid.Cid

	for i := 0; i < numKeys; i++ {
		k := fmt.Sprintf("key-%d", i)
		v := fmt.Sprintf("value-%d", i)
		if i%compactEvery == 0 {
			// get the current head
			m, ok := store.State().GetState().Members[h.ID().String()]
			if ok {
				heads := m.DagHeads
				head := heads[len(heads)-1]
				_, hcid, err := cid.CidFromBytes(head.Cid)
				require.NoError(t, err)
				ss, err = store.CompactToHAMT(ctx)
				require.NoError(t, err)
				err = store.RemoveDeltaDAG(ctx, hcid)
				maxId = i
			}
		}
		err = store.Put(ctx, ds.NewKey(k), []byte(v))
		if err != nil {
			log.Fatalf("Error adding key %s: %v", k, err)
		}
	}

	store.PrintDAG()

	r := PrintSnapshot(ctx, dagService, ss)
	require.NoError(t, err)

	for i := 0; i < maxId; i++ {
		k := fmt.Sprintf("/key-%d", i)
		v := fmt.Sprintf("value-%d", i)

		mv, ok := r[k]
		require.True(t, ok, fmt.Sprintf("key %s should exist", k))
		require.Equal(t, v, mv)
	}
}
