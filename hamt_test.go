package crdt_test

import (
	"context"
	"fmt"
	"log"
	"testing"

	ipfslite "github.com/hsanjuan/ipfs-lite"
	bserv "github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange/offline"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs/hamt"
	ds "github.com/ipfs/go-datastore"
	crdt "github.com/ipfs/go-ds-crdt"
	format "github.com/ipfs/go-ipld-format"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/multiformats/go-multiaddr"

	"github.com/libp2p/go-libp2p-pubsub"
)

func Test(t *testing.T) {
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
	const batchSize = 100
	var (
		b ds.Batch
	)
	for i := 0; i < 1000; i++ {
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

	PrintShardContent(ctx, hamtShard, dagService)
}

func PrintShardContent(ctx context.Context, shard *hamt.Shard, getter format.NodeGetter) {
	err := shard.ForEachLink(ctx, func(link *format.Link) error {
		fmt.Printf("Name: %s, Cid: %s, Size: %d\n", link.Name, link.Cid.String(), link.Size)

		// Check if this is a sub-shard or a value node
		node, err := link.GetNode(ctx, getter)
		if err != nil {
			log.Printf("Error retrieving node %s: %v", link.Cid.String(), err)
			return nil
		}

		// Print the raw data if it's a value node
		fmt.Printf("Data: %s\n", string(node.RawData()))

		return nil
	})

	if err != nil {
		log.Fatalf("Error traversing shard: %v", err)
	}
}
