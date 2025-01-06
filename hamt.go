package crdt

import (
	"context"
	"fmt"

	dag "github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs/hamt"
	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore/query"
	ipld "github.com/ipfs/go-ipld-format"
)

// Load or create a new HAMT shard
func loadOrCreateHAMT(ctx context.Context, dagService ipld.DAGService, rootCID cid.Cid) (*hamt.Shard, error) {
	if rootCID.Defined() {
		// Load existing HAMT shard
		rootNode, err := dagService.Get(ctx, rootCID)
		if err != nil {
			return nil, err
		}
		return hamt.NewHamtFromDag(dagService, rootNode)
	}
	// Create a new HAMT shard
	return hamt.NewShard(dagService, 8)
}

func (store *Datastore) CompactToHAMT(ctx context.Context) (cid.Cid, error) {
	// Retrieve the existing root CID if available
	var rootCID cid.Cid
	rootData, err := store.store.Get(ctx, store.namespace.ChildString(snapshotKey))
	if err == nil {
		rootCID, _ = cid.Cast(rootData)
	}

	// Load or create the HAMT shard
	hamtShard, err := loadOrCreateHAMT(ctx, store.dagService, rootCID)
	if err != nil {
		return cid.Undef, err
	}

	// Iterate over all keys and values in the datastore
	results, err := store.set.Elements(ctx, query.Query{})
	if err != nil {
		return cid.Undef, err
	}

	for result := range results.Next() {
		if result.Error != nil {
			return cid.Undef, result.Error
		}

		// Convert the value into an IPLD node
		node, err := valueToNode(ctx, store.dagService, result.Entry.Value)
		if err != nil {
			return cid.Undef, err
		}

		// Add the key-node pair to the HAMT shard
		err = hamtShard.Set(ctx, result.Entry.Key, node)
		if err != nil {
			return cid.Undef, err
		}
	}

	// Retrieve the root IPLD node of the HAMT
	rootNode, err := hamtShard.Node()
	if err != nil {
		return cid.Undef, err
	}

	// Save the new root CID
	newRootCID := rootNode.Cid()
	err = store.store.Put(ctx, store.namespace.ChildString("snapshot"), newRootCID.Bytes())
	if err != nil {
		return cid.Undef, err
	}

	// Clear the delta log
	store.curDeltaMux.Lock()
	store.curDelta = nil
	store.curDeltaMux.Unlock()

	return newRootCID, nil
}

func valueToNode(ctx context.Context, dagService ipld.DAGService, value []byte) (ipld.Node, error) {
	node := dag.NodeWithData(value)
	err := node.SetCidBuilder(dag.V1CidPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to set cid builder: %w", err)
	}
	if err := dagService.Add(ctx, node); err != nil {
		return nil, err
	}
	return node, nil
}
