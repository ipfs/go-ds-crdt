package crdt

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/exchange/offline"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs/hamt"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-ds-crdt/pb"
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

func (store *Datastore) compactAndTruncate(ctx context.Context, startCID, endCID cid.Cid) (cid.Cid, error) {
	// Step 1: Set up a DAG service with offline exchange
	offlineDAG := dag.NewDAGService(blockservice.New(store.bs, offline.Exchange(store.bs)))
	ng := &crdtNodeGetter{NodeGetter: offlineDAG}

	// Step 2: Collect all deltas using the offline DAG service
	var allDeltas []*pb.Delta
	queue := []cid.Cid{startCID}
	visited := make(map[cid.Cid]struct{})

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if endCID.Defined() && current == endCID {
			continue
		}

		if _, ok := visited[current]; ok {
			continue
		}
		visited[current] = struct{}{}

		// Fetch the IPLD node using the offline DAG service
		node, delta, err := ng.GetDelta(ctx, current)
		if err != nil {
			if isOfflineBlockNotFoundError(err) {
				return cid.Undef, fmt.Errorf("block %s not found locally: %w", current, err)
			}
			return cid.Undef, fmt.Errorf("failed to retrieve DAG node %s: %w", current, err)
		}

		allDeltas = append(allDeltas, delta)

		// Queue child nodes for traversal
		for _, link := range node.Links() {
			queue = append(queue, link.Cid)
		}
	}

	// Step 3: Sort deltas by priority
	sort.SliceStable(allDeltas, func(i, j int) bool {
		return allDeltas[i].Priority < allDeltas[j].Priority
	})

	// Step 4: Apply deltas to the HAMT
	// Retrieve the existing root CID if available
	var rootCID cid.Cid
	rootData, err := store.store.Get(ctx, store.namespace.ChildString(snapshotKey))
	if err == nil {
		rootCID, _ = cid.Cast(rootData)
	}
	hamtShard, err := loadOrCreateHAMT(ctx, store.dagService, rootCID)
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to initialize HAMT: %w", err)
	}

	type resolvedEntry struct {
		value    []byte
		priority uint64
		id       string
	}
	resolvedState := make(map[string]resolvedEntry)
	tombstoned := make(map[string]resolvedEntry)

	for _, delta := range allDeltas {
		for _, elem := range delta.Elements {
			key := elem.Key
			existing, exists := resolvedState[key]
			if !exists || delta.Priority > existing.priority || (delta.Priority == existing.priority && elem.Id > existing.id) {
				resolvedState[key] = resolvedEntry{
					value:    elem.Value,
					priority: delta.Priority,
					id:       elem.Id,
				}
			}
		}

		for _, tomb := range delta.Tombstones {
			key := tomb.Key
			existing, exists := resolvedState[key]
			if exists {
				if delta.Priority > existing.priority || (delta.Priority == existing.priority && tomb.Id > existing.id) {
					delete(resolvedState, key)
					tombstoned[key] = resolvedEntry{
						priority: delta.Priority,
						id:       tomb.Id,
					}
				}
			} else {
				tombstoned[key] = resolvedEntry{
					priority: delta.Priority,
					id:       tomb.Id,
				}
			}
		}
	}

	// Step 5: Apply resolved state to the HAMT
	for key, entry := range resolvedState {
		node, err := valueToNode(ctx, store.dagService, entry.value)
		if err != nil {
			return cid.Undef, fmt.Errorf("failed to convert value to node: %w", err)
		}
		err = hamtShard.Set(ctx, key, node)
		if err != nil {
			return cid.Undef, fmt.Errorf("failed to insert key %s into HAMT: %w", key, err)
		}
	}

	for key := range tombstoned {
		err := hamtShard.Remove(ctx, key)
		if err != nil && !ipld.IsNotFound(err) {
			return cid.Undef, fmt.Errorf("failed to remove key %s from HAMT: %w", key, err)
		}
	}

	// Step 6: Finalize the HAMT snapshot
	rootNode, err := hamtShard.Node()
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to retrieve HAMT root node: %w", err)
	}
	newRootCID := rootNode.Cid()

	err = store.store.Put(ctx, store.namespace.ChildString(snapshotKey), newRootCID.Bytes())
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to save snapshot CID: %w", err)
	}

	// Step 7: Truncate the old DAG
	err = store.truncateDAG(ctx, startCID, endCID)
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to truncate DAG: %w", err)
	}

	return newRootCID, nil
}

// Helper function to check for offline fetch errors
func isOfflineBlockNotFoundError(err error) bool {
	return strings.Contains(err.Error(), "block was not found locally (offline)")
}

// Truncate the DAG from startCID to endCID using an offline DAG service
func (store *Datastore) truncateDAG(ctx context.Context, startCID, endCID cid.Cid) error {
	// Initialize a DAG service with an offline exchange
	offlineDAG := dag.NewDAGService(blockservice.New(store.bs, offline.Exchange(store.bs)))

	queue := []cid.Cid{startCID}
	visited := make(map[cid.Cid]struct{})

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		// Stop if we've reached the endCID (for partial truncation)
		if endCID.Defined() && current == endCID {
			continue
		}

		if _, ok := visited[current]; ok {
			continue
		}
		visited[current] = struct{}{}

		// Fetch the IPLD node locally using the offline DAG service
		node, err := offlineDAG.Get(ctx, current)
		if err != nil {
			if isOfflineBlockNotFoundError(err) {
				// Block is already gone, continue cleanup
				continue
			}
			return fmt.Errorf("failed to retrieve DAG node %s: %w", current, err)
		}

		// Step 1: Queue child nodes for further deletion
		for _, link := range node.Links() {
			queue = append(queue, link.Cid)
		}

		// Step 2: Clean up from the DAG service
		err = store.dagService.Remove(ctx, current)
		if err != nil {
			return fmt.Errorf("failed to remove DAG node %s: %w", current, err)
		}
	}

	return nil
}
