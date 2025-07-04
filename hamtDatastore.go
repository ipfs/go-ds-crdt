package crdt

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	dag "github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs/hamt"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	ipld "github.com/ipfs/go-ipld-format"
)

// HAMTDatastore is a wrapper around a HAMT shard that implements the datastore interface
type HAMTDatastore struct {
	hamtShard  *hamt.Shard
	dagService ipld.DAGService
	ctx        context.Context
	mu         sync.RWMutex
}

// NewHAMTDatastore creates a new datastore backed by a HAMT
func NewHAMTDatastore(ctx context.Context, dagService ipld.DAGService, rootCID cid.Cid) (*HAMTDatastore, error) {
	var hamtShard *hamt.Shard
	var err error

	if rootCID == cid.Undef {
		// Create a new empty HAMT
		hamtShard, err = hamt.NewShard(dagService, 8)
	} else {
		// Load existing HAMT
		var hamtNode ipld.Node
		hamtNode, err = dagService.Get(ctx, rootCID)
		if err != nil {
			return nil, fmt.Errorf("failed to load HAMT root: %w", err)
		}

		hamtShard, err = hamt.NewHamtFromDag(dagService, hamtNode)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to initialize HAMT: %w", err)
	}

	if hamtShard == nil {
		return nil, fmt.Errorf("failed to initialize HAMT shard")
	}

	return &HAMTDatastore{
		hamtShard:  hamtShard,
		dagService: dagService,
		ctx:        ctx,
	}, nil
}

// Get implements the datastore.Read interface
func (hd *HAMTDatastore) Get(ctx context.Context, key ds.Key) ([]byte, error) {
	hd.mu.RLock()
	defer hd.mu.RUnlock()
	link, err := hd.hamtShard.Find(ctx, key.String())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ds.ErrNotFound
		}
		return nil, err
	}

	node, err := link.GetNode(ctx, hd.dagService)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve node %s: %w", link.Cid.String(), err)
	}

	switch n := node.(type) {
	case *dag.ProtoNode:
		return n.Data(), nil
	default:
		return n.RawData(), nil
	}
}

// Has implements the datastore.Read interface
func (hd *HAMTDatastore) Has(ctx context.Context, key ds.Key) (bool, error) {
	hd.mu.RLock()
	defer hd.mu.RUnlock()
	_, err := hd.hamtShard.Find(ctx, key.String())
	if err != nil {
		if err == os.ErrNotExist {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// GetSize implements the datastore.Read interface
func (hd *HAMTDatastore) GetSize(ctx context.Context, key ds.Key) (size int, err error) {
	d, err := hd.Get(ctx, key)
	return len(d), err
}

// Query implements the datastore.Read interface
func (hd *HAMTDatastore) Query(_ context.Context, q query.Query) (query.Results, error) {
	hd.mu.RLock()
	defer hd.mu.RUnlock()
	// This is a more complex operation for HAMTs
	// We need to properly iterate and filter based on the query
	node, err := hd.hamtShard.Node()
	if err != nil {
		return nil, fmt.Errorf("failed to get Node for shard: %w", err)
	}
	hamtShard, err := hamt.NewHamtFromDag(hd.dagService, node)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize HAMT shard: %w", err)
	}

	// Create and return results
	return query.ResultsWithContext(q, func(ctx context.Context, results chan<- query.Result) {

		prefix := ds.NewKey(q.Prefix)

		// Use ForEach to iterate through all entries in the HAMT
		err := hamtShard.ForEachLink(ctx, func(link *ipld.Link) error {
			k := ds.NewKey(link.Name)

			// Check if the key matches the query prefix
			if q.Prefix != "" && !k.IsDescendantOf(prefix) {
				return nil // Skip this entry
			}

			node, err := link.GetNode(ctx, hd.dagService)
			if err != nil {
				return fmt.Errorf("failed to retrieve node %s: %w", link.Cid.String(), err)
			}

			var d []byte
			switch n := node.(type) {
			case *dag.ProtoNode:
				d = n.Data()
			default:
				d = n.RawData()
			}

			// Create an entry
			entry := query.Entry{
				Key:   k.String(),
				Value: d,
				Size:  len(d),
			}

			// Send the entry to the results channel
			select {
			case results <- query.Result{Entry: entry}:
				// Successfully sent
			case <-ctx.Done():
				return ctx.Err() // Context was cancelled
			}

			return nil
		})

		// If there was an error during iteration, send it
		if err != nil {
			select {
			case results <- query.Result{Error: err}:
				// Successfully sent error
			case <-ctx.Done():
				// Context was cancelled, just return
			}
		}
	}), nil
}

// Put implements the datastore.Write interface
func (hd *HAMTDatastore) Put(ctx context.Context, key ds.Key, value []byte) error {
	hd.mu.Lock()
	defer hd.mu.Unlock()
	// Create a raw node for the value
	nd := dag.NodeWithData(value)
	if err := nd.SetCidBuilder(dag.V1CidPrefix()); err != nil {
		return fmt.Errorf("failed to set cid builder: %w", err)
	}

	// Add the node to the DAG service
	cctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	if err := hd.dagService.Add(cctx, nd); err != nil {
		return fmt.Errorf("failed to add node to DAG service: %w", err)
	}

	// Set the node in the HAMT
	if err := hd.hamtShard.Set(ctx, key.String(), nd); err != nil {
		return fmt.Errorf("failed to set value in HAMT: %w", err)
	}

	return nil
}

// Delete implements the datastore.Write interface
func (hd *HAMTDatastore) Delete(ctx context.Context, key ds.Key) error {
	hd.mu.Lock()
	defer hd.mu.Unlock()
	err := hd.hamtShard.Remove(ctx, key.String())
	if err != nil && err != os.ErrNotExist {
		return fmt.Errorf("failed to remove key from HAMT: %w", err)
	}
	return nil
}

// Sync is a no-op for HAMTDatastore
func (hd *HAMTDatastore) Sync(_ context.Context, _ ds.Key) error {
	// No-op - HAMT changes are only persisted when Root() is called
	return nil
}

// Close is a no-op for HAMTDatastore
func (hd *HAMTDatastore) Close() error {
	// No-op - nothing to close
	return nil
}

// Batch returns a new batch for this datastore
func (hd *HAMTDatastore) Batch(ctx context.Context) (ds.Batch, error) {
	return &hamtBatch{
		hd:  hd,
		ops: make(map[ds.Key]batchOp),
		ctx: ctx,
	}, nil
}

// GetRoot returns the current root CID of the HAMT
func (hd *HAMTDatastore) GetRoot(_ context.Context) (cid.Cid, error) {
	rootNode, err := hd.hamtShard.Node()
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to retrieve HAMT root node: %w", err)
	}
	return rootNode.Cid(), nil
}

// Batch implementation for HAMTDatastore
type hamtBatch struct {
	hd  *HAMTDatastore
	ops map[ds.Key]batchOp
	ctx context.Context
}

type batchOp struct {
	value  []byte
	delete bool
}

func (b *hamtBatch) Put(_ context.Context, key ds.Key, value []byte) error {
	b.ops[key] = batchOp{value: value, delete: false}
	return nil
}

func (b *hamtBatch) Delete(_ context.Context, key ds.Key) error {
	b.ops[key] = batchOp{delete: true}
	return nil
}

func (b *hamtBatch) Commit(ctx context.Context) error {
	for k, op := range b.ops {
		if op.delete {
			if err := b.hd.Delete(ctx, k); err != nil {
				return err
			}
		} else {
			if err := b.hd.Put(ctx, k, op.value); err != nil {
				return err
			}
		}
	}
	return nil
}
