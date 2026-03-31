package crdt

import (
	"context"
	"errors"

	"github.com/ipfs/boxo/ipld/merkledag/traverse"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	ipld "github.com/ipfs/go-ipld-format"
)

// PurgeDAG removes all state associated with a named DAG: its heads, all DAG
// blocks reachable from those heads, the set entries created by those blocks,
// and processed block markers. Returns the number of DAG nodes removed.
//
// The caller must ensure no concurrent writes to this dagName during purge.
// The Datastore's background workers (rebroadcast, repair, DAG walking) also
// access heads and set state — callers should either Close() the Datastore
// first or ensure the dagName is not being actively synced.
func (mcrdt *MerkleCRDT) PurgeDAG(ctx context.Context, dagName string) (int, error) {
	deletedHeads, err := mcrdt.heads.DeleteDAG(ctx, dagName)
	if err != nil {
		return 0, err
	}
	if len(deletedHeads) == 0 {
		return 0, nil
	}

	headCIDs := make([]cid.Cid, len(deletedHeads))
	for i, h := range deletedHeads {
		headCIDs[i] = h.Cid
	}

	dagCIDSet := make(map[cid.Cid]struct{})

	// purgeKeyKind tracks which namespaces a key appeared in across the DAG's
	// deltas, so purgeKeyBlocks can skip querying namespaces that the DAG never
	// wrote to for a given key.
	type purgeKeyKind uint8
	const (
		purgeKeyElem purgeKeyKind = 1 << iota
		purgeKeyTomb
	)
	setKeys := make(map[string]purgeKeyKind)

	if err := mcrdt.TraverseWithOptions(ctx, headCIDs, func(nd ipld.Node) error {
		c := nd.Cid()
		if _, seen := dagCIDSet[c]; seen {
			return nil
		}
		dagCIDSet[c] = struct{}{}

		deltaBytes, err := extractDelta(nd)
		if err != nil {
			return err
		}
		delta := mcrdt.newDelta()
		if err := delta.Unmarshal(deltaBytes); err != nil {
			return err
		}

		elems, err := delta.GetElements()
		if err != nil {
			return err
		}
		for _, e := range elems {
			setKeys[e.GetKey()] |= purgeKeyElem
		}

		tombs, err := delta.GetTombstones()
		if err != nil {
			return err
		}
		for _, t := range tombs {
			setKeys[t.GetKey()] |= purgeKeyTomb
		}
		return nil
	}, TraverseOptions{DisableSkipDuplicates: true}); err != nil {
		return 0, err
	}

	for key, kind := range setKeys {
		if err := mcrdt.set.purgeKeyBlocks(ctx, key, dagCIDSet, kind&purgeKeyElem != 0, kind&purgeKeyTomb != 0); err != nil {
			return 0, err
		}
	}

	dagCIDs := make([]cid.Cid, 0, len(dagCIDSet))
	for c := range dagCIDSet {
		dagCIDs = append(dagCIDs, c)
	}

	var store ds.Write = mcrdt.store
	batchingDs, batching := store.(ds.Batching)
	if batching {
		store, err = batchingDs.Batch(ctx)
		if err != nil {
			return 0, err
		}
	}
	for _, c := range dagCIDs {
		if err := store.Delete(ctx, mcrdt.processedBlockKey(c)); err != nil {
			return 0, err
		}
	}
	if batching {
		if err := store.(ds.Batch).Commit(ctx); err != nil {
			return 0, err
		}
	}

	if err := mcrdt.dagService.RemoveMany(ctx, dagCIDs); err != nil {
		return 0, err
	}

	return len(dagCIDs), nil
}

// DatatstoreNamespaces carries configuration for how internal namespaces are named.
type InternalNamespaces struct {
	Heads           string
	DAGHeads        string
	Set             string
	ProcessedBlocks string
	DirtyBitKey     string
	VersionKey      string
}

type MerkleCRDTOptions struct {
	DeltaFactory func() Delta
	Namespaces   InternalNamespaces
}

// A MerkleCRDT is an advanced type to manually customize the merkle-CRDT. It
// allows submission of custom deltas and other advanced methods. If you just
// need a key-value store that conforms to the Datastore interface, use New()
// instead.
type MerkleCRDT struct {
	*Datastore
}

// NewMerkleCRDT returns a Merkle-CRDT Datastore wrapped with advanced
// methods. It allows setting internal options.
func NewMerkleCRDT(
	store ds.Datastore,
	namespace ds.Key,
	dagSyncer ipld.DAGService,
	bcast Broadcaster,
	opts *Options,
	internalOptions *MerkleCRDTOptions,
) (*MerkleCRDT, error) {
	if opts == nil {
		opts = DefaultOptions()
	}
	if internalOptions != nil {
		if df := internalOptions.DeltaFactory; df != nil {
			opts.crdtOpts.DeltaFactory = df
		}
		in := internalOptions.Namespaces
		if ns := in.Heads; ns != "" {
			opts.crdtOpts.Namespaces.Heads = ns
		}
		if ns := in.DAGHeads; ns != "" {
			opts.crdtOpts.Namespaces.DAGHeads = ns
		}
		if ns := in.Set; ns != "" {
			opts.crdtOpts.Namespaces.Set = ns
		}
		if ns := in.ProcessedBlocks; ns != "" {
			opts.crdtOpts.Namespaces.ProcessedBlocks = ns
		}
		if ns := in.DirtyBitKey; ns != "" {
			opts.crdtOpts.Namespaces.DirtyBitKey = ns
		}
		if ns := in.VersionKey; ns != "" {
			opts.crdtOpts.Namespaces.VersionKey = ns
		}
	}

	d, err := New(store, namespace, dagSyncer, bcast, opts)
	if err != nil {
		return nil, err
	}
	return &MerkleCRDT{
		Datastore: d,
	}, nil
}

// Publish allows to manually publish a Delta. The Priority is set
// automatically, upon which the delta is merged, serialized and broadcasted.
// Returns the CID of the new root node resulting from applying the delta.
func (mcrdt *MerkleCRDT) Publish(ctx context.Context, delta Delta) (Head, error) {
	return mcrdt.publish(ctx, delta)
}

// Set returns the internal set.
func (mcrdt *MerkleCRDT) Set() Set {
	return mcrdt.set
}

func (mcrdt *MerkleCRDT) Heads() Heads {
	return mcrdt.heads
}

// IsProcessed returns whether the given CID has been processed. Nodes are
// marked as processed as they are traversed during the DAG walk, so a CID
// being processed means it has been visited and merged into the set.
func (mcrdt *MerkleCRDT) IsProcessed(ctx context.Context, c cid.Cid) (bool, error) {
	return mcrdt.isProcessed(ctx, c)
}

// TraverseOptions configures the behavior of Traverse and TraverseWithOptions.
type TraverseOptions struct {
	// DisableSkipDuplicates disables the built-in duplicate tracking in the
	// traversal library. Callers that maintain their own visited set can
	// use this to avoid keeping two copies of every CID in memory.
	DisableSkipDuplicates bool
}

// Traverse visits nodes in the Merkle-CRDT tree. It skips duplicates
// and calls the visit function with the Deltas extracted from every
// node. An error results in the traversal operations being aborted.
func (mcrdt *MerkleCRDT) Traverse(ctx context.Context,
	from []cid.Cid,
	visit func(ipld.Node) error,
) error {
	return mcrdt.TraverseWithOptions(ctx, from, visit, TraverseOptions{})
}

// TraverseWithOptions is like Traverse but accepts a TraverseOptions to
// configure traversal behavior.
func (mcrdt *MerkleCRDT) TraverseWithOptions(ctx context.Context,
	from []cid.Cid,
	visit func(ipld.Node) error,
	opts TraverseOptions,
) error {
	if len(from) == 0 {
		return errors.New("no roots to traverse from")
	}

	var ignoreCid cid.Cid
	var root ipld.Node
	var err error

	tFunc := func(current traverse.State) error {
		n := current.Node
		if ignoreCid.Defined() && ignoreCid.Equals(n.Cid()) {
			return nil
		}

		return visit(n)
	}

	// this is the default. Just to be explicit.
	tErrFunc := func(err error) error {
		return err
	}

	if len(from) == 1 { // root node is the given cid
		rootCid := from[0]
		root, err = mcrdt.dagService.Get(ctx, rootCid)
		if err != nil {
			return err
		}
	} else {
		heads := make([]Head, len(from))
		for i, c := range from {
			heads[i] = Head{
				Cid: c,
			}
		}
		delta := mcrdt.newDelta()
		// we are going to make a single root node to traverse from.
		root, err = makeNode(delta, heads)
		if err != nil {
			return err
		}
		ignoreCid = root.Cid() // tFunc will skip this.
	}

	travOpts := traverse.Options{
		DAG:            mcrdt.dagService,
		Order:          traverse.DFSPre, // default
		Func:           tFunc,
		ErrFunc:        tErrFunc,
		SkipDuplicates: !opts.DisableSkipDuplicates,
	}

	return traverse.Traverse(root, travOpts)
}
