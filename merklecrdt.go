package crdt

import (
	"context"
	"errors"

	"github.com/ipfs/boxo/ipld/merkledag/traverse"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	ipld "github.com/ipfs/go-ipld-format"
)

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

// Publish allows to manually publish a Delta. The Priority is set automatically, upon which the delta is merged, serialized and broadcasted.
func (mcrdt *MerkleCRDT) Publish(ctx context.Context, delta Delta) error {
	return mcrdt.publish(ctx, delta)
}

// Set returns the internal set.
func (mcrdt *MerkleCRDT) Set() Set {
	return mcrdt.set
}

func (mcrdt *MerkleCRDT) Heads() Heads {
	return mcrdt.heads
}

// Traverse visits nodes in the Merkle-CRDT tree. It skips duplicates
// and calls the visit function with the Deltas extracted from every
// node. An error results in the traversal operations being aborted.
func (mcrdt *MerkleCRDT) Traverse(ctx context.Context,
	from []cid.Cid,
	visit func(ipld.Node) error,
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

	opts := traverse.Options{
		DAG:            mcrdt.dagService,
		Order:          traverse.DFSPre, // default
		Func:           tFunc,
		ErrFunc:        tErrFunc,
		SkipDuplicates: true,
	}

	return traverse.Traverse(root, opts)
}
