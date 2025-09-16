package crdt

import (
	"context"

	ds "github.com/ipfs/go-datastore"
	ipld "github.com/ipfs/go-ipld-format"
)

// DatatstoreNamespaces carries configuration for how internal namespaces are named.
type InternalNamespaces struct {
	Heads           string
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
	return mcrdt.Datastore.publish(ctx, delta)
}

// Set returns the internal set.
func (mcrdt *MerkleCRDT) Set() Set {
	return mcrdt.set
}
