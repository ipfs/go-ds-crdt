package crdt

import (
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
)

// blocks annotates processed blocks and provides utility
// to enable fast tree comparison.
// It is essentially a cache.
type blocks struct {
	store     ds.Datastore
	namespace ds.Key
}

func newBlocks(store ds.Datastore, namespace ds.Key) *blocks {
	return &blocks{
		store:     store,
		namespace: namespace,
	}
}

func (bb *blocks) key(c cid.Cid) ds.Key {
	return bb.namespace.Child(dshelp.CidToDsKey(c))
}

// IsKnown returns if a block has been added
func (bb *blocks) IsKnown(c cid.Cid) (bool, error) {
	return bb.store.Has(bb.key(c))
}

// Add marks a block as known
func (bb *blocks) Add(c cid.Cid) error {
	return bb.store.Put(bb.key(c), nil)
}
