package crdt

import (
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
)

// cidToDsKey creates a Key from the given Cid.
func cidToDsKey(k cid.Cid) datastore.Key {
	return dshelp.NewKeyFromBinary(k.Bytes())
}

// DsKeyToCid converts the given Key to its corresponding Cid.
func dsKeyToCid(dsKey datastore.Key) (cid.Cid, error) {
	kb, err := dshelp.BinaryFromDsKey(dsKey)
	if err != nil {
		return cid.Cid{}, err
	}
	return cid.Cast(kb)
}
