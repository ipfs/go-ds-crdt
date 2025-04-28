package crdt

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"sort"
	"strings"
	"sync"

	dshelp "github.com/ipfs/boxo/datastore/dshelp"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
)

// Define flags as bit positions

type HeadFlags byte

const (
	FlagCompacted HeadFlags = 1
)

type head struct {
	flags  HeadFlags
	height uint64
}

// UnmarshalBinary decodes a head from binary data.
// First, it attempts to decode the height as a uvarint.
// If there's a leftover byte, it treats it as the flags byte.
func (h *head) UnmarshalBinary(data []byte) error {
	if len(data) == 0 {
		return errors.New("invalid data: too short")
	}

	// Decode height as uvarint
	height, n := binary.Uvarint(data)
	if n <= 0 {
		return errors.New("error decoding height")
	}
	h.height = height

	// Check for leftover byte as flags
	if len(data) > n {
		h.flags = HeadFlags(data[n])
	} else {
		// Default to zero if no flags byte exists
		h.flags = 0
	}

	return nil
}

// MarshalBinary encodes a head into binary data.
// Encodes the height as a uvarint and appends the flags byte if it's non-zero.
func (h *head) MarshalBinary() ([]byte, error) {
	// Encode height as uvarint
	buf := make([]byte, binary.MaxVarintLen64+1) // +1 for the flags byte
	n := binary.PutUvarint(buf, h.height)
	if n == 0 {
		return nil, errors.New("error encoding height")
	}

	// Append the flags byte
	if h.flags != 0 {
		buf[n] = byte(h.flags)
		n++
	}

	return buf[:n], nil
}

// heads manages the current Merkle-CRDT heads.
type heads struct {
	store ds.Datastore
	// cache contains the current contents of the store
	cache     map[cid.Cid]head
	cacheMux  sync.RWMutex
	namespace ds.Key
	logger    logging.StandardLogger
}

func newHeads(ctx context.Context, store ds.Datastore, namespace ds.Key, logger logging.StandardLogger) (*heads, error) {
	hh := &heads{
		store:     store,
		namespace: namespace,
		logger:    logger,
		cache:     make(map[cid.Cid]head),
	}
	if err := hh.primeCache(ctx); err != nil {
		return nil, err
	}
	return hh, nil
}

func (hh *heads) key(c cid.Cid) ds.Key {
	// /<namespace>/<cid>
	return hh.namespace.Child(dshelp.MultihashToDsKey(c.Hash()))
}

func (hh *heads) write(ctx context.Context, store ds.Write, c cid.Cid, h head) error {
	data, err := h.MarshalBinary()
	if err != nil {
		return err
	}
	return store.Put(ctx, hh.key(c), data)
}

func (hh *heads) delete(ctx context.Context, store ds.Write, c cid.Cid) error {
	err := store.Delete(ctx, hh.key(c))
	// The go-datastore API currently says Delete doesn't return
	// ErrNotFound, but it used to say otherwise.  Leave this
	// here to be safe.
	if err == ds.ErrNotFound {
		return nil
	}
	return err
}

// IsHead returns if a given cid is among the current heads.
func (hh *heads) IsHead(ctx context.Context, c cid.Cid) (ok bool, height uint64, err error) {
	var h head
	hh.cacheMux.RLock()
	{
		h, ok = hh.cache[c]
		if ok {
			height = h.height
		}
	}
	hh.cacheMux.RUnlock()
	return ok, height, nil
}

func (hh *heads) Len(ctx context.Context) (int, error) {
	var ret int
	hh.cacheMux.RLock()
	{
		ret = len(hh.cache)
	}
	hh.cacheMux.RUnlock()
	return ret, nil
}

// Replace replaces a head with a new cid.
func (hh *heads) Add(ctx context.Context, c cid.Cid, h head) error {
	hh.logger.Debugf("adding new DAG head: %s (height: %d, flags: %b)", c, h.height, h.flags)
	if err := hh.write(ctx, hh.store, c, h); err != nil {
		return err
	}

	hh.cacheMux.Lock()
	{
		hh.cache[c] = h
	}
	hh.cacheMux.Unlock()
	return nil
}

func (hh *heads) Replace(ctx context.Context, h, c cid.Cid, newHead head) error {
	hh.logger.Debugf("replacing DAG head: %s -> %s (new height: %d, flags: %b)", h, c, newHead.height, newHead.flags)
	var store ds.Write = hh.store

	batchingDs, batching := store.(ds.Batching)
	var err error
	if batching {
		store, err = batchingDs.Batch(ctx)
		if err != nil {
			return err
		}
	}

	err = hh.write(ctx, store, c, newHead)
	if err != nil {
		return err
	}

	hh.cacheMux.Lock()
	defer hh.cacheMux.Unlock()

	if !batching {
		hh.cache[c] = newHead
	}

	err = hh.delete(ctx, store, h)
	if err != nil {
		return err
	}
	if !batching {
		delete(hh.cache, h)
	}

	if batching {
		err := store.(ds.Batch).Commit(ctx)
		if err != nil {
			return err
		}
		delete(hh.cache, h)
		hh.cache[c] = newHead
	}
	return nil
}

// List returns the list of current heads plus the max height.
func (hh *heads) List(ctx context.Context) ([]cid.Cid, uint64, error) {
	var maxHeight uint64
	var heads []cid.Cid

	hh.cacheMux.RLock()
	{
		heads = make([]cid.Cid, 0, len(hh.cache))
		for headKey, h := range hh.cache {
			heads = append(heads, headKey)
			if h.height > maxHeight {
				maxHeight = h.height
			}
		}
	}
	hh.cacheMux.RUnlock()

	sort.Slice(heads, func(i, j int) bool {
		ci := heads[i].Bytes()
		cj := heads[j].Bytes()
		return bytes.Compare(ci, cj) < 0
	})

	return heads, maxHeight, nil
}

// primeCache builds the heads cache based on what's in storage; since
// it is called from the constructor only we don't bother locking.
func (hh *heads) primeCache(ctx context.Context) (ret error) {
	q := query.Query{
		Prefix:   hh.namespace.String(),
		KeysOnly: false,
	}

	results, err := hh.store.Query(ctx, q)
	if err != nil {
		return err
	}
	defer results.Close()

	for r := range results.Next() {
		if r.Error != nil {
			return r.Error
		}
		headKey := ds.NewKey(strings.TrimPrefix(r.Key, hh.namespace.String()))
		headCid, err := dshelp.DsKeyToCidV1(headKey, cid.DagProtobuf)
		if err != nil {
			return err
		}

		var h head
		if err := h.UnmarshalBinary(r.Value); err != nil {
			return err
		}
		hh.cache[headCid] = h
	}

	return nil
}
