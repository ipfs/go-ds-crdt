package crdt

import (
	"bytes"
	"encoding/binary"
	"errors"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	logging "github.com/ipfs/go-log/v2"
	"sort"
	"strings"
	"sync"
)

// heads manages the current Merkle-CRDT heads.
type heads struct {
	store ds.Datastore
	// cache contains the current contents of the store
	cache     map[cid.Cid]uint64
	cacheMux  sync.RWMutex
	namespace ds.Key
	logger    logging.StandardLogger
}

func newHeads(store ds.Datastore, namespace ds.Key, logger logging.StandardLogger) (*heads, error) {
	hh := &heads{
		store:     store,
		namespace: namespace,
		logger:    logger,
		cache:     make(map[cid.Cid]uint64),
	}
	if err := hh.primeCache(); err != nil {
		return nil, err
	}
	return hh, nil
}

func (hh *heads) key(c cid.Cid) ds.Key {
	// /<namespace>/<cid>
	return hh.namespace.Child(dshelp.MultihashToDsKey(c.Hash()))
}

func canonicalizeCid(c cid.Cid) (cid.Cid, error) {
	// Can we no-op this if c is already a V1 PB CID?
	key := dshelp.MultihashToDsKey(c.Hash())
	return dshelp.DsKeyToCidV1(key, cid.DagProtobuf)
}

// TODO unused, remove?
func (hh *heads) load(c cid.Cid) (uint64, error) {
	v, err := hh.store.Get(hh.key(c))
	if err != nil {
		return 0, err
	}
	height, n := binary.Uvarint(v)
	if n <= 0 {
		return 0, errors.New("error decoding height")
	}
	return height, nil
}

func (hh *heads) write(store ds.Write, c cid.Cid, height uint64) error {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, height)
	if n == 0 {
		return errors.New("error encoding height")
	}
	return store.Put(hh.key(c), buf[0:n])
}

func (hh *heads) delete(store ds.Write, c cid.Cid) error {
	err := store.Delete(hh.key(c))
	// The go-datastore API currently says Delete doesn't return
	// ErrNotFound, but it used to say otherwise.  Leave this
	// here to be safe.
	if err == ds.ErrNotFound {
		return nil
	}
	return err
}

// IsHead returns if a given cid is among the current heads.
func (hh *heads) IsHead(c cid.Cid) (bool, uint64, error) {
	c, err := canonicalizeCid(c)
	if err != nil {
		return false, 0, err
	}

	hh.cacheMux.RLock()
	defer hh.cacheMux.RUnlock()
	height, ok := hh.cache[c]
	return ok, height, nil
}

func (hh *heads) Len() (int, error) {
	hh.cacheMux.RLock()
	defer hh.cacheMux.RUnlock()
	return len(hh.cache), nil
}

// Replace replaces a head with a new cid.
func (hh *heads) Replace(h, c cid.Cid, height uint64) error {
	c, err := canonicalizeCid(c)
	if err != nil {
		return err
	}
	h, err = canonicalizeCid(h)
	if err != nil {
		return err
	}

	hh.logger.Infof("replacing DAG head: %s -> %s (new height: %d)", h, c, height)
	var store ds.Write = hh.store

	batchingDs, batching := store.(ds.Batching)
	if batching {
		store, err = batchingDs.Batch()
		if err != nil {
			return err
		}
	}

	err = hh.write(store, c, height)
	if err != nil {
		return err
	}

	hh.cacheMux.Lock()
	defer hh.cacheMux.Unlock()

	if !batching {
		hh.cache[c] = height
	}

	err = hh.delete(store, h)
	if err != nil {
		return err
	}
	if !batching {
		delete(hh.cache, h)
	}

	if batching {
		err := store.(ds.Batch).Commit()
		if err != nil {
			return err
		}
		delete(hh.cache, h)
		hh.cache[c] = height
	}
	return nil
}

func (hh *heads) Add(c cid.Cid, height uint64) error {
	c, err := canonicalizeCid(c)
	if err != nil {
		return err
	}
	hh.logger.Infof("adding new DAG head: %s (height: %d)", c, height)
	if err := hh.write(hh.store, c, height); err != nil {
		return err
	}

	hh.cacheMux.Lock()
	defer hh.cacheMux.Unlock()
	hh.cache[c] = height
	return nil
}

// List returns the list of current heads plus the max height.
func (hh *heads) List() ([]cid.Cid, uint64, error) {
	hh.cacheMux.RLock()
	defer hh.cacheMux.RUnlock()

	var maxHeight uint64
	heads := make([]cid.Cid, 0, len(hh.cache))
	for head, height := range hh.cache {
		heads = append(heads, head)
		if height > maxHeight {
			maxHeight = height
		}
	}

	sort.Slice(heads, func(i, j int) bool {
		ci := heads[i].Bytes()
		cj := heads[j].Bytes()
		return bytes.Compare(ci, cj) < 0
	})

	return heads, maxHeight, nil
}

func (hh *heads) primeCache() (ret error) {
	q := query.Query{
		Prefix:   hh.namespace.String(),
		KeysOnly: false,
	}

	results, err := hh.store.Query(q)
	if err != nil {
		return err
	}
	defer func() {
		if results != nil {
			results.Close()
		}
	}()

	hh.cacheMux.Lock()
	defer hh.cacheMux.Unlock()
	for r := range results.Next() {
		if r.Error != nil {
			return r.Error
		}
		headKey := ds.NewKey(strings.TrimPrefix(r.Key, hh.namespace.String()))
		headCid, err := dshelp.DsKeyToCidV1(headKey, cid.DagProtobuf)
		if err != nil {
			return err
		}
		height, n := binary.Uvarint(r.Value)
		if n <= 0 {
			return errors.New("error decoding height")
		}
		hh.cache[headCid] = height
	}

	err = results.Close()
	results = nil
	return err
}
