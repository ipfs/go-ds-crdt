package crdt

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"

	pb "github.com/ipfs/go-ds-crdt/pb"

	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
)

var (
	elemsNs        = "s" // /elements namespace /set/s/<key>/<block>
	tombsNs        = "t" // /tombstones namespace /set/t/<key>/<block>
	keysNs         = "k" // /keys namespace /set/k/<key>/{v,p}
	valueSuffix    = "v" // for /keys namespace
	prioritySuffix = "p"
)

// set implements an Add-Wins Observed-Remove Set using delta-CRDTs
// (https://arxiv.org/abs/1410.2803) and backing all the data in a
// go-datastore. It is fully agnostic to MerkleCRDTs or the delta distribution
// layer.  It chooses the Value with most priority for a Key as the current
// Value. When two values have the same priority, it chooses by alphabetically
// sorting their unique IDs alphabetically.
type set struct {
	store     ds.Datastore
	namespace ds.Key
}

func newCRDTSet(d ds.Datastore, namespace ds.Key) *set {
	return &set{
		namespace: namespace,
		store:     d,
	}
}

// Add returns a new delta-set adding the given key/value.
func (s *set) Add(key string, value []byte) *pb.Delta {
	return &pb.Delta{
		Elements: []*pb.Element{
			&pb.Element{
				Key:   key,
				Value: value,
			},
		},
		Tombstones: nil,
	}
}

// Rmv returns a new delta-set removing the given key.
func (s *set) Rmv(key string) (*pb.Delta, error) {
	delta := &pb.Delta{}

	// /namespace/<key>/elements
	prefix := s.elemsPrefix(key)
	q := query.Query{
		Prefix:   prefix.String(),
		KeysOnly: true,
	}

	results, err := s.store.Query(q)
	if err != nil {
		return nil, err
	}
	defer results.Close()

	for r := range results.Next() {
		if r.Error != nil {
			return delta, err
		}

		id := strings.TrimPrefix(r.Key, prefix.String())
		delta.Tombstones = append(delta.Tombstones, &pb.Element{
			Key: key,
			Id:  id,
		})
	}
	return delta, nil
}

// Element retrieves the value of an element from the CRDT set.
func (s *set) Element(key string) ([]byte, error) {
	// We can only GET an element if it's part of the Set (in
	// "elemements" and not in "tombstones").

	// As an optimization:
	// * If the key has a value in the store it means:
	//   -> It occurs at least once in "elems"
	//   -> It may or not be tombstoned
	// * If the key does not have a value in the store:
	//   -> It was either never added
	//   -> Or it was tombstoned
	//   -> In both cases the element "does not exist".

	valueK := s.valueKey(key)
	value, err := s.store.Get(valueK)
	if err != nil { // not found is fine
		return value, err
	}

	// We have an existing element. Check if tombstoned.
	inSet, err := s.InSet(key)
	if err != nil {
		return nil, err
	}
	if !inSet {
		// attempt to remove so next time we do not have to do this
		// lookup.
		s.store.Delete(valueK)
		return nil, ds.ErrNotFound
	}
	// otherwise return the value
	return value, nil
}

type filterIsKey struct {
}

func (f *filterIsKey) Filter(e query.Entry) bool {
	dsk := ds.NewKey(e.Key)
	return ds.NewKey(valueSuffix).IsAncestorOf(dsk.Reverse())
}

// Elements returns all the elements in the set.
// It comes handy to use query.Result to wrap key, value and error.
func (s *set) Elements() <-chan query.Result {
	prefix := s.keyPrefix(keysNs).String()
	q := query.Query{
		Prefix:   prefix,
		KeysOnly: true,
		Filters: []query.Filter{
			&filterIsKey{},
		},
	}

	retChan := make(chan query.Result, 1)
	go func() {
		defer close(retChan)
		results, err := s.store.Query(q)
		if err != nil {
			retChan <- query.Result{Error: err}
			return
		}
		defer results.Close()

		for r := range results.Next() {
			if r.Error != nil {
				retChan <- query.Result{Error: err}
				return
			}

			// /namespace/keys/<key>/v -> <key>
			// If our filter worked well we should have only
			// got good keys like that.
			key := strings.TrimSuffix(
				strings.TrimPrefix(r.Key, prefix),
				"/"+valueSuffix,
			)

			value, err := s.Element(key)
			if err == ds.ErrNotFound {
				continue
			}
			if err != nil {
				retChan <- query.Result{Error: err}
				return
			}
			entry := query.Entry{
				Key:   key,
				Value: value,
			}

			retChan <- query.Result{Entry: entry}
		}
	}()
	return retChan
}

// InSet returns true if the key belongs to one of the elements in the "elems"
// set, and this element is not tombstoned.
func (s *set) InSet(key string) (bool, error) {
	// /namespace/elems/<key>
	prefix := s.elemsPrefix(key)
	q := query.Query{
		Prefix:   prefix.String(),
		KeysOnly: true,
	}

	results, err := s.store.Query(q)
	if err != nil {
		return false, err
	}
	defer results.Close()

	// range all the /namespace/elems/<key>/<block_cid>.
	for r := range results.Next() {
		if r.Error != nil {
			return false, err
		}

		id := strings.TrimPrefix(r.Key, prefix.String())
		// if not tombstoned, we have it
		inTomb, err := s.inTombsKeyID(key, id)
		if err != nil {
			return false, err
		}
		if !inTomb {
			return true, nil
		}
	}
	return false, nil
}

// /namespace/<key>
func (s *set) keyPrefix(key string) ds.Key {
	return s.namespace.ChildString(key)
}

// /namespace/elems/<key>
func (s *set) elemsPrefix(key string) ds.Key {
	return s.keyPrefix(elemsNs).ChildString(key)
}

// /namespace/tombs/<key>
func (s *set) tombsPrefix(key string) ds.Key {
	return s.keyPrefix(tombsNs).ChildString(key)
}

// /namespace/keys/<key>/value
func (s *set) valueKey(key string) ds.Key {
	return s.keyPrefix(keysNs).ChildString(key).ChildString(valueSuffix)
}

// /namespace/keys/<key>/priority
func (s *set) priorityKey(key string) ds.Key {
	return s.keyPrefix(keysNs).ChildString(key).ChildString(prioritySuffix)
}

func (s *set) getPriority(key string) (uint64, error) {
	prioK := s.priorityKey(key)
	data, err := s.store.Get(prioK)
	if err != nil {
		if err == ds.ErrNotFound {
			return 0, nil
		}
		return 0, err
	}

	prio, n := binary.Uvarint(data)
	if n <= 0 {
		return prio, errors.New("error decoding priority")
	}
	return prio - 1, nil
}

func (s *set) setPriority(key string, prio uint64) error {
	prioK := s.priorityKey(key)
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, prio+1)
	if n == 0 {
		return errors.New("error encoding priority")
	}

	return s.store.Put(prioK, buf[0:n])
}

// sets a value if priority is higher. When equal, it sets if the
// value is lexicographically higher than the current value.
func (s *set) setValue(key string, value []byte, prio uint64) error {
	curPrio, err := s.getPriority(key)
	if err != nil {
		return err
	}

	if prio < curPrio {
		return nil
	}
	valueK := s.valueKey(key)

	if prio == curPrio {
		curValue, _ := s.store.Get(valueK)
		// new value greater than old
		if bytes.Compare(curValue, value) >= 0 {
			return nil
		}
	}

	// store value
	err = s.store.Put(valueK, value)
	if err != nil {
		return err
	}

	// store priority
	return s.setPriority(key, prio)
}

// putElems adds items to the "elems" set.
func (s *set) putElems(elems []*pb.Element, id string, prio uint64) error {
	if len(elems) == 0 {
		return nil
	}

	var store ds.Write = s.store
	var err error
	batchingDs, batching := store.(ds.Batching)
	if batching {
		store, err = batchingDs.Batch()
		if err != nil {
			return err
		}
	}

	for _, e := range elems {
		e.Id = id // overwrite the identifier as it would come unset
		key := e.GetKey()
		// /namespace/<key>/elems/<id>
		k := s.elemsPrefix(key).ChildString(id)
		err := store.Put(k, nil)
		if err != nil {
			return err
		}

		// update the value if higher priority than we currently have.
		err = s.setValue(key, e.GetValue(), prio)
		if err != nil {
			return err
		}
	}

	if batching {
		err := store.(ds.Batch).Commit()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *set) putTombs(tombs []*pb.Element) error {
	if len(tombs) == 0 {
		return nil
	}

	var store ds.Write = s.store
	var err error
	batchingDs, batching := store.(ds.Batching)
	if batching {
		store, err = batchingDs.Batch()
		if err != nil {
			return err
		}
	}

	for _, e := range tombs {
		// /namespace/<key>/tombs/<id>
		k := s.tombsPrefix(e.GetKey()).ChildString(e.GetId())
		err := store.Put(k, nil)
		if err != nil {
			return err
		}
	}

	if batching {
		err := store.(ds.Batch).Commit()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *set) Merge(d *pb.Delta, id string) error {
	err := s.putElems(d.GetElements(), id, d.GetPriority())
	if err != nil {
		return err
	}

	return s.putTombs(d.GetTombstones())
}

func (s *set) inElemsKeyID(key, id string) (bool, error) {
	k := s.elemsPrefix(key).ChildString(id)
	return s.store.Has(k)
}

func (s *set) inTombsKeyID(key, id string) (bool, error) {
	k := s.tombsPrefix(key).ChildString(id)
	return s.store.Has(k)
}

// currently unused
// // inSet returns if the given cid/block is in elems and not in tombs (and
// // thus, it is an elemement of the set).
// func (s *set) inSetKeyID(key, id string) (bool, error) {
// 	inTombs, err := s.inTombsKeyID(key, id)
// 	if err != nil {
// 		return false, err
// 	}
// 	if inTombs {
// 		return false, nil
// 	}

// 	return s.inElemsKeyID(key, id)
// }
