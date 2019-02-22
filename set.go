package crdt

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"

	pb "github.com/hsanjuan/go-ds-crdt/pb"

	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
)

var (
	elemsNs        = "s" // elems
	tombsNs        = "t" // tombs
	valueSuffix    = "v" // value
	prioritySuffix = "p" // priority
)

var ErrStateModify = "the CRDT state cannot be modified directly"

// set implements an Add-Wins Observed-Removed Set using delta-CRDTs.
// and backing all the data in a go-datastore. It is fully agnostic to
// MerkleCRDTs or the delta distribution layer.  It chooses the Value with
// most priority for a Key as the current Value. When two values have the
// priority, it chooses by alphabetically sorting their unique IDs
// alphabetically.
type set struct {
	store     ds.Datastore
	namespace ds.Key
}

func newCRDTSet(
	d ds.Datastore,
	namespace ds.Key,
) *set {

	s := &set{
		namespace: namespace,
		store:     d,
	}
	return s
}

// Add returns a new delta-set adding the given elements.
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

// Rmv returns a new delta-set removing the given elements.
func (s *set) Rmv(key string) (*pb.Delta, error) {
	delta := &pb.Delta{
		Elements:   nil,
		Tombstones: nil,
	}

	// /namespace/<key>/elems
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

// element retrieves the value of an element from the CRDT set.
func (s *set) Element(key string) ([]byte, error) {
	// We can only GET an element if it's part of the Set (in
	// "elems" and not in "tombstones").

	// As an optimization, an element without a value in the store
	// implies it is not in the Set (never added or tombstoned).
	// But not the other way around: a tombtstoned element may still
	// have a value in the store.

	// An element having a value in the store implies that it is in
	// "elems".

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
	ns ds.Key
}

// func (f *filterIsKey) Filter(e query.Entry) bool {
// 	k := ds.NewKey(e.Key)
// 	res := f.Filter2(e)
// 	fmt.Printf("\n\n%s %s: %t", f.ns, k, res)
// 	return res
// }

func (f *filterIsKey) Filter(e query.Entry) bool {
	dsk := ds.NewKey(e.Key)
	return f.ns.IsAncestorOf(dsk) && ds.NewKey(valueSuffix).IsAncestorOf(dsk.Reverse())
}

// elements throws all the elements in the set.
// it comes handy to use query.Result to wrap key, value and error.
func (s *set) Elements() <-chan query.Result {
	q := query.Query{
		Prefix:   s.namespace.String(),
		KeysOnly: true,
		Filters: []query.Filter{
			&filterIsKey{ns: s.namespace},
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

			// /namespace/<key>/v -> <key>
			// If our filter worked well we should have only
			// got good keys like that.
			key := strings.TrimSuffix(
				strings.TrimPrefix(r.Key, s.namespace.String()),
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

// InSet returns if we have a Cid. For it we must have at least one element
// for that CID which is not tombstoned.
func (s *set) InSet(key string) (bool, error) {
	// /namespace/<key>/elems/
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

	// range for all the <cid> in the elems set.
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

// /namespace/<key>/elems
func (s *set) elemsPrefix(key string) ds.Key {
	return s.keyPrefix(key).ChildString(elemsNs)
}

// /namespace/<key>/tombs
func (s *set) tombsPrefix(key string) ds.Key {
	return s.keyPrefix(key).ChildString(tombsNs)
}

// /namespace/<key>/value
func (s *set) valueKey(key string) ds.Key {
	return s.keyPrefix(key).ChildString(valueSuffix)
}

// /namespace/<key>/priority
func (s *set) priorityKey(key string) ds.Key {
	return s.keyPrefix(key).ChildString(prioritySuffix)
}

func (s *set) getPrio(key string) (uint64, error) {
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

func (s *set) setPrio(key string, prio uint64) error {
	prioK := s.priorityKey(key)
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, prio+1)
	if n == 0 {
		return errors.New("error encoding priority")
	}

	return s.store.Put(prioK, buf[0:n])
}

// sets a value if priority is higher. When equal, it sets if the
// value is lexicographically higher than current.
func (s *set) setValue(key string, value []byte, prio uint64) error {
	curPrio, err := s.getPrio(key)
	if err != nil {
		return err
	}

	valueK := s.valueKey(key)
	if prio < curPrio {
		return nil
	}

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
	return s.setPrio(key, prio)
}

// putElems adds items to the "elems" set.
func (s *set) putElems(elems []*pb.Element, id string, prio uint64) error {
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
		err := s.store.Put(k, nil)
		if err != nil {
			return err
		}

		// update the value if higher priority than we currently have.
		err = s.setValue(key, e.GetValue(), prio)
		if err != nil {
			return err
		}
	}

	if batching && len(elems) > 0 {
		err := store.(ds.Batch).Commit()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *set) putTombs(tombs []*pb.Element) error {
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
		err := s.store.Put(k, nil)
		if err != nil {
			return err
		}
	}

	if batching && len(tombs) > 0 {
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
