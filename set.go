package crdt

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"sync"

	dshelp "github.com/ipfs/boxo/datastore/dshelp"
	cid "github.com/ipfs/go-cid"
	pb "github.com/ipfs/go-ds-crdt/pb"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"

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

// Set specifies operations that the add-wins observed-removed set must fulfil.
type Set interface {
	Add(ctx context.Context, key string, value []byte) (Delta, error)
	Rmv(ctx context.Context, key string) (Delta, error)
	Merge(ctx context.Context, d Delta, id string) error
	Element(ctx context.Context, key string) ([]byte, error)
	Elements(ctx context.Context, q query.Query) (query.Results, error)
	InSet(ctx context.Context, key string) (bool, error)
}

// setHooks holds the hook functions for put and delete events and whether
// the hooks should be invoked with the previous value loaded from the store.
type setHooks struct {
	putHook               func(PutEvent)
	deleteHook            func(DeleteEvent)
	hookLoadPreviousValue bool
}

// keyState pairs a materialised-view value with its priority. Used inside
// putTombs to snapshot the pre-tombstone state and track the post-tombstone
// winner without maintaining parallel value/priority maps.
type keyState struct {
	value    []byte
	priority uint64
}

// set implements an Add-Wins Observed-Remove Set using delta-CRDTs
// (https://arxiv.org/abs/1410.2803) and backing all the data in a
// go-datastore. It is fully agnostic to MerkleCRDTs or the delta distribution
// layer.  It chooses the Value with most priority for a Key as the current
// Value. When two values have the same priority, it chooses by alphabetically
// sorting their unique IDs alphabetically.
type set struct {
	store        ds.Datastore
	dagService   ipld.DAGService
	namespace    ds.Key
	hooks        setHooks
	deltaFactory func() Delta
	logger       logging.StandardLogger

	// Avoid merging two things at the same time since
	// we read-write value-priorities in a non-atomic way.
	putElemsMux sync.Mutex
}

func newCRDTSet(
	ctx context.Context,
	d ds.Datastore,
	namespace ds.Key,
	dagService ipld.DAGService,
	logger logging.StandardLogger,
	hooks setHooks,
	deltaFactory func() Delta,
) (*set, error) {
	set := &set{
		namespace:    namespace,
		store:        d,
		dagService:   dagService,
		logger:       logger,
		hooks:        hooks,
		deltaFactory: deltaFactory,
	}

	return set, nil
}

// triggerPutHook fires the put hook with evt. Callers build evt themselves
// and must invoke this after the relevant batch has been committed, so the
// hook callback can observe the post-write state via s.store.
func (s *set) triggerPutHook(evt PutEvent) {
	if s.hooks.putHook == nil {
		return
	}
	s.hooks.putHook(evt)
}

// triggerDeleteHook fires the delete hook with evt. Callers build evt
// themselves and must invoke this after the relevant batch has been
// committed, so the hook callback can observe the post-write state via
// s.store.
func (s *set) triggerDeleteHook(evt DeleteEvent) {
	if s.hooks.deleteHook == nil {
		return
	}
	s.hooks.deleteHook(evt)
}

// Add returns a new delta-set adding the given key/value.
func (s *set) Add(ctx context.Context, key string, value []byte) (Delta, error) {
	delta := s.deltaFactory()
	delta.SetElements([]*pb.Element{
		{
			Key:   key,
			Value: value,
		},
	})
	return delta, nil
}

// Rmv returns a new delta-set removing the given key.
func (s *set) Rmv(ctx context.Context, key string) (Delta, error) {
	delta := s.deltaFactory()
	var tombs []*pb.Element

	// /namespace/<key>/elements
	prefix := s.elemsPrefix(key)
	q := query.Query{
		Prefix:   prefix.String(),
		KeysOnly: true,
	}

	results, err := s.store.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	//nolint:errcheck
	defer results.Close()

	for r := range results.Next() {
		if r.Error != nil {
			return nil, r.Error
		}
		id := strings.TrimPrefix(r.Key, prefix.String())
		if !ds.RawKey(id).IsTopLevel() {
			// our prefix matches blocks from other keys i.e. our
			// prefix is "hello" and we have a different key like
			// "hello/bye" so we have a block id like
			// "bye/<block>". If we got the right key, then the id
			// should be the block id only.
			continue
		}

		// check if its already tombed, which case don't add it to the
		// Rmv delta set.
		deleted, err := s.inTombsKeyID(ctx, key, id)
		if err != nil {
			return nil, err
		}
		if !deleted {
			tombs = append(tombs, &pb.Element{
				Key: key,
				Id:  id,
			})
		}
	}

	if len(tombs) > 0 {
		delta.SetTombstones(tombs)
	}

	return delta, nil
}

// Element retrieves the value of an element from the CRDT set.
func (s *set) Element(ctx context.Context, key string) ([]byte, error) {
	// We can only GET an element if it's part of the Set (in
	// "elements" and not in "tombstones").

	// * If the key has a value in the store it means that it has been
	//   written and is alive. putTombs will delete the value if all elems
	//   are tombstoned, or leave the best one.

	valueK := s.valueKey(key)
	value, err := s.store.Get(ctx, valueK)
	if err != nil { // not found is fine, we just return it
		return value, err
	}
	return value, nil
}

// Elements returns all the elements in the set.
func (s *set) Elements(ctx context.Context, q query.Query) (query.Results, error) {
	// This will cleanup user the query prefix first.
	// This makes sure the use of things like "/../" in the query
	// does not affect our setQuery.
	srcQueryPrefixKey := ds.NewKey(q.Prefix)

	keyNamespacePrefix := s.keyPrefix(keysNs)
	keyNamespacePrefixStr := keyNamespacePrefix.String()
	setQueryPrefix := keyNamespacePrefix.Child(srcQueryPrefixKey).String()
	vSuffix := "/" + valueSuffix

	// We are going to be reading everything in the /set/ namespace which
	// will return items in the form:
	// * /set/<key>/value
	// * /set<key>/priority (a Uvarint)

	// It is clear that KeysOnly=true should be used here when the original
	// query only wants keys.
	//
	// However, there is a question of what is best when the original
	// query wants also values:
	// * KeysOnly: true avoids reading all the priority key values
	//   which are skipped at the cost of doing a separate Get() for the
	//   values (50% of the keys).
	// * KeysOnly: false reads everything from the start. Priorities
	//   and tombstoned values are read for nothing
	//
	// KeysOnly retrieval can be faster with Pebble, at least for larger
	// values, due to pebble's ability to bypass value retrieval. This results
	// in reduced I/O, and reduced memory allocation and garbage collection.
	// Performance gains may be less significant with small values.
	setQuery := query.Query{
		Prefix:   setQueryPrefix,
		KeysOnly: false,
	}

	// send the result and returns false if we must exit.
	sendResult := func(ctx, qctx context.Context, r query.Result, out chan<- query.Result) bool {
		select {
		case out <- r:
		case <-ctx.Done():
			return false
		case <-qctx.Done():
			return false
		}
		return r.Error == nil
	}

	// The code below was very inspired in the Query implementation in
	// flatfs.

	// Originally we were able to set the output channel capacity and it
	// was set to 128 even though not much difference to 1 could be
	// observed on mem-based testing.

	// Using KeysOnly still gives a 128-item channel.
	// See: https://github.com/ipfs/go-datastore/issues/40
	r := query.ResultsWithContext(q, func(qctx context.Context, out chan<- query.Result) {
		// qctx is a Background context for the query. It is not
		// associated to ctx. It is closed when this function finishes
		// along with the output channel, or when the Results are
		// Closed directly.
		results, err := s.store.Query(ctx, setQuery)
		if err != nil {
			sendResult(ctx, qctx, query.Result{Error: err}, out)
			return
		}
		//nolint:errcheck
		defer results.Close()

		var entry query.Entry
		for r := range results.Next() {
			if r.Error != nil {
				sendResult(ctx, qctx, query.Result{Error: r.Error}, out)
				return
			}

			// We will be getting keys in the form of
			// /namespace/keys/<key>/v and /namespace/keys/<key>/p
			// We discard anything not ending in /v and sanitize
			// those from:
			// /namespace/keys/<key>/v -> <key>
			if !strings.HasSuffix(r.Key, vSuffix) { // "/v"
				continue
			}

			key := strings.TrimSuffix(
				strings.TrimPrefix(r.Key, keyNamespacePrefixStr),
				"/"+valueSuffix,
			)

			entry.Key = key
			entry.Value = r.Value
			entry.Size = r.Size
			entry.Expiration = r.Expiration

			// The fact that /v is set means it is not tombstoned,
			// as tombstoning removes /v and /p or sets them to
			// the best value.

			if q.KeysOnly {
				entry.Size = -1
				entry.Value = nil
			}
			if !sendResult(ctx, qctx, query.Result{Entry: entry}, out) {
				return
			}
		}
	})

	return r, nil
}

// InSet returns true if the key belongs to one of the elements in the "elems"
// set, and this element is not tombstoned.
func (s *set) InSet(ctx context.Context, key string) (bool, error) {
	// If we do not have a value this key was never added or it was fully
	// tombstoned.
	valueK := s.valueKey(key)
	return s.store.Has(ctx, valueK)
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

func (s *set) getPriority(ctx context.Context, key string) (uint64, error) {
	prioK := s.priorityKey(key)
	data, err := s.store.Get(ctx, prioK)
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

func (s *set) setPriority(ctx context.Context, writeStore ds.Write, key string, prio uint64) error {
	prioK := s.priorityKey(key)
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, prio+1)
	if n == 0 {
		return errors.New("error encoding priority")
	}

	return writeStore.Put(ctx, prioK, buf[0:n])
}

// findBestValue looks for all entries for the given key, figures out their
// priority from their delta (skipping the blocks by the given pendingTombIDs)
// and returns the value with the highest priority that is not tombstoned nor
// about to be tombstoned.
func (s *set) findBestValue(ctx context.Context, key string, pendingTombIDs []string) ([]byte, uint64, error) {
	// /namespace/elems/<key>
	prefix := s.elemsPrefix(key)
	q := query.Query{
		Prefix:   prefix.String(),
		KeysOnly: true,
	}

	results, err := s.store.Query(ctx, q)
	if err != nil {
		return nil, 0, err
	}
	//nolint:errcheck
	defer results.Close()

	var bestValue []byte
	var bestPriority uint64
	var deltaCid cid.Cid
	ng := crdtNodeGetter{NodeGetter: s.dagService}

	// range all the /namespace/elems/<key>/<block_cid>.
NEXT:
	for r := range results.Next() {
		if r.Error != nil {
			return nil, 0, r.Error
		}

		id := strings.TrimPrefix(r.Key, prefix.String())
		if !ds.RawKey(id).IsTopLevel() {
			// our prefix matches blocks from other keys i.e. our
			// prefix is "hello" and we have a different key like
			// "hello/bye" so we have a block id like
			// "bye/<block>". If we got the right key, then the id
			// should be the block id only.
			continue
		}
		// if block is one of the pending tombIDs, continue
		for _, tombID := range pendingTombIDs {
			if tombID == id {
				continue NEXT
			}
		}

		// if tombstoned, continue
		inTomb, err := s.inTombsKeyID(ctx, key, id)
		if err != nil {
			return nil, 0, err
		}
		if inTomb {
			continue
		}

		// get the block
		mhash, err := dshelp.DsKeyToMultihash(ds.NewKey(id))
		if err != nil {
			return nil, 0, err
		}
		deltaCid = cid.NewCidV1(cid.DagProtobuf, mhash)
		_, deltaBytes, err := ng.GetDelta(ctx, deltaCid)
		if err != nil {
			return nil, 0, err
		}

		delta := s.deltaFactory()
		err = delta.Unmarshal(deltaBytes)
		if err != nil {
			return nil, 0, err
		}
		priority := delta.GetPriority()

		// discard this delta.
		if priority < bestPriority {
			continue
		}

		// When equal priority, choose the greatest among values in
		// the delta and current. When higher priority, choose the
		// greatest only among those in the delta.
		var greatestValueInDelta []byte
		elems, err := delta.GetElements()
		if err != nil {
			return nil, 0, err
		}
		for _, elem := range elems {
			if elem.GetKey() != key {
				continue
			}
			v := elem.GetValue()
			if bytes.Compare(greatestValueInDelta, v) < 0 {
				greatestValueInDelta = v
			}
		}

		if priority > bestPriority {
			bestValue = greatestValueInDelta
			bestPriority = priority
			continue
		}

		// equal priority
		if bytes.Compare(bestValue, greatestValueInDelta) < 0 {
			bestValue = greatestValueInDelta
		}
	}

	return bestValue, bestPriority, nil
}

// putElems adds items to the "elems" set. It will also set current
// values and priorities for each element. This needs to run in a lock,
// as otherwise races may occur when reading/writing the priorities, resulting
// in bad behaviours.
//
// Technically the lock should only affect the keys that are being written,
// but with the batching optimization the locks would need to be hold until
// the batch is written), and one lock per key might be way worse than a single
// global lock in the end.
//
// delta is the triggering delta; it supplies the write priority and is
// forwarded to the put hook. delta must not be nil when elems is non-empty.
func (s *set) putElems(ctx context.Context, elems []*pb.Element, id string, delta Delta) error {
	s.putElemsMux.Lock()
	defer s.putElemsMux.Unlock()

	if len(elems) == 0 {
		return nil
	}
	if delta == nil {
		return errors.New("putElems: delta must not be nil when elems is non-empty")
	}

	var store ds.Write = s.store
	var err error
	batchingDs, batching := store.(ds.Batching)
	if batching {
		store, err = batchingDs.Batch(ctx)
		if err != nil {
			return err
		}
	}

	// Per-key accumulator: resolve the winning value across all same-key
	// elements in this delta before writing, so the store and the put hook
	// reflect CRDT semantics (lex-largest wins at tied priority) independent
	// of element order, and so the hook fires once per key rather than once
	// per element.
	type putKeyState struct {
		bestVal    []byte
		skipWrite  bool // delta loses to the current store winner for this key
		prev       keyState
		prevLoaded bool // prev snapshot has been read from the store
	}

	prio := delta.GetPriority()
	loadPrev := s.hooks.hookLoadPreviousValue && s.hooks.putHook != nil
	states := make(map[string]*putKeyState)

	for _, e := range elems {
		e.Id = id // overwrite the identifier as it would come unset
		key := e.GetKey()
		// /namespace/elems/<key>/<id>
		k := s.elemsPrefix(key).ChildString(id)
		if err := store.Put(ctx, k, nil); err != nil {
			return err
		}

		// Skip tombstoned elements: they cannot contribute to the in-delta
		// winner (mirrors setValue's inTombsKeyID check).
		deleted, err := s.inTombsKeyID(ctx, key, id)
		if err != nil {
			return err
		}
		if deleted {
			continue
		}

		st, ok := states[key]
		if !ok {
			st = &putKeyState{}
			states[key] = st

			curPrio, err := s.getPriority(ctx, key)
			if err != nil {
				return err
			}
			if prio < curPrio {
				st.skipWrite = true
			} else if prio == curPrio {
				// Tied priority: the store's current value is a competitor in
				// the lex-largest tiebreak. Seed bestVal with it and snapshot
				// it as prev so the post-loop no-op check can suppress a
				// redundant write when no in-delta elem beats curVal.
				curVal, _ := s.store.Get(ctx, s.valueKey(key))
				st.bestVal = curVal
				st.prev = keyState{value: curVal, priority: curPrio}
				st.prevLoaded = true
			} else {
				// prio > curPrio: delta always wins, no tiebreak needed
				// bestVal stays nil and will be replaced by any in-delta value.
				if loadPrev {
					curVal, _ := s.store.Get(ctx, s.valueKey(key))
					st.prev = keyState{value: curVal, priority: curPrio}
					st.prevLoaded = true
				}
			}
		}

		if st.skipWrite {
			continue
		}

		if v := e.GetValue(); bytes.Compare(v, st.bestVal) > 0 {
			st.bestVal = v
		}
	}

	var events []PutEvent
	if s.hooks.putHook != nil {
		events = make([]PutEvent, 0, len(states))
	}

	for key, st := range states {
		if st.skipWrite {
			continue
		}
		// Suppress no-op writes: when the chosen value and priority match
		// what is already in the store (tied priority, equal bytes), nothing
		// changes. Mirrors setValue's equal-bytes skip.
		if st.prevLoaded && prio == st.prev.priority && bytes.Equal(st.bestVal, st.prev.value) {
			continue
		}

		if err := store.Put(ctx, s.valueKey(key), st.bestVal); err != nil {
			return err
		}
		if err := s.setPriority(ctx, store, key, prio); err != nil {
			return err
		}

		if s.hooks.putHook == nil {
			continue
		}
		evt := PutEvent{
			Key:         ds.NewKey(key),
			NewValue:    st.bestVal,
			NewPriority: prio,
			Delta:       delta,
		}
		if loadPrev {
			evt.OldValue = st.prev.value
			evt.OldPriority = st.prev.priority
		}
		events = append(events, evt)
	}

	if batching {
		err := store.(ds.Batch).Commit(ctx)
		if err != nil {
			return err
		}
	}

	for _, evt := range events {
		s.triggerPutHook(evt)
	}
	return nil
}

// putTombs applies tombstones and recomputes winners for the affected keys.
// delta is the tombstone delta that triggered the removal and is forwarded to
// put/delete hooks. delta may be nil when the caller has no originating delta
// (e.g. the purge path), since it is only used for hook forwarding.
func (s *set) putTombs(ctx context.Context, tombs []*pb.Element, delta Delta) error {
	if len(tombs) == 0 {
		return nil
	}

	var store ds.Write = s.store
	var err error
	batchingDs, batching := store.(ds.Batching)
	if batching {
		store, err = batchingDs.Batch(ctx)
		if err != nil {
			return err
		}
	}

	// key -> tombstonedBlockID. Carries the tombstoned blocks for each
	// element in this delta.
	deletedElems := make(map[string][]string)

	var newStates, prevStates map[string]keyState
	if s.hooks.putHook != nil || s.hooks.deleteHook != nil {
		// newStates holds the winning (value, priority) for keys that were
		// partially tombstoned (tombstone removed a previous winner but a
		// surviving element took over). A key absent from this map was fully
		// deleted. Doubles as the fully-deleted oracle, so it is allocated
		// whenever any hook is registered; nil means no hooks are configured
		// and the firing loop is skipped entirely.
		newStates = make(map[string]keyState)

		if s.hooks.hookLoadPreviousValue {
			// prevStates caches the (value, priority) at the time each key is
			// first seen in this delta, before any write. Only keys that
			// existed in the store are added; absent keys are omitted so the
			// two-value map lookup (v, ok) cleanly distinguishes "had a value"
			// from "was not in the store".
			prevStates = make(map[string]keyState)
		}
	}

	var errs []error
	for _, e := range tombs {
		// /namespace/tombs/<key>/<id>
		key := e.GetKey()
		id := e.GetId()
		valueK := s.valueKey(key)

		// Capture the current value and priority on first encounter, before any
		// write for this key, so hooks receive the pre-tombstone snapshot.
		if prevStates != nil {
			if _, seen := prevStates[key]; !seen {
				if curVal, err := s.store.Get(ctx, valueK); err == nil {
					entry := keyState{value: curVal}
					// Any error (including ErrNotFound): omit from map; hook
					// firing uses (v, ok) to detect absence.
					if curPrio, err := s.getPriority(ctx, key); err == nil {
						entry.priority = curPrio
					}
					prevStates[key] = entry
				}
			}
		}
		deletedElems[key] = append(deletedElems[key], id)

		// Find best value for element that we are going to delete
		v, p, err := s.findBestValue(ctx, key, deletedElems[key])
		if err != nil {
			return err
		}

		// p == 0 means findBestValue found no surviving element: real deltas
		// always have priority >= 1 (assigned as height+1 in addDAGNode), so a
		// zero priority can only come from the zero-value init.
		if p == 0 {
			delete(newStates, key)
			if err = store.Delete(ctx, valueK); err != nil {
				errs = append(errs, err)
			}
			if err = store.Delete(ctx, s.priorityKey(key)); err != nil {
				errs = append(errs, err)
			}
		} else {
			if newStates != nil {
				newStates[key] = keyState{value: v, priority: p}
			}
			if err = store.Put(ctx, valueK, v); err != nil {
				errs = append(errs, err)
			}
			if err = s.setPriority(ctx, store, key, p); err != nil {
				errs = append(errs, err)
			}
		}

		// Write tomb into store.
		k := s.tombsPrefix(key).ChildString(id)
		if err = store.Put(ctx, k, nil); err != nil {
			errs = append(errs, err)
		}
		if err := errors.Join(errs...); err != nil {
			return err
		}
	}

	if batching {
		err := store.(ds.Batch).Commit(ctx)
		if err != nil {
			return err
		}
	}

	// Fire hooks once per key after all writes are committed.
	// Skipped entirely when no hooks are registered (newStates == nil). Fully
	// deleted keys (absent from newStates) trigger the delete hook; partially
	// tombstoned keys (present in newStates) trigger the put hook. When
	// hookLoadPreviousValue is set, prevStates is used to suppress no-op hook
	// firings (winning element unchanged — same value and priority — or
	// tombstone for a key that had no value).
	if newStates != nil {
		for del := range deletedElems {
			if newState, partial := newStates[del]; partial {
				var prev keyState
				if prevStates != nil {
					prev = prevStates[del] // zero-valued if key was absent
					if newState.priority == prev.priority && bytes.Equal(newState.value, prev.value) {
						continue // same winning element, genuine no-op
					}
				}
				s.triggerPutHook(PutEvent{
					Key:         ds.NewKey(del),
					OldValue:    prev.value,
					NewValue:    newState.value,
					OldPriority: prev.priority,
					NewPriority: newState.priority,
					Delta:       delta,
				})
			} else {
				var prev keyState
				if prevStates != nil {
					var ok bool
					prev, ok = prevStates[del]
					if !ok {
						continue // key had no value before tombstone, nothing to notify
					}
				}
				s.triggerDeleteHook(DeleteEvent{
					Key:          ds.NewKey(del),
					LastValue:    prev.value,
					LastPriority: prev.priority,
					Delta:        delta,
				})
			}
		}
	}

	return nil
}

func (s *set) Merge(ctx context.Context, d Delta, id string) error {
	tombs, err := d.GetTombstones()
	if err != nil {
		return err
	}

	elems, err := d.GetElements()
	if err != nil {
		return err
	}

	err = s.putTombs(ctx, tombs, d)
	if err != nil {
		return err
	}

	return s.putElems(ctx, elems, id, d)
}

// currently unused
// func (s *set) inElemsKeyID(key, id string) (bool, error) {
// 	k := s.elemsPrefix(key).ChildString(id)
// 	return s.store.Has(k)
// }

func (s *set) inTombsKeyID(ctx context.Context, key, id string) (bool, error) {
	k := s.tombsPrefix(key).ChildString(id)
	return s.store.Has(ctx, k)
}

// currently unused
// // inSet returns if the given cid/block is in elems and not in tombs (and
// // thus, it is an element of the set).
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

// purgeKeyBlocks removes element and tombstone entries for the given key that
// were created by any of the given block CIDs. After removal, it recomputes
// the best value from surviving elements. If no elements survive, the key's
// value and priority are deleted.
//
// hasElems and hasTombs indicate which namespaces the DAG being purged
// actually wrote entries for this key; passing false for either skips the
// corresponding datastore query.
func (s *set) purgeKeyBlocks(ctx context.Context, key string, blockCIDs map[cid.Cid]struct{}, hasElems, hasTombs bool) error {
	s.putElemsMux.Lock()
	defer s.putElemsMux.Unlock()

	var store ds.Write = s.store
	batchingDs, batching := store.(ds.Batching)
	var err error
	if batching {
		store, err = batchingDs.Batch(ctx)
		if err != nil {
			return err
		}
	}

	deleteMatchingIDs := func(prefix ds.Key) error {
		q := query.Query{
			Prefix:   prefix.String(),
			KeysOnly: true,
		}
		results, err := s.store.Query(ctx, q)
		if err != nil {
			return err
		}
		defer results.Close() //nolint:errcheck

		for r := range results.Next() {
			if r.Error != nil {
				return r.Error
			}
			// Strip the query prefix to get the relative remainder, which for a
			// direct child is just the block ID (a CID encoded as a datastore key
			// string): "/namespace/s/foo/<blockID>" → "/<blockID>".
			blockID := strings.TrimPrefix(r.Key, prefix.String())
			// The prefix query also returns entries for child keys: prefix "foo"
			// matches both "foo/<block>" and "foo/bar/<block>". After trimming, a
			// direct child yields a top-level key "/<blockID>", while a grandchild
			// yields "/bar/<blockID>". Reject the latter since it belongs to a
			// different key.
			if !ds.RawKey(blockID).IsTopLevel() {
				continue
			}
			// Decode the datastore key back into a CID so we can look it up in the
			// caller-supplied set.
			mhash, err := dshelp.DsKeyToMultihash(ds.NewKey(blockID))
			if err != nil {
				return err
			}
			c := cid.NewCidV1(cid.DagProtobuf, mhash)
			if _, ok := blockCIDs[c]; !ok {
				continue
			}
			if err := store.Delete(ctx, prefix.ChildString(blockID)); err != nil {
				return err
			}
		}
		return nil
	}

	if hasElems {
		if err := deleteMatchingIDs(s.elemsPrefix(key)); err != nil {
			return err
		}
	}
	if hasTombs {
		if err := deleteMatchingIDs(s.tombsPrefix(key)); err != nil {
			return err
		}
	}

	// The delete batch and the value/priority rewrite below are not atomic.
	// A crash between them leaves a stale value key, but PurgeDAG is
	// idempotent: a retry will skip the already-deleted entries and rewrite
	// the value correctly.
	if batching {
		if err := store.(ds.Batch).Commit(ctx); err != nil {
			return err
		}
	}

	// Recompute best value from surviving elements. Entries are already
	// deleted from the store, so nil pendingTombIDs is correct.
	bestVal, bestPrio, err := s.findBestValue(ctx, key, nil)
	if err != nil {
		return err
	}

	valueK := s.valueKey(key)

	// Fetch old value and priority before modifying the value key, so hooks
	// receive the pre-purge snapshot. Only read when a hook that needs it is
	// configured. A missing key yields prev.value == nil (Get returns nil on
	// ErrNotFound), which is used below to detect "no prior value".
	var prev keyState
	if s.hooks.hookLoadPreviousValue && (s.hooks.putHook != nil || s.hooks.deleteHook != nil) {
		prev.value, _ = s.store.Get(ctx, valueK)
		prev.priority, _ = s.getPriority(ctx, key)
	}

	// bestPrio == 0 means findBestValue found no surviving element: real deltas
	// always have priority >= 1 (assigned as height+1 in addDAGNode), so a zero
	// priority can only come from the zero-value init.
	if bestPrio == 0 {
		var errs []error
		if err := s.store.Delete(ctx, valueK); err != nil && !errors.Is(err, ds.ErrNotFound) {
			errs = append(errs, err)
		}
		if err := s.store.Delete(ctx, s.priorityKey(key)); err != nil && !errors.Is(err, ds.ErrNotFound) {
			errs = append(errs, err)
		}
		if err := errors.Join(errs...); err != nil {
			return err
		}
		// Suppress the delete hook when the key had no prior value in the
		// store. Matches putTombs' "nothing to notify" rule. Only active when
		// hookLoadPreviousValue is set, since that's when we know the prior
		// state.
		if s.hooks.hookLoadPreviousValue && prev.value == nil {
			return nil
		}
		s.triggerDeleteHook(DeleteEvent{
			Key:          ds.NewKey(key),
			LastValue:    prev.value,
			LastPriority: prev.priority,
		})
	} else {
		if err := s.store.Put(ctx, valueK, bestVal); err != nil {
			return err
		}
		if err := s.setPriority(ctx, s.store, key, bestPrio); err != nil {
			return err
		}
		s.triggerPutHook(PutEvent{
			Key:         ds.NewKey(key),
			OldValue:    prev.value,
			NewValue:    bestVal,
			OldPriority: prev.priority,
			NewPriority: bestPrio,
		})
	}

	return nil
}

// perform a sync against all the paths associated with a key prefix
func (s *set) datastoreSync(ctx context.Context, prefix ds.Key) error {
	prefixStr := prefix.String()
	toSync := []ds.Key{
		s.elemsPrefix(prefixStr),
		s.tombsPrefix(prefixStr),
		s.keyPrefix(keysNs).Child(prefix), // covers values and priorities
	}

	errs := make([]error, len(toSync))

	for i, k := range toSync {
		if err := s.store.Sync(ctx, k); err != nil {
			errs[i] = err
		}
	}

	return errors.Join(errs...)
}
