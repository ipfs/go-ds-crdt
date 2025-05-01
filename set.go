package crdt

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"

	pb "github.com/ipfs/go-ds-crdt/pb"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	multierr "go.uber.org/multierr"

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
	store      ds.Datastore
	dagService ipld.DAGService
	namespace  ds.Key
	putHook    func(key string, v []byte)
	deleteHook func(key string)
	logger     logging.StandardLogger

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
	putHook func(key string, v []byte),
	deleteHook func(key string),
) (*set, error) {

	set := &set{
		namespace:  namespace,
		store:      d,
		dagService: dagService,
		logger:     logger,
		putHook:    putHook,
		deleteHook: deleteHook,
	}

	return set, nil
}

// Add returns a new delta-set adding the given key/value.
func (s *set) Add(ctx context.Context, key string, value []byte) *pb.Delta {
	return &pb.Delta{
		Elements: []*pb.Element{
			{
				Key:   key,
				Value: value,
			},
		},
		Tombstones: nil,
	}
}

// Rmv returns a new delta-set removing the given key.
func (s *set) Rmv(ctx context.Context, key string) (*pb.Delta, error) {
	delta := &pb.Delta{}

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
			delta.Tombstones = append(delta.Tombstones, &pb.Element{
				Key: key,
				Id:  id,
			})
		}
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

	_, v, err := decodeValue(value)
	if err != nil { // not found is fine, we just return it
		return value, err
	}

	return v, nil
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
	// In-mem benchmarking shows no clear winner. Badger docs say that
	// KeysOnly "is several order of magnitudes faster than regular
	// iteration". Contrary to my original feeling, however, live testing
	// with a 50GB badger with millions of keys shows more speed when
	// querying with value. It may be that speed is fully affected by the
	// current state of table compaction as well.
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
			// decode the value
			_, v, err := decodeValue(r.Value)
			if err != nil {
				sendResult(ctx, qctx, query.Result{Error: r.Error}, out)
			}
			entry.Value = v
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
	valueK := s.valueKey(key)
	data, err := s.store.Get(ctx, valueK)
	if err != nil {
		if err == ds.ErrNotFound {
			return 0, nil
		}
		return 0, err
	}
	prio, _, err := decodeValue(data)
	if err != nil {
		return 0, err
	}
	return prio, nil
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

// sets a value if priority is higher. When equal, it sets if the
// value is lexicographically higher than the current value.
func (s *set) setValue(ctx context.Context, writeStore ds.Write, key, id string, value []byte, prio uint64) error {
	// Do not update if this delta has been tombstoned.
	deleted, err := s.inTombsKeyID(ctx, key, id)
	if err != nil || deleted {
		return err
	}

	// Encode the candidate value.
	newEncoded := encodeValue(prio, value)
	valueK := s.valueKey(key)
	curEncoded, err := s.store.Get(ctx, valueK)
	if err != nil && !errors.Is(err, ds.ErrNotFound) {
		return err
	}
	if err == nil {
		curPrio, curVal, err := decodeValue(curEncoded)
		if err != nil {
			return err
		}
		// Only update if the new candidate has higher priority or,
		// when equal, a lexicographically greater value.
		if prio < curPrio {
			return nil
		}
		if prio == curPrio && bytes.Compare(curVal, value) >= 0 {
			return nil
		}
	}

	// Store the new “best” encoded value.
	if err = writeStore.Put(ctx, valueK, newEncoded); err != nil {
		return err
	}

	// Trigger the add hook with the original (unencoded) value.
	if s.putHook == nil {
		return nil
	}
	s.putHook(key, value)
	return nil
}

// findBestValue looks for all entries for the given key, figures out their
// priority from their delta (skipping the blocks by the given pendingTombIDs)
// and returns the value with the highest priority that is not tombstoned nor
// about to be tombstoned.
func (s *set) findBestValue(ctx context.Context, key string, pendingTombIDs []string) ([]byte, uint64, error) {
	prefix := s.elemsPrefix(key)
	q := query.Query{
		Prefix:   prefix.String(),
		KeysOnly: false,
	}

	results, err := s.store.Query(ctx, q)
	if err != nil {
		return nil, 0, err
	}
	defer results.Close()

	var bestValue []byte
	var bestPriority uint64

NEXT:
	for r := range results.Next() {
		if r.Error != nil {
			return nil, 0, r.Error
		}

		id := strings.TrimPrefix(r.Key, prefix.String())
		if !ds.RawKey(id).IsTopLevel() {
			continue
		}

		for _, tombID := range pendingTombIDs {
			if tombID == id {
				continue NEXT
			}
		}

		inTomb, err := s.inTombsKeyID(ctx, key, id)
		if err != nil {
			return nil, 0, err
		}
		if inTomb {
			continue
		}

		// Instead of doing a block lookup for the delta, we simply read the
		// encoded candidate value stored in the elems entry.
		candidateEncoded := r.Value
		if candidateEncoded == nil {
			continue
		}
		candidatePrio, candidateVal, err := decodeValue(candidateEncoded)
		if err != nil {
			return nil, 0, err
		}

		if candidatePrio < bestPriority {
			continue
		}
		if candidatePrio > bestPriority {
			bestPriority = candidatePrio
			bestValue = candidateVal
			continue
		}
		// If equal priority, choose the lexicographically greater value.
		if bytes.Compare(bestValue, candidateVal) < 0 {
			bestValue = candidateVal
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
func (s *set) putElems(ctx context.Context, elems []*pb.Element, id string, prio uint64) error {
	s.putElemsMux.Lock()
	defer s.putElemsMux.Unlock()

	if len(elems) == 0 {
		return nil
	}

	var store ds.Write = s.store
	var err error
	if batchingDs, ok := store.(ds.Batching); ok {
		store, err = batchingDs.Batch(ctx)
		if err != nil {
			return err
		}
	}

	for _, e := range elems {
		e.Id = id // overwrite the identifier if not set
		key := e.GetKey()
		// Write into /namespace/elems/<key>/<id> the encoded candidate value.
		k := s.elemsPrefix(key).ChildString(id)

		v := e.GetValue()

		candidateEncoded := encodeValue(prio, v)
		if err := store.Put(ctx, k, candidateEncoded); err != nil {
			return err
		}

		// Update the best value for this key if needed.
		if err := s.setValue(ctx, store, key, id, e.GetValue(), prio); err != nil {
			return err
		}
	}

	if batchingDs, ok := store.(ds.Batch); ok {
		if err := batchingDs.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *set) putTombs(ctx context.Context, tombs []*pb.Element) error {
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

	for _, e := range tombs {
		// /namespace/tombs/<key>/<id>
		key := e.GetKey()
		id := e.GetId()
		valueK := s.valueKey(key)
		deletedElems[key] = append(deletedElems[key], id)

		// Find best value for element that we are going to delete
		v, p, err := s.findBestValue(ctx, key, deletedElems[key])
		if err != nil {
			return err
		}
		if v == nil {
			store.Delete(ctx, valueK)
			store.Delete(ctx, s.priorityKey(key))
		} else {
			candidateEncoded := encodeValue(p, v)
			if err := store.Put(ctx, valueK, candidateEncoded); err != nil {
				return err
			}
			s.setPriority(ctx, store, key, p)
		}

		// Write tomb into store.
		err = s.recordTombstone(ctx, key, id, store)
		if err != nil {
			return err
		}
	}

	if batching {
		err := store.(ds.Batch).Commit(ctx)
		if err != nil {
			return err
		}
	}

	if s.deleteHook == nil {
		return nil
	}

	// run delete hook only once for all versions of the same element
	// tombstoned in this delta. Note it may be that the element was not
	// fully deleted and only a different value took its place.
	for del := range deletedElems {
		s.deleteHook(del)
	}

	return nil
}

func (s *set) recordTombstone(ctx context.Context, key string, id string, store ds.Write) error {
	k := s.tombsPrefix(key).ChildString(id)
	err := store.Put(ctx, k, nil)
	if err != nil {
		return err
	}
	return nil
}

func (s *set) Merge(ctx context.Context, d *pb.Delta, id string) error {
	err := s.putTombs(ctx, d.GetTombstones())
	if err != nil {
		return err
	}

	return s.putElems(ctx, d.GetElements(), id, d.GetPriority())
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

	return multierr.Combine(errs...)
}

func encodeValue(prio uint64, value []byte) []byte {
	buf := make([]byte, 8+len(value))
	binary.BigEndian.PutUint64(buf[:8], prio)
	copy(buf[8:], value)
	return buf
}

func decodeValue(encoded []byte) (uint64, []byte, error) {
	if len(encoded) < 8 {
		return 0, nil, errors.New("encoded value too short")
	}
	prio := binary.BigEndian.Uint64(encoded[:8])
	return prio, encoded[8:], nil
}

func (s *set) CloneFrom(ctx context.Context, src *set, priorityHint uint64) error {
	// 1) Snapshot the OLD state (before any writes)
	old := make(map[string][]byte)
	prefix := s.keyPrefix(keysNs).String()
	results, _ := s.store.Query(ctx, query.Query{Prefix: prefix, KeysOnly: false})
	for r := range results.Next() {
		if !strings.HasSuffix(r.Key, "/"+valueSuffix) {
			continue
		}
		key := extractKey(r.Key, prefix)
		prio, val, _ := decodeValue(r.Value)
		if prio > priorityHint {
			old[key] = val
		}
	}
	results.Close()

	// 2) Bulk‐wipe the namespace so we don’t leave old keys behind
	if err := s.clearNamespace(ctx); err != nil {
		return err
	}

	// 3) Bulk‐copy everything from src
	if err := s.cloneDataOnly(ctx, src); err != nil {
		return err
	}

	// 4) Scan NEW state, fire putHooks & remove seen keys from `old`
	results2, _ := s.store.Query(ctx, query.Query{Prefix: prefix, KeysOnly: false})
	for r := range results2.Next() {
		if !strings.HasSuffix(r.Key, "/"+valueSuffix) {
			continue
		}
		key := extractKey(r.Key, prefix)
		prio, newVal, _ := decodeValue(r.Value)
		if prio <= priorityHint {
			delete(old, key)
			continue
		}
		if oldVal, seen := old[key]; seen {
			delete(old, key)
			if !bytes.Equal(oldVal, newVal) && s.putHook != nil {
				s.putHook(key, newVal)
			}
		} else if s.putHook != nil {
			s.putHook(key, newVal)
		}
	}
	results2.Close()

	// 5) Any key still in `old` was deleted—fire deleteHook and delete the key
	for key := range old {
		if s.deleteHook != nil {
			s.deleteHook(key)
		}
		_ = s.store.Delete(ctx, s.valueKey(key))
	}

	return nil
}

// extractKey strips off the prefix and the "/v" suffix,
// turning e.g. "/my/ns/set/k/foo/v" with prefix "/my/ns/set/k/"
// into "foo".
func extractKey(fullKey, prefix string) string {
	// remove the namespace+keysNs prefix
	rel := strings.TrimPrefix(fullKey, prefix)
	// strip any leading "/"
	rel = strings.TrimPrefix(rel, "/")
	// strip the "/v" suffix
	if strings.HasSuffix(rel, "/"+valueSuffix) {
		rel = strings.TrimSuffix(rel, "/"+valueSuffix)
	}
	return rel
}

// clearNamespace wipes *all* entries under this set's namespace
// so we can do a clean bulk‐copy.
func (s *set) clearNamespace(ctx context.Context) error {
	prefix := s.namespace.String()
	// list everything under s.namespace
	res, err := s.store.Query(ctx, query.Query{
		Prefix:   prefix,
		KeysOnly: false,
	})
	if err != nil {
		return fmt.Errorf("clearNamespace: query failed: %w", err)
	}
	defer res.Close()

	// batch if supported
	var batch ds.Batch
	if b, ok := s.store.(ds.Batching); ok {
		batch, err = b.Batch(ctx)
		if err != nil {
			return fmt.Errorf("clearNamespace: open batch: %w", err)
		}
	}

	for r := range res.Next() {
		if r.Error != nil {
			return fmt.Errorf("clearNamespace: scan error: %w", r.Error)
		}
		key := ds.NewKey(r.Entry.Key)
		if batch != nil {
			if err := batch.Delete(ctx, key); err != nil {
				return fmt.Errorf("clearNamespace: batch delete %s: %w", key, err)
			}
		} else {
			if err := s.store.Delete(ctx, key); err != nil {
				return fmt.Errorf("clearNamespace: delete %s: %w", key, err)
			}
		}
	}

	if batch != nil {
		if err := batch.Commit(ctx); err != nil {
			return fmt.Errorf("clearNamespace: commit: %w", err)
		}
	}
	return nil
}

// cloneDataOnly copies *all* entries under src.namespace into s.namespace
// one‐for‐one, without firing any hooks.
func (s *set) cloneDataOnly(ctx context.Context, src *set) error {
	prefix := src.namespace.String()
	// fetch every key/value under src.namespace
	res, err := src.store.Query(ctx, query.Query{
		Prefix:   prefix,
		KeysOnly: false,
	})
	if err != nil {
		return fmt.Errorf("cloneDataOnly: query src: %w", err)
	}
	defer res.Close()

	// batch the writes if possible
	var batch ds.Batch
	if b, ok := s.store.(ds.Batching); ok {
		batch, err = b.Batch(ctx)
		if err != nil {
			return fmt.Errorf("cloneDataOnly: open dest batch: %w", err)
		}
	}

	for r := range res.Next() {
		if r.Error != nil {
			return fmt.Errorf("cloneDataOnly: scan error: %w", r.Error)
		}
		// drop the src.namespace prefix, rebase under s.namespace
		rel := strings.TrimPrefix(r.Entry.Key, prefix)
		rel = strings.TrimPrefix(rel, "/")
		targetKey := s.namespace.ChildString(rel)

		if batch != nil {
			if err := batch.Put(ctx, targetKey, r.Entry.Value); err != nil {
				return fmt.Errorf("cloneDataOnly: batch put %s: %w", targetKey, err)
			}
		} else {
			if err := s.store.Put(ctx, targetKey, r.Entry.Value); err != nil {
				return fmt.Errorf("cloneDataOnly: put %s: %w", targetKey, err)
			}
		}
	}

	if batch != nil {
		if err := batch.Commit(ctx); err != nil {
			return fmt.Errorf("cloneDataOnly: commit: %w", err)
		}
	}
	return nil
}
