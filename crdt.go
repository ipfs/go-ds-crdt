// Package crdt provides a replicated go-datastore implementation using
// Merkle-CRDTs built with IPLD nodes. This Datastore is agnostic to how new
// MerkleDAG roots are broadcasted to the rest of replicas (Broadcaster
// component) and to how the IPLD nodes are made discoverable and retrievable
// to by other replicas (DAG-Syncer component).
package crdt

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "github.com/ipfs/go-ds-crdt/pb"

	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log"
	"github.com/pkg/errors"
)

var _ ds.Datastore = (*Datastore)(nil)
var _ ds.Batching = (*Datastore)(nil)

// datastore namespace keys. Short keys save space and memory.
const (
	headsNs = "h" // heads
	setNs   = "s" // set
)

// Common errors.
var (
	ErrNoMoreBroadcast = errors.New("receiving blocks aborted since no new blocks will be broadcasted")
)

// A Broadcaster provides a way to send (notify) an opaque payload to
// all replicas and to retrieve payloads broadcasted.
type Broadcaster interface {
	// Send payload to other replicas.
	Broadcast([]byte) error
	// Obtain the next payload received from the network.
	Next() ([]byte, error)
}

// A DAGSyncer is an abstraction to an IPLD-based p2p storage layer.  A
// DAGSyncer is a DAGService with the ability to publish new ipld nodes to the
// network, and retrieving others from it.
type DAGSyncer interface {
	ipld.DAGService
	// Returns true if the block is locally available (therefore, it
	// is considered processed).
	IsKnownBlock(c cid.Cid) (bool, error)
}

// Options holds configurable values for Datastore.
type Options struct {
	Logger              logging.StandardLogger
	RebroadcastInterval time.Duration
	// The PutHook function is triggered whenever an element
	// is successfully added to the datastore (either by a local
	// or remote update), and only when that addition is considered the
	// prevalent value.
	PutHook func(k ds.Key, v []byte)
	// The DeleteHook function is triggered whenever a version of an
	// element is successfully removed from the datastore (either by a
	// local or remote update). Unordered and concurrent updates may
	// result in the DeleteHook being triggered even though the element is
	// still present in the datastore because it was re-added. If that is
	// relevant, use Has() to check if the removed element is still part
	// of the datastore.
	DeleteHook func(k ds.Key)
}

func (opts *Options) verify() error {
	if opts == nil {
		return errors.New("options cannot be nil")
	}

	if opts.RebroadcastInterval <= 0 {
		return errors.New("invalid RebroadcastInterval")
	}

	if opts.Logger == nil {
		return errors.New("logger should not be undefined")
	}
	return nil
}

// DefaultOptions initializes an Options object with sensible defaults.
func DefaultOptions() *Options {
	return &Options{
		Logger:              logging.Logger("crdt"),
		RebroadcastInterval: time.Minute,
		PutHook:             nil,
		DeleteHook:          nil,
	}
}

// Datastore makes a go-datastore a distributed Key-Value store using
// Merkle-CRDTs and IPLD.
type Datastore struct {
	ctx    context.Context
	cancel context.CancelFunc

	opts   *Options
	logger logging.StandardLogger

	// permanent storage
	store     ds.Datastore
	namespace ds.Key
	set       *set
	heads     *heads

	dags        *crdtDAGService
	broadcaster Broadcaster

	lastHeadTSMux sync.RWMutex
	lastHeadTS    time.Time

	rebroadcastTicker *time.Ticker

	curDeltaMux sync.Mutex
	curDelta    *pb.Delta // current, unpublished delta
}

// New returns a Merkle-CRDT-based Datastore using the given one to persist
// all the necessary data under the given namespace. It needs a DAG-Syncer
// component for IPLD nodes and a Broadcaster component to distribute and
// receive information to and from the rest of replicas. Actual implementation
// of these must be provided by the user.
func New(
	store ds.Datastore,
	namespace ds.Key,
	dagSyncer DAGSyncer,
	bcast Broadcaster,
	opts *Options,
) (*Datastore, error) {
	if opts == nil {
		opts = DefaultOptions()
	}

	if err := opts.verify(); err != nil {
		return nil, err
	}

	// <namespace>/set
	fullSetNs := namespace.ChildString(setNs)
	// <namespace>/heads
	fullHeadsNs := namespace.ChildString(headsNs)

	setPutHook := func(k string, v []byte) {
		if opts.PutHook == nil {
			return
		}
		dsk := ds.NewKey(k)
		opts.PutHook(dsk, v)
	}

	setDeleteHook := func(k string) {
		if opts.DeleteHook == nil {
			return
		}
		dsk := ds.NewKey(k)
		opts.DeleteHook(dsk)
	}

	set := newCRDTSet(store, fullSetNs, setPutHook, setDeleteHook)
	heads := newHeads(store, fullHeadsNs)

	crdtdags := &crdtDAGService{
		DAGSyncer: dagSyncer,
	}

	ctx, cancel := context.WithCancel(context.Background())

	dstore := &Datastore{
		ctx:               ctx,
		cancel:            cancel,
		opts:              opts,
		logger:            opts.Logger,
		store:             store,
		namespace:         namespace,
		set:               set,
		heads:             heads,
		dags:              crdtdags,
		broadcaster:       bcast,
		rebroadcastTicker: time.NewTicker(opts.RebroadcastInterval),
	}

	go dstore.handleNext()
	go dstore.rebroadcast()

	return dstore, nil
}

func (store *Datastore) handleNext() {
	if store.broadcaster == nil { // offline
		return
	}
	for {
		select {
		case <-store.ctx.Done():
			return
		default:
		}

		data, err := store.broadcaster.Next()
		if err != nil {
			if err == ErrNoMoreBroadcast || store.ctx.Err() != nil {
				return
			}
			store.logger.Error(err)

			continue
		}
		err = store.handleBlock(context.Background(), data)
		if err != nil {
			store.logger.Error(err)
		}
		store.lastHeadTSMux.Lock()
		store.lastHeadTS = time.Now()
		store.lastHeadTSMux.Unlock()
	}
}

func (store *Datastore) rebroadcast() {
	ticker := store.rebroadcastTicker
	for {
		select {
		case <-store.ctx.Done():
			return
		case <-ticker.C:
			store.lastHeadTSMux.RLock()
			timeSinceHead := time.Since(store.lastHeadTS)
			store.lastHeadTSMux.RUnlock()
			if timeSinceHead > store.opts.RebroadcastInterval {
				store.rebroadcastHeads()
			}
		}
	}
}

func (store *Datastore) rebroadcastHeads() {
	heads, _, err := store.heads.List()
	if err != nil {
		store.logger.Error(err)
	}

	for _, h := range heads {
		store.broadcast(h)
	}
}

// handleBlock takes care of vetting, retrieving and applying
// CRDT blocks to the Datastore.
func (store *Datastore) handleBlock(ctx context.Context, data []byte) error {
	c, err := cid.Cast(data)
	if err != nil {
		store.logger.Error(err)
		return err
	}

	// Ignore already known blocks.
	// This includes the case when the block is a current
	// head.
	known, err := store.dags.IsKnownBlock(c)
	if err != nil {
		return errors.Wrap(err, "error checking for known block")
	}
	if known {
		store.logger.Debugf("%s is known. Skip walking tree", c)
		return nil
	}

	// Walk down from this block.
	err = store.walkBranch(c, c, 1)
	if err != nil {
		return errors.Wrap(err, "error walking new branch")
	}
	return nil
}

// walkBranch walks down a branch and applies each node's delta until it
// arrives to a known head or the bottom.  The given CID is assumed to not be
// a known block and will be fetched.
func (store *Datastore) walkBranch(current, top cid.Cid, depth uint64) error {
	//store.logger.Debugf("walking on %s, from %s, depth %d", current, top, depth)

	// TODO: Pre-fetching of children?
	nd, delta, err := store.dags.GetDelta(store.ctx, current)
	if err != nil {
		return errors.Wrapf(err, "error fetching block %s", current)
	}

	// Once the DAGService has fetched the block, it should be in the
	// blockstore.

	// merge the delta
	err = store.set.Merge(delta, dshelp.CidToDsKey(current).String())
	if err != nil {
		return errors.Wrapf(err, "error merging delta from %s", current)
	}

	links := nd.Links()
	if len(links) == 0 { // we reached the bottom, we are a leaf.
		err := store.heads.Add(top, depth)
		if err != nil {
			return errors.Wrapf(err, "error adding head %s", top)
		}
		store.logger.Debugf("new head %s@%d", top, depth)
		return nil
	}

	// walkToChildren
	for _, l := range links {
		child := l.Cid
		isHead, height, err := store.heads.IsHead(child)
		if err != nil {
			return errors.Wrapf(err, "error checking if %s is head", child)
		}

		if isHead {
			// reached one of the current heads. Replace it with
			// the tip of this branch
			err := store.heads.Replace(child, top, height+depth)
			if err != nil {
				return errors.Wrapf(err, "error replacing head: %s->%s", child, top)
			}

			store.logger.Debugf("replaced head %s with %s@%d", child, top, height+depth)
			continue
		}

		known, err := store.dags.IsKnownBlock(child)
		if err != nil {
			return errors.Wrapf(err, "error checking for known block %s", child)
		}
		if known {
			// we reached a non-head node in the known tree.
			// This means our top block is a new head.
			height, err := store.dags.GetPriority(store.ctx, child)
			if err != nil {
				return errors.Wrapf(err, "error getting height for block %s", child)
			}
			store.heads.Add(top, height+depth)
			store.logger.Debugf("new head %s@%d", top, height+depth)
			continue
		}

		// TODO: parallelize
		err = store.walkBranch(child, top, depth+1)
		if err != nil {
			return err
		}
	}

	return nil
}

// Get retrieves the object `value` named by `key`.
// Get will return ErrNotFound if the key is not mapped to a value.
func (store *Datastore) Get(key ds.Key) (value []byte, err error) {
	return store.set.Element(key.String())
}

// Has returns whether the `key` is mapped to a `value`.
// In some contexts, it may be much cheaper only to check for existence of
// a value, rather than retrieving the value itself. (e.g. HTTP HEAD).
// The default implementation is found in `GetBackedHas`.
func (store *Datastore) Has(key ds.Key) (exists bool, err error) {
	return store.set.InSet(key.String())
}

// GetSize returns the size of the `value` named by `key`.
// In some contexts, it may be much cheaper to only get the size of the
// value rather than retrieving the value itself.
func (store *Datastore) GetSize(key ds.Key) (size int, err error) {
	return ds.GetBackedSize(store, key)
}

// Query searches the datastore and returns a query result. This function
// may return before the query actually runs. To wait for the query:
//
//   result, _ := ds.Query(q)
//
//   // use the channel interface; result may come in at different times
//   for entry := range result.Next() { ... }
//
//   // or wait for the query to be completely done
//   entries, _ := result.Rest()
//   for entry := range entries { ... }
//
func (store *Datastore) Query(q query.Query) (query.Results, error) {
	qr := query.ResultsWithChan(q, store.set.Elements())
	return query.NaiveQueryApply(q, qr), nil
}

// Put stores the object `value` named by `key`.
func (store *Datastore) Put(key ds.Key, value []byte) error {
	delta := store.set.Add(key.String(), value)
	return store.publish(delta)
}

// Delete removes the value for given `key`.
func (store *Datastore) Delete(key ds.Key) error {
	delta, err := store.set.Rmv(key.String())
	if err != nil {
		return err
	}

	if len(delta.Tombstones) == 0 {
		return ds.ErrNotFound
	}
	return store.publish(delta)
}

// Close shuts down the CRDT datastore. It should not be used afterwards.
func (store *Datastore) Close() error {
	store.cancel()
	return nil
}

// Batch implements batching for writes by accumulating
// Put and Delete in the same CRDT-delta and only applying it and
// broadcasting it on Commit().
func (store *Datastore) Batch() (ds.Batch, error) {
	return &batch{store: store}, nil
}

func deltaMerge(d1, d2 *pb.Delta) *pb.Delta {
	result := &pb.Delta{
		Elements:   append(d1.GetElements(), d2.GetElements()...),
		Tombstones: append(d1.GetTombstones(), d2.GetTombstones()...),
		Priority:   d1.GetPriority(),
	}
	if h := d2.GetPriority(); h > result.Priority {
		result.Priority = h
	}
	return result
}

func (store *Datastore) addToDelta(key string, value []byte) error {
	store.updateDelta(store.set.Add(key, value))
	return nil

}

func (store *Datastore) rmvToDelta(key string) error {
	delta, err := store.set.Rmv(key)
	if err != nil {
		return err
	}

	store.updateDeltaWithRemove(key, delta)
	return nil
}

// to satisfy datastore semantics, we need to remove elements from the current
// batch if they were added.
func (store *Datastore) updateDeltaWithRemove(key string, newDelta *pb.Delta) {
	store.curDeltaMux.Lock()
	defer store.curDeltaMux.Unlock()
	elems := make([]*pb.Element, 0)
	for _, e := range store.curDelta.GetElements() {
		if e.GetKey() != key {
			elems = append(elems, e)
		}
	}
	store.curDelta = &pb.Delta{
		Elements:   elems,
		Tombstones: store.curDelta.GetTombstones(),
		Priority:   store.curDelta.GetPriority(),
	}
	store.curDelta = deltaMerge(store.curDelta, newDelta)
}

func (store *Datastore) updateDelta(newDelta *pb.Delta) {
	store.curDeltaMux.Lock()
	defer store.curDeltaMux.Unlock()
	store.curDelta = deltaMerge(store.curDelta, newDelta)
}

func (store *Datastore) publishDelta() error {
	store.curDeltaMux.Lock()
	defer store.curDeltaMux.Unlock()
	err := store.publish(store.curDelta)
	if err != nil {
		return err
	}
	store.curDelta = nil
	return nil
}

func (store *Datastore) putBlock(heads []cid.Cid, height uint64, delta *pb.Delta) (cid.Cid, error) {
	if delta != nil {
		delta.Priority = height
	}
	node, err := makeNode(delta, heads)
	if err != nil {
		return cid.Undef, errors.Wrap(err, "error creating new block")
	}
	err = store.dags.Add(store.ctx, node)
	if err != nil {
		return cid.Undef, errors.Wrapf(err, "error writing new block %s", node.Cid())
	}
	return node.Cid(), nil
}

func (store *Datastore) publish(delta *pb.Delta) error {
	c, err := store.addDAGNode(delta)
	if err != nil {
		return err
	}
	return store.broadcast(c)
}

func (store *Datastore) addDAGNode(delta *pb.Delta) (cid.Cid, error) {
	heads, height, err := store.heads.List()
	if err != nil {
		return cid.Undef, errors.Wrap(err, "error listing heads")
	}
	height = height + 1

	delta.Priority = height

	// for _, e := range delta.GetElements() {
	// 	e.Value = append(e.GetValue(), []byte(fmt.Sprintf(" height: %d", height))...)
	// }

	c, err := store.putBlock(heads, height, delta)
	if err != nil {
		return cid.Undef, err
	}

	// Process new block. This makes that every operation applied
	// to this store takes effect (delta is merged) before
	// returning. Optimizations possible by not fetching the block
	// again from the DAG and passing it directly.
	store.logger.Debugf("processing generated block %s", c)
	err = store.walkBranch(c, c, 1)
	if err != nil {
		return cid.Undef, errors.Wrap(err, "error processing new block")
	}

	return c, nil
}

func (store *Datastore) broadcast(c cid.Cid) error {
	if store.broadcaster == nil { // offline
		return nil
	}
	store.logger.Debugf("broadcasting %s", c)
	err := store.broadcaster.Broadcast(c.Bytes())
	if err != nil {
		return errors.Wrapf(err, "error broadcasting %s", c)
	}
	return nil
}

// IsThreadSafe declares that this datastore implementation is thread-safe.
func (store *Datastore) IsThreadSafe() {}

type batch struct {
	store *Datastore
}

func (b *batch) Put(key ds.Key, value []byte) error {
	return b.store.addToDelta(key.String(), value)
}

func (b *batch) Delete(key ds.Key) error {
	return b.store.rmvToDelta(key.String())
}

func (b *batch) Commit() error {
	return b.store.publishDelta()
}

// PrintDAG pretty prints the current Merkle-DAG using the given printFunc
func (store *Datastore) PrintDAG() error {
	heads, _, err := store.heads.List()
	if err != nil {
		return err
	}
	for _, h := range heads {
		err := store.printDAGRec(h, 0)
		if err != nil {
			return err
		}
	}
	return nil
}

func (store *Datastore) printDAGRec(from cid.Cid, depth uint64) error {
	nd, delta, err := store.dags.GetDelta(context.Background(), from)
	if err != nil {
		return err
	}

	line := ""
	for i := uint64(0); i < depth; i++ {
		line += " "
	}

	line += fmt.Sprintf("- %d | %s: ", delta.GetPriority(), nd.Cid().String()[0:4])
	line += "Add: {"
	for _, e := range delta.GetElements() {
		line += fmt.Sprintf("%s,", e.GetKey())
	}
	line += "}. Rmv: {"
	for _, e := range delta.GetTombstones() {
		line += fmt.Sprintf("%s,", e.GetKey())
	}
	line += "}:"
	fmt.Println(line)
	for _, l := range nd.Links() {
		store.printDAGRec(l.Cid, depth+1)
	}
	return nil
}
