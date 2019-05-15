// Package crdt provides a replicated go-datastore (key-value store)
// implementation using Merkle-CRDTs built with IPLD nodes.
//
// This Datastore is agnostic to how new MerkleDAG roots are broadcasted to
// the rest of replicas (`Broadcaster` component) and to how the IPLD nodes
// are made discoverable and retrievable to by other replicas (`DAGSyncer`
// component).
//
// The implementation is based on the "Merkle-CRDTs: Merkle-DAGs meet CRDTs"
// paper by Héctor Sanjuán, Samuli Pöyhtäri and Pedro Teixeira.
//
// Note that, in the absence of compaction (which must be performed manually),
// a crdt.Datastore will only grow in size even when keys are deleted.
//
// The time to be fully synced for new Datastore replicas will depend on how
// fast they can retrieve the DAGs announced by the other replicas, but newer
// values will be available before older ones.
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
	HasBlock(c cid.Cid) (bool, error)
}

// A SessionDAGSyncer is a Sessions-enabled DAGSyncer. This type of DAG-Syncer
// provides an optimized NodeGetter to make multiple related requests. The
// same session-enabled NodeGetter is used to download DAG branches when
// the DAGSyncer supports it.
type SessionDAGSyncer interface {
	DAGSyncer
	Session(context.Context) ipld.NodeGetter
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
	// NumWorkers specifies the number of workers ready to walk DAGs
	NumWorkers int
	// DAGSyncerTimeout specifies how long to wait for a DAGSyncer.
	// Set to 0 to disable.
	DAGSyncerTimeout time.Duration
}

func (opts *Options) verify() error {
	if opts == nil {
		return errors.New("options cannot be nil")
	}

	if opts.RebroadcastInterval <= 0 {
		return errors.New("invalid RebroadcastInterval")
	}

	if opts.Logger == nil {
		return errors.New("the Logger is undefined")
	}

	if opts.NumWorkers <= 0 {
		return errors.New("bad number of NumWorkers")
	}

	if opts.DAGSyncerTimeout < 0 {
		return errors.New("invalid DAGSyncerTimeout")
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
		NumWorkers:          5,
		DAGSyncerTimeout:    5 * time.Minute,
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

	dagSyncer   DAGSyncer
	broadcaster Broadcaster

	lastHeadTSMux sync.RWMutex
	lastHeadTS    time.Time

	rebroadcastTicker *time.Ticker

	curDeltaMux sync.Mutex
	curDelta    *pb.Delta // current, unpublished delta

	wg sync.WaitGroup

	jobQueue chan *dagJob
	sendJobs chan *dagJob
}

type dagJob struct {
	session    *sync.WaitGroup // A waitgroup to wait for all related jobs to conclude
	nodeGetter *crdtNodeGetter // a node getter to use
	root       cid.Cid         // the root of the branch we are walking down
	rootPrio   uint64          // the priority of the root delta
	delta      *pb.Delta       // the current delta
	node       ipld.Node       // the current ipld Node

}

// New returns a Merkle-CRDT-based Datastore using the given one to persist
// all the necessary data under the given namespace. It needs a DAG-Syncer
// component for IPLD nodes and a Broadcaster component to distribute and
// receive information to and from the rest of replicas. Actual implementation
// of these must be provided by the user.
//
// The CRDT-Datastore should call Close() before the given store is closed.
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
	heads := newHeads(store, fullHeadsNs, opts.Logger)

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
		dagSyncer:         dagSyncer,
		broadcaster:       bcast,
		rebroadcastTicker: time.NewTicker(opts.RebroadcastInterval),
		jobQueue:          make(chan *dagJob),
		sendJobs:          make(chan *dagJob),
	}

	headList, maxHeight, err := dstore.heads.List()
	if err != nil {
		return nil, err
	}
	dstore.logger.Infof(
		"crdt Datastore created. Number of heads: %d. Current max-height: %d",
		len(headList),
		maxHeight,
	)

	dstore.wg.Add(1)
	go dstore.sendJobWorker()
	for i := 0; i < dstore.opts.NumWorkers; i++ {
		dstore.wg.Add(1)
		go func() {
			defer dstore.wg.Done()
			dstore.dagWorker()
		}()
	}
	dstore.wg.Add(2)
	go dstore.handleNext()
	go dstore.rebroadcast()

	return dstore, nil
}

func (store *Datastore) handleNext() {
	defer store.wg.Done()

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

		c, err := cid.Cast(data)
		if err != nil {
			store.logger.Error(err)
			continue
		}

		go func(c cid.Cid) {
			err = store.handleBlock(c)
			if err != nil {
				store.logger.Error(err)
			}
		}(c)

		store.lastHeadTSMux.Lock()
		store.lastHeadTS = time.Now()
		store.lastHeadTSMux.Unlock()
	}
}

func (store *Datastore) rebroadcast() {
	defer store.wg.Done()

	ticker := store.rebroadcastTicker
	defer ticker.Stop()
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
		return
	}

	for _, h := range heads {
		store.broadcast(h)
	}
}

// handleBlock takes care of vetting, retrieving and applying
// CRDT blocks to the Datastore.
func (store *Datastore) handleBlock(c cid.Cid) error {
	// Ignore already known blocks.
	// This includes the case when the block is a current
	// head.
	known, err := store.dagSyncer.HasBlock(c)
	if err != nil {
		return errors.Wrap(err, "error checking for known block")
	}
	if known {
		store.logger.Debugf("%s is known. Skip walking tree", c)
		return nil
	}

	// Walk down from this block.
	ctx, cancel := context.WithCancel(store.ctx)
	defer cancel()

	dg := &crdtNodeGetter{NodeGetter: store.dagSyncer}
	if sessionMaker, ok := store.dagSyncer.(SessionDAGSyncer); ok {
		dg = &crdtNodeGetter{NodeGetter: sessionMaker.Session(ctx)}
	}

	var session sync.WaitGroup
	store.sendNewJobs(&session, dg, c, 0, []cid.Cid{c})
	session.Wait()
	return nil
}

// dagWorker shouold run in its own gorountine. Workers are launched during
// initialization in New().
func (store *Datastore) dagWorker() {
	for job := range store.jobQueue {
		children, err := store.processNode(
			job.nodeGetter,
			job.root,
			job.rootPrio,
			job.delta,
			job.node,
		)

		if err != nil {
			store.logger.Error(err)
			job.session.Done()
			continue
		}
		go func(j *dagJob) {
			store.sendNewJobs(j.session, j.nodeGetter, j.root, j.rootPrio, children)
			j.session.Done()
		}(job)
	}
}

// sendNewJobs calls getDeltas (GetMany) on the crdtDAGService with the given
// children and sends each response to the workers. It will block until all
// jobs have been queued.
func (store *Datastore) sendNewJobs(session *sync.WaitGroup, ng *crdtNodeGetter, root cid.Cid, rootPrio uint64, children []cid.Cid) {
	if len(children) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(store.ctx, store.opts.DAGSyncerTimeout)
	defer cancel()

	// Special case for root
	if rootPrio == 0 {
		prio, err := ng.GetPriority(ctx, children[0])
		if err != nil {
			store.logger.Error("error getting root delta priority: %s", err)
		}
		rootPrio = prio
	}

	for deltaOpt := range ng.GetDeltas(ctx, children) {
		if deltaOpt.err != nil {
			store.logger.Errorf("error getting delta: %s", deltaOpt.err)
			continue
		}

		session.Add(1)
		job := &dagJob{
			session:    session,
			nodeGetter: ng,
			root:       root,
			delta:      deltaOpt.delta,
			node:       deltaOpt.node,
			rootPrio:   rootPrio,
		}
		select {
		case store.sendJobs <- job:
		case <-store.ctx.Done():
			return
		}
	}

}

// the only purpose of this worker is to be able to orderly shut-down job
// workers without races by becoming the only sender for the store.jobQueue
// channel.
func (store *Datastore) sendJobWorker() {
	defer store.wg.Done()
	for {
		select {
		case <-store.ctx.Done():
			close(store.jobQueue)
			return
		case j := <-store.sendJobs:
			store.jobQueue <- j
		}
	}
}

func (store *Datastore) processNode(ng *crdtNodeGetter, root cid.Cid, rootPrio uint64, delta *pb.Delta, node ipld.Node) ([]cid.Cid, error) {
	// merge the delta
	current := node.Cid()
	err := store.set.Merge(delta, dshelp.CidToDsKey(current).String())
	if err != nil {
		return nil, errors.Wrapf(err, "error merging delta from %s", current)
	}

	if prio := delta.GetPriority(); prio%10 == 0 {
		store.logger.Infof("merged delta from %s (priority: %d)", current, prio)
	} else {
		store.logger.Debugf("merged delta from %s (priority: %d)", current, prio)
	}

	links := node.Links()
	if len(links) == 0 { // we reached the bottom, we are a leaf.
		err := store.heads.Add(root, rootPrio)
		if err != nil {
			return nil, errors.Wrapf(err, "error adding head %s", root)
		}
		return nil, nil
	}

	children := []cid.Cid{}

	// walkToChildren
	for _, l := range links {
		child := l.Cid
		isHead, _, err := store.heads.IsHead(child)
		if err != nil {
			return nil, errors.Wrapf(err, "error checking if %s is head", child)
		}

		if isHead {
			// reached one of the current heads. Replace it with
			// the tip of this branch
			err := store.heads.Replace(child, root, rootPrio)
			if err != nil {
				return nil, errors.Wrapf(err, "error replacing head: %s->%s", child, root)
			}

			continue
		}

		known, err := store.dagSyncer.HasBlock(child)
		if err != nil {
			return nil, errors.Wrapf(err, "error checking for known block %s", child)
		}
		if known {
			// we reached a non-head node in the known tree.
			// This means our root block is a new head.
			store.heads.Add(root, rootPrio)
			continue
		}

		children = append(children, child)
	}

	return children, nil
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
	store.wg.Wait()
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

func (store *Datastore) putBlock(heads []cid.Cid, height uint64, delta *pb.Delta) (ipld.Node, error) {
	if delta != nil {
		delta.Priority = height
	}
	node, err := makeNode(delta, heads)
	if err != nil {
		return nil, errors.Wrap(err, "error creating new block")
	}

	ctx, cancel := context.WithTimeout(store.ctx, store.opts.DAGSyncerTimeout)
	defer cancel()
	err = store.dagSyncer.Add(ctx, node)
	if err != nil {
		return nil, errors.Wrapf(err, "error writing new block %s", node.Cid())
	}

	return node, nil
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
	height = height + 1 // This implies our minimum height is 1

	delta.Priority = height

	// for _, e := range delta.GetElements() {
	// 	e.Value = append(e.GetValue(), []byte(fmt.Sprintf(" height: %d", height))...)
	// }

	nd, err := store.putBlock(heads, height, delta)
	if err != nil {
		return cid.Undef, err
	}

	// Process new block. This makes that every operation applied
	// to this store take effect (delta is merged) before
	// returning. Since our block references current heads, children
	// should be empty
	store.logger.Debugf("processing generated block %s", nd.Cid())
	children, err := store.processNode(
		&crdtNodeGetter{store.dagSyncer},
		nd.Cid(),
		height,
		delta,
		nd,
	)
	if err != nil {
		return cid.Undef, errors.Wrap(err, "error processing new block")
	}
	if len(children) != 0 {
		store.logger.Warningf("bug: created a block to unknown children")
	}

	return nd.Cid(), nil
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

	ng := &crdtNodeGetter{NodeGetter: store.dagSyncer}

	for _, h := range heads {
		err := store.printDAGRec(h, 0, ng)
		if err != nil {
			return err
		}
	}
	return nil
}

func (store *Datastore) printDAGRec(from cid.Cid, depth uint64, ng *crdtNodeGetter) error {
	nd, delta, err := ng.GetDelta(context.Background(), from)
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
		line += fmt.Sprintf("%s:%s,", e.GetKey(), e.GetValue())
	}
	line += "}. Rmv: {"
	for _, e := range delta.GetTombstones() {
		line += fmt.Sprintf("%s,", e.GetKey())
	}
	line += "}. Links: {"
	for _, l := range nd.Links() {
		line += fmt.Sprintf("%s,", l.Cid.String()[0:4])
	}
	line += "}:"
	fmt.Println(line)
	for _, l := range nd.Links() {
		store.printDAGRec(l.Cid, depth+1, ng)
	}
	return nil
}
