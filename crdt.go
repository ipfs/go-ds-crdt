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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	dshelp "github.com/ipfs/boxo/datastore/dshelp"
	"github.com/ipfs/boxo/exchange/offline"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	pb "github.com/ipfs/go-ds-crdt/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/proto"

	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
)

var _ ds.Datastore = (*Datastore)(nil)
var _ ds.Batching = (*Datastore)(nil)

var jobTimeout = 24 * time.Hour

// datastore namespace keys. Short keys save space and memory.
const (
	headsNs           = "h" // state
	membersNs         = "m" // member
	setNs             = "s" // set
	processedBlocksNs = "b" // blocks
	dirtyBitKey       = "d" // dirty
	versionKey        = "crdt_version"
	snapshotKey       = "g"
	jobsNs            = "j" // jobs
)

type ProcessedState byte

const (
	ProcessedComplete ProcessedState = iota
	FetchRequested
	Fetched
)

// Child status constants
const (
	ChildPending byte = iota
	ChildRequested
	ChildFetched
	ChildProcessed
)

// Common errors.
var (
	ErrNoMoreBroadcast = errors.New("receiving blocks aborted since no new blocks will be broadcasted")
)

// A Broadcaster provides a way to send (notify) an opaque payload to
// all replicas and to retrieve payloads broadcasted.
type Broadcaster interface {
	// Send payload to other replicas.
	Broadcast(context.Context, []byte) error
	// Obtain the next payload received from the network.
	Next(context.Context) ([]byte, error)
}

// A SessionDAGService is a Sessions-enabled DAGService. This type of DAG-Service
// provides an optimized NodeGetter to make multiple related requests. The
// same session-enabled NodeGetter is used to download DAG branches when
// the DAGSyncer supports it.
type SessionDAGService interface {
	ipld.DAGService
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
	// still present in the datastore because it was re-added or not fully
	// tombstoned. If that is relevant, use Has() to check if the removed
	// element is still part of the datastore.
	DeleteHook func(k ds.Key)
	// NumWorkers specifies the number of workers ready to walk DAGs
	NumWorkers int
	// DAGSyncerTimeout specifies how long to wait for a DAGSyncer.
	// Set to 0 to disable.
	DAGSyncerTimeout time.Duration
	// MaxBatchDeltaSize will automatically commit any batches whose
	// delta size gets too big. This helps keep DAG nodes small
	// enough that they will be transferred by the network.
	MaxBatchDeltaSize int
	// RepairInterval specifies how often to walk the full DAG until
	// the root(s) if it has been marked dirty. 0 to disable.
	RepairInterval time.Duration
	// MultiHeadProcessing lets several new state to be processed in
	// parallel.  This results in more branching in general. More
	// branching is not necessarily a bad thing and may improve
	// throughput, but everything depends on usage.
	MultiHeadProcessing bool

	// CompactInterval specifies how often to attempt to compact the DAG.
	CompactInterval time.Duration

	//CompactDagSize the height of the dag which would trigger a compaction.
	CompactDagSize uint64
	// CompactRetainNodes defines how many recent nodes should remain after DAG compaction
	CompactRetainNodes uint64

	//MembershipHook function is triggered whenever membership changes or is updated.
	MembershipHook func(members map[string]*pb.Participant)

	// TTL is the duration this node is considered lost when its isolated from peers ( figured out on rejoin )
	TTL time.Duration
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

	if opts.MaxBatchDeltaSize <= 0 {
		return errors.New("invalid MaxBatchDeltaSize")
	}

	if opts.RepairInterval < 0 {
		return errors.New("invalid RepairInterval")
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
		// always keeping
		// https://github.com/libp2p/go-libp2p-core/blob/master/network/network.go#L23
		// in sight
		MaxBatchDeltaSize:   1 * 1024 * 1024, // 1MB,
		RepairInterval:      time.Hour,
		MultiHeadProcessing: false,
		TTL:                 time.Hour * 24 * 7,
		CompactInterval:     time.Hour,
		CompactDagSize:      10000,
		CompactRetainNodes:  1000,
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
	state     *StateManager

	dagService  ipld.DAGService
	bs          blockstore.Blockstore
	broadcaster Broadcaster

	seenHeadsMux sync.RWMutex
	seenHeads    map[cid.Cid]struct{}

	compactMux sync.Mutex

	curDeltaMux sync.Mutex
	curDelta    *pb.Delta // current, unpublished delta

	wg sync.WaitGroup

	jobQueue chan *dagJob
	sendJobs chan *dagJob
	// keep track of children to be fetched so only one job does every
	// child
	queuedChildren *cidSafeSet
	h              Peer
	seenSnapshots  *ringSnapshots
}

type dagJob struct {
	ctx        context.Context // A job context for tracing
	session    *sync.WaitGroup // A waitgroup to wait for all related jobs to conclude
	nodeGetter *crdtNodeGetter // a node getter to use
	root       cid.Cid         // the root of the branch we are walking down
	rootPrio   uint64          // the priority of the root delta
	delta      *pb.Delta       // the current delta
	node       ipld.Node       // the current ipld Node
}

type Peer interface {
	ID() peer.ID
}

// New returns a Merkle-CRDT-based Datastore using the given one to persist
// all the necessary data under the given namespace. It needs a DAG-Service
// component for IPLD nodes and a Broadcaster component to distribute and
// receive information to and from the rest of replicas. Actual implementation
// of these must be provided by the user, but it normally means using
// ipfs-lite (https://github.com/hsanjuan/ipfs-lite) as a DAG Service and the
// included libp2p PubSubBroadcaster as a Broadcaster.
//
// The given Datastore is used to back all CRDT-datastore contents and
// accounting information. When using an asynchronous datastore, the user is
// in charge of calling Sync() regularly. Sync() will persist paths related to
// the given prefix, but note that if other replicas are modifying the
// datastore, the prefixes that will need syncing are not only those modified
// by the local replica. Therefore the user should consider calling Sync("/"),
// with an empty prefix, in that case, or use a synchronous underlying
// datastore that persists things directly on write.
//
// The CRDT-Datastore should call Close() before the given store is closed.
func New(h Peer, store ds.Datastore, bs blockstore.Blockstore, namespace ds.Key, dagSyncer ipld.DAGService, bcast Broadcaster, opts *Options) (*Datastore, error) {
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

	// <namespace>/state
	stateNs := namespace.ChildString(membersNs)

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

	ctx, cancel := context.WithCancel(context.Background())
	set, err := newCRDTSet(ctx, store, fullSetNs, dagSyncer, opts.Logger, setPutHook, setDeleteHook)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("error setting up crdt set: %w", err)
	}
	heads, err := newHeads(ctx, store, fullHeadsNs, opts.Logger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("error building heads: %w", err)
	}

	// load the state
	sm, err := NewStateManager(ctx, store, stateNs, opts.TTL, opts.Logger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("error building statemanager: %w", err)
	}

	sm.SetMembershipUpdateCallback(opts.MembershipHook)

	dstore := &Datastore{
		h:              h,
		ctx:            ctx,
		cancel:         cancel,
		opts:           opts,
		logger:         opts.Logger,
		store:          store,
		bs:             bs,
		namespace:      namespace,
		set:            set,
		heads:          heads,
		state:          sm,
		dagService:     dagSyncer,
		broadcaster:    bcast,
		seenHeads:      make(map[cid.Cid]struct{}),
		jobQueue:       make(chan *dagJob, opts.NumWorkers),
		sendJobs:       make(chan *dagJob),
		queuedChildren: newCidSafeSet(),
		seenSnapshots:  newRingSnapshots(10),
	}

	err = dstore.applyMigrations(ctx)
	if err != nil {
		cancel()
		return nil, err
	}

	headList, maxHeight, err := dstore.heads.List(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	dstore.logger.Infof(
		"crdt Datastore created. Number of state: %d. Current max-height: %d. Dirty: %t",
		len(headList),
		maxHeight,
		dstore.IsDirty(ctx),
	)

	// sendJobWorker + NumWorkers
	dstore.wg.Add(1 + dstore.opts.NumWorkers)
	go func() {
		defer dstore.wg.Done()
		dstore.sendJobWorker(ctx)
	}()
	for i := 0; i < dstore.opts.NumWorkers; i++ {
		go func() {
			defer dstore.wg.Done()
			dstore.dagWorker()
		}()
	}
	dstore.wg.Add(6)
	go func() {
		defer dstore.wg.Done()
		dstore.handleNext(ctx)
	}()
	go func() {
		defer dstore.wg.Done()
		dstore.rebroadcast(ctx)
	}()

	go func() {
		defer dstore.wg.Done()
		dstore.repair(ctx)
	}()

	go func() {
		defer dstore.wg.Done()
		dstore.logStats(ctx)
	}()

	go func() {
		defer dstore.wg.Done()
		dstore.compact()
	}()

	go func() {
		defer dstore.wg.Done()
	}()

	// Restore persisted jobs on startup
	if err := dstore.restorePersistedJobs(ctx); err != nil {
		cancel()
		return nil, fmt.Errorf("error restoring persisted jobs: %w", err)
	}

	// Start job cleanup goroutine
	dstore.wg.Add(1)
	go func() {
		defer dstore.wg.Done()
		dstore.jobCleanup(ctx)
	}()

	return dstore, nil
}

func (store *Datastore) handleNext(ctx context.Context) {
	if store.broadcaster == nil { // offline
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		data, err := store.broadcaster.Next(ctx)
		if err != nil {
			if err == ErrNoMoreBroadcast || ctx.Err() != nil {
				return
			}
			store.logger.Error(err)
			continue
		}

		broadcast, err := store.decodeBroadcast(ctx, data)
		if err != nil {
			store.logger.Error(err)
			continue
		}

		store.logger.Debugf("Received broadcast with %d members", len(broadcast.Members))
		for id, member := range broadcast.Members {
			headCount := len(member.DagHeads)
			store.logger.Debugf("  Received member %s: %d heads, bestBefore=%d", id, headCount, member.BestBefore)
		}

		// Always merge member metadata, even if we diverge
		if err := store.state.MergeMembers(ctx, broadcast); err != nil {
			store.logger.Errorf("failed to merge broadcast: %v", err)
			continue
		}

		for peerID, participant := range broadcast.Members {
			// Skip our own participant data - we don't need to fast-forward to ourselves
			if peerID == store.h.ID().String() {
				continue
			}

			// Get our snapshot for comparison
			var mySnapshot *pb.Snapshot
			myParticipant := store.GetState(ctx).Members[store.h.ID().String()]
			if myParticipant != nil {
				mySnapshot = myParticipant.Snapshot
			}

			if snapshotsEqual(mySnapshot, participant.Snapshot) {
				// Snapshots match, we can safely process heads
				store.processParticipantHeads(ctx, peerID, participant)
				continue
			}

			if participant.Snapshot == nil {
				continue // No snapshot, skip
			}

			if store.hasProcessedSnapshot(cidFromBytes(participant.Snapshot.DagHead.Cid)) {
				continue // Already seen, skip
			}

			// Snapshots differ, attempt fast-forward
			err := store.tryFastForwardToSnapshot(ctx, mySnapshot, participant)
			if err != nil {
				store.logger.Warnf("divergence detected with %s: %v", peerID, err)
				store.handleDivergence(ctx, participant.Snapshot)
			}
		}
	}
}

func (store *Datastore) hasProcessedSnapshot(c cid.Cid) bool {
	if c == cid.Undef {
		return false
	}
	return store.seenSnapshots.Contains(c)
}

func (store *Datastore) processHead(ctx context.Context, c cid.Cid) {
	err := store.handleBlock(ctx, c) // handleBlock blocks
	if err != nil {
		store.logger.Errorf("error processing new head: %s", err)
		// Do NOT mark dirty here — see previous logic notes
	}
}

func (store *Datastore) processParticipantHeads(ctx context.Context, peerID string, participant *pb.Participant) {
	bcastHeads := map[cid.Cid]cid.Cid{}

	for _, h := range participant.DagHeads {
		c, err := cid.Cast(h.Cid)
		if err == nil && c != cid.Undef {
			bcastHeads[c] = c
		}
	}

	for head := range bcastHeads {
		// Same logic as before
		store.seenHeadsMux.Lock()
		store.seenHeads[head] = struct{}{}
		store.seenHeadsMux.Unlock()

		if store.opts.MultiHeadProcessing {
			go store.processHead(ctx, head)
		} else {
			store.processHead(ctx, head)
		}
	}
}

func (store *Datastore) handleDivergence(ctx context.Context, target *pb.Snapshot) {
	// TODO: Apply divergence policy
	store.logger.Warn("divergence handling not implemented")
}

func snapshotsEqual(a, b *pb.Snapshot) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return bytes.Equal(a.SnapshotKey.Cid, b.SnapshotKey.Cid)
}

func cidFromBytes(b []byte) cid.Cid {
	c, _ := cid.Cast(b)
	return c
}

func (store *Datastore) decodeBroadcast(ctx context.Context, data []byte) (*pb.StateBroadcast, error) {
	// Make a list of state we received
	bcastData := pb.StateBroadcast{}
	err := proto.Unmarshal(data, &bcastData)
	if err != nil {
		return nil, err
	}

	return &bcastData, nil
}

func (store *Datastore) encodeBroadcast(ctx context.Context, state *pb.StateBroadcast) ([]byte, error) {
	return proto.Marshal(state)
}

func randomizeInterval(d time.Duration) time.Duration {
	// 30% of the configured interval
	leeway := (d * 30 / 100)
	// A random number between -leeway|+leeway
	randGen := rand.New(rand.NewSource(time.Now().UnixNano()))
	randomInterval := time.Duration(randGen.Int63n(int64(leeway*2))) - leeway
	return d + randomInterval
}

func (store *Datastore) rebroadcast(ctx context.Context) {
	timer := time.NewTimer(randomizeInterval(store.opts.RebroadcastInterval))

	for {
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
			store.rebroadcastHeads(ctx)
			timer.Reset(randomizeInterval(store.opts.RebroadcastInterval))
		}
	}
}

func (store *Datastore) repair(ctx context.Context) {
	if store.opts.RepairInterval == 0 {
		return
	}
	timer := time.NewTimer(0) // fire immediately on start
	for {
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
			if !store.IsDirty(ctx) {
				store.logger.Info("store is marked clean. No need to repair")
			} else {
				store.logger.Warn("store is marked dirty. Starting DAG repair operation")
				err := store.repairDAG(ctx)
				if err != nil {
					store.logger.Error(err)
				}
			}
			timer.Reset(store.opts.RepairInterval)
		}
	}
}

// regularly send out a list of state that we have not recently seen
func (store *Datastore) rebroadcastHeads(ctx context.Context) {
	// Get our current list of state
	heads, _, err := store.heads.List(ctx)
	if err != nil {
		store.logger.Error(err)
		return
	}

	var headsToBroadcast []cid.Cid
	store.seenHeadsMux.RLock()
	{
		headsToBroadcast = make([]cid.Cid, 0, len(store.seenHeads))
		for _, h := range heads {
			if _, ok := store.seenHeads[h]; !ok {
				headsToBroadcast = append(headsToBroadcast, h)
			}
		}
	}
	store.seenHeadsMux.RUnlock()
	store.logger.Debugf("rebroadcastHeads %d", len(headsToBroadcast))

	if len(headsToBroadcast) > 0 {
		err = store.state.UpdateHeads(ctx, store.h.ID(), headsToBroadcast, true)
		if err != nil {
			store.logger.Warn("broadcast failed: %v", err)
		}

		// Send them out
		err = store.broadcast(ctx)
		if err != nil {
			store.logger.Warn("broadcast failed: %v", err)
		}
	}

	// Reset the map
	store.seenHeadsMux.Lock()
	store.seenHeads = make(map[cid.Cid]struct{})
	store.seenHeadsMux.Unlock()
}

// Log some stats every 5 minutes.
func (store *Datastore) logStats(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	for {
		select {
		case <-ticker.C:
			heads, height, err := store.heads.List(ctx)
			if err != nil {
				store.logger.Errorf("error listing state: %s", err)
			}

			store.logger.Infof(
				"Number of state: %d. Current max height: %d. Queued jobs: %d. Dirty: %t",
				len(heads),
				height,
				len(store.jobQueue),
				store.IsDirty(ctx),
			)
		case <-ctx.Done():
			ticker.Stop()
			return
		}
	}
}

// handleBlock takes care of vetting, retrieving and applying
// CRDT blocks to the Datastore.
func (store *Datastore) handleBlock(ctx context.Context, c cid.Cid) error {
	// Ignore already processed blocks.
	// This includes the case when the block is a current
	// head.
	isProcessed, err := store.isProcessed(ctx, c)
	if err != nil {
		return fmt.Errorf("error checking for known block %s: %w", c, err)
	}
	if isProcessed {
		store.logger.Debugf("%s is known. Skip walking tree", c)
		return nil
	}

	return store.handleBranch(ctx, c, c)
}

// send job starting at the given CID in a branch headed by a given head.
// this can be used to continue branch processing from a certain point.
func (store *Datastore) handleBranch(ctx context.Context, head, c cid.Cid) error {
	// Check if this should be a persistent job (from network/broadcast)

	// Check if job already exists
	exists, err := store.jobExists(ctx, head)
	if err != nil {
		return fmt.Errorf("error checking existing job: %w", err)
	}

	if !exists {
		// Create new persistent job
		if err := store.createJob(ctx, head); err != nil {
			return fmt.Errorf("error creating job: %w", err)
		}

		// Add initial child as pending
		if err := store.setChildStatus(ctx, head, c, ChildPending); err != nil {
			return fmt.Errorf("error adding initial child: %w", err)
		}

		store.logger.Debugf("created persistent job for head %s", head)
	}

	// Use existing logic but with persistence awareness
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	dg := &crdtNodeGetter{NodeGetter: store.dagService}
	if sessionMaker, ok := store.dagService.(SessionDAGService); ok {
		dg = &crdtNodeGetter{NodeGetter: sessionMaker.Session(cctx)}
	}

	var session sync.WaitGroup
	err = store.sendNewJobs(ctx, &session, dg, head, 0, []cid.Cid{c})
	session.Wait()
	return err
}

// dagWorker should run in its own goroutine. Workers are launched during
// initialization in New().
func (store *Datastore) dagWorker() {
	for job := range store.jobQueue {
		ctx := job.ctx
		select {
		case <-ctx.Done():
			// drain jobs from queue when we are done
			job.session.Done()
			continue
		default:
		}

		children, err := store.processNode(ctx, job.root, job.nodeGetter, job.rootPrio, job.delta, job.node)

		if err != nil {
			store.logger.Error(err)
			store.MarkDirty(ctx)
			job.session.Done()
			continue
		}
		go func(j *dagJob) {
			err := store.sendNewJobs(ctx, j.session, j.nodeGetter, j.root, j.rootPrio, children)
			if err != nil {
				store.logger.Error(err)
				store.MarkDirty(ctx)
			}
			j.session.Done()
		}(job)
	}
}

// sendNewJobs calls getDeltas (GetMany) on the crdtNodeGetter with the given
// children and sends each response to the workers. It will block until all
// jobs have been queued.
func (store *Datastore) sendNewJobs(ctx context.Context, session *sync.WaitGroup, ng *crdtNodeGetter, root cid.Cid, rootPrio uint64, children []cid.Cid) error {
	if len(children) == 0 {
		return nil
	}

	// Update job children status if persistent
	for _, child := range children {
		// Only add if not already in job
		if status, _ := store.getChildStatus(ctx, root, child); status == 255 {
			if err := store.setChildStatus(ctx, root, child, ChildPending); err != nil {
				store.logger.Errorf("error setting job child status: %s", err)
			}
		}
	}

	cctx, cancel := context.WithTimeout(ctx, store.opts.DAGSyncerTimeout)
	defer cancel()

	// Special case for root priority
	if rootPrio == 0 {
		prio, err := ng.GetPriority(cctx, children[0])
		if err != nil {
			return fmt.Errorf("error getting root delta priority: %w", err)
		}
		rootPrio = prio
	}

	goodDeltas := make(map[cid.Cid]struct{})
	var err error

	// Mark children as requested
	for _, child := range children {
		err := store.setChildStatus(ctx, root, child, ChildRequested)
		if err != nil {
			return fmt.Errorf("error adding child to job: %w", err)
		}
		err = store.setBlockState(ctx, child, FetchRequested)
		if err != nil {
			return fmt.Errorf("error setting block state: %w", err)
		}
	}

loop:
	for deltaOpt := range ng.GetDeltas(cctx, children) {
		// we abort whenever we a delta comes back in error.
		if deltaOpt.err != nil {
			err = fmt.Errorf("error getting delta: %w", deltaOpt.err)
			break
		}

		childCID := deltaOpt.node.Cid()
		goodDeltas[childCID] = struct{}{}

		// Update persistence state
		err = store.setChildStatus(ctx, root, childCID, ChildFetched)
		if err != nil {
			return fmt.Errorf("error setting job child to status: %w", err)
		}
		err = store.setBlockState(ctx, childCID, Fetched)
		if err != nil {
			return fmt.Errorf("error setting block state: %w", err)
		}

		session.Add(1)
		job := &dagJob{
			ctx:        ctx,
			session:    session,
			nodeGetter: ng,
			root:       root,
			delta:      deltaOpt.delta,
			node:       deltaOpt.node,
			rootPrio:   rootPrio,
		}
		select {
		case store.sendJobs <- job:
		case <-ctx.Done():
			// the job was never sent, so it cannot complete.
			session.Done()
			// We are in the middle of sending jobs, thus we left
			// something unprocessed.
			err = ctx.Err()
			break loop
		}
	}

	// This is a safe-guard in case GetDeltas() returns less deltas than
	// asked for. It clears up any children that could not be fetched from
	// the queue. The rest will remove themselves in processNode().
	// Hector: as far as I know, this should not execute unless errors
	// happened.
	for _, child := range children {
		if _, ok := goodDeltas[child]; !ok {
			store.logger.Warn("GetDeltas did not include all children")
			store.queuedChildren.Remove(child)
		}
	}
	return err
}

// the only purpose of this worker is to be able to orderly shut-down job
// workers without races by becoming the only sender for the store.jobQueue
// channel.
func (store *Datastore) sendJobWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			if len(store.sendJobs) > 0 {
				// we left something in the queue
				store.MarkDirty(ctx)
			}
			close(store.jobQueue)
			return
		case j := <-store.sendJobs:
			store.jobQueue <- j
		}
	}
}

func (store *Datastore) processedBlockKey(c cid.Cid) ds.Key {
	return store.namespace.ChildString(processedBlocksNs).ChildString(dshelp.MultihashToDsKey(c.Hash()).String())
}

func (store *Datastore) isProcessed(ctx context.Context, c cid.Cid) (bool, error) {
	o, err := store.store.Get(ctx, store.processedBlockKey(c))

	if err != nil {
		if errors.Is(err, ds.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return len(o) > 0 && o[0] == byte(ProcessedComplete), nil
}

func (store *Datastore) markProcessed(ctx context.Context, c cid.Cid) error {
	return store.store.Put(ctx, store.processedBlockKey(c), []byte{byte(ProcessedComplete)})
}

func (store *Datastore) dirtyKey() ds.Key {
	return store.namespace.ChildString(dirtyBitKey)
}

// MarkDirty marks the Datastore as dirty.
func (store *Datastore) MarkDirty(ctx context.Context) {
	store.logger.Warn("marking datastore as dirty")
	err := store.store.Put(ctx, store.dirtyKey(), nil)
	if err != nil {
		store.logger.Errorf("error setting dirty bit: %s", err)
	}
}

// IsDirty returns whether the datastore is marked dirty.
func (store *Datastore) IsDirty(ctx context.Context) bool {
	ok, err := store.store.Has(ctx, store.dirtyKey())
	if err != nil {
		store.logger.Errorf("error checking dirty bit: %s", err)
	}
	return ok
}

// MarkClean removes the dirty mark from the datastore.
func (store *Datastore) MarkClean(ctx context.Context) {
	store.logger.Info("marking datastore as clean")
	err := store.store.Delete(ctx, store.dirtyKey())
	if err != nil {
		store.logger.Errorf("error clearing dirty bit: %s", err)
	}
}

// processNode merges the delta in a node and has the logic about what to do
// then.
func (store *Datastore) processNode(ctx context.Context, root cid.Cid, getter *crdtNodeGetter, rootPrio uint64, delta *pb.Delta, node ipld.Node) ([]cid.Cid, error) {
	// Check if all ancestors (children in DAG) are processed
	AllAncestorsReady := true
	var unprocessedAncestors []cid.Cid
	var headsToReplace []cid.Cid

	for _, link := range node.Links() {
		ancestorCID := link.Cid

		bs, err := store.getBlockState(ctx, ancestorCID)
		if err != nil {
			return nil, fmt.Errorf("error checking ancestor state %s: %w", ancestorCID, err)
		}

		isFetching := bs == FetchRequested
		isProcessed := bs == ProcessedComplete
		if !isFetching && !isProcessed {
			AllAncestorsReady = false
			unprocessedAncestors = append(unprocessedAncestors, ancestorCID)
		}
		if ok, _, err := store.heads.IsHead(ctx, ancestorCID); err == nil && ok {
			headsToReplace = append(headsToReplace, ancestorCID)
		} else if err != nil {
			return nil, fmt.Errorf("error checking ancestor is head %s: %w", ancestorCID, err)
		}
	}

	if !AllAncestorsReady {
		current := node.Cid()
		// Some ancestors not processed, mark as pending
		store.logger.Debugf("Block %s has unready ancestors", current)

		// Return unprocessed ancestors for continued processing
		return unprocessedAncestors, nil
	}

	jobNodes, ready, err := store.isJobReady(ctx, root)
	if err != nil {
		return nil, fmt.Errorf("error checking if job is ready: %w", err)
	}

	if !ready {
		store.logger.Debugf("Job '%s' is not yet ready", root)
		return unprocessedAncestors, nil
	}

	store.logger.Debugf("Job '%s' is ready, processing %d nodes", root, len(jobNodes))
	for _, child := range jobNodes {
		blockKey := dshelp.MultihashToDsKey(child.Hash()).String()
		// Get the specific delta for this child node
		_, childDelta, err := getter.GetDelta(ctx, child)
		if err != nil {
			return nil, fmt.Errorf("error getting delta for %s: %w", child, err)
		}

		if err := store.set.Merge(ctx, childDelta, blockKey); err != nil {
			return nil, fmt.Errorf("error merging delta from %s: %w", child, err)
		}
		// Mark as processed globally
		if err := store.markProcessed(ctx, child); err != nil {
			return nil, fmt.Errorf("error recording %s as processed: %w", child, err)
		}

		store.queuedChildren.Remove(child)

		err = store.setChildStatus(ctx, root, child, ChildProcessed)
		if err != nil {
			return nil, fmt.Errorf("error recording %s as processed: %w", child, err)
		}

		// Logging
		if prio := childDelta.GetPriority(); prio%50 == 0 {
			store.logger.Infof("merged delta from node %s (priority: %d)", child, prio)
		} else {
			store.logger.Debugf("merged delta from node %s (priority: %d)", child, prio)
		}

	}

	if err := store.completeJob(ctx, root); err != nil {
		store.logger.Errorf("error updating job progress: %s", err)
	}

	if len(headsToReplace) > 0 {
		for _, child := range headsToReplace {
			// Replace the existing head
			store.logger.Debugf("replacing head %s -> %s", child, root)
			if err := store.heads.Replace(ctx, child, root, head{height: rootPrio}); err != nil {
				return nil, fmt.Errorf("error replacing head: %s->%s: %w", child, root, err)
			}
		}
	} else {
		// Add new head
		store.logger.Debugf("adding new head %s", root)
		if err := store.heads.Add(ctx, root, head{height: rootPrio}); err != nil {
			return nil, fmt.Errorf("error adding head %s: %w", root, err)
		}
	}

	return nil, nil
}

// RepairDAG is used to walk down the chain until a non-processed node is
// found and at that moment, queues it for processing.
func (store *Datastore) repairDAG(ctx context.Context) error {
	start := time.Now()
	defer func() {
		store.logger.Infof("DAG repair finished. Took %s", time.Since(start).Truncate(time.Second))
	}()

	getter := &crdtNodeGetter{store.dagService}

	heads, _, err := store.heads.List(ctx)
	if err != nil {
		return fmt.Errorf("error listing state: %w", err)
	}

	type nodeHead struct {
		head cid.Cid
		node cid.Cid
	}

	var nodes []nodeHead
	queued := cid.NewSet()
	for _, h := range heads {
		nodes = append(nodes, nodeHead{head: h, node: h})
		queued.Add(h)
	}

	// For logging
	var visitedNodes uint64
	var lastPriority uint64
	var queuedNodes uint64

	exitLogging := make(chan struct{})
	defer close(exitLogging)
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		for {
			select {
			case <-exitLogging:
				ticker.Stop()
				return
			case <-ticker.C:
				store.logger.Infof(
					"DAG repair in progress. Visited nodes: %d. Last priority: %d. Queued nodes: %d",
					atomic.LoadUint64(&visitedNodes),
					atomic.LoadUint64(&lastPriority),
					atomic.LoadUint64(&queuedNodes),
				)
			}
		}
	}()

	for {
		// GetDelta does not seem to respond well to context
		// cancellations (probably this goes down to the Blockstore
		// still working with a cancelled context). So we need to put
		// this here.
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if len(nodes) == 0 {
			break
		}
		nh := nodes[0]
		nodes = nodes[1:]
		cur := nh.node
		head := nh.head

		cctx, cancel := context.WithTimeout(ctx, store.opts.DAGSyncerTimeout)
		n, delta, err := getter.GetDelta(cctx, cur)
		if err != nil {
			cancel()
			return fmt.Errorf("error getting node for reprocessing %s: %w", cur, err)
		}
		cancel()

		isProcessed, err := store.isProcessed(ctx, cur)
		if err != nil {
			return fmt.Errorf("error checking for reprocessed block %s: %w", cur, err)
		}
		if !isProcessed {
			store.logger.Debugf("reprocessing %s / %d", cur, delta.Priority)
			// start syncing from here.
			// do not add children to our queue.
			err = store.handleBranch(ctx, head, cur)
			if err != nil {
				return fmt.Errorf("error reprocessing block %s: %w", cur, err)
			}
		}
		links := n.Links()
		for _, l := range links {
			if queued.Visit(l.Cid) {
				nodes = append(nodes, (nodeHead{head: head, node: l.Cid}))
			}
		}

		atomic.StoreUint64(&queuedNodes, uint64(len(nodes)))
		atomic.AddUint64(&visitedNodes, 1)
		atomic.StoreUint64(&lastPriority, delta.Priority)
	}

	// If we are here we have successfully reprocessed the chain until the
	// bottom.
	store.MarkClean(ctx)
	return nil
}

// Repair triggers a DAG-repair, which tries to re-walk the CRDT-DAG from the
// current state until the roots, processing currently unprocessed branches.
//
// Calling Repair will walk the full DAG even if the dirty bit is unset, but
// will mark the store as clean unpon successful completion.
func (store *Datastore) Repair(ctx context.Context) error {
	return store.repairDAG(ctx)
}

// Get retrieves the object `value` named by `key`.
// Get will return ErrNotFound if the key is not mapped to a value.
func (store *Datastore) Get(ctx context.Context, key ds.Key) (value []byte, err error) {
	return store.set.Element(ctx, key.String())
}

// Has returns whether the `key` is mapped to a `value`.
// In some contexts, it may be much cheaper only to check for existence of
// a value, rather than retrieving the value itself. (e.g. HTTP HEAD).
// The default implementation is found in `GetBackedHas`.
func (store *Datastore) Has(ctx context.Context, key ds.Key) (exists bool, err error) {
	return store.set.InSet(ctx, key.String())
}

// GetSize returns the size of the `value` named by `key`.
// In some contexts, it may be much cheaper to only get the size of the
// value rather than retrieving the value itself.
func (store *Datastore) GetSize(ctx context.Context, key ds.Key) (size int, err error) {
	return ds.GetBackedSize(ctx, store, key)
}

// Query searches the datastore and returns a query result. This function
// may return before the query actually runs. To wait for the query:
//
//	result, _ := ds.Query(q)
//
//	// use the channel interface; result may come in at different times
//	for entry := range result.Next() { ... }
//
//	// or wait for the query to be completely done
//	entries, _ := result.Rest()
//	for entry := range entries { ... }
func (store *Datastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	qr, err := store.set.Elements(ctx, q)
	if err != nil {
		return nil, err
	}
	return query.NaiveQueryApply(q, qr), nil
}

// Put stores the object `value` named by `key`.
func (store *Datastore) Put(ctx context.Context, key ds.Key, value []byte) error {
	delta := store.set.Add(ctx, key.String(), value)
	return store.publish(ctx, delta)
}

// Delete removes the value for given `key`.
func (store *Datastore) Delete(ctx context.Context, key ds.Key) error {
	delta, err := store.set.Rmv(ctx, key.String())
	if err != nil {
		return err
	}

	if len(delta.Tombstones) == 0 {
		return nil
	}
	return store.publish(ctx, delta)
}

// Sync ensures that all the data under the given prefix is flushed to disk in
// the underlying datastore.
func (store *Datastore) Sync(ctx context.Context, prefix ds.Key) error {
	// This is a quick write up of the internals from the time when
	// I was thinking many underlying datastore entries are affected when
	// an add operation happens:
	//
	// When a key is added:
	// - a new delta is made
	// - Delta is marshalled and a DAG-node is created with the bytes,
	//   pointing to previous state. DAG-node is added to DAGService.
	// - Heads are replaced with new CID.
	// - New CID is broadcasted to everyone
	// - The new CID is processed (up until now the delta had not
	//   taken effect). Implementation detail: it is processed before
	//   broadcast actually.
	// - processNode() starts processing that branch from that CID
	// - it calls set.MergeMembers()
	// - that calls putElems() and putTombs()
	// - that may make a batch for all the elems which is later committed
	// - each element has a datastore entry /setNamespace/elemsNamespace/<key>/<block_id>
	// - each tomb has a datastore entry /setNamespace/tombsNamespace/<key>/<block_id>
	// - each value has a datastore entry /setNamespace/keysNamespace/<key>/valueSuffix
	// - each value has an additional priority entry /setNamespace/keysNamespace/<key>/prioritySuffix
	// - the last two are only written if the added entry has more priority than any the existing
	// - For a value to not be lost, those entries should be fully synced.
	// - In order to check if a value is in the set:
	//   - List all elements on /setNamespace/elemsNamespace/<key> (will return several block_ids)
	//   - If we find an element which is not tombstoned, then value is in the set
	// - In order to retrieve an element's value:
	//   - Check that it is in the set
	//   - Read the value entry from the /setNamespace/keysNamespace/<key>/valueSuffix path

	// Be safe and just sync everything in our namespace
	if prefix.String() == "/" {
		return store.store.Sync(ctx, store.namespace)
	}

	// attempt to be intelligent and sync only all state and the
	// set entries related to the given prefix.
	err := store.set.datastoreSync(ctx, prefix)
	err2 := store.store.Sync(ctx, store.heads.namespace)
	return multierr.Combine(err, err2)
}

// Close shuts down the CRDT datastore. It should not be used afterwards.
func (store *Datastore) Close() error {
	store.cancel()
	store.wg.Wait()
	if store.IsDirty(store.ctx) {
		store.logger.Warn("datastore is being closed marked as dirty")
	}
	return nil
}

// Batch implements batching for writes by accumulating
// Put and Delete in the same CRDT-delta and only applying it and
// broadcasting it on Commit().
func (store *Datastore) Batch(ctx context.Context) (ds.Batch, error) {
	return &batch{ctx: ctx, store: store}, nil
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

// returns delta size and error
func (store *Datastore) addToDelta(ctx context.Context, key string, value []byte) (int, error) {
	return store.updateDelta(store.set.Add(ctx, key, value)), nil

}

// returns delta size and error
func (store *Datastore) rmvToDelta(ctx context.Context, key string) (int, error) {
	delta, err := store.set.Rmv(ctx, key)
	if err != nil {
		return 0, err
	}

	return store.updateDeltaWithRemove(key, delta), nil
}

// to satisfy datastore semantics, we need to remove elements from the current
// batch if they were added.
func (store *Datastore) updateDeltaWithRemove(key string, newDelta *pb.Delta) int {
	var size int
	store.curDeltaMux.Lock()
	{
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
		size = proto.Size(store.curDelta)
	}
	store.curDeltaMux.Unlock()
	return size
}

func (store *Datastore) updateDelta(newDelta *pb.Delta) int {
	var size int
	store.curDeltaMux.Lock()
	{
		store.curDelta = deltaMerge(store.curDelta, newDelta)
		size = proto.Size(store.curDelta)
	}
	store.curDeltaMux.Unlock()
	return size
}

func (store *Datastore) publishDelta(ctx context.Context) error {
	store.curDeltaMux.Lock()
	defer store.curDeltaMux.Unlock()
	err := store.publish(ctx, store.curDelta)
	if err != nil {
		return err
	}
	store.curDelta = nil
	return nil
}

func (store *Datastore) putBlock(ctx context.Context, heads []cid.Cid, height uint64, delta *pb.Delta) (ipld.Node, error) {
	if delta != nil {
		delta.Priority = height
	}
	node, err := makeNode(delta, heads)
	if err != nil {
		return nil, fmt.Errorf("error creating new block: %w", err)
	}

	cctx, cancel := context.WithTimeout(ctx, store.opts.DAGSyncerTimeout)
	defer cancel()
	err = store.dagService.Add(cctx, node)
	if err != nil {
		return nil, fmt.Errorf("error writing new block %s: %w", node.Cid(), err)
	}

	return node, nil
}

func (store *Datastore) publish(ctx context.Context, delta *pb.Delta) error {
	// curDelta might be nil if nothing has been added to it
	if delta == nil {
		return nil
	}
	c, err := store.addDAGNode(ctx, delta)
	if err != nil {
		return err
	}
	headsToBroadcast := []cid.Cid{c}
	err = store.state.UpdateHeads(ctx, store.h.ID(), headsToBroadcast, true)
	if err != nil {
		store.logger.Warn("broadcast failed: %v", err)
	}

	return store.broadcast(ctx)
}

func (store *Datastore) addDAGNode(ctx context.Context, delta *pb.Delta) (cid.Cid, error) {
	heads, height, err := store.heads.List(ctx)
	if err != nil {
		return cid.Undef, fmt.Errorf("error listing state: %w", err)
	}
	height = height + 1 // This implies our minimum height is 1

	delta.Priority = height

	nd, err := store.putBlock(ctx, heads, height, delta)
	if err != nil {
		return cid.Undef, err
	}

	// Process new block. This makes that every operation applied
	// to this store take effect (delta is merged) before
	// returning. Since our block references current state, children
	// should be empty
	store.logger.Debugf("processing generated block %s", nd.Cid())
	children, err := store.processNode(ctx, nd.Cid(), &crdtNodeGetter{store.dagService}, height, delta, nd)
	if err != nil {
		store.MarkDirty(ctx) // not sure if this will fix much if this happens.
		return cid.Undef, fmt.Errorf("error processing new block: %w", err)
	}
	if len(children) != 0 {
		store.logger.Warnf("bug: created a block to unknown children")
	}

	return nd.Cid(), nil
}

func (store *Datastore) broadcast(ctx context.Context) error {
	if store.broadcaster == nil { // offline
		return nil
	}

	select {
	case <-ctx.Done():
		store.logger.Debugf("skipping broadcast: %s", ctx.Err())
	default:
	}

	currentState := store.state.GetState()
	store.logger.Debugf("Broadcasting state with %d members", len(currentState.Members))
	for id, member := range currentState.Members {
		headCount := len(member.DagHeads)
		store.logger.Debugf("  Broadcasting member %s: %d heads, bestBefore=%d", id, headCount, member.BestBefore)
	}

	bcastBytes, err := store.encodeBroadcast(ctx, currentState)
	if err != nil {
		return err
	}

	err = store.broadcaster.Broadcast(ctx, bcastBytes)
	if err != nil {
		return fmt.Errorf("error broadcasting state: %w", err)
	}
	return nil
}

type batch struct {
	ctx   context.Context
	store *Datastore
}

func (b *batch) Put(ctx context.Context, key ds.Key, value []byte) error {
	size, err := b.store.addToDelta(ctx, key.String(), value)
	if err != nil {
		return err
	}
	if size > b.store.opts.MaxBatchDeltaSize {
		b.store.logger.Warn("delta size over MaxBatchDeltaSize. Commiting.")
		return b.Commit(ctx)
	}
	return nil
}

func (b *batch) Delete(ctx context.Context, key ds.Key) error {
	size, err := b.store.rmvToDelta(ctx, key.String())
	if err != nil {
		return err
	}
	if size > b.store.opts.MaxBatchDeltaSize {
		b.store.logger.Warn("delta size over MaxBatchDeltaSize. Commiting.")
		return b.Commit(ctx)
	}
	return nil
}

// Commit writes the current delta as a new DAG node and publishes the new
// head. The publish step is skipped if the context is cancelled.
func (b *batch) Commit(ctx context.Context) error {
	return b.store.publishDelta(ctx)
}

type DAGCallback func(from cid.Cid, depth uint64, nd ipld.Node, delta *pb.Delta) error

func (store *Datastore) WalkDAG(ctx context.Context, heads []cid.Cid, callback DAGCallback) error {
	ng := &crdtNodeGetter{NodeGetter: store.dagService}
	set := cid.NewSet()

	for _, h := range heads {
		if err := store.walkDAGRec(ctx, h, 0, ng, set, callback); err != nil {
			return err
		}
	}
	return nil
}

func (store *Datastore) walkDAGRec(ctx context.Context, from cid.Cid, depth uint64, ng *crdtNodeGetter, set *cid.Set, callback DAGCallback) error {
	if !set.Visit(from) {
		return nil
	}

	cctx, cancel := context.WithTimeout(ctx, store.opts.DAGSyncerTimeout)
	defer cancel()
	nd, delta, err := ng.GetDelta(cctx, from)
	if err != nil {
		return err
	}

	if err := callback(from, depth, nd, delta); err != nil {
		return err
	}

	for _, l := range nd.Links() {
		if err := store.walkDAGRec(ctx, l.Cid, depth+1, ng, set, callback); err != nil {
			return err
		}
	}
	return nil
}

func (store *Datastore) PrintDAG(ctx context.Context) error {
	heads, _, err := store.heads.List(ctx)
	if err != nil {
		return err
	}

	return store.WalkDAG(ctx, heads, func(from cid.Cid, depth uint64, nd ipld.Node, delta *pb.Delta) error {
		line := ""
		for i := uint64(0); i < depth; i++ {
			line += " "
		}

		cidStr := from.String()
		cidStr = cidStr[len(cidStr)-4:]
		line += fmt.Sprintf("- %d | %s: ", delta.GetPriority(), cidStr)
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
			cidStr := l.Cid.String()
			cidStr = cidStr[len(cidStr)-4:]
			line += fmt.Sprintf("%s,", cidStr)
		}
		line += "}"
		fmt.Println(line)
		return nil
	})
}

func (store *Datastore) DotDAG(ctx context.Context, w io.Writer) error {
	heads, _, err := store.heads.List(ctx)
	if err != nil {
		return err
	}

	_, _ = fmt.Fprintln(w, "digraph CRDTDAG {")
	_, _ = fmt.Fprintln(w, "subgraph state {")
	for _, h := range heads {
		_, _ = fmt.Fprintln(w, h)
	}
	_, _ = fmt.Fprintln(w, "}")

	err = store.WalkDAG(ctx, heads, func(from cid.Cid, depth uint64, nd ipld.Node, delta *pb.Delta) error {
		cidStr := from.String()
		_, _ = fmt.Fprintf(w, "%s [label=\"%d | %s: +%d -%d\"]\n",
			cidStr, delta.GetPriority(), cidStr[len(cidStr)-4:], len(delta.GetElements()), len(delta.GetTombstones()))
		_, _ = fmt.Fprintf(w, "%s -> {", cidStr)
		for _, l := range nd.Links() {
			_, _ = fmt.Fprintf(w, "%s ", l.Cid)
		}
		_, _ = fmt.Fprintln(w, "}")
		return nil
	})

	_, _ = fmt.Fprintln(w, "}")
	return err
}

// Stats wraps internal information about the datastore.
// Might be expanded in the future.
type Stats struct {
	Heads      []cid.Cid
	MaxHeight  uint64
	QueuedJobs int
	State      *pb.StateBroadcast
}

// InternalStats returns internal datastore information like the current state
// and max height.
func (store *Datastore) InternalStats(ctx context.Context) Stats {
	heads, height, _ := store.heads.List(ctx)

	return Stats{
		State:      store.state.GetState(),
		Heads:      heads,
		MaxHeight:  height,
		QueuedJobs: len(store.jobQueue),
	}
}

// GetState returns the current membership state
func (store *Datastore) GetState(ctx context.Context) *pb.StateBroadcast {
	return store.state.GetState()
}

type cidSafeSet struct {
	set map[cid.Cid]struct{}
	mux sync.RWMutex
}

func newCidSafeSet() *cidSafeSet {
	return &cidSafeSet{
		set: make(map[cid.Cid]struct{}),
	}
}

func (s *cidSafeSet) Visit(c cid.Cid) bool {
	var b bool
	s.mux.Lock()
	{
		if _, ok := s.set[c]; !ok {
			s.set[c] = struct{}{}
			b = true
		}
	}
	s.mux.Unlock()
	return b
}

func (s *cidSafeSet) Remove(c cid.Cid) {
	s.mux.Lock()
	{
		delete(s.set, c)
	}
	s.mux.Unlock()
}

func (s *cidSafeSet) Has(c cid.Cid) (ok bool) {
	s.mux.RLock()
	{
		_, ok = s.set[c]
	}
	s.mux.RUnlock()
	return
}

// DAGNodeInfo holds the additions and tombstones of a DAG node.
type DAGNodeInfo struct {
	Additions  map[string][]byte // key -> value
	Tombstones []string          // keys marked for deletion
}

// ExtractDAGContent walks through the DAG starting from the given heads
// and collects the additions and tombstones for each node.
func (store *Datastore) ExtractDAGContent(ctx context.Context, blockstore blockstore.Blockstore) (map[uint64]DAGNodeInfo, error) {
	heads, _, err := store.heads.List(ctx)
	if err != nil {
		return nil, err
	}

	offlineDAG := dag.NewDAGService(blockservice.New(blockstore, offline.Exchange(blockstore)))
	ng := &crdtNodeGetter{NodeGetter: offlineDAG}

	set := cid.NewSet()

	result := map[uint64]DAGNodeInfo{}

	for _, h := range heads {
		err := store.extractDAGContent(ctx, h, 0, ng, set, result)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (store *Datastore) extractDAGContent(ctx context.Context, from cid.Cid, depth uint64, ng *crdtNodeGetter, set *cid.Set, result map[uint64]DAGNodeInfo) error {
	ok := set.Visit(from)
	if !ok {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, store.opts.DAGSyncerTimeout)
	defer cancel()
	nd, delta, err := ng.GetDelta(ctx, from)
	if err != nil {
		return err
	}
	info := DAGNodeInfo{
		Additions: map[string][]byte{},
	}
	for _, e := range delta.GetElements() {
		info.Additions[e.GetKey()] = e.GetValue()
	}
	for _, e := range delta.GetTombstones() {
		info.Tombstones = append(info.Tombstones, e.GetKey())
	}
	result[delta.GetPriority()] = info

	for _, l := range nd.Links() {
		_ = store.extractDAGContent(ctx, l.Cid, depth+1, ng, set, result)
	}
	return nil
}

func (store *Datastore) compact() {
	if store.opts.CompactInterval == 0 {
		return
	}
	timer := time.NewTimer(0) // fire immediately on start
	for {
		select {
		case <-store.ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
			err := store.triggerCompactionIfNeeded(store.ctx)
			if err != nil {
				store.logger.Errorf("error during compaction: %v", err)
				//todo
			}
		}
		timer.Reset(store.opts.CompactInterval)
	}

}

// triggerCompactionIfNeeded handles snapshot creation logic if the DAG grows too large.
// triggerCompactionIfNeeded checks if compaction is needed and creates a new snapshot lagging behind current heads.
func (store *Datastore) triggerCompactionIfNeeded(ctx context.Context) error {

	store.logger.Debugf("Compaction triggered")
	state := store.state.GetState()
	myParticipant := state.Members[store.h.ID().String()]

	// ADD: Log all member states
	store.logger.Debugf("=== Compaction Analysis ===")
	for memberID, participant := range state.Members {
		headCount := len(participant.DagHeads)
		var headCIDs []string
		for _, head := range participant.DagHeads {
			if c, err := cid.Cast(head.Cid); err == nil {
				headCIDs = append(headCIDs, c.String())
			}
		}
		store.logger.Debugf("Member %s: %d heads %v, snapshot=%v",
			memberID, headCount, headCIDs, participant.Snapshot != nil)
	}

	var lastDagHead cid.Cid
	var snapshotHeight uint64
	if myParticipant != nil && myParticipant.Snapshot != nil {
		lastDagHead = cidFromBytes(myParticipant.Snapshot.DagHead.Cid)
		snapshotHeight = myParticipant.Snapshot.Height
		store.logger.Debugf("My snapshot: height=%d, dagHead=%s", snapshotHeight, lastDagHead.String())
	} else {
		store.logger.Debugf("My snapshot: none")
	}

	commonCID, highestPriority, err := store.getHighestCommonCid(ctx)
	if err != nil {
		return fmt.Errorf("failed to get highest common cid: %w", err)
	}

	if highestPriority == 0 {
		store.logger.Debugf("No common CID found - aborting compaction")
		return nil // Nothing to compact
	}

	store.logger.Debugf("Common analysis: highestPriority=%d, commonCID=%s",
		highestPriority, commonCID.String())

	dagSize := highestPriority - snapshotHeight
	store.logger.Debugf("DAG size check: %d - %d = %d (threshold: %d)",
		highestPriority, snapshotHeight, dagSize, store.opts.CompactDagSize)

	if dagSize < store.opts.CompactDagSize {
		store.logger.Debugf("DAG size %d below threshold %d - no compaction needed",
			dagSize, store.opts.CompactDagSize)
		return nil // DAG still small enough
	}

	store.logger.Debugf("=== PROCEEDING WITH COMPACTION ===")
	// ... rest of function

	store.compactMux.Lock()
	defer store.compactMux.Unlock()

	store.logger.Debugf("Dag Size %d Common SID for compaction %s", dagSize, commonCID.String())

	// Walk back from common head to a stable point lagging behind
	targetCID, h, err := store.walkBackDAG(ctx, commonCID, store.opts.CompactRetainNodes)
	if err != nil {
		return fmt.Errorf("failed to walk back DAG for compaction: %w", err)
	}

	if !targetCID.Defined() {
		return fmt.Errorf("could not determine stable compaction point")
	}

	store.logger.Debugf("Target SID %s, Priority %d", targetCID.String(), h)

	latestSnapshotInfo, err := store.compactAndSnapshot(ctx, targetCID, lastDagHead)
	if err != nil {
		return fmt.Errorf("failed to create snapshot at compaction point: %w", err)
	}

	// Update our snapshot pointer
	if err := store.state.SetSnapshot(ctx, store.h.ID(), latestSnapshotInfo); err != nil {
		return fmt.Errorf("failed to update snapshot after compaction: %w", err)
	}

	return store.broadcast(store.ctx)
}

// getHighestCommonCid gets the highest common cid of all members
func (store *Datastore) getHighestCommonCid(ctx context.Context) (cid.Cid, uint64, error) {
	heads := store.getAllMemberCommonHeads()
	var (
		c cid.Cid
		h uint64
	)

	offlineDAG := dag.NewDAGService(blockservice.New(store.bs, offline.Exchange(store.bs)))
	ng := &crdtNodeGetter{NodeGetter: offlineDAG}
	for _, head := range heads {
		p, err := ng.GetPriority(ctx, head)
		if err != nil {
			return c, h, fmt.Errorf("getting head: %w", err)
		}
		if p > h {
			c = head
			h = p
		}
	}
	return c, h, nil
}

// getAllMemberCommonHeads retrieves the DAG heads from all peers who share our snapshot
func (store *Datastore) getAllMemberCommonHeads() []cid.Cid {
	snapshotHeads := make(map[string]map[cid.Cid]int)
	snapshotMembers := make(map[string]int)

	for _, member := range store.state.GetState().Members {
		var snapshotKey string
		if member.Snapshot != nil {
			snapshotKey = cidFromBytes(member.Snapshot.SnapshotKey.Cid).String()
		} else {
			snapshotKey = "no_snapshot"
		}

		snapshotMembers[snapshotKey]++
		if snapshotHeads[snapshotKey] == nil {
			snapshotHeads[snapshotKey] = make(map[cid.Cid]int)
		}

		for _, c := range member.DagHeads {
			id, err := cid.Cast(c.Cid)
			if err != nil {
				continue
			}
			snapshotHeads[snapshotKey][id]++
		}
	}

	// Choose the most recent snapshotKey with full membership
	var chosenKey string
	var maxMembers int
	for k, count := range snapshotMembers {
		if count > maxMembers {
			maxMembers = count
			chosenKey = k
		}
	}

	var cids []cid.Cid
	for k, v := range snapshotHeads[chosenKey] {
		if v == snapshotMembers[chosenKey] {
			cids = append(cids, k)
		}
	}

	return cids
}

// walkBackDAG traverses the DAG and selects a stable head to compact from thats retainNodes behind the given startCID
func (store *Datastore) walkBackDAG(ctx context.Context, startCID cid.Cid, retainNodes uint64) (cid.Cid, uint64, error) {
	offlineDAG := dag.NewDAGService(blockservice.New(store.bs, offline.Exchange(store.bs)))
	ng := crdtNodeGetter{NodeGetter: offlineDAG}
	visited := cid.NewSet()

	var (
		targetCID cid.Cid
		height    uint64
		steps     uint64
	)

	var walk func(currentCID cid.Cid) error
	walk = func(currentCID cid.Cid) error {
		if steps >= retainNodes {
			targetCID = currentCID
			_, delta, err := ng.GetDelta(ctx, currentCID)
			if err != nil {
				return fmt.Errorf("getting delta: %w", err)
			}
			height = delta.GetPriority()
			return nil
		}
		if !visited.Visit(currentCID) {
			return nil
		}

		node, _, err := ng.GetDelta(ctx, currentCID)
		if err != nil {
			return err
		}

		links := node.Links()
		if len(links) == 1 {
			// Linear path, continue walking
			steps++
			return walk(links[0].Cid)
		} else if len(links) > 1 {
			// Select the most stable branch (e.g., by priority)
			stableChild, err := store.selectStableChild(ctx, links)
			if err != nil {
				return err
			}
			steps++
			return walk(stableChild)
		}

		return nil
	}

	if err := walk(startCID); err != nil {
		return cid.Undef, 0, err
	}
	return targetCID, height, nil
}

// selectStableChild selects the most stable child node from multiple links
func (store *Datastore) selectStableChild(ctx context.Context, links []*ipld.Link) (cid.Cid, error) {
	offlineDAG := dag.NewDAGService(blockservice.New(store.bs, offline.Exchange(store.bs)))
	ng := crdtNodeGetter{NodeGetter: offlineDAG}

	var (
		bestCID         cid.Cid
		highestPriority uint64
	)

	for _, link := range links {
		_, delta, err := ng.GetDelta(ctx, link.Cid)
		if err != nil {
			continue // Skip if priority can't be determined
		}

		priority := delta.GetPriority()
		if priority > highestPriority {
			bestCID = link.Cid
			highestPriority = priority
		}
	}

	if bestCID.Defined() {
		return bestCID, nil
	}
	return cid.Undef, fmt.Errorf("no stable child found among branches")
}

// UpdateMeta returns the current membership state
func (store *Datastore) UpdateMeta(ctx context.Context, meta map[string]string) error {
	return store.state.SetMeta(ctx, store.h.ID(), meta)
}

// Minimal job persistence functions
func (store *Datastore) jobKey(rootCID cid.Cid) ds.Key {
	return store.namespace.ChildString(jobsNs).ChildString(rootCID.String())
}

func (store *Datastore) jobChildKey(rootCID, childCID cid.Cid) ds.Key {
	return store.namespace.ChildString(jobsNs).ChildString(rootCID.String()).ChildString(childCID.String())
}

func (store *Datastore) createJob(ctx context.Context, rootCID cid.Cid) error {
	timestamp := make([]byte, 8)
	binary.BigEndian.PutUint64(timestamp, uint64(time.Now().Unix()))
	return store.store.Put(ctx, store.jobKey(rootCID), timestamp)
}

func (store *Datastore) jobExists(ctx context.Context, rootCID cid.Cid) (bool, error) {
	return store.store.Has(ctx, store.jobKey(rootCID))
}

func (store *Datastore) setChildStatus(ctx context.Context, rootCID, childCID cid.Cid, status byte) error {
	return store.store.Put(ctx, store.jobChildKey(rootCID, childCID), []byte{status})
}

func (store *Datastore) getChildStatus(ctx context.Context, rootCID, childCID cid.Cid) (byte, error) {
	data, err := store.store.Get(ctx, store.jobChildKey(rootCID, childCID))
	if err != nil {
		if errors.Is(err, ds.ErrNotFound) {
			return 255, nil // Unknown
		}
		return 255, err
	}
	if len(data) == 0 {
		return 255, nil
	}
	return data[0], nil
}

func (store *Datastore) setBlockState(ctx context.Context, c cid.Cid, state ProcessedState) error {
	return store.store.Put(ctx, store.processedBlockKey(c), []byte{byte(state)})
}

func (store *Datastore) getBlockState(ctx context.Context, c cid.Cid) (ProcessedState, error) {
	data, err := store.store.Get(ctx, store.processedBlockKey(c))
	if err != nil {
		if errors.Is(err, ds.ErrNotFound) {
			return ProcessedState(255), nil
		}
		return ProcessedState(255), err
	}
	if len(data) == 0 {
		return ProcessedState(255), nil
	}
	return ProcessedState(data[0]), nil
}

func (store *Datastore) restorePersistedJobs(ctx context.Context) error {
	// First cleanup old jobs
	if err := store.cleanupOldJobs(ctx); err != nil {
		store.logger.Errorf("error during job cleanup: %s", err)
		// Continue anyway
	}

	// Then get remaining jobs to resume
	jobRoots, err := store.listActiveJobs(ctx)
	if err != nil {
		return fmt.Errorf("error listing active jobs: %w", err)
	}

	if len(jobRoots) == 0 {
		store.logger.Info("no persisted jobs found")
		return nil
	}

	store.logger.Infof("restoring %d persisted jobs", len(jobRoots))

	// Resume each job
	for _, rootCID := range jobRoots {
		go func(root cid.Cid) {
			if err := store.resumePersistedJob(ctx, root); err != nil {
				store.logger.Errorf("error resuming job %s: %s", root, err)
			}
		}(rootCID)
	}

	return nil
}

func (store *Datastore) listActiveJobs(ctx context.Context) ([]cid.Cid, error) {
	prefix := store.namespace.ChildString(jobsNs).String()
	q := query.Query{Prefix: prefix}

	results, err := store.store.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("error querying jobs: %w", err)
	}
	defer func() {
		_ = results.Close()
	}()

	var jobRoots []cid.Cid
	for result := range results.Next() {
		if result.Error != nil {
			continue
		}

		key := ds.NewKey(result.Key)
		namespaces := key.Namespaces()
		if len(namespaces) > 2 { // Skip child entries
			continue
		}

		rootCIDStr := namespaces[1]
		rootCID, err := cid.Decode(rootCIDStr)
		if err != nil {
			store.logger.Errorf("invalid root CID in job: %s", err)
			continue
		}

		jobRoots = append(jobRoots, rootCID)
	}

	return jobRoots, nil
}

func (store *Datastore) resumePersistedJob(ctx context.Context, rootCID cid.Cid) error {
	// Find pending children to resume fetching
	pendingChildren, err := store.getPendingChildren(ctx, rootCID)
	if err != nil {
		return fmt.Errorf("error getting pending children: %w", err)
	}

	if len(pendingChildren) == 0 {
		// No pending work, job might be complete already
		store.logger.Debugf("job %s has no pending work, may already be complete", rootCID)
		return nil
	}

	store.logger.Debugf("resuming job %s with %d pending children", rootCID, len(pendingChildren))

	// Just pass off to existing machinery
	dg := &crdtNodeGetter{NodeGetter: store.dagService}
	if sessionMaker, ok := store.dagService.(SessionDAGService); ok {
		dg = &crdtNodeGetter{NodeGetter: sessionMaker.Session(ctx)}
	}

	var session sync.WaitGroup
	err = store.sendNewJobs(ctx, &session, dg, rootCID, 0, pendingChildren)
	session.Wait()

	return err
}

func (store *Datastore) getPendingChildren(ctx context.Context, rootCID cid.Cid) ([]cid.Cid, error) {
	prefix := store.jobKey(rootCID).String() + "/"
	q := query.Query{Prefix: prefix}

	results, err := store.store.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("error querying job children: %w", err)
	}
	defer func() {
		_ = results.Close()
	}()

	var pendingChildren []cid.Cid
	for result := range results.Next() {
		if result.Error != nil || len(result.Value) == 0 {
			continue
		}

		status := result.Value[0]
		if status == ChildPending || status == ChildRequested {
			key := ds.NewKey(result.Key)
			namespaces := key.Namespaces()
			if len(namespaces) >= 3 {
				childCIDStr := namespaces[2]
				if childCID, err := cid.Decode(childCIDStr); err == nil {
					pendingChildren = append(pendingChildren, childCID)
				}
			}
		}
	}

	return pendingChildren, nil
}

func (store *Datastore) cleanupOldJobs(ctx context.Context) error {
	prefix := store.namespace.ChildString(jobsNs).String()
	q := query.Query{Prefix: prefix}

	results, err := store.store.Query(ctx, q)
	if err != nil {
		return err
	}
	defer func() {
		_ = results.Close()
	}()

	for result := range results.Next() {
		if result.Error != nil {
			continue
		}

		key := ds.NewKey(result.Key)
		namespaces := key.Namespaces()
		if len(namespaces) > 2 { // Skip child entries
			continue
		}

		if len(result.Value) == 8 {
			timestamp := int64(binary.BigEndian.Uint64(result.Value))
			created := time.Unix(timestamp, 0)
			if time.Since(created) > jobTimeout {
				rootCIDStr := namespaces[1]
				if rootCID, err := cid.Decode(rootCIDStr); err == nil {
					store.logger.Debugf("cleaning up old job %s", rootCID)
					if err := store.completeJob(ctx, rootCID); err != nil {
						store.logger.Errorf("error deleting old job %s: %s", rootCID, err)
					}
				}
			}
		}
	}

	return nil
}

// Simplified job cleanup that just calls cleanupOldJobs
func (store *Datastore) jobCleanup(ctx context.Context) {
	ticker := time.NewTicker(6 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := store.cleanupOldJobs(ctx); err != nil {
				store.logger.Errorf("error during job cleanup: %s", err)
			}
		}
	}
}

func (store *Datastore) isJobReady(ctx context.Context, root cid.Cid) ([]cid.Cid, bool, error) {
	prefix := store.jobKey(root).String() + "/"
	q := query.Query{Prefix: prefix}

	results, err := store.store.Query(ctx, q)
	if err != nil {
		return nil, false, fmt.Errorf("error querying job children: %w", err)
	}
	defer func() {
		_ = results.Close()
	}()

	var allNodes []cid.Cid
	hasChildren := false

	for result := range results.Next() {
		if result.Error != nil || len(result.Value) == 0 {
			continue
		}
		hasChildren = true
		status := result.Value[0]

		// Extract child CID from key
		key := ds.NewKey(strings.TrimPrefix(result.Key, prefix))
		namespaces := key.Namespaces()
		if len(namespaces) < 1 {
			continue
		}

		childCIDStr := namespaces[0]
		childCID, err := cid.Decode(childCIDStr)
		if err != nil {
			continue
		}

		// Only consider fetched nodes for processing
		if status == ChildFetched {
			allNodes = append(allNodes, childCID)
		} else if status != ChildProcessed {
			// If any node is not fetched (and not already processed), job not ready
			return nil, false, nil
		}
	}

	if !hasChildren {
		return []cid.Cid{root}, true, nil
	}

	// Additional check: ensure we have complete branches
	// For each fetched node, verify all its dependencies are either:
	// 1. Already processed globally, OR
	// 2. Part of this job and fetched
	if len(allNodes) > 0 {
		complete, err := store.verifyBranchCompleteness(ctx, allNodes)
		if err != nil {
			return nil, false, err
		}
		if !complete {
			store.logger.Debugf("job %s not ready - incomplete branches", root)
			return nil, false, nil
		}
	}

	return allNodes, len(allNodes) > 0, nil
}

// verifyBranchCompleteness ensures all dependencies are available before processing
func (store *Datastore) verifyBranchCompleteness(ctx context.Context, nodes []cid.Cid) (bool, error) {
	dg := &crdtNodeGetter{NodeGetter: store.dagService}

	nodeSet := make(map[cid.Cid]bool)
	for _, node := range nodes {
		nodeSet[node] = true
	}

	for _, nodeCID := range nodes {
		node, _, err := dg.GetDelta(ctx, nodeCID)
		if err != nil {
			return false, fmt.Errorf("error getting node %s: %w", nodeCID, err)
		}

		// Check each dependency
		for _, link := range node.Links() {
			depCID := link.Cid

			// Is this dependency already processed globally?
			isProcessed, err := store.isProcessed(ctx, depCID)
			if err != nil {
				return false, fmt.Errorf("error checking if %s is processed: %w", depCID, err)
			}

			if isProcessed {
				continue // This dependency is satisfied
			}

			// Is this dependency part of our current job?
			if nodeSet[depCID] {
				continue // This dependency will be processed with us
			}

			// Dependency is missing - branch is incomplete
			store.logger.Debugf("incomplete branch: node %s depends on %s which is not processed or in current job",
				nodeCID, depCID)
			return false, nil
		}
	}

	return true, nil
}

func (store *Datastore) completeJob(ctx context.Context, root cid.Cid) error {
	// Delete child keys first
	prefix := store.jobKey(root).String() + "/"
	q := query.Query{Prefix: prefix}

	results, err := store.store.Query(ctx, q)
	if err != nil {
		return fmt.Errorf("error querying job children for deletion: %w", err)
	}
	defer func() {
		_ = results.Close()
	}()

	// Delete all child keys
	for result := range results.Next() {
		if result.Error != nil {
			continue
		}
		if err := store.store.Delete(ctx, ds.NewKey(result.Key)); err != nil {
			store.logger.Errorf("error deleting child key %s: %s", result.Key, err)
		}
	}

	// Then delete the job metadata
	if err := store.store.Delete(ctx, store.jobKey(root)); err != nil && !errors.Is(err, ds.ErrNotFound) {
		return fmt.Errorf("error deleting job metadata: %w", err)
	}

	return nil
}
