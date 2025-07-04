package crdt

import (
	"context"
	"crypto/ed25519"
	"crypto/sha512"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger"
	blockstore "github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/ipld/merkledag"
	mdutils "github.com/ipfs/boxo/ipld/merkledag/test"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	dssync "github.com/ipfs/go-datastore/sync"
	dstest "github.com/ipfs/go-datastore/test"
	badgerds "github.com/ipfs/go-ds-badger"
	"github.com/ipfs/go-ds-crdt/pb"
	ipld "github.com/ipfs/go-ipld-format"
	log "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

var numReplicas = 15
var debug = false

const (
	mapStore = iota
	badgerStore
)

var store int = mapStore

func init() {
	dstest.ElemCount = 10
}

type testLogger struct {
	name string
	l    log.StandardLogger
}

func (tl *testLogger) Debug(args ...interface{}) {
	args = append([]interface{}{tl.name}, args...)
	tl.l.Debug(args...)
}
func (tl *testLogger) Debugf(format string, args ...interface{}) {
	args = append([]interface{}{tl.name}, args...)
	tl.l.Debugf("%s "+format, args...)
}
func (tl *testLogger) Error(args ...interface{}) {
	args = append([]interface{}{tl.name}, args...)
	tl.l.Error(args...)
}
func (tl *testLogger) Errorf(format string, args ...interface{}) {
	args = append([]interface{}{tl.name}, args...)
	tl.l.Errorf("%s "+format, args...)
}
func (tl *testLogger) Fatal(args ...interface{}) {
	args = append([]interface{}{tl.name}, args...)
	tl.l.Fatal(args...)
}
func (tl *testLogger) Fatalf(format string, args ...interface{}) {
	args = append([]interface{}{tl.name}, args...)
	tl.l.Fatalf("%s "+format, args...)
}
func (tl *testLogger) Info(args ...interface{}) {
	args = append([]interface{}{tl.name}, args...)
	tl.l.Info(args...)
}
func (tl *testLogger) Infof(format string, args ...interface{}) {
	args = append([]interface{}{tl.name}, args...)
	tl.l.Infof("%s "+format, args...)
}
func (tl *testLogger) Panic(args ...interface{}) {
	args = append([]interface{}{tl.name}, args...)
	tl.l.Panic(args...)
}
func (tl *testLogger) Panicf(format string, args ...interface{}) {
	args = append([]interface{}{tl.name}, args...)
	tl.l.Panicf("%s "+format, args...)
}
func (tl *testLogger) Warn(args ...interface{}) {
	args = append([]interface{}{tl.name}, args...)
	tl.l.Warn(args...)
}
func (tl *testLogger) Warnf(format string, args ...interface{}) {
	args = append([]interface{}{tl.name}, args...)
	tl.l.Warnf("%s "+format, args...)
}

type mockBroadcaster struct {
	ctx      context.Context
	chans    []chan []byte
	myChan   chan []byte
	dropProb *atomic.Int64 // probability of dropping a message instead of receiving it
	t        testing.TB
	mu       sync.RWMutex
}

func newBroadcasters(t testing.TB, n int) ([]*mockBroadcaster, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	broadcasters := make([]*mockBroadcaster, n)
	chans := make([]chan []byte, n)
	for i := range chans {
		dropP := &atomic.Int64{}
		chans[i] = make(chan []byte, 300)
		broadcasters[i] = &mockBroadcaster{
			ctx:      ctx,
			chans:    chans,
			myChan:   chans[i],
			dropProb: dropP,
			t:        t,
		}
	}
	return broadcasters, cancel
}

func (mb *mockBroadcaster) Broadcast(ctx context.Context, data []byte) error {
	mb.mu.RLock()
	defer mb.mu.RUnlock()
	var wg sync.WaitGroup

	randg := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i, ch := range mb.chans {
		n := randg.Int63n(100)
		if n < mb.dropProb.Load() {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			randg := rand.New(rand.NewSource(int64(i)))
			// randomize when we send a little bit
			if randg.Intn(100) < 30 {
				// Sleep for a very small time that will
				// effectively be pretty random
				time.Sleep(time.Nanosecond)

			}
			timer := time.NewTimer(5 * time.Second)
			defer timer.Stop()

			select {
			case ch <- data:
			case <-timer.C:
				mb.t.Errorf("broadcasting to %d timed out", i)
			}
		}(i)
		wg.Wait()
	}
	return nil
}

func (mb *mockBroadcaster) Next(ctx context.Context) ([]byte, error) {
	select {
	case data := <-mb.myChan:
		return data, nil
	case <-ctx.Done():
		return nil, ErrNoMoreBroadcast
	case <-mb.ctx.Done():
		return nil, ErrNoMoreBroadcast
	}
}

type mockDAGSvc struct {
	ipld.DAGService
	bs blockstore.Blockstore
}

func (mds *mockDAGSvc) Add(ctx context.Context, n ipld.Node) error {
	return mds.DAGService.Add(ctx, n)
}

func (mds *mockDAGSvc) Get(ctx context.Context, c cid.Cid) (ipld.Node, error) {
	nd, err := mds.DAGService.Get(ctx, c)
	if err != nil {
		return nd, err
	}
	return nd, nil
}

func (mds *mockDAGSvc) GetMany(ctx context.Context, cids []cid.Cid) <-chan *ipld.NodeOption {
	return mds.DAGService.GetMany(ctx, cids)
}

type mockPeer struct {
	id peer.ID
}

func newMockPeer(id string) Peer {
	pk := generateEd25519Key(id)
	pid, err := peer.IDFromPublicKey(pk.GetPublic())
	if err != nil {
		panic(err)
	}
	return &mockPeer{pid}
}

func (m *mockPeer) ID() peer.ID {
	return m.id
}

func generateEd25519Key(seed string) crypto.PrivKey {
	// Hash the seed string to generate a 32-byte seed
	hash := sha512.Sum512([]byte(seed))
	seedBytes := hash[:32] // Take the first 32 bytes

	// Generate an Ed25519 private key from the seed
	pk, err := crypto.UnmarshalEd25519PrivateKey(ed25519.NewKeyFromSeed(seedBytes))
	if err != nil {
		panic(err)
	}

	return pk
}

func storeFolder(i int) string {
	return fmt.Sprintf("test-badger-%d", i)
}

func makeStore(t testing.TB, i int) ds.Datastore {
	t.Helper()

	switch store {
	case mapStore:
		return dssync.MutexWrap(ds.NewMapDatastore())
	case badgerStore:
		folder := storeFolder(i)
		err := os.MkdirAll(folder, 0700)
		if err != nil {
			t.Fatal(err)
		}

		badgerOpts := badger.DefaultOptions("")
		badgerOpts.SyncWrites = false
		badgerOpts.MaxTableSize = 1048576

		opts := badgerds.Options{Options: badgerOpts}
		dstore, err := badgerds.NewDatastore(folder, &opts)
		if err != nil {
			t.Fatal(err)
		}
		return dstore
	default:
		t.Fatal("bad store type selected for tests")
		return nil

	}
}

func makeNReplicas(t testing.TB, n int, opts *Options) ([]*Datastore, func()) {
	bcasts, bcastCancel := newBroadcasters(t, n)

	bs := mdutils.Bserv()
	dagserv := merkledag.NewDAGService(bs)

	replicaOpts := make([]*Options, n)
	for i := range replicaOpts {
		if opts == nil {
			replicaOpts[i] = DefaultOptions()
		} else {
			copy := *opts
			replicaOpts[i] = &copy
		}

		replicaOpts[i].Logger = &testLogger{
			name: fmt.Sprintf("r#%d: ", i),
			l:    DefaultOptions().Logger,
		}
		replicaOpts[i].RebroadcastInterval = time.Second * 5
		replicaOpts[i].NumWorkers = 5
		replicaOpts[i].DAGSyncerTimeout = time.Second
	}

	replicas := make([]*Datastore, n)
	for i := range replicas {
		dagsync := &mockDAGSvc{
			DAGService: dagserv,
			bs:         bs.Blockstore(),
		}

		h := newMockPeer(fmt.Sprintf("peer-%d", i))
		var err error
		replicas[i], err = New(h, makeStore(t, i), bs.Blockstore(), ds.NewKey("crdttest"), dagsync, bcasts[i], replicaOpts[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	if debug {
		log.SetLogLevel("crdt", "debug")
	}

	closeReplicas := func() {
		bcastCancel()
		for i, r := range replicas {
			err := r.Close()
			if err != nil {
				t.Error(err)
			}
			os.RemoveAll(storeFolder(i))
		}
	}

	return replicas, closeReplicas
}

func makeReplicas(t testing.TB, opts *Options) ([]*Datastore, func()) {
	return makeNReplicas(t, numReplicas, opts)
}

func TestCRDT(t *testing.T) {
	ctx := context.Background()
	replicas, closeReplicas := makeReplicas(t, nil)
	defer closeReplicas()
	k := ds.NewKey("hi")
	err := replicas[0].Put(ctx, k, []byte("hola"))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)

	for _, r := range replicas {
		v, err := r.Get(ctx, k)
		if err != nil {
			t.Error(err)
		}
		if string(v) != "hola" {
			t.Error("bad content: ", string(v))
		}
	}
}

func TestCRDTReplication(t *testing.T) {
	ctx := context.Background()
	nItems := 50
	randGen := rand.New(rand.NewSource(time.Now().UnixNano()))

	replicas, closeReplicas := makeReplicas(t, nil)
	defer closeReplicas()

	// Add nItems choosing the replica randomly
	for i := 0; i < nItems; i++ {
		k := ds.RandomKey()
		v := []byte(fmt.Sprintf("%d", i))
		n := randGen.Intn(len(replicas))
		err := replicas[n].Put(ctx, k, v)
		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(500 * time.Millisecond)

	// Query all items
	q := query.Query{
		KeysOnly: true,
	}
	results, err := replicas[0].Query(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	defer results.Close()
	rest, err := results.Rest()
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != nItems {
		t.Fatalf("expected %d elements", nItems)
	}

	// make sure each item has arrived to every replica
	for _, res := range rest {
		for _, r := range replicas {
			ok, err := r.Has(ctx, ds.NewKey(res.Key))
			if err != nil {
				t.Error(err)
			}
			if !ok {
				t.Error("replica should have key")
			}
		}
	}

	// give a new value for each item
	for _, r := range rest {
		n := randGen.Intn(len(replicas))
		err := replicas[n].Put(ctx, ds.NewKey(r.Key), []byte("hola"))
		if err != nil {
			t.Error(err)
		}
	}

	time.Sleep(200 * time.Millisecond)

	// query everything again
	results, err = replicas[0].Query(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	defer results.Close()

	total := 0
	for r := range results.Next() {
		total++
		if r.Error != nil {
			t.Error(err)
		}
		k := ds.NewKey(r.Key)
		for i, r := range replicas {
			v, err := r.Get(ctx, k)
			if err != nil {
				t.Error(err)
			}
			if string(v) != "hola" {
				t.Errorf("value should be hola for %s in replica %d", k, i)
			}
		}
	}
	if total != nItems {
		t.Fatalf("expected %d elements again", nItems)
	}

	for _, r := range replicas {
		list, _, err := r.heads.List(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Log(list)
	}
	//replicas[0].PrintDAG()
	//fmt.Println("==========================================================")
	//replicas[1].PrintDAG()
}

// TestCRDTPriority tests that given multiple concurrent updates from several
// replicas on the same key, the resulting values converge to the same key.
//
// It does this by launching one go routine for every replica, where it replica
// writes the value #replica-number repeteadly (nItems-times).
//
// Finally, it puts a final value for a single key in the first replica and
// checks that all replicas got it.
//
// If key priority rules are respected, the "last" update emitted for the key
// K (which could have come from any replica) should take place everywhere.
func TestCRDTPriority(t *testing.T) {
	ctx := context.Background()
	nItems := 50

	replicas, closeReplicas := makeReplicas(t, nil)
	defer closeReplicas()

	k := ds.NewKey("k")

	var wg sync.WaitGroup
	wg.Add(len(replicas))
	for i, r := range replicas {
		go func(r *Datastore, i int) {
			defer wg.Done()
			for j := 0; j < nItems; j++ {
				err := r.Put(ctx, k, []byte(fmt.Sprintf("r#%d", i)))
				if err != nil {
					t.Error(err)
				}
			}
		}(r, i)
	}
	wg.Wait()

	// Wait for convergence
	RequireConvergence(t, replicas, ctx)

	var v, lastv []byte
	var err error
	for i, r := range replicas {
		v, err = r.Get(ctx, k)
		if err != nil {
			t.Error(err)
		}
		t.Logf("Replica %d got value %s", i, string(v))
		if lastv != nil && string(v) != string(lastv) {
			t.Error("value was different between replicas, but should be the same")
		}
		lastv = v
	}

	err = replicas[0].Put(ctx, k, []byte("final value"))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1000 * time.Millisecond)

	for i, r := range replicas {
		v, err := r.Get(ctx, k)
		if err != nil {
			t.Error(err)
		}
		if string(v) != "final value" {
			t.Errorf("replica %d has wrong final value: %s", i, string(v))
		}
	}

	//replicas[14].PrintDAG()
	//fmt.Println("=======================================================")
	//replicas[1].PrintDAG()
}

func RequireConvergence(t *testing.T, replicas []*Datastore, ctx context.Context) {
	require.Eventually(t, func() bool {
		// Check all adjacent replicas have the same state
		for i := 0; i < len(replicas)-1; i++ {
			j := i + 1
			stateI := replicas[i].GetState(ctx)
			stateJ := replicas[j].GetState(ctx)

			// Check same number of members
			if len(stateI.Members) != len(stateJ.Members) {
				if debug {
					t.Logf("Replica %d has %d members, replica %d has %d members",
						i, len(stateI.Members), j, len(stateJ.Members))
				}
				return false
			}

			// Check all members from stateI exist in stateJ with same heads
			for memberID, memberI := range stateI.Members {
				memberJ, exists := stateJ.Members[memberID]
				if !exists {
					if debug {
						t.Logf("Replica %d has member %s, replica %d does not", i, memberID, j)
					}
					return false
				}

				// Check same number of heads
				if len(memberI.DagHeads) != len(memberJ.DagHeads) {
					if debug {
						t.Logf("Replica %d member %s has %d heads, replica %d has %d heads",
							i, memberID, len(memberI.DagHeads), j, len(memberJ.DagHeads))
					}
					return false
				}

				// Convert heads to sets for comparison (order-independent)
				headsI := make(map[string]bool)
				for _, head := range memberI.DagHeads {
					headsI[head.String()] = true
				}

				headsJ := make(map[string]bool)
				for _, head := range memberJ.DagHeads {
					headsJ[head.String()] = true
				}

				// Check all heads match
				for cidI := range headsI {
					if !headsJ[cidI] {
						if debug {
							t.Logf("Replica %d member %s has head %s, replica %d does not",
								i, memberID, cidI, j)
						}
						return false
					}
				}
			}
		}
		return true
	}, 10*time.Second, 100*time.Millisecond)
}

func TestCRDTCatchUp(t *testing.T) {
	ctx := context.Background()
	nItems := 50
	replicas, closeReplicas := makeReplicas(t, nil)
	defer closeReplicas()

	r := replicas[len(replicas)-1]
	br := r.broadcaster.(*mockBroadcaster)
	br.dropProb.Store(101)

	// this items will not get to anyone
	for i := 0; i < nItems; i++ {
		k := ds.RandomKey()
		err := r.Put(ctx, k, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(100 * time.Millisecond)
	br.dropProb.Store(0)

	// this message will get to everyone
	err := r.Put(ctx, ds.RandomKey(), nil)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)
	q := query.Query{KeysOnly: true}
	results, err := replicas[0].Query(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	defer results.Close()
	rest, err := results.Rest()
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != nItems+1 {
		t.Fatal("replica 0 did not get all the things")
	}
}

func TestCRDTPrintDAG(t *testing.T) {
	ctx := context.Background()

	nItems := 5
	replicas, closeReplicas := makeReplicas(t, nil)
	defer closeReplicas()

	// this items will not get to anyone
	for i := 0; i < nItems; i++ {
		k := ds.RandomKey()
		err := replicas[0].Put(ctx, k, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	err := replicas[0].PrintDAG(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCRDTHooks(t *testing.T) {
	ctx := context.Background()

	var put int64
	var deleted int64

	opts := DefaultOptions()
	opts.PutHook = func(k ds.Key, v []byte) {
		count := atomic.AddInt64(&put, 1)
		t.Logf("PutHook called for key %s, count now: %d", k, count)
	}
	opts.DeleteHook = func(k ds.Key) {
		count := atomic.AddInt64(&deleted, 1)
		t.Logf("DeleteHook called for key %s, count now: %d", k, count)
	}

	replicas, closeReplicas := makeReplicas(t, opts)
	defer closeReplicas()

	t.Logf("Created %d replicas", len(replicas))

	k := ds.RandomKey()
	t.Logf("Using key: %s", k)

	err := replicas[0].Put(ctx, k, []byte("test-value"))
	if err != nil {
		t.Fatal(err)
	}

	require.Eventually(t, func() bool {
		putCount := atomic.LoadInt64(&put)
		t.Logf("Current put count: %d, expected: %d", putCount, len(replicas))
		return putCount == int64(len(replicas))
	}, 10*time.Second, 500*time.Millisecond, "all replicas should have notified Put")

	// Verify the key exists on all replicas before deleting
	for i, replica := range replicas {
		has, err := replica.Has(ctx, k)
		require.NoError(t, err)
		t.Logf("Replica %d has key %s: %t", i, k, has)
		require.True(t, has, "Key should exist on replica %d before delete", i)
	}

	err = replicas[0].Delete(ctx, k)
	if err != nil {
		t.Fatal(err)
	}

	require.Eventually(t, func() bool {
		deleteCount := atomic.LoadInt64(&deleted)
		t.Logf("Current delete count: %d, expected: %d", deleteCount, len(replicas))
		return deleteCount == int64(len(replicas))
	}, 10*time.Second, 500*time.Millisecond, "all replicas should have notified Delete")
}

func TestCRDTBatch(t *testing.T) {
	ctx := context.Background()

	opts := DefaultOptions()
	opts.MaxBatchDeltaSize = 500 // bytes

	replicas, closeReplicas := makeReplicas(t, opts)
	defer closeReplicas()

	btch, err := replicas[0].Batch(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// This should be batched
	k := ds.RandomKey()
	err = btch.Put(ctx, k, make([]byte, 200))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := replicas[0].Get(ctx, k); err != ds.ErrNotFound {
		t.Fatal("should not have commited the batch")
	}

	k2 := ds.RandomKey()
	err = btch.Put(ctx, k2, make([]byte, 400))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := replicas[0].Get(ctx, k2); err != nil {
		t.Fatal("should have commited the batch: delta size was over threshold")
	}

	err = btch.Delete(ctx, k)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := replicas[0].Get(ctx, k); err != nil {
		t.Fatal("should not have committed the batch")
	}

	err = btch.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	for _, r := range replicas {
		if _, err := r.Get(ctx, k); err != ds.ErrNotFound {
			t.Error("k should have been deleted everywhere")
		}
		if _, err := r.Get(ctx, k2); err != nil {
			t.Error("k2 should be everywhere")
		}
	}
}

func TestCRDTNamespaceClash(t *testing.T) {
	ctx := context.Background()

	opts := DefaultOptions()
	replicas, closeReplicas := makeReplicas(t, opts)
	defer closeReplicas()

	k := ds.NewKey("path/to/something")
	err := replicas[0].Put(ctx, k, nil)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	k = ds.NewKey("path")
	ok, _ := replicas[0].Has(ctx, k)
	if ok {
		t.Error("it should not have the key")
	}

	_, err = replicas[0].Get(ctx, k)
	if err != ds.ErrNotFound {
		t.Error("should return err not found")
	}

	err = replicas[0].Put(ctx, k, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}

	v, err := replicas[0].Get(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != "hello" {
		t.Error("wrong value read from database")
	}

	err = replicas[0].Delete(ctx, ds.NewKey("path/to/something"))
	if err != nil {
		t.Fatal(err)
	}

	v, err = replicas[0].Get(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != "hello" {
		t.Error("wrong value read from database")
	}
}

var _ ds.Datastore = (*syncedTrackDs)(nil)

type syncedTrackDs struct {
	ds.Datastore
	syncs map[ds.Key]struct{}
	set   *set
}

func (st *syncedTrackDs) Sync(ctx context.Context, k ds.Key) error {
	st.syncs[k] = struct{}{}
	return st.Datastore.Sync(ctx, k)
}

func (st *syncedTrackDs) isSynced(k ds.Key) bool {
	prefixStr := k.String()
	mustBeSynced := []ds.Key{
		st.set.elemsPrefix(prefixStr),
		st.set.tombsPrefix(prefixStr),
		st.set.keyPrefix(keysNs).Child(k),
	}

	for k := range st.syncs {
		synced := false
		for _, t := range mustBeSynced {
			if k == t || k.IsAncestorOf(t) {
				synced = true
				break
			}
		}
		if !synced {
			return false
		}
	}
	return true
}

func TestCRDTSync(t *testing.T) {
	ctx := context.Background()

	opts := DefaultOptions()
	replicas, closeReplicas := makeReplicas(t, opts)
	defer closeReplicas()

	syncedDs := &syncedTrackDs{
		Datastore: replicas[0].set.store,
		syncs:     make(map[ds.Key]struct{}),
		set:       replicas[0].set,
	}

	replicas[0].set.store = syncedDs
	k1 := ds.NewKey("/hello/bye")
	k2 := ds.NewKey("/hello")
	k3 := ds.NewKey("/hell")

	err := replicas[0].Put(ctx, k1, []byte("value1"))
	if err != nil {
		t.Fatal(err)
	}

	err = replicas[0].Put(ctx, k2, []byte("value2"))
	if err != nil {
		t.Fatal(err)
	}

	err = replicas[0].Put(ctx, k3, []byte("value3"))
	if err != nil {
		t.Fatal(err)
	}

	err = replicas[0].Sync(ctx, ds.NewKey("/hello"))
	if err != nil {
		t.Fatal(err)
	}

	if !syncedDs.isSynced(k1) {
		t.Error("k1 should have been synced")
	}

	if !syncedDs.isSynced(k2) {
		t.Error("k2 should have been synced")
	}

	if syncedDs.isSynced(k3) {
		t.Error("k3 should have not been synced")
	}
}

func BenchmarkQueryElements(b *testing.B) {
	ctx := context.Background()
	replicas, closeReplicas := makeNReplicas(b, 1, nil)
	defer closeReplicas()

	for i := 0; i < b.N; i++ {
		k := ds.RandomKey()
		err := replicas[0].Put(ctx, k, make([]byte, 2000))
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()

	q := query.Query{
		KeysOnly: false,
	}
	results, err := replicas[0].Query(ctx, q)
	if err != nil {
		b.Fatal(err)
	}
	defer results.Close()

	totalSize := 0
	for r := range results.Next() {
		if r.Error != nil {
			b.Error(r.Error)
		}
		totalSize += len(r.Value)
	}
	b.Log(totalSize)
}

func TestRandomizeInterval(t *testing.T) {
	prevR := 100 * time.Second
	for i := 0; i < 1000; i++ {
		r := randomizeInterval(100 * time.Second)
		if r < 70*time.Second || r > 130*time.Second {
			t.Error("r was ", r)
		}
		if prevR == r {
			t.Log("r and prevR were equal")
		}
		prevR = r
	}
}

func TestCRDTPutPutDelete(t *testing.T) {
	replicas, closeReplicas := makeNReplicas(t, 2, nil)
	defer closeReplicas()

	ctx := context.Background()

	br0 := replicas[0].broadcaster.(*mockBroadcaster)
	br0.dropProb.Store(101)

	br1 := replicas[1].broadcaster.(*mockBroadcaster)
	br1.dropProb.Store(101)

	k := ds.NewKey("k1")

	// r0 - put put delete
	err := replicas[0].Put(ctx, k, []byte("r0-1"))
	if err != nil {
		t.Fatal(err)
	}
	err = replicas[0].Put(ctx, k, []byte("r0-2"))
	if err != nil {
		t.Fatal(err)
	}
	err = replicas[0].Delete(ctx, k)
	if err != nil {
		t.Fatal(err)
	}

	// r1 - put
	err = replicas[1].Put(ctx, k, []byte("r1-1"))
	if err != nil {
		t.Fatal(err)
	}

	br0.dropProb.Store(0)
	br1.dropProb.Store(0)

	time.Sleep(15 * time.Second)

	r0Res, err := replicas[0].Get(ctx, ds.NewKey("k1"))
	if err != nil {
		if !errors.Is(err, ds.ErrNotFound) {
			t.Fatal(err)
		}
	}

	r1Res, err := replicas[1].Get(ctx, ds.NewKey("k1"))
	if err != nil {
		t.Fatal(err)
	}
	closeReplicas()

	if string(r0Res) != string(r1Res) {
		fmt.Printf("r0Res: %s\nr1Res: %s\n", string(r0Res), string(r1Res))
		t.Log("r0 dag")
		replicas[0].PrintDAG(ctx)

		t.Log("r1 dag")
		replicas[1].PrintDAG(ctx)

		t.Fatal("r0 and r1 should have the same value")
	}
}

func TestMigration0to1(t *testing.T) {
	replicas, closeReplicas := makeNReplicas(t, 1, nil)
	defer closeReplicas()
	replica := replicas[0]
	ctx := context.Background()

	nItems := 200
	var keys []ds.Key
	// Add nItems
	for i := 0; i < nItems; i++ {
		k := ds.RandomKey()
		keys = append(keys, k)
		v := []byte(fmt.Sprintf("%d", i))
		err := replica.Put(ctx, k, v)
		if err != nil {
			t.Fatal(err)
		}

	}

	// Overwrite n/2 items 5 times to have multiple tombstones per key
	// later...
	for j := 0; j < 5; j++ {
		for i := 0; i < nItems/2; i++ {
			v := []byte(fmt.Sprintf("%d", i))
			err := replica.Put(ctx, keys[i], v)
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	// delete keys
	for i := 0; i < nItems/2; i++ {
		err := replica.Delete(ctx, keys[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	// And write them again
	for i := 0; i < nItems/2; i++ {
		err := replica.Put(ctx, keys[i], []byte("final value"))
		if err != nil {
			t.Fatal(err)
		}
	}

	// And now we manually put the wrong value
	for i := 0; i < nItems/2; i++ {
		valueK := replica.set.valueKey(keys[i].String())
		err := replica.set.store.Put(ctx, valueK, []byte("wrong value"))
		if err != nil {
			t.Fatal(err)
		}
		err = replica.set.setPriority(ctx, replica.set.store, keys[i].String(), 1)
		if err != nil {
			t.Fatal(err)
		}
	}

	err := replica.migrate0to1(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < nItems/2; i++ {
		v, err := replica.Get(ctx, keys[i])
		if err != nil {
			t.Fatal(err)
		}
		if string(v) != "final value" {
			t.Fatalf("value for elem %d should be final value: %s", i, string(v))
		}
	}
}

func TestGetAllMemberCommonHeads(t *testing.T) {
	ctx := context.Background()
	stateMgr, err := NewStateManager(ctx, ds.NewMapDatastore(), ds.NewKey(""), 300, log.Logger("crdt"))
	require.NoError(t, err)
	store := &Datastore{state: stateMgr}

	// Generate three valid dummy CIDs:
	head1CID := cid.MustParse("bafybeigdyrzt6xjftfs3m76psn3fupk7zvj7jl5p5zfw5v4ic22fxh45pe")
	head2CID := cid.MustParse("bafybeia6z6rlvqfwtknx7br7dxes5gj32rhrcprdz7sw5kxjdlkbkoqvfa")
	head3CID := cid.MustParse("bafybeig5jt5xfx4tn7zqomwqxg7tq6cphg7h5wr64p4quzpsolhx5ypmpu")

	snapshotA := &pb.Snapshot{SnapshotKey: &pb.Head{Cid: head1CID.Bytes()}}
	snapshotB := &pb.Snapshot{SnapshotKey: &pb.Head{Cid: head3CID.Bytes()}}

	stateMgr.state = &pb.StateBroadcast{
		Members: map[string]*pb.Participant{
			"peer1": {
				Snapshot: snapshotA,
				DagHeads: []*pb.Head{
					{Cid: head1CID.Bytes()},
					{Cid: head2CID.Bytes()},
				},
			},
			"peer2": {
				Snapshot: snapshotA,
				DagHeads: []*pb.Head{
					{Cid: head1CID.Bytes()},
					{Cid: head2CID.Bytes()},
				},
			},
			"peer3": {
				Snapshot: snapshotB,
				DagHeads: []*pb.Head{
					{Cid: head3CID.Bytes()},
				},
			},
		},
	}

	commonHeads := store.getAllMemberCommonHeads()
	require.Len(t, commonHeads, 2)
	require.Contains(t, commonHeads, head1CID)
	require.Contains(t, commonHeads, head2CID)
}
