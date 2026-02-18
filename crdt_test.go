package crdt

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	blockstore "github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/ipld/merkledag"
	mdutils "github.com/ipfs/boxo/ipld/merkledag/test"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	dssync "github.com/ipfs/go-datastore/sync"
	dstest "github.com/ipfs/go-datastore/test"
	pebbleds "github.com/ipfs/go-ds-pebble"
	ipld "github.com/ipfs/go-ipld-format"
	log "github.com/ipfs/go-log/v2"
	"github.com/multiformats/go-multihash"
)

var numReplicas = 15
var debug = false

const (
	mapStore = iota
	pebbleStore
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
	dropProb *atomic.Int64 // probability of dropping a message instead of broadcasting it
	t        testing.TB
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

func storeFolder(i int) string {
	return fmt.Sprintf("test-pebble-%d", i)
}

func makeStore(t testing.TB, i int) ds.Datastore {
	t.Helper()

	switch store {
	case mapStore:
		return dssync.MutexWrap(ds.NewMapDatastore())
	case pebbleStore:
		folder := storeFolder(i)
		err := os.MkdirAll(folder, 0700)
		if err != nil {
			t.Fatal(err)
		}

		dstore, err := pebbleds.NewDatastore(folder)
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

		var err error
		replicas[i], err = New(
			makeStore(t, i),
			// ds.NewLogDatastore(
			// 	makeStore(t, i),
			// 	fmt.Sprintf("crdt-test-%d", i),
			// ),
			ds.NewKey("crdttest"),
			dagsync,
			bcasts[i],
			replicaOpts[i],
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	if debug {
		// nolint:errcheck
		log.SetLogLevel("crdt", "debug")
	}

	closeReplicas := func() {
		bcastCancel()
		for i, r := range replicas {
			err := r.Close()
			if err != nil {
				t.Error(err)
			}
			// nolint:errcheck
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
	// nolint:errcheck
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
	// nolint:errcheck
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
	time.Sleep(5000 * time.Millisecond)
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
	// nolint:errcheck
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
		atomic.AddInt64(&put, 1)
	}
	opts.DeleteHook = func(k ds.Key) {
		atomic.AddInt64(&deleted, 1)
	}

	replicas, closeReplicas := makeReplicas(t, opts)
	defer closeReplicas()

	k := ds.RandomKey()
	err := replicas[0].Put(ctx, k, nil)
	if err != nil {
		t.Fatal(err)
	}

	err = replicas[0].Delete(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if atomic.LoadInt64(&put) != int64(len(replicas)) {
		t.Error("all replicas should have notified Put", put)
	}
	if atomic.LoadInt64(&deleted) != int64(len(replicas)) {
		t.Error("all replicas should have notified Remove", deleted)
	}
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

func TestCRDTBroadcastBackwardsCompat(t *testing.T) {
	ctx := context.Background()
	mh, err := multihash.Sum([]byte("emacs is best"), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	cidV0 := cid.NewCidV0(mh)

	opts := DefaultOptions()
	replicas, closeReplicas := makeReplicas(t, opts)
	defer closeReplicas()

	heads, err := replicas[0].decodeBroadcast(ctx, cidV0.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	if len(heads) != 1 || !heads[0].Cid.Equals(cidV0) {
		t.Error("should have returned a single cidV0", heads)
	}

	data, err := replicas[0].encodeBroadcast(ctx, heads)
	if err != nil {
		t.Fatal(err)
	}

	heads2, err := replicas[0].decodeBroadcast(ctx, data)
	if err != nil {
		t.Fatal(err)
	}

	if len(heads2) != 1 || !heads2[0].Cid.Equals(cidV0) {
		t.Error("should have reparsed cid0", heads2)
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
	// nolint:errcheck
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
		// nolint:errcheck
		replicas[0].PrintDAG(ctx)

		t.Log("r1 dag")
		// nolint:errcheck
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

func TestCRDTDagNames(t *testing.T) {
	// make 2 replicas
	replicas, closeReplicas := makeNReplicas(t, 2, nil)
	defer closeReplicas()

	ctx := context.Background()

	nItems := 50

	// create delta manually for each item with store.set.Add()
	// delta.SetDagName() alternatively to "dag1" and "dag2"
	for i := 0; i < nItems; i++ {
		k := ds.RandomKey()
		v := []byte(fmt.Sprintf("value-%d", i))

		// Create delta manually
		delta, err := replicas[0].set.Add(ctx, k.String(), v)
		if err != nil {
			t.Fatal(err)
		}

		// Set different DAG names for alternating items
		if i%2 == 0 {
			delta.SetDagName("dag1")
		} else {
			delta.SetDagName("dag2")
		}

		// Commit the delta
		_, err = replicas[0].publish(ctx, delta)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Wait for propagation
	time.Sleep(100 * time.Millisecond)

	// verify there are 2 distinct heads (.List())
	heads, _, err := replicas[0].heads.List(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// replicas[0].PrintDAG(ctx)

	if len(heads) != 2 {
		t.Fatalf("Expected 2 heads, got %d", len(heads))
	}

	// verify one head has dagName set to "dag1" and the other to "dag2"
	headDagNames := make(map[string]bool)
	for _, head := range heads {
		headDagNames[head.DAGName] = true
	}

	if !headDagNames["dag1"] {
		t.Error("Expected a head with DAGName 'dag1'")
	}

	if !headDagNames["dag2"] {
		t.Error("Expected a head with DAGName 'dag2'")
	}

	var expectedDagName string
	visit := func(n ipld.Node) error {
		dbytes, err := extractDelta(n)
		if err != nil {
			t.Fatal(err)
		}
		d := replicas[0].opts.crdtOpts.DeltaFactory()
		d.Unmarshal(dbytes)

		if d.GetDagName() != expectedDagName {
			return fmt.Errorf("wrong dagName in subtree: got %s, expected %s", d.GetDagName(), expectedDagName)
		}
		return nil
	}

	mcrdt := MerkleCRDT{replicas[0]}
	for _, h := range heads {
		expectedDagName = h.DAGName
		err := mcrdt.Traverse(ctx, []cid.Cid{h.Cid}, visit)
		if err != nil {
			t.Error(err)
		}
	}
}

func TestCRDTHeadsSaveLoad(t *testing.T) {
	// make 1 replica
	replicas, closeReplicas := makeNReplicas(t, 1, nil)
	defer closeReplicas()
	replica := replicas[0]
	ctx := context.Background()

	// for dagname = range ["", "dag1", "dag2"]:
	//  generate a random cid
	//  add a head to the replica heads.
	dagNames := []string{"", "dag1", "dag2"}
	for _, dagName := range dagNames {
		// Generate a random cid
		k := ds.RandomKey()
		pref := cid.Prefix{
			Version: 1,
		}

		c, err := pref.Sum([]byte(k.String()))
		if err != nil {
			t.Fatal(err)
		}

		// Add a head to the replica heads
		head := Head{
			Cid: c,
			HeadValue: HeadValue{
				Height:  1,
				DAGName: dagName,
			},
		}
		err = replica.heads.Add(ctx, head)
		if err != nil {
			t.Fatal(err)
		}
	}

	heads, err := newHeads(ctx, replica.store, replica.heads.namespace, replica.heads.namespaceDags, replica.heads.logger)
	if err != nil {
		t.Fatal(err)
	}

	// Verify there are 3 heads and they are the ones written before
	newheads, _, err := heads.List(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if len(newheads) != 3 {
		t.Fatalf("Expected 3 heads, got %d", len(newheads))
	}

	// Verify the heads have the correct DAG names
	headDagNames := make(map[string]bool)
	for _, head := range newheads {
		headDagNames[head.DAGName] = true
	}

	if !headDagNames[""] {
		t.Error("Expected a head with DAGName ''")
	}

	if !headDagNames["dag1"] {
		t.Error("Expected a head with DAGName 'dag1'")
	}

	if !headDagNames["dag2"] {
		t.Error("Expected a head with DAGName 'dag2'")
	}
}
