package crdt

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	dssync "github.com/ipfs/go-datastore/sync"
	dstest "github.com/ipfs/go-datastore/test"
	badgerds "github.com/ipfs/go-ds-badger"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	ipld "github.com/ipfs/go-ipld-format"
	log "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-merkledag"
	mdutils "github.com/ipfs/go-merkledag/test"
)

var numReplicas = 15
var debug = false

const (
	mapStore = iota
	badgerStore
)

var store int = mapStore

func init() {
	rand.Seed(time.Now().UnixNano())
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
	dropProb int // probability of dropping a message instead of receiving it
	t        *testing.T
}

func newBroadcasters(t *testing.T, n int) ([]*mockBroadcaster, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	broadcasters := make([]*mockBroadcaster, n, n)
	chans := make([]chan []byte, n, n)
	for i := range chans {
		chans[i] = make(chan []byte, 300)
		broadcasters[i] = &mockBroadcaster{
			ctx:      ctx,
			chans:    chans,
			myChan:   chans[i],
			dropProb: 0,
			t:        t,
		}
	}
	return broadcasters, cancel
}

func (mb *mockBroadcaster) Broadcast(data []byte) error {
	var wg sync.WaitGroup
	for i, ch := range mb.chans {
		n := rand.Intn(100)
		if n < mb.dropProb {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			// randomize when we send a little bit
			if rand.Intn(100) < 30 {
				time.Sleep(1)
			}
			timer := time.NewTimer(5 * time.Second)
			defer timer.Stop()

			select {
			case ch <- data:
			case <-timer.C:
				mb.t.Errorf("broadcasting to %d timed out", i)
			}
		}()
		wg.Wait()
	}
	return nil
}

func (mb *mockBroadcaster) Next() ([]byte, error) {
	select {
	case data := <-mb.myChan:
		return data, nil
	case <-mb.ctx.Done():
		return nil, ErrNoMoreBroadcast
	}
}

type mockDAGSync struct {
	ipld.DAGService
	bs             blockstore.Blockstore
	knownBlocksMux sync.RWMutex
	knownBlocks    map[cid.Cid]struct{}
}

func (mds *mockDAGSync) HasBlock(c cid.Cid) (bool, error) {
	mds.knownBlocksMux.RLock()
	_, ok := mds.knownBlocks[c]
	mds.knownBlocksMux.RUnlock()
	return ok, nil
}

func (mds *mockDAGSync) Add(ctx context.Context, n ipld.Node) error {
	mds.knownBlocksMux.Lock()
	mds.knownBlocks[n.Cid()] = struct{}{}
	mds.knownBlocksMux.Unlock()
	return mds.DAGService.Add(ctx, n)
}

func (mds *mockDAGSync) Get(ctx context.Context, c cid.Cid) (ipld.Node, error) {
	nd, err := mds.DAGService.Get(ctx, c)
	if err != nil {
		return nd, err
	}
	mds.knownBlocksMux.Lock()
	mds.knownBlocks[c] = struct{}{}
	mds.knownBlocksMux.Unlock()
	return nd, nil
}

func (mds *mockDAGSync) GetMany(ctx context.Context, cids []cid.Cid) <-chan *ipld.NodeOption {
	ch := make(chan *ipld.NodeOption)
	go func() {
		defer close(ch)
		resp := mds.DAGService.GetMany(ctx, cids)
		for no := range resp {
			if no.Err == nil {
				mds.knownBlocksMux.Lock()
				mds.knownBlocks[no.Node.Cid()] = struct{}{}
				mds.knownBlocksMux.Unlock()
			}
			ch <- no
		}
	}()

	return ch
}

func storeFolder(i int) string {
	return fmt.Sprintf("/dev/shm/test-badger-%d", i)
}

func makeStore(t *testing.T, i int) ds.Datastore {
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

func makeReplicas(t *testing.T, opts *Options) ([]*Datastore, func()) {
	bcasts, bcastCancel := newBroadcasters(t, numReplicas)
	bs := mdutils.Bserv()
	dagserv := merkledag.NewDAGService(bs)

	replicaOpts := make([]*Options, numReplicas)
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
		replicaOpts[i].RebroadcastInterval = time.Second * 10
		replicaOpts[i].NumWorkers = 5
		replicaOpts[i].DAGSyncerTimeout = time.Second
	}

	replicas := make([]*Datastore, numReplicas, numReplicas)
	for i := range replicas {
		dagsync := &mockDAGSync{
			DAGService:  dagserv,
			bs:          bs.Blockstore(),
			knownBlocks: make(map[cid.Cid]struct{}),
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

func TestCRDT(t *testing.T) {
	replicas, closeReplicas := makeReplicas(t, nil)
	defer closeReplicas()
	k := ds.NewKey("hi")
	err := replicas[0].Put(k, []byte("hola"))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)

	for _, r := range replicas {
		v, err := r.Get(k)
		if err != nil {
			t.Error(err)
		}
		if string(v) != "hola" {
			t.Error("bad content: ", string(v))
		}
	}
}

func TestDatastoreSuite(t *testing.T) {
	numReplicasOld := numReplicas
	numReplicas = 1
	defer func() {
		numReplicas = numReplicasOld
	}()
	opts := DefaultOptions()
	opts.MaxBatchDeltaSize = 200 * 1024 * 1024 // 200 MB
	replicas, closeReplicas := makeReplicas(t, opts)
	defer closeReplicas()

	go func() {
		for {
			q := query.Query{KeysOnly: true}
			results, err := replicas[0].store.Query(q)
			if err != nil {
				t.Fatal(err)
			}
			defer results.Close()
			rest, err := results.Rest()
			if err != nil {
				t.Fatal(err)
			}
			fmt.Println(len(rest))
			time.Sleep(5 * time.Second)
		}
	}()

	dstest.SubtestAll(t, replicas[0])
	time.Sleep(time.Second)

	for _, r := range replicas {
		q := query.Query{KeysOnly: true}
		results, err := r.Query(q)
		if err != nil {
			t.Fatal(err)
		}
		defer results.Close()
		rest, err := results.Rest()
		if err != nil {
			t.Fatal(err)
		}
		if len(rest) != 0 {
			t.Error("all elements in the suite should be gone")
		}
	}
}

func TestCRDTSync(t *testing.T) {
	nItems := 50

	replicas, closeReplicas := makeReplicas(t, nil)
	defer closeReplicas()

	// Add nItems choosing the replica randomly
	for i := 0; i < nItems; i++ {
		k := ds.RandomKey()
		v := []byte(fmt.Sprintf("%d", i))
		n := rand.Intn(len(replicas))
		err := replicas[n].Put(k, v)
		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(500 * time.Millisecond)

	// Query all items
	q := query.Query{
		KeysOnly: true,
	}
	results, err := replicas[0].Query(q)
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
			ok, err := r.Has(ds.NewKey(res.Key))
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
		n := rand.Intn(len(replicas))
		err := replicas[n].Put(ds.NewKey(r.Key), []byte("hola"))
		if err != nil {
			t.Error(err)
		}
	}

	time.Sleep(200 * time.Millisecond)

	// query everything again
	results, err = replicas[0].Query(q)
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
			v, err := r.Get(k)
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
		list, _, err := r.heads.List()
		if err != nil {
			t.Fatal(err)
		}
		t.Log(list)
	}
	//replicas[0].PrintDAG()
	//fmt.Println("==========================================================")
	//replicas[1].PrintDAG()
}

func TestCRDTPriority(t *testing.T) {
	nItems := 50

	replicas, closeReplicas := makeReplicas(t, nil)
	defer closeReplicas()

	k := ds.NewKey("k")

	for i, r := range replicas {
		for j := 0; j < nItems; j++ {
			err := r.Put(k, []byte(fmt.Sprintf("r#%d", i)))
			if err != nil {
				t.Error(err)
			}
		}
	}
	time.Sleep(500 * time.Millisecond)
	var v, lastv []byte
	var err error
	for i, r := range replicas {
		v, err = r.Get(k)
		if err != nil {
			t.Error(err)
		}
		t.Logf("Replica %d got value %s", i, string(v))
		if lastv != nil && string(v) != string(lastv) {
			t.Error("value was different between replicas, but should be the same")
		}
		lastv = v
	}

	err = replicas[0].Put(k, []byte("final value"))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)

	for i, r := range replicas {
		v, err := r.Get(k)
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
	nItems := 50
	replicas, closeReplicas := makeReplicas(t, nil)
	defer closeReplicas()

	r := replicas[len(replicas)-1]
	br := r.broadcaster.(*mockBroadcaster)
	br.dropProb = 101

	// this items will not get to anyone
	for i := 0; i < nItems; i++ {
		k := ds.RandomKey()
		err := r.Put(k, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(100 * time.Millisecond)
	br.dropProb = 0

	// this message will get to everyone
	err := r.Put(ds.RandomKey(), nil)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)
	q := query.Query{KeysOnly: true}
	results, err := replicas[0].Query(q)
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
	nItems := 5
	replicas, closeReplicas := makeReplicas(t, nil)
	defer closeReplicas()

	// this items will not get to anyone
	for i := 0; i < nItems; i++ {
		k := ds.RandomKey()
		err := replicas[0].Put(k, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	err := replicas[0].PrintDAG()
	if err != nil {
		t.Fatal(err)
	}
}

func TestCRDTHooks(t *testing.T) {
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
	err := replicas[0].Put(k, nil)
	if err != nil {
		t.Fatal(err)
	}

	err = replicas[0].Delete(k)
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
	opts := DefaultOptions()
	opts.MaxBatchDeltaSize = 500 // bytes

	replicas, closeReplicas := makeReplicas(t, opts)
	defer closeReplicas()

	btch, err := replicas[0].Batch()
	if err != nil {
		t.Fatal(err)
	}

	// This should be batched
	k := ds.RandomKey()
	err = btch.Put(k, make([]byte, 200))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := replicas[0].Get(k); err != ds.ErrNotFound {
		t.Fatal("should not have commited the batch")
	}

	k2 := ds.RandomKey()
	err = btch.Put(k2, make([]byte, 400))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := replicas[0].Get(k2); err != nil {
		t.Fatal("should have commited the batch: delta size was over threshold")
	}

	err = btch.Delete(k)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := replicas[0].Get(k); err != nil {
		t.Fatal("should not have committed the batch")
	}

	err = btch.Commit()
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	for _, r := range replicas {
		if _, err := r.Get(k); err != ds.ErrNotFound {
			t.Error("k should have been deleted everywhere")
		}
		if _, err := r.Get(k2); err != nil {
			t.Error("k2 should be everywhere")
		}
	}
}

func TestCRDTNamespaceClash(t *testing.T) {
	opts := DefaultOptions()
	replicas, closeReplicas := makeReplicas(t, opts)
	defer closeReplicas()

	k := ds.NewKey("path/to/something")
	err := replicas[0].Put(k, nil)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	k = ds.NewKey("path")
	ok, err := replicas[0].Has(k)
	if ok {
		t.Error("it should not have the key")
	}

	_, err = replicas[0].Get(k)
	if err != ds.ErrNotFound {
		t.Error("should return err not found")
	}

	err = replicas[0].Put(k, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}

	v, err := replicas[0].Get(k)
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != "hello" {
		t.Error("wrong value read from database")
	}

	err = replicas[0].Delete(ds.NewKey("path/to/something"))
	if err != nil {
		t.Fatal(err)
	}

	v, err = replicas[0].Get(k)
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != "hello" {
		t.Error("wrong value read from database")
	}
}
