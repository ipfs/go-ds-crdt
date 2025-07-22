package testutils

import (
	"context"
	"crypto/ed25519"
	"crypto/sha512"
	"fmt"
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
	dssync "github.com/ipfs/go-datastore/sync"
	badgerds "github.com/ipfs/go-ds-badger"
	"github.com/ipfs/go-ds-crdt"
	ipld "github.com/ipfs/go-ipld-format"
	log "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	MapStore = iota
	BadgerStore
)

// TestLogger wraps the standard logger with test-specific naming
type TestLogger struct {
	name string
	l    log.StandardLogger
}

func (tl *TestLogger) Debug(args ...interface{}) {
	args = append([]interface{}{tl.name}, args...)
	tl.l.Debug(args...)
}
func (tl *TestLogger) Debugf(format string, args ...interface{}) {
	args = append([]interface{}{tl.name}, args...)
	tl.l.Debugf("%s "+format, args...)
}
func (tl *TestLogger) Error(args ...interface{}) {
	args = append([]interface{}{tl.name}, args...)
	tl.l.Error(args...)
}
func (tl *TestLogger) Errorf(format string, args ...interface{}) {
	args = append([]interface{}{tl.name}, args...)
	tl.l.Errorf("%s "+format, args...)
}
func (tl *TestLogger) Fatal(args ...interface{}) {
	args = append([]interface{}{tl.name}, args...)
	tl.l.Fatal(args...)
}
func (tl *TestLogger) Fatalf(format string, args ...interface{}) {
	args = append([]interface{}{tl.name}, args...)
	tl.l.Fatalf("%s "+format, args...)
}
func (tl *TestLogger) Info(args ...interface{}) {
	args = append([]interface{}{tl.name}, args...)
	tl.l.Info(args...)
}
func (tl *TestLogger) Infof(format string, args ...interface{}) {
	args = append([]interface{}{tl.name}, args...)
	tl.l.Infof("%s "+format, args...)
}
func (tl *TestLogger) Panic(args ...interface{}) {
	args = append([]interface{}{tl.name}, args...)
	tl.l.Panic(args...)
}
func (tl *TestLogger) Panicf(format string, args ...interface{}) {
	args = append([]interface{}{tl.name}, args...)
	tl.l.Panicf("%s "+format, args...)
}
func (tl *TestLogger) Warn(args ...interface{}) {
	args = append([]interface{}{tl.name}, args...)
	tl.l.Warn(args...)
}
func (tl *TestLogger) Warnf(format string, args ...interface{}) {
	args = append([]interface{}{tl.name}, args...)
	tl.l.Warnf("%s "+format, args...)
}

// NewTestLogger creates a logger for tests with the test name as prefix
func NewTestLogger(name string) *TestLogger {
	return &TestLogger{
		name: name,
		l:    crdt.DefaultOptions().Logger,
	}
}

// NewTestDatastore creates a test datastore with the specified type
func NewTestDatastore(t testing.TB, storeType int, id int) ds.Datastore {
	t.Helper()

	switch storeType {
	case MapStore:
		return dssync.MutexWrap(ds.NewMapDatastore())
	case BadgerStore:
		folder := fmt.Sprintf("test-badger-%d", id)
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
		t.Cleanup(func() {
			dstore.Close()
			os.RemoveAll(folder)
		})
		return dstore
	default:
		t.Fatal("bad store type selected for tests")
		return nil
	}
}

// NewTestDAGService creates a mock DAG service for testing
func NewTestDAGService() ipld.DAGService {
	return mdutils.Mock()
}

// NewTestBlockstore creates a test blockstore
func NewTestBlockstore() blockstore.Blockstore {
	return mdutils.Bserv().Blockstore()
}

// MockPeer implements the Peer interface for testing
type MockPeer struct {
	id peer.ID
}

func (m *MockPeer) ID() peer.ID {
	return m.id
}

// NewTestPeer creates a test peer with a deterministic ID based on the seed
func NewTestPeer(seed string) *MockPeer {
	pk := generateEd25519Key(seed)
	pid, err := peer.IDFromPublicKey(pk.GetPublic())
	if err != nil {
		panic(err)
	}
	return &MockPeer{id: pid}
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

// MockBroadcaster implements the Broadcaster interface for testing
type MockBroadcaster struct {
	ctx      context.Context
	chans    []chan []byte
	myChan   chan []byte
	dropProb *atomic.Int64 // probability of dropping a message instead of receiving it
	t        testing.TB
	mu       sync.RWMutex
}

// NewTestBroadcasters creates n connected mock broadcasters for testing
func NewTestBroadcasters(t testing.TB, n int) ([]*MockBroadcaster, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	broadcasters := make([]*MockBroadcaster, n)
	chans := make([]chan []byte, n)
	for i := range chans {
		dropP := &atomic.Int64{}
		chans[i] = make(chan []byte, 300)
		broadcasters[i] = &MockBroadcaster{
			ctx:      ctx,
			chans:    chans,
			myChan:   chans[i],
			dropProb: dropP,
			t:        t,
		}
	}
	return broadcasters, cancel
}

func (mb *MockBroadcaster) Broadcast(ctx context.Context, data []byte) error {
	mb.mu.RLock()
	defer mb.mu.RUnlock()
	var wg sync.WaitGroup

	for i, ch := range mb.chans {
		if mb.dropProb.Load() > 0 {
			// Implement drop probability logic if needed
		}
		wg.Add(1)
		go func(i int, ch chan []byte) {
			defer wg.Done()
			timer := time.NewTimer(5 * time.Second)
			defer timer.Stop()

			select {
			case ch <- data:
			case <-timer.C:
				mb.t.Errorf("broadcasting to %d timed out", i)
			}
		}(i, ch)
	}
	wg.Wait()
	return nil
}

func (mb *MockBroadcaster) Next(ctx context.Context) ([]byte, error) {
	select {
	case data := <-mb.myChan:
		return data, nil
	case <-ctx.Done():
		return nil, crdt.ErrNoMoreBroadcast
	case <-mb.ctx.Done():
		return nil, crdt.ErrNoMoreBroadcast
	}
}

// MockDAGService wraps a DAG service for testing
type MockDAGService struct {
	ipld.DAGService
	bs blockstore.Blockstore
}

func (mds *MockDAGService) Add(ctx context.Context, n ipld.Node) error {
	return mds.DAGService.Add(ctx, n)
}

func (mds *MockDAGService) Get(ctx context.Context, c cid.Cid) (ipld.Node, error) {
	return mds.DAGService.Get(ctx, c)
}

func (mds *MockDAGService) GetMany(ctx context.Context, cids []cid.Cid) <-chan *ipld.NodeOption {
	return mds.DAGService.GetMany(ctx, cids)
}

// NewMockDAGService creates a mock DAG service with blockstore
func NewMockDAGService() (*MockDAGService, blockstore.Blockstore) {
	bs := mdutils.Bserv()
	dagserv := merkledag.NewDAGService(bs)
	return &MockDAGService{
		DAGService: dagserv,
		bs:         bs.Blockstore(),
	}, bs.Blockstore()
}

// TestReplicas creates n CRDT replicas for testing
func NewTestReplicas(t testing.TB, n int, storeType int, opts *crdt.Options) ([]*crdt.Datastore, func()) {
	bcasts, bcastCancel := NewTestBroadcasters(t, n)

	dagSvc, bs := NewMockDAGService()

	replicaOpts := make([]*crdt.Options, n)
	for i := range replicaOpts {
		if opts == nil {
			replicaOpts[i] = crdt.DefaultOptions()
		} else {
			copy := *opts
			replicaOpts[i] = &copy
		}

		replicaOpts[i].Logger = NewTestLogger(fmt.Sprintf("r#%d", i))
		replicaOpts[i].RebroadcastInterval = time.Second * 5
		replicaOpts[i].NumWorkers = 5
		replicaOpts[i].DAGSyncerTimeout = time.Second
	}

	replicas := make([]*crdt.Datastore, n)
	for i := range replicas {
		dagsync := &MockDAGService{
			DAGService: dagSvc.DAGService,
			bs:         bs,
		}

		h := NewTestPeer(fmt.Sprintf("peer-%d", i))
		var err error
		replicas[i], err = crdt.New(
			h,
			NewTestDatastore(t, storeType, i),
			bs,
			ds.NewKey("crdttest"),
			dagsync,
			bcasts[i],
			replicaOpts[i],
		)
		if err != nil {
			t.Fatal(err)
		}
	}

	cleanup := func() {
		for _, r := range replicas {
			r.Close()
		}
		bcastCancel()
	}

	return replicas, cleanup
}

// Common test contexts and timeouts
func NewTestContext() context.Context {
	return context.Background()
}

func NewTestContextWithTimeout(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout)
}
