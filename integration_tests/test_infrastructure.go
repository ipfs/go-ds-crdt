package integration_tests

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange"
	"github.com/ipfs/boxo/ipld/merkledag"
	mdutils "github.com/ipfs/boxo/ipld/merkledag/test"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-ds-crdt"
	"github.com/ipfs/go-ds-crdt/testutils"
	ipld "github.com/ipfs/go-ipld-format"
)

// MockBlockExchange simulates block exchange between replicas with partition awareness
// This allows replicas to fetch missing IPLD blocks from each other, but respects
// network partitions to simulate realistic failure scenarios
type MockBlockExchange struct {
	replicaID   int
	blockstores []blockstore.Blockstore
	network     *NetworkController
	t           testing.TB
}

// NewMockBlockExchange creates a block exchange service for a specific replica
func NewMockBlockExchange(replicaID int, blockstores []blockstore.Blockstore, network *NetworkController, t testing.TB) *MockBlockExchange {
	return &MockBlockExchange{
		replicaID:   replicaID,
		blockstores: blockstores,
		network:     network,
		t:           t,
	}
}

// GetBlock attempts to retrieve a block, first from local store, then from other replicas
func (mbe *MockBlockExchange) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	// First try local blockstore
	if block, err := mbe.blockstores[mbe.replicaID].Get(ctx, c); err == nil {
		return block, nil
	}

	// If not found locally, try to fetch from other replicas (if not partitioned)
	for i, bs := range mbe.blockstores {
		if i == mbe.replicaID {
			continue // Skip self
		}

		// Check if we can communicate with this replica
		if mbe.network.IsPartitioned(mbe.replicaID, i) {
			//mbe.t.Logf("Replica %d blocked from fetching block %s from replica %d (partitioned)", mbe.replicaID, c, i)
			continue // Skip partitioned replicas
		}

		// Try to get block from this replica
		if block, err := bs.Get(ctx, c); err == nil {
			// Found the block! Store it locally and return it
			if err := mbe.blockstores[mbe.replicaID].Put(ctx, block); err != nil {
				mbe.t.Logf("Warning: failed to cache block locally: %v", err)
			}
			//mbe.t.Logf("Replica %d fetched block %s from replica %d", mbe.replicaID, c, i)
			return block, nil
		}
	}

	// Block not found anywhere we can access
	return nil, fmt.Errorf("block not found: %s", c)
}

// GetBlocks attempts to retrieve multiple blocks
func (mbe *MockBlockExchange) GetBlocks(ctx context.Context, cids []cid.Cid) (<-chan blocks.Block, error) {
	out := make(chan blocks.Block, len(cids))
	go func() {
		defer close(out)
		for _, c := range cids {
			if block, err := mbe.GetBlock(ctx, c); err == nil {
				select {
				case out <- block:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

// HasBlock checks if a block exists locally
func (mbe *MockBlockExchange) HasBlock(ctx context.Context, c cid.Cid) (bool, error) {
	return mbe.blockstores[mbe.replicaID].Has(ctx, c)
}

// Close is a no-op for the mock implementation
func (mbe *MockBlockExchange) Close() error {
	return nil
}

// NotifyNewBlocks is a no-op for the mock implementation
func (mbe *MockBlockExchange) NotifyNewBlocks(ctx context.Context, blocks ...blocks.Block) error {
	return nil
}

// Ensure MockBlockExchange implements the exchange.Interface
var _ exchange.Interface = (*MockBlockExchange)(nil)

// PartitionableBroadcaster simulates network infrastructure failures between replicas
// In trusted networks, partitions are caused by infrastructure issues, not malicious actors
type PartitionableBroadcaster struct {
	id         int
	ctx        context.Context
	channels   []chan []byte
	myChannel  chan []byte
	partitions map[int]bool // if true, cannot communicate with replica i
	dropped    *atomic.Int64
	mutex      sync.RWMutex
	t          testing.TB
}

// NetworkController simulates infrastructure failures and network partitions
// Used to test realistic failure scenarios in trusted internal networks
type NetworkController struct {
	broadcasters []*PartitionableBroadcaster
	mutex        sync.RWMutex
}

func NewNetworkController(t testing.TB, n int) (*NetworkController, []crdt.Broadcaster, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	broadcasters := make([]*PartitionableBroadcaster, n)
	channels := make([]chan []byte, n)

	// Create channels for each broadcaster
	for i := range channels {
		channels[i] = make(chan []byte, 1000) // Large buffer for integration tests
	}

	// Create broadcasters
	for i := range broadcasters {
		broadcasters[i] = &PartitionableBroadcaster{
			id:         i,
			ctx:        ctx,
			channels:   channels,
			myChannel:  channels[i],
			partitions: make(map[int]bool),
			dropped:    &atomic.Int64{},
			t:          t,
		}
	}

	controller := &NetworkController{
		broadcasters: broadcasters,
	}

	// Convert to interface slice
	broadcastersIface := make([]crdt.Broadcaster, n)
	for i, b := range broadcasters {
		broadcastersIface[i] = b
	}

	return controller, broadcastersIface, cancel
}

// CreatePartition simulates infrastructure failure causing network partition between replica groups
// Examples: switch failures, firewall misconfigurations, datacenter connectivity issues
func (nc *NetworkController) CreatePartition(group1, group2 []int) {
	nc.mutex.Lock()
	defer nc.mutex.Unlock()

	for _, i := range group1 {
		for _, j := range group2 {
			nc.broadcasters[i].mutex.Lock()
			nc.broadcasters[i].partitions[j] = true
			nc.broadcasters[i].mutex.Unlock()

			nc.broadcasters[j].mutex.Lock()
			nc.broadcasters[j].partitions[i] = true
			nc.broadcasters[j].mutex.Unlock()
		}
	}
}

// CreateAsymmetricPartition creates a unidirectional partition where 'from' replicas cannot send to 'to' replicas
// but 'to' replicas can still send to 'from' replicas. This simulates scenarios like:
// - Firewall rules blocking outbound traffic from certain nodes
// - NAT issues preventing one-way communication
// - Asymmetric routing problems
func (nc *NetworkController) CreateAsymmetricPartition(from []int, to []int) {
	nc.mutex.Lock()
	defer nc.mutex.Unlock()

	for _, i := range from {
		for _, j := range to {
			nc.broadcasters[i].mutex.Lock()
			nc.broadcasters[i].partitions[j] = true
			nc.broadcasters[i].mutex.Unlock()
			// Note: we don't set partitions[i] = true on replica j, allowing j -> i communication
		}
	}
}

// HealPartition simulates infrastructure repair, restoring connectivity between all replicas
func (nc *NetworkController) HealPartition() {
	nc.mutex.Lock()
	defer nc.mutex.Unlock()

	for _, b := range nc.broadcasters {
		b.mutex.Lock()
		b.partitions = make(map[int]bool)
		b.mutex.Unlock()
	}
}

// SetMessageDropRate simulates network quality issues affecting message delivery
// Examples: packet loss, intermittent connectivity, overloaded network equipment
func (nc *NetworkController) SetMessageDropRate(replicaID int, dropRate int64) {
	nc.mutex.RLock()
	defer nc.mutex.RUnlock()

	if replicaID < len(nc.broadcasters) {
		nc.broadcasters[replicaID].dropped.Store(dropRate)
	}
}

// IsPartitioned returns true if two replicas cannot communicate
func (nc *NetworkController) IsPartitioned(replica1, replica2 int) bool {
	nc.mutex.RLock()
	defer nc.mutex.RUnlock()

	nc.broadcasters[replica1].mutex.RLock()
	isPartitioned := nc.broadcasters[replica1].partitions[replica2]
	nc.broadcasters[replica1].mutex.RUnlock()

	return isPartitioned
}

func (pb *PartitionableBroadcaster) Broadcast(ctx context.Context, data []byte) error {
	pb.mutex.RLock()
	defer pb.mutex.RUnlock()

	for i, ch := range pb.channels {
		// Skip self
		if i == pb.id {
			continue
		}

		// Skip if partitioned
		if pb.partitions[i] {
			//pb.t.Logf("Replica %d broadcast blocked to replica %d (partitioned)", pb.id, i)
			continue
		}

		// Apply message dropping
		if pb.dropped.Load() > 0 {
			// Simple drop probability
			if time.Now().UnixNano()%100 < pb.dropped.Load() {
				continue
			}
		}

		// Send with timeout to avoid blocking
		select {
		case ch <- data:
		case <-time.After(5 * time.Second):
			pb.t.Errorf("broadcast timeout to replica %d", i)
		case <-ctx.Done():
			return ctx.Err()
		case <-pb.ctx.Done():
			return pb.ctx.Err()
		}
	}

	return nil
}

func (pb *PartitionableBroadcaster) Next(ctx context.Context) ([]byte, error) {
	select {
	case data := <-pb.myChannel:
		return data, nil
	case <-ctx.Done():
		return nil, crdt.ErrNoMoreBroadcast
	case <-pb.ctx.Done():
		return nil, crdt.ErrNoMoreBroadcast
	}
}

// IntegrationTestReplicas creates replicas with controllable network
type IntegrationTestReplicas struct {
	Replicas   []*crdt.Datastore
	Network    *NetworkController
	Cleanup    func()
	Context    context.Context
	Cancel     context.CancelFunc
	Blockstore blockstore.Blockstore
}

func NewIntegrationTestReplicas(t testing.TB, n int, opts *crdt.Options) *IntegrationTestReplicas {
	ctx, cancel := context.WithCancel(context.Background())
	network, broadcasters, netCancel := NewNetworkController(t, n)

	// Create individual blockstores for each replica (realistic separation)
	blockstores := make([]blockstore.Blockstore, n)
	for i := 0; i < n; i++ {
		individualBS := mdutils.Bserv()
		blockstores[i] = individualBS.Blockstore()
	}

	// Create DAG services with MockBlockExchange for each replica
	dagServices := make([]ipld.DAGService, n)
	for i := 0; i < n; i++ {
		// Create mock block exchange that can fetch from other replicas
		mockExchange := NewMockBlockExchange(i, blockstores, network, t)

		// Create block service with the mock exchange
		blockService := blockservice.New(blockstores[i], mockExchange)

		// Create DAG service
		dagServices[i] = merkledag.NewDAGService(blockService)
	}

	replicas := make([]*crdt.Datastore, n)

	for i := 0; i < n; i++ {
		// Use separate datastore per replica to reduce mutex contention
		store := dssync.MutexWrap(ds.NewMapDatastore())
		peer := testutils.NewTestPeer(fmt.Sprintf("replica-%d", i))

		replicaOpts := crdt.DefaultOptions()
		if opts != nil {
			*replicaOpts = *opts
		}
		replicaOpts.Logger = testutils.NewTestLogger(fmt.Sprintf("replica-%d", i))
		replicaOpts.RebroadcastInterval = time.Second * 3 // Slightly longer to reduce pressure
		replicaOpts.NumWorkers = 2                        // Reduce workers to minimize lock contention
		replicaOpts.DAGSyncerTimeout = time.Second * 15   // Longer timeout

		var err error
		replicas[i], err = crdt.New(
			peer,
			store,
			blockstores[i],
			ds.NewKey(fmt.Sprintf("integration-test-%d", i)),
			dagServices[i],
			broadcasters[i],
			replicaOpts,
		)
		if err != nil {
			t.Fatal(err)
		}
	}

	cleanup := func() {
		for _, r := range replicas {
			if r != nil {
				r.Close()
			}
		}
		netCancel()
		cancel()
	}

	return &IntegrationTestReplicas{
		Replicas:   replicas,
		Network:    network,
		Cleanup:    cleanup,
		Context:    ctx,
		Cancel:     cancel,
		Blockstore: blockstores[0], // Use first replica's blockstore for interface compatibility
	}
}

// WaitForConsistency waits for all replicas to reach consistency
func (itr *IntegrationTestReplicas) WaitForConsistency(t testing.TB, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(itr.Context, timeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatal("timeout waiting for consistency")
		case <-ticker.C:
			if itr.AreConsistent(t) {
				return
			}
		}
	}
}

// AreConsistent checks if all replicas have the same set of keys and values
func (itr *IntegrationTestReplicas) AreConsistent(t testing.TB) bool {
	if len(itr.Replicas) == 0 {
		return true
	}

	// Get all keys from first replica
	ctx := context.Background()
	results, err := itr.Replicas[0].Query(ctx, query.Query{})
	if err != nil {
		t.Errorf("query error: %v", err)
		return false
	}
	defer results.Close()

	expectedEntries := make(map[string][]byte)
	for result := range results.Next() {
		if result.Error != nil {
			t.Errorf("result error: %v", result.Error)
			return false
		}
		expectedEntries[result.Key] = result.Value
	}

	// Check all other replicas have the same data
	for i, replica := range itr.Replicas[1:] {
		results, err := replica.Query(ctx, query.Query{})
		if err != nil {
			t.Errorf("replica %d query error: %v", i+1, err)
			return false
		}

		actualEntries := make(map[string][]byte)
		for result := range results.Next() {
			if result.Error != nil {
				t.Errorf("replica %d result error: %v", i+1, result.Error)
				results.Close()
				return false
			}
			actualEntries[result.Key] = result.Value
		}
		results.Close()

		// Compare entries
		if len(expectedEntries) != len(actualEntries) {
			t.Logf("replica %d has %d entries, expected %d", i+1, len(actualEntries), len(expectedEntries))
			return false
		}

		for key, expectedValue := range expectedEntries {
			if actualValue, exists := actualEntries[key]; !exists {
				t.Logf("replica %d missing key %s", i+1, key)
				return false
			} else if string(actualValue) != string(expectedValue) {
				t.Logf("replica %d has different value for key %s: got %q, expected %q",
					i+1, key, actualValue, expectedValue)
				return false
			}
		}
	}

	return true
}

// GetReplicaStats returns stats for each replica
func (itr *IntegrationTestReplicas) GetReplicaStats(t testing.TB) []crdt.Stats {
	ctx := context.Background()
	stats := make([]crdt.Stats, len(itr.Replicas))
	for i, replica := range itr.Replicas {
		stats[i] = replica.InternalStats(ctx)
	}
	return stats
}

// NetworkLatencySimulator adds realistic network delays
type NetworkLatencySimulator struct {
	broadcaster crdt.Broadcaster
	minLatency  time.Duration
	maxLatency  time.Duration
}

func NewNetworkLatencySimulator(broadcaster crdt.Broadcaster, minLatency, maxLatency time.Duration) *NetworkLatencySimulator {
	return &NetworkLatencySimulator{
		broadcaster: broadcaster,
		minLatency:  minLatency,
		maxLatency:  maxLatency,
	}
}

func (nls *NetworkLatencySimulator) Broadcast(ctx context.Context, data []byte) error {
	// Simulate network latency
	latency := nls.minLatency + time.Duration(time.Now().UnixNano()%(int64(nls.maxLatency-nls.minLatency)))
	time.Sleep(latency)

	return nls.broadcaster.Broadcast(ctx, data)
}

func (nls *NetworkLatencySimulator) Next(ctx context.Context) ([]byte, error) {
	return nls.broadcaster.Next(ctx)
}

// ConcurrentOperationTester helps test concurrent operations
type ConcurrentOperationTester struct {
	replicas []*crdt.Datastore
	wg       sync.WaitGroup
	errors   chan error
	results  chan TestResult
}

type TestResult struct {
	ReplicaID int
	Key       string
	Value     []byte
	Operation string
	Timestamp time.Time
	Error     error
}

func NewConcurrentOperationTester(replicas []*crdt.Datastore) *ConcurrentOperationTester {
	return &ConcurrentOperationTester{
		replicas: replicas,
		errors:   make(chan error, 100),
		results:  make(chan TestResult, 1000),
	}
}

func (cot *ConcurrentOperationTester) ExecuteConcurrentWrites(t testing.TB, keys []string, values [][]byte, numOperations int) {
	cot.wg.Add(numOperations)

	for i := 0; i < numOperations; i++ {
		go func(opID int) {
			defer cot.wg.Done()

			replicaID := opID % len(cot.replicas)
			keyID := opID % len(keys)
			valueID := opID % len(values)

			key := ds.NewKey(keys[keyID])
			value := values[valueID]

			start := time.Now()
			err := cot.replicas[replicaID].Put(context.Background(), key, value)

			result := TestResult{
				ReplicaID: replicaID,
				Key:       key.String(),
				Value:     value,
				Operation: "PUT",
				Timestamp: start,
				Error:     err,
			}

			select {
			case cot.results <- result:
			default:
				t.Errorf("results channel full")
			}

			if err != nil {
				select {
				case cot.errors <- err:
				default:
					t.Errorf("errors channel full")
				}
			}
		}(i)
	}

	cot.wg.Wait()
	close(cot.results)
	close(cot.errors)
}

func (cot *ConcurrentOperationTester) GetResults() ([]TestResult, []error) {
	var results []TestResult
	var errors []error

	for result := range cot.results {
		results = append(results, result)
	}

	for err := range cot.errors {
		errors = append(errors, err)
	}

	return results, errors
}
