package crdt

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-ds-crdt/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"
)

// StateManager manages a StateBroadcast proto object in a datastore.
type StateManager struct {
	datastore          ds.Datastore
	key                ds.Key
	state              *pb.StateBroadcast
	clock              clock.Clock
	ttl                time.Duration
	mu                 sync.Mutex // Lock for preventing concurrent access to state
	onMembershipUpdate func(members map[string]*pb.Participant)
}

// NewStateManager initializes a StateManager and loads the state from the datastore.
func NewStateManager(ctx context.Context, datastore ds.Datastore, key ds.Key, ttl time.Duration) (*StateManager, error) {
	manager := &StateManager{
		datastore: datastore,
		key:       key,
		state:     &pb.StateBroadcast{},
		clock:     clock.New(),
		ttl:       ttl,
	}
	if err := manager.Load(ctx); err != nil {
		return nil, err
	}
	return manager, nil
}

// Load loads the StateBroadcast from the datastore into memory.
func (m *StateManager) Load(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, err := m.datastore.Get(ctx, m.key)
	if errors.Is(err, ds.ErrNotFound) {
		// Initialize with an empty state if not found.
		m.state = &pb.StateBroadcast{
			Members: make(map[string]*pb.Participant),
		}
		return nil
	} else if err != nil {
		return err
	}

	if err := proto.Unmarshal(data, m.state); err != nil {
		return err
	}
	return nil
}

// Save saves the current StateBroadcast to the datastore.
func (m *StateManager) Save(ctx context.Context) error {
	data, err := proto.Marshal(m.state)
	if err != nil {
		return err
	}
	return m.datastore.Put(ctx, m.key, data)
}

// GetState returns a copy of the current StateBroadcast.
func (m *StateManager) GetState() *pb.StateBroadcast {
	m.mu.Lock()
	defer m.mu.Unlock()

	return proto.Clone(m.state).(*pb.StateBroadcast)
}

// IsNew returns if the state is fresh
func (m *StateManager) IsNew() bool {
	return m.state.Snapshot == nil
}

func (m *StateManager) UpdateHeads(ctx context.Context, id peer.ID, heads []cid.Cid, updateTTL bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	member, ok := m.state.Members[id.String()]
	if !ok {
		member = &pb.Participant{}
		m.state.Members[id.String()] = member
	}

	member.DagHeads = make([]*pb.Head, 0, len(heads))
	for _, h := range heads {
		member.DagHeads = append(member.DagHeads, &pb.Head{Cid: h.Bytes()})
	}

	if updateTTL {
		member.BestBefore = uint64(m.clock.Now().Add(m.ttl).Unix())
	}

	if m.onMembershipUpdate != nil {
		go m.onMembershipUpdate(m.state.Members)
	}

	return m.Save(ctx)
}

func (m *StateManager) MergeMembers(ctx context.Context, broadcast *pb.StateBroadcast) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range broadcast.Members {
		// if our state is missing this member or the update is newer than the one we have
		// take theirs
		if ov, ok := m.state.Members[k]; !ok || v.BestBefore > ov.BestBefore {
			m.state.Members[k] = v
		}
	}

	if m.onMembershipUpdate != nil {
		go m.onMembershipUpdate(m.state.Members)
	}

	return m.Save(ctx)
}

func (m *StateManager) SetSnapshot(ctx context.Context, root cid.Cid, dagHead cid.Cid, dagHeight uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.Snapshot = &pb.Snapshot{
		SnapshotKey: &pb.Head{Cid: root.Bytes()},
		DagHead:     &pb.Head{Cid: dagHead.Bytes()},
		Height:      dagHeight,
	}
	return m.Save(ctx)
}

// SetMembershipUpdateCallback registers a callback to be invoked whenever
// membership information is updated.
func (m *StateManager) SetMembershipUpdateCallback(callback func(members map[string]*pb.Participant)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onMembershipUpdate = callback
}
