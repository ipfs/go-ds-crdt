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
	logging "github.com/ipfs/go-log/v2"
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
	logger             logging.StandardLogger
}

// NewStateManager initializes a StateManager and loads the state from the datastore.
func NewStateManager(ctx context.Context, datastore ds.Datastore, key ds.Key, ttl time.Duration, logger logging.StandardLogger) (*StateManager, error) {
	manager := &StateManager{
		datastore: datastore,
		key:       key,
		state:     &pb.StateBroadcast{},
		clock:     clock.New(),
		ttl:       ttl,
		logger:    logger,
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

// seq32Newer reports whether seqA is *later* than seqB
// in a 32‑bit modulo‑2³² sequence space (RFC 1982 / TCP rule).
func seq32Newer(seqA, seqB uint32) bool {
	diff := seqA - seqB // unsigned subtraction, wraps mod 2³²
	return diff != 0 && diff < 0x80000000
}

func (m *StateManager) UpdateHeads(ctx context.Context, id peer.ID, heads []cid.Cid, updateTTL bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	idStr := id.String()
	m.logger.Debugf("=== HEAD UPDATE for %s ===", idStr)
	m.logger.Debugf("Updating %d heads, updateTTL=%t", len(heads), updateTTL)

	member, ok := m.state.Members[idStr]
	if !ok {
		m.logger.Debugf("Creating new member %s", idStr)
		member = &pb.Participant{
			BestBefore: uint64(m.clock.Now().Add(m.ttl).Unix()),
			StateSeq:   1,
		}
		m.state.Members[idStr] = member
	} else {
		m.logger.Debugf("Updating existing member %s (had %d heads)", idStr, len(member.DagHeads))
		member.StateSeq++
	}

	member.DagHeads = make([]*pb.Head, 0, len(heads))
	for i, h := range heads {
		member.DagHeads = append(member.DagHeads, &pb.Head{Cid: h.Bytes()})
		m.logger.Debugf("  Head %d: %s", i, h.String())
	}

	if updateTTL {
		oldTTL := member.BestBefore
		member.BestBefore = uint64(m.clock.Now().Add(m.ttl).Unix())
		m.logger.Debugf("Updated TTL: %d -> %d", oldTTL, member.BestBefore)
	}

	m.logger.Debugf("Member %s now has %d heads, bestBefore=%d", idStr, len(member.DagHeads), member.BestBefore)

	if m.onMembershipUpdate != nil {
		go func() {
			m.onMembershipUpdate(m.GetState().Members)
		}()
	}

	return m.Save(ctx)
}

func (m *StateManager) MergeMembers(ctx context.Context, broadcast *pb.StateBroadcast) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.logger.Debugf("Before merge: %d local members", len(m.state.Members))
	for id, member := range m.state.Members {
		headCount := len(member.DagHeads)
		m.logger.Debugf("  Local member %s: %d heads, bestBefore=%d, seq=%d", id, headCount, member.BestBefore, member.StateSeq)
	}

	m.logger.Debugf("Incoming broadcast: %d members", len(broadcast.Members))
	for id, member := range broadcast.Members {
		headCount := len(member.DagHeads)
		m.logger.Debugf("  Broadcast member %s: %d heads, bestBefore=%d, seq=%d", id, headCount, member.BestBefore, member.StateSeq)
	}

	changesTracker := make(map[string]string)

	for k, v := range broadcast.Members {
		// if our state is missing this member or the incoming sequence is newer
		if ov, ok := m.state.Members[k]; !ok {
			m.state.Members[k] = v
			changesTracker[k] = "ADDED"
			m.logger.Debugf("ADDED member %s (bestBefore=%d, seq=%d)", k, v.BestBefore, v.StateSeq)
		} else if seq32Newer(v.StateSeq, ov.StateSeq) {
			m.state.Members[k] = v
			changesTracker[k] = "UPDATED"
			m.logger.Debugf("UPDATED member %s: seq %d -> %d", k, ov.StateSeq, v.StateSeq)
		} else {
			changesTracker[k] = "IGNORED"
			m.logger.Debugf("IGNORED member %s: incoming seq %d <= existing seq %d", k, v.StateSeq, ov.StateSeq)
		}
	}

	// throw away members that have outlived their ttl
	now := uint64(m.clock.Now().Unix())
	expiredCount := 0
	for k, v := range m.state.Members {
		if v.BestBefore < now {
			delete(m.state.Members, k)
			changesTracker[k] = "EXPIRED"
			expiredCount++
			m.logger.Debugf("EXPIRED member %s (bestBefore=%d < now=%d)", k, v.BestBefore, now)
		}
	}

	m.logger.Debugf("After merge: %d members (%d expired)", len(m.state.Members), expiredCount)
	for id, member := range m.state.Members {
		headCount := len(member.DagHeads)
		action := changesTracker[id]
		if action == "" {
			action = "UNCHANGED"
		}
		m.logger.Debugf("  Final member %s: %d heads, bestBefore=%d, seq=%d [%s]", id, headCount, member.BestBefore, member.StateSeq, action)
	}

	if m.onMembershipUpdate != nil {
		go func() {
			m.onMembershipUpdate(m.GetState().Members)
		}()
	}

	return m.Save(ctx)
}

// MarkLeaving sets a short TTL for the specified peer to signal imminent departure
func (m *StateManager) MarkLeaving(ctx context.Context, id peer.ID, grace time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	idStr := id.String()
	member, ok := m.state.Members[idStr]
	if !ok {
		// Create new participant if doesn't exist
		member = &pb.Participant{
			Metadata: make(map[string]string),
			StateSeq: 1,
		}
		m.state.Members[idStr] = member
	} else {
		// Increment sequence number to ensure this update propagates
		member.StateSeq++
	}

	// Set the short TTL
	member.BestBefore = uint64(m.clock.Now().Add(grace).Unix())

	m.logger.Infof("marked peer %s as leaving with grace period %v (expires at %d, seq=%d)",
		idStr, grace, member.BestBefore, member.StateSeq)

	// Trigger membership update callback if set
	if m.onMembershipUpdate != nil {
		go func() {
			m.onMembershipUpdate(m.GetState().Members)
		}()
	}

	return m.Save(ctx)
}

func (m *StateManager) SetSnapshot(ctx context.Context, selfID peer.ID, info *SnapshotInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	member, ok := m.state.Members[selfID.String()]
	if !ok {
		member = &pb.Participant{
			BestBefore: uint64(m.clock.Now().Add(m.ttl).Unix()),
			StateSeq:   1,
		}
		m.state.Members[selfID.String()] = member
	} else {
		// Increment sequence number for snapshot updates
		member.StateSeq++
	}

	member.Snapshot = &pb.Snapshot{
		SnapshotKey: &pb.Head{Cid: info.WrapperCID.Bytes()},
		DagHead:     &pb.Head{Cid: info.DeltaHeadCID.Bytes()},
		Height:      info.Height,
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

func (m *StateManager) SetMeta(ctx context.Context, id peer.ID, metaData map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var changesMade bool

	member, ok := m.state.Members[id.String()]
	if !ok {
		changesMade = true
		member = &pb.Participant{
			Metadata: metaData,
			StateSeq: 1,
		}
		m.state.Members[id.String()] = member
	} else {
		for k, v := range metaData {
			if current, exists := member.Metadata[k]; !exists || v != current {
				changesMade = true
				if member.Metadata == nil {
					member.Metadata = make(map[string]string)
				}
				member.Metadata[k] = v
			}
		}
	}

	if !changesMade {
		return nil
	}
	member.StateSeq++
	member.BestBefore = uint64(m.clock.Now().Add(m.ttl).Unix())

	return m.Save(ctx)
}
