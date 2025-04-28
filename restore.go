package crdt

import (
	"context"
	"fmt"

	dshelp "github.com/ipfs/boxo/datastore/dshelp"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-ds-crdt/pb"
)

func (store *Datastore) tryFastForwardToSnapshot(
	ctx context.Context,
	mySnapshot *pb.Snapshot,
	participant *pb.Participant,
) error {
	target := participant.Snapshot
	var mySnapshotCID cid.Cid
	if mySnapshot != nil {
		mySnapshotCID = cidFromBytes(mySnapshot.SnapshotKey.Cid)
	}

	targetSnapshotCID := cidFromBytes(target.SnapshotKey.Cid)

	// Ignore older snapshots.
	if mySnapshot != nil && target.Height < mySnapshot.Height {
		store.logger.Infof("ignoring older snapshot: target %d < ours %d",
			target.Height, mySnapshot.Height)
		store.seenSnapshots.Add(targetSnapshotCID)
		return nil
	}

	store.logger.Debugf("fast-forward: ours=%s target=%s",
		mySnapshotCID, targetSnapshotCID)

	// 1. Walk back the snapshot chain until common ancestor.
	snapshotPath, err := store.walkBackSnapshots(ctx, targetSnapshotCID, mySnapshotCID)
	if err != nil {
		store.seenSnapshots.Add(targetSnapshotCID)
		return fmt.Errorf("walking snapshot chain: %w", err)
	}

	// 2. Build the DAG replay path from our current heads → ancestor.
	heads, _, err := store.heads.List(ctx)
	if err != nil {
		store.seenSnapshots.Add(targetSnapshotCID)
		return fmt.Errorf("listing heads: %w", err)
	}
	dagPath, err := store.walkReplayPath(ctx, heads, mySnapshotCID)
	if err != nil {
		store.seenSnapshots.Add(targetSnapshotCID)
		return fmt.Errorf("walking DAG: %w", err)
	}

	// 3. Load HAMT starting at the *oldest* common snapshot.
	hamtDS, err := NewHAMTDatastore(ctx, store.dagService, snapshotPath[len(snapshotPath)-1].HamtRootCID)
	if err != nil {
		store.seenSnapshots.Add(targetSnapshotCID)
		return fmt.Errorf("loading HAMT: %w", err)
	}

	//---------------------------------------------------------------------
	// Fast-forward through snapshots newest→oldest (reverse order).
	//---------------------------------------------------------------------

	for i := len(snapshotPath) - 1; i >= 0; i-- {
		snapInfo := snapshotPath[i]
		store.seenSnapshots.Add(snapInfo.WrapperCID)
		if len(dagPath) > 0 {
			// Replay our local DAG changes onto the HAMT.
			newRootCID, processedCIDs, err := store.replayDAGPath(ctx, dagPath, snapInfo.DeltaHeadCID, hamtDS)
			if err != nil {
				return fmt.Errorf("replaying DAG: %w", err)
			}

			if !newRootCID.Equals(snapInfo.HamtRootCID) {
				return fmt.Errorf("divergence after snapshot %s", snapInfo.WrapperCID)
			}

			// Pop processed CIDs from dagPath
			dagPath = dagPath[len(processedCIDs):]

			if len(processedCIDs) > 0 {
				err = store.dagService.RemoveMany(ctx, processedCIDs)
				if err != nil {
					return err
				}
			}
		}
	}

	// ---------------------------------------------------------------------
	// 4.   Adopt the snapshot we just validated
	// ---------------------------------------------------------------------

	lastSnapinfo := snapshotPath[0]
	err = store.restoreSnapshot(ctx, lastSnapinfo)
	if err != nil {
		return fmt.Errorf("restoring snapshot: %w", err)
	}

	store.logger.Debugf("marking snapshot dag head as processed %s (priority: %d)", lastSnapinfo.DeltaHeadCID, lastSnapinfo.Height)
	if err := store.markProcessed(ctx, lastSnapinfo.DeltaHeadCID); err != nil {
		store.logger.Warnf("failed to mark snapshot delta head as processed: %v", err)
	}

	// ---------------------------------------------------------------------
	// 5.   Continue with normal operation: ingest the peer’s heads
	// ---------------------------------------------------------------------

	for _, h := range participant.DagHeads {
		c, _ := cid.Cast(h.Cid)
		if c == cid.Undef {
			continue
		}

		store.logger.Debugf("processing dag head %s", c)

		err := store.handleBlock(ctx, c) // handleBlock blocks
		if err != nil {
			store.logger.Errorf("error handling block %s: %s", h.Cid, err)
		}

	}

	store.logger.Debugf("setting snapshot %s", lastSnapinfo.WrapperCID)
	// Persist snapshot pointer in our StateManager.
	if err := store.state.SetSnapshot(ctx, store.h.ID(), lastSnapinfo); err != nil {
		return fmt.Errorf("setting snapshot: %w", err)
	}

	return nil
}

// walkBackSnapshots walks backward through the snapshot chain starting from `from`
// until reaching `until`, or until no previous snapshot is found.
// It loads and returns a list of *SnapshotInfo objects representing each snapshot
// in the path, starting from `from` and walking toward ancestors.
// If `until` is found, it is included in the result; if not, the walk stops at the oldest snapshot.
// The returned list is ordered from newest to oldest (latest snapshot first).
func (store *Datastore) walkBackSnapshots(ctx context.Context, from cid.Cid, until cid.Cid) ([]*SnapshotInfo, error) {
	var path []*SnapshotInfo
	current := from

	for current.Defined() {
		store.logger.Debugf("Loading snapshot wrapper CID = %s", current)
		snapInfo, err := store.loadSnapshotInfo(ctx, current)
		if err != nil {
			return nil, fmt.Errorf("loading snapshot info: %w", err)
		}

		path = append(path, snapInfo)
		if current.Equals(until) {
			break
		}
		if snapInfo.PrevCID == cid.Undef {
			break
		}
		current = snapInfo.PrevCID
	}

	return path, nil
}

// walkReplayPath walks the DAG backwards starting from the given heads
// and collects the deltas needed to replay up to the target snapshotDagHead.
// Returns a list of CIDs to replay in topological order (parents before children).
func (store *Datastore) walkReplayPath(ctx context.Context, heads []cid.Cid, snapshotDagHead cid.Cid) ([]cid.Cid, error) {
	visited := cid.NewSet()
	var replayOrder []cid.Cid

	queue := make([]cid.Cid, len(heads))
	copy(queue, heads)

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if !current.Defined() {
			continue
		}

		if !visited.Visit(current) {
			continue
		}

		replayOrder = append(replayOrder, current)

		if current.Equals(snapshotDagHead) {
			// We reached the known snapshot DAG Head
			continue
		}

		node, err := store.dagService.Get(ctx, current)
		if err != nil {
			return nil, fmt.Errorf("failed to load DAG node %s: %w", current, err)
		}

		// Walk all links backward
		for _, link := range node.Links() {
			if link.Cid.Defined() {
				queue = append(queue, link.Cid)
			}
		}
	}

	return replayOrder, nil
}

func (store *Datastore) replayDAGPath(ctx context.Context, path []cid.Cid, stopAt cid.Cid, hamtDS *HAMTDatastore) (cid.Cid, []cid.Cid, error) {
	var processed []cid.Cid

	set, err := newCRDTSet(ctx, hamtDS, ds.NewKey(""), store.dagService, store.logger, nil, nil)
	if err != nil {
		return cid.Undef, nil, err
	}

	for _, nodeCID := range path {
		if nodeCID.Equals(stopAt) {
			break
		}

		nodeGetter := &crdtNodeGetter{NodeGetter: store.dagService}
		_, delta, err := nodeGetter.GetDelta(ctx, nodeCID)
		if err != nil {
			return cid.Undef, nil, err
		}

		id := dshelp.MultihashToDsKey(nodeCID.Hash()).String()
		if err := set.Merge(ctx, delta, id); err != nil {
			return cid.Undef, nil, err
		}
		processed = append(processed, nodeCID)
	}

	root, err := hamtDS.GetRoot(ctx)
	return root, processed, err
}

func (store *Datastore) restoreSnapshot(ctx context.Context, snapshotInfo *SnapshotInfo) error {
	hamtDS, err := NewHAMTDatastore(ctx, store.dagService, snapshotInfo.HamtRootCID)
	if err != nil {
		return err
	}

	// Create a temporary Set backed by HAMT
	snapshotSet, err := newCRDTSet(ctx, hamtDS, ds.NewKey(""), store.dagService, store.logger, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to create snapshot set: %w", err)
	}

	store.logger.Debugf("restoring from snapshot %s", snapshotInfo.WrapperCID)
	// Clone snapshot into the live Set
	if err := store.set.CloneFrom(ctx, snapshotSet); err != nil {
		return fmt.Errorf("failed to clone snapshot into live set: %w", err)
	}

	return nil
}
