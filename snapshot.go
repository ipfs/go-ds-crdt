package crdt

import (
	"context"
	"fmt"
	"strings"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/datastore/dshelp"
	"github.com/ipfs/boxo/exchange/offline"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-ds-crdt/pb"
	ipld "github.com/ipfs/go-ipld-format"
	"google.golang.org/protobuf/proto"
)

// SnapshotInfo holds metadata about a snapshot
type SnapshotInfo struct {
	WrapperCID   cid.Cid // CID of the wrapper node
	HamtRootCID  cid.Cid // CID of the HAMT root inside that wrapper
	Height       uint64  // The height/sequence number of this snapshot
	PrevCID      cid.Cid // Link to the previous snapshot (wrapper CID)
	DeltaHeadCID cid.Cid // The head of the delta DAG at the time of snapshot
}

// buildSnapshot creates a snapshot of the current state by replaying the DAG into a HAMT-backed set.
// If prevSnapshotCID is defined, it will start from that snapshot and apply only new deltas.
func (store *Datastore) buildSnapshot(
	ctx context.Context,
	headCID cid.Cid,
	prevSnapshotCID cid.Cid,
) (*SnapshotInfo, error) {
	var hamtDS *HAMTDatastore
	var err error
	var prevInfo = &SnapshotInfo{}

	// 1) Initialize the HAMTDatastore, either from the previous snapshot or empty
	if prevSnapshotCID.Defined() {
		// Load the previous snapshot info
		prevInfo, err = store.loadSnapshotInfo(ctx, prevSnapshotCID)
		if err != nil {
			return nil, fmt.Errorf("failed to load previous snapshot info: %w", err)
		}
	}

	// Create a HAMT datastore starting from the previous snapshot's root
	hamtDS, err = NewHAMTDatastore(ctx, store.dagService, prevInfo.HamtRootCID)
	if err != nil {
		return nil, fmt.Errorf("failed to create HAMT datastore from previous snapshot: %w", err)
	}

	// collect only the new deltas
	deltas, blockIDs, err := store.collectDeltasFromDAG(ctx, headCID, prevInfo.DeltaHeadCID)
	if err != nil {
		return nil, fmt.Errorf("failed to collect deltas since previous snapshot: %w", err)
	}

	// Create a set using the HAMT datastore
	snapshotSet, err := newCRDTSet(
		ctx,
		hamtDS,
		ds.NewKey(""),
		store.dagService,
		store.logger,
		nil, // No hooks needed for the snapshot
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot set: %w", err)
	}

	var maxHeight uint64
	// Apply only the new deltas to the snapshot set
	for i, delta := range deltas {
		if maxHeight < delta.Priority {
			maxHeight = delta.Priority
		}
		if err := snapshotSet.Merge(ctx, delta, blockIDs[i]); err != nil {
			return nil, fmt.Errorf("failed to merge delta into snapshot: %w", err)
		}
	}

	// 2) Get the new HAMT root CID
	hamtRootCID, err := hamtDS.GetRoot(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get HAMT root CID: %w", err)
	}

	// 3) Build and persist the wrapper node
	wrapper, err := store.createSnapshotWrapper(ctx, hamtRootCID, prevSnapshotCID, maxHeight, headCID)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot wrapper: %w", err)
	}

	store.logger.Debugf("Snapshot created: wrapper CID = %s, hamtRoot CID = %s, deltaHead = %s",
		wrapper.Cid(), hamtRootCID, headCID)

	cctx, cancel := context.WithTimeout(ctx, store.opts.DAGSyncerTimeout)
	defer cancel()
	if err := store.dagService.Add(cctx, wrapper); err != nil {
		return nil, fmt.Errorf("failed to add snapshot wrapper to DAG: %w", err)
	}

	// 4) Return the SnapshotInfo
	return &SnapshotInfo{
		WrapperCID:   wrapper.Cid(),
		HamtRootCID:  hamtRootCID,
		Height:       maxHeight,
		PrevCID:      prevSnapshotCID,
		DeltaHeadCID: headCID,
	}, nil
}

func (store *Datastore) createSnapshotWrapper(
	ctx context.Context,
	hamtRootCID, prevSnapshotCID cid.Cid,
	height uint64,
	deltaHeadCID cid.Cid,
) (*dag.ProtoNode, error) {
	// 1) pack metadata
	meta := &pb.SnapshotInfo{
		Height:       height,
		DeltaHeadCid: &pb.Head{Cid: deltaHeadCID.Bytes()},
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot meta: %w", err)
	}

	// 2) create node with that data
	w := dag.NodeWithData(data)
	if err := w.SetCidBuilder(dag.V1CidPrefix()); err != nil {
		return nil, err
	}

	// 3) attach links
	if err := w.AddRawLink("root", &ipld.Link{Cid: hamtRootCID}); err != nil {
		return nil, err
	}
	if prevSnapshotCID.Defined() {
		if err := w.AddRawLink("prev", &ipld.Link{Cid: prevSnapshotCID}); err != nil {
			return nil, err
		}
	}
	return w, nil
}

// loadSnapshotInfo reads a wrapper node and extracts the metadata and links.
func (store *Datastore) loadSnapshotInfo(
	ctx context.Context,
	wrapperCID cid.Cid,
) (*SnapshotInfo, error) {
	// 1) Fetch the wrapper node
	nd, err := store.dagService.Get(ctx, wrapperCID)
	if err != nil {
		return nil, fmt.Errorf("failed to get wrapper node %s: %w", wrapperCID, err)
	}
	pnd, ok := nd.(*dag.ProtoNode)
	if !ok {
		return nil, fmt.Errorf("expected ProtoNode, got %T", nd)
	}

	// 2) Unmarshal the SnapshotInfo proto from the node's Data
	var meta pb.SnapshotInfo
	if err := proto.Unmarshal(pnd.Data(), &meta); err != nil {
		return nil, fmt.Errorf("unmarshal snapshot metadata: %w", err)
	}

	// 3) Read the "root" link (the HAMT root CID)
	rootLink, err := pnd.GetNodeLink("root")
	if err != nil {
		return nil, fmt.Errorf("missing root link: %w", err)
	}
	hamtRootCID := rootLink.Cid

	// 4) Read the optional "prev" link (previous wrapper CID)
	var prevCID cid.Cid
	if pl, err := pnd.GetNodeLink("prev"); err == nil {
		prevCID = pl.Cid
	}

	// 5) Extract the delta‐head CID from the proto
	var deltaHeadCID cid.Cid
	if meta.DeltaHeadCid != nil && len(meta.DeltaHeadCid.Cid) > 0 {
		deltaHeadCID, err = cid.Cast(meta.DeltaHeadCid.Cid)
		if err != nil {
			return nil, fmt.Errorf("parse delta‐head CID: %w", err)
		}
	}

	return &SnapshotInfo{
		WrapperCID:   wrapperCID,
		HamtRootCID:  hamtRootCID,
		Height:       meta.GetHeight(),
		PrevCID:      prevCID,
		DeltaHeadCID: deltaHeadCID,
	}, nil
}

// collectDeltasFromDAG walks the DAG and collects all deltas with their block IDs.
func (store *Datastore) collectDeltasFromDAG(
	ctx context.Context,
	startCID, endCID cid.Cid, // pass cid.Undef for no end bound
) ([]*pb.Delta, []string, error) {
	var (
		deltas   []*pb.Delta
		blockIDs []string
		visited  = make(map[string]struct{})
		queue    = []cid.Cid{startCID}
	)
	offlineDAG := dag.NewDAGService(blockservice.New(store.bs, offline.Exchange(store.bs)))
	ng := &crdtNodeGetter{NodeGetter: offlineDAG}

	// pre-mark endCID and its ancestors
	if endCID.Defined() {
		visited[endCID.String()] = struct{}{}
	}

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		key := cur.String()
		if _, seen := visited[key]; seen {
			continue
		}
		visited[key] = struct{}{}

		// use the same extractor everywhere
		node, delta, err := ng.GetDelta(ctx, cur)
		if err != nil {
			return nil, nil, fmt.Errorf("GetDelta at %s: %w", cur, err)
		}
		if delta != nil {
			deltas = append(deltas, delta)
			blockIDs = append(blockIDs, dshelp.MultihashToDsKey(cur.Hash()).String())
		}
		for _, link := range node.Links() {
			queue = append(queue, link.Cid)
		}
	}

	// reverse to chronological
	for i, j := 0, len(deltas)-1; i < j; i, j = i+1, j-1 {
		deltas[i], deltas[j] = deltas[j], deltas[i]
		blockIDs[i], blockIDs[j] = blockIDs[j], blockIDs[i]
	}
	return deltas, blockIDs, nil
}

// compactAndSnapshot compacts the DAG by creating a snapshot and truncating old history.
func (store *Datastore) compactAndSnapshot(
	ctx context.Context,
	headCID, truncateUpTo cid.Cid,
) (*SnapshotInfo, error) {
	latest, err := store.getLatestSnapshotCID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest snapshot CID: %w", err)
	}

	si, err := store.buildSnapshot(ctx, headCID, latest)
	if err != nil {
		return nil, fmt.Errorf("failed to build snapshot: %w", err)
	}

	if err := store.updateLatestSnapshotCID(ctx, si.WrapperCID); err != nil {
		return nil, fmt.Errorf("failed to update latest snapshot CID: %w", err)
	}

	if err := store.truncateDAG(ctx, headCID, truncateUpTo); err != nil {
		return nil, fmt.Errorf("failed to truncate DAG: %w", err)
	}

	return si, nil
}

// Helper function to check for offline fetch errors
func isOfflineBlockNotFoundError(err error) bool {
	return strings.Contains(err.Error(), "block was not found locally (offline)")
}

// Truncate the DAG from startCID to endCID using an offline DAG service
func (store *Datastore) truncateDAG(ctx context.Context, startCID, endCID cid.Cid) error {
	// Initialize a DAG service with an offline exchange
	offlineDAG := dag.NewDAGService(blockservice.New(store.bs, offline.Exchange(store.bs)))

	queue := []cid.Cid{startCID}
	visited := make(map[cid.Cid]struct{})

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		// Stop if we've reached the endCID (for partial truncation)
		if endCID.Defined() && current == endCID {
			continue
		}

		if _, ok := visited[current]; ok {
			continue
		}
		visited[current] = struct{}{}

		// Fetch the IPLD node locally using the offline DAG service
		node, err := offlineDAG.Get(ctx, current)
		if err != nil {
			if isOfflineBlockNotFoundError(err) {
				// Block is already gone, continue cleanup
				continue
			}
			return fmt.Errorf("failed to retrieve DAG node %s: %w", current, err)
		}

		// Step 1: Queue child nodes for further deletion
		for _, link := range node.Links() {
			queue = append(queue, link.Cid)
		}

		// Step 2: Clean up from the DAG service
		err = store.dagService.Remove(ctx, current)
		if err != nil {
			return fmt.Errorf("failed to remove DAG node %s: %w", current, err)
		}
	}

	return nil
}

// getLatestSnapshotCID returns the CID of the latest snapshot wrapper.
func (store *Datastore) getLatestSnapshotCID(ctx context.Context) (cid.Cid, error) {
	key := store.namespace.ChildString(snapshotKey)
	b, err := store.store.Get(ctx, key)
	if err != nil {
		if err == ds.ErrNotFound {
			return cid.Undef, nil
		}
		return cid.Undef, err
	}
	return cid.Cast(b)
}

// updateLatestSnapshotCID updates the stored reference to the latest snapshot.
func (store *Datastore) updateLatestSnapshotCID(ctx context.Context, c cid.Cid) error {
	key := store.namespace.ChildString(snapshotKey)
	return store.store.Put(ctx, key, c.Bytes())
}
