package crdt

import (
	"context"

	"github.com/ipfs/boxo/datastore/dshelp"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-ds-crdt/pb"
	"github.com/pkg/errors"
)

func (store *Datastore) CompactDAG(ctx context.Context) error {
	store.startNewHead.Store(true)
	store.logger.Infof("Starting DAG compaction")

	// List current heads of the DAG
	heads, _, err := store.heads.List()
	if err != nil {
		return errors.Wrap(err, "error listing DAG heads")
	}

	// Initialize maps for processing
	processed := make(map[cid.Cid]struct{})
	addMap := make(map[string]*pb.Element)    // Tracks add operations
	removeMap := make(map[string]*pb.Element) // Tracks removed operations

	// Process each head in the DAG
	for _, head := range heads {
		if !store.heads.IsCompact(head) {
			err := store.removeHead(ctx, head)
			if err != nil {
				return errors.Wrapf(err, "error removing old head %s", head)
			}
			err = store.compactNode(ctx, head, processed, addMap, removeMap)
			if err != nil {
				return errors.Wrapf(err, "error compacting node %s", head)
			}
		}
	}

	// Create the final compacted delta
	finalAdd := make([]*pb.Element, 0, len(addMap))
	for key, value := range addMap {
		finalAdd = append(finalAdd, &pb.Element{
			Key:   key,
			Value: value.Value,
		})
	}

	finalRm := make([]*pb.Element, 0, len(removeMap))
	for key, value := range removeMap {
		finalRm = append(finalRm, &pb.Element{
			Key:   key,
			Value: value.Value,
		})
	}

	compactedDelta := &pb.Delta{
		Elements:   finalAdd,
		Tombstones: finalRm,
	}

	// Store the compacted node
	compactedNode, err := store.putBlock(nil, 0, compactedDelta)
	if err != nil {
		return errors.Wrap(err, "error storing compacted node")
	}
	for key, _ := range addMap {
		//store the snapshots for each key
		err = store.store.Put(ctx, store.set.namespace.ChildString(setNs).ChildString(key).ChildString(dshelp.MultihashToDsKey(compactedNode.Cid().Hash()).String()), nil)
	}

	// mark it as process
	err = store.markProcessed(ctx, compactedNode.Cid())
	if err != nil {
		return err
	}

	err = store.heads.Add(ctx, compactedNode.Cid(), head{height: 1, flags: FlagCompacted})
	if err != nil {
		return errors.Wrap(err, "error adding new compacted head")
	}

	store.logger.Infof("DAG compaction completed successfully")
	return nil
}

// Remove a head from the datastore and cache
func (store *Datastore) removeHead(ctx context.Context, head cid.Cid) error {
	store.logger.Debugf("Removing head: %s", head)

	// Remove from the datastore
	err := store.heads.store.Delete(ctx, store.heads.key(head))
	if err != nil && err != ds.ErrNotFound {
		return errors.Wrapf(err, "error deleting head %s from datastore", head)
	}

	// Remove from the cache
	store.heads.cacheMux.Lock()
	delete(store.heads.cache, head)
	store.heads.cacheMux.Unlock()

	return nil
}

// Compact a single node and update the addMap and tombstoneSet
func (store *Datastore) compactNode(
	ctx context.Context,
	cid cid.Cid,
	processed map[cid.Cid]struct{},
	addMap map[string]*pb.Element,
	removeMap map[string]*pb.Element,
) error {
	if _, ok := processed[cid]; ok {
		return nil // Node already processed
	}
	processed[cid] = struct{}{}

	// Retrieve the node
	nd, err := store.dagService.Get(ctx, cid)
	if err != nil {
		return errors.Wrapf(err, "error retrieving node %s", cid)
	}

	// remove this node as we've processed it
	err = store.dagService.Remove(ctx, cid)
	if err != nil {
		return errors.Wrapf(err, "error removing node %s", cid)
	}
	err = store.store.Delete(ctx, store.processedBlockKey(cid))
	if err != nil {
		return errors.Wrapf(err, "error removing node %s", cid)
	}

	delta, err := extractDelta(nd)
	if err != nil {
		return errors.Wrapf(err, "error extracting delta from node %s", cid)
	}

	// Apply Add operations
	for _, add := range delta.Elements {
		// delete this element we've processed it
		err = store.store.Delete(ctx, store.set.namespace.ChildString(setNs).ChildString(add.Key).ChildString(dshelp.MultihashToDsKey(cid.Hash()).String()))
		if err != nil {
			return err
		}

		// add if its not removed
		if _, ok := removeMap[add.Key]; ok {
			delete(addMap, add.Key)
			delete(removeMap, add.Key)
			continue
		}
		addMap[add.Key] = add
	}

	// Apply Rmv operations
	for _, rmv := range delta.Tombstones {
		err = store.store.Delete(ctx, store.set.namespace.ChildString(tombsNs).ChildString(rmv.Key).ChildString(rmv.Id))
		if err != nil {
			return err
		}
		err = store.store.Delete(ctx, store.set.namespace.ChildString(setNs).ChildString(rmv.Key).ChildString(rmv.Id))
		if err != nil {
			return err
		}
		removeMap[rmv.Key] = rmv
	}

	// Traverse and process child nodes
	for _, link := range nd.Links() {
		err := store.compactNode(ctx, link.Cid, processed, addMap, removeMap)
		if err != nil {
			return errors.Wrapf(err, "error compacting child node %s", link.Cid)
		}
	}

	return nil
}
