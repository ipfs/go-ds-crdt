package crdt

import (
	"context"
	"errors"
	"fmt"

	"github.com/ipfs/boxo/ipld/merkledag/traverse"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	ipld "github.com/ipfs/go-ipld-format"
)

// PurgeDAG removes all state associated with a named DAG: its heads, all DAG
// blocks reachable from those heads (including pending blocks and inflight
// tips left by an unfinished walk), the set entries created by those blocks,
// and processed/pending block markers. Returns the number of DAG nodes
// removed.
//
// Heads are deleted last so that a partial failure leaves them intact and a
// subsequent call can resume the cleanup.
//
// The caller must ensure no concurrent writes to this dagName during purge.
// The Datastore's background workers (rebroadcast, repair, DAG walking) also
// access heads and set state — callers should either Close() the Datastore
// first or ensure the dagName is not being actively synced.
func (mcrdt *MerkleCRDT) PurgeDAG(ctx context.Context, dagName string) (int, error) {
	currentHeads, _, err := mcrdt.heads.ListDAG(ctx, dagName)
	if err != nil {
		return 0, err
	}
	inflightTips, err := mcrdt.listInflightHeadsForDAG(ctx, dagName)
	if err != nil {
		return 0, err
	}
	if len(currentHeads) == 0 && len(inflightTips) == 0 {
		return 0, nil
	}

	rootCIDs := make([]cid.Cid, 0, len(currentHeads)+len(inflightTips))
	for _, h := range currentHeads {
		rootCIDs = append(rootCIDs, h.Cid)
	}
	for _, h := range inflightTips {
		rootCIDs = append(rootCIDs, h.Cid)
	}

	processedCIDs := make(map[cid.Cid]struct{})
	pendingCIDs := make(map[cid.Cid]struct{})
	visited := make(map[cid.Cid]struct{})

	// purgeKeyKind tracks which namespaces a key appeared in across the DAG's
	// deltas, so purgeKeyBlocks can skip querying namespaces that the DAG never
	// wrote to for a given key.
	type purgeKeyKind uint8
	const (
		purgeKeyElem purgeKeyKind = 1 << iota
		purgeKeyTomb
	)
	setKeys := make(map[string]purgeKeyKind)

	// Walk the DAG with a local-only DFS: check isProcessed and pendingTag
	// before fetching each block so we never trigger network requests. Blocks
	// that are neither processed nor pending have no state to clean up, so
	// skipping them is correct. Pending blocks were merged into the set by an
	// unfinished walk and must be collected to clean their set state, pending
	// markers, and DAG blocks.
	stack := make([]cid.Cid, len(rootCIDs))
	copy(stack, rootCIDs)
	for len(stack) > 0 {
		c := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if _, seen := visited[c]; seen {
			continue
		}
		processed, err := mcrdt.isProcessed(ctx, c)
		if err != nil {
			return 0, err
		}
		if processed {
			processedCIDs[c] = struct{}{}
		} else {
			_, pending, err := mcrdt.pendingTag(ctx, c)
			if err != nil {
				return 0, err
			}
			if !pending {
				continue
			}
			pendingCIDs[c] = struct{}{}
		}
		visited[c] = struct{}{}

		nd, err := mcrdt.dagService.Get(ctx, c)
		if err != nil {
			return 0, err
		}

		deltaBytes, err := extractDelta(nd)
		if err != nil {
			return 0, err
		}
		delta := mcrdt.newDelta()
		if err := delta.Unmarshal(deltaBytes); err != nil {
			return 0, err
		}

		elems, err := delta.GetElements()
		if err != nil {
			return 0, err
		}
		for _, e := range elems {
			setKeys[e.GetKey()] |= purgeKeyElem
		}

		tombs, err := delta.GetTombstones()
		if err != nil {
			return 0, err
		}
		for _, t := range tombs {
			setKeys[t.GetKey()] |= purgeKeyTomb
		}

		for _, link := range nd.Links() {
			stack = append(stack, link.Cid)
		}
	}

	for key, kind := range setKeys {
		if err := mcrdt.set.purgeKeyBlocks(ctx, key, visited, kind&purgeKeyElem != 0, kind&purgeKeyTomb != 0); err != nil {
			return 0, err
		}
	}

	dagCIDs := make([]cid.Cid, 0, len(visited))
	for c := range visited {
		dagCIDs = append(dagCIDs, c)
	}

	var store ds.Write = mcrdt.store
	batchingDs, batching := store.(ds.Batching)
	if batching {
		store, err = batchingDs.Batch(ctx)
		if err != nil {
			return 0, err
		}
	}
	for c := range processedCIDs {
		if err := store.Delete(ctx, mcrdt.processedBlockKey(c)); err != nil {
			return 0, err
		}
	}
	for c := range pendingCIDs {
		prefix := mcrdt.pendingCIDBaseKey(c).String()
		results, qerr := mcrdt.store.Query(ctx, query.Query{Prefix: prefix, KeysOnly: true})
		if qerr != nil {
			return 0, qerr
		}
		for r := range results.Next() {
			if r.Error != nil {
				results.Close()
				return 0, r.Error
			}
			if err := store.Delete(ctx, ds.NewKey(r.Key)); err != nil && !errors.Is(err, ds.ErrNotFound) {
				results.Close()
				return 0, err
			}
		}
		results.Close()
	}
	for _, h := range inflightTips {
		if err := store.Delete(ctx, mcrdt.inflightHeadKey(h.DAGName, h.Cid)); err != nil {
			return 0, err
		}
	}
	if batching {
		if err := store.(ds.Batch).Commit(ctx); err != nil {
			return 0, err
		}
	}

	if err := mcrdt.dagService.RemoveMany(ctx, dagCIDs); err != nil {
		return 0, err
	}

	if _, err := mcrdt.heads.DeleteDAG(ctx, dagName); err != nil {
		return 0, err
	}

	return len(dagCIDs), nil
}

// DatatstoreNamespaces carries configuration for how internal namespaces are named.
type InternalNamespaces struct {
	Heads           string
	DAGHeads        string
	Set             string
	ProcessedBlocks string
	PendingBlocks   string
	InflightHeads   string
	DirtyBitKey     string
	BadShutdownKey  string
	VersionKey      string
}

type MerkleCRDTOptions struct {
	DeltaFactory func() Delta
	Namespaces   InternalNamespaces
}

// A MerkleCRDT is an advanced type to manually customize the merkle-CRDT. It
// allows submission of custom deltas and other advanced methods. If you just
// need a key-value store that conforms to the Datastore interface, use New()
// instead.
type MerkleCRDT struct {
	*Datastore
}

// NewMerkleCRDT returns a Merkle-CRDT Datastore wrapped with advanced
// methods. It allows setting internal options.
func NewMerkleCRDT(
	store ds.Datastore,
	namespace ds.Key,
	dagSyncer ipld.DAGService,
	bcast Broadcaster,
	opts *Options,
	internalOptions *MerkleCRDTOptions,
) (*MerkleCRDT, error) {
	if opts == nil {
		opts = DefaultOptions()
	}
	override := InternalNamespaces{}
	if internalOptions != nil {
		if df := internalOptions.DeltaFactory; df != nil {
			opts.crdtOpts.DeltaFactory = df
		}
		override = internalOptions.Namespaces
	}
	if err := resolveNamespaces(&opts.crdtOpts.Namespaces, &override); err != nil {
		return nil, err
	}

	d, err := New(store, namespace, dagSyncer, bcast, opts)
	if err != nil {
		return nil, err
	}
	return &MerkleCRDT{
		Datastore: d,
	}, nil
}

// resolveNamespaces applies non-empty override values onto dst and returns
// an error if the resulting set contains a duplicate. The intent is that
// dst arrives populated with defaults; only override fields with non-empty
// strings replace them.
func resolveNamespaces(dst, override *InternalNamespaces) error {
	fields := []struct {
		name string
		src  string
		dst  *string
	}{
		{"Heads", override.Heads, &dst.Heads},
		{"DAGHeads", override.DAGHeads, &dst.DAGHeads},
		{"Set", override.Set, &dst.Set},
		{"ProcessedBlocks", override.ProcessedBlocks, &dst.ProcessedBlocks},
		{"PendingBlocks", override.PendingBlocks, &dst.PendingBlocks},
		{"InflightHeads", override.InflightHeads, &dst.InflightHeads},
		{"DirtyBitKey", override.DirtyBitKey, &dst.DirtyBitKey},
		{"BadShutdownKey", override.BadShutdownKey, &dst.BadShutdownKey},
		{"VersionKey", override.VersionKey, &dst.VersionKey},
	}
	seen := make(map[string]string, len(fields))
	for _, f := range fields {
		if f.src != "" {
			*f.dst = f.src
		}
		if prev, ok := seen[*f.dst]; ok {
			return fmt.Errorf("internal namespace collision: %s and %s both use %q", prev, f.name, *f.dst)
		}
		seen[*f.dst] = f.name
	}
	return nil
}

// Publish allows to manually publish a Delta. The Priority is set
// automatically, upon which the delta is merged, serialized and broadcasted.
// Returns the CID of the new root node resulting from applying the delta.
func (mcrdt *MerkleCRDT) Publish(ctx context.Context, delta Delta) (Head, error) {
	return mcrdt.publish(ctx, delta)
}

// Set returns the internal set.
func (mcrdt *MerkleCRDT) Set() Set {
	return mcrdt.set
}

func (mcrdt *MerkleCRDT) Heads() Heads {
	return mcrdt.heads
}

// IsProcessed returns whether the given CID has been processed. Nodes are
// marked as processed as they are traversed during the DAG walk, so a CID
// being processed means it has been visited and merged into the set.
func (mcrdt *MerkleCRDT) IsProcessed(ctx context.Context, c cid.Cid) (bool, error) {
	return mcrdt.isProcessed(ctx, c)
}

// Traverse visits nodes in the Merkle-CRDT tree. It skips duplicates
// and calls the visit function with the Deltas extracted from every
// node. An error results in the traversal operations being aborted.
func (mcrdt *MerkleCRDT) Traverse(ctx context.Context,
	from []cid.Cid,
	visit func(ipld.Node) error,
) error {
	if len(from) == 0 {
		return errors.New("no roots to traverse from")
	}

	var ignoreCid cid.Cid
	var root ipld.Node
	var err error

	tFunc := func(current traverse.State) error {
		n := current.Node
		if ignoreCid.Defined() && ignoreCid.Equals(n.Cid()) {
			return nil
		}

		return visit(n)
	}

	// this is the default. Just to be explicit.
	tErrFunc := func(err error) error {
		return err
	}

	if len(from) == 1 { // root node is the given cid
		rootCid := from[0]
		root, err = mcrdt.dagService.Get(ctx, rootCid)
		if err != nil {
			return err
		}
	} else {
		heads := make([]Head, len(from))
		for i, c := range from {
			heads[i] = Head{
				Cid: c,
			}
		}
		delta := mcrdt.newDelta()
		// we are going to make a single root node to traverse from.
		root, err = makeNode(delta, heads)
		if err != nil {
			return err
		}
		ignoreCid = root.Cid() // tFunc will skip this.
	}

	opts := traverse.Options{
		DAG:            mcrdt.dagService,
		Order:          traverse.DFSPre, // default
		Func:           tFunc,
		ErrFunc:        tErrFunc,
		SkipDuplicates: true,
	}

	return traverse.Traverse(root, opts)
}
