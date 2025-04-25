package crdt

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/stretchr/testify/require"
)

func TestSnapshotBasicFlow(t *testing.T) {
	ctx := context.Background()

	replicas, closeReplicas := makeNReplicas(t, 1, nil)
	defer closeReplicas()
	datastore := replicas[0]

	// Add some keys
	kv := map[string]string{
		"k1": "v1",
		"k2": "v2",
		"k3": "v3",
	}

	for k, v := range kv {
		require.NoError(t, datastore.Put(ctx, dsKey(k), []byte(v)))
	}

	// Take a snapshot
	headCID, _, err := datastore.heads.List(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, headCID)

	snapInfo, err := datastore.compactAndSnapshot(ctx, headCID[0], cid.Undef)
	require.NoError(t, err)
	require.True(t, snapInfo.WrapperCID.Defined())

	// Ensure snapshot CID is saved
	saved, err := datastore.getLatestSnapshotCID(ctx)
	require.NoError(t, err)
	require.Equal(t, snapInfo.WrapperCID, saved)

	// Verify keys still accessible
	for k, v := range kv {
		val, err := datastore.Get(ctx, dsKey(k))
		require.NoError(t, err)
		require.Equal(t, []byte(v), val)
	}
}

func TestSnapshotWithTruncate(t *testing.T) {
	ctx := context.Background()

	replicas, closeReplicas := makeNReplicas(t, 1, nil)
	defer closeReplicas()
	datastore := replicas[0]

	// Add a chain of deltas
	kv := map[string]string{
		"foo": "one",
		"bar": "two",
	}

	for k, v := range kv {
		require.NoError(t, datastore.Put(ctx, dsKey(k), []byte(v)))
	}

	// Take initial snapshot
	headCID, _, err := datastore.heads.List(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, headCID)

	firstSnap, err := datastore.compactAndSnapshot(ctx, headCID[0], cid.Undef)
	require.NoError(t, err)

	// Add more keys
	extraKV := map[string]string{
		"baz": "three",
	}

	for k, v := range extraKV {
		require.NoError(t, datastore.Put(ctx, dsKey(k), []byte(v)))
	}

	// Second snapshot with truncation up to first snapshot
	headCID2, _, err := datastore.heads.List(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, headCID2)

	secondSnap, err := datastore.compactAndSnapshot(ctx, headCID2[0], firstSnap.DeltaHeadCID)
	require.NoError(t, err)

	require.True(t, secondSnap.WrapperCID.Defined())

	// Confirm all keys are accessible
	for k, v := range mergeMaps(kv, extraKV) {
		val, err := datastore.Get(ctx, dsKey(k))
		require.NoError(t, err)
		require.Equal(t, []byte(v), val)
	}

	// Validate snapshot linkage
	secondSnapInfo, err := datastore.loadSnapshotInfo(ctx, secondSnap.WrapperCID)
	require.NoError(t, err)
	require.Equal(t, firstSnap.WrapperCID, secondSnapInfo.PrevCID)
	require.Greater(t, secondSnapInfo.Height, firstSnap.Height)
}

func dsKey(k string) ds.Key {
	return ds.NewKey("/test/namespace/").ChildString(k)
}

func mergeMaps(m1, m2 map[string]string) map[string]string {
	merged := make(map[string]string, len(m1)+len(m2))
	for k, v := range m1 {
		merged[k] = v
	}
	for k, v := range m2 {
		merged[k] = v
	}
	return merged
}

func TestCompactionRetainsExpectedHeads(t *testing.T) {
	ctx := context.Background()

	opts := DefaultOptions()
	opts.CompactDagSize = 10 // Set low so we trigger compaction easily
	opts.CompactRetainNodes = 1
	opts.CompactInterval = time.Hour // Disable timer-based compaction

	replicas, closeReplicas := makeNReplicas(t, 1, opts)
	defer closeReplicas()
	datastore := replicas[0]

	// Insert 11 items → should trigger 1 compact
	for i := 0; i < 11; i++ {
		k := fmt.Sprintf("key-%d", i)
		v := fmt.Sprintf("value-%d", i)
		require.NoError(t, datastore.Put(ctx, ds.NewKey(k), []byte(v)))
	}

	// Manually trigger compaction
	require.NoError(t, datastore.triggerCompactionIfNeeded(ctx))

	// Check: Only 1 head should remain
	heads, _, err := datastore.heads.List(ctx)
	require.NoError(t, err)
	require.Len(t, heads, 1, "expected 1 head after compaction")

	// Insert 20 more items
	for i := 11; i < 31; i++ {
		k := fmt.Sprintf("key-%d", i)
		v := fmt.Sprintf("value-%d", i)
		require.NoError(t, datastore.Put(ctx, ds.NewKey(k), []byte(v)))
	}

	// Manually compact again
	require.NoError(t, datastore.triggerCompactionIfNeeded(ctx))

	// Should still retain only 1 head
	heads, _, err = datastore.heads.List(ctx)
	require.NoError(t, err)
	require.Len(t, heads, 1, "expected 1 head after second compaction")
}

func TestCollectDeltasStopsAtEndCID(t *testing.T) {
	ctx := context.Background()

	replicas, closeReplicas := makeNReplicas(t, 1, nil)
	defer closeReplicas()
	datastore := replicas[0]

	// 1. Create a DAG chain: delta-1 -> delta-2 -> delta-3
	k1 := ds.NewKey("k1")
	require.NoError(t, datastore.Put(ctx, k1, []byte("v1")))

	k2 := ds.NewKey("k2")
	require.NoError(t, datastore.Put(ctx, k2, []byte("v2")))

	k3 := ds.NewKey("k3")
	require.NoError(t, datastore.Put(ctx, k3, []byte("v3")))

	// 2. Grab heads
	heads, _, err := datastore.heads.List(ctx)
	require.NoError(t, err)
	require.Len(t, heads, 1)

	// Assume head is at delta-3
	headCID := heads[0]

	// 3. Snapshot at delta-2 (simulate it)
	//    (Normally you'd take a snapshot here — for now, just fake it.)
	var endCID cid.Cid
	{
		// Walk back 1 link manually to simulate delta-2
		node, err := datastore.dagService.Get(ctx, headCID)
		require.NoError(t, err)
		require.Len(t, node.Links(), 1)

		endCID = node.Links()[0].Cid
	}

	// 4. Now call collectDeltasFromDAG(start = delta-3, end = delta-2)
	deltas, blockIDs, err := datastore.collectDeltasFromDAG(ctx, headCID, endCID)
	require.NoError(t, err)

	// ✅ Should collect exactly 1 delta (delta-3 only)
	require.Len(t, deltas, 1, "should have collected exactly 1 delta")
	require.Len(t, blockIDs, 1, "should have collected exactly 1 block ID")

	// 5. Extra safety: delta should correspond to "k3"
	found := false
	for _, d := range deltas {
		for _, op := range d.Elements {
			if string(op.GetKey()) == "/k3" {
				found = true
				break
			}
		}
	}
	require.True(t, found, "expected delta for /k3 to be present")
}
