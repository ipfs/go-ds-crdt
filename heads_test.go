package crdt

import (
	"bytes"
	"context"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/multiformats/go-multihash"
)

var headsTestNS = ds.NewKey("headstest")

// TODO we should also test with a non-batching store
func newTestHeads(t *testing.T) *heads {
	t.Helper()
	store := dssync.MutexWrap(ds.NewMapDatastore())
	heads, err := newHeads(store, headsTestNS, &testLogger{
		name: t.Name(),
		l:    DefaultOptions().Logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	return heads
}

func newCID(t *testing.T) cid.Cid {
	t.Helper()
	var buf [32]byte
	_, _ = rand.Read(buf[:])

	mh, err := multihash.Sum(buf[:], multihash.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	return cid.NewCidV1(cid.DagProtobuf, mh)
}

func TestHeadsBasic(t *testing.T) {
	ctx := context.Background()

	heads := newTestHeads(t)
	l, err := heads.Len()
	if err != nil {
		t.Fatal(err)
	}
	if l != 0 {
		t.Errorf("new heads should have Len==0, got: %d", l)
	}

	cidHeights := make(map[cid.Cid]uint64)
	numHeads := 5
	for i := 0; i < numHeads; i++ {
		c, height := newCID(t), uint64(rand.Int())
		cidHeights[c] = height
		err := heads.Add(ctx, c, height)
		if err != nil {
			t.Fatal(err)
		}
	}

	assertHeads(t, heads, cidHeights)

	for c := range cidHeights {
		newC, newHeight := newCID(t), uint64(rand.Int())
		err := heads.Replace(ctx, c, newC, newHeight)
		if err != nil {
			t.Fatal(err)
		}
		delete(cidHeights, c)
		cidHeights[newC] = newHeight
		assertHeads(t, heads, cidHeights)
	}

	// Now try creating a new heads object and make sure what we
	// stored before is still there.
	err = heads.store.Sync(ctx, headsTestNS)
	if err != nil {
		t.Fatal(err)
	}

	heads, err = newHeads(heads.store, headsTestNS, &testLogger{
		name: t.Name(),
		l:    DefaultOptions().Logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertHeads(t, heads, cidHeights)
}

func assertHeads(t *testing.T, hh *heads, cidHeights map[cid.Cid]uint64) {
	t.Helper()

	headCids, maxHeight, err := hh.List()
	if err != nil {
		t.Fatal(err)
	}

	var expectedMaxHeight uint64
	for _, height := range cidHeights {
		if height > expectedMaxHeight {
			expectedMaxHeight = height
		}
	}
	if maxHeight != expectedMaxHeight {
		t.Errorf("expected max height=%d, got=%d", expectedMaxHeight, maxHeight)
	}

	headsLen, err := hh.Len()
	if err != nil {
		t.Fatal(err)
	}
	if len(headCids) != headsLen {
		t.Errorf("expected len and list to agree, got listLen=%d, len=%d", len(headCids), headsLen)
	}

	cids := make([]cid.Cid, 0, len(cidHeights))
	for c := range cidHeights {
		cids = append(cids, c)
	}
	sort.Slice(cids, func(i, j int) bool {
		ci := cids[i].Bytes()
		cj := cids[j].Bytes()
		return bytes.Compare(ci, cj) < 0
	})
	if !reflect.DeepEqual(cids, headCids) {
		t.Errorf("given cids don't match cids returned by List: %v, %v", cids, headCids)
	}
	for _, c := range cids {
		present, height, err := hh.IsHead(c)
		if err != nil {
			t.Fatal(err)
		}

		if !present {
			t.Errorf("cid returned by List reported absent by IsHead: %v", c)
		}
		if height != cidHeights[c] {
			t.Errorf("expected cid %v to have height %d, got: %d", c, cidHeights[c], height)
		}
	}
}
