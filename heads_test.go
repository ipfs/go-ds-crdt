package crdt

import (
	"bytes"
	"context"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/multiformats/go-multihash"
)

var headsTestNS = ds.NewKey("headstest")
var headsTestDagsNS = ds.NewKey("headstestdags")

var randg = rand.New(rand.NewSource(time.Now().UnixNano()))

// TODO we should also test with a non-batching store
func newTestHeads(t *testing.T) *heads {
	t.Helper()
	ctx := context.Background()
	store := dssync.MutexWrap(ds.NewMapDatastore())
	heads, err := newHeads(ctx, store, headsTestNS, headsTestDagsNS, &testLogger{
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
	_, _ = randg.Read(buf[:])

	mh, err := multihash.Sum(buf[:], multihash.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	return cid.NewCidV1(cid.DagProtobuf, mh)
}

func TestHeadsBasic(t *testing.T) {
	ctx := context.Background()

	heads := newTestHeads(t)
	l, err := heads.Len(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if l != 0 {
		t.Errorf("new heads should have Len==0, got: %d", l)
	}

	cidHeights := make(map[cid.Cid]Head)
	numHeads := 5
	for i := 0; i < numHeads; i++ {
		c, height := newCID(t), uint64(randg.Int())
		head := Head{Cid: c}
		head.Height = height
		cidHeights[c] = head

		err := heads.Add(ctx, head)
		if err != nil {
			t.Fatal(err)
		}
	}

	assertHeads(t, heads, cidHeights)

	for c, old := range cidHeights {
		newC, newHeight := newCID(t), uint64(randg.Int())
		head := Head{Cid: newC}
		head.Height = newHeight
		err := heads.Replace(ctx, old, head)
		if err != nil {
			t.Fatal(err)
		}
		delete(cidHeights, c)
		cidHeights[newC] = head
		assertHeads(t, heads, cidHeights)
	}

	// Now try creating a new heads object and make sure what we
	// stored before is still there.
	err = heads.store.Sync(ctx, headsTestNS)
	if err != nil {
		t.Fatal(err)
	}

	heads, err = newHeads(ctx, heads.store, headsTestNS, headsTestDagsNS, &testLogger{
		name: t.Name(),
		l:    DefaultOptions().Logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertHeads(t, heads, cidHeights)
}

func assertHeads(t *testing.T, hh *heads, headsMap map[cid.Cid]Head) {
	t.Helper()
	ctx := context.Background()

	heads, maxHeight, err := hh.List(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var expectedMaxHeight uint64
	for _, head := range headsMap {
		if head.Height > expectedMaxHeight {
			expectedMaxHeight = head.Height
		}
	}
	if maxHeight != expectedMaxHeight {
		t.Errorf("expected max height=%d, got=%d", expectedMaxHeight, maxHeight)
	}

	headsLen, err := hh.Len(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(heads) != headsLen {
		t.Errorf("expected len and list to agree, got listLen=%d, len=%d", len(heads), headsLen)
	}

	mapcids := make([]cid.Cid, 0, len(headsMap))
	for c := range headsMap {
		mapcids = append(mapcids, c)
	}

	headcids := make([]cid.Cid, 0, len(headsMap))
	for _, h := range headsMap {
		headcids = append(headcids, h.Cid)
	}

	sort.Slice(mapcids, func(i, j int) bool {
		ci := mapcids[i].Bytes()
		cj := mapcids[j].Bytes()
		return bytes.Compare(ci, cj) < 0
	})

	sort.Slice(headcids, func(i, j int) bool {
		ci := headcids[i].Bytes()
		cj := headcids[j].Bytes()
		return bytes.Compare(ci, cj) < 0
	})

	if !reflect.DeepEqual(mapcids, headcids) {
		t.Errorf("given cids don't match cids returned by List: %v, %v", mapcids, headcids)
	}
	for _, c := range mapcids {
		head, ok := hh.Get(ctx, c)
		if !ok {
			t.Errorf("cid returned by List reported absent by IsHead: %v", c)
		}
		if head.Height != headsMap[head.Cid].Height {
			t.Errorf("expected cid %v to have height %d, got: %d", c, headsMap[c].Height, head.Height)
		}
	}
}
