package crdt

import "github.com/ipfs/go-cid"

type ringSnapshots struct {
	cids []cid.Cid
	max  int
	next int
}

func newRingSnapshots(max int) *ringSnapshots {
	return &ringSnapshots{
		cids: make([]cid.Cid, max),
		max:  max,
	}
}

func (r *ringSnapshots) Add(c cid.Cid) {
	r.cids[r.next] = c
	r.next = (r.next + 1) % r.max
}

func (r *ringSnapshots) Contains(c cid.Cid) bool {
	for _, seen := range r.cids {
		if seen.Defined() && seen.Equals(c) {
			return true
		}
	}
	return false
}
