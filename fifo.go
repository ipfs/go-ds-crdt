package crdt

import (
	"sync"

	cid "github.com/ipfs/go-cid"
)

type fifoElem struct {
	current cid.Cid
	top     cid.Cid
	depth   uint64
}

type fifo struct {
	mux   sync.RWMutex
	slice []*fifoElem
}

func newFifo(elems ...*fifoElem) *fifo {
	return &fifo{
		slice: elems,
	}
}

func (f *fifo) Pop() *fifoElem {
	//f.mux.Lock()
	//defer f.mux.Unlock()
	if len(f.slice) == 0 {
		return nil
	}
	elem := f.slice[0]
	f.slice = f.slice[1:]
	return elem
}

func (f *fifo) PopN(n int) []*fifoElem {
	//f.mux.Lock()
	//defer f.mux.Unlock()
	if l := len(f.slice); l < n {
		n = l
	}
	elems := f.slice[0:n]
	f.slice = f.slice[n:]
	return elems
}

func (f *fifo) Push(e *fifoElem) {
	//f.mux.Lock()
	//defer f.mux.Unlock()
	f.slice = append(f.slice, e)
}

func (f *fifo) PushN(elems ...*fifoElem) {
	if len(elems) == 0 {
		return
	}
	//f.mux.Lock()
	//defer f.mux.Unlock()
	f.slice = append(f.slice, elems...)
}
