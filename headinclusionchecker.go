package crdt

import (
	"context"
	"fmt"
	"sync"

	cid "github.com/ipfs/go-cid"
)

// DAGWalkerInterface defines the interface needed for head inclusion checking
type DAGWalkerInterface interface {
	WalkReverseTopo(ctx context.Context, heads []cid.Cid, stopAt cid.Cid) ([]cid.Cid, error)
}

type HeadInclusionChecker struct {
	walker DAGWalkerInterface
	// Cache to avoid redundant DAG walks - using KeyString() for efficiency
	reachabilityCache map[string]bool
	cacheMux          sync.RWMutex
}

// NewHeadInclusionChecker creates a checker with pre-allocated cache
func NewHeadInclusionChecker(walker DAGWalkerInterface, myHeadCount, maxPeerHeads int) *HeadInclusionChecker {
	// Pre-allocate based on expected cache size (myHeads × peerHeads per peer)
	estimatedSize := myHeadCount * maxPeerHeads
	if estimatedSize < 16 {
		estimatedSize = 16 // Reasonable minimum
	}

	return &HeadInclusionChecker{
		walker:            walker,
		reachabilityCache: make(map[string]bool, estimatedSize),
	}
}

// cacheKey creates efficient cache key using KeyString() instead of String()
func (h *HeadInclusionChecker) cacheKey(from, to cid.Cid) string {
	return from.KeyString() + ":" + to.KeyString()
}

// Reset clears the cache for reuse across multiple shutdown attempts
func (h *HeadInclusionChecker) Reset() {
	h.cacheMux.Lock()
	defer h.cacheMux.Unlock()

	// Clear existing map
	for k := range h.reachabilityCache {
		delete(h.reachabilityCache, k)
	}
}

func (h *HeadInclusionChecker) isReachable(ctx context.Context, from, to cid.Cid) (bool, error) {
	key := h.cacheKey(from, to)

	// Fast path: check cache with read lock
	h.cacheMux.RLock()
	if result, exists := h.reachabilityCache[key]; exists {
		h.cacheMux.RUnlock()
		return result, nil
	}
	h.cacheMux.RUnlock()

	// Slow path: perform DAG walk
	path, err := h.walker.WalkReverseTopo(ctx, []cid.Cid{from}, to)
	if err != nil {
		return false, fmt.Errorf("DAG walk failed from %s to %s: %w", from, to, err)
	}

	// Check if target is reachable in the path
	reachable := false
	for _, pathCid := range path {
		if pathCid.Equals(to) {
			reachable = true
			break
		}
	}

	// Cache the result with write lock
	h.cacheMux.Lock()
	h.reachabilityCache[key] = reachable
	h.cacheMux.Unlock()

	return reachable, nil
}

// HeadsIncluded checks if all myHeads are reachable from at least one head in peerHeads
// This implements the subset containment check: myHeads ⊆ ancestry(peerHeads)
func (h *HeadInclusionChecker) HeadsIncluded(ctx context.Context, myHeads, peerHeads []cid.Cid) (bool, error) {
	if len(myHeads) == 0 {
		return true, nil // Vacuously true
	}

	if len(peerHeads) == 0 {
		return false, nil // Can't include anything in empty set
	}

	// For each of our heads, check if it's reachable from any peer head
	for _, myHead := range myHeads {
		found := false
		for _, peerHead := range peerHeads {
			reachable, err := h.isReachable(ctx, peerHead, myHead)
			if err != nil {
				return false, fmt.Errorf("reachability check failed from %s to %s: %w",
					peerHead, myHead, err)
			}
			if reachable {
				found = true
				break // This head is covered, check next
			}
		}
		if !found {
			return false, nil // At least one of our heads is not included
		}
	}

	return true, nil // All our heads are included
}
