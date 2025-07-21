package crdt

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
)

// DefaultGracefulShutdownOptions provides sensible defaults
func DefaultGracefulShutdownOptions() *GracefulShutdownOptions {
	return &GracefulShutdownOptions{
		MaxWaitTime:     30 * time.Second,
		GracePeriod:     10 * time.Second,
		CheckInterval:   time.Second,
		PeerSelector:    nil,
		RequiredMatches: 0,
	}
}

// PeerSelector is a function that determines if a peer should be considered for shutdown acknowledgment
type PeerSelector func(metadata map[string]string) bool

// GracefulShutdownOptions configures the graceful shutdown behavior
type GracefulShutdownOptions struct {
	MaxWaitTime     time.Duration // Maximum time to wait for head inclusion
	GracePeriod     time.Duration // TTL override period for departure announcement
	CheckInterval   time.Duration // How often to check peer heads
	PeerSelector    PeerSelector  // Function to select which peers must acknowledge our heads
	RequiredMatches int           // Required number of peers that much match the peer selector
}

// AndSelector combines multiple selectors with AND logic
func AndSelector(selectors ...PeerSelector) PeerSelector {
	return func(metadata map[string]string) bool {
		for _, selector := range selectors {
			if !selector(metadata) {
				return false
			}
		}
		return true
	}
}

// OrSelector combines multiple selectors with OR logic
func OrSelector(selectors ...PeerSelector) PeerSelector {
	return func(metadata map[string]string) bool {
		for _, selector := range selectors {
			if selector(metadata) {
				return true
			}
		}
		return false
	}
}

// GracefulShutdown implements the revised shutdown sequence using subset containment
//
// This function assumes:
// 1. Local writes have stopped
// 2. Node continues to process gossip during shutdown
func (store *Datastore) GracefulShutdown(ctx context.Context, opts *GracefulShutdownOptions) error {
	if opts == nil {
		opts = DefaultGracefulShutdownOptions()
	}

	store.logger.Info("initiating graceful shutdown with subset containment logic")

	// 1. Snapshot the heads we must protect (after writes are frozen)
	myHeads, maxHeight, err := store.ListHeads(ctx)
	if err != nil {
		return fmt.Errorf("failed to get local heads: %w", err)
	}

	if len(myHeads) == 0 {
		store.logger.Info("no local heads to protect, proceeding with immediate shutdown")
		return store.announceAndClose(ctx, opts.GracePeriod)
	}

	store.logger.Infof("protecting %d heads (max height: %d)", len(myHeads), maxHeight)
	for i, head := range myHeads {
		store.logger.Debugf("  head[%d]: %s", i, head)
	}

	// 2. Set up head inclusion checker with caching
	walker := NewDAGWalker(store.dagService)
	selectedPeers := store.getSelectedPeers(opts.PeerSelector)
	delete(selectedPeers, store.h.ID().String()) // Exclude ourselves

	maxPeerHeads := 0
	for _, heads := range selectedPeers {
		if len(heads) > maxPeerHeads {
			maxPeerHeads = len(heads)
		}
	}

	checker := NewHeadInclusionChecker(walker, len(myHeads), maxPeerHeads)

	if len(selectedPeers) == 0 {
		store.logger.Warn("no selected peers found - proceeding with immediate shutdown")
		return store.announceAndClose(ctx, opts.GracePeriod)
	}

	// 3. Wait for subset inclusion: myHeads ⊆ ancestry(core.DagHeads)
	deadline := time.After(opts.MaxWaitTime)
	ticker := time.NewTicker(opts.CheckInterval)
	defer ticker.Stop()

	store.logger.Infof("waiting for head inclusion by %d selected peers", len(selectedPeers))

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled during graceful shutdown: %w", ctx.Err())
		case <-deadline:
			return fmt.Errorf("timeout waiting for head inclusion after %v", opts.MaxWaitTime)
		case <-ticker.C:
			satisfied, progress, err := store.checkHeadInclusion(ctx, checker, myHeads, opts.PeerSelector, opts.RequiredMatches)
			if err != nil {
				store.logger.Errorf("error checking head inclusion: %v", err)
				continue // Retry on next tick
			}

			if satisfied {
				store.logger.Info("head inclusion satisfied - proceeding with shutdown")
				return store.announceAndClose(ctx, opts.GracePeriod)
			}

			// Log progress
			store.logger.Infof("head inclusion progress: %s", progress)
		}
	}
}

// checkHeadInclusion verifies that our heads are included in selected peer heads
func (store *Datastore) checkHeadInclusion(ctx context.Context, checker *HeadInclusionChecker, myHeads []cid.Cid, selector PeerSelector, requiredMatches int) (bool, string, error) {
	selectedPeers := store.getSelectedPeers(selector)
	delete(selectedPeers, store.h.ID().String()) // Exclude ourselves

	if len(selectedPeers) == 0 {
		return true, "no selected peers to check", nil
	}

	satisfiedPeers := []string{}
	unsatisfiedPeers := []string{}

	for peerID, peerHeads := range selectedPeers {
		if len(peerHeads) == 0 {
			unsatisfiedPeers = append(unsatisfiedPeers, peerID+" (no heads)")
			continue
		}

		included, err := checker.HeadsIncluded(ctx, myHeads, peerHeads)
		if err != nil {
			return false, "", fmt.Errorf("failed to check inclusion for peer %s: %w", peerID, err)
		}

		if included {
			satisfiedPeers = append(satisfiedPeers, peerID)
		} else {
			unsatisfiedPeers = append(unsatisfiedPeers, peerID)
		}
	}

	progress := fmt.Sprintf("%d/%d selected peers satisfied (%v)",
		len(satisfiedPeers), len(selectedPeers), satisfiedPeers)

	// Handle special cases for requiredMatches
	var satisfied bool
	switch {
	case requiredMatches <= 0:
		// 0 or negative means "any one peer" (at least 1)
		satisfied = len(satisfiedPeers) > 0
	case requiredMatches >= len(selectedPeers):
		// Requesting more than available means "all peers"
		satisfied = len(unsatisfiedPeers) == 0
	default:
		// Exact count requested
		satisfied = len(satisfiedPeers) >= requiredMatches
	}

	return satisfied, progress, nil
}

// announceAndClose handles the final steps: announce departure and close
func (store *Datastore) announceAndClose(ctx context.Context, gracePeriod time.Duration) error {
	// 4. Announce departure with short TTL so cluster forgets us quickly
	if err := store.state.MarkLeaving(ctx, store.h.ID(), gracePeriod); err != nil {
		store.logger.Warnf("failed to mark leaving: %v", err)
		// Continue anyway - this is not fatal
	}

	if err := store.broadcast(ctx); err != nil {
		store.logger.Warnf("failed to broadcast departure: %v", err)
		// Continue anyway
	}

	store.logger.Infof("announced departure with %v grace period", gracePeriod)

	// 5. Close the datastore
	return store.Close()
}

// Additional helper methods needed for the graceful shutdown API

func (store *Datastore) ListHeads(ctx context.Context) ([]cid.Cid, uint64, error) {
	return store.heads.List(ctx)
}

// getSelectedPeers returns peers that match the selector function
func (store *Datastore) getSelectedPeers(selector PeerSelector) map[string][]cid.Cid {
	out := make(map[string][]cid.Cid)
	state := store.state.GetState()

	for id, p := range state.Members {
		// Apply selector function
		if selector != nil && !selector(p.Metadata) {
			continue
		}

		var heads []cid.Cid
		for _, h := range p.DagHeads {
			if c, err := cid.Cast(h.Cid); err == nil {
				heads = append(heads, c)
			}
		}

		if len(heads) > 0 {
			out[id] = heads
		}
	}
	return out
}

func (store *Datastore) PeerHeads(roleFilter string) map[string][]cid.Cid {
	// Backward compatibility: convert roleFilter to selector
	var selector PeerSelector
	if roleFilter != "" {
		selector = func(metadata map[string]string) bool {
			return metadata["role"] == roleFilter
		}
	}
	return store.getSelectedPeers(selector)
}
