package crdt

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

// DAGWalker defines a reusable walker for traversing a DAG backward from heads.
type DAGWalker struct {
	dagService ipld.DAGService
}

func NewDAGWalker(dagService ipld.DAGService) *DAGWalker {
	return &DAGWalker{dagService: dagService}
}

// WalkReverseTopo performs a reverse topological walk from the given heads to the stopAt CID.
// It ensures all ancestors of stopAt are visited before stopAt itself.
func (w *DAGWalker) WalkReverseTopo(ctx context.Context, heads []cid.Cid, stopAt cid.Cid) ([]cid.Cid, error) {
	visited := cid.NewSet()
	tempMark := cid.NewSet()
	var order []cid.Cid

	var visit func(cid.Cid) error
	visit = func(current cid.Cid) error {
		if !current.Defined() {
			return nil
		}
		if visited.Has(current) {
			return nil
		}
		if tempMark.Has(current) {
			return fmt.Errorf("cycle detected at %s", current)
		}
		tempMark.Add(current)

		node, err := w.dagService.Get(ctx, current)
		if err != nil {
			return fmt.Errorf("loading node %s: %w", current, err)
		}
		for _, link := range node.Links() {
			if err := visit(link.Cid); err != nil {
				return err
			}
		}

		visited.Add(current)
		order = append(order, current)
		return nil
	}

	for _, h := range heads {
		if err := visit(h); err != nil {
			return nil, err
		}
	}

	// Ensure stopAt is included, even if not explicitly linked
	if stopAt.Defined() && !visited.Has(stopAt) {
		if err := visit(stopAt); err != nil {
			return nil, err
		}
	}

	return order, nil
}
