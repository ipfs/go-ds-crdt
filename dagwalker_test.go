package crdt

import (
	"context"
	"testing"

	cid "github.com/ipfs/go-cid"
	pb "github.com/ipfs/go-ds-crdt/pb"
	"github.com/stretchr/testify/require"
)

func TestWalkReplayPath(t *testing.T) {
	tests := map[string]struct {
		setup    func(ctx context.Context, datastore *Datastore) (heads []cid.Cid, stopAt cid.Cid, expectedNodes []cid.Cid)
		validate func(t *testing.T, path []cid.Cid, stopAt cid.Cid, expectedNodes []cid.Cid, datastore *Datastore)
	}{
		"basic_predecessors_included": {
			setup: func(ctx context.Context, datastore *Datastore) ([]cid.Cid, cid.Cid, []cid.Cid) {
				// DAG: head1 → A → B → stopAt, head2 → C → stopAt
				stopCid := makeAndAdd(datastore, ctx, 5, nil)
				bCid := makeAndAdd(datastore, ctx, 4, []cid.Cid{stopCid})
				aCid := makeAndAdd(datastore, ctx, 3, []cid.Cid{bCid})
				cCid := makeAndAdd(datastore, ctx, 2, []cid.Cid{stopCid})

				return []cid.Cid{aCid, cCid}, stopCid, []cid.Cid{aCid, bCid, cCid, stopCid}
			},
			validate: func(t *testing.T, path []cid.Cid, stopAt cid.Cid, expectedNodes []cid.Cid, datastore *Datastore) {
				// Verify all expected nodes are present
				seen := make(map[cid.Cid]int)
				for i, c := range path {
					seen[c] = i
				}

				for _, node := range expectedNodes {
					require.Contains(t, seen, node, "missing expected node: %s", node.String())
				}

				// The key invariant: stopAt position determines what gets processed
				// In reverse topo order, dependencies come first, then dependents
				// So stopAt should come before nodes that depend on it
				stopAtIndex := seen[stopAt]
				for _, node := range expectedNodes {
					if !node.Equals(stopAt) {
						require.Greater(t, seen[node], stopAtIndex,
							"dependent node %s must appear after stopAt %s in reverse topo order",
							node.String(), stopAt.String())
					}
				}
			},
		},
		"respects_topological_order": {
			setup: func(ctx context.Context, datastore *Datastore) ([]cid.Cid, cid.Cid, []cid.Cid) {
				// Same DAG structure, but focus on explicit B→stopAt relationship
				stopCid := makeAndAdd(datastore, ctx, 5, nil)
				bCid := makeAndAdd(datastore, ctx, 4, []cid.Cid{stopCid})
				aCid := makeAndAdd(datastore, ctx, 3, []cid.Cid{bCid})
				cCid := makeAndAdd(datastore, ctx, 2, []cid.Cid{stopCid})

				return []cid.Cid{aCid, cCid}, stopCid, []cid.Cid{bCid}
			},
			validate: func(t *testing.T, path []cid.Cid, stopAt cid.Cid, expectedNodes []cid.Cid, datastore *Datastore) {
				var stopAtIndex, bIndex int = -1, -1
				bCid := expectedNodes[0] // B is the specific node we're testing

				for i, c := range path {
					if c.Equals(stopAt) {
						stopAtIndex = i
					}
					if c.Equals(bCid) {
						bIndex = i
					}
				}

				require.NotEqual(t, -1, stopAtIndex, "stopAt (%s) not found in path", stopAt.String())
				require.NotEqual(t, -1, bIndex, "B (%s) not found in path", bCid.String())
				require.Less(t, stopAtIndex, bIndex,
					"stopAt (%s) must appear before B (%s)", stopAt.String(), bCid.String())
			},
		},
		"deep_branch_ordering": {
			setup: func(ctx context.Context, datastore *Datastore) ([]cid.Cid, cid.Cid, []cid.Cid) {
				// Deep path: A → B → D → E → F → stopAt, Short path: C → stopAt
				stopCid := makeAndAdd(datastore, ctx, 99, nil)
				fCid := makeAndAdd(datastore, ctx, 6, []cid.Cid{stopCid})
				eCid := makeAndAdd(datastore, ctx, 5, []cid.Cid{fCid})
				dCid := makeAndAdd(datastore, ctx, 4, []cid.Cid{eCid})
				bCid := makeAndAdd(datastore, ctx, 3, []cid.Cid{dCid})
				aCid := makeAndAdd(datastore, ctx, 2, []cid.Cid{bCid})
				cCid := makeAndAdd(datastore, ctx, 1, []cid.Cid{stopCid})

				return []cid.Cid{aCid, cCid}, stopCid, []cid.Cid{bCid, dCid, eCid, fCid}
			},
			validate: func(t *testing.T, path []cid.Cid, stopAt cid.Cid, expectedNodes []cid.Cid, datastore *Datastore) {
				seen := make(map[string]int)
				for i, c := range path {
					seen[c.String()] = i
				}

				// Ensure all intermediate nodes come after stopAt
				for _, cid := range expectedNodes {
					require.Contains(t, seen, cid.String(), "missing node in path: %s", cid.String())
					require.Less(t, seen[stopAt.String()], seen[cid.String()],
						"stopAt (%s) must come before %s", stopAt.String(), cid.String())
				}
			},
		},
		"unreachable_stopAt": {
			setup: func(ctx context.Context, datastore *Datastore) ([]cid.Cid, cid.Cid, []cid.Cid) {
				// Disconnected DAG: head has no path to stopAt
				stopCid := makeAndAdd(datastore, ctx, 100, nil)
				headCid := makeAndAdd(datastore, ctx, 1, nil)

				return []cid.Cid{headCid}, stopCid, []cid.Cid{headCid}
			},
			validate: func(t *testing.T, path []cid.Cid, stopAt cid.Cid, expectedNodes []cid.Cid, datastore *Datastore) {
				// Verify unreachable stopAt is not included
				for _, c := range path {
					require.NotEqual(t, c, stopAt,
						"unreachable stopAt (%s) must not be included in path", stopAt.String())
				}

				// Verify expected nodes are present
				seen := make(map[cid.Cid]bool)
				for _, c := range path {
					seen[c] = true
				}

				for _, node := range expectedNodes {
					require.True(t, seen[node], "expected node (%s) missing from path", node.String())
				}
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			replicas, closeReplicas := makeNReplicas(t, 1, nil)
			defer closeReplicas()
			datastore := replicas[0]

			heads, stopAt, expectedNodes := tt.setup(ctx, datastore)

			path, err := datastore.walkReplayPath(ctx, heads, stopAt)
			require.NoError(t, err)

			tt.validate(t, path, stopAt, expectedNodes, datastore)
		})
	}
}

func makeAndAdd(datastore *Datastore, ctx context.Context, priority int, parents []cid.Cid) cid.Cid {
	node, err := makeNode(&pb.Delta{Priority: uint64(priority)}, parents)
	if err != nil {
		panic(err)
	}
	if err := datastore.dagService.Add(ctx, node); err != nil {
		panic(err)
	}
	return node.Cid()
}
