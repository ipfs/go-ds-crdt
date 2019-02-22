package crdt

import (
	"context"
	"errors"

	"github.com/gogo/protobuf/proto"
	pb "github.com/hsanjuan/go-ds-crdt/pb"
	blockfmt "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
)

// IPLD related things

var _ ipld.DAGService = (*crdtDAGService)(nil)

var defaultWorkers = 20

var errNotImplemented = errors.New("not implemented")

var errNoDAGSyncer = errors.New("no DAGSyncer configured. This replica is offline.")

func init() {
	ipld.Register(cid.DagProtobuf, dag.DecodeProtobufBlock)
}

// crdtDAGService wraps an ipld.DAGService with some additional methods.
type crdtDAGService struct {
	ds      DAGSyncer
	workers int
}

func (ng *crdtDAGService) Get(ctx context.Context, c cid.Cid) (ipld.Node, error) {
	if ng.ds == nil {
		return nil, errNoDAGSyncer
	}

	blk, err := ng.ds.Get(ctx, c)
	if err != nil {
		return nil, err
	}

	return ipld.Decode(blk)
}

func (ng *crdtDAGService) GetMany(ctx context.Context, cids []cid.Cid) <-chan *ipld.NodeOption {
	results := make(chan *ipld.NodeOption, len(cids))
	jobs := make(chan cid.Cid, len(cids))

	if ng.workers <= 0 {
		ng.workers = defaultWorkers
	}

	// launch maximum ng.workers. Do not launch more workers than cids
	// though.
	for w := 0; w < ng.workers && w < len(cids); w++ {
		go func() {
			for c := range jobs {
				nd, err := ng.Get(ctx, c)
				results <- &ipld.NodeOption{
					Node: nd,
					Err:  err,
				}
			}
		}()
	}

	for _, c := range cids {
		jobs <- c
	}
	close(jobs)
	return nil
}

func (ng *crdtDAGService) Add(ctx context.Context, nd ipld.Node) error {
	if ng.ds == nil {
		return errNoDAGSyncer
	}

	block, err := blockfmt.NewBlockWithCid(nd.RawData(), nd.Cid())
	if err != nil {
		return err
	}

	return ng.ds.Put(ctx, block)
}

func (ng *crdtDAGService) AddMany(ctx context.Context, nds []ipld.Node) error {
	for _, n := range nds {
		err := ng.Add(ctx, n)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ng *crdtDAGService) Remove(ctx context.Context, c cid.Cid) error {
	return errNotImplemented
}

func (ng *crdtDAGService) RemoveMany(ctx context.Context, cids []cid.Cid) error {
	return errNotImplemented
}

func (ng *crdtDAGService) GetDelta(ctx context.Context, c cid.Cid) (ipld.Node, *pb.Delta, error) {
	nd, err := ng.Get(ctx, c)
	if err != nil {
		return nil, nil, err
	}
	delta, err := extractDelta(nd)
	return nd, delta, err
}

// GetHeight returns the height of a block
func (ng *crdtDAGService) GetPriority(ctx context.Context, c cid.Cid) (uint64, error) {
	_, delta, err := ng.GetDelta(ctx, c)
	if err != nil {
		return 0, err
	}
	return delta.Priority, nil
}

// func (ng *crdtDAGService) FetchRefs(ctx context.Context, depth int) error {
// 	return ng.rpcClient.CallContext(
// 		ctx,
// 		"",
// 		"Cluster",
// 		"IPFSConnectorFetchRefs",
// 		depth,
// 		&struct{}{},
// 	)
// }

func extractDelta(nd ipld.Node) (*pb.Delta, error) {
	protonode, ok := nd.(*dag.ProtoNode)
	if !ok {
		return nil, errors.New("node is not a ProtoNode")
	}
	d := &pb.Delta{}
	err := proto.Unmarshal(protonode.Data(), d)
	return d, err
}

func makeNode(delta *pb.Delta, heads []cid.Cid) (ipld.Node, error) {
	var data []byte
	var err error
	if delta != nil {
		data, err = proto.Marshal(delta)
		if err != nil {
			return nil, err
		}
	}

	nd := dag.NodeWithData(data)
	for _, h := range heads {
		err = nd.AddRawLink("", &ipld.Link{Cid: h})
		if err != nil {
			return nil, err
		}
	}
	return nd, nil
}
