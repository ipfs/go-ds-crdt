package crdt

import (
	"context"
	"errors"

	"github.com/gogo/protobuf/proto"
	cid "github.com/ipfs/go-cid"
	pb "github.com/ipfs/go-ds-crdt/pb"
	ipld "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
)

// IPLD related things

var _ ipld.DAGService = (*crdtDAGService)(nil)

func init() {
	ipld.Register(cid.DagProtobuf, dag.DecodeProtobufBlock)
}

// crdtDAGService wraps an ipld.DAGService with some additional methods.
type crdtDAGService struct {
	DAGSyncer
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
