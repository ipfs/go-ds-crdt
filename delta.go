package crdt

import (
	pb "github.com/ipfs/go-ds-crdt/pb"
	"google.golang.org/protobuf/proto"
)

// Delta represent a CRDT changeset, it carries new or updated elements and new or updated tombstones. The priority value allows comparing this update with others, to determine which elements take precedence.
type Delta interface {
	GetElements() []*pb.Element
	GetTombstones() []*pb.Element
	GetPriority() uint64
	SetElements(elems []*pb.Element)
	SetTombstones(tombs []*pb.Element)
	SetPriority(p uint64)
	Size() int
	IsEmpty() bool
	Unmarshal([]byte) error
	Marshal() ([]byte, error)
}

type pbDelta struct {
	*pb.Delta
}

func (d *pbDelta) SetElements(elems []*pb.Element) {
	d.Delta.Elements = elems
}

func (d *pbDelta) SetTombstones(tombs []*pb.Element) {
	d.Delta.Tombstones = tombs
}

func (d *pbDelta) SetPriority(p uint64) {
	d.Delta.Priority = p
}

func (d *pbDelta) Size() int {
	if d == nil {
		return 0
	}
	return proto.Size(d.Delta)
}

func (d *pbDelta) IsEmpty() bool {
	return d == nil || (len(d.Delta.Tombstones)+len(d.Delta.Elements) == 0)
}

func (d *pbDelta) Unmarshal(b []byte) error {
	return proto.Unmarshal(b, d)
}

func (d *pbDelta) Marshal() ([]byte, error) {
	if d != nil {
		return proto.Marshal(d)
	}
	return nil, nil
}
