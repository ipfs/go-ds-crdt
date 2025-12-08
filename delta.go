package crdt

import (
	pb "github.com/ipfs/go-ds-crdt/pb"
	"google.golang.org/protobuf/proto"
)

// Delta represent a CRDT changeset, it carries new or updated elements and new or updated tombstones. The priority value allows comparing this update with others, to determine which elements take precedence.
type Delta interface {
	GetElements() ([]*pb.Element, error)
	GetTombstones() ([]*pb.Element, error)
	GetPriority() uint64
	SetElements(elems []*pb.Element)
	SetTombstones(tombs []*pb.Element)
	SetPriority(p uint64)
	Size() int
	GetDagName() string
	SetDagName(string)
	IsEmpty() bool
	Unmarshal([]byte) error
	Marshal() ([]byte, error)
}

var _ Delta = (*pbDelta)(nil)

type pbDelta struct {
	*pb.Delta
}

func (d *pbDelta) GetElements() ([]*pb.Element, error) {
	return d.Delta.GetElements(), nil
}

func (d *pbDelta) GetTombstones() ([]*pb.Element, error) {
	return d.Delta.GetTombstones(), nil
}

func (d *pbDelta) SetElements(elems []*pb.Element) {
	d.Elements = elems
}

func (d *pbDelta) SetTombstones(tombs []*pb.Element) {
	d.Tombstones = tombs
}

func (d *pbDelta) SetPriority(p uint64) {
	d.Priority = p
}

func (d *pbDelta) Size() int {
	if d == nil {
		return 0
	}
	return proto.Size(d.Delta)
}

func (d *pbDelta) GetDagName() string {
	return d.Delta.GetDagName()
}

func (d *pbDelta) SetDagName(n string) {
	d.DagName = n
}

func (d *pbDelta) IsEmpty() bool {
	return d == nil || (len(d.Tombstones)+len(d.Elements) == 0)
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
