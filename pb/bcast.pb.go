// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v5.28.2
// source: bcast.proto

package pb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type StateBroadcast struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Snapshot *Head                   `protobuf:"bytes,2,opt,name=Snapshot,proto3" json:"Snapshot,omitempty"`                                                                                       // latest HAMT snapshot head
	Members  map[string]*Participant `protobuf:"bytes,1,rep,name=Members,proto3" json:"Members,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"` // Map of participants
}

func (x *StateBroadcast) Reset() {
	*x = StateBroadcast{}
	if protoimpl.UnsafeEnabled {
		mi := &file_bcast_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StateBroadcast) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StateBroadcast) ProtoMessage() {}

func (x *StateBroadcast) ProtoReflect() protoreflect.Message {
	mi := &file_bcast_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StateBroadcast.ProtoReflect.Descriptor instead.
func (*StateBroadcast) Descriptor() ([]byte, []int) {
	return file_bcast_proto_rawDescGZIP(), []int{0}
}

func (x *StateBroadcast) GetSnapshot() *Head {
	if x != nil {
		return x.Snapshot
	}
	return nil
}

func (x *StateBroadcast) GetMembers() map[string]*Participant {
	if x != nil {
		return x.Members
	}
	return nil
}

type Participant struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	BestBefore uint64  `protobuf:"varint,1,opt,name=BestBefore,proto3" json:"BestBefore,omitempty"` // Timestamp for garbage collection
	DagHeads   []*Head `protobuf:"bytes,2,rep,name=DagHeads,proto3" json:"DagHeads,omitempty"`      // List of DAG heads
}

func (x *Participant) Reset() {
	*x = Participant{}
	if protoimpl.UnsafeEnabled {
		mi := &file_bcast_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Participant) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Participant) ProtoMessage() {}

func (x *Participant) ProtoReflect() protoreflect.Message {
	mi := &file_bcast_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Participant.ProtoReflect.Descriptor instead.
func (*Participant) Descriptor() ([]byte, []int) {
	return file_bcast_proto_rawDescGZIP(), []int{1}
}

func (x *Participant) GetBestBefore() uint64 {
	if x != nil {
		return x.BestBefore
	}
	return 0
}

func (x *Participant) GetDagHeads() []*Head {
	if x != nil {
		return x.DagHeads
	}
	return nil
}

type Head struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Cid []byte `protobuf:"bytes,1,opt,name=Cid,proto3" json:"Cid,omitempty"` // CID representing a head
}

func (x *Head) Reset() {
	*x = Head{}
	if protoimpl.UnsafeEnabled {
		mi := &file_bcast_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Head) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Head) ProtoMessage() {}

func (x *Head) ProtoReflect() protoreflect.Message {
	mi := &file_bcast_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Head.ProtoReflect.Descriptor instead.
func (*Head) Descriptor() ([]byte, []int) {
	return file_bcast_proto_rawDescGZIP(), []int{2}
}

func (x *Head) GetCid() []byte {
	if x != nil {
		return x.Cid
	}
	return nil
}

var File_bcast_proto protoreflect.FileDescriptor

var file_bcast_proto_rawDesc = []byte{
	0x0a, 0x0b, 0x62, 0x63, 0x61, 0x73, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x07, 0x63,
	0x72, 0x64, 0x74, 0x2e, 0x70, 0x62, 0x22, 0xcd, 0x01, 0x0a, 0x0e, 0x53, 0x74, 0x61, 0x74, 0x65,
	0x42, 0x72, 0x6f, 0x61, 0x64, 0x63, 0x61, 0x73, 0x74, 0x12, 0x29, 0x0a, 0x08, 0x53, 0x6e, 0x61,
	0x70, 0x73, 0x68, 0x6f, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0d, 0x2e, 0x63, 0x72,
	0x64, 0x74, 0x2e, 0x70, 0x62, 0x2e, 0x48, 0x65, 0x61, 0x64, 0x52, 0x08, 0x53, 0x6e, 0x61, 0x70,
	0x73, 0x68, 0x6f, 0x74, 0x12, 0x3e, 0x0a, 0x07, 0x4d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x73, 0x18,
	0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x24, 0x2e, 0x63, 0x72, 0x64, 0x74, 0x2e, 0x70, 0x62, 0x2e,
	0x53, 0x74, 0x61, 0x74, 0x65, 0x42, 0x72, 0x6f, 0x61, 0x64, 0x63, 0x61, 0x73, 0x74, 0x2e, 0x4d,
	0x65, 0x6d, 0x62, 0x65, 0x72, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x07, 0x4d, 0x65, 0x6d,
	0x62, 0x65, 0x72, 0x73, 0x1a, 0x50, 0x0a, 0x0c, 0x4d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x73, 0x45,
	0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x2a, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x14, 0x2e, 0x63, 0x72, 0x64, 0x74, 0x2e, 0x70, 0x62, 0x2e,
	0x50, 0x61, 0x72, 0x74, 0x69, 0x63, 0x69, 0x70, 0x61, 0x6e, 0x74, 0x52, 0x05, 0x76, 0x61, 0x6c,
	0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x22, 0x58, 0x0a, 0x0b, 0x50, 0x61, 0x72, 0x74, 0x69, 0x63,
	0x69, 0x70, 0x61, 0x6e, 0x74, 0x12, 0x1e, 0x0a, 0x0a, 0x42, 0x65, 0x73, 0x74, 0x42, 0x65, 0x66,
	0x6f, 0x72, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0a, 0x42, 0x65, 0x73, 0x74, 0x42,
	0x65, 0x66, 0x6f, 0x72, 0x65, 0x12, 0x29, 0x0a, 0x08, 0x44, 0x61, 0x67, 0x48, 0x65, 0x61, 0x64,
	0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0d, 0x2e, 0x63, 0x72, 0x64, 0x74, 0x2e, 0x70,
	0x62, 0x2e, 0x48, 0x65, 0x61, 0x64, 0x52, 0x08, 0x44, 0x61, 0x67, 0x48, 0x65, 0x61, 0x64, 0x73,
	0x22, 0x18, 0x0a, 0x04, 0x48, 0x65, 0x61, 0x64, 0x12, 0x10, 0x0a, 0x03, 0x43, 0x69, 0x64, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03, 0x43, 0x69, 0x64, 0x42, 0x06, 0x5a, 0x04, 0x2e, 0x3b,
	0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_bcast_proto_rawDescOnce sync.Once
	file_bcast_proto_rawDescData = file_bcast_proto_rawDesc
)

func file_bcast_proto_rawDescGZIP() []byte {
	file_bcast_proto_rawDescOnce.Do(func() {
		file_bcast_proto_rawDescData = protoimpl.X.CompressGZIP(file_bcast_proto_rawDescData)
	})
	return file_bcast_proto_rawDescData
}

var file_bcast_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_bcast_proto_goTypes = []interface{}{
	(*StateBroadcast)(nil), // 0: crdt.pb.StateBroadcast
	(*Participant)(nil),    // 1: crdt.pb.Participant
	(*Head)(nil),           // 2: crdt.pb.Head
	nil,                    // 3: crdt.pb.StateBroadcast.MembersEntry
}
var file_bcast_proto_depIdxs = []int32{
	2, // 0: crdt.pb.StateBroadcast.Snapshot:type_name -> crdt.pb.Head
	3, // 1: crdt.pb.StateBroadcast.Members:type_name -> crdt.pb.StateBroadcast.MembersEntry
	2, // 2: crdt.pb.Participant.DagHeads:type_name -> crdt.pb.Head
	1, // 3: crdt.pb.StateBroadcast.MembersEntry.value:type_name -> crdt.pb.Participant
	4, // [4:4] is the sub-list for method output_type
	4, // [4:4] is the sub-list for method input_type
	4, // [4:4] is the sub-list for extension type_name
	4, // [4:4] is the sub-list for extension extendee
	0, // [0:4] is the sub-list for field type_name
}

func init() { file_bcast_proto_init() }
func file_bcast_proto_init() {
	if File_bcast_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_bcast_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StateBroadcast); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_bcast_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Participant); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_bcast_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Head); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_bcast_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_bcast_proto_goTypes,
		DependencyIndexes: file_bcast_proto_depIdxs,
		MessageInfos:      file_bcast_proto_msgTypes,
	}.Build()
	File_bcast_proto = out.File
	file_bcast_proto_rawDesc = nil
	file_bcast_proto_goTypes = nil
	file_bcast_proto_depIdxs = nil
}
