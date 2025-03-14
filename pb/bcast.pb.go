// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.35.1
// 	protoc        v5.28.3
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

type CRDTBroadcast struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Heads []*Head `protobuf:"bytes,1,rep,name=Heads,proto3" json:"Heads,omitempty"` // A list of heads
}

func (x *CRDTBroadcast) Reset() {
	*x = CRDTBroadcast{}
	mi := &file_bcast_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CRDTBroadcast) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CRDTBroadcast) ProtoMessage() {}

func (x *CRDTBroadcast) ProtoReflect() protoreflect.Message {
	mi := &file_bcast_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CRDTBroadcast.ProtoReflect.Descriptor instead.
func (*CRDTBroadcast) Descriptor() ([]byte, []int) {
	return file_bcast_proto_rawDescGZIP(), []int{0}
}

func (x *CRDTBroadcast) GetHeads() []*Head {
	if x != nil {
		return x.Heads
	}
	return nil
}

type Head struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Cid []byte `protobuf:"bytes,1,opt,name=Cid,proto3" json:"Cid,omitempty"`
}

func (x *Head) Reset() {
	*x = Head{}
	mi := &file_bcast_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Head) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Head) ProtoMessage() {}

func (x *Head) ProtoReflect() protoreflect.Message {
	mi := &file_bcast_proto_msgTypes[1]
	if x != nil {
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
	return file_bcast_proto_rawDescGZIP(), []int{1}
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
	0x72, 0x64, 0x74, 0x2e, 0x70, 0x62, 0x22, 0x34, 0x0a, 0x0d, 0x43, 0x52, 0x44, 0x54, 0x42, 0x72,
	0x6f, 0x61, 0x64, 0x63, 0x61, 0x73, 0x74, 0x12, 0x23, 0x0a, 0x05, 0x48, 0x65, 0x61, 0x64, 0x73,
	0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0d, 0x2e, 0x63, 0x72, 0x64, 0x74, 0x2e, 0x70, 0x62,
	0x2e, 0x48, 0x65, 0x61, 0x64, 0x52, 0x05, 0x48, 0x65, 0x61, 0x64, 0x73, 0x22, 0x18, 0x0a, 0x04,
	0x48, 0x65, 0x61, 0x64, 0x12, 0x10, 0x0a, 0x03, 0x43, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x0c, 0x52, 0x03, 0x43, 0x69, 0x64, 0x42, 0x06, 0x5a, 0x04, 0x2e, 0x3b, 0x70, 0x62, 0x62, 0x06,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
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

var file_bcast_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_bcast_proto_goTypes = []any{
	(*CRDTBroadcast)(nil), // 0: crdt.pb.CRDTBroadcast
	(*Head)(nil),          // 1: crdt.pb.Head
}
var file_bcast_proto_depIdxs = []int32{
	1, // 0: crdt.pb.CRDTBroadcast.Heads:type_name -> crdt.pb.Head
	1, // [1:1] is the sub-list for method output_type
	1, // [1:1] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_bcast_proto_init() }
func file_bcast_proto_init() {
	if File_bcast_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_bcast_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
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
