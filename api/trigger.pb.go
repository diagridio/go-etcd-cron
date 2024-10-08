//
//Copyright (c) 2024 Diagrid Inc.
//Licensed under the MIT License.

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.33.0
// 	protoc        v4.25.4
// source: proto/trigger.proto

package api

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	anypb "google.golang.org/protobuf/types/known/anypb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// TriggerRequest is the request sent to the caller when a job is triggered.
type TriggerRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// name is the name of the job that was triggered.
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// metadata is the arbitrary metadata associated with the job.
	Metadata *anypb.Any `protobuf:"bytes,2,opt,name=metadata,proto3" json:"metadata,omitempty"`
	// payload is the job payload.
	Payload *anypb.Any `protobuf:"bytes,3,opt,name=payload,proto3" json:"payload,omitempty"`
}

func (x *TriggerRequest) Reset() {
	*x = TriggerRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_trigger_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TriggerRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TriggerRequest) ProtoMessage() {}

func (x *TriggerRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_trigger_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TriggerRequest.ProtoReflect.Descriptor instead.
func (*TriggerRequest) Descriptor() ([]byte, []int) {
	return file_proto_trigger_proto_rawDescGZIP(), []int{0}
}

func (x *TriggerRequest) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *TriggerRequest) GetMetadata() *anypb.Any {
	if x != nil {
		return x.Metadata
	}
	return nil
}

func (x *TriggerRequest) GetPayload() *anypb.Any {
	if x != nil {
		return x.Payload
	}
	return nil
}

var File_proto_trigger_proto protoreflect.FileDescriptor

var file_proto_trigger_proto_rawDesc = []byte{
	0x0a, 0x13, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x74, 0x72, 0x69, 0x67, 0x67, 0x65, 0x72, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x03, 0x61, 0x70, 0x69, 0x1a, 0x19, 0x67, 0x6f, 0x6f, 0x67,
	0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x61, 0x6e, 0x79, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x86, 0x01, 0x0a, 0x0e, 0x54, 0x72, 0x69, 0x67, 0x67, 0x65,
	0x72, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x30, 0x0a, 0x08,
	0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x14,
	0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66,
	0x2e, 0x41, 0x6e, 0x79, 0x52, 0x08, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x12, 0x2e,
	0x0a, 0x07, 0x70, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x14, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2e, 0x41, 0x6e, 0x79, 0x52, 0x07, 0x70, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x42, 0x27,
	0x5a, 0x25, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x64, 0x69, 0x61,
	0x67, 0x72, 0x69, 0x64, 0x69, 0x6f, 0x2f, 0x67, 0x6f, 0x2d, 0x65, 0x74, 0x63, 0x64, 0x2d, 0x63,
	0x72, 0x6f, 0x6e, 0x2f, 0x61, 0x70, 0x69, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_proto_trigger_proto_rawDescOnce sync.Once
	file_proto_trigger_proto_rawDescData = file_proto_trigger_proto_rawDesc
)

func file_proto_trigger_proto_rawDescGZIP() []byte {
	file_proto_trigger_proto_rawDescOnce.Do(func() {
		file_proto_trigger_proto_rawDescData = protoimpl.X.CompressGZIP(file_proto_trigger_proto_rawDescData)
	})
	return file_proto_trigger_proto_rawDescData
}

var file_proto_trigger_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_proto_trigger_proto_goTypes = []interface{}{
	(*TriggerRequest)(nil), // 0: api.TriggerRequest
	(*anypb.Any)(nil),      // 1: google.protobuf.Any
}
var file_proto_trigger_proto_depIdxs = []int32{
	1, // 0: api.TriggerRequest.metadata:type_name -> google.protobuf.Any
	1, // 1: api.TriggerRequest.payload:type_name -> google.protobuf.Any
	2, // [2:2] is the sub-list for method output_type
	2, // [2:2] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_proto_trigger_proto_init() }
func file_proto_trigger_proto_init() {
	if File_proto_trigger_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_proto_trigger_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TriggerRequest); i {
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
			RawDescriptor: file_proto_trigger_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_proto_trigger_proto_goTypes,
		DependencyIndexes: file_proto_trigger_proto_depIdxs,
		MessageInfos:      file_proto_trigger_proto_msgTypes,
	}.Build()
	File_proto_trigger_proto = out.File
	file_proto_trigger_proto_rawDesc = nil
	file_proto_trigger_proto_goTypes = nil
	file_proto_trigger_proto_depIdxs = nil
}
