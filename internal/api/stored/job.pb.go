//
//Copyright (c) 2024 Diagrid Inc.
//Licensed under the MIT License.

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.32.0
// 	protoc        v4.25.4
// source: proto/stored/job.proto

package stored

import (
	api "github.com/diagridio/go-etcd-cron/api"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// Job is the wrapped stored version of a Job which has a partition_id
// associated.
type Job struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// partion_id is an identifier for the job, used for distinguishing jobs with
	// the same name and assigning the job to a partition.
	// Doesn't need to be globally unique.
	PartitionId uint32 `protobuf:"varint,1,opt,name=partition_id,json=partitionId,proto3" json:"partition_id,omitempty"`
	// begin is the beginning time of the job.
	//
	// Types that are assignable to Begin:
	//
	//	*Job_Start
	//	*Job_DueTime
	Begin isJob_Begin `protobuf_oneof:"begin"`
	// expiration is the optional time at which the job should no longer be
	// scheduled and will be ignored and garbage collected thereafter.
	// A job may be removed earlier if repeats are exhausted or schedule doesn't
	// permit.
	Expiration *timestamppb.Timestamp `protobuf:"bytes,4,opt,name=expiration,proto3,oneof" json:"expiration,omitempty"`
	// job is the job spec.
	Job *api.Job `protobuf:"bytes,5,opt,name=job,proto3" json:"job,omitempty"`
}

func (x *Job) Reset() {
	*x = Job{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_stored_job_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Job) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Job) ProtoMessage() {}

func (x *Job) ProtoReflect() protoreflect.Message {
	mi := &file_proto_stored_job_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Job.ProtoReflect.Descriptor instead.
func (*Job) Descriptor() ([]byte, []int) {
	return file_proto_stored_job_proto_rawDescGZIP(), []int{0}
}

func (x *Job) GetPartitionId() uint32 {
	if x != nil {
		return x.PartitionId
	}
	return 0
}

func (m *Job) GetBegin() isJob_Begin {
	if m != nil {
		return m.Begin
	}
	return nil
}

func (x *Job) GetStart() *timestamppb.Timestamp {
	if x, ok := x.GetBegin().(*Job_Start); ok {
		return x.Start
	}
	return nil
}

func (x *Job) GetDueTime() *timestamppb.Timestamp {
	if x, ok := x.GetBegin().(*Job_DueTime); ok {
		return x.DueTime
	}
	return nil
}

func (x *Job) GetExpiration() *timestamppb.Timestamp {
	if x != nil {
		return x.Expiration
	}
	return nil
}

func (x *Job) GetJob() *api.Job {
	if x != nil {
		return x.Job
	}
	return nil
}

type isJob_Begin interface {
	isJob_Begin()
}

type Job_Start struct {
	// start is the epoch time of the job whereby the clock starts on the
	// schedule. The job _will not_ trigger at this time.
	Start *timestamppb.Timestamp `protobuf:"bytes,2,opt,name=start,proto3,oneof"`
}

type Job_DueTime struct {
	// due_time is the epoch time of the job whereby the clock starts on the
	// schedule. The job _will_ trigger at this time.
	DueTime *timestamppb.Timestamp `protobuf:"bytes,3,opt,name=due_time,json=dueTime,proto3,oneof"`
}

func (*Job_Start) isJob_Begin() {}

func (*Job_DueTime) isJob_Begin() {}

var File_proto_stored_job_proto protoreflect.FileDescriptor

var file_proto_stored_job_proto_rawDesc = []byte{
	0x0a, 0x16, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x64, 0x2f, 0x6a,
	0x6f, 0x62, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x06, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x64,
	0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x1a, 0x13, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x6a, 0x6f, 0x62,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x8a, 0x02, 0x0a, 0x03, 0x4a, 0x6f, 0x62, 0x12, 0x21,
	0x0a, 0x0c, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x0d, 0x52, 0x0b, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x49,
	0x64, 0x12, 0x32, 0x0a, 0x05, 0x73, 0x74, 0x61, 0x72, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62,
	0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x48, 0x00, 0x52, 0x05,
	0x73, 0x74, 0x61, 0x72, 0x74, 0x12, 0x37, 0x0a, 0x08, 0x64, 0x75, 0x65, 0x5f, 0x74, 0x69, 0x6d,
	0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74,
	0x61, 0x6d, 0x70, 0x48, 0x00, 0x52, 0x07, 0x64, 0x75, 0x65, 0x54, 0x69, 0x6d, 0x65, 0x12, 0x3f,
	0x0a, 0x0a, 0x65, 0x78, 0x70, 0x69, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x04, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x48, 0x01,
	0x52, 0x0a, 0x65, 0x78, 0x70, 0x69, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x88, 0x01, 0x01, 0x12,
	0x1a, 0x0a, 0x03, 0x6a, 0x6f, 0x62, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x08, 0x2e, 0x61,
	0x70, 0x69, 0x2e, 0x4a, 0x6f, 0x62, 0x52, 0x03, 0x6a, 0x6f, 0x62, 0x42, 0x07, 0x0a, 0x05, 0x62,
	0x65, 0x67, 0x69, 0x6e, 0x42, 0x0d, 0x0a, 0x0b, 0x5f, 0x65, 0x78, 0x70, 0x69, 0x72, 0x61, 0x74,
	0x69, 0x6f, 0x6e, 0x42, 0x37, 0x5a, 0x35, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f,
	0x6d, 0x2f, 0x64, 0x69, 0x61, 0x67, 0x72, 0x69, 0x64, 0x69, 0x6f, 0x2f, 0x67, 0x6f, 0x2d, 0x65,
	0x74, 0x63, 0x64, 0x2d, 0x63, 0x72, 0x6f, 0x6e, 0x2f, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61,
	0x6c, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x64, 0x62, 0x06, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_proto_stored_job_proto_rawDescOnce sync.Once
	file_proto_stored_job_proto_rawDescData = file_proto_stored_job_proto_rawDesc
)

func file_proto_stored_job_proto_rawDescGZIP() []byte {
	file_proto_stored_job_proto_rawDescOnce.Do(func() {
		file_proto_stored_job_proto_rawDescData = protoimpl.X.CompressGZIP(file_proto_stored_job_proto_rawDescData)
	})
	return file_proto_stored_job_proto_rawDescData
}

var file_proto_stored_job_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_proto_stored_job_proto_goTypes = []interface{}{
	(*Job)(nil),                   // 0: stored.Job
	(*timestamppb.Timestamp)(nil), // 1: google.protobuf.Timestamp
	(*api.Job)(nil),               // 2: api.Job
}
var file_proto_stored_job_proto_depIdxs = []int32{
	1, // 0: stored.Job.start:type_name -> google.protobuf.Timestamp
	1, // 1: stored.Job.due_time:type_name -> google.protobuf.Timestamp
	1, // 2: stored.Job.expiration:type_name -> google.protobuf.Timestamp
	2, // 3: stored.Job.job:type_name -> api.Job
	4, // [4:4] is the sub-list for method output_type
	4, // [4:4] is the sub-list for method input_type
	4, // [4:4] is the sub-list for extension type_name
	4, // [4:4] is the sub-list for extension extendee
	0, // [0:4] is the sub-list for field type_name
}

func init() { file_proto_stored_job_proto_init() }
func file_proto_stored_job_proto_init() {
	if File_proto_stored_job_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_proto_stored_job_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Job); i {
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
	file_proto_stored_job_proto_msgTypes[0].OneofWrappers = []interface{}{
		(*Job_Start)(nil),
		(*Job_DueTime)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_proto_stored_job_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_proto_stored_job_proto_goTypes,
		DependencyIndexes: file_proto_stored_job_proto_depIdxs,
		MessageInfos:      file_proto_stored_job_proto_msgTypes,
	}.Build()
	File_proto_stored_job_proto = out.File
	file_proto_stored_job_proto_rawDesc = nil
	file_proto_stored_job_proto_goTypes = nil
	file_proto_stored_job_proto_depIdxs = nil
}
