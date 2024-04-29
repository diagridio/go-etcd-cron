//
//Copyright (c) 2024 Diagrid Inc.
//Licensed under the MIT License.

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.33.0
// 	protoc        v4.24.4
// source: proto/job.proto

package api

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	anypb "google.golang.org/protobuf/types/known/anypb"
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

// JobStored is the wrapped stored version of a Job which has a uuid
// associated.
type JobStored struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// uuid is a unique identifier for the job.
	Uuid uint32 `protobuf:"varint,1,opt,name=uuid,proto3" json:"uuid,omitempty"`
	// start is the timestamp the job is sheduled to run.
	Start *timestamppb.Timestamp `protobuf:"bytes,2,opt,name=start,proto3" json:"start,omitempty"`
	// expiration is the optional time at which the job should no longer be
	// scheduled and will be ignored and garbage collected thereafter.
	// A job may be removed earlier if repeats are exhausted or schedule doesn't
	// permit.
	Expiration *timestamppb.Timestamp `protobuf:"bytes,3,opt,name=expiration,proto3,oneof" json:"expiration,omitempty"`
	// job is the job spec.
	Job *Job `protobuf:"bytes,4,opt,name=job,proto3" json:"job,omitempty"`
}

func (x *JobStored) Reset() {
	*x = JobStored{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_job_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *JobStored) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*JobStored) ProtoMessage() {}

func (x *JobStored) ProtoReflect() protoreflect.Message {
	mi := &file_proto_job_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use JobStored.ProtoReflect.Descriptor instead.
func (*JobStored) Descriptor() ([]byte, []int) {
	return file_proto_job_proto_rawDescGZIP(), []int{0}
}

func (x *JobStored) GetUuid() uint32 {
	if x != nil {
		return x.Uuid
	}
	return 0
}

func (x *JobStored) GetStart() *timestamppb.Timestamp {
	if x != nil {
		return x.Start
	}
	return nil
}

func (x *JobStored) GetExpiration() *timestamppb.Timestamp {
	if x != nil {
		return x.Expiration
	}
	return nil
}

func (x *JobStored) GetJob() *Job {
	if x != nil {
		return x.Job
	}
	return nil
}

// Job defines a scheduled rhythmic job stored in the database.
// Job holds the desired spec of the job, not the current trigger state, held
// by Counter.
type Job struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// schedule is an optional schedule at which the job is to be run.
	// Accepts both systemd timer style cron expressions, as well as human
	// readable '@' prefixed period strings as defined below.
	//
	// Systemd timer style cron accepts 6 fields:
	// seconds | minutes | hours | day of month | month        | day of week
	// 0-59    | 0-59    | 0-23  | 1-31         | 1-12/jan-dec | 0-7/sun-sat
	//
	// "0 30 * * * *" - every hour on the half hour
	// "0 15 3 * * *" - every day at 03:15
	//
	// Period string expressions:
	// Entry                  | Description                                | Equivalent To
	// -----                  | -----------                                | -------------
	// @every <duration>      | Run every <duration> (e.g. '@every 1h30m') | N/A
	// @yearly (or @annually) | Run once a year, midnight, Jan. 1st        | 0 0 0 1 1 *
	// @monthly               | Run once a month, midnight, first of month | 0 0 0 1 * *
	// @weekly                | Run once a week, midnight on Sunday        | 0 0 0 * * 0
	// @daily (or @midnight)  | Run once a day, midnight                   | 0 0 0 * * *
	// @hourly                | Run once an hour, beginning of hour        | 0 0 * * * *
	Schedule *string `protobuf:"bytes,1,opt,name=schedule,proto3,oneof" json:"schedule,omitempty"`
	// due_time is the optional time at which the job should be active, or the
	// "one shot" time if other scheduling type fields are not provided.
	// Accepts a "point in time" string in the format of RFC3339, Go duration
	// string (therefore calculated from now), or non-repeating ISO8601.
	DueTime *string `protobuf:"bytes,2,opt,name=due_time,json=dueTime,proto3,oneof" json:"due_time,omitempty"`
	// ttl is the optional time to live or expiration of the job.
	// Accepts a "point in time" string in the format of RFC3339, Go duration
	// string (therefore calculated from now), or non-repeating ISO8601.
	Ttl *string `protobuf:"bytes,3,opt,name=ttl,proto3,oneof" json:"ttl,omitempty"`
	// repeats is the optional number of times in which the job should be
	// triggered. If not set, the job will run indefinitely or until expiration.
	Repeats *uint32 `protobuf:"varint,4,opt,name=repeats,proto3,oneof" json:"repeats,omitempty"`
	// metadata is a arbitrary metadata asociated with the job.
	Metadata *anypb.Any `protobuf:"bytes,5,opt,name=metadata,proto3" json:"metadata,omitempty"`
	// payload is the serialized job payload that will be sent to the recipient
	// when the job is triggered.
	Payload *anypb.Any `protobuf:"bytes,6,opt,name=payload,proto3" json:"payload,omitempty"`
}

func (x *Job) Reset() {
	*x = Job{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_job_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Job) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Job) ProtoMessage() {}

func (x *Job) ProtoReflect() protoreflect.Message {
	mi := &file_proto_job_proto_msgTypes[1]
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
	return file_proto_job_proto_rawDescGZIP(), []int{1}
}

func (x *Job) GetSchedule() string {
	if x != nil && x.Schedule != nil {
		return *x.Schedule
	}
	return ""
}

func (x *Job) GetDueTime() string {
	if x != nil && x.DueTime != nil {
		return *x.DueTime
	}
	return ""
}

func (x *Job) GetTtl() string {
	if x != nil && x.Ttl != nil {
		return *x.Ttl
	}
	return ""
}

func (x *Job) GetRepeats() uint32 {
	if x != nil && x.Repeats != nil {
		return *x.Repeats
	}
	return 0
}

func (x *Job) GetMetadata() *anypb.Any {
	if x != nil {
		return x.Metadata
	}
	return nil
}

func (x *Job) GetPayload() *anypb.Any {
	if x != nil {
		return x.Payload
	}
	return nil
}

var File_proto_job_proto protoreflect.FileDescriptor

var file_proto_job_proto_rawDesc = []byte{
	0x0a, 0x0f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x6a, 0x6f, 0x62, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x12, 0x03, 0x61, 0x70, 0x69, 0x1a, 0x19, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x61, 0x6e, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62,
	0x75, 0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x22, 0xbd, 0x01, 0x0a, 0x09, 0x4a, 0x6f, 0x62, 0x53, 0x74, 0x6f, 0x72, 0x65, 0x64,
	0x12, 0x12, 0x0a, 0x04, 0x75, 0x75, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x04,
	0x75, 0x75, 0x69, 0x64, 0x12, 0x30, 0x0a, 0x05, 0x73, 0x74, 0x61, 0x72, 0x74, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52,
	0x05, 0x73, 0x74, 0x61, 0x72, 0x74, 0x12, 0x3f, 0x0a, 0x0a, 0x65, 0x78, 0x70, 0x69, 0x72, 0x61,
	0x74, 0x69, 0x6f, 0x6e, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f,
	0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d,
	0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x48, 0x00, 0x52, 0x0a, 0x65, 0x78, 0x70, 0x69, 0x72, 0x61,
	0x74, 0x69, 0x6f, 0x6e, 0x88, 0x01, 0x01, 0x12, 0x1a, 0x0a, 0x03, 0x6a, 0x6f, 0x62, 0x18, 0x04,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x08, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x4a, 0x6f, 0x62, 0x52, 0x03,
	0x6a, 0x6f, 0x62, 0x42, 0x0d, 0x0a, 0x0b, 0x5f, 0x65, 0x78, 0x70, 0x69, 0x72, 0x61, 0x74, 0x69,
	0x6f, 0x6e, 0x22, 0x8c, 0x02, 0x0a, 0x03, 0x4a, 0x6f, 0x62, 0x12, 0x1f, 0x0a, 0x08, 0x73, 0x63,
	0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x48, 0x00, 0x52, 0x08,
	0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x88, 0x01, 0x01, 0x12, 0x1e, 0x0a, 0x08, 0x64,
	0x75, 0x65, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x48, 0x01, 0x52,
	0x07, 0x64, 0x75, 0x65, 0x54, 0x69, 0x6d, 0x65, 0x88, 0x01, 0x01, 0x12, 0x15, 0x0a, 0x03, 0x74,
	0x74, 0x6c, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x48, 0x02, 0x52, 0x03, 0x74, 0x74, 0x6c, 0x88,
	0x01, 0x01, 0x12, 0x1d, 0x0a, 0x07, 0x72, 0x65, 0x70, 0x65, 0x61, 0x74, 0x73, 0x18, 0x04, 0x20,
	0x01, 0x28, 0x0d, 0x48, 0x03, 0x52, 0x07, 0x72, 0x65, 0x70, 0x65, 0x61, 0x74, 0x73, 0x88, 0x01,
	0x01, 0x12, 0x30, 0x0a, 0x08, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x18, 0x05, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x14, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x41, 0x6e, 0x79, 0x52, 0x08, 0x6d, 0x65, 0x74, 0x61, 0x64,
	0x61, 0x74, 0x61, 0x12, 0x2e, 0x0a, 0x07, 0x70, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x18, 0x06,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x14, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x41, 0x6e, 0x79, 0x52, 0x07, 0x70, 0x61, 0x79, 0x6c,
	0x6f, 0x61, 0x64, 0x42, 0x0b, 0x0a, 0x09, 0x5f, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65,
	0x42, 0x0b, 0x0a, 0x09, 0x5f, 0x64, 0x75, 0x65, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x42, 0x06, 0x0a,
	0x04, 0x5f, 0x74, 0x74, 0x6c, 0x42, 0x0a, 0x0a, 0x08, 0x5f, 0x72, 0x65, 0x70, 0x65, 0x61, 0x74,
	0x73, 0x42, 0x27, 0x5a, 0x25, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f,
	0x64, 0x69, 0x61, 0x67, 0x72, 0x69, 0x64, 0x69, 0x6f, 0x2f, 0x67, 0x6f, 0x2d, 0x65, 0x74, 0x63,
	0x64, 0x2d, 0x63, 0x72, 0x6f, 0x6e, 0x2f, 0x61, 0x70, 0x69, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x33,
}

var (
	file_proto_job_proto_rawDescOnce sync.Once
	file_proto_job_proto_rawDescData = file_proto_job_proto_rawDesc
)

func file_proto_job_proto_rawDescGZIP() []byte {
	file_proto_job_proto_rawDescOnce.Do(func() {
		file_proto_job_proto_rawDescData = protoimpl.X.CompressGZIP(file_proto_job_proto_rawDescData)
	})
	return file_proto_job_proto_rawDescData
}

var file_proto_job_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_proto_job_proto_goTypes = []interface{}{
	(*JobStored)(nil),             // 0: api.JobStored
	(*Job)(nil),                   // 1: api.Job
	(*timestamppb.Timestamp)(nil), // 2: google.protobuf.Timestamp
	(*anypb.Any)(nil),             // 3: google.protobuf.Any
}
var file_proto_job_proto_depIdxs = []int32{
	2, // 0: api.JobStored.start:type_name -> google.protobuf.Timestamp
	2, // 1: api.JobStored.expiration:type_name -> google.protobuf.Timestamp
	1, // 2: api.JobStored.job:type_name -> api.Job
	3, // 3: api.Job.metadata:type_name -> google.protobuf.Any
	3, // 4: api.Job.payload:type_name -> google.protobuf.Any
	5, // [5:5] is the sub-list for method output_type
	5, // [5:5] is the sub-list for method input_type
	5, // [5:5] is the sub-list for extension type_name
	5, // [5:5] is the sub-list for extension extendee
	0, // [0:5] is the sub-list for field type_name
}

func init() { file_proto_job_proto_init() }
func file_proto_job_proto_init() {
	if File_proto_job_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_proto_job_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*JobStored); i {
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
		file_proto_job_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
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
	file_proto_job_proto_msgTypes[0].OneofWrappers = []interface{}{}
	file_proto_job_proto_msgTypes[1].OneofWrappers = []interface{}{}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_proto_job_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_proto_job_proto_goTypes,
		DependencyIndexes: file_proto_job_proto_depIdxs,
		MessageInfos:      file_proto_job_proto_msgTypes,
	}.Build()
	File_proto_job_proto = out.File
	file_proto_job_proto_rawDesc = nil
	file_proto_job_proto_goTypes = nil
	file_proto_job_proto_depIdxs = nil
}