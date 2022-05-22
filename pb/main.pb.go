// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.0
// 	protoc        v3.15.8
// source: pb/main.proto

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

type Applied struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Applied bool `protobuf:"varint,1,opt,name=applied,proto3" json:"applied,omitempty"`
}

func (x *Applied) Reset() {
	*x = Applied{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pb_main_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Applied) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Applied) ProtoMessage() {}

func (x *Applied) ProtoReflect() protoreflect.Message {
	mi := &file_pb_main_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Applied.ProtoReflect.Descriptor instead.
func (*Applied) Descriptor() ([]byte, []int) {
	return file_pb_main_proto_rawDescGZIP(), []int{0}
}

func (x *Applied) GetApplied() bool {
	if x != nil {
		return x.Applied
	}
	return false
}

type AckRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TaskID string `protobuf:"bytes,1,opt,name=TaskID,proto3" json:"TaskID,omitempty"`
}

func (x *AckRequest) Reset() {
	*x = AckRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pb_main_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AckRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AckRequest) ProtoMessage() {}

func (x *AckRequest) ProtoReflect() protoreflect.Message {
	mi := &file_pb_main_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AckRequest.ProtoReflect.Descriptor instead.
func (*AckRequest) Descriptor() ([]byte, []int) {
	return file_pb_main_proto_rawDescGZIP(), []int{1}
}

func (x *AckRequest) GetTaskID() string {
	if x != nil {
		return x.TaskID
	}
	return ""
}

type NackRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TaskID       string `protobuf:"bytes,1,opt,name=TaskID,proto3" json:"TaskID,omitempty"`
	DelaySeconds int32  `protobuf:"zigzag32,2,opt,name=DelaySeconds,proto3" json:"DelaySeconds,omitempty"`
}

func (x *NackRequest) Reset() {
	*x = NackRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pb_main_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *NackRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NackRequest) ProtoMessage() {}

func (x *NackRequest) ProtoReflect() protoreflect.Message {
	mi := &file_pb_main_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NackRequest.ProtoReflect.Descriptor instead.
func (*NackRequest) Descriptor() ([]byte, []int) {
	return file_pb_main_proto_rawDescGZIP(), []int{2}
}

func (x *NackRequest) GetTaskID() string {
	if x != nil {
		return x.TaskID
	}
	return ""
}

func (x *NackRequest) GetDelaySeconds() int32 {
	if x != nil {
		return x.DelaySeconds
	}
	return 0
}

type DequeueRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Topic              string `protobuf:"bytes,1,opt,name=Topic,proto3" json:"Topic,omitempty"`
	InFlightTTLSeconds int32  `protobuf:"zigzag32,2,opt,name=InFlightTTLSeconds,proto3" json:"InFlightTTLSeconds,omitempty"`
	Tasks              int32  `protobuf:"zigzag32,3,opt,name=Tasks,proto3" json:"Tasks,omitempty"`
}

func (x *DequeueRequest) Reset() {
	*x = DequeueRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pb_main_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DequeueRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DequeueRequest) ProtoMessage() {}

func (x *DequeueRequest) ProtoReflect() protoreflect.Message {
	mi := &file_pb_main_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DequeueRequest.ProtoReflect.Descriptor instead.
func (*DequeueRequest) Descriptor() ([]byte, []int) {
	return file_pb_main_proto_rawDescGZIP(), []int{3}
}

func (x *DequeueRequest) GetTopic() string {
	if x != nil {
		return x.Topic
	}
	return ""
}

func (x *DequeueRequest) GetInFlightTTLSeconds() int32 {
	if x != nil {
		return x.InFlightTTLSeconds
	}
	return 0
}

func (x *DequeueRequest) GetTasks() int32 {
	if x != nil {
		return x.Tasks
	}
	return 0
}

type TaskResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Tasks []*TreeTask `protobuf:"bytes,1,rep,name=Tasks,proto3" json:"Tasks,omitempty"`
}

func (x *TaskResponse) Reset() {
	*x = TaskResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pb_main_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TaskResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TaskResponse) ProtoMessage() {}

func (x *TaskResponse) ProtoReflect() protoreflect.Message {
	mi := &file_pb_main_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TaskResponse.ProtoReflect.Descriptor instead.
func (*TaskResponse) Descriptor() ([]byte, []int) {
	return file_pb_main_proto_rawDescGZIP(), []int{4}
}

func (x *TaskResponse) GetTasks() []*TreeTask {
	if x != nil {
		return x.Tasks
	}
	return nil
}

type TreeTask struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ID   string `protobuf:"bytes,1,opt,name=ID,proto3" json:"ID,omitempty"`
	Task *Task  `protobuf:"bytes,2,opt,name=Task,proto3" json:"Task,omitempty"`
}

func (x *TreeTask) Reset() {
	*x = TreeTask{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pb_main_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TreeTask) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TreeTask) ProtoMessage() {}

func (x *TreeTask) ProtoReflect() protoreflect.Message {
	mi := &file_pb_main_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TreeTask.ProtoReflect.Descriptor instead.
func (*TreeTask) Descriptor() ([]byte, []int) {
	return file_pb_main_proto_rawDescGZIP(), []int{5}
}

func (x *TreeTask) GetID() string {
	if x != nil {
		return x.ID
	}
	return ""
}

func (x *TreeTask) GetTask() *Task {
	if x != nil {
		return x.Task
	}
	return nil
}

type Task struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ID               string `protobuf:"bytes,1,opt,name=ID,proto3" json:"ID,omitempty"`
	Topic            string `protobuf:"bytes,2,opt,name=Topic,proto3" json:"Topic,omitempty"`
	Payload          []byte `protobuf:"bytes,3,opt,name=Payload,proto3" json:"Payload,omitempty"`
	CreatedAt        string `protobuf:"bytes,4,opt,name=CreatedAt,proto3" json:"CreatedAt,omitempty"`
	Version          int32  `protobuf:"zigzag32,5,opt,name=Version,proto3" json:"Version,omitempty"`
	DeliveryAttempts int32  `protobuf:"zigzag32,6,opt,name=DeliveryAttempts,proto3" json:"DeliveryAttempts,omitempty"`
	Priority         int32  `protobuf:"zigzag32,7,opt,name=Priority,proto3" json:"Priority,omitempty"`
}

func (x *Task) Reset() {
	*x = Task{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pb_main_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Task) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Task) ProtoMessage() {}

func (x *Task) ProtoReflect() protoreflect.Message {
	mi := &file_pb_main_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Task.ProtoReflect.Descriptor instead.
func (*Task) Descriptor() ([]byte, []int) {
	return file_pb_main_proto_rawDescGZIP(), []int{6}
}

func (x *Task) GetID() string {
	if x != nil {
		return x.ID
	}
	return ""
}

func (x *Task) GetTopic() string {
	if x != nil {
		return x.Topic
	}
	return ""
}

func (x *Task) GetPayload() []byte {
	if x != nil {
		return x.Payload
	}
	return nil
}

func (x *Task) GetCreatedAt() string {
	if x != nil {
		return x.CreatedAt
	}
	return ""
}

func (x *Task) GetVersion() int32 {
	if x != nil {
		return x.Version
	}
	return 0
}

func (x *Task) GetDeliveryAttempts() int32 {
	if x != nil {
		return x.DeliveryAttempts
	}
	return 0
}

func (x *Task) GetPriority() int32 {
	if x != nil {
		return x.Priority
	}
	return 0
}

var File_pb_main_proto protoreflect.FileDescriptor

var file_pb_main_proto_rawDesc = []byte{
	0x0a, 0x0d, 0x70, 0x62, 0x2f, 0x6d, 0x61, 0x69, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22,
	0x23, 0x0a, 0x07, 0x41, 0x70, 0x70, 0x6c, 0x69, 0x65, 0x64, 0x12, 0x18, 0x0a, 0x07, 0x61, 0x70,
	0x70, 0x6c, 0x69, 0x65, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x08, 0x52, 0x07, 0x61, 0x70, 0x70,
	0x6c, 0x69, 0x65, 0x64, 0x22, 0x24, 0x0a, 0x0a, 0x41, 0x63, 0x6b, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x12, 0x16, 0x0a, 0x06, 0x54, 0x61, 0x73, 0x6b, 0x49, 0x44, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x06, 0x54, 0x61, 0x73, 0x6b, 0x49, 0x44, 0x22, 0x49, 0x0a, 0x0b, 0x4e, 0x61,
	0x63, 0x6b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x16, 0x0a, 0x06, 0x54, 0x61, 0x73,
	0x6b, 0x49, 0x44, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x54, 0x61, 0x73, 0x6b, 0x49,
	0x44, 0x12, 0x22, 0x0a, 0x0c, 0x44, 0x65, 0x6c, 0x61, 0x79, 0x53, 0x65, 0x63, 0x6f, 0x6e, 0x64,
	0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x11, 0x52, 0x0c, 0x44, 0x65, 0x6c, 0x61, 0x79, 0x53, 0x65,
	0x63, 0x6f, 0x6e, 0x64, 0x73, 0x22, 0x6c, 0x0a, 0x0e, 0x44, 0x65, 0x71, 0x75, 0x65, 0x75, 0x65,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x54, 0x6f, 0x70, 0x69, 0x63,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x54, 0x6f, 0x70, 0x69, 0x63, 0x12, 0x2e, 0x0a,
	0x12, 0x49, 0x6e, 0x46, 0x6c, 0x69, 0x67, 0x68, 0x74, 0x54, 0x54, 0x4c, 0x53, 0x65, 0x63, 0x6f,
	0x6e, 0x64, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x11, 0x52, 0x12, 0x49, 0x6e, 0x46, 0x6c, 0x69,
	0x67, 0x68, 0x74, 0x54, 0x54, 0x4c, 0x53, 0x65, 0x63, 0x6f, 0x6e, 0x64, 0x73, 0x12, 0x14, 0x0a,
	0x05, 0x54, 0x61, 0x73, 0x6b, 0x73, 0x18, 0x03, 0x20, 0x01, 0x28, 0x11, 0x52, 0x05, 0x54, 0x61,
	0x73, 0x6b, 0x73, 0x22, 0x2f, 0x0a, 0x0c, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x12, 0x1f, 0x0a, 0x05, 0x54, 0x61, 0x73, 0x6b, 0x73, 0x18, 0x01, 0x20, 0x03,
	0x28, 0x0b, 0x32, 0x09, 0x2e, 0x54, 0x72, 0x65, 0x65, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x05, 0x54,
	0x61, 0x73, 0x6b, 0x73, 0x22, 0x35, 0x0a, 0x08, 0x54, 0x72, 0x65, 0x65, 0x54, 0x61, 0x73, 0x6b,
	0x12, 0x0e, 0x0a, 0x02, 0x49, 0x44, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x49, 0x44,
	0x12, 0x19, 0x0a, 0x04, 0x54, 0x61, 0x73, 0x6b, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x05,
	0x2e, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x04, 0x54, 0x61, 0x73, 0x6b, 0x22, 0xc6, 0x01, 0x0a, 0x04,
	0x54, 0x61, 0x73, 0x6b, 0x12, 0x0e, 0x0a, 0x02, 0x49, 0x44, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x02, 0x49, 0x44, 0x12, 0x14, 0x0a, 0x05, 0x54, 0x6f, 0x70, 0x69, 0x63, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x05, 0x54, 0x6f, 0x70, 0x69, 0x63, 0x12, 0x18, 0x0a, 0x07, 0x50, 0x61,
	0x79, 0x6c, 0x6f, 0x61, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x07, 0x50, 0x61, 0x79,
	0x6c, 0x6f, 0x61, 0x64, 0x12, 0x1c, 0x0a, 0x09, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x64, 0x41,
	0x74, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x64,
	0x41, 0x74, 0x12, 0x18, 0x0a, 0x07, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x05, 0x20,
	0x01, 0x28, 0x11, 0x52, 0x07, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x2a, 0x0a, 0x10,
	0x44, 0x65, 0x6c, 0x69, 0x76, 0x65, 0x72, 0x79, 0x41, 0x74, 0x74, 0x65, 0x6d, 0x70, 0x74, 0x73,
	0x18, 0x06, 0x20, 0x01, 0x28, 0x11, 0x52, 0x10, 0x44, 0x65, 0x6c, 0x69, 0x76, 0x65, 0x72, 0x79,
	0x41, 0x74, 0x74, 0x65, 0x6d, 0x70, 0x74, 0x73, 0x12, 0x1a, 0x0a, 0x08, 0x50, 0x72, 0x69, 0x6f,
	0x72, 0x69, 0x74, 0x79, 0x18, 0x07, 0x20, 0x01, 0x28, 0x11, 0x52, 0x08, 0x50, 0x72, 0x69, 0x6f,
	0x72, 0x69, 0x74, 0x79, 0x32, 0x7d, 0x0a, 0x12, 0x55, 0x6c, 0x74, 0x72, 0x61, 0x51, 0x75, 0x65,
	0x75, 0x65, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x12, 0x29, 0x0a, 0x07, 0x44, 0x65,
	0x71, 0x75, 0x65, 0x75, 0x65, 0x12, 0x0f, 0x2e, 0x44, 0x65, 0x71, 0x75, 0x65, 0x75, 0x65, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x0d, 0x2e, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x1c, 0x0a, 0x03, 0x41, 0x63, 0x6b, 0x12, 0x0b, 0x2e, 0x41,
	0x63, 0x6b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x08, 0x2e, 0x41, 0x70, 0x70, 0x6c,
	0x69, 0x65, 0x64, 0x12, 0x1e, 0x0a, 0x04, 0x4e, 0x61, 0x63, 0x6b, 0x12, 0x0c, 0x2e, 0x4e, 0x61,
	0x63, 0x6b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x08, 0x2e, 0x41, 0x70, 0x70, 0x6c,
	0x69, 0x65, 0x64, 0x42, 0x06, 0x5a, 0x04, 0x2e, 0x2f, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x33,
}

var (
	file_pb_main_proto_rawDescOnce sync.Once
	file_pb_main_proto_rawDescData = file_pb_main_proto_rawDesc
)

func file_pb_main_proto_rawDescGZIP() []byte {
	file_pb_main_proto_rawDescOnce.Do(func() {
		file_pb_main_proto_rawDescData = protoimpl.X.CompressGZIP(file_pb_main_proto_rawDescData)
	})
	return file_pb_main_proto_rawDescData
}

var file_pb_main_proto_msgTypes = make([]protoimpl.MessageInfo, 7)
var file_pb_main_proto_goTypes = []interface{}{
	(*Applied)(nil),        // 0: Applied
	(*AckRequest)(nil),     // 1: AckRequest
	(*NackRequest)(nil),    // 2: NackRequest
	(*DequeueRequest)(nil), // 3: DequeueRequest
	(*TaskResponse)(nil),   // 4: TaskResponse
	(*TreeTask)(nil),       // 5: TreeTask
	(*Task)(nil),           // 6: Task
}
var file_pb_main_proto_depIdxs = []int32{
	5, // 0: TaskResponse.Tasks:type_name -> TreeTask
	6, // 1: TreeTask.Task:type_name -> Task
	3, // 2: UltraQueueInternal.Dequeue:input_type -> DequeueRequest
	1, // 3: UltraQueueInternal.Ack:input_type -> AckRequest
	2, // 4: UltraQueueInternal.Nack:input_type -> NackRequest
	4, // 5: UltraQueueInternal.Dequeue:output_type -> TaskResponse
	0, // 6: UltraQueueInternal.Ack:output_type -> Applied
	0, // 7: UltraQueueInternal.Nack:output_type -> Applied
	5, // [5:8] is the sub-list for method output_type
	2, // [2:5] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_pb_main_proto_init() }
func file_pb_main_proto_init() {
	if File_pb_main_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_pb_main_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Applied); i {
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
		file_pb_main_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AckRequest); i {
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
		file_pb_main_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*NackRequest); i {
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
		file_pb_main_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DequeueRequest); i {
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
		file_pb_main_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TaskResponse); i {
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
		file_pb_main_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TreeTask); i {
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
		file_pb_main_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Task); i {
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
			RawDescriptor: file_pb_main_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   7,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_pb_main_proto_goTypes,
		DependencyIndexes: file_pb_main_proto_depIdxs,
		MessageInfos:      file_pb_main_proto_msgTypes,
	}.Build()
	File_pb_main_proto = out.File
	file_pb_main_proto_rawDesc = nil
	file_pb_main_proto_goTypes = nil
	file_pb_main_proto_depIdxs = nil
}
