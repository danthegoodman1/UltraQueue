package taskdb

import (
	"errors"
	"time"
)

var (
	ErrAttachEnded = errors.New("attach ended")
)

type TaskDB interface {
	// Acquires the TaskDB lock as needed, then returns an AttachIterator
	Attach() AttachIterator

	// A Task will be inserted into the task table, and its first state inserted
	PutPayload(topicName, taskID string, payload string) WriteResult

	// A new task state
	PutState(state *TaskDBTaskState) WriteResult

	// Retrieves the payload for a given task
	GetPayload(topicName, taskID string) (string, error)

	// Deletes all task states for a topic, and removes the topic from the task table. If no more tasks exist then the task will be removed from the task table
	Delete(topicName, taskID string) WriteResult

	// Returns an iterator that will read all payloads from the DB, so they can be drained into other partitions
	Drain() DrainIterator
}

type TaskDBTaskState struct {
	Topic     string
	Partition string

	ID               string
	State            TaskState
	Version          int32
	DeliveryAttempts int32
	CreatedAt        time.Time
	Priority         int32
}

func NewTaskDBTaskState(partition, topicName, taskID string, state TaskState, version, deliveryAttempts, priority int32, createdAt time.Time) *TaskDBTaskState {
	return &TaskDBTaskState{
		Topic:            topicName,
		Partition:        partition,
		ID:               taskID,
		State:            state,
		Version:          version,
		DeliveryAttempts: deliveryAttempts,
		Priority:         priority,
		CreatedAt:        createdAt,
	}
}

type AttachIterator interface {
	// Returns an array of task states. Returns nil when there are no more
	Next() ([]*TaskDBTaskState, error)
}

type DrainTask struct {
	Topic   string
	ID      string
	Payload string
}

type DrainIterator interface {
	// Returns an array of task payloads that can be joined to their current states, and sent to other partitions. Returns nil when there are no more
	Next() ([]*DrainTask, error)
}

type WriteResult interface {
	// Waits for the write to be committed to the TaskDB.
	// Should use a buffered channel to wait for a batch to be committed.
	// If the TaskDB does not batch it should still immediately write to
	// the buffered channel so that function calls are non-blocking and
	// return immediately
	Get() error
}
