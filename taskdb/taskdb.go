package taskdb

import "time"

type TaskDB interface {
	// Returns a channel that will be written to when the TaskDB is ready, and the UltraQueue should start accepting requests
	Attach() (chan struct{}, error)

	// A Task will be inserted into the task table, and its first state inserted
	Enqueue(state *TaskDBTaskState, payload []byte) error

	// A new task state
	PutState(state *TaskDBTaskState) error

	// Retrieves the payload for a given task
	GetPayload(topicName, taskID string) ([]byte, error)

	// Deletes all task states for a topic, and removes the topic from the task table. If no more tasks exist then the task will be removed from the task table
	Delete(topicName, taskID string) error

	// Returns an interator that will read all payloads from the DB, so they can be drained into other partitions
	Drain()
}

type TaskDBTaskState struct {
	Topic     string
	Partition string

	ID               string
	State            TaskState
	Version          int
	DeliveryAttempts int
	CreatedAt        time.Time
	Priority         int
}

func NewTaskDBTaskState(partition, topicName, taskID string, state TaskState, version, deliveryAttempts, priority int, createdAt time.Time) *TaskDBTaskState {
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
