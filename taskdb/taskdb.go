package taskdb

import "time"

type TaskDB interface {
	// Acquires the TaskDB lock as needed, then returns an AttachIterator
	Attach() (AttachIterator, error)

	// A Task will be inserted into the task table, and its first state inserted
	Enqueue(state *TaskDBTaskState, payload []byte) error

	// A new task state
	PutState(state *TaskDBTaskState) error

	// Retrieves the payload for a given task
	GetPayload(topicName, taskID string) ([]byte, error)

	// Deletes all task states for a topic, and removes the topic from the task table. If no more tasks exist then the task will be removed from the task table
	Delete(topicName, taskID string) error

	// Returns an interator that will read all payloads from the DB, so they can be drained into other partitions
	Drain() (DrainIterator, error)
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

type AttachIterator interface {
	// Returns an array of task states. Returns nil when there are no more
	Next() ([]*TaskDBTaskState, error)
}

type DrainTask struct {
	Topic   string
	ID      string
	Payload []byte
}

type DrainIterator interface {
	// Returns an array of task payloads that can be joined to their current states, and sent to other partitions. Returns nil when there are no more
	Next() ([]*DrainTask, error)
}
