package main

import (
	"fmt"
	"time"

	nanoid "github.com/matoous/go-nanoid/v2"
)

type Task struct {
	ID    string
	Topic string

	Payload   []byte
	ExpireAt  time.Time
	CreatedAt time.Time

	Version          int64
	DeliveryAttempts int64

	Priority int32
}

func NewTask(topic, partition string, payload []byte, priority int32, ttlSeconds int64) *Task {
	now := time.Now()

	task := &Task{
		Topic:            topic,
		ID:               genRandomID(),
		Payload:          payload,
		ExpireAt:         now.Add(time.Second * time.Duration(ttlSeconds)),
		CreatedAt:        now,
		Version:          1,
		DeliveryAttempts: 0,
		Priority:         priority,
	}

	return task
}

// The ID generated to route the ack/nack requests back to the partition
func (task *Task) genExternalID(partition string) string {
	// partition#id
	return fmt.Sprintf("%s#%s", partition, genRandomID())
}

func genRandomID() string {
	return nanoid.MustGenerate("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ", 16)
}

func (task *Task) genTimeTreeID(delayTo time.Time) string {
	// UnixMS_TaskID
	return fmt.Sprintf("%d_%s", delayTo.UnixMilli(), task.ID)
}

// When we want to story by priority
func (task *Task) genPriorityTreeID() string {
	// Priority_TaskID
	return fmt.Sprintf("%d_%s", task.Priority, task.ID)
}
