package main

import (
	"sync"
	"time"

	"github.com/danthegoodman1/UltraQueue/taskdb"
	"github.com/google/btree"
)

type UltraQueue struct {
	Partition string

	TaskDB *taskdb.TaskDB

	topicLengths   map[string]int64
	inFlightTree   *btree.BTree
	inFlightTreeMu *sync.Mutex
	delayTree      *btree.BTree
	delayTreeMu    *sync.Mutex
	inFlightTicker *time.Ticker
	closeChan      chan struct{}
}

func NewUltraQueue(partition string, bufferLen int64) (*UltraQueue, error) {
	// TODO: Initialize taskdb

	uq := &UltraQueue{
		Partition:      partition,
		inFlightTree:   btree.New(3),
		inFlightTreeMu: &sync.Mutex{},
		delayTree:      btree.New(3),
		delayTreeMu:    &sync.Mutex{},
		inFlightTicker: time.NewTicker(time.Millisecond * 5),
		closeChan:      make(chan struct{}),
		topicLengths:   make(map[string]int64),
	}

	return uq, nil
}

func (uq *UltraQueue) Shutdown() {

}

func (uq *UltraQueue) Enqueue(topic string, payload []byte, priority int32, delaySeconds int64) (err error) {
	task := NewTask(topic, uq.Partition, payload, priority, 0) // FIXME: TTL 0

	// TODO: Insert task into DB first

	if delaySeconds > 0 {
		err = uq.enqueueDelayedTask(task, delaySeconds)
	} else {
		err = uq.enqueueTask(task)
	}

	if err != nil {
		// TODO: Increment enqueue metric
	}
	return
}

func (uq *UltraQueue) Dequeue(topic string, numTasks, ttlSeconds int64) (tasks []*Task, err error) {
	// Get numTasks from the topic
	// Increment delivery attempts
	// Insert in-flight task state in DB
	// Add to in-flight tree
	// TODO: Increment dequeue metric
	return
}

func (uq *UltraQueue) Ack(taskID, topic string) (err error) {
	// Check if in the in-flight tree
	// delete task from DB
	// delete task states from DB
	if err != nil {
		// TODO: Increment ack metric
	}
	return
}

func (uq *UltraQueue) Nack(taskID, topic string, delaySeconds int64) (err error) {
	// Check if in in-flight tree
	// insert new task state into DB
	if delaySeconds > 0 {
		// err = uq.enqueueDelayedTask(task)
	} else {
		// err = uq.enqueueTask(task)
	}
	if err != nil {
		// TODO: Increment nack metric
	}
	return
}

func (uq *UltraQueue) enqueueDelayedTask(task *Task, delaySeconds int64) error {
	treeID := task.genTimeTreeID(task.CreatedAt.Add(time.Second * time.Duration(delaySeconds)))
	treeTask := NewInTreeTask(treeID, task)

	// TODO: Add task state to DB

	uq.delayTreeMu.Lock()
	defer uq.delayTreeMu.Unlock()

	uq.delayTree.ReplaceOrInsert(treeTask)
	return nil
}

func (uq *UltraQueue) enqueueTask(task *Task) error {
	// TODO: Add task state to DB

	// Add to topic outbox tree
	// treeID := task.genPriorityTreeID()

	return nil
}
