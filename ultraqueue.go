package main

import (
	"sync"
	"time"

	"github.com/danthegoodman1/UltraQueue/taskdb"
	"github.com/google/btree"
	"github.com/rs/zerolog/log"
)

type UltraQueue struct {
	Partition string

	TaskDB  *taskdb.TaskDB
	topics  map[string]*Topic
	topicMu *sync.RWMutex

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
		topics:         make(map[string]*Topic),
		topicMu:        &sync.RWMutex{},
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
	log.Debug().Str("partition", uq.Partition).Msg("Enqueuing topic")
	// TODO: Add task state to DB

	// Add to topic outbox tree
	topic := uq.getSafeTopic(task.Topic)
	if topic == nil {
		topic = uq.putSafeTopic(task.Topic)
	}

	treeID := task.genPriorityTreeID()
	topic.Enqueue(&InTreeTask{
		TreeID: treeID,
		Task:   task,
	})

	return nil
}

// Safely gets a topic respecting read lock
func (uq *UltraQueue) getSafeTopic(topicName string) *Topic {
	uq.topicMu.RLock()
	defer uq.topicMu.RUnlock()

	if topic, exists := uq.topics[topicName]; exists {
		return topic
	}
	return nil
}

// Creates or overwrites a topic
func (uq *UltraQueue) putSafeTopic(topicName string) *Topic {
	uq.topicMu.Lock()
	defer uq.topicMu.Unlock()

	topic := NewTopic(topicName)
	uq.topics[topicName] = topic
	return topic
}

func (uq *UltraQueue) getTopicLengths() map[string]int {
	uq.topicMu.RLock()
	defer uq.topicMu.RUnlock()

	topicLengths := make(map[string]int)

	// Iterate over all of the topics and get their current lengths, non-sync read is ok
	for _, topic := range uq.topics {
		topicLengths[topic.Name] = topic.tree.Len()
	}

	return topicLengths
}
