package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/danthegoodman1/UltraQueue/taskdb"
	"github.com/google/btree"
	"github.com/rs/zerolog/log"
)

var (
	// Will only process up to 10_000 items per tick to prevent massive stalls
	DelayInFlightIteratorMaxItems = 10_000
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
	closeChan      chan chan struct{}
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
		closeChan:      make(chan chan struct{}),
		topics:         make(map[string]*Topic),
		topicMu:        &sync.RWMutex{},
	}

	// Start background inflight and delay tree scanner
	go uq.pollDelayAndInFlightTrees(time.NewTicker(time.Millisecond * 200))

	return uq, nil
}

func (uq *UltraQueue) Shutdown() {
	log.Info().Str("partition", uq.Partition).Msg("Shutting down ultra queue...")
	returnChan := make(chan struct{}, 1)
	uq.closeChan <- returnChan
	<-returnChan
	log.Info().Str("partition", uq.Partition).Msg("Shut down ultra queue")
}

func (uq *UltraQueue) Enqueue(topic string, payload []byte, priority int32, delaySeconds int64) {
	task := NewTask(topic, uq.Partition, payload, priority, 0) // FIXME: TTL 0

	// TODO: Insert task into task DB task table

	if delaySeconds > 0 {
		uq.enqueueDelayedTask(task, delaySeconds)
	} else {
		uq.enqueueTask(task)
	}

	// TODO: Increment enqueue metric
	return
}

func (uq *UltraQueue) Dequeue(topicName string, numTasks, inFlightTTLSeconds int) (tasks []*InTreeTask, err error) {
	// Get numTasks from the topic
	rawTasks, err := uq.dequeueTask(topicName, numTasks, inFlightTTLSeconds)
	if err != nil {
		log.Error().Err(err).Msg("Error dequeuing task")
		return
	}

	// Prepend partition to task ID
	for _, task := range rawTasks {
		tasks = append(tasks, &InTreeTask{
			TreeID: task.genExternalID(uq.Partition),
			Task:   task,
		})
	}

	// TODO: Increment dequeue metric
	return
}

func (uq *UltraQueue) Ack(taskID, topicName string) (err error) {
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
	log.Debug().Str("partition", uq.Partition).Str("topic", task.Topic).Msg("Enqueuing topic")
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

func (uq *UltraQueue) dequeueTask(topicName string, numTasks, ttlSeconds int) (tasks []*Task, err error) {
	log.Debug().Str("partition", uq.Partition).Str("topic", topicName).Msg("Dequeueing topic")
	// TODO: Add task state to DB

	topic := uq.getSafeTopic(topicName)
	if topic == nil {
		return nil, nil
	}

	uq.inFlightTreeMu.Lock()
	defer uq.inFlightTreeMu.Unlock()

	// Get tasks and add to inflight tree
	inTreeTasks := topic.Dequeue(numTasks)
	dequeueTime := time.Now()
	for _, itt := range inTreeTasks {
		tasks = append(tasks, itt.Task)
		itt.TreeID = itt.Task.genTimeTreeID(dequeueTime.Add(time.Second * time.Duration(ttlSeconds)))
		uq.inFlightTree.ReplaceOrInsert(itt)
	}

	return
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

// Launched as goroutine, moves tasks from delay and inflight trees when they expire
func (uq *UltraQueue) pollDelayAndInFlightTrees(t *time.Ticker) {
	for {
		select {
		case tickTime := <-t.C:
			// Poll each
			uq.expireDelayedTasks(tickTime)
			uq.expireInFlightTasks(tickTime)
		case returnChan := <-uq.closeChan:
			log.Info().Str("partition", uq.Partition).Msg("Delay and InFlight poll got stop channel, exiting")
			returnChan <- struct{}{}
			return
		}
	}
}

// Moves tasks from the delayed queue to the topic queue
func (uq *UltraQueue) expireDelayedTasks(t time.Time) {
	uq.delayTreeMu.Lock()
	defer uq.delayTreeMu.Unlock()

	tasks := make([]*InTreeTask, 0)

	// UnixMS prefix
	treeID := fmt.Sprintf("%d", t.UnixMilli())

	count := 0
	uq.delayTree.AscendLessThan(&InTreeTask{
		TreeID: treeID,
	}, func(i btree.Item) bool {
		itt, _ := i.(*InTreeTask)
		tasks = append(tasks, itt)

		// Stall protection
		count++
		return count < DelayInFlightIteratorMaxItems
	})

	// Delete from delayed and insert into topic queues
	for _, itt := range tasks {
		uq.delayTree.Delete(itt)
		uq.enqueueTask(itt.Task)
	}
	// TODO: Increment delay expire metric?
}

func (uq *UltraQueue) expireInFlightTasks(t time.Time) {
	uq.inFlightTreeMu.Lock()
	defer uq.inFlightTreeMu.Unlock()

	tasks := make([]*InTreeTask, 0)

	// UnixMS prefix
	treeID := fmt.Sprintf("%d", t.UnixMilli())

	count := 0
	uq.inFlightTree.AscendLessThan(&InTreeTask{
		TreeID: treeID,
	}, func(i btree.Item) bool {
		itt, _ := i.(*InTreeTask)
		tasks = append(tasks, itt)

		// Stall protection
		count++
		return count < DelayInFlightIteratorMaxItems
	})

	// Delete from delayed and insert into topic queues
	for _, itt := range tasks {
		uq.inFlightTree.Delete(itt)
		uq.enqueueTask(itt.Task)
	}
	// TODO: Increment inflight ttl metric
}
