package main

import (
	"errors"
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

	ErrNotFoundInFlight = errors.New("task not found in flight")
)

type UltraQueue struct {
	Partition string

	TaskDB  taskdb.TaskDB
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
	// Initialize taskdb based on config
	// FIXME: Temporary in memory task db
	taskDB, err := taskdb.NewDiskKVTaskDB()
	if err != nil {
		return nil, fmt.Errorf("error creating new memory task db: %w", err)
	}

	uq := &UltraQueue{
		Partition:      partition,
		inFlightTree:   btree.New(32),
		inFlightTreeMu: &sync.Mutex{},
		delayTree:      btree.New(32),
		delayTreeMu:    &sync.Mutex{},
		inFlightTicker: time.NewTicker(time.Millisecond * 5),
		closeChan:      make(chan chan struct{}),
		topics:         make(map[string]*Topic),
		topicMu:        &sync.RWMutex{},
		TaskDB:         taskDB,
	}

	// Attach TaskDB
	s := time.Now()
	attachIter := taskDB.Attach()
	for {
		tasks, err := attachIter.Next()
		// fmt.Println("taskls: ", len(tasks))
		if len(tasks) == 0 && err == nil {
			log.Debug().Str("partition", uq.Partition).Msg("Finished attach in " + time.Since(s).String())
			break
		} else if err != nil {
			log.Fatal().Err(err).Msg("error attaching to taskdb")
		}

		// Process tasks
		// TODO: Wait for write commits
		results := make([]taskdb.WriteResult, 0)
		for _, task := range tasks {
			// Enqueue all tasks for now, maybe add per-state handling later
			result := uq.enqueueTask(&Task{
				ID:               task.ID,
				Topic:            task.Topic,
				Payload:          nil,
				CreatedAt:        task.CreatedAt,
				Version:          task.Version,
				DeliveryAttempts: task.DeliveryAttempts,
				Priority:         task.Priority,
			})
			results = append(results, result)
		}

		// Wait for all of the write results
		for _, result := range results {
			err := result.Get()
			if err != nil {
				// We need to fail because now we have it enqueued locally but not in the remote db
				return nil, fmt.Errorf("error waiting for write result: %w", err)
			}
		}
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

func (uq *UltraQueue) Enqueue(topics []string, payload []byte, priority int32, delaySeconds int32) error {
	for _, topicName := range topics {
		task := NewTask(topicName, uq.Partition, payload, priority)

		// Insert task payload
		uq.TaskDB.PutPayload(task.Topic, task.ID, payload)

		// Strip the payload so we don't store it in the topic
		task.Payload = []byte{}

		if delaySeconds > 0 {
			uq.enqueueDelayedTask(task, delaySeconds)
		} else {
			uq.enqueueTask(task)
		}
		// TODO: Wait for commits
	}

	// TODO: Increment enqueue metric
	return nil
}

func (uq *UltraQueue) Dequeue(topicName string, numTasks, inFlightTTLSeconds int32) (tasks []*InTreeTask, err error) {
	// Get numTasks from the topic
	dequeuedTasks, err := uq.dequeueTask(topicName, numTasks, inFlightTTLSeconds)
	if err != nil {
		log.Error().Err(err).Msg("Error dequeuing task")
		return nil, err
	}

	// Get task payloads
	for _, task := range dequeuedTasks {
		payload, err := uq.TaskDB.GetPayload(topicName, task.Task.ID)
		if err != nil {
			// TODO: Handle this properly
			log.Error().Err(err).Str("partition", uq.Partition).Str("topicName", topicName).Str("taskID", task.Task.ID).Msg("failed to get task payload")
		}
		// Create a new task going out so we can assign the payload without storing it
		tasks = append(tasks, &InTreeTask{
			TreeID: task.TreeID,
			Task: &Task{
				ID:               task.Task.ID,
				Topic:            task.Task.Topic,
				Payload:          payload,
				CreatedAt:        task.Task.CreatedAt,
				Version:          task.Task.Version,
				DeliveryAttempts: task.Task.DeliveryAttempts,
				Priority:         task.Task.Priority,
			},
		})
	}

	// TODO: Increment dequeue metric
	return
}

func (uq *UltraQueue) Ack(inFlightTaskID string) (err error) {
	// Check if in the in-flight tree
	topicName, taskID, deleted := uq.ack(inFlightTaskID)
	if !deleted {
		return ErrNotFoundInFlight
	}
	// Delete from TaskDB
	uq.TaskDB.Delete(topicName, taskID)
	// TODO: optionally wait for commit?

	// TODO: Increment ack metric
	return nil
}

func (uq *UltraQueue) Nack(inFlightTaskID string, delaySeconds int32) (err error) {
	// TODO: insert new task state

	nacked := uq.nack(inFlightTaskID, delaySeconds)
	if !nacked {
		return ErrNotFoundInFlight
	}

	// TODO: Increment nack metric
	return
}

func (uq *UltraQueue) enqueueDelayedTask(task *Task, delaySeconds int32) taskdb.WriteResult {

	treeID := task.genTimeTreeID(task.CreatedAt.Add(time.Second * time.Duration(delaySeconds)))
	treeTask := NewInTreeTask(treeID, task)

	// Add task state to DB
	wr := uq.TaskDB.PutState(&taskdb.TaskDBTaskState{
		Topic:            task.Topic,
		Partition:        uq.Partition,
		ID:               task.ID,
		State:            taskdb.TASK_STATE_DELAYED,
		Version:          task.Version,
		DeliveryAttempts: task.DeliveryAttempts,
		CreatedAt:        task.CreatedAt,
		Priority:         task.Priority,
	})
	// TODO: Wait for commit, Optionally MUST

	uq.delayTreeMu.Lock()
	defer uq.delayTreeMu.Unlock()

	uq.delayTree.ReplaceOrInsert(treeTask)
	return wr
}

func (uq *UltraQueue) enqueueTask(task *Task) taskdb.WriteResult {
	log.Debug().Str("partition", uq.Partition).Str("topic", task.Topic).Msg("Enqueuing topic")
	// Add task state to DB
	wr := uq.TaskDB.PutState(&taskdb.TaskDBTaskState{
		Topic:            task.Topic,
		Partition:        uq.Partition,
		ID:               task.ID,
		State:            taskdb.TASK_STATE_ENQUEUED,
		Version:          task.Version,
		DeliveryAttempts: task.DeliveryAttempts,
		CreatedAt:        task.CreatedAt,
		Priority:         task.Priority,
	})
	// TODO: Wait for ack, Optionally MUST

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

	return wr
}

func (uq *UltraQueue) dequeueTask(topicName string, numTasks, inFlightTTLSeconds int32) (tasks []*InTreeTask, err error) {
	log.Debug().Str("partition", uq.Partition).Str("topic", topicName).Msg("Dequeueing topic")

	dequeueTime := time.Now()

	topic := uq.getSafeTopic(topicName)
	if topic == nil {
		return nil, nil
	}

	uq.inFlightTreeMu.Lock()
	defer uq.inFlightTreeMu.Unlock()

	// Get tasks and add to inflight tree
	inTreeTasks := topic.Dequeue(numTasks)
	for _, itt := range inTreeTasks {
		itt.Task.DeliveryAttempts++
		itt.TreeID = itt.Task.genExternalID(uq.Partition, dequeueTime.Add(time.Second*time.Duration(inFlightTTLSeconds)))
		tasks = append(tasks, itt)
		// Add task state to DB
		uq.TaskDB.PutState(&taskdb.TaskDBTaskState{
			Topic:            itt.Task.Topic,
			Partition:        uq.Partition,
			ID:               itt.Task.ID,
			State:            taskdb.TASK_STATE_INFLIGHT,
			Version:          itt.Task.Version,
			DeliveryAttempts: itt.Task.DeliveryAttempts,
			CreatedAt:        itt.Task.CreatedAt,
			Priority:         itt.Task.Priority,
		})
		// TODO: Wait for commit? Probably not needed

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

// Removes from the in-flight tree, returns whether the task existed
func (uq *UltraQueue) ack(inTreeTaskID string) (topic, taskID string, deleted bool) {
	uq.inFlightTreeMu.Lock()
	defer uq.inFlightTreeMu.Unlock()

	// shitty google btree needs the entire object for the delete, so we have to go FIND IT FIRST
	var foundTask *InTreeTask
	uq.inFlightTree.AscendGreaterOrEqual(&InTreeTask{
		TreeID: inTreeTaskID,
	}, func(i btree.Item) bool {
		itt, _ := i.(*InTreeTask)
		if itt.TreeID == inTreeTaskID {
			foundTask = itt
		}
		// If we didn't find it as the first one, then we don't have it
		return false
	})
	if foundTask != nil {
		// Delete from inflight tree
		uq.inFlightTree.Delete(foundTask)

		return foundTask.Task.Topic, foundTask.Task.ID, true
	} else {
		return "", "", false
	}
}

func (uq *UltraQueue) nack(inTreeTaskID string, delaySeconds int32) bool {
	uq.inFlightTreeMu.Lock()
	defer uq.inFlightTreeMu.Unlock()

	// shitty google btree needs the entire object for the delete, so we have to go FIND IT FIRST
	var foundTask *InTreeTask
	uq.inFlightTree.AscendGreaterOrEqual(&InTreeTask{
		TreeID: inTreeTaskID,
	}, func(i btree.Item) bool {
		itt, _ := i.(*InTreeTask)
		if itt.TreeID == inTreeTaskID {
			foundTask = itt
		}
		// If we didn't find it as the first one, then we don't have it
		return false
	})
	if foundTask != nil {
		uq.inFlightTree.Delete(foundTask)
		if delaySeconds > 0 {
			// // Update time id
			// foundTask.TreeID = foundTask.Task.genTimeTreeID(time.Now().Add(time.Second * time.Duration(delaySeconds)))
			// // Put in delay tree
			// uq.delayTreeMu.Lock()
			// defer uq.delayTreeMu.Unlock()
			// uq.delayTree.ReplaceOrInsert(foundTask)

			uq.enqueueDelayedTask(foundTask.Task, delaySeconds)

		} else {
			// // Put in topic
			// topic := uq.getSafeTopic(foundTask.Task.Topic)
			// if topic == nil {
			// 	// Create if it doesn't exist
			// 	topic = uq.putSafeTopic(foundTask.Task.Topic)
			// }
			// topic.Enqueue(NewInTreeTask(foundTask.Task.genPriorityTreeID(), foundTask.Task))

			uq.enqueueTask(foundTask.Task)

		}
		return true
	} else {
		return false
	}
}
