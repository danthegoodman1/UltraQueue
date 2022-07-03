package taskdb

import (
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

type MemoryTaskDB struct {
	payloadMu  *sync.Mutex
	payloadMap map[string]string

	taskStateMu  *sync.Mutex
	taskStateMap map[string][]*TaskDBTaskState
}

func NewMemoryTaskDB() (*MemoryTaskDB, error) {
	return &MemoryTaskDB{
		payloadMu:    &sync.Mutex{},
		payloadMap:   make(map[string]string),
		taskStateMu:  &sync.Mutex{},
		taskStateMap: make(map[string][]*TaskDBTaskState),
	}, nil
}

type FakeAttachIterator struct{}

type MemoryDrainIterator struct {
	feed     chan *DrainTask
	doneChan chan struct{}
	tdb      *MemoryTaskDB
}

type MemoryWriteResult struct{}

func (fai *FakeAttachIterator) Next() ([]*TaskDBTaskState, error) {
	return nil, nil
}

func (mwr MemoryWriteResult) Get() error {
	return nil
}

func (mtdb *MemoryTaskDB) NewMemoryTaskDB() (*MemoryTaskDB, error) {
	return &MemoryTaskDB{
		payloadMu:  &sync.Mutex{},
		payloadMap: make(map[string]string),
	}, nil
}

func (mtdb *MemoryTaskDB) Attach() AttachIterator {
	return &FakeAttachIterator{}
}

func (mtdb *MemoryTaskDB) PutPayload(topicName, taskID string, payload string) WriteResult {
	mapID := mtdb.getMapID(topicName, taskID)
	mtdb.insertPayload(mapID, payload)
	return &MemoryWriteResult{}
}

func (mtdb *MemoryTaskDB) insertPayload(mapID string, payload string) {
	mtdb.payloadMu.Lock()
	defer mtdb.payloadMu.Unlock()

	mtdb.payloadMap[mapID] = payload
}

func (mtdb *MemoryTaskDB) insertTaskState(mapID string, state *TaskDBTaskState) {
	mtdb.taskStateMu.Lock()
	defer mtdb.taskStateMu.Unlock()

	states, exists := mtdb.taskStateMap[mapID]
	if exists {
		mtdb.taskStateMap[mapID] = append(states, state)
	} else {
		mtdb.taskStateMap[mapID] = []*TaskDBTaskState{state}
	}
}

func (mtdb *MemoryTaskDB) PutState(state *TaskDBTaskState) WriteResult {
	mtdb.insertTaskState(mtdb.getMapID(state.Topic, state.ID), state)
	return &MemoryWriteResult{}
}

func (mtdb *MemoryTaskDB) GetPayload(topicName, taskID string) (string, error) {
	mtdb.payloadMu.Lock()
	defer mtdb.payloadMu.Unlock()

	payload, exists := mtdb.payloadMap[mtdb.getMapID(topicName, taskID)]
	if !exists {
		return "", ErrPayloadNotFound
	}
	return payload, nil
}

func (mtdb *MemoryTaskDB) Delete(topicName, taskID string) WriteResult {
	mapID := mtdb.getMapID(topicName, taskID)

	mtdb.deletePayload(mapID)
	mtdb.deleteTaskStates(mapID)

	return &MemoryWriteResult{}
}

func (mtdb *MemoryTaskDB) deletePayload(mapID string) {
	mtdb.payloadMu.Lock()
	defer mtdb.payloadMu.Unlock()
	delete(mtdb.payloadMap, mapID)
}

func (mtdb *MemoryTaskDB) deleteTaskStates(mapID string) {
	mtdb.taskStateMu.Lock()
	defer mtdb.taskStateMu.Unlock()
	delete(mtdb.taskStateMap, mapID)
}

func (mtdb *MemoryTaskDB) Drain() DrainIterator {
	feedChan := make(chan *DrainTask, 1000) // extra buffer size
	doneChan := make(chan struct{}, 1)

	di := &MemoryDrainIterator{
		tdb:      mtdb,
		feed:     feedChan,
		doneChan: doneChan,
	}

	go mtdb.drainLoad(di)

	return di
}

// Launched in a goroutine, continues to pass over the state map looking for enqueued tasks and draining them until the tree is empty
func (mtdb *MemoryTaskDB) drainLoad(di *MemoryDrainIterator) {
	// Hehe risky manual lock releasing
	for {
		mtdb.taskStateMu.Lock()
		if len(mtdb.taskStateMap) == 0 {
			// We are done
			log.Debug().Msg("done drain loading!")
			close(di.feed)
			di.doneChan <- struct{}{}
			return
		}
		// Find any enqueued tasks
		for mapID, states := range mtdb.taskStateMap {
			// We know the len can never be zero
			lastState := states[len(states)-1]
			if lastState.State == TASK_STATE_ENQUEUED {
				// Get the payload
				payload, err := mtdb.GetPayload(lastState.Topic, lastState.ID)
				if err == ErrPayloadNotFound {
					log.Error().Str("taskID", lastState.ID).Str("topic", lastState.Topic).Msg("payload not found while draining")
					delete(mtdb.taskStateMap, mapID)
					continue
				}

				di.feed <- &DrainTask{
					Topic:    lastState.Topic,
					Priority: lastState.Priority,
					Payload:  payload,
				}
				delete(mtdb.taskStateMap, mapID)
			}
		}
		mtdb.taskStateMu.Unlock()
		// Sleep for 10ms to release the lock and not spin CPU too hard
		time.Sleep(time.Millisecond * 10)
	}
}

func (di *MemoryDrainIterator) Next() ([]*DrainTask, error) {
	buf := make([]*DrainTask, 0)
	for {
		state, open := <-di.feed
		if !open {
			return buf, nil
		}
		buf = append(buf, state)
		if len(buf) >= 100 {
			// Read up to 100 items
			return buf, nil
		}
	}
}

func (MemoryTaskDB) getMapID(topicName, taskID string) string {
	return fmt.Sprintf("%s_%s", topicName, taskID)
}

func (MemoryTaskDB) Close() error {
	return nil
}
