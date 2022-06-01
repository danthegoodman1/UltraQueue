package taskdb

import (
	"fmt"
	"sync"
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

type MemoryDrainIterator struct{}

type MemoryWriteResult struct{}

func (fai *FakeAttachIterator) Next() ([]*TaskDBTaskState, error) {
	return nil, nil
}

func (mdi *MemoryDrainIterator) Next() ([]*DrainTask, error) {
	// TODO: Drain from map, release every X Y
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
	return &MemoryDrainIterator{}
}

func (MemoryTaskDB) getMapID(topicName, taskID string) string {
	return fmt.Sprintf("%s_%s", topicName, taskID)
}
