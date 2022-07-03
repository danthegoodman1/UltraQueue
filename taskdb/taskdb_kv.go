package taskdb

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/rs/zerolog/log"
	"github.com/vmihailenco/msgpack/v5"
)

type KVTaskDB struct {
	payloadDB *pebble.DB
	stateDB   *pebble.DB

	payloadPath string
	statePath   string

	closed bool
}

func NewKVTaskDB(partition string) (*KVTaskDB, error) {
	// TODO: Get file location from config
	payloadPath, err := filepath.Abs(fmt.Sprintf("payload_%s", partition))
	if err != nil {
		return nil, fmt.Errorf("error getting payload absolute path: %w", err)
	}
	pdb, err := pebble.Open(payloadPath, &pebble.Options{
		// Logger: nil,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open badger payload DB: %w", err)
	}
	statePath, err := filepath.Abs(fmt.Sprintf("states_%s", partition))
	if err != nil {
		return nil, fmt.Errorf("error getting payload absolute path: %w", err)
	}
	sdb, err := pebble.Open(statePath, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("failed to open badger state DB: %w", err)
	}
	return &KVTaskDB{
		payloadDB:   pdb,
		stateDB:     sdb,
		payloadPath: payloadPath,
		statePath:   statePath,
		closed:      false,
	}, nil
}

type KVAttachIterator struct {
	db       *pebble.DB
	feed     chan *TaskDBTaskState
	doneChan chan struct{}
}

type KVDrainIterator struct {
	tdb *KVTaskDB

	feed     chan *DrainTask
	doneChan chan struct{}
}

type BadgerTaskStateWithID struct {
	State   *TaskDBTaskState
	ID      []byte
	Payload string
}

type KVWriteResult struct {
	returnChan chan error
}

func (ai *KVAttachIterator) Next() ([]*TaskDBTaskState, error) {
	buf := make([]*TaskDBTaskState, 0)
	for {
		state, open := <-ai.feed
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

func (wr KVWriteResult) Get() error {
	err := <-wr.returnChan
	return err
}

func (tdb *KVTaskDB) Attach() AttachIterator {
	// Start a transaction  goroutine
	feedChan := make(chan *TaskDBTaskState, 1000) // extra buffer size
	doneChan := make(chan struct{}, 1)

	ai := &KVAttachIterator{
		db:       tdb.stateDB,
		feed:     feedChan,
		doneChan: doneChan,
	}

	go tdb.attachLoad(ai)

	return ai
}

func (tdb *KVTaskDB) PutPayload(topicName, taskID string, payload string) WriteResult {
	returnChan := make(chan error, 1)
	go tdb.insertPayload(topicName, taskID, payload, returnChan)
	return &KVWriteResult{
		returnChan: returnChan,
	}
}

// Launched in a goroutine, communicates through the returnChan
func (tdb *KVTaskDB) insertPayload(topicName, taskID string, payload string, returnChan chan error) {
	payloadID := tdb.genPayloadKey(topicName, taskID)
	err := tdb.payloadDB.Set([]byte(payloadID), []byte(payload), pebble.Sync)
	returnChan <- err
}

func (tdb *KVTaskDB) PutState(state *TaskDBTaskState) WriteResult {
	returnChan := make(chan error, 1)
	go tdb.insertState(state, returnChan)
	return &KVWriteResult{
		returnChan: returnChan,
	}
}

func (tdb *KVTaskDB) insertState(state *TaskDBTaskState, returnChan chan error) {
	stateID := tdb.genStateKey(state.Topic, state.ID)
	b, err := tdb.taskStateToBytes(state)
	if err != nil {
		returnChan <- err
	} else {
		err = tdb.stateDB.Set([]byte(stateID), b, pebble.Sync)
		returnChan <- err
	}
}

func (tdb *KVTaskDB) GetPayload(topicName, taskID string) (payload string, err error) {
	payloadID := tdb.genPayloadKey(topicName, taskID)
	value, closer, err := tdb.payloadDB.Get([]byte(payloadID))
	payload = string(value)
	if err == pebble.ErrNotFound {
		return "", ErrPayloadNotFound
	}
	if err != nil {
		return "", fmt.Errorf("error getting task payload: %w", err)
	}
	if err = closer.Close(); err != nil {
		return "", fmt.Errorf("error closing closer from get: %w", err)
	}
	return
}

func (tdb *KVTaskDB) Delete(topicName, taskID string) WriteResult {
	tdb.deletePayload(topicName, taskID)
	tdb.deleteTaskStates(topicName, taskID)

	return &KVWriteResult{}
}

func (tdb *KVTaskDB) deletePayload(topicName, taskID string) {
	payloadID := tdb.genPayloadKey(topicName, taskID)
	err := tdb.payloadDB.Delete([]byte(payloadID), pebble.NoSync)
	if err != nil {
		log.Error().Err(err).Str("topic", topicName).Str("taskID", taskID).Msg("error deleting task payload")
	}
}

func (tdb *KVTaskDB) deleteTaskStates(topicName, taskID string) {
	stateID := tdb.genStateKey(topicName, taskID)
	err := tdb.stateDB.Delete([]byte(stateID), pebble.NoSync)
	if err != nil {
		log.Error().Err(err).Str("topic", topicName).Str("taskID", taskID).Msg("error deleting task state")
	}
}

func (tdb *KVTaskDB) Drain() DrainIterator {

	feedChan := make(chan *DrainTask, 1000) // extra buffer size
	doneChan := make(chan struct{}, 1)

	di := &KVDrainIterator{
		feed:     feedChan,
		doneChan: doneChan,
		tdb:      tdb,
	}

	go tdb.drainLoad(di)

	return di
}

func (di *KVDrainIterator) Next() ([]*DrainTask, error) {
	buf := make([]*DrainTask, 0)
	for {
		state, open := <-di.feed
		if !open {
			if !di.tdb.closed {
				// Drop everything and close
				fmt.Println("flushing")
				di.tdb.Close()
				// Delete the folders
				err := os.RemoveAll(di.tdb.payloadPath)
				if err != nil {
					log.Error().Err(err).Msg("error removing payload path")
				}
				err = os.RemoveAll(di.tdb.statePath)
				if err != nil {
					log.Error().Err(err).Msg("error removing state path")
				}
				di.tdb.closed = true
			}
			return buf, nil
		}
		buf = append(buf, state)
		if len(buf) >= 100 {
			// Read up to 100 items
			return buf, nil
		}
	}
}

// Launched in a goroutine
func (tdb *KVTaskDB) drainLoad(di *KVDrainIterator) {
	// While we still have non-enqueued states, keep scanning
	// TODO: Handle the errors instead of going fatal
	for {
		rows := 0
		buf := make([]*BadgerTaskStateWithID, 0)
		it := tdb.stateDB.NewIter(nil)
		for it.First(); it.Valid(); it.Next() {
			rows++
			item := it.Value()
			state, err := tdb.bytesToTaskState(item)
			if err != nil {
				log.Fatal().Err(err).Msg("error getting task state from bytes")
			}
			if state.State != TASK_STATE_ENQUEUED {
				// Not ready
				continue
			}

			// Get the payload
			payload, err := tdb.GetPayload(state.Topic, state.ID)
			if err != nil {
				log.Fatal().Err(err).Str("topic", state.Topic).Str("taskID", state.ID).Msg("error getting payload for task")
			}

			// Add to channel
			buf = append(buf, &BadgerTaskStateWithID{
				State:   state,
				Payload: payload,
				ID:      it.Key(),
			})

			if len(buf) >= 100 {
				// Send the buffer
				break
			}
		}
		it.Close()

		if len(buf) > 0 {
			// We have at least one item
			// Dump into channel and delete from states
			for _, taskState := range buf {
				di.feed <- &DrainTask{
					Topic:    taskState.State.Topic,
					Priority: taskState.State.Priority,
					Payload:  taskState.Payload,
				}
				// Delete it
				// res :=
				tdb.Delete(taskState.State.Topic, taskState.State.ID)
				// if err := res.Get(); err != nil {
				// 	log.Error().Err(err).Msg("error deleting task")
				// }
				fmt.Println("deleting", taskState.State.Topic, taskState.State.ID)
			}
		}

		if rows == 0 {
			// No more states, we can exit
			di.doneChan <- struct{}{}
			close(di.feed)
			return
		}

		time.Sleep(time.Millisecond * 10) // prevent spin
	}
}

// Launched in a goroutine, scans the rows and feeds a buffer into the feed chan
func (tdb *KVTaskDB) attachLoad(ai *KVAttachIterator) {
	it := tdb.stateDB.NewIter(nil)
	buf := make([]*TaskDBTaskState, 0)
	// for i := 0; i < 200; i++ {
	// 	buf = append(buf, &TaskDBTaskState{
	// 		Topic:            "test",
	// 		Partition:        "test",
	// 		ID:               "test",
	// 		State:            TASK_STATE_DELAYED,
	// 		Version:          1,
	// 		DeliveryAttempts: 1,
	// 		CreatedAt:        time.Now(),
	// 		Priority:         1,
	// 	})
	// }
	for it.First(); it.Valid(); it.Next() {
		item := it.Value()
		state, err := tdb.bytesToTaskState(item)
		if err != nil {
			log.Fatal().Err(err).Msg("error getting task state from bytes")
		}

		// Add to channel
		buf = append(buf, state)
		if len(buf) >= 100 {
			// Dump into channel
			for _, taskState := range buf {
				ai.feed <- taskState
			}
			buf = make([]*TaskDBTaskState, 0)
		}
	}

	it.Close()

	if len(buf) > 0 {
		// We have at least one more item left
		// Dump into channel
		for _, taskState := range buf {
			ai.feed <- taskState
		}
	}

	close(ai.feed)
}

func (tdb *KVTaskDB) genPayloadKey(topicName, taskID string) string {
	return fmt.Sprintf("%s_%s_pd", topicName, taskID)
}

func (tdb *KVTaskDB) genStateKey(topicName, taskID string) string {
	return fmt.Sprintf("%s_%s_st", topicName, taskID)
}

func (tdb *KVTaskDB) taskStateToBytes(state *TaskDBTaskState) ([]byte, error) {
	b, err := msgpack.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("error marshaling task state: %w", err)
	}
	return b, nil
}

func (tdb *KVTaskDB) bytesToTaskState(b []byte) (*TaskDBTaskState, error) {
	var state *TaskDBTaskState
	err := msgpack.Unmarshal(b, &state)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling taskdb task state from badger bytes: %w", err)
	}
	return state, nil
}

func (tdb *KVTaskDB) Close() error {
	log.Debug().Msg("closing KVTaskDB")
	err := tdb.payloadDB.Flush()
	if err != nil {
		log.Error().Err(err).Msg("error flushing payloadDB")
	}
	err = tdb.stateDB.Flush()
	if err != nil {
		log.Error().Err(err).Msg("error flushing stateDB")
	}
	err = tdb.payloadDB.Close()
	if err != nil {
		log.Error().Err(err).Msg("error closing payloadDB")
	}
	err = tdb.stateDB.Close()
	if err != nil {
		log.Error().Err(err).Msg("error closing stateDB")
	}
	return nil
}
