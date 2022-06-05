package taskdb

import (
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/rs/zerolog/log"
	"github.com/vmihailenco/msgpack/v5"
)

type BadgerTaskDB struct {
	payloadDB *badger.DB
	stateDB   *badger.DB
}

func NewBadgerTaskDB(partition string) (*BadgerTaskDB, error) {
	// TODO: Get file location from config
	opts := badger.DefaultOptions(fmt.Sprintf("./badger_payload_%s", partition))
	opts.Logger = nil
	pdb, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger payload DB: %w", err)
	}
	opts = badger.DefaultOptions(fmt.Sprintf("./badger_states_%s", partition))
	opts.Logger = nil
	sdb, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger state DB: %w", err)
	}
	return &BadgerTaskDB{
		payloadDB: pdb,
		stateDB:   sdb,
	}, nil
}

type BadgerAttachIterator struct {
	db       *badger.DB
	feed     chan *TaskDBTaskState
	doneChan chan struct{}
}

type BadgerDrainIterator struct {
	payloadDB *badger.DB
	stateDB   *badger.DB

	feed     chan *DrainTask
	doneChan chan struct{}
}

type BadgerTaskStateWithID struct {
	State   *TaskDBTaskState
	ID      []byte
	Payload string
}

type BadgerWriteResult struct {
	returnChan chan error
}

func (ai *BadgerAttachIterator) Next() ([]*TaskDBTaskState, error) {
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

func (wr BadgerWriteResult) Get() error {
	err := <-wr.returnChan
	return err
}

func (tdb *BadgerTaskDB) Attach() AttachIterator {
	// Start a transaction  goroutine
	feedChan := make(chan *TaskDBTaskState, 1000) // extra buffer size
	doneChan := make(chan struct{}, 1)

	ai := &BadgerAttachIterator{
		db:       tdb.stateDB,
		feed:     feedChan,
		doneChan: doneChan,
	}

	go tdb.attachLoad(ai)

	return ai
}

func (tdb *BadgerTaskDB) PutPayload(topicName, taskID string, payload string) WriteResult {
	returnChan := make(chan error, 1)
	go tdb.insertPayload(topicName, taskID, payload, returnChan)
	return &BadgerWriteResult{
		returnChan: returnChan,
	}
}

// Launched in a goroutine, communicates through the returnChan
func (tdb *BadgerTaskDB) insertPayload(topicName, taskID string, payload string, returnChan chan error) {
	err := tdb.payloadDB.Update(func(txn *badger.Txn) error {
		payloadID := tdb.genPayloadKey(topicName, taskID)
		err := txn.Set([]byte(payloadID), []byte(payload))
		if err != nil {
			return fmt.Errorf("error setting payload: %w", err)
		}
		return nil
	})
	returnChan <- err
}

func (tdb *BadgerTaskDB) PutState(state *TaskDBTaskState) WriteResult {
	returnChan := make(chan error, 1)
	go tdb.insertState(state, returnChan)
	return &BadgerWriteResult{
		returnChan: returnChan,
	}
}

func (tdb *BadgerTaskDB) insertState(state *TaskDBTaskState, returnChan chan error) {
	err := tdb.stateDB.Update(func(txn *badger.Txn) error {
		stateID := tdb.genStateKey(state.Topic, state.ID)
		b, err := tdb.taskStateToBytes(state)
		if err != nil {
			return fmt.Errorf("failed to convert state to bytes: %w", err)
		}
		err = txn.Set([]byte(stateID), b)
		if err != nil {
			return fmt.Errorf("error setting state: %w", err)
		}
		return nil
	})
	returnChan <- err
}

func (tdb *BadgerTaskDB) GetPayload(topicName, taskID string) (payload string, err error) {
	err = tdb.payloadDB.View(func(txn *badger.Txn) error {
		payloadID := tdb.genPayloadKey(topicName, taskID)
		item, err := txn.Get([]byte(payloadID))
		if err != nil {
			return fmt.Errorf("error getting task payload: %w", err)
		}
		return item.Value(func(val []byte) error {
			payload = string(val)
			return nil
		})
	})
	return
}

func (tdb *BadgerTaskDB) Delete(topicName, taskID string) WriteResult {
	go tdb.deletePayload(topicName, taskID)
	go tdb.deleteTaskStates(topicName, taskID)

	return &BadgerWriteResult{}
}

func (tdb *BadgerTaskDB) deletePayload(topicName, taskID string) {
	err := tdb.payloadDB.Update(func(txn *badger.Txn) error {
		payloadID := tdb.genPayloadKey(topicName, taskID)
		err := txn.Delete([]byte(payloadID))
		if err != nil {
			return fmt.Errorf("error deleting payload: %w", err)
		}
		return nil
	})
	if err != nil {
		log.Error().Err(err).Str("topic", topicName).Str("taskID", taskID).Msg("error deleting task payload")
	}
}

func (tdb *BadgerTaskDB) deleteTaskStates(topicName, taskID string) {
	err := tdb.stateDB.Update(func(txn *badger.Txn) error {
		stateID := tdb.genStateKey(topicName, taskID)
		err := txn.Delete([]byte(stateID))
		if err != nil {
			return fmt.Errorf("error setting state: %w", err)
		}
		return nil
	})
	if err != nil {
		log.Error().Err(err).Str("topic", topicName).Str("taskID", taskID).Msg("error deleting task state")
	}
}

func (tdb *BadgerTaskDB) Drain() DrainIterator {

	feedChan := make(chan *DrainTask, 1000) // extra buffer size
	doneChan := make(chan struct{}, 1)

	di := &BadgerDrainIterator{
		feed:      feedChan,
		doneChan:  doneChan,
		payloadDB: tdb.payloadDB,
		stateDB:   tdb.stateDB,
	}

	go tdb.drainLoad(di)

	return di
}

func (di *BadgerDrainIterator) Next() ([]*DrainTask, error) {
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

// Launched in a goroutine
func (tdb *BadgerTaskDB) drainLoad(di *BadgerDrainIterator) {
	// While we still have non-enqueued states, keep scanning
	for {
		rows := 0
		buf := make([]*BadgerTaskStateWithID, 0)
		err := di.stateDB.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				rows++
				item := it.Item()
				err := item.Value(func(val []byte) error {
					state, err := tdb.bytesToTaskState(val)
					if err != nil {
						return fmt.Errorf("error getting task state from bytes: %w", err)
					}
					if state.State != TASK_STATE_ENQUEUED {
						// Not ready
						return nil
					}

					// Get the payload
					payload, err := tdb.GetPayload(state.Topic, state.ID)
					if err != nil {
						return fmt.Errorf("error getting payload for task: %w", err)
					}

					// Add to channel
					buf = append(buf, &BadgerTaskStateWithID{
						State:   state,
						Payload: payload,
						ID:      item.Key(),
					})
					return nil
				})
				if len(buf) >= 100 {
					// Send the buffer
					break
				}
				if err != nil {
					return fmt.Errorf("error getting item value from badger: %w", err)
				}
			}
			return nil
		})

		if err != nil {
			log.Fatal().Err(err).Msg("failed to iterate over drain view transaction")
			// TODO: Something here
		}

		if len(buf) > 0 {
			// We have at least one item
			// Dump into channel and delete from states
			err := di.stateDB.Update(func(txn *badger.Txn) error {
				for _, taskState := range buf {
					di.feed <- &DrainTask{
						Topic:    taskState.State.Topic,
						Priority: taskState.State.Priority,
						Payload:  taskState.Payload,
					}
					err := txn.Delete(taskState.ID)
					if err != nil {
						return fmt.Errorf("error deleting task state: %w", err)
					}
				}
				return nil
			})
			if err != nil {
				log.Fatal().Err(err).Msg("failed to send badger drain buffer to chan")
				// TODO: Something here
			}
		}

		if rows == 0 {
			// No more states, we can exit
			di.doneChan <- struct{}{}
			close(di.feed)

			// Drop everything and close
			tdb.payloadDB.DropAll()
			tdb.stateDB.DropAll()
			tdb.payloadDB.Close()
			tdb.stateDB.Close()
			return
		}

		time.Sleep(time.Millisecond * 10) // prevent spin
	}
}

// Launched in a goroutine, scans the rows and feeds a buffer into the feed chan
func (tdb *BadgerTaskDB) attachLoad(ai *BadgerAttachIterator) {
	err := ai.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
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
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				state, err := tdb.bytesToTaskState(val)
				if err != nil {
					return fmt.Errorf("error getting task state from bytes: %w", err)
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
				return nil
			})
			if err != nil {
				return fmt.Errorf("error getting item value from badger: %w", err)
			}
		}
		if len(buf) > 0 {
			// We have at least one more item left
			// Dump into channel
			for _, taskState := range buf {
				ai.feed <- taskState
			}
		}
		return nil
	})
	if err != nil {
		log.Fatal().Err(err).Msg("failed to iterate")
	}
	close(ai.feed)
}

func (tdb *BadgerTaskDB) genPayloadKey(topicName, taskID string) string {
	return fmt.Sprintf("%s_%s_pd", topicName, taskID)
}

func (tdb *BadgerTaskDB) genStateKey(topicName, taskID string) string {
	return fmt.Sprintf("%s_%s_st", topicName, taskID)
}

func (tdb *BadgerTaskDB) taskStateToBytes(state *TaskDBTaskState) ([]byte, error) {
	b, err := msgpack.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("error marshaling task state: %w", err)
	}
	return b, nil
}

func (tdb *BadgerTaskDB) bytesToTaskState(b []byte) (*TaskDBTaskState, error) {
	var state *TaskDBTaskState
	err := msgpack.Unmarshal(b, &state)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling taskdb task state from badger bytes: %w", err)
	}
	return state, nil
}
