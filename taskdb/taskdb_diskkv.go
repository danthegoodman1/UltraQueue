package taskdb

import (
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/rs/zerolog/log"
	"github.com/vmihailenco/msgpack/v5"
)

type DiskKVTaskDB struct {
	db *badger.DB
}

func NewDiskKVTaskDB() (*DiskKVTaskDB, error) {
	// TODO: Get file location from config
	bdb, err := badger.Open(badger.DefaultOptions("/tmp/badger"))
	if err != nil {
		return nil, fmt.Errorf("failed to open badger DB: %w", err)
	}
	return &DiskKVTaskDB{
		db: bdb,
	}, nil
}

type DiskKVAttachIterator struct {
	db       *badger.DB
	feed     chan *TaskDBTaskState
	doneChan chan struct{}
}

type DiskKVDrainIterator struct {
	db *badger.DB
}

type DiskKVWriteResult struct {
	returnChan chan error
}

func (ai *DiskKVAttachIterator) Next() ([]*TaskDBTaskState, error) {
	buf := make([]*TaskDBTaskState, 0)
	for {
		state, open := <-ai.feed
		if !open {
			return nil, nil
		}
		buf = append(buf, state)
		if len(buf) >= 100 {
			// Read up to 100 items
			return buf, nil
		}
	}
}

func (di *DiskKVDrainIterator) Next() ([]*DrainTask, error) {
	// TODO: Drain from map, release every X Y
	di.db.Close()
	return nil, nil
}

func (wr DiskKVWriteResult) Get() error {
	err := <-wr.returnChan
	return err
}

func (tdb *DiskKVTaskDB) Attach() AttachIterator {
	// Start a transaction  goroutine
	feedChan := make(chan *TaskDBTaskState, 1000) // extra buffer size
	doneChan := make(chan struct{}, 1)

	ai := &DiskKVAttachIterator{
		db:       tdb.db,
		feed:     feedChan,
		doneChan: doneChan,
	}

	go tdb.attachLoad(ai)

	return ai
}

func (tdb *DiskKVTaskDB) PutPayload(topicName, taskID string, payload []byte) WriteResult {
	returnChan := make(chan error, 1)
	go tdb.insertPayload(topicName, taskID, payload, returnChan)
	return &DiskKVWriteResult{
		returnChan: returnChan,
	}
}

// Launched in a goroutine, communicates through the returnChan
func (tdb *DiskKVTaskDB) insertPayload(topicName, taskID string, payload []byte, returnChan chan error) {
	err := tdb.db.Update(func(txn *badger.Txn) error {
		payloadID := tdb.genPayloadID(topicName, taskID)
		err := txn.Set([]byte(payloadID), payload)
		if err != nil {
			return fmt.Errorf("error setting payload: %w", err)
		}
		return nil
	})
	returnChan <- err
}

func (tdb *DiskKVTaskDB) PutState(state *TaskDBTaskState) WriteResult {

	return &DiskKVWriteResult{}
}

func (tdb *DiskKVTaskDB) GetPayload(topicName, taskID string) ([]byte, error) {

	return nil, nil
}

func (tdb *DiskKVTaskDB) Delete(topicName, taskID string) WriteResult {

	tdb.deletePayload(topicName, taskID)
	tdb.deleteTaskStates(topicName, taskID)

	return &DiskKVWriteResult{}
}

func (tdb *DiskKVTaskDB) deletePayload(topicName, taskID string) {
	// If there are no more topics, delete, otherwise pop the topic
}

func (tdb *DiskKVTaskDB) deleteTaskStates(topicName, taskID string) {

}

func (tdb *DiskKVTaskDB) Drain() DrainIterator {
	return &DiskKVDrainIterator{
		db: tdb.db,
	}
}

// Launched in a goroutine, scans the rows and feeds a buffer into the feed chan
func (tdb *DiskKVTaskDB) attachLoad(ai *DiskKVAttachIterator) {
	// TODO: Remove log line
	log.Debug().Msg("starting attach loader")
	ai.db.View(func(txn *badger.Txn) error {
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
					// TODO: Remove log line
					log.Debug().Msg("dumping into channel")
					// Dump into channel
					for _, taskState := range buf {
						ai.feed <- taskState
					}
					// TODO: Remove log line
					log.Debug().Msg("dumped into channel")
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
			// TODO: Remove log line
			log.Debug().Msg("final dumping into channel")
			// Dump into channel
			for _, taskState := range buf {
				ai.feed <- taskState
			}
			// TODO: Remove log line
			log.Debug().Msg("final dumped into channel")
		}
		return nil
	})
	close(ai.feed)
}

func (tdb *DiskKVTaskDB) genPayloadID(topicName, taskID string) string {
	return fmt.Sprintf("%s_%s", topicName, taskID)
}

func (tdb *DiskKVTaskDB) genStateID(topicName, taskID string, version int32) string {
	return fmt.Sprintf("%s_%s_%d", topicName, taskID, version)
}

func (tdb *DiskKVTaskDB) taskStateToBytes(state *TaskDBTaskState) ([]byte, error) {
	b, err := msgpack.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("error marshaling task state: %w", err)
	}
	return b, nil
}

func (tdb *DiskKVTaskDB) bytesToTaskState(b []byte) (*TaskDBTaskState, error) {
	var state *TaskDBTaskState
	err := msgpack.Unmarshal(b, &state)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling taskdb task state from badger bytes: %w", err)
	}
	return state, nil
}
