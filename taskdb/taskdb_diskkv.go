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

type DiskKVWriteResult struct{}

func (ai *DiskKVAttachIterator) Next() ([]*TaskDBTaskState, error) {
	buf := make([]*TaskDBTaskState, 0)
	// Read up to 100 items
	for {
		state, open := <-ai.feed
		if open {
			buf = append(buf, state)
			if len(buf) >= 100 {
				return buf, nil
			}
		} else if len(ai.feed) > 0 {
			for state := range ai.feed {
				buf = append(buf, state)
			}
			return buf, nil
		} else if !open {
			return nil, nil
		}
	}
}

func (di *DiskKVDrainIterator) Next() ([]*DrainTask, error) {
	// TODO: Drain from map, release every X Y
	di.db.Close()
	return nil, nil
}

func (wr DiskKVWriteResult) Get() error {
	return nil
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

	go attachLoad(ai)

	return ai
}

func (tdb *DiskKVTaskDB) PutPayload(topicName, taskID string, payload []byte) WriteResult {
	mapID := tdb.getMapID(topicName, taskID)
	tdb.insertPayload(mapID, payload)
	return &DiskKVWriteResult{}
}

func (tdb *DiskKVTaskDB) insertPayload(mapID string, payload []byte) {

}

func (tdb *DiskKVTaskDB) insertTaskState(mapID string, state *TaskDBTaskState) {

}

func (tdb *DiskKVTaskDB) PutState(state *TaskDBTaskState) WriteResult {
	tdb.insertTaskState(tdb.getMapID(state.Topic, state.ID), state)
	return &DiskKVWriteResult{}
}

func (tdb *DiskKVTaskDB) GetPayload(topicName, taskID string) ([]byte, error) {

	return nil, nil
}

func (tdb *DiskKVTaskDB) Delete(topicName, taskID string) WriteResult {
	mapID := tdb.getMapID(topicName, taskID)

	tdb.deletePayload(mapID)
	tdb.deleteTaskStates(mapID)

	return &DiskKVWriteResult{}
}

func (tdb *DiskKVTaskDB) deletePayload(mapID string) {

}

func (tdb *DiskKVTaskDB) deleteTaskStates(mapID string) {

}

func (tdb *DiskKVTaskDB) Drain() DrainIterator {
	return &DiskKVDrainIterator{
		db: tdb.db,
	}
}

func (DiskKVTaskDB) getMapID(topicName, taskID string) string {
	return fmt.Sprintf("%s_%s", topicName, taskID)
}

// Launched in a goroutine, scans the rows and feeds a buffer into the feed chan
func attachLoad(ai *DiskKVAttachIterator) {
	// TODO: Remove log line
	log.Debug().Msg("starting attach loader")
	ai.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		buf := make([]*TaskDBTaskState, 0)
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				var state *TaskDBTaskState
				err := msgpack.Unmarshal(val, &state)
				if err != nil {
					return fmt.Errorf("error unmarshalling taskdb task state from badger bytes: %w", err)
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
