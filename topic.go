package main

import (
	"sync"

	"github.com/google/btree"
)

type Topic struct {
	tree *btree.BTree
	mu   *sync.Mutex
}

func NewTopic(topicName string) *Topic {
	return &Topic{
		tree: btree.New(3),
		mu:   &sync.Mutex{},
	}
}

// Retrieves up to numTasks tasks
func (topic *Topic) Dequeue(numTasks int64) []*InTreeTask {
	items := make([]*InTreeTask, 0)

	topic.mu.Lock()
	defer topic.mu.Unlock()
	// TODO: Iterate and delete greatest numTasks items, or until the end
	return items
}

func (topic *Topic) Enqueue(task *InTreeTask) {
	topic.mu.Lock()
	defer topic.mu.Lock()

	topic.tree.ReplaceOrInsert(task)
}
