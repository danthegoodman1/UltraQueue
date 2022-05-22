package main

import (
	"sync"

	"github.com/google/btree"
)

type Topic struct {
	tree *btree.BTree
	mu   *sync.Mutex
	Name string
}

func NewTopic(topicName string) *Topic {
	return &Topic{
		tree: btree.New(32),
		mu:   &sync.Mutex{},
		Name: topicName,
	}
}

// Retrieves up to numTasks tasks in priority order
func (topic *Topic) Dequeue(numTasks int32) []*InTreeTask {
	tasks := make([]*InTreeTask, 0)

	topic.mu.Lock()
	defer topic.mu.Unlock()
	var count int32 = 0
	topic.tree.Descend(func(i btree.Item) bool {
		itt, _ := i.(*InTreeTask)
		tasks = append(tasks, itt)

		// Keep track of how many we have done, exit when needed
		count++
		return count < numTasks
	})

	// Delete all of the in tree tasks from the tree
	for _, itt := range tasks {
		topic.tree.Delete(itt)
	}

	return tasks
}

func (topic *Topic) Enqueue(task *InTreeTask) {
	topic.mu.Lock()
	defer topic.mu.Unlock()

	topic.tree.ReplaceOrInsert(task)
}
