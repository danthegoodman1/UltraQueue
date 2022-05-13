package main

import (
	"github.com/google/btree"
)

// Simply a task with a sorting ID for a given tree
type InTreeTask struct {
	TreeID string
	Task   *Task
}

func NewInTreeTask(treeID string, task *Task) *InTreeTask {
	return &InTreeTask{
		Task:   task,
		TreeID: treeID,
	}
}

// For the btree package
func (itt *InTreeTask) Less(than btree.Item) bool {
	temp, _ := than.(*InTreeTask)
	return itt.TreeID < temp.TreeID
}
