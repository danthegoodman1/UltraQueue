package main

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
