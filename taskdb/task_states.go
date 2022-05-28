package taskdb

import "fmt"

type TaskState int32

const (
	TASK_STATE_ENQUEUED TaskState = 0
	TASK_STATE_DELAYED  TaskState = 1
	TASK_STATE_INFLIGHT TaskState = 2
)

func (ts TaskState) String() string {
	switch ts {
	case TASK_STATE_ENQUEUED:
		return "enqueued"
	case TASK_STATE_DELAYED:
		return "delayed"
	case TASK_STATE_INFLIGHT:
		return "inflight"
	default:
		return fmt.Sprintf("%d", int(ts))
	}
}
