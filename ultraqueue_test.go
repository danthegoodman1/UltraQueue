package main

import (
	"testing"
	"time"
)

func TestEnqueueDequeue(t *testing.T) {
	uq, err := NewUltraQueue("testpart", 100)
	if err != nil {
		t.Fatal(err)
	}

	uq.enqueueTask(&Task{
		ID:               "test_task",
		Topic:            "test_topic",
		Payload:          "",
		CreatedAt:        time.Now(),
		Version:          1,
		DeliveryAttempts: 0,
		Priority:         4,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Bad topic
	tasks, err := uq.dequeueTask("badtopic", 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 0 {
		t.Fatalf("Got %d tasks from an unknown topic", len(tasks))
	}

	// Good topic, up to 2 tasks
	tasks, err = uq.dequeueTask("test_topic", 2, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 {
		t.Fatal("Missing tasks from test_topic")
	}

	uq.enqueueTask(&Task{
		ID:               "test_task-2",
		Topic:            "test_topic",
		Payload:          "",
		CreatedAt:        time.Now(),
		Version:          1,
		DeliveryAttempts: 0,
		Priority:         4,
	})
	if err != nil {
		t.Fatal(err)
	}

	uq.enqueueTask(&Task{
		ID:               "test_task-3",
		Topic:            "test_topic",
		Payload:          "",
		CreatedAt:        time.Now(),
		Version:          1,
		DeliveryAttempts: 0,
		Priority:         4,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Good topic, up to 2 tasks
	tasks, err = uq.dequeueTask("test_topic", 2, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 2 {
		t.Fatal("Missing tasks from test_topic")
	}

	uq.enqueueTask(&Task{
		ID:               "test_task-2",
		Topic:            "test_topic",
		Payload:          "",
		CreatedAt:        time.Now(),
		Version:          1,
		DeliveryAttempts: 0,
		Priority:         4,
	})
	if err != nil {
		t.Fatal(err)
	}

	uq.enqueueTask(&Task{
		ID:               "test_task-3",
		Topic:            "test_topic",
		Payload:          "",
		CreatedAt:        time.Now(),
		Version:          1,
		DeliveryAttempts: 0,
		Priority:         4,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Only pull 1 task
	tasks, err = uq.dequeueTask("test_topic", 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 {
		t.Fatal("Too many tasks from test_topic")
	}

	t.Log("Shuting down...")
	uq.Shutdown()
	t.Log("Shut down")
}

func TestDelayedEnqueue(t *testing.T) {
	uq, err := NewUltraQueue("testpart", 100)
	if err != nil {
		t.Fatal(err)
	}

	uq.enqueueDelayedTask(&Task{
		ID:               "test_task",
		Topic:            "test_topic",
		Payload:          "",
		CreatedAt:        time.Now(),
		Version:          1,
		DeliveryAttempts: 0,
		Priority:         4,
	}, 5)
	if err != nil {
		t.Fatal(err)
	}

	// Should not be ready yet
	tasks, err := uq.dequeueTask("test_topic", 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(tasks)
	if len(tasks) != 0 {
		t.Fatal("Got a task when I should not have")
	}

	time.Sleep(time.Millisecond * 1500)

	// Should not be ready yet
	tasks, err = uq.dequeueTask("test_topic", 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(tasks)
	if len(tasks) != 0 {
		t.Fatal("Got a task when I should not have")
	}

	time.Sleep(time.Millisecond * 3500)

	// Should be ready now
	tasks, err = uq.dequeueTask("test_topic", 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(tasks)
	if len(tasks) != 1 {
		t.Fatal("Got no tasks when I should have")
	}

	// Should be empty
	tasks, err = uq.dequeueTask("test_topic", 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(tasks)
	if len(tasks) != 0 {
		t.Fatal("Got a task when I shouldn't have")
	}
}

func TestAck(t *testing.T) {
	uq, err := NewUltraQueue("testpart", 100)
	if err != nil {
		t.Fatal(err)
	}

	err = uq.Enqueue([]string{"topic1", "topic2"}, "hey this is a payload", 3, 0)
	if err != nil {
		t.Fatal(err)
	}

	tasks, err := uq.Dequeue("topic2", 1, 2)
	if err != nil {
		t.Fatal(err)
	}

	if len(tasks) != 1 {
		t.Fatal("Did not get a task")
	}

	tasks, err = uq.Dequeue("topic1", 1, 2)
	if err != nil {
		t.Fatal(err)
	}

	if len(tasks) != 1 {
		t.Fatal("Did not get a task")
	}

	t.Log(tasks)

	t.Logf("Acking task %+v %+v", tasks[0], tasks[0].Task.Payload)
	err = uq.Ack(tasks[0].TreeID)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Sleeping for 2s")
	time.Sleep(time.Second * 2)
	t.Log("Checking dequeue")

	tasks, err = uq.Dequeue("topic1", 1, 2)
	if err != nil {
		t.Fatal(err)
	}

	if len(tasks) != 0 {
		t.Fatal("Task did not ack")
	}

	t.Log("Sleeping for 1s")
	time.Sleep(time.Second * 1)
	t.Log("Checking dequeue")

	tasks, err = uq.Dequeue("topic2", 1, 2)
	if err != nil {
		t.Fatal(err)
	}

	if len(tasks) != 1 {
		t.Fatal("Task did not timeout for inflight")
	}
}

func TestNack(t *testing.T) {
	uq, err := NewUltraQueue("testpart", 100)
	if err != nil {
		t.Fatal(err)
	}

	err = uq.Enqueue([]string{"topic1", "topic2"}, "hey this is a payload", 3, 0)
	if err != nil {
		t.Fatal(err)
	}

	tasks, err := uq.Dequeue("topic2", 1, 2)
	if err != nil {
		t.Fatal(err)
	}

	if len(tasks) != 1 {
		t.Fatal("Did not get a task")
	}

	t.Logf("Nacking task %+v %+v", tasks[0], tasks[0].Task.Payload)
	err = uq.Nack(tasks[0].TreeID, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Should be immediately available again
	newTasks, err := uq.Dequeue("topic2", 1, 0)
	if err != nil {
		t.Fatal(err)
	}

	if newTasks[0].Task.ID != tasks[0].Task.ID {
		t.Fatal("Did not get task immediately after dequeue")
	}

	t.Log(newTasks)

	// Test nack with delay
	tasks, err = uq.Dequeue("topic1", 1, 2)
	if err != nil {
		t.Fatal(err)
	}

	err = uq.Nack(tasks[0].TreeID, 2)
	if err != nil {
		t.Fatal(err)
	}

	tasks, err = uq.Dequeue("topic1", 1, 2)
	if err != nil {
		t.Fatal(err)
	}

	if len(tasks) != 0 {
		t.Fatal("Nack did not delay")
	}

	t.Log("Sleeping for 3s")
	time.Sleep(time.Second * 3)
	t.Log("Checking dequeue")

	tasks, err = uq.Dequeue("topic1", 1, 2)
	if err != nil {
		t.Fatal(err)
	}

	if len(tasks) != 1 {
		t.Fatal("Task not found after delay")
	}
}
