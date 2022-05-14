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

	err = uq.enqueueTask(&Task{
		ID:               "test_task",
		Topic:            "test_topic",
		Payload:          nil,
		ExpireAt:         time.Now().Add(time.Second * 30),
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
		Payload:          nil,
		ExpireAt:         time.Now().Add(time.Second * 30),
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
		Payload:          nil,
		ExpireAt:         time.Now().Add(time.Second * 30),
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

	err = uq.enqueueTask(&Task{
		ID:               "test_task-2",
		Topic:            "test_topic",
		Payload:          nil,
		ExpireAt:         time.Now().Add(time.Second * 30),
		CreatedAt:        time.Now(),
		Version:          1,
		DeliveryAttempts: 0,
		Priority:         4,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = uq.enqueueTask(&Task{
		ID:               "test_task-3",
		Topic:            "test_topic",
		Payload:          nil,
		ExpireAt:         time.Now().Add(time.Second * 30),
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

	err = uq.enqueueDelayedTask(&Task{
		ID:               "test_task",
		Topic:            "test_topic",
		Payload:          nil,
		ExpireAt:         time.Now().Add(time.Second * 30),
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
