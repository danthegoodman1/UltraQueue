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
	tasks := uq.dequeueTask("badtopic", 1, 10)
	if len(tasks) != 0 {
		t.Fatalf("Got %d tasks from an unknown topic", len(tasks))
	}

	// Good topic, up to 2 tasks
	tasks = uq.dequeueTask("test_topic", 2, 10)
	if len(tasks) != 1 {
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

	// Good topic, up to 2 tasks
	tasks = uq.dequeueTask("test_topic", 2, 10)
	if len(tasks) != 2 {
		t.Fatal("Missing tasks from test_topic")
	}
}
