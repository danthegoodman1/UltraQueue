package main

import (
	"testing"
	"time"
)

func TestGossipSingleNode(t *testing.T) {
	t.Log("Testing gossip single")
	uq, err := NewUltraQueue("testpart", 100)
	gm, err := NewGossipManager("testpart", "0.0.0.0", uq, 0, []string{})
	if err != nil {
		t.Fatal(err)
	}

	gm.Shutdown()
}

func TestGossipDualNode(t *testing.T) {
	t.Log("Testing gossip double")

	uq, err := NewUltraQueue("testpart", 100)
	gm, err := NewGossipManager("testpart", "0.0.0.0", uq, 9900, []string{})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("First node set")
	uq2, err := NewUltraQueue("testpart2", 100)
	gm2, err := NewGossipManager("testpart2", "0.0.0.0", uq2, 9901, []string{"localhost:9900"})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Second node set")

	time.Sleep(time.Second * 10)
	t.Log("Directly enqueuing item")
	gm.UltraQ.enqueueTask(&Task{
		ID:               "test_task",
		Topic:            "test_topic",
		Payload:          nil,
		ExpireAt:         time.Now().Add(time.Second * 30),
		CreatedAt:        time.Now(),
		Version:          1,
		DeliveryAttempts: 0,
		Priority:         4,
	})

	time.Sleep(time.Second * 3)
	t.Log("Directly enqueuing item")
	gm.UltraQ.enqueueTask(&Task{
		ID:               "test_task2",
		Topic:            "test_topic",
		Payload:          nil,
		ExpireAt:         time.Now().Add(time.Second * 30),
		CreatedAt:        time.Now(),
		Version:          1,
		DeliveryAttempts: 0,
		Priority:         4,
	})

	time.Sleep(time.Second)
	t.Log("Dequeueing item")
	gm.UltraQ.dequeueTask("test_topic", 1, 100)

	time.Sleep(time.Second)
	t.Log("Dequeueing item")
	gm.UltraQ.dequeueTask("test_topic", 1, 100)

	time.Sleep(time.Second * 5)
	t.Log("Shutting down")
	gm.Shutdown()
	gm2.Shutdown()
}
