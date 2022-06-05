package main

import (
	"fmt"
	"testing"
	"time"
)

func TestGossipSingleNode(t *testing.T) {
	t.Log("Testing gossip single")
	uq, err := NewUltraQueue("testpart", 100)
	if err != nil {
		t.Fatal(err)
	}
	gm, err := NewGossipManager("testpart", "0.0.0.0", uq, 0, "127.0.0.1", "9999", []string{})
	if err != nil {
		t.Fatal(err)
	}

	gm.Shutdown(false)
}

func TestGossipDualNode(t *testing.T) {
	gossipPort1 := 9800
	gossipPort2 := 9801

	t.Log("Testing gossip double")

	uq, err := NewUltraQueue("testpart", 100)
	if err != nil {
		t.Fatal(err)
	}
	gm, err := NewGossipManager("testpart", "0.0.0.0", uq, gossipPort1, "127.0.0.1", "9990", []string{})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("First node set")
	uq2, err := NewUltraQueue("testpart2", 100)
	if err != nil {
		t.Fatal(err)
	}
	gm2, err := NewGossipManager("testpart2", "0.0.0.0", uq2, gossipPort2, "127.0.0.1", "9991", []string{fmt.Sprintf("localhost:%d", gossipPort1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Second node set")

	time.Sleep(time.Second * 10)
	t.Log("Directly enqueuing item")
	gm.UltraQ.enqueueTask(&Task{
		ID:               "test_task",
		Topic:            "test_topic",
		Payload:          "",
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
		Payload:          "",
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
	gm.Shutdown(false)
	time.Sleep(time.Second * 2)
	gm2.Shutdown(false)
}
