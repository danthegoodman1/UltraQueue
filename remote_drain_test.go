package main

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/soheilhy/cmux"
)

func TestRemoteDrain(t *testing.T) {
	// ---------------------------------------------------------------------------
	// Setup the nodes
	// ---------------------------------------------------------------------------

	httpPort1 := "9080"
	httpPort2 := "9081"
	grpcPort1 := "9090"
	grpcPort2 := "9091"
	partition1 := "part1"
	partition2 := "part2"
	gossipPort1 := 9070
	gossipPort2 := 9071

	uq1, err := NewUltraQueue(partition1, 100)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to start new ultra queue")
	}

	gm1, err := NewGossipManager(partition1, "0.0.0.0", uq1, gossipPort1, "127.0.0.1", grpcPort1, []string{})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to start new gossip manager")
	}

	uq2, err := NewUltraQueue(partition2, 100)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to start new ultra queue")
	}

	gm2, err := NewGossipManager(partition2, "0.0.0.0", uq2, gossipPort2, "127.0.0.1", grpcPort2, []string{fmt.Sprintf("localhost:%d", gossipPort1)})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to start new gossip manager")
	}

	lis1, err := net.Listen("tcp", fmt.Sprintf(":%s", httpPort1))
	if err != nil {
		log.Fatal().Err(err).Str("port", httpPort1).Msg("Failed to start cmux listener")
	}
	lisInternal1, err := net.Listen("tcp", fmt.Sprintf(":%s", grpcPort1))
	if err != nil {
		log.Fatal().Err(err).Str("port", grpcPort1).Msg("Failed to start cmux internal listener")
	}

	lis2, err := net.Listen("tcp", fmt.Sprintf(":%s", httpPort2))
	if err != nil {
		log.Fatal().Err(err).Str("port", httpPort2).Msg("Failed to start cmux listener")
	}
	lisInternal2, err := net.Listen("tcp", fmt.Sprintf(":%s", grpcPort2))
	if err != nil {
		log.Fatal().Err(err).Str("port", grpcPort2).Msg("Failed to start cmux internal listener")
	}

	m1 := cmux.New(lis1)
	mInternal1 := cmux.New(lisInternal1)

	httpL1 := m1.Match(cmux.HTTP2(), cmux.HTTP1Fast())
	go StartHTTPServer(httpL1, uq1, gm1)

	go m1.Serve()
	defer m1.Close()

	internalGRPCListener1 := mInternal1.Match(cmux.HTTP2())
	go NewInternalGRPCServer(internalGRPCListener1, uq1, gm1)

	go mInternal1.Serve()
	defer mInternal1.Close()

	m2 := cmux.New(lis2)
	mInternal2 := cmux.New(lisInternal2)

	httpL2 := m2.Match(cmux.HTTP2(), cmux.HTTP1Fast())
	go StartHTTPServer(httpL2, uq2, gm2)

	go m2.Serve()
	defer m2.Close()

	internalGRPCListener2 := mInternal2.Match(cmux.HTTP2())
	go NewInternalGRPCServer(internalGRPCListener2, uq2, gm2)

	go mInternal2.Serve()
	defer mInternal2.Close()

	// ---------------------------------------------------------------------------
	// Run the test
	// ---------------------------------------------------------------------------

	// Enqueue a message on partition 1
	err = uq1.Enqueue([]string{"topic1", "topic2"}, "hey this is a payload", 3, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for gossip propagation
	time.Sleep(time.Millisecond * 400)

	// Drain the partition
	t.Log("shutting down partition 1 to drain")
	gm1.Shutdown(true)
	t.Log("finished shutting down partition 1")
	uq1.Shutdown()

	// Check partition 2
	tasks, err := uq2.Dequeue("topic1", 1, 10)
	if err != nil {
		t.Fatal(err)
	}

	if len(tasks) == 0 || tasks[0].Task.Payload != "hey this is a payload" {
		t.Fatal("did not get the task from the second partition")
	}
	t.Logf("%+v", tasks[0])

	gm2.Shutdown(false)
	uq2.Shutdown()

	t.Log("success!")
}
