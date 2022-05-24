package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/soheilhy/cmux"
)

func TestRemoteAck(t *testing.T) {
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

	internalGRPCListener1 := mInternal1.Match(cmux.HTTP2())
	go NewInternalGRPCServer(internalGRPCListener1, uq1, gm1)

	go mInternal1.Serve()

	m2 := cmux.New(lis2)
	mInternal2 := cmux.New(lisInternal2)

	httpL2 := m2.Match(cmux.HTTP2(), cmux.HTTP1Fast())
	go StartHTTPServer(httpL2, uq2, gm2)

	go m2.Serve()

	internalGRPCListener2 := mInternal2.Match(cmux.HTTP2())
	go NewInternalGRPCServer(internalGRPCListener2, uq2, gm2)

	go mInternal2.Serve()

	// ---------------------------------------------------------------------------
	// Run the test
	// ---------------------------------------------------------------------------

	// Enqueue a message on node 1
	err = uq1.Enqueue([]string{"topic1", "topic2"}, nil, 3, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Dequeue the message
	tasks, err := uq1.Dequeue("topic1", 1, 10)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for gossip propagation
	time.Sleep(time.Millisecond * 4000)

	bodyStruct := HTTPAckRequest{
		TaskID: tasks[0].TreeID,
	}

	b, err := json.Marshal(&bodyStruct)
	if err != nil {
		t.Fatal(err)
	}

	// Check node 2 known partitions
	req, err := http.NewRequest("GET", fmt.Sprintf("http://localhost:%s/debug/remotePartitions.json", httpPort2), nil)
	if err != nil {
		t.Fatal(err)
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	remotePartitions, _ := ioutil.ReadAll(res.Body)
	t.Log("got remote partitions: ", string(remotePartitions))

	// Dequeue from node 2
	req, err = http.NewRequest("POST", fmt.Sprintf("http://localhost:%s/ack", httpPort2), bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Add("content-type", "application/json")

	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode > 299 {
		msg, _ := ioutil.ReadAll(res.Body)
		t.Fatalf("Got high status code %d: %s", res.StatusCode, string(msg))
	}
	t.Log("success!")
}

func TestRemoteNack(t *testing.T) {
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

	internalGRPCListener1 := mInternal1.Match(cmux.HTTP2())
	go NewInternalGRPCServer(internalGRPCListener1, uq1, gm1)

	go mInternal1.Serve()

	m2 := cmux.New(lis2)
	mInternal2 := cmux.New(lisInternal2)

	httpL2 := m2.Match(cmux.HTTP2(), cmux.HTTP1Fast())
	go StartHTTPServer(httpL2, uq2, gm2)

	go m2.Serve()

	internalGRPCListener2 := mInternal2.Match(cmux.HTTP2())
	go NewInternalGRPCServer(internalGRPCListener2, uq2, gm2)

	go mInternal2.Serve()

	// ---------------------------------------------------------------------------
	// Run the test
	// ---------------------------------------------------------------------------

	// Enqueue a message on node 1
	err = uq1.Enqueue([]string{"topic1", "topic2"}, nil, 3, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Dequeue the message
	tasks, err := uq1.Dequeue("topic1", 1, 10)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for gossip propagation
	time.Sleep(time.Millisecond * 4000)

	delay := int32(10)
	bodyStruct := HTTPNackRequest{
		TaskID:       tasks[0].TreeID,
		DelaySeconds: &delay,
	}

	b, err := json.Marshal(&bodyStruct)
	if err != nil {
		t.Fatal(err)
	}

	// Check node 2 known partitions
	req, err := http.NewRequest("GET", fmt.Sprintf("http://localhost:%s/debug/remotePartitions.json", httpPort2), nil)
	if err != nil {
		t.Fatal(err)
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	remotePartitions, _ := ioutil.ReadAll(res.Body)
	t.Log("got remote partitions: ", string(remotePartitions))

	// Dequeue from node 2
	req, err = http.NewRequest("POST", fmt.Sprintf("http://localhost:%s/nack", httpPort2), bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Add("content-type", "application/json")

	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode > 299 {
		msg, _ := ioutil.ReadAll(res.Body)
		t.Fatalf("Got high status code %d: %s", res.StatusCode, string(msg))
	}
	t.Log("success!")
}
