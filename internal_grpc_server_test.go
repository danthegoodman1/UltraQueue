package main

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/danthegoodman1/UltraQueue/pb"
	"github.com/danthegoodman1/UltraQueue/utils"
	"github.com/rs/zerolog/log"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestInternalGRPCServer(t *testing.T) {
	uq, err := NewUltraQueue("testpart", 100)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to start new ultra queue")
	}

	gm, err := NewGossipManager("testpart", "0.0.0.0", uq, 0, "127.0.0.1", "9999", []string{})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to start new gossip manager")
	}

	port := utils.GetEnvOrDefault("PORT", "9090")
	internalPort := utils.GetEnvOrDefault("INTERNAL_PORT", "9091")
	log.Debug().Msg("Starting cmux listener on port " + port)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatal().Err(err).Str("port", port).Msg("Failed to start cmux listener")
	}
	lisInternal, err := net.Listen("tcp", fmt.Sprintf(":%s", internalPort))
	if err != nil {
		log.Fatal().Err(err).Str("port", port).Msg("Failed to start cmux internal listener")
	}

	m := cmux.New(lis)
	mInternal := cmux.New(lisInternal)

	httpL := m.Match(cmux.HTTP2(), cmux.HTTP1Fast())
	go StartHTTPServer(httpL, uq, gm)

	go m.Serve()

	// internalGRPCListener := mInternal.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
	internalGRPCListener := mInternal.Match(cmux.HTTP2())
	go NewInternalGRPCServer(internalGRPCListener, uq, gm)

	go mInternal.Serve()

	// Test dequeue ack and nack
	conn, err := grpc.Dial("localhost:9091", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewUltraQueueInternalClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*5000)
	defer cancel()

	err = uq.Enqueue([]string{"topic1", "topic2"}, nil, 3, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Dequeue
	resp, err := client.Dequeue(ctx, &pb.DequeueRequest{
		Topic:              "topic1",
		InFlightTTLSeconds: 10,
		Tasks:              1,
	})
	if err != nil {
		t.Fatal(err)
	}

	if resp == nil {
		t.Fatal("resp 1 was nil")
	}
	if len(resp.GetTasks()) == 0 {
		t.Fatal("resp 1 was 0 len")
	}

	// Ack
	_, err = client.Ack(ctx, &pb.AckRequest{
		TaskID: resp.GetTasks()[0].ID,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Dequeue
	resp, err = client.Dequeue(ctx, &pb.DequeueRequest{
		Topic:              "topic2",
		InFlightTTLSeconds: 10,
		Tasks:              1,
	})
	if err != nil {
		t.Fatal(err)
	}

	if resp == nil {
		t.Fatal("resp 2 was nil")
	}
	if len(resp.GetTasks()) == 0 {
		t.Fatal("resp 2 was 0 len")
	}

	// Nack
	_, err = client.Nack(ctx, &pb.NackRequest{
		TaskID:       resp.GetTasks()[0].ID,
		DelaySeconds: 0,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Dequeue to verify
	resp, err = client.Dequeue(ctx, &pb.DequeueRequest{
		Topic:              "topic2",
		InFlightTTLSeconds: 10,
		Tasks:              1,
	})
	if err != nil {
		t.Fatal(err)
	}

	if resp == nil {
		t.Fatal("resp 3 was nil")
	}
	if len(resp.GetTasks()) == 0 {
		t.Fatal("resp 3 was 0 len")
	}
}
