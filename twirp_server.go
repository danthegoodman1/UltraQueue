package main

import (
	"context"
	"net"
	"time"

	"github.com/danthegoodman1/UltraQueue/pb"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/http2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type TwirpServer struct {
	pb.UltraQueueInternal
	UQ *UltraQueue
	GM *GossipManager
}

// Dequeue(context.Context, *DequeueRequest) (*TaskResponse, error)

// 	Ack(context.Context, *AckRequest) (*Applied, error)

// 	Nack(context.Context, *NackRequest) (*Applied, error)

// 	DrainReceive(context.Context, *DrainTaskList) (*Applied, error)

func NewTwripServer(lis net.Listener, uq *UltraQueue, gm *GossipManager) (twirpServer *TwirpServer) {
	server := &TwirpServer{
		UQ: uq,
		GM: gm,
	}

	twirpHandler := pb.NewUltraQueueInternalServer(server)

	httpServer := &http2.Server{}
}

func (g *TwirpServer) Dequeue(ctx context.Context, in *pb.DequeueRequest) (*pb.TaskResponse, error) {
	tasks, err := g.UQ.Dequeue(in.GetTopic(), in.GetTasks(), in.GetInFlightTTLSeconds())
	if err != nil {
		log.Error().Err(err).Interface("body", in).Msg("failed to dequeue message from internal grpc")
		return nil, status.Error(codes.Internal, err.Error())
	}
	if len(tasks) == 0 {
		return nil, status.Error(codes.NotFound, "not found")
	}

	out := &pb.TaskResponse{
		Tasks: make([]*pb.TreeTask, 0),
	}
	for _, task := range tasks {
		out.Tasks = append(out.Tasks, &pb.TreeTask{
			ID: task.TreeID,
			Task: &pb.Task{
				ID:               task.Task.ID,
				Topic:            task.Task.Topic,
				Payload:          task.Task.Payload,
				CreatedAt:        task.Task.CreatedAt.Format(time.RFC3339Nano),
				Version:          task.Task.Version,
				DeliveryAttempts: task.Task.DeliveryAttempts,
				Priority:         task.Task.Priority,
			},
		})
	}

	return out, nil
}

func (g *TwirpServer) Ack(ctx context.Context, in *pb.AckRequest) (*pb.Applied, error) {
	err := g.UQ.Ack(in.GetTaskID())
	if err != nil {
		log.Error().Err(err).Interface("body", in).Str("partition", g.UQ.Partition).Msg("failed to ack message from internal grpc")
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.Applied{}, nil
}

func (g *TwirpServer) Nack(ctx context.Context, in *pb.NackRequest) (*pb.Applied, error) {
	err := g.UQ.Nack(in.GetTaskID(), in.GetDelaySeconds())
	if err != nil {
		log.Error().Err(err).Interface("body", in).Msg("failed to nack message from internal grpc")
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.Applied{}, nil
}

func (g *TwirpServer) DrainReceive(ctx context.Context, in *pb.DrainTaskList) (*pb.Applied, error) {
	log.Debug().Msg("Getting drain tasks...")

	taskList := in.GetTasks()
	for _, task := range taskList {
		err := g.UQ.Enqueue([]string{task.Topic}, task.Payload, task.Priority, 0)
		if err != nil {
			log.Error().Err(err).Interface("task", task).Msg("error enqueueing drain task")
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	log.Debug().Int("tasks", len(taskList)).Msg("accepted drain tasks")
	return &pb.Applied{Applied: true}, nil
}
