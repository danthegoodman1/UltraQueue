package main

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/danthegoodman1/UltraQueue/pb"
	"github.com/rs/zerolog/log"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type InternalGRPCServer struct {
	pb.UnsafeUltraQueueInternalServer
	UQ *UltraQueue
	GM *GossipManager
}

func NewInternalGRPCServer(lis net.Listener, uq *UltraQueue, gm *GossipManager) (internalGRPCServer *grpc.Server) {
	var opts []grpc.ServerOption
	internalGRPCServer = grpc.NewServer(opts...)

	pb.RegisterUltraQueueInternalServer(internalGRPCServer, &InternalGRPCServer{
		UQ: uq,
		GM: gm,
	})
	log.Info().Msg("Starting internal grpc server on " + lis.Addr().String())
	go func() {
		err := internalGRPCServer.Serve(lis)
		if err != nil && err != grpc.ErrServerStopped && err != cmux.ErrServerClosed {
			log.Fatal().Err(err).Msg("failed to start internal grpc server")
		}
	}()
	return internalGRPCServer
}

func (g *InternalGRPCServer) Dequeue(ctx context.Context, in *pb.DequeueRequest) (*pb.TaskResponse, error) {
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

func (g *InternalGRPCServer) Ack(ctx context.Context, in *pb.AckRequest) (*pb.Applied, error) {
	err := g.UQ.Ack(in.GetTaskID())
	if err != nil {
		log.Error().Err(err).Interface("body", in).Str("partition", g.UQ.Partition).Msg("failed to ack message from internal grpc")
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.Applied{}, nil
}

func (g *InternalGRPCServer) Nack(ctx context.Context, in *pb.NackRequest) (*pb.Applied, error) {
	err := g.UQ.Nack(in.GetTaskID(), in.GetDelaySeconds())
	if err != nil {
		log.Error().Err(err).Interface("body", in).Msg("failed to nack message from internal grpc")
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.Applied{}, nil
}

func (g *InternalGRPCServer) DrainReceive(stream pb.UltraQueueInternal_DrainReceiveServer) error {
	log.Debug().Msg("Getting drain tasks...")
	for {
		drainTask, err := stream.Recv()
		if err == io.EOF {
			log.Debug().Msg("got EOF, exiting")
			return stream.SendAndClose(&pb.Applied{
				Applied: true,
			})
		}
		if err != nil {
			log.Error().Err(err).Msg("error receiving drain tasks")
			return status.Error(codes.Internal, err.Error())
		}

		tasks := drainTask.GetTasks()
		for _, task := range tasks {
			// TODO: Bulk enqueue where we don't wait for each commit
			err = g.UQ.Enqueue([]string{task.Topic}, task.Payload, task.Priority, 0)
			if err != nil {
				log.Error().Err(err).Interface("task", task).Msg("error enqueueing drain task")
				return status.Error(codes.Internal, err.Error())
			}
		}
	}
}
