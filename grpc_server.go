package main

import (
	"context"
	"net"

	"github.com/danthegoodman1/UltraQueue/pb"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
)

var (
	internalGRPCServer *grpc.Server
)

type InternalGRPCServer struct {
	pb.UnsafeUltraQueueInternalServer
}

func NewInternalServer(lis net.Listener, uq *UltraQueue, gm *GossipManager) {
	internalGRPCServer = grpc.NewServer()

	pb.RegisterUltraQueueInternalServer(internalGRPCServer, &InternalGRPCServer{})
	err := internalGRPCServer.Serve(lis)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to start internal grpc server")
	}
}

func (g *InternalGRPCServer) Dequeue(context.Context, *pb.DequeueRequest) (*pb.Task, error) {

}

func (g *InternalGRPCServer) Ack(context.Context, *pb.AckRequest) (*pb.Applied, error) {

}

func (g *InternalGRPCServer) Nack(context.Context, *pb.NackRequest) (*pb.Applied, error) {

}
