package main

import (
	"context"
	"net"

	"github.com/danthegoodman1/UltraQueue/pb"
	"google.golang.org/grpc"
)

type GRPCServer struct {
	pb.UnsafeUltraQueueInternalServer
}

func NewServer(lis net.Listener, uq *UltraQueue, gm *GossipManager) *grpc.Server {
	grpcServer := grpc.NewServer()

	pb.RegisterUltraQueueInternalServer(grpcServer, &GRPCServer{})

	return grpcServer
}

func (g *GRPCServer) Dequeue(context.Context, *pb.DequeueRequest) (*pb.Task, error) {

}

func (g *GRPCServer) Ack(context.Context, *pb.AckRequest) (*pb.Applied, error) {

}

func (g *GRPCServer) Nack(context.Context, *pb.NackRequest) (*pb.Applied, error) {

}
