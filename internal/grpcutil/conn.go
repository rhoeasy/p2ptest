package grpcutil

import (
	"context"
	"fmt"
	pb "p2ptest/proto"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
)

// NewClientConn 创建并等待就绪的 gRPC ClientConn
func NewClientConn(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(resolver.Get("passthrough")),
	}

	// 现代标准：grpc.NewClient
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("grpc.NewClient failed: %w", err)
	}

	// 触发连接
	conn.Connect()

	// 等待状态从 Idle 变化
	ok := conn.WaitForStateChange(ctx, connectivity.Idle)
	if !ok {
		conn.Close()
		return nil, fmt.Errorf("connection not ready in time")
	}

	return conn, nil
}

// ConnectPeerStream 建立 PeerMessageStream 双向流
func ConnectPeerStream(
	ctx context.Context,
	conn *grpc.ClientConn,
) (pb.P2PPeerService_PeerMessageStreamClient, error) {
	cli := pb.NewP2PPeerServiceClient(conn)
	return cli.PeerMessageStream(ctx)
}

// GetPeerClient 直接获取可用的 P2P Client（给 peer 包广播用）
func GetPeerClient(ctx context.Context, addr string) (pb.P2PPeerServiceClient, *grpc.ClientConn, error) {
	conn, err := NewClientConn(ctx, addr)
	if err != nil {
		return nil, nil, err
	}
	return pb.NewP2PPeerServiceClient(conn), conn, nil
}
