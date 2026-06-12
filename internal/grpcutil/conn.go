package grpcutil

import (
	"context"
	"fmt"
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

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("grpc.NewClient failed: %w", err)
	}

	conn.Connect()

	ok := conn.WaitForStateChange(ctx, connectivity.Idle)
	if !ok {
		conn.Close()
		return nil, fmt.Errorf("connection not ready in time")
	}

	return conn, nil
}
