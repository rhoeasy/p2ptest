package client

import (
	"context"
	"fmt"
	"time"

	"p2ptest/internal/grpcutil"
	"p2ptest/internal/logger"
	pb "p2ptest/proto/p2p"
	"go.uber.org/zap"
)

// GossipWithPeer queries a peer for its known peers list using Discovery.GetPeers RPC.
func GossipWithPeer(n PeerNode, peerAddr string) ([]*pb.NodeInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpcutil.NewClientConn(ctx, peerAddr)
	if err != nil {
		return nil, fmt.Errorf("connect to %s for gossip failed: %w", peerAddr, err)
	}
	defer conn.Close()

	cli := pb.NewDiscoveryClient(conn)
	resp, err := cli.GetPeers(ctx, &pb.GetPeersReq{})
	if err != nil {
		return nil, fmt.Errorf("GetPeers RPC failed: %w", err)
	}

	logger.L().Debug("[client] gossip response", zap.String("addr", peerAddr), zap.Int("peers", len(resp.Peers)))
	return resp.Peers, nil
}

// FindNode calls Discovery.FindNode RPC on a specific peer to look up a node by UUID.
func FindNode(peerAddr string, targetUUID string) (*pb.NodeInfo, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpcutil.NewClientConn(ctx, peerAddr)
	if err != nil {
		return nil, false, fmt.Errorf("connect to %s for FindNode failed: %w", peerAddr, err)
	}
	defer conn.Close()

	cli := pb.NewDiscoveryClient(conn)
	resp, err := cli.FindNode(ctx, &pb.FindNodeReq{Target: &pb.NodeID{Uuid: targetUUID}})
	if err != nil {
		return nil, false, fmt.Errorf("FindNode RPC failed: %w", err)
	}

	return resp.Node, resp.Found, nil
}
