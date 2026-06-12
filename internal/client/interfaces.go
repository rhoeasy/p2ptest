package client

import (
	"google.golang.org/grpc"

	"p2ptest/internal/types"
	pb "p2ptest/proto/p2p"
)

// PeerNode 定义 client 包对节点操作所需的最小接口。
// 用于解耦 client 包与 node.Node 的具体实现，使测试可以注入 mock。
type PeerNode interface {
	NodeIdentity
	PeerRegistryWriter
	ConnPoolManager
}

// NodeIdentity 提供节点身份信息。
type NodeIdentity interface {
	GetNodeID() *pb.NodeID
	Cfg() *types.NodeConfig
}

// PeerRegistryWriter 提供 peer 注册能力。
type PeerRegistryWriter interface {
	AddOnlinePeer(peer *pb.NodeInfo) error
}

// ConnPoolManager 管理 gRPC 连接和消息流。
type ConnPoolManager interface {
	SetPeerConn(addr string, conn *grpc.ClientConn)
	SetPeerStream(addr string, stream pb.Messaging_StreamClient)
	DeletePeerConn(addr string)
	DeletePeerStream(addr string)
	GetPeerStreams() map[string]pb.Messaging_StreamClient
}
