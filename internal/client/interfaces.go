package client

import (
	"time"

	"google.golang.org/grpc"

	"p2ptest/internal/notifier"
	"p2ptest/internal/types"
	pb "p2ptest/proto/p2p"
)

// PeerNode 定义 client 包对节点操作所需的最小接口。
// 用于解耦 client 包与 node.Node 的具体实现，使测试可以注入 mock。
type PeerNode interface {
	NodeIdentity
	PeerRegistryWriter
	ConnPoolManager
	NotifierProvider
	PingTracker
}

// NodeIdentity 提供节点身份信息。
type NodeIdentity interface {
	GetNodeID() *pb.NodeID
	Cfg() *types.NodeConfig
}

// PeerRegistryWriter 提供 peer 注册/注销能力。
type PeerRegistryWriter interface {
	AddOnlinePeer(peer *pb.NodeInfo) error
	RemoveOnlinePeer(uuid string) (bool, error)
}

// ConnPoolManager 管理 gRPC 连接和消息流。
type ConnPoolManager interface {
	SetPeerConn(addr string, conn *grpc.ClientConn)
	SetPeerStream(addr string, stream pb.Messaging_StreamClient)
	DeletePeerConn(addr string)
	DeletePeerStream(addr string)
	HasStream(addr string) bool
	StreamAddrs() []string
	SendToStream(addr string, env *pb.Envelope) error
}

// PingTracker 管理 ping/pong 往返时间跟踪。
type PingTracker interface {
	RecordPingSent(nonce string) chan time.Duration
	CancelPendingPing(nonce string)
	HandlePongReceived(nonce string, pingTimestamp uint64)
}

// NotifierProvider 提供通知器。
type NotifierProvider interface {
	Notifier() *notifier.Notifier
}
