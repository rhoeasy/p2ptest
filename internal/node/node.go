package node

import (
	"context"
	"fmt"
	"net"
	discoveryApp "p2ptest/internal/discovery/application"
	discoveryDomain "p2ptest/internal/discovery/domain"
	"p2ptest/internal/logger"
	membershipApp "p2ptest/internal/membership/application"
	messagingApp "p2ptest/internal/messaging/application"
	"p2ptest/internal/transport"
	"p2ptest/internal/types"
	pb "p2ptest/proto/p2p"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Node 是聚合根，协调三个限界上下文（Discovery, Membership, Messaging）。
type Node struct {
	cfg      *types.NodeConfig
	nodeID   *pb.NodeID
	selfInfo *pb.NodeInfo

	registry discoveryDomain.PeerRegistry
	connPool *transport.ConnPool

	discoverySvc  *discoveryApp.DiscoveryService
	membershipSvc *membershipApp.MembershipService
	messagingSvc  *messagingApp.MessagingService

	mu         sync.RWMutex
	grpcServer *grpc.Server
	stopChan   chan struct{}
}

func NewNode(cfg *types.NodeConfig) *Node {
	selfInfo := buildNodeInfo(cfg)
	stopChan := make(chan struct{})
	registry := discoveryDomain.NewPeerRegistry(selfInfo.Id.Uuid)

	discoverySvc := discoveryApp.NewDiscoveryService(registry)
	membershipSvc := membershipApp.NewMembershipService(registry, selfInfo, cfg)
	messagingSvc := messagingApp.NewMessagingService()

	return &Node{
		cfg:           cfg,
		nodeID:        selfInfo.Id,
		selfInfo:      selfInfo,
		registry:      registry,
		connPool:      transport.NewConnPool(),
		discoverySvc:  discoverySvc,
		membershipSvc: membershipSvc,
		messagingSvc:  messagingSvc,
		stopChan:      stopChan,
	}
}

// Start 启动节点（启动 gRPC 服务端，注册三个 DDD 上下文服务）
func (n *Node) Start() error {
	listenAddr := formatNodeAddr(n.cfg.ListenIP, n.cfg.ListenPort)

	logger.L().Info("[node] self info",
		zap.String("node_name", n.cfg.NodeName),
		zap.String("uuid", n.nodeID.Uuid),
		zap.String("listen_addr", listenAddr),
		zap.Uint32("proto_version", n.cfg.ProtoVer),
	)

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen %v, err: %v", listenAddr, err)
	}

	n.grpcServer = grpc.NewServer()

	// 注册三个 DDD 限界上下文服务
	pb.RegisterDiscoveryServer(n.grpcServer, n.discoverySvc)
	pb.RegisterMembershipServer(n.grpcServer, n.membershipSvc)
	pb.RegisterMessagingServer(n.grpcServer, n.messagingSvc)

	go func() {
		if err := n.grpcServer.Serve(lis); err != nil {
			logger.L().Fatal("[node] failed to serve", zap.Error(err))
		}
	}()

	logger.L().Info("[node] start successfully",
		zap.String("node_name", n.cfg.NodeName),
		zap.String("listen_addr", listenAddr),
	)
	return nil
}

// Stop 停止节点
func (n *Node) Stop() {
	// 向所有 peer 发送 Disconnect
	allPeers := n.registry.List()
	for _, p := range allPeers {
		n.sendDisconnectToPeer(p)
	}

	close(n.stopChan)

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.grpcServer != nil {
		n.grpcServer.GracefulStop()
		logger.L().Info("[node] grpc server stopped")
	}

	n.connPool.CloseAll()

	logger.L().Info("[node] node stopped")
}

func (n *Node) sendDisconnectToPeer(p *pb.NodeInfo) {
	if p == nil || p.Id == nil || p.Id.Uuid == n.nodeID.Uuid {
		return
	}

	addr, err := getPeerFirstAddr(p)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure())
	if err != nil {
		return
	}
	defer conn.Close()

	cli := pb.NewMembershipClient(conn)
	_, _ = cli.Disconnect(ctx, &pb.DisconnectReq{NodeId: n.nodeID, Reason: "node stopping"})
}

// GetNodeID 获取节点 ID
func (n *Node) GetNodeID() *pb.NodeID {
	return n.nodeID
}

// Cfg 获取节点配置
func (n *Node) Cfg() *types.NodeConfig {
	return n.cfg
}

func (n *Node) GetOnlinePeers() []map[string]string {
	peers := n.registry.List()
	result := make([]map[string]string, 0, len(peers))
	for _, p := range peers {
		peerMap := map[string]string{
			"name": p.Id.Name,
			"uuid": p.Id.Uuid,
		}
		if len(p.Addrs) > 0 {
			peerMap["addr"] = formatNodeAddr(p.Addrs[0].Ip, p.Addrs[0].Port)
		}
		result = append(result, peerMap)
	}
	return result
}

func (n *Node) GetAddrByName(name string) (string, error) {
	return n.registry.GetAddrByName(name)
}

func (n *Node) AddOnlinePeer(peer *pb.NodeInfo) error {
	return n.registry.Register(peer)
}

func (n *Node) GetPeerStreams() map[string]pb.Messaging_StreamClient {
	return n.connPool.GetStreamsCopy()
}

func (n *Node) SetPeerConn(addr string, conn *grpc.ClientConn) {
	n.connPool.SetConn(addr, conn)
}

func (n *Node) SetPeerStream(addr string, stream pb.Messaging_StreamClient) {
	n.connPool.SetStream(addr, stream)
}

func (n *Node) DeletePeerConn(addr string) {
	n.connPool.DeleteConn(addr)
}

func (n *Node) DeletePeerStream(addr string) {
	n.connPool.DeleteStream(addr)
}

func buildNodeInfo(cfg *types.NodeConfig) *pb.NodeInfo {
	return &pb.NodeInfo{
		Id: &pb.NodeID{
			Uuid: uuid.NewString(),
			Name: cfg.NodeName,
		},
		Addrs: []*pb.NodeAddr{
			{
				Ip:   cfg.ListenIP,
				Port: cfg.ListenPort,
			},
		},
		Status: pb.NodeStatus_ONLINE,
	}
}

func formatNodeAddr(ip string, port uint32) string {
	return fmt.Sprintf("%s:%d", ip, port)
}

func getPeerFirstAddr(peer *pb.NodeInfo) (string, error) {
	if peer == nil || len(peer.Addrs) == 0 {
		return "", types.ErrNoValidNodeAddress
	}
	return formatNodeAddr(peer.Addrs[0].Ip, peer.Addrs[0].Port), nil
}
