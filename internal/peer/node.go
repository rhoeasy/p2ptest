package peer

import (
	"context"
	"fmt"
	"net"
	"p2ptest/internal/logger"
	"p2ptest/internal/types"
	pb "p2ptest/proto"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Node struct {
	pb.UnimplementedP2PPeerServiceServer
	cfg    *types.NodeConfig
	nodeID *pb.NodeID

	onlinePeers map[string]*pb.NodeInfo     // UUID→NodeInfo
	peerConns   map[string]*grpc.ClientConn // addr-> grpcConn
	peerStreams map[string]pb.P2PPeerService_PeerMessageStreamClient
	nameToAddrs map[string][]string // 节点名称→地址映射
	lastActive  map[string]time.Time

	mu         sync.RWMutex
	grpcServer *grpc.Server
	stopChan   chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
}

func NewNode(cfg *types.NodeConfig) *Node {
	nodeInfo := buildNodeInfo(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	return &Node{
		cfg:         cfg,
		nodeID:      nodeInfo.Id,
		onlinePeers: make(map[string]*pb.NodeInfo),
		peerConns:   make(map[string]*grpc.ClientConn),
		peerStreams: make(map[string]pb.P2PPeerService_PeerMessageStreamClient),
		nameToAddrs: make(map[string][]string),
		lastActive:  make(map[string]time.Time),

		stopChan: make(chan struct{}),
		ctx:      ctx,
		cancel:   cancel,
	}
}

// ========== 节点生命周期管理 ==========
// Start 启动节点（启动gRPC服务端）
func (n *Node) Start() error {
	// 🔥 复用地址格式化
	listenAddr := formatNodeAddr(n.cfg.ListenIP, n.cfg.ListenPort)

	// 1. 打印自身核心信息
	logger.L().Info("[node] self info",
		zap.String("node_name", n.cfg.NodeName),
		zap.String("uuid", n.nodeID.Uuid),
		zap.String("listen_addr", listenAddr),
		zap.Uint32("proto_version", n.cfg.ProtoVer),
	)

	// 添加自身名称→地址映射
	n.AddNameAddrMapping(n.cfg.NodeName, listenAddr)

	// 2. 启动gRPC服务
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen %v, err: %v", listenAddr, err)
	}

	n.grpcServer = grpc.NewServer()
	pb.RegisterP2PPeerServiceServer(n.grpcServer, n)

	// 3. 异步启动gRPC服务
	go func() {
		if err := n.grpcServer.Serve(lis); err != nil {
			logger.L().Fatal("[node] failed to serve", zap.Error(err))
		}
	}()

	// 启动后台任务
	n.startPeerCleaner()
	n.startHeartbeatLoop()

	logger.L().Info("[node] start successfully",
		zap.String("node_name", n.cfg.NodeName),
		zap.String("listen_addr", listenAddr),
	)
	return nil
}

// Stop 停止节点
func (n *Node) Stop() {
	// 1. 先拿在线节点列表（读锁）
	n.mu.RLock()
	allPeers := make([]*pb.NodeInfo, 0, len(n.onlinePeers))
	for _, p := range n.onlinePeers {
		allPeers = append(allPeers, p)
	}
	n.mu.RUnlock()

	// 2. 同步发送 Leave 通知
	for _, p := range allPeers {
		if p.Id.Uuid == n.nodeID.Uuid {
			continue
		}

		// 🔥 复用工具函数获取地址
		addr, err := getPeerFirstAddr(p)
		if err != nil {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		// 拨号并发送Leave
		conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure())
		if err == nil {
			cli := pb.NewP2PPeerServiceClient(conn)
			_, _ = cli.Leave(ctx, n.nodeID)
			conn.Close()
			logger.L().Info("[exit] 同步发送Leave成功", zap.String("to", addr))
		}
	}

	// 3. 关闭全局上下文与通道
	n.cancel()
	close(n.stopChan)

	n.mu.Lock()
	defer n.mu.Unlock()

	// 关闭grpc服务
	if n.grpcServer != nil {
		n.grpcServer.GracefulStop()
		logger.L().Info("[node] grpc server stopped")
	}

	// 关闭所有peer连接
	for _, conn := range n.peerConns {
		conn.Close()
	}
	// 关闭流
	for _, stream := range n.peerStreams {
		_ = stream.CloseSend()
	}

	// 清空本地所有数据
	n.peerConns = make(map[string]*grpc.ClientConn)
	n.peerStreams = make(map[string]pb.P2PPeerService_PeerMessageStreamClient)
	n.onlinePeers = make(map[string]*pb.NodeInfo)
	n.lastActive = make(map[string]time.Time)
	n.nameToAddrs = make(map[string][]string)

	logger.L().Info("[node] node stopped")
}

// GetNodeID 获取节点ID（对外暴露）
func (n *Node) GetNodeID() *pb.NodeID {
	return n.nodeID
}

// GetPeerStreams 获取双向流映射
func (n *Node) GetPeerStreams() map[string]pb.P2PPeerService_PeerMessageStreamClient {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.peerStreams
}

// Cfg 获取节点配置
func (n *Node) Cfg() *types.NodeConfig {
	return n.cfg
}

// ToPbTimestamp 暴露给client包使用
func ToPbTimestamp() *timestamppb.Timestamp {
	return toPbTimestamp()
}

// GetTimestampMs 暴露给client包使用
func GetTimestampMs() uint64 {
	return getTimestampMs()
}

// GenMsgID 暴露给client包使用
func GenMsgID() string {
	return genMsgID()
}
