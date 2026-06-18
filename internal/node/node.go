package node

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"p2ptest/internal/client"
	discoveryApp "p2ptest/internal/discovery/application"
	discoveryDomain "p2ptest/internal/discovery/domain"
	"p2ptest/internal/grpcutil"
	"p2ptest/internal/logger"
	membershipApp "p2ptest/internal/membership/application"
	messagingApp "p2ptest/internal/messaging/application"
	"p2ptest/internal/notifier"
	"p2ptest/internal/transport"
	"p2ptest/internal/types"
	pb "p2ptest/proto/p2p"
	"strconv"
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
	notifier *notifier.Notifier

	discoverySvc  *discoveryApp.DiscoveryService
	membershipSvc *membershipApp.MembershipService
	messagingSvc  *messagingApp.MessagingService

	mu         sync.RWMutex
	grpcServer *grpc.Server
	stopChan   chan struct{}

	// Configurable intervals for testing
	heartbeatInterval time.Duration
	cleanInterval     time.Duration
	gossipInterval    time.Duration

	pendingPings map[string]chan time.Duration
	pingMu       sync.Mutex
	statusMu     sync.RWMutex
}

func NewNode(cfg *types.NodeConfig) *Node {
	selfInfo := buildNodeInfo(cfg)
	stopChan := make(chan struct{})
	registry := discoveryDomain.NewPeerRegistry(selfInfo.Id.Uuid)

	// Create notifier with buffer size 100
	notifier := notifier.NewNotifier(100)

	discoverySvc := discoveryApp.NewDiscoveryService(registry)
	membershipSvc := membershipApp.NewMembershipService(registry, selfInfo, cfg, notifier)
	messagingSvc := messagingApp.NewMessagingService(selfInfo, notifier)

	n := &Node{
		cfg:               cfg,
		nodeID:            selfInfo.Id,
		selfInfo:          selfInfo,
		registry:          registry,
		connPool:          transport.NewConnPool(),
		notifier:          notifier,
		discoverySvc:      discoverySvc,
		membershipSvc:     membershipSvc,
		messagingSvc:      messagingSvc,
		stopChan:          stopChan,
		heartbeatInterval: time.Duration(types.HeartbeatInterval) * time.Millisecond,
		cleanInterval:     types.CleanInterval,
		gossipInterval:    time.Duration(types.GossipInterval) * time.Millisecond,
		pendingPings:      make(map[string]chan time.Duration),
	}

	membershipSvc.SetStatusGetter(n.GetStatusPB)

	return n
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
			logger.L().Warn("[node] grpc server stopped", zap.Error(err))
		}
	}()

	logger.L().Info("[node] start successfully",
		zap.String("node_name", n.cfg.NodeName),
		zap.String("listen_addr", listenAddr),
	)

	n.startHeartbeatLoop()
	n.startPeerCleaner()
	n.startGossipLoop()

	// Subscribe to peer_discovered to broadcast NotifyNodeJoin to all connected peers
	n.notifier.Subscribe(func(notif notifier.Notification) {
		if notif.Type == "peer_discovered" {
			n.broadcastNodeJoin(notif)
		}
	})

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

	conn, err := grpcutil.NewClientConn(ctx, addr)
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
	streams := n.connPool.GetStreamsCopy()
	result := make([]map[string]string, 0, len(peers))
	for _, p := range peers {
		peerMap := map[string]string{
			"name": p.Id.Name,
			"uuid": p.Id.Uuid,
		}
		if len(p.Addrs) > 0 {
			peerMap["addr"] = formatNodeAddr(p.Addrs[0].Ip, p.Addrs[0].Port)
		}
		peerMap["status"] = statusToString(p.Status)
		if lastActive, ok := n.registry.GetLastActive(p.Id.Uuid); ok {
			peerMap["last_active"] = lastActive.Format("15:04:05")
		}
		if registeredAt, ok := n.registry.GetRegisteredAt(p.Id.Uuid); ok {
			peerMap["online_for"] = formatDuration(time.Since(registeredAt))
		}
		if len(p.Addrs) > 0 {
			addr := formatNodeAddr(p.Addrs[0].Ip, p.Addrs[0].Port)
			if _, connected := streams[addr]; connected {
				peerMap["stream"] = "已连接"
			} else {
				peerMap["stream"] = "未连接"
			}
		}
		result = append(result, peerMap)
	}
	return result
}

// DisconnectPeer actively disconnects from a peer by name.
// Returns the peer's address if found, or an error.
func (n *Node) DisconnectPeer(name string) (string, error) {
	peer, found := n.registry.GetByName(name)
	if !found {
		return "", fmt.Errorf("节点「%s」不在线", name)
	}

	addr, err := getPeerFirstAddr(peer)
	if err != nil {
		return "", err
	}

	n.sendDisconnectToPeer(peer)
	if removed, _ := n.registry.Unregister(peer.Id.Uuid); removed {
		n.connPool.CloseByAddr(addr)
		n.notifier.Emit(notifier.NewPeerOfflineNotification(name, "主动断开"))
	}

	return addr, nil
}

// formatDuration formats a duration in human-readable form.
func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
}

func (n *Node) GetAddrByName(name string) (string, error) {
	return n.registry.GetAddrByName(name)
}

func (n *Node) AddOnlinePeer(peer *pb.NodeInfo) error {
	return n.registry.Register(peer)
}

func (n *Node) RemoveOnlinePeer(uuid string) (bool, error) {
	return n.registry.Unregister(uuid)
}

func (n *Node) HasStream(addr string) bool {
	return n.connPool.HasStream(addr)
}

func (n *Node) StreamAddrs() []string {
	return n.connPool.StreamAddrs()
}

func (n *Node) SendToStream(addr string, env *pb.Envelope) error {
	return n.connPool.SendToStream(addr, env)
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

// Notifier returns the node's notifier for subscribing to notifications.
func (n *Node) Notifier() *notifier.Notifier {
	return n.notifier
}

func (n *Node) RecordPingSent(nonce string) chan time.Duration {
	n.pingMu.Lock()
	defer n.pingMu.Unlock()
	ch := make(chan time.Duration, 1)
	n.pendingPings[nonce] = ch
	return ch
}

func (n *Node) CancelPendingPing(nonce string) {
	n.pingMu.Lock()
	defer n.pingMu.Unlock()
	delete(n.pendingPings, nonce)
}

func (n *Node) HandlePongReceived(nonce string, pingTimestamp uint64) {
	n.pingMu.Lock()
	ch, ok := n.pendingPings[nonce]
	if ok {
		delete(n.pendingPings, nonce)
	}
	n.pingMu.Unlock()

	if !ok {
		return
	}

	rtt := time.Since(time.UnixMilli(int64(pingTimestamp)))
	select {
	case ch <- rtt:
	default:
	}
}

// startHeartbeatLoop starts a goroutine that periodically sends heartbeats to all peers
func (n *Node) startHeartbeatLoop() {
	go func() {
		ticker := time.NewTicker(n.heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				n.sendHeartbeatToAllPeers()
			case <-n.stopChan:
				return
			}
		}
	}()
}

// startPeerCleaner starts a goroutine that periodically cleans up stale peers
func (n *Node) startPeerCleaner() {
	go func() {
		ticker := time.NewTicker(n.cleanInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				n.cleanupStalePeers()
			case <-n.stopChan:
				return
			}
		}
	}()
}

func (n *Node) startGossipLoop() {
	go func() {
		ticker := time.NewTicker(n.gossipInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				n.performGossipRound()
			case <-n.stopChan:
				return
			}
		}
	}()
}

func (n *Node) performGossipRound() {
	addrs := n.connPool.StreamAddrs()
	for _, addr := range addrs {
		peers, err := client.GossipWithPeer(n, addr)
		if err != nil {
			logger.L().Debug("[node] gossip failed", zap.String("addr", addr), zap.Error(err))
			continue
		}
		for _, p := range peers {
			if p.Id == nil || p.Id.Uuid == "" || p.Id.Uuid == n.nodeID.Uuid {
				continue
			}
			if _, exists := n.registry.Get(p.Id.Uuid); exists {
				continue
			}
			peerAddr := ""
			if len(p.Addrs) > 0 {
				peerAddr = formatNodeAddr(p.Addrs[0].Ip, p.Addrs[0].Port)
			}
			n.notifier.Emit(notifier.NewPeerDiscoveredNotification(p.Id.Name, peerAddr, p.Id.Uuid))
		}
	}
}

// sendHeartbeatToAllPeers sends heartbeat to all registered peers
func (n *Node) sendHeartbeatToAllPeers() {
	peers := n.registry.List()
	for _, p := range peers {
		if p.Id.Uuid == n.nodeID.Uuid || len(p.Addrs) == 0 {
			continue
		}
		addr := formatNodeAddr(p.Addrs[0].Ip, p.Addrs[0].Port)
		uuid := p.Id.Uuid
		go n.sendHeartbeatToPeer(addr, uuid)
	}
}

// sendHeartbeatToPeer sends a single heartbeat to a peer
func (n *Node) sendHeartbeatToPeer(addr string, uuid string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, ok := n.connPool.GetConn(addr)
	if !ok {
		var err error
		conn, err = grpcutil.NewClientConn(ctx, addr)
		if err != nil {
			logger.L().Debug("[node] heartbeat dial failed", zap.String("addr", addr), zap.Error(err))
			return
		}
		n.connPool.SetConn(addr, conn)
	}

	cli := pb.NewMembershipClient(conn)
	resp, err := cli.Heartbeat(ctx, &pb.HeartbeatReq{
		NodeId:    n.nodeID,
		Timestamp: uint64(time.Now().UnixMilli()),
	})
	if err != nil {
		logger.L().Debug("[node] heartbeat failed", zap.String("addr", addr), zap.Error(err))
		return
	}
	if resp != nil && resp.Status != pb.NodeStatus_UNKNOWN {
		_ = n.registry.UpdateStatus(uuid, resp.Status)
	}
}

// cleanupStalePeers removes peers that haven't sent a heartbeat recently
func (n *Node) cleanupStalePeers() {
	staleUUIDs := n.registry.GetStale(types.HeartbeatTimeout)
	for _, uuid := range staleUUIDs {
		peer, found := n.registry.Get(uuid)
		if !found {
			continue
		}
		name := peer.Id.Name
		addr := ""
		if len(peer.Addrs) > 0 {
			addr = formatNodeAddr(peer.Addrs[0].Ip, peer.Addrs[0].Port)
		}
		_ = n.registry.UpdateStatus(uuid, pb.NodeStatus_OFFLINE)
		if removed, _ := n.registry.Unregister(uuid); removed {
			n.notifier.Emit(notifier.NewPeerOfflineNotification(name, "heartbeat timeout"))
			if addr != "" {
				n.connPool.CloseByAddr(addr)
			}
		}
	}
}

func (n *Node) SetNodeStatus(status pb.NodeStatus) {
	n.statusMu.Lock()
	n.selfInfo.Status = status
	n.statusMu.Unlock()
}

func (n *Node) GetNodeStatus() string {
	n.statusMu.RLock()
	s := n.selfInfo.Status
	n.statusMu.RUnlock()
	switch s {
	case pb.NodeStatus_ONLINE:
		return "online"
	case pb.NodeStatus_BUSY:
		return "busy"
	case pb.NodeStatus_OFFLINE:
		return "offline"
	default:
		return "unknown"
	}
}

func (n *Node) GetStatusPB() pb.NodeStatus {
	n.statusMu.RLock()
	s := n.selfInfo.Status
	n.statusMu.RUnlock()
	return s
}

func (n *Node) broadcastNodeJoin(notif notifier.Notification) {
	var payload map[string]string
	if err := json.Unmarshal(notif.Payload, &payload); err != nil {
		return
	}
	addr := payload["addr"]
	peerUUID := payload["uuid"]
	peerName := payload["name"]
	if addr == "" || peerUUID == "" {
		return
	}

	host, port, ok := parseHostPort(addr)
	if !ok {
		return
	}

	newNodeInfo := &pb.NodeInfo{
		Id:    &pb.NodeID{Uuid: peerUUID, Name: peerName},
		Addrs: []*pb.NodeAddr{{Ip: host, Port: port}},
	}

	peers := n.registry.List()
	for _, p := range peers {
		if p.Id.Uuid == n.nodeID.Uuid || p.Id.Uuid == peerUUID || len(p.Addrs) == 0 {
			continue
		}
		peerAddr := formatNodeAddr(p.Addrs[0].Ip, p.Addrs[0].Port)
		go n.notifySinglePeerJoin(peerAddr, newNodeInfo)
	}
}

func (n *Node) notifySinglePeerJoin(peerAddr string, newNodeInfo *pb.NodeInfo) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, ok := n.connPool.GetConn(peerAddr)
	if !ok {
		var err error
		conn, err = grpcutil.NewClientConn(ctx, peerAddr)
		if err != nil {
			logger.L().Debug("[node] notify-node-join dial failed", zap.String("addr", peerAddr), zap.Error(err))
			return
		}
		n.connPool.SetConn(peerAddr, conn)
	}

	cli := pb.NewMembershipClient(conn)
	_, err := cli.NotifyNodeJoin(ctx, &pb.NotifyNodeJoinReq{NewNode: newNodeInfo})
	if err != nil {
		logger.L().Debug("[node] notify-node-join failed", zap.String("addr", peerAddr), zap.Error(err))
	}
}

// parseHostPort splits a "host:port" address into its parts.
// It never panics: any malformed input returns ok=false. This replaces the
// panic-prone addr[:strings.LastIndex(addr, ":")] idiom and the mustParsePort
// helper that silently returned 0 on error.
func parseHostPort(addr string) (host string, port uint32, ok bool) {
	h, pStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, false
	}
	p, err := strconv.Atoi(pStr)
	if err != nil || p < 0 {
		return "", 0, false
	}
	return h, uint32(p), true
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

func statusToString(s pb.NodeStatus) string {
	switch s {
	case pb.NodeStatus_ONLINE:
		return "online"
	case pb.NodeStatus_BUSY:
		return "busy"
	case pb.NodeStatus_OFFLINE:
		return "offline"
	default:
		return "unknown"
	}
}

func getPeerFirstAddr(peer *pb.NodeInfo) (string, error) {
	if peer == nil || len(peer.Addrs) == 0 {
		return "", types.ErrNoValidNodeAddress
	}
	return formatNodeAddr(peer.Addrs[0].Ip, peer.Addrs[0].Port), nil
}
