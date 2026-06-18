package node

import (
	"testing"
	"time"

	"p2ptest/internal/client"
	"p2ptest/internal/logger"
	membershipApp "p2ptest/internal/membership/application"
	"p2ptest/internal/notifier"
	"p2ptest/internal/types"
	pb "p2ptest/proto/p2p"
)

func init() {
	logger.InitLogger(false)
}

func newTestConfig(name string, port uint32) *types.NodeConfig {
	return &types.NodeConfig{
		NodeName:   name,
		ListenIP:   "127.0.0.1",
		ListenPort: port,
		ProtoVer:   types.DefaultProtoVer,
	}
}

func TestNewNode(t *testing.T) {
	cfg := newTestConfig("test-node", 50001)
	n := NewNode(cfg)

	if n == nil {
		t.Fatal("NewNode should not return nil")
	}
	if n.nodeID == nil || n.nodeID.Uuid == "" {
		t.Fatal("nodeID should be generated")
	}
	if n.nodeID.Name != "test-node" {
		t.Fatalf("expected node name 'test-node', got %s", n.nodeID.Name)
	}
	if n.registry == nil {
		t.Fatal("registry should be initialized")
	}
	if n.connPool == nil {
		t.Fatal("connPool should be initialized")
	}
}

func TestNodeStartStop(t *testing.T) {
	cfg := newTestConfig("start-stop", 50002)
	n := NewNode(cfg)

	if err := n.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Verify node ID is accessible
	if n.GetNodeID() == nil {
		t.Fatal("GetNodeID should return non-nil")
	}

	// Verify config
	if n.Cfg() != cfg {
		t.Fatal("Cfg should return the config")
	}

	n.Stop()
}

func TestNodeRegistry(t *testing.T) {
	cfg := newTestConfig("registry-test", 50003)
	n := NewNode(cfg)

	peer := &pb.NodeInfo{
		Id: &pb.NodeID{Uuid: "peer-1", Name: "peer1"},
		Addrs: []*pb.NodeAddr{{
			Ip: "127.0.0.1", Port: 50004,
		}},
	}

	if err := n.registry.Register(peer); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	p, found := n.registry.Get("peer-1")
	if !found {
		t.Fatal("peer should be found")
	}
	if p.Id.Name != "peer1" {
		t.Fatalf("expected peer name 'peer1', got %s", p.Id.Name)
	}

	peers := n.registry.List()
	if len(peers) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(peers))
	}
}

func TestNodeMultiplePeers(t *testing.T) {
	cfg := newTestConfig("multi-peer", 50005)
	n := NewNode(cfg)

	peers := []*pb.NodeInfo{
		{
			Id: &pb.NodeID{Uuid: "peer-a", Name: "peerA"},
			Addrs: []*pb.NodeAddr{{Ip: "127.0.0.1", Port: 50006}},
		},
		{
			Id: &pb.NodeID{Uuid: "peer-b", Name: "peerB"},
			Addrs: []*pb.NodeAddr{{Ip: "127.0.0.1", Port: 50007}},
		},
	}

	for _, p := range peers {
		if err := n.registry.Register(p); err != nil {
			t.Fatalf("Register failed: %v", err)
		}
	}

	list := n.registry.List()
	if len(list) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(list))
	}
}

func TestNodeGetPeerAddrByName(t *testing.T) {
	cfg := newTestConfig("addr-by-name", 50008)
	n := NewNode(cfg)

	peer := &pb.NodeInfo{
		Id: &pb.NodeID{Uuid: "peer-x", Name: "peerX"},
		Addrs: []*pb.NodeAddr{{
			Ip: "127.0.0.1", Port: 50009,
		}},
	}

	if err := n.registry.Register(peer); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	addr, err := n.registry.GetAddrByName("peerX")
	if err != nil {
		t.Fatalf("GetAddrByName failed: %v", err)
	}
	if addr != "127.0.0.1:50009" {
		t.Fatalf("expected address '127.0.0.1:50009', got %s", addr)
	}
}

func TestNodeContextCancellation(t *testing.T) {
	cfg := newTestConfig("ctx-cancel", 50010)
	n := NewNode(cfg)

	if err := n.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Stop should not panic
	n.Stop()

	// Verify stopChan is closed
	select {
	case <-n.stopChan:
		// Expected
	default:
		t.Fatal("stopChan should be closed after Stop()")
	}
}

func TestNodeSelfRegistration(t *testing.T) {
	cfg := newTestConfig("self-reg", 50011)
	n := NewNode(cfg)

	// Self should not be registered in the peer list
	peers := n.registry.List()
	for _, p := range peers {
		if p.Id.Uuid == n.nodeID.Uuid {
			t.Fatal("self should not appear in peer list")
		}
	}
}

func TestNodeCreatesNotifier(t *testing.T) {
	cfg := newTestConfig("notifier-test", 50012)
	n := NewNode(cfg)
	
	// Test that Node has a Notifier method
	if n.Notifier() == nil {
		t.Fatal("Node.Notifier() should return non-nil")
	}
	
	// Test that notifier is created with buffer
	ntf := n.Notifier()
	
	// Subscribe to notifications to verify it works
	notificationReceived := false
	ntf.Subscribe(func(notification notifier.Notification) {
		notificationReceived = true
	})
	
	// Emit a test notification
	ntf.Emit(notifier.NewMessageReceivedNotification("test", "hello"))
	
	// Check that notification was received
	if !notificationReceived {
		t.Fatal("Notifier should be functional and deliver notifications")
	}
}

func TestNodePeerCleanup(t *testing.T) {
	cfg := newTestConfig("cleanup-test", 50013)
	n := NewNode(cfg)

	// Register a peer
	peer := &pb.NodeInfo{
		Id: &pb.NodeID{Uuid: "stale-peer", Name: "stale"},
		Addrs: []*pb.NodeAddr{{
			Ip: "127.0.0.1", Port: 50014,
		}},
	}

	if err := n.registry.Register(peer); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Verify peer is registered
	peers := n.registry.List()
	if len(peers) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(peers))
	}

	// Subscribe to notifications
	notificationReceived := false
	n.Notifier().Subscribe(func(notification notifier.Notification) {
		notificationReceived = true
		if notification.Type != "peer_offline" {
			t.Errorf("expected notification type 'peer_offline', got %s", notification.Type)
		}
	})

	// Call cleanupStalePeers - at this point the peer is NOT stale
	// because it was just registered (lastActive = time.Now())
	// So it should NOT be cleaned up
	n.cleanupStalePeers()

	// Verify peer is still there
	peers = n.registry.List()
	if len(peers) != 1 {
		t.Fatalf("peer should not be cleaned up yet, expected 1 peer, got %d", len(peers))
	}

	// Verify no notification was emitted
	if notificationReceived {
		t.Fatal("notification should not be emitted for non-stale peer")
	}
}

func TestNodeStartsBackgroundLoops(t *testing.T) {
	cfg := newTestConfig("background-loops", 50015)
	n := NewNode(cfg)

	// Start the node
	if err := n.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Verify stopChan is not closed (loops are running)
	select {
	case <-n.stopChan:
		t.Fatal("stopChan should not be closed while node is running")
	default:
		// Expected - loops are running
	}

	// Stop the node - this should close stopChan and stop the loops
	n.Stop()

	// Verify stopChan is closed after Stop()
	select {
	case <-n.stopChan:
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("stopChan should be closed after Stop()")
	}
}

func TestHeartbeatIntegration(t *testing.T) {
	// This is an integration test that would require two real nodes
	// Since it's complex, we'll test the individual methods instead
	// and rely on the existing integration tests for end-to-end validation
	t.Skip("Integration test requires two real nodes - tested in manual e2e tests")
}

func TestRecordPingSentCreatesChannel(t *testing.T) {
	cfg := newTestConfig("ping-test", 50020)
	n := NewNode(cfg)

	ch := n.RecordPingSent("nonce1")
	if ch == nil {
		t.Fatal("RecordPingSent should return a non-nil channel")
	}

	select {
	case <-ch:
		t.Fatal("Channel should be empty initially")
	default:
	}
}

func TestHandlePongReceivesRTT(t *testing.T) {
	cfg := newTestConfig("pong-test", 50021)
	n := NewNode(cfg)

	nonce := "nonce2"
	ch := n.RecordPingSent(nonce)

	pingTimestamp := uint64(time.Now().Add(-100 * time.Millisecond).UnixMilli())
	n.HandlePongReceived(nonce, pingTimestamp)

	select {
	case rtt := <-ch:
		if rtt <= 0 {
			t.Fatalf("Expected positive RTT, got %v", rtt)
		}
	case <-time.After(time.Second):
		t.Fatal("Expected RTT on channel, got timeout")
	}
}

func TestCancelPendingPingCleansUp(t *testing.T) {
	cfg := newTestConfig("cancel-ping", 50022)
	n := NewNode(cfg)

	nonce := "nonce3"
	ch := n.RecordPingSent(nonce)

	n.CancelPendingPing(nonce)

	n.HandlePongReceived(nonce, uint64(time.Now().UnixMilli()))

	select {
	case <-ch:
		t.Fatal("Cancelled ping should not receive pong")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestGetOnlinePeersIncludesStatus(t *testing.T) {
	cfg := newTestConfig("status-peek", 50030)
	n := NewNode(cfg)

	peer := &pb.NodeInfo{
		Id:     &pb.NodeID{Uuid: "peer-busy", Name: "busy_peer"},
		Addrs:  []*pb.NodeAddr{{Ip: "127.0.0.1", Port: 50031}},
		Status: pb.NodeStatus_BUSY,
	}
	if err := n.registry.Register(peer); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	_ = n.registry.UpdateStatus("peer-busy", pb.NodeStatus_BUSY)

	peers := n.GetOnlinePeers()
	if len(peers) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(peers))
	}
	if peers[0]["status"] != "busy" {
		t.Errorf("expected status 'busy', got '%s'", peers[0]["status"])
	}
}

func TestGetOnlinePeersUnknownStatus(t *testing.T) {
	cfg := newTestConfig("status-unknown", 50032)
	n := NewNode(cfg)

	peer := &pb.NodeInfo{
		Id:    &pb.NodeID{Uuid: "peer-unknown", Name: "unknown_peer"},
		Addrs: []*pb.NodeAddr{{Ip: "127.0.0.1", Port: 50033}},
	}
	if err := n.registry.Register(peer); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	peers := n.GetOnlinePeers()
	if len(peers) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(peers))
	}
	if peers[0]["status"] != "unknown" {
		t.Errorf("expected status 'unknown', got '%s'", peers[0]["status"])
	}
}

func TestSetGetNodeStatus(t *testing.T) {
	cfg := newTestConfig("set-status", 50034)
	n := NewNode(cfg)

	if s := n.GetNodeStatus(); s != "online" {
		t.Fatalf("initial status should be 'online', got '%s'", s)
	}

	n.SetNodeStatus(pb.NodeStatus_BUSY)
	if s := n.GetNodeStatus(); s != "busy" {
		t.Fatalf("expected 'busy', got '%s'", s)
	}

	n.SetNodeStatus(pb.NodeStatus_OFFLINE)
	if s := n.GetNodeStatus(); s != "offline" {
		t.Fatalf("expected 'offline', got '%s'", s)
	}
}

func TestGetStatusPB(t *testing.T) {
	cfg := newTestConfig("get-status-pb", 50035)
	n := NewNode(cfg)

	if s := n.GetStatusPB(); s != pb.NodeStatus_ONLINE {
		t.Fatalf("expected ONLINE, got %v", s)
	}

	n.SetNodeStatus(pb.NodeStatus_BUSY)
	if s := n.GetStatusPB(); s != pb.NodeStatus_BUSY {
		t.Fatalf("expected BUSY, got %v", s)
	}
}

func TestPerformGossipRoundDiscoversNewPeers(t *testing.T) {
	seedCfg := newTestConfig("gossip-seed", 50100)
	seed := NewNode(seedCfg)
	if err := seed.Start(); err != nil {
		t.Fatalf("seed start failed: %v", err)
	}
	defer seed.Stop()

	node2Cfg := newTestConfig("gossip-node2", 50101)
	node2 := NewNode(node2Cfg)
	if err := node2.Start(); err != nil {
		t.Fatalf("node2 start failed: %v", err)
	}
	defer node2.Stop()

	if err := client.HandshakeAndConnect(node2, "127.0.0.1:50100"); err != nil {
		t.Fatalf("handshake failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	node3Cfg := newTestConfig("gossip-node3", 50102)
	node3 := NewNode(node3Cfg)
	if err := node3.Start(); err != nil {
		t.Fatalf("node3 start failed: %v", err)
	}
	defer node3.Stop()

	if err := client.HandshakeAndConnect(node3, "127.0.0.1:50100"); err != nil {
		t.Fatalf("node3 handshake failed: %v", err)
	}

	seed.performGossipRound()

	time.Sleep(200 * time.Millisecond)

	peers := seed.GetOnlinePeers()
	names := map[string]bool{}
	for _, p := range peers {
		names[p["name"]] = true
	}
	if len(peers) < 2 {
		t.Errorf("expected at least 2 peers after gossip, got %d: %v", len(peers), names)
	}
}

func TestPerformGossipRoundSkipsKnownAndSelf(t *testing.T) {
	cfg := newTestConfig("gossip-skip", 50110)
	n := NewNode(cfg)
	if err := n.Start(); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer n.Stop()

	n.registry.Register(&pb.NodeInfo{
		Id:    &pb.NodeID{Uuid: "known-peer", Name: "known"},
		Addrs: []*pb.NodeAddr{{Ip: "127.0.0.1", Port: 50111}},
	})

	if len(n.connPool.StreamAddrs()) > 0 {
		t.Fatal("expected no streams — gossip should be no-op")
	}

	n.performGossipRound()

	peers := n.registry.List()
	if len(peers) != 1 {
		t.Fatalf("expected 1 peer (no new discoveries), got %d", len(peers))
	}
}

func TestSendHeartbeatToPeerStoresStatus(t *testing.T) {
	seedCfg := newTestConfig("hb-seed", 50120)
	seed := NewNode(seedCfg)

	var receivedStatus pb.NodeStatus
	svc := membershipApp.NewMembershipService(seed.registry, seed.selfInfo, seed.cfg, seed.notifier)
	svc.SetStatusGetter(func() pb.NodeStatus { return pb.NodeStatus_BUSY })
	seed.membershipSvc = svc

	if err := seed.Start(); err != nil {
		t.Fatalf("seed start failed: %v", err)
	}
	defer seed.Stop()

	node2Cfg := newTestConfig("hb-node2", 50121)
	node2 := NewNode(node2Cfg)
	if err := node2.Start(); err != nil {
		t.Fatalf("node2 start failed: %v", err)
	}
	defer node2.Stop()

	if err := client.HandshakeAndConnect(node2, "127.0.0.1:50120"); err != nil {
		t.Fatalf("handshake failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	node2.sendHeartbeatToAllPeers()

	time.Sleep(500 * time.Millisecond)

	peers := node2.registry.List()
	for _, p := range peers {
		if p.Id.Name == "hb-seed" {
			receivedStatus = p.Status
		}
	}

	if receivedStatus != pb.NodeStatus_BUSY {
		t.Errorf("expected peer status BUSY from heartbeat response, got %v", receivedStatus)
	}
}
