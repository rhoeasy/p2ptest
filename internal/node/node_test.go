package node

import (
	"testing"
	"time"

	"p2ptest/internal/logger"
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
