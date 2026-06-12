package application

import (
	"context"
	"fmt"
	"testing"
	"time"

	"p2ptest/internal/logger"
	"p2ptest/internal/types"
	pb "p2ptest/proto/p2p"
)

func TestMain(m *testing.M) {
	logger.InitLogger(true)
	m.Run()
}

// mockRegistry is a simple in-memory implementation of PeerRegistry for testing.
type mockRegistry struct {
	peers map[string]*pb.NodeInfo
}

func newMockRegistry() *mockRegistry {
	return &mockRegistry{peers: make(map[string]*pb.NodeInfo)}
}

func (m *mockRegistry) Register(peer *pb.NodeInfo) error {
	if peer == nil || peer.Id == nil || peer.Id.Uuid == "" {
		return types.ErrInvalidPeerInfo
	}
	m.peers[peer.Id.Uuid] = peer
	return nil
}

func (m *mockRegistry) Unregister(uuid string) error {
	if _, ok := m.peers[uuid]; !ok {
		return types.ErrPeerNotFound
	}
	delete(m.peers, uuid)
	return nil
}

func (m *mockRegistry) Get(uuid string) (*pb.NodeInfo, bool) {
	p, ok := m.peers[uuid]
	return p, ok
}

func (m *mockRegistry) List() []*pb.NodeInfo {
	peers := make([]*pb.NodeInfo, 0, len(m.peers))
	for _, p := range m.peers {
		peers = append(peers, p)
	}
	return peers
}

func (m *mockRegistry) UpdateLastActive(uuid string) {}

func (m *mockRegistry) GetStale(threshold time.Duration) []string { return nil }

func (m *mockRegistry) GetAddrByName(name string) (string, error) {
	for _, p := range m.peers {
		if p.Id.Name == name {
			return fmt.Sprintf("%s:%d", p.Addrs[0].Ip, p.Addrs[0].Port), nil
		}
	}
	return "", types.ErrPeerNotFound
}

func TestMembershipService_Handshake(t *testing.T) {
	selfInfo := &pb.NodeInfo{
		Id: &pb.NodeID{Uuid: "self-uuid", Name: "self"},
		Addrs: []*pb.NodeAddr{{Ip: "127.0.0.1", Port: 50051}},
	}
	registry := newMockRegistry()
	svc := NewMembershipService(registry, selfInfo, &types.NodeConfig{})

	t.Run("valid handshake", func(t *testing.T) {
		req := &pb.HandshakeReq{
			Self: &pb.NodeInfo{
				Id:    &pb.NodeID{Uuid: "peer-uuid", Name: "peer"},
				Addrs: []*pb.NodeAddr{{Ip: "127.0.0.1", Port: 50052}},
			},
		}

		resp, err := svc.Handshake(context.Background(), req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !resp.Accepted {
			t.Fatal("expected handshake to be accepted")
		}
		if resp.Peer == nil || resp.Peer.Id.Uuid != "self-uuid" {
			t.Fatal("expected self info in response")
		}

		// Verify peer was registered
		if _, ok := registry.Get("peer-uuid"); !ok {
			t.Fatal("expected peer to be registered")
		}
	})

	t.Run("nil request", func(t *testing.T) {
		resp, err := svc.Handshake(context.Background(), nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.Accepted {
			t.Fatal("expected handshake to be rejected")
		}
	})

	t.Run("empty uuid", func(t *testing.T) {
		req := &pb.HandshakeReq{
			Self: &pb.NodeInfo{
				Id:    &pb.NodeID{Uuid: "", Name: "peer"},
				Addrs: []*pb.NodeAddr{{Ip: "127.0.0.1", Port: 50052}},
			},
		}
		resp, err := svc.Handshake(context.Background(), req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.Accepted {
			t.Fatal("expected handshake to be rejected")
		}
	})
}

func TestMembershipService_Heartbeat(t *testing.T) {
	registry := newMockRegistry()
	svc := NewMembershipService(registry, nil, &types.NodeConfig{})

	t.Run("valid heartbeat", func(t *testing.T) {
		req := &pb.HeartbeatReq{
			NodeId: &pb.NodeID{Uuid: "peer-uuid", Name: "peer"},
		}
		resp, err := svc.Heartbeat(context.Background(), req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.Status != pb.NodeStatus_ONLINE {
			t.Fatalf("expected ONLINE status, got %v", resp.Status)
		}
	})

	t.Run("nil request", func(t *testing.T) {
		resp, err := svc.Heartbeat(context.Background(), nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.Status != pb.NodeStatus_UNKNOWN {
			t.Fatalf("expected UNKNOWN status, got %v", resp.Status)
		}
	})
}

func TestMembershipService_Disconnect(t *testing.T) {
	registry := newMockRegistry()
	_ = registry.Register(&pb.NodeInfo{
		Id:    &pb.NodeID{Uuid: "peer-uuid", Name: "peer"},
		Addrs: []*pb.NodeAddr{{Ip: "127.0.0.1", Port: 50052}},
	})

	svc := NewMembershipService(registry, nil, &types.NodeConfig{})

	t.Run("valid disconnect", func(t *testing.T) {
		req := &pb.DisconnectReq{
			NodeId: &pb.NodeID{Uuid: "peer-uuid", Name: "peer"},
			Reason: "test",
		}
		resp, err := svc.Disconnect(context.Background(), req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !resp.Acknowledged {
			t.Fatal("expected disconnect to be acknowledged")
		}
		if _, ok := registry.Get("peer-uuid"); ok {
			t.Fatal("expected peer to be unregistered")
		}
	})

	t.Run("nil request", func(t *testing.T) {
		resp, err := svc.Disconnect(context.Background(), nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !resp.Acknowledged {
			t.Fatal("expected disconnect to be acknowledged")
		}
	})
}
