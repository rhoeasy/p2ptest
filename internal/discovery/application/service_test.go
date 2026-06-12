package application

import (
	"context"
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
	return "", types.ErrPeerNotFound
}

func TestDiscoveryService_GetPeers(t *testing.T) {
	registry := newMockRegistry()
	svc := NewDiscoveryService(registry)

	t.Run("empty registry", func(t *testing.T) {
		resp, err := svc.GetPeers(context.Background(), nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(resp.Peers) != 0 {
			t.Fatalf("expected 0 peers, got %d", len(resp.Peers))
		}
	})

	t.Run("with peers", func(t *testing.T) {
		_ = registry.Register(&pb.NodeInfo{
			Id:    &pb.NodeID{Uuid: "peer-1", Name: "peer1"},
			Addrs: []*pb.NodeAddr{{Ip: "127.0.0.1", Port: 50052}},
		})
		_ = registry.Register(&pb.NodeInfo{
			Id:    &pb.NodeID{Uuid: "peer-2", Name: "peer2"},
			Addrs: []*pb.NodeAddr{{Ip: "127.0.0.1", Port: 50053}},
		})

		resp, err := svc.GetPeers(context.Background(), nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(resp.Peers) != 2 {
			t.Fatalf("expected 2 peers, got %d", len(resp.Peers))
		}
	})

	t.Run("with limit", func(t *testing.T) {
		req := &pb.GetPeersReq{Limit: 1}
		resp, err := svc.GetPeers(context.Background(), req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(resp.Peers) != 1 {
			t.Fatalf("expected 1 peer, got %d", len(resp.Peers))
		}
	})
}

func TestDiscoveryService_FindNode(t *testing.T) {
	registry := newMockRegistry()
	svc := NewDiscoveryService(registry)

	_ = registry.Register(&pb.NodeInfo{
		Id:    &pb.NodeID{Uuid: "peer-1", Name: "peer1"},
		Addrs: []*pb.NodeAddr{{Ip: "127.0.0.1", Port: 50052}},
	})

	t.Run("found", func(t *testing.T) {
		req := &pb.FindNodeReq{Target: &pb.NodeID{Uuid: "peer-1"}}
		resp, err := svc.FindNode(context.Background(), req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !resp.Found {
			t.Fatal("expected node to be found")
		}
		if resp.Node == nil || resp.Node.Id.Uuid != "peer-1" {
			t.Fatal("expected correct node info")
		}
	})

	t.Run("not found", func(t *testing.T) {
		req := &pb.FindNodeReq{Target: &pb.NodeID{Uuid: "peer-2"}}
		resp, err := svc.FindNode(context.Background(), req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.Found {
			t.Fatal("expected node to not be found")
		}
	})

	t.Run("nil request", func(t *testing.T) {
		resp, err := svc.FindNode(context.Background(), nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.Found {
			t.Fatal("expected not found for nil request")
		}
	})
}
