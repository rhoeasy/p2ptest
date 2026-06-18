package application

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"p2ptest/internal/logger"
	"p2ptest/internal/notifier"
	"p2ptest/internal/types"
	pb "p2ptest/proto/p2p"
)

func TestMain(m *testing.M) {
	logger.InitLogger(true)
	m.Run()
}

// mockRegistry is a simple in-memory implementation of PeerRegistry for testing.
type mockRegistry struct {
	peers    map[string]*pb.NodeInfo
	statuses map[string]pb.NodeStatus
}

func newMockRegistry() *mockRegistry {
	return &mockRegistry{
		peers:    make(map[string]*pb.NodeInfo),
		statuses: make(map[string]pb.NodeStatus),
	}
}

func (m *mockRegistry) Register(peer *pb.NodeInfo) error {
	if peer == nil || peer.Id == nil || peer.Id.Uuid == "" {
		return types.ErrInvalidPeerInfo
	}
	m.peers[peer.Id.Uuid] = peer
	return nil
}

func (m *mockRegistry) Unregister(uuid string) (bool, error) {
	if _, ok := m.peers[uuid]; !ok {
		return false, nil
	}
	delete(m.peers, uuid)
	return true, nil
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

func (m *mockRegistry) UpdateStatus(uuid string, status pb.NodeStatus) error {
	if _, ok := m.peers[uuid]; !ok {
		return types.ErrPeerNotFound
	}
	m.statuses[uuid] = status
	return nil
}

func (m *mockRegistry) GetStale(threshold time.Duration) []string { return nil }

func (m *mockRegistry) GetAddrByName(name string) (string, error) {
	for _, p := range m.peers {
		if p.Id.Name == name {
			return fmt.Sprintf("%s:%d", p.Addrs[0].Ip, p.Addrs[0].Port), nil
		}
	}
	return "", types.ErrPeerNotFound
}

func (m *mockRegistry) GetByName(name string) (*pb.NodeInfo, bool) {
	for _, p := range m.peers {
		if p.Id.Name == name {
			return p, true
		}
	}
	return nil, false
}

func (m *mockRegistry) GetLastActive(uuid string) (time.Time, bool) {
	return time.Time{}, false
}

func (m *mockRegistry) GetRegisteredAt(uuid string) (time.Time, bool) {
	return time.Time{}, false
}

func TestMembershipService_Handshake(t *testing.T) {
	selfInfo := &pb.NodeInfo{
		Id: &pb.NodeID{Uuid: "self-uuid", Name: "self"},
		Addrs: []*pb.NodeAddr{{Ip: "127.0.0.1", Port: 50051}},
	}
	registry := newMockRegistry()
	svc := NewMembershipService(registry, selfInfo, &types.NodeConfig{}, nil)

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
	svc := NewMembershipService(registry, nil, &types.NodeConfig{}, nil)

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

	svc := NewMembershipService(registry, nil, &types.NodeConfig{}, nil)

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

func TestMembershipServiceEmitsPeerOnlineOnHandshake(t *testing.T) {
	// Create a notifier
	n := notifier.NewNotifier(10)
	
	// Create a slice to collect notifications
	var notifications []notifier.Notification
	var mu sync.Mutex
	
	// Subscribe to notifications
	n.Subscribe(func(notification notifier.Notification) {
		mu.Lock()
		notifications = append(notifications, notification)
		mu.Unlock()
	})
	
	selfInfo := &pb.NodeInfo{
		Id: &pb.NodeID{Uuid: "self-uuid", Name: "self"},
		Addrs: []*pb.NodeAddr{{Ip: "127.0.0.1", Port: 50051}},
	}
	registry := newMockRegistry()
	
	// Create service with notifier - this will fail until we update NewMembershipService
	svc := NewMembershipService(registry, selfInfo, &types.NodeConfig{}, n)
	
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
	
	// Check that we received a notification
	mu.Lock()
	notificationCount := len(notifications)
	mu.Unlock()
	
	if notificationCount == 0 {
		t.Fatal("Expected at least one notification, got none")
	}
	
	// Check the notification type
	mu.Lock()
	notification := notifications[0]
	mu.Unlock()
	
	if notification.Type != "peer_online" {
		t.Fatalf("Expected notification type 'peer_online', got '%s'", notification.Type)
	}
	
	// Check the payload
	var payload map[string]string
	if err := json.Unmarshal(notification.Payload, &payload); err != nil {
		t.Fatalf("Failed to unmarshal notification payload: %v", err)
	}
	
	if payload["name"] != "peer" {
		t.Fatalf("Expected payload.name='peer', got '%s'", payload["name"])
	}
	
	expectedAddr := "127.0.0.1:50052"
	if payload["addr"] != expectedAddr {
		t.Fatalf("Expected payload.addr='%s', got '%s'", expectedAddr, payload["addr"])
	}
}

func TestMembershipServiceEmitsPeerOfflineOnDisconnect(t *testing.T) {
	// Create a notifier
	n := notifier.NewNotifier(10)
	
	// Create a slice to collect notifications
	var notifications []notifier.Notification
	var mu sync.Mutex
	
	// Subscribe to notifications
	n.Subscribe(func(notification notifier.Notification) {
		mu.Lock()
		notifications = append(notifications, notification)
		mu.Unlock()
	})
	
	registry := newMockRegistry()
	_ = registry.Register(&pb.NodeInfo{
		Id:    &pb.NodeID{Uuid: "peer-uuid", Name: "peer"},
		Addrs: []*pb.NodeAddr{{Ip: "127.0.0.1", Port: 50052}},
	})
	
	svc := NewMembershipService(registry, nil, &types.NodeConfig{}, n)
	
	req := &pb.DisconnectReq{
		NodeId: &pb.NodeID{Uuid: "peer-uuid", Name: "peer"},
		Reason: "test disconnect",
	}
	
	resp, err := svc.Disconnect(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Acknowledged {
		t.Fatal("expected disconnect to be acknowledged")
	}
	
	// Check that we received a notification
	mu.Lock()
	notificationCount := len(notifications)
	mu.Unlock()
	
	if notificationCount == 0 {
		t.Fatal("Expected at least one notification, got none")
	}
	
	// Check the notification type
	mu.Lock()
	notification := notifications[0]
	mu.Unlock()
	
	if notification.Type != "peer_offline" {
		t.Fatalf("Expected notification type 'peer_offline', got '%s'", notification.Type)
	}
	
	// Check the payload
	var payload map[string]string
	if err := json.Unmarshal(notification.Payload, &payload); err != nil {
		t.Fatalf("Failed to unmarshal notification payload: %v", err)
	}
	
	if payload["name"] != "peer" {
		t.Fatalf("Expected payload.name='peer', got '%s'", payload["name"])
	}
	
	if payload["reason"] != "test disconnect" {
		t.Fatalf("Expected payload.reason='test disconnect', got '%s'", payload["reason"])
	}
}

func TestHeartbeatReturnsActualStatus(t *testing.T) {
	selfInfo := &pb.NodeInfo{
		Id:    &pb.NodeID{Uuid: "self-uuid", Name: "self"},
		Addrs: []*pb.NodeAddr{{Ip: "127.0.0.1", Port: 50051}},
	}
	registry := newMockRegistry()
	svc := NewMembershipService(registry, selfInfo, &types.NodeConfig{}, nil)
	svc.SetStatusGetter(func() pb.NodeStatus { return pb.NodeStatus_BUSY })

	req := &pb.HeartbeatReq{
		NodeId:    &pb.NodeID{Uuid: "peer-uuid", Name: "peer"},
		Timestamp: 12345,
	}
	resp, err := svc.Heartbeat(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Status != pb.NodeStatus_BUSY {
		t.Fatalf("expected BUSY status, got %v", resp.Status)
	}
	if resp.Timestamp != 12345 {
		t.Fatalf("expected timestamp 12345, got %d", resp.Timestamp)
	}
}

func TestDisconnectSetsOffline(t *testing.T) {
	registry := newMockRegistry()
	_ = registry.Register(&pb.NodeInfo{
		Id:    &pb.NodeID{Uuid: "peer-uuid", Name: "peer"},
		Addrs: []*pb.NodeAddr{{Ip: "127.0.0.1", Port: 50052}},
	})

	svc := NewMembershipService(registry, nil, &types.NodeConfig{}, nil)

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

	if _, ok := registry.peers["peer-uuid"]; ok {
		t.Fatal("expected peer to be unregistered")
	}

	if status, ok := registry.statuses["peer-uuid"]; !ok || status != pb.NodeStatus_OFFLINE {
		t.Fatalf("expected peer status to be set to OFFLINE, got %v", status)
	}
}
