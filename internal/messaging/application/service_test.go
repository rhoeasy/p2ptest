package application

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"p2ptest/internal/logger"
	"p2ptest/internal/notifier"
	pb "p2ptest/proto/p2p"
)

func TestMain(m *testing.M) {
	logger.InitLogger(true)
	m.Run()
}

// mockStream implements pb.Messaging_StreamServer for testing
type mockStream struct {
	pb.Messaging_StreamServer
	sent []*pb.Envelope
}

func (m *mockStream) Send(env *pb.Envelope) error {
	m.sent = append(m.sent, env)
	return nil
}

func (m *mockStream) Context() context.Context {
	return context.Background()
}

func TestMessagingServiceEmitsNotificationOnMessage(t *testing.T) {
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
		Id: &pb.NodeID{
			Name: "test-self",
			Uuid: "test-self-uuid",
		},
	}
	svc := NewMessagingService(selfInfo, n)
	
	// Create a mock stream
	stream := &mockStream{}
	
	// Create a text message envelope
	env := &pb.Envelope{
		MsgId: "test-msg-1",
		From: &pb.NodeID{
			Name: "test-peer",
			Uuid: "test-uuid",
		},
		Timestamp: 1234567890,
		Payload: &pb.Envelope_Text{
			Text: &pb.TextMessage{
				Content: "Hello, world!",
			},
		},
	}
	
	// Call handleEnvelope directly
	svc.handleEnvelope(stream, env)
	
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
	
	if notification.Type != "message_received" {
		t.Fatalf("Expected notification type 'message_received', got '%s'", notification.Type)
	}
	
	// Check the payload
	var payload map[string]string
	if err := json.Unmarshal(notification.Payload, &payload); err != nil {
		t.Fatalf("Failed to unmarshal notification payload: %v", err)
	}
	
	if payload["from"] != "test-peer" {
		t.Fatalf("Expected payload.from='test-peer', got '%s'", payload["from"])
	}
	
	if payload["content"] != "Hello, world!" {
		t.Fatalf("Expected payload.content='Hello, world!', got '%s'", payload["content"])
	}
}

func TestPingHandlerSendsPong(t *testing.T) {
	nonce := []byte("testnonce")
	selfInfo := &pb.NodeInfo{
		Id: &pb.NodeID{Name: "test-self", Uuid: "self-uuid"},
	}
	svc := NewMessagingService(selfInfo, nil)
	stream := &mockStream{}

	env := &pb.Envelope{
		MsgId:     "ping-1",
		From:      &pb.NodeID{Name: "peer", Uuid: "peer-uuid"},
		Timestamp: 1234567890,
		Payload: &pb.Envelope_Ping{
			Ping: &pb.Ping{Nonce: nonce},
		},
	}

	svc.handleEnvelope(stream, env)

	if len(stream.sent) != 1 {
		t.Fatalf("Expected 1 sent envelope, got %d", len(stream.sent))
	}

	sent := stream.sent[0]
	if sent.MsgId != "ping-1-pong" {
		t.Fatalf("Expected MsgId 'ping-1-pong', got %s", sent.MsgId)
	}

	pongPayload, ok := sent.Payload.(*pb.Envelope_Pong)
	if !ok {
		t.Fatalf("Expected Payload type *pb.Envelope_Pong, got %T", sent.Payload)
	}

	if pongPayload.Pong == nil {
		t.Fatal("Expected Pong to be non-nil")
	}
}

func TestPingHandlerPreservesNonce(t *testing.T) {
	nonce := []byte("preserved-nonce-123")
	selfInfo := &pb.NodeInfo{
		Id: &pb.NodeID{Name: "test-self", Uuid: "self-uuid"},
	}
	svc := NewMessagingService(selfInfo, nil)
	stream := &mockStream{}

	env := &pb.Envelope{
		MsgId:     "ping-2",
		From:      &pb.NodeID{Name: "peer", Uuid: "peer-uuid"},
		Timestamp: 9876543210,
		Payload: &pb.Envelope_Ping{
			Ping: &pb.Ping{Nonce: nonce},
		},
	}

	svc.handleEnvelope(stream, env)

	if len(stream.sent) != 1 {
		t.Fatalf("Expected 1 sent envelope, got %d", len(stream.sent))
	}

	pongPayload := stream.sent[0].Payload.(*pb.Envelope_Pong)
	if string(pongPayload.Pong.Nonce) != string(nonce) {
		t.Fatalf("Expected nonce '%s', got '%s'", string(nonce), string(pongPayload.Pong.Nonce))
	}

	if pongPayload.Pong.PingTimestamp != 9876543210 {
		t.Fatalf("Expected PingTimestamp 9876543210, got %d", pongPayload.Pong.PingTimestamp)
	}
}