package notifier

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNotifierEmitCallsCallback(t *testing.T) {
	// Create a notifier with buffer size 0 (no buffering)
	n := NewNotifier(0)
	
	// Create a channel to receive the notification in the callback
	received := make(chan Notification, 1)
	
	// Subscribe a callback that records the notification
	n.Subscribe(func(notification Notification) {
		received <- notification
	})
	
	// Create a test notification
	testNotif := Notification{
		Type:    "test_type",
		Time:    time.Now(),
		Payload: []byte(`{"test": "data"}`),
	}
	
	// Emit the notification
	n.Emit(testNotif)
	
	// Verify the callback was called with the correct notification
	select {
	case notif := <-received:
		if notif.Type != testNotif.Type {
			t.Errorf("Expected type %q, got %q", testNotif.Type, notif.Type)
		}
		if !notif.Time.Equal(testNotif.Time) {
			t.Errorf("Expected time %v, got %v", testNotif.Time, notif.Time)
		}
		if string(notif.Payload) != string(testNotif.Payload) {
			t.Errorf("Expected payload %q, got %q", testNotif.Payload, notif.Payload)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Callback was not called within timeout")
	}
}

func TestNotifierMultipleSubscribers(t *testing.T) {
	// Create a notifier with buffer size 0
	n := NewNotifier(0)
	
	// Create channels to receive notifications from two callbacks
	received1 := make(chan Notification, 1)
	received2 := make(chan Notification, 1)
	
	// Subscribe two callbacks
	callback1Called := false
	callback2Called := false
	
	n.Subscribe(func(notification Notification) {
		received1 <- notification
		callback1Called = true
	})
	
	n.Subscribe(func(notification Notification) {
		received2 <- notification
		callback2Called = true
	})
	
	// Create a test notification
	testNotif := Notification{
		Type:    "test_type",
		Time:    time.Now(),
		Payload: []byte(`{"test": "data"}`),
	}
	
	// Emit the notification
	n.Emit(testNotif)
	
	// Verify both callbacks were called with the correct notification
	select {
	case notif := <-received1:
		if notif.Type != testNotif.Type {
			t.Errorf("Callback1: Expected type %q, got %q", testNotif.Type, notif.Type)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Callback1 was not called within timeout")
	}
	
	select {
	case notif := <-received2:
		if notif.Type != testNotif.Type {
			t.Errorf("Callback2: Expected type %q, got %q", testNotif.Type, notif.Type)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Callback2 was not called within timeout")
	}
	
	// Verify both callbacks were actually called
	if !callback1Called {
		t.Error("Callback1 was not called")
	}
	if !callback2Called {
		t.Error("Callback2 was not called")
	}
}

func TestNotifierConcurrentEmit(t *testing.T) {
	// Create a notifier with buffer size 0
	n := NewNotifier(0)
	
	// Create a channel to receive all notifications
	const numGoroutines = 10
	const notificationsPerGoroutine = 10
	totalNotifications := numGoroutines * notificationsPerGoroutine
	
	received := make(chan Notification, totalNotifications)
	
	// Subscribe a callback that records all notifications
	n.Subscribe(func(notification Notification) {
		received <- notification
	})
	
	// Start multiple goroutines that emit notifications concurrently
	done := make(chan bool, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			for j := 0; j < notificationsPerGoroutine; j++ {
				n.Emit(Notification{
					Type:    "concurrent_test",
					Time:    time.Now(),
					Payload: []byte(`{"goroutine": "` + string(rune(id)) + `", "index": "` + string(rune(j)) + `"}`),
				})
			}
			done <- true
		}(i)
	}
	
	// Wait for all goroutines to finish
	for i := 0; i < numGoroutines; i++ {
		<-done
	}
	
	// Collect all received notifications
	receivedNotifications := make([]Notification, 0, totalNotifications)
	timeout := time.After(1 * time.Second)
	for i := 0; i < totalNotifications; i++ {
		select {
		case notif := <-received:
			receivedNotifications = append(receivedNotifications, notif)
		case <-timeout:
			t.Fatalf("Timeout waiting for notifications. Received %d out of %d", i, totalNotifications)
		}
	}
	
	// Verify we received all notifications
	if len(receivedNotifications) != totalNotifications {
		t.Errorf("Expected %d notifications, received %d", totalNotifications, len(receivedNotifications))
	}
	
	// Verify all notifications have the correct type
	for _, notif := range receivedNotifications {
		if notif.Type != "concurrent_test" {
			t.Errorf("Expected type 'concurrent_test', got %q", notif.Type)
		}
	}
}

func TestNotifierBuffer(t *testing.T) {
	// Create a notifier with buffer size 3
	n := NewNotifier(3)
	
	// Emit 5 notifications
	for i := 0; i < 5; i++ {
		n.Emit(Notification{
			Type:    "test",
			Time:    time.Now(),
			Payload: []byte(`{"index": "` + string(rune(i)) + `"}`),
		})
	}
	
	// History() should return the last 3 notifications (indices 2, 3, 4)
	history := n.History()
	
	if len(history) != 3 {
		t.Fatalf("Expected 3 notifications in history, got %d", len(history))
	}
	
	// Verify the notifications are the last 3 (oldest first)
	for i, notif := range history {
		expectedIndex := i + 2 // indices 2, 3, 4
		expectedPayload := `{"index": "` + string(rune(expectedIndex)) + `"}`
		if string(notif.Payload) != expectedPayload {
			t.Errorf("History[%d]: Expected payload %q, got %q", i, expectedPayload, notif.Payload)
		}
		if notif.Type != "test" {
			t.Errorf("History[%d]: Expected type 'test', got %q", i, notif.Type)
		}
	}
}

func TestNotifierZeroBuffer(t *testing.T) {
	// Create a notifier with buffer size 0 (no buffering)
	n := NewNotifier(0)
	
	// Emit some notifications
	for i := 0; i < 5; i++ {
		n.Emit(Notification{
			Type:    "test",
			Time:    time.Now(),
			Payload: []byte(`{"index": "` + string(rune(i)) + `"}`),
		})
	}
	
	// History() should return empty slice when bufferSize=0
	history := n.History()
	
	if len(history) != 0 {
		t.Fatalf("Expected empty history when bufferSize=0, got %d notifications", len(history))
	}
}

func TestNewMessageReceivedNotification(t *testing.T) {
	from := "seed"
	content := "hello world"
	
	notif := NewMessageReceivedNotification(from, content)
	
	// Verify Type
	if notif.Type != "message_received" {
		t.Errorf("Expected Type 'message_received', got %q", notif.Type)
	}
	
	// Verify Time is recent (within last second)
	if time.Since(notif.Time) > time.Second {
		t.Errorf("Time should be recent, got %v", notif.Time)
	}
	
	// Verify Payload marshals to expected JSON
	var payload map[string]string
	if err := json.Unmarshal(notif.Payload, &payload); err != nil {
		t.Fatalf("Failed to unmarshal payload: %v", err)
	}
	
	if payload["from"] != from {
		t.Errorf("Expected payload.from %q, got %q", from, payload["from"])
	}
	if payload["content"] != content {
		t.Errorf("Expected payload.content %q, got %q", content, payload["content"])
	}
	
	// Verify exact JSON (don't depend on key order, just verify it's valid JSON)
	var decoded map[string]string
	if err := json.Unmarshal(notif.Payload, &decoded); err != nil {
		t.Errorf("Payload is not valid JSON: %v", err)
	}
	if len(decoded) != 2 {
		t.Errorf("Expected 2 keys in payload, got %d", len(decoded))
	}
}

func TestNewPeerOnlineNotification(t *testing.T) {
	name := "node2"
	addr := "127.0.0.1:50052"
	
	notif := NewPeerOnlineNotification(name, addr)
	
	// Verify Type
	if notif.Type != "peer_online" {
		t.Errorf("Expected Type 'peer_online', got %q", notif.Type)
	}
	
	// Verify Time is recent
	if time.Since(notif.Time) > time.Second {
		t.Errorf("Time should be recent, got %v", notif.Time)
	}
	
	// Verify Payload
	var payload map[string]string
	if err := json.Unmarshal(notif.Payload, &payload); err != nil {
		t.Fatalf("Failed to unmarshal payload: %v", err)
	}
	
	if payload["name"] != name {
		t.Errorf("Expected payload.name %q, got %q", name, payload["name"])
	}
	if payload["addr"] != addr {
		t.Errorf("Expected payload.addr %q, got %q", addr, payload["addr"])
	}
	
	// Verify exact JSON (don't depend on key order, just verify it's valid JSON)
	var decoded map[string]string
	if err := json.Unmarshal(notif.Payload, &decoded); err != nil {
		t.Errorf("Payload is not valid JSON: %v", err)
	}
	if len(decoded) != 2 {
		t.Errorf("Expected 2 keys in payload, got %d", len(decoded))
	}
}

func TestNewPeerOfflineNotification(t *testing.T) {
	name := "node2"
	reason := "disconnect"
	
	notif := NewPeerOfflineNotification(name, reason)
	
	// Verify Type
	if notif.Type != "peer_offline" {
		t.Errorf("Expected Type 'peer_offline', got %q", notif.Type)
	}
	
	// Verify Time is recent
	if time.Since(notif.Time) > time.Second {
		t.Errorf("Time should be recent, got %v", notif.Time)
	}
	
	// Verify Payload
	var payload map[string]string
	if err := json.Unmarshal(notif.Payload, &payload); err != nil {
		t.Fatalf("Failed to unmarshal payload: %v", err)
	}
	
	if payload["name"] != name {
		t.Errorf("Expected payload.name %q, got %q", name, payload["name"])
	}
	if payload["reason"] != reason {
		t.Errorf("Expected payload.reason %q, got %q", reason, payload["reason"])
	}
	
	// Verify exact JSON (don't depend on key order, just verify it's valid JSON)
	var decoded map[string]string
	if err := json.Unmarshal(notif.Payload, &decoded); err != nil {
		t.Errorf("Payload is not valid JSON: %v", err)
	}
	if len(decoded) != 2 {
		t.Errorf("Expected 2 keys in payload, got %d", len(decoded))
	}
}