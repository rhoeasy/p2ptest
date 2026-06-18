package notifier

import (
	"encoding/json"
	"time"
)

// Notification represents a system event that users need to actively monitor.
type Notification struct {
	Type    string          // snake_case: "message_received", "peer_online", "peer_offline"
	Time    time.Time
	Payload json.RawMessage // structured data, varies by Type
}

// NewMessageReceivedNotification creates a notification for when a message is received.
// Payload: {"from": "seed", "content": "hello"}
func NewMessageReceivedNotification(from string, content string) Notification {
	payload, _ := json.Marshal(map[string]string{
		"from":    from,
		"content": content,
	})
	return Notification{
		Type:    "message_received",
		Time:    time.Now(),
		Payload: payload,
	}
}

// NewPeerOnlineNotification creates a notification for when a peer comes online.
// Payload: {"name": "node2", "addr": "127.0.0.1:50052"}
func NewPeerOnlineNotification(name string, addr string) Notification {
	payload, _ := json.Marshal(map[string]string{
		"name": name,
		"addr": addr,
	})
	return Notification{
		Type:    "peer_online",
		Time:    time.Now(),
		Payload: payload,
	}
}

// NewPeerOfflineNotification creates a notification for when a peer goes offline.
// Payload: {"name": "node2", "reason": "disconnect"}
func NewPeerOfflineNotification(name string, reason string) Notification {
	payload, _ := json.Marshal(map[string]string{
		"name":   name,
		"reason": reason,
	})
	return Notification{
		Type:    "peer_offline",
		Time:    time.Now(),
		Payload: payload,
	}
}

// NewPeerDiscoveredNotification creates a notification for when a new peer should be connected to.
// Payload: {"name": "node3", "addr": "127.0.0.1:50053", "uuid": "..."}
func NewPeerDiscoveredNotification(name string, addr string, uuid string) Notification {
	payload, _ := json.Marshal(map[string]string{
		"name": name,
		"addr": addr,
		"uuid": uuid,
	})
	return Notification{
		Type:    "peer_discovered",
		Time:    time.Now(),
		Payload: payload,
	}
}