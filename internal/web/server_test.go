package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"p2ptest/internal/notifier"
	"github.com/gorilla/websocket"
)

type mockPeerInfoProvider struct {
	peers      []map[string]string
	nameToAddr map[string]string
}

func (m *mockPeerInfoProvider) GetOnlinePeers() []map[string]string {
	return m.peers
}

func (m *mockPeerInfoProvider) GetAddrByName(name string) (string, error) {
	if addr, ok := m.nameToAddr[name]; ok {
		return addr, nil
	}
	return "", nil
}

type mockMessageSender struct {
	lastAddr        string
	lastContent     string
	broadcastContent string
	broadcastSuccess int
	broadcastFailed  int
	err             error
}

func (m *mockMessageSender) SendTextMessage(targetAddr string, content string) error {
	m.lastAddr = targetAddr
	m.lastContent = content
	return m.err
}

func (m *mockMessageSender) BroadcastMessage(content string) (int, int) {
	m.broadcastContent = content
	return m.broadcastSuccess, m.broadcastFailed
}

type mockPeerConnector struct {
	connectErr     error
	disconnectAddr string
	disconnectErr  error
}

func (m *mockPeerConnector) ConnectToPeer(addr string) error {
	return m.connectErr
}

func (m *mockPeerConnector) DisconnectPeer(name string) (string, error) {
	return m.disconnectAddr, m.disconnectErr
}

type mockPingSender struct {
	latency time.Duration
	err     error
}

func (m *mockPingSender) SendPing(targetAddr string) (time.Duration, error) {
	return m.latency, m.err
}

type mockStatusSetter struct {
	status string
	err    error
}

func (m *mockStatusSetter) SetNodeStatus(status string) error {
	if m.err != nil {
		return m.err
	}
	m.status = status
	return nil
}

func (m *mockStatusSetter) GetNodeStatus() string {
	return m.status
}

func newTestServer(info *mockPeerInfoProvider, sender *mockMessageSender, n *notifier.Notifier) *Server {
	connector := &mockPeerConnector{}
	pingSender := &mockPingSender{}
	statusSetter := &mockStatusSetter{status: "online"}
	return NewServer(":8080", info, sender, connector, pingSender, statusSetter, n)
}

func TestServerGetPeers(t *testing.T) {
	mockInfo := &mockPeerInfoProvider{
		peers: []map[string]string{
			{"name": "seed", "uuid": "uuid1", "addr": "127.0.0.1:50051"},
			{"name": "node2", "uuid": "uuid2", "addr": "127.0.0.1:50052"},
		},
	}

	server := newTestServer(mockInfo, &mockMessageSender{}, notifier.NewNotifier(10))

	req := httptest.NewRequest("GET", "/api/peers", nil)
	w := httptest.NewRecorder()

	handler := server.getHandler()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	if w.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Errorf("expected CORS header, got %s", w.Header().Get("Access-Control-Allow-Origin"))
	}

	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to parse JSON response: %v", err)
	}

	peers, ok := response["peers"].([]interface{})
	if !ok {
		t.Fatalf("expected 'peers' array, got %T", response["peers"])
	}

	if len(peers) != 2 {
		t.Errorf("expected 2 peers, got %d", len(peers))
	}
}

func TestServerSend(t *testing.T) {
	mockInfo := &mockPeerInfoProvider{
		nameToAddr: map[string]string{
			"seed": "127.0.0.1:50051",
		},
	}
	mockSender := &mockMessageSender{}

	server := newTestServer(mockInfo, mockSender, notifier.NewNotifier(10))

	t.Run("success", func(t *testing.T) {
		reqBody := `{"target": "seed", "content": "hello world"}`
		req := httptest.NewRequest("POST", "/api/send", strings.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")

		w := httptest.NewRecorder()
		handler := server.getHandler()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var response map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatalf("failed to parse JSON response: %v", err)
		}

		if success, ok := response["success"].(bool); !ok || !success {
			t.Errorf("expected success: true, got %v", response["success"])
		}

		if mockSender.lastAddr != "127.0.0.1:50051" {
			t.Errorf("expected addr '127.0.0.1:50051', got %s", mockSender.lastAddr)
		}
	})

	t.Run("failure", func(t *testing.T) {
		mockSender.err = fmt.Errorf("no stream connection")
		mockSender.lastAddr = ""
		mockSender.lastContent = ""

		reqBody := `{"target": "seed", "content": "hello world"}`
		req := httptest.NewRequest("POST", "/api/send", strings.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")

		w := httptest.NewRecorder()
		handler := server.getHandler()
		handler.ServeHTTP(w, req)

		var response map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatalf("failed to parse JSON: %v", err)
		}

		if success, ok := response["success"].(bool); !ok || success {
			t.Errorf("expected success: false, got %v", response["success"])
		}
	})

	t.Run("invalid json", func(t *testing.T) {
		reqBody := `{"target": "seed", "content": "hello world"`
		req := httptest.NewRequest("POST", "/api/send", strings.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")

		w := httptest.NewRecorder()
		handler := server.getHandler()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", w.Code)
		}
	})

	t.Run("missing fields", func(t *testing.T) {
		reqBody := `{"target": "seed"}`
		req := httptest.NewRequest("POST", "/api/send", strings.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")

		w := httptest.NewRecorder()
		handler := server.getHandler()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", w.Code)
		}
	})
}

func TestServerBroadcast(t *testing.T) {
	mockSender := &mockMessageSender{broadcastSuccess: 3, broadcastFailed: 1}
	server := newTestServer(&mockPeerInfoProvider{}, mockSender, notifier.NewNotifier(10))

	reqBody := `{"content": "hello all"}`
	req := httptest.NewRequest("POST", "/api/broadcast", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	handler := server.getHandler()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var response broadcastResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if response.Success != 3 {
		t.Errorf("expected 3 success, got %d", response.Success)
	}
	if response.Failed != 1 {
		t.Errorf("expected 1 failed, got %d", response.Failed)
	}
}

func TestServerConnect(t *testing.T) {
	connector := &mockPeerConnector{}
	sender := &mockMessageSender{}
	server := NewServer(":8080", &mockPeerInfoProvider{}, sender, connector, &mockPingSender{}, &mockStatusSetter{status: "online"}, notifier.NewNotifier(10))

	t.Run("success", func(t *testing.T) {
		reqBody := `{"addr": "127.0.0.1:50053"}`
		req := httptest.NewRequest("POST", "/api/connect", strings.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")

		w := httptest.NewRecorder()
		handler := server.getHandler()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected 200, got %d", w.Code)
		}

		var response connectResponse
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatalf("failed to parse JSON: %v", err)
		}
		if !response.Success {
			t.Error("expected success: true")
		}
	})

	t.Run("failure", func(t *testing.T) {
		connector.connectErr = fmt.Errorf("connection refused")

		reqBody := `{"addr": "127.0.0.1:50099"}`
		req := httptest.NewRequest("POST", "/api/connect", strings.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")

		w := httptest.NewRecorder()
		handler := server.getHandler()
		handler.ServeHTTP(w, req)

		var response connectResponse
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatalf("failed to parse JSON: %v", err)
		}
		if response.Success {
			t.Error("expected success: false")
		}
	})
}

func TestServerDisconnect(t *testing.T) {
	connector := &mockPeerConnector{disconnectAddr: "127.0.0.1:50052"}
	sender := &mockMessageSender{}
	server := NewServer(":8080", &mockPeerInfoProvider{}, sender, connector, &mockPingSender{}, &mockStatusSetter{status: "online"}, notifier.NewNotifier(10))

	t.Run("success", func(t *testing.T) {
		reqBody := `{"name": "node2"}`
		req := httptest.NewRequest("POST", "/api/disconnect", strings.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")

		w := httptest.NewRecorder()
		handler := server.getHandler()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected 200, got %d", w.Code)
		}

		var response disconnectResponse
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatalf("failed to parse JSON: %v", err)
		}
		if !response.Success {
			t.Error("expected success: true")
		}
		if response.Addr != "127.0.0.1:50052" {
			t.Errorf("expected addr '127.0.0.1:50052', got %s", response.Addr)
		}
	})

	t.Run("failure", func(t *testing.T) {
		connector.disconnectErr = fmt.Errorf("节点不在线")

		reqBody := `{"name": "unknown"}`
		req := httptest.NewRequest("POST", "/api/disconnect", strings.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")

		w := httptest.NewRecorder()
		handler := server.getHandler()
		handler.ServeHTTP(w, req)

		var response disconnectResponse
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatalf("failed to parse JSON: %v", err)
		}
		if response.Success {
			t.Error("expected success: false")
		}
	})
}

func TestServerGetNotifications(t *testing.T) {
	n := notifier.NewNotifier(10)

	n.Emit(notifier.NewMessageReceivedNotification("seed", "hello"))
	n.Emit(notifier.NewPeerOnlineNotification("node2", "127.0.0.1:50052"))

	server := newTestServer(&mockPeerInfoProvider{}, &mockMessageSender{}, n)

	req := httptest.NewRequest("GET", "/api/notifications", nil)
	w := httptest.NewRecorder()

	handler := server.getHandler()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	if w.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Errorf("expected CORS header, got %s", w.Header().Get("Access-Control-Allow-Origin"))
	}

	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to parse JSON response: %v", err)
	}

	notifications, ok := response["notifications"].([]interface{})
	if !ok {
		t.Fatalf("expected 'notifications' array, got %T", response["notifications"])
	}

	if len(notifications) != 2 {
		t.Errorf("expected 2 notifications, got %d", len(notifications))
	}
}

func TestWebSocketReceivesNotification(t *testing.T) {
	n := notifier.NewNotifier(10)
	server := newTestServer(&mockPeerInfoProvider{}, &mockMessageSender{}, n)

	testServer := httptest.NewServer(server.getHandler())
	defer testServer.Close()

	wsURL := "ws" + strings.TrimPrefix(testServer.URL, "http") + "/ws"

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("failed to connect WebSocket: %v", err)
	}
	defer conn.Close()

	time.Sleep(50 * time.Millisecond)

	n.Emit(notifier.NewMessageReceivedNotification("seed", "hello via ws"))

	conn.SetReadDeadline(time.Now().Add(1 * time.Second))

	var msg map[string]interface{}
	err = conn.ReadJSON(&msg)
	if err != nil {
		t.Fatalf("failed to read WebSocket message: %v", err)
	}

	if msg["Type"] != "message_received" {
		t.Errorf("expected type 'message_received', got %v", msg["Type"])
	}
}

func TestWebSocketReceivesHistory(t *testing.T) {
	n := notifier.NewNotifier(10)

	n.Emit(notifier.NewMessageReceivedNotification("seed", "message 1"))
	n.Emit(notifier.NewPeerOnlineNotification("node2", "127.0.0.1:50052"))
	n.Emit(notifier.NewMessageReceivedNotification("node2", "message 2"))

	server := newTestServer(&mockPeerInfoProvider{}, &mockMessageSender{}, n)

	testServer := httptest.NewServer(server.getHandler())
	defer testServer.Close()

	wsURL := "ws" + strings.TrimPrefix(testServer.URL, "http") + "/ws"

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("failed to connect WebSocket: %v", err)
	}
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(1 * time.Second))

	var notifications []map[string]interface{}
	for i := 0; i < 3; i++ {
		var msg map[string]interface{}
		err = conn.ReadJSON(&msg)
		if err != nil {
			t.Fatalf("failed to read WebSocket message %d: %v", i+1, err)
		}
		notifications = append(notifications, msg)
	}

	if len(notifications) != 3 {
		t.Errorf("expected 3 notifications, got %d", len(notifications))
	}

	n.Emit(notifier.NewPeerOfflineNotification("node2", "disconnect"))

	var liveMsg map[string]interface{}
	err = conn.ReadJSON(&liveMsg)
	if err != nil {
		t.Fatalf("failed to read live WebSocket message: %v", err)
	}

	if liveMsg["Type"] != "peer_offline" {
		t.Errorf("expected 'peer_offline', got %v", liveMsg["Type"])
	}
}

func TestServerGetMessages(t *testing.T) {
	n := notifier.NewNotifier(10)

	n.Emit(notifier.NewMessageReceivedNotification("seed", "hello"))
	n.Emit(notifier.NewPeerOnlineNotification("node2", "127.0.0.1:50052"))
	n.Emit(notifier.NewMessageReceivedNotification("node2", "world"))

	server := newTestServer(&mockPeerInfoProvider{}, &mockMessageSender{}, n)

	req := httptest.NewRequest("GET", "/api/messages", nil)
	w := httptest.NewRecorder()

	handler := server.getHandler()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to parse JSON response: %v", err)
	}

	messages, ok := response["messages"].([]interface{})
	if !ok {
		t.Fatalf("expected 'messages' array, got %T", response["messages"])
	}

	if len(messages) != 2 {
		t.Errorf("expected 2 messages (filtered from 3 notifications), got %d", len(messages))
	}

	for _, m := range messages {
		msg, ok := m.(map[string]interface{})
		if !ok {
			t.Fatalf("expected map, got %T", m)
		}
		if msg["Type"] != "message_received" {
			t.Errorf("expected type 'message_received', got %v", msg["Type"])
		}
	}
}

func TestServerPing(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		pingSender := &mockPingSender{latency: 15 * time.Millisecond}
		server := NewServer(":8080", &mockPeerInfoProvider{
			nameToAddr: map[string]string{"seed": "127.0.0.1:50051"},
		}, &mockMessageSender{}, &mockPeerConnector{}, pingSender, &mockStatusSetter{status: "online"}, notifier.NewNotifier(10))

		reqBody := `{"target": "seed"}`
		req := httptest.NewRequest("POST", "/api/ping", strings.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		server.getHandler().ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp pingResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if !resp.Success {
			t.Errorf("expected success=true, got error: %s", resp.Error)
		}
		if resp.LatencyMs != 15 {
			t.Errorf("expected latency 15ms, got %dms", resp.LatencyMs)
		}
	})

	t.Run("ping failure", func(t *testing.T) {
		pingSender := &mockPingSender{err: fmt.Errorf("timeout")}
		server := NewServer(":8080", &mockPeerInfoProvider{}, &mockMessageSender{}, &mockPeerConnector{}, pingSender, &mockStatusSetter{status: "online"}, notifier.NewNotifier(10))

		reqBody := `{"target": "127.0.0.1:50051"}`
		req := httptest.NewRequest("POST", "/api/ping", strings.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		server.getHandler().ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Code)
		}

		var resp pingResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if resp.Success {
			t.Error("expected success=false")
		}
		if resp.Error != "timeout" {
			t.Errorf("expected error 'timeout', got %q", resp.Error)
		}
	})

	t.Run("missing target", func(t *testing.T) {
		server := newTestServer(&mockPeerInfoProvider{}, &mockMessageSender{}, notifier.NewNotifier(10))

		reqBody := `{"target": ""}`
		req := httptest.NewRequest("POST", "/api/ping", strings.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		server.getHandler().ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", w.Code)
		}
	})

	t.Run("wrong method", func(t *testing.T) {
		server := newTestServer(&mockPeerInfoProvider{}, &mockMessageSender{}, notifier.NewNotifier(10))

		req := httptest.NewRequest("GET", "/api/ping", nil)
		w := httptest.NewRecorder()
		server.getHandler().ServeHTTP(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", w.Code)
		}
	})

	t.Run("resolve name to address", func(t *testing.T) {
		pingSender := &mockPingSender{latency: 5 * time.Millisecond}
		server := NewServer(":8080", &mockPeerInfoProvider{
			nameToAddr: map[string]string{"node2": "127.0.0.1:50052"},
		}, &mockMessageSender{}, &mockPeerConnector{}, pingSender, &mockStatusSetter{status: "online"}, notifier.NewNotifier(10))

		reqBody := `{"target": "node2"}`
		req := httptest.NewRequest("POST", "/api/ping", strings.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		server.getHandler().ServeHTTP(w, req)

		var resp pingResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode: %v", err)
		}
		if !resp.Success {
			t.Errorf("expected success, got error: %s", resp.Error)
		}
		if pingSender.latency == 0 {
		}
	})
}

func TestServerStartStop(t *testing.T) {
	server := newTestServer(&mockPeerInfoProvider{}, &mockMessageSender{}, notifier.NewNotifier(10))

	err := server.Start()
	if err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	err = server.Stop()
	if err != nil {
		t.Fatalf("failed to stop server: %v", err)
	}

	err = server.Stop()
	if err != nil {
		t.Fatalf("second Stop() should not return error, got: %v", err)
	}
}

func TestServerStatus(t *testing.T) {
	t.Run("get status", func(t *testing.T) {
		statusSetter := &mockStatusSetter{status: "online"}
		server := NewServer(":8080", &mockPeerInfoProvider{}, &mockMessageSender{}, &mockPeerConnector{}, &mockPingSender{}, statusSetter, notifier.NewNotifier(10))

		req := httptest.NewRequest("GET", "/api/status", nil)
		w := httptest.NewRecorder()
		server.getHandler().ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Code)
		}

		var resp statusResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode: %v", err)
		}
		if !resp.Success {
			t.Error("expected success=true")
		}
		if resp.Status != "online" {
			t.Errorf("expected status 'online', got %q", resp.Status)
		}
	})

	t.Run("set status success", func(t *testing.T) {
		statusSetter := &mockStatusSetter{status: "online"}
		server := NewServer(":8080", &mockPeerInfoProvider{}, &mockMessageSender{}, &mockPeerConnector{}, &mockPingSender{}, statusSetter, notifier.NewNotifier(10))

		reqBody := `{"status": "busy"}`
		req := httptest.NewRequest("POST", "/api/status", strings.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		server.getHandler().ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Code)
		}

		var resp statusResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode: %v", err)
		}
		if !resp.Success {
			t.Error("expected success=true")
		}
		if resp.Status != "busy" {
			t.Errorf("expected status 'busy', got %q", resp.Status)
		}
		if statusSetter.status != "busy" {
			t.Errorf("mock not updated, got %q", statusSetter.status)
		}
	})

	t.Run("set status failure", func(t *testing.T) {
		statusSetter := &mockStatusSetter{err: fmt.Errorf("invalid status")}
		server := NewServer(":8080", &mockPeerInfoProvider{}, &mockMessageSender{}, &mockPeerConnector{}, &mockPingSender{}, statusSetter, notifier.NewNotifier(10))

		reqBody := `{"status": "unknown"}`
		req := httptest.NewRequest("POST", "/api/status", strings.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		server.getHandler().ServeHTTP(w, req)

		var resp statusResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode: %v", err)
		}
		if resp.Success {
			t.Error("expected success=false")
		}
		if resp.Error != "invalid status" {
			t.Errorf("expected error 'invalid status', got %q", resp.Error)
		}
	})

	t.Run("missing status field", func(t *testing.T) {
		server := newTestServer(&mockPeerInfoProvider{}, &mockMessageSender{}, notifier.NewNotifier(10))

		reqBody := `{"status": ""}`
		req := httptest.NewRequest("POST", "/api/status", strings.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		server.getHandler().ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", w.Code)
		}
	})

	t.Run("wrong method", func(t *testing.T) {
		server := newTestServer(&mockPeerInfoProvider{}, &mockMessageSender{}, notifier.NewNotifier(10))

		req := httptest.NewRequest("DELETE", "/api/status", nil)
		w := httptest.NewRecorder()
		server.getHandler().ServeHTTP(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", w.Code)
		}
	})
}
