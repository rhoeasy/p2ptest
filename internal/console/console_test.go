package console

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"p2ptest/internal/notifier"
	tea "github.com/charmbracelet/bubbletea"
)

type mockPeerInfoProvider struct {
	peers      []map[string]string
	nameToAddr map[string]string
	err        error
}

func (m *mockPeerInfoProvider) GetOnlinePeers() []map[string]string {
	return m.peers
}

func (m *mockPeerInfoProvider) GetAddrByName(name string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
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
	connectAddr    string
	connectErr     error
	disconnectName string
	disconnectAddr string
	disconnectErr  error
}

func (m *mockPeerConnector) ConnectToPeer(addr string) error {
	m.connectAddr = addr
	return m.connectErr
}

func (m *mockPeerConnector) DisconnectPeer(name string) (string, error) {
	m.disconnectName = name
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

func TestConsoleModelInit(t *testing.T) {
	info := &mockPeerInfoProvider{}
	sender := &mockMessageSender{}
	connector := &mockPeerConnector{}
	n := notifier.NewNotifier(0)

	m := createTestModel(info, sender, connector, n)

	cmd := m.Init()

	if !m.textInput.Focused() {
		t.Error("textInput should be focused")
	}

	if m.textInput.Prompt != "> " {
		t.Errorf("Expected prompt '> ', got '%s'", m.textInput.Prompt)
	}

	if cmd == nil {
		t.Error("Init() should return a tea.Cmd")
	}
}

func TestConsoleExitCommand(t *testing.T) {
	info := &mockPeerInfoProvider{}
	sender := &mockMessageSender{}
	connector := &mockPeerConnector{}
	n := notifier.NewNotifier(0)

	m := createTestModel(info, sender, connector, n)
	m.textInput.SetValue("exit")

	msg := tea.KeyMsg{Type: tea.KeyEnter}
	newModel, cmd := m.Update(msg)

	m, ok := newModel.(model)
	if !ok {
		t.Fatal("Update should return model type")
	}

	if !m.quitting {
		t.Error("model.quitting should be true after exit command")
	}

	if cmd == nil {
		t.Error("Update should return a tea.Cmd for exit")
	}
}

func TestConsoleListCommand(t *testing.T) {
	info := &mockPeerInfoProvider{
		peers: []map[string]string{
			{"name": "seed", "addr": "127.0.0.1:50051", "last_active": "12:00:00", "online_for": "30s", "stream": "已连接"},
			{"name": "node2", "addr": "127.0.0.1:50052"},
		},
	}
	sender := &mockMessageSender{}
	connector := &mockPeerConnector{}
	n := notifier.NewNotifier(0)

	m := createTestModel(info, sender, connector, n)
	m.textInput.SetValue("list")

	msg := tea.KeyMsg{Type: tea.KeyEnter}
	newModel, _ := m.Update(msg)

	m, ok := newModel.(model)
	if !ok {
		t.Fatal("Update should return model type")
	}

	view := m.View()

	if !contains(view, "seed") {
		t.Error("View should contain 'seed'")
	}
	if !contains(view, "127.0.0.1:50051") {
		t.Error("View should contain '127.0.0.1:50051'")
	}
	if !contains(view, "最后心跳") {
		t.Error("View should show last heartbeat info")
	}
	if !contains(view, "在线时长") {
		t.Error("View should show online duration")
	}
	if !contains(view, "连接状态") {
		t.Error("View should show connection status")
	}
}

func TestConsoleSendCommand(t *testing.T) {
	info := &mockPeerInfoProvider{
		nameToAddr: map[string]string{
			"seed": "127.0.0.1:50051",
		},
	}
	sender := &mockMessageSender{}
	connector := &mockPeerConnector{}
	n := notifier.NewNotifier(0)

	m := createTestModel(info, sender, connector, n)
	m.textInput.SetValue("send seed hello")

	msg := tea.KeyMsg{Type: tea.KeyEnter}
	newModel, _ := m.Update(msg)

	m, ok := newModel.(model)
	if !ok {
		t.Fatal("Update should return model type")
	}

	if sender.lastAddr != "127.0.0.1:50051" {
		t.Errorf("Expected sender.lastAddr '127.0.0.1:50051', got '%s'", sender.lastAddr)
	}
	if sender.lastContent != "hello" {
		t.Errorf("Expected sender.lastContent 'hello', got '%s'", sender.lastContent)
	}

	view := m.View()
	if !contains(view, "[SUCCESS]") {
		t.Error("View should contain '[SUCCESS]' after successful send")
	}
}

func TestConsoleBroadcastCommand(t *testing.T) {
	info := &mockPeerInfoProvider{}
	sender := &mockMessageSender{broadcastSuccess: 2, broadcastFailed: 0}
	connector := &mockPeerConnector{}
	n := notifier.NewNotifier(0)

	m := createTestModel(info, sender, connector, n)
	m.textInput.SetValue("broadcast hello all")

	msg := tea.KeyMsg{Type: tea.KeyEnter}
	newModel, _ := m.Update(msg)

	m, ok := newModel.(model)
	if !ok {
		t.Fatal("Update should return model type")
	}

	if sender.broadcastContent != "hello all" {
		t.Errorf("Expected broadcastContent 'hello all', got '%s'", sender.broadcastContent)
	}

	view := m.View()
	if !contains(view, "广播完成") {
		t.Error("View should contain broadcast result")
	}
}

func TestConsoleHelpCommand(t *testing.T) {
	info := &mockPeerInfoProvider{}
	sender := &mockMessageSender{}
	connector := &mockPeerConnector{}
	n := notifier.NewNotifier(0)

	m := createTestModel(info, sender, connector, n)
	m.textInput.SetValue("help")

	msg := tea.KeyMsg{Type: tea.KeyEnter}
	newModel, _ := m.Update(msg)

	m, ok := newModel.(model)
	if !ok {
		t.Fatal("Update should return model type")
	}

	view := m.View()
	if !contains(view, "帮助信息") {
		t.Error("View should show help info")
	}
	if !contains(view, "broadcast") {
		t.Error("Help should mention broadcast command")
	}
	if !contains(view, "connect") {
		t.Error("Help should mention connect command")
	}
	if !contains(view, "disconnect") {
		t.Error("Help should mention disconnect command")
	}
}

func TestConsoleConnectCommand(t *testing.T) {
	info := &mockPeerInfoProvider{}
	sender := &mockMessageSender{}
	connector := &mockPeerConnector{disconnectAddr: "127.0.0.1:50053"}
	n := notifier.NewNotifier(0)

	m := createTestModel(info, sender, connector, n)
	m.textInput.SetValue("connect 127.0.0.1:50053")

	msg := tea.KeyMsg{Type: tea.KeyEnter}
	newModel, _ := m.Update(msg)

	m, ok := newModel.(model)
	if !ok {
		t.Fatal("Update should return model type")
	}

	if connector.connectAddr != "127.0.0.1:50053" {
		t.Errorf("Expected connectAddr '127.0.0.1:50053', got '%s'", connector.connectAddr)
	}

	view := m.View()
	if !contains(view, "[SUCCESS]") {
		t.Error("View should show success after connect")
	}
}

func TestConsoleDisconnectCommand(t *testing.T) {
	info := &mockPeerInfoProvider{}
	sender := &mockMessageSender{}
	connector := &mockPeerConnector{disconnectAddr: "127.0.0.1:50052"}
	n := notifier.NewNotifier(0)

	m := createTestModel(info, sender, connector, n)
	m.textInput.SetValue("disconnect node2")

	msg := tea.KeyMsg{Type: tea.KeyEnter}
	newModel, _ := m.Update(msg)

	m, ok := newModel.(model)
	if !ok {
		t.Fatal("Update should return model type")
	}

	if connector.disconnectName != "node2" {
		t.Errorf("Expected disconnectName 'node2', got '%s'", connector.disconnectName)
	}

	view := m.View()
	if !contains(view, "[SUCCESS]") {
		t.Error("View should show success after disconnect")
	}
}

func TestConsoleConnectInvalidFormat(t *testing.T) {
	info := &mockPeerInfoProvider{}
	sender := &mockMessageSender{}
	connector := &mockPeerConnector{}
	n := notifier.NewNotifier(0)

	m := createTestModel(info, sender, connector, n)
	m.textInput.SetValue("connect noport")

	msg := tea.KeyMsg{Type: tea.KeyEnter}
	newModel, _ := m.Update(msg)

	m, ok := newModel.(model)
	if !ok {
		t.Fatal("Update should return model type")
	}

	view := m.View()
	if !contains(view, "[ERROR]") {
		t.Error("View should show error for invalid connect format")
	}
}

func TestConsoleDisconnectError(t *testing.T) {
	info := &mockPeerInfoProvider{}
	sender := &mockMessageSender{}
	connector := &mockPeerConnector{disconnectErr: fmt.Errorf("节点不在线")}
	n := notifier.NewNotifier(0)

	m := createTestModel(info, sender, connector, n)
	m.textInput.SetValue("disconnect unknown")

	msg := tea.KeyMsg{Type: tea.KeyEnter}
	newModel, _ := m.Update(msg)

	m, ok := newModel.(model)
	if !ok {
		t.Fatal("Update should return model type")
	}

	view := m.View()
	if !contains(view, "[ERROR]") {
		t.Error("View should show error when disconnect fails")
	}
}

func TestConsoleNotificationDisplay(t *testing.T) {
	info := &mockPeerInfoProvider{}
	sender := &mockMessageSender{}
	connector := &mockPeerConnector{}
	n := notifier.NewNotifier(0)

	m := createTestModel(info, sender, connector, n)

	payload, _ := json.Marshal(map[string]string{
		"from":    "seed",
		"content": "hello",
	})
	notification := notifier.Notification{
		Type:    "message_received",
		Time:    time.Now(),
		Payload: payload,
	}

	msg := notificationMsg{notification: notification}
	newModel, _ := m.Update(msg)

	m, ok := newModel.(model)
	if !ok {
		t.Fatal("Update should return model type")
	}

	view := m.View()
	if !contains(view, "[消息] seed: hello") {
		t.Error("View should contain '[消息] seed: hello' for message_received notification")
	}
}

func createTestModel(info PeerInfoProvider, sender MessageSender, connector PeerConnector, ntfr *notifier.Notifier) model {
	return newModel(info, sender, connector, &mockPingSender{latency: 10 * time.Millisecond}, &mockStatusSetter{status: "online"}, ntfr)
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || contains(s[1:], substr)))
}

func TestConsolePingCommand(t *testing.T) {
	info := &mockPeerInfoProvider{
		nameToAddr: map[string]string{"node2": "127.0.0.1:50052"},
	}
	sender := &mockMessageSender{}
	connector := &mockPeerConnector{}
	pingSender := &mockPingSender{latency: 15 * time.Millisecond}
	statusSetter := &mockStatusSetter{status: "online"}
	n := notifier.NewNotifier(0)

	m := newModel(info, sender, connector, pingSender, statusSetter, n)
	m.textInput.SetValue("ping node2")

	newModel, _ := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m, ok := newModel.(model)
	if !ok {
		t.Fatal("Update should return model type")
	}

	view := m.View()
	if !contains(view, "[SUCCESS]") {
		t.Errorf("View should show ping success, got: %s", view)
	}
	if !contains(view, "15ms") {
		t.Errorf("View should show 15ms latency, got: %s", view)
	}
}

func TestConsolePingFailure(t *testing.T) {
	info := &mockPeerInfoProvider{}
	sender := &mockMessageSender{}
	connector := &mockPeerConnector{}
	pingSender := &mockPingSender{err: fmt.Errorf("timeout")}
	statusSetter := &mockStatusSetter{status: "online"}
	n := notifier.NewNotifier(0)

	m := newModel(info, sender, connector, pingSender, statusSetter, n)
	m.textInput.SetValue("ping 127.0.0.1:50099")

	newModel, _ := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m, ok := newModel.(model)
	if !ok {
		t.Fatal("Update should return model type")
	}

	view := m.View()
	if !contains(view, "[ERROR]") {
		t.Errorf("View should show ping error, got: %s", view)
	}
}

func TestConsoleStatusGetCommand(t *testing.T) {
	info := &mockPeerInfoProvider{}
	sender := &mockMessageSender{}
	connector := &mockPeerConnector{}
	pingSender := &mockPingSender{}
	statusSetter := &mockStatusSetter{status: "online"}
	n := notifier.NewNotifier(0)

	m := newModel(info, sender, connector, pingSender, statusSetter, n)
	m.textInput.SetValue("status")

	newModel, _ := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m, ok := newModel.(model)
	if !ok {
		t.Fatal("Update should return model type")
	}

	view := m.View()
	if !contains(view, "当前状态: online") {
		t.Errorf("View should show current status, got: %s", view)
	}
}

func TestConsoleStatusSetCommand(t *testing.T) {
	info := &mockPeerInfoProvider{}
	sender := &mockMessageSender{}
	connector := &mockPeerConnector{}
	pingSender := &mockPingSender{}
	statusSetter := &mockStatusSetter{status: "online"}
	n := notifier.NewNotifier(0)

	m := newModel(info, sender, connector, pingSender, statusSetter, n)
	m.textInput.SetValue("status busy")

	newModel, _ := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m, ok := newModel.(model)
	if !ok {
		t.Fatal("Update should return model type")
	}

	view := m.View()
	if !contains(view, "[SUCCESS]") {
		t.Errorf("View should show success, got: %s", view)
	}
	if statusSetter.status != "busy" {
		t.Errorf("Expected status 'busy', got '%s'", statusSetter.status)
	}
}

func TestConsoleStatusSetError(t *testing.T) {
	info := &mockPeerInfoProvider{}
	sender := &mockMessageSender{}
	connector := &mockPeerConnector{}
	pingSender := &mockPingSender{}
	statusSetter := &mockStatusSetter{err: fmt.Errorf("invalid status")}
	n := notifier.NewNotifier(0)

	m := newModel(info, sender, connector, pingSender, statusSetter, n)
	m.textInput.SetValue("status unknown")

	newModel, _ := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m, ok := newModel.(model)
	if !ok {
		t.Fatal("Update should return model type")
	}

	view := m.View()
	if !contains(view, "[ERROR]") {
		t.Errorf("View should show error, got: %s", view)
	}
}
