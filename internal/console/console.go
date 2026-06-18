package console

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"p2ptest/internal/notifier"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
)

type PeerInfoProvider interface {
	GetOnlinePeers() []map[string]string
	GetAddrByName(name string) (string, error)
}

type MessageSender interface {
	SendTextMessage(targetAddr string, content string) error
	BroadcastMessage(content string) (int, int)
}

type PeerConnector interface {
	ConnectToPeer(addr string) error
	DisconnectPeer(name string) (string, error)
}

type PingSender interface {
	SendPing(targetAddr string) (time.Duration, error)
}

type StatusSetter interface {
	SetNodeStatus(status string) error
	GetNodeStatus() string
}

type model struct {
	textInput     textinput.Model
	info          PeerInfoProvider
	sender        MessageSender
	connector     PeerConnector
	pingSender    PingSender
	statusSetter  StatusSetter
	notifier      *notifier.Notifier
	notifications []string
	quitting      bool
	err           error
	output        []string
}

type notificationMsg struct {
	notification notifier.Notification
}

func newModel(info PeerInfoProvider, sender MessageSender, connector PeerConnector, pingSender PingSender, statusSetter StatusSetter, n *notifier.Notifier) model {
	ti := textinput.New()
	ti.Placeholder = ""
	ti.Prompt = "> "
	ti.Focus()
	ti.CharLimit = 256
	ti.Width = 80

	return model{
		textInput:     ti,
		info:          info,
		sender:        sender,
		connector:     connector,
		pingSender:    pingSender,
		statusSetter:  statusSetter,
		notifier:      n,
		notifications: []string{},
		quitting:      false,
		err:           nil,
		output: []string{
			"\n===== P2P节点控制台 =====",
			"send <节点名称/IP地址> <消息内容> - 发送文本消息",
			"list                     - 查看在线节点",
			"exit                     - 退出节点",
			"==========================",
		},
	}
}

func (m model) Init() tea.Cmd {
	return textinput.Blink
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyCtrlC:
			m.quitting = true
			return m, tea.Quit
		case tea.KeyEnter:
			input := m.textInput.Value()
			m.textInput.SetValue("")
			return m.handleCommand(input)
		}
	case notificationMsg:
		m.handleNotification(msg.notification)
		return m, nil
	}

	var cmd tea.Cmd
	m.textInput, cmd = m.textInput.Update(msg)
	return m, cmd
}

func (m model) View() string {
	if m.quitting {
		return ""
	}

	var b strings.Builder

	for _, notif := range m.notifications {
		b.WriteString(notif + "\n")
	}

	for _, line := range m.output {
		b.WriteString(line + "\n")
	}

	if m.err != nil {
		b.WriteString(fmt.Sprintf("[ERROR] %v\n", m.err))
	}

	b.WriteString(m.textInput.View())

	return b.String()
}

func (m model) handleCommand(input string) (tea.Model, tea.Cmd) {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return m, nil
	}

	switch parts[0] {
	case "exit":
		m.quitting = true
		return m, tea.Quit
	case "list":
		return m.handleListCommand()
	case "send":
		return m.handleSendCommand(parts)
	case "broadcast":
		return m.handleBroadcastCommand(parts)
	case "connect":
		return m.handleConnectCommand(parts)
	case "disconnect":
		return m.handleDisconnectCommand(parts)
	case "ping":
		return m.handlePingCommand(parts)
	case "status":
		return m.handleStatusCommand(parts)
	case "help":
		return m.handleHelpCommand()
	default:
		m.output = append(m.output, fmt.Sprintf("[ERROR] 未知命令: %s", parts[0]))
		return m, nil
	}
}

func (m model) handleListCommand() (tea.Model, tea.Cmd) {
	peers := m.info.GetOnlinePeers()
	if len(peers) == 0 {
		m.output = append(m.output, "暂无在线节点")
		return m, nil
	}

	m.output = append(m.output, "\n===== 在线节点列表 =====")
	for _, peer := range peers {
		name := peer["name"]
		addr := peer["addr"]
		lastActive := peer["last_active"]
		onlineFor := peer["online_for"]
		stream := peer["stream"]

		m.output = append(m.output, fmt.Sprintf("名称: %s", name))
		if addr != "" {
			m.output = append(m.output, fmt.Sprintf("地址: %s", addr))
		}
		if lastActive != "" {
			m.output = append(m.output, fmt.Sprintf("最后心跳: %s", lastActive))
		}
		if onlineFor != "" {
			m.output = append(m.output, fmt.Sprintf("在线时长: %s", onlineFor))
		}
		if stream != "" {
			m.output = append(m.output, fmt.Sprintf("连接状态: %s", stream))
		}
		m.output = append(m.output, "------------------------")
	}
	m.output = append(m.output, "========================")
	return m, nil
}

func (m model) handleSendCommand(parts []string) (tea.Model, tea.Cmd) {
	if len(parts) < 3 {
		m.output = append(m.output, "用法：send <节点名称/IP地址> <消息内容>")
		return m, nil
	}

	target := parts[1]
	content := strings.Join(parts[2:], " ")

	targetAddr := target
	if addr, err := m.info.GetAddrByName(target); err == nil && addr != "" {
		targetAddr = addr
	} else if !strings.Contains(target, ":") {
		m.output = append(m.output, fmt.Sprintf("[ERROR] 无法解析目标节点: %s", target))
		return m, nil
	}

	if err := m.sender.SendTextMessage(targetAddr, content); err != nil {
		m.output = append(m.output, fmt.Sprintf("[ERROR] 发送失败: %v", err))
	} else {
		m.output = append(m.output, fmt.Sprintf("[SUCCESS] 消息已发送到 %s: %s", targetAddr, content))
	}
	return m, nil
}

func (m model) handleBroadcastCommand(parts []string) (tea.Model, tea.Cmd) {
	if len(parts) < 2 {
		m.output = append(m.output, "用法：broadcast <消息内容>")
		return m, nil
	}

	content := strings.Join(parts[1:], " ")
	success, failed := m.sender.BroadcastMessage(content)
	m.output = append(m.output, fmt.Sprintf("广播完成: 成功 %d, 失败 %d", success, failed))
	return m, nil
}

func (m model) handleConnectCommand(parts []string) (tea.Model, tea.Cmd) {
	if len(parts) < 2 {
		m.output = append(m.output, "用法：connect <IP:Port>")
		return m, nil
	}

	addr := parts[1]
	if !strings.Contains(addr, ":") {
		m.output = append(m.output, "[ERROR] 地址格式错误，请使用 IP:Port 格式")
		return m, nil
	}

	if err := m.connector.ConnectToPeer(addr); err != nil {
		m.output = append(m.output, fmt.Sprintf("[ERROR] 连接失败: %v", err))
	} else {
		m.output = append(m.output, fmt.Sprintf("[SUCCESS] 已连接到 %s", addr))
	}
	return m, nil
}

func (m model) handleDisconnectCommand(parts []string) (tea.Model, tea.Cmd) {
	if len(parts) < 2 {
		m.output = append(m.output, "用法：disconnect <节点名称>")
		return m, nil
	}

	name := parts[1]
	addr, err := m.connector.DisconnectPeer(name)
	if err != nil {
		m.output = append(m.output, fmt.Sprintf("[ERROR] 断开失败: %v", err))
	} else {
		m.output = append(m.output, fmt.Sprintf("[SUCCESS] 已断开节点 %s (%s)", name, addr))
	}
	return m, nil
}

func (m model) handlePingCommand(parts []string) (tea.Model, tea.Cmd) {
	if len(parts) < 2 {
		m.output = append(m.output, "用法：ping <节点名称/IP地址>")
		return m, nil
	}

	target := parts[1]
	targetAddr := target
	if addr, err := m.info.GetAddrByName(target); err == nil && addr != "" {
		targetAddr = addr
	}

	if m.pingSender == nil {
		m.output = append(m.output, "[ERROR] ping 功能不可用")
		return m, nil
	}

	latency, err := m.pingSender.SendPing(targetAddr)
	if err != nil {
		m.output = append(m.output, fmt.Sprintf("[ERROR] ping 失败: %v", err))
	} else {
		m.output = append(m.output, fmt.Sprintf("[SUCCESS] %s 延迟: %s", target, formatLatency(latency)))
	}
	return m, nil
}

func (m model) handleStatusCommand(parts []string) (tea.Model, tea.Cmd) {
	if m.statusSetter == nil {
		m.output = append(m.output, "[ERROR] 状态功能不可用")
		return m, nil
	}

	if len(parts) == 1 {
		status := m.statusSetter.GetNodeStatus()
		m.output = append(m.output, fmt.Sprintf("当前状态: %s", status))
		return m, nil
	}

	if len(parts) == 2 {
		status := parts[1]
		if err := m.statusSetter.SetNodeStatus(status); err != nil {
			m.output = append(m.output, fmt.Sprintf("[ERROR] 设置状态失败: %v", err))
		} else {
			m.output = append(m.output, fmt.Sprintf("[SUCCESS] 状态已设置为 %s", status))
		}
		return m, nil
	}

	m.output = append(m.output, "用法：status 或 status <online|busy>")
	return m, nil
}

func (m model) handleHelpCommand() (tea.Model, tea.Cmd) {
	helpText := []string{
		"\n===== 帮助信息 =====",
		"send <节点名称/IP地址> <消息内容> - 发送文本消息",
		"broadcast <消息内容>              - 广播消息",
		"list                              - 查看在线节点",
		"connect <IP:Port>                 - 连接节点",
		"disconnect <节点名称>              - 断开节点",
		"ping <节点名称/IP地址>             - 测试延迟",
		"status [online|busy]              - 查看或设置状态",
		"exit                              - 退出",
		"========================",
	}
	m.output = append(m.output, helpText...)
	return m, nil
}

func (m *model) handleNotification(n notifier.Notification) {
	switch n.Type {
	case "message_received":
		var payload map[string]string
		if err := json.Unmarshal(n.Payload, &payload); err != nil {
			return
		}
		from := payload["from"]
		content := payload["content"]
		m.notifications = append(m.notifications, fmt.Sprintf("[消息] %s: %s", from, content))
	case "peer_online":
		var payload map[string]string
		if err := json.Unmarshal(n.Payload, &payload); err != nil {
			return
		}
		name := payload["name"]
		addr := payload["addr"]
		m.notifications = append(m.notifications, fmt.Sprintf("[上线] %s (%s)", name, addr))
	case "peer_offline":
		var payload map[string]string
		if err := json.Unmarshal(n.Payload, &payload); err != nil {
			return
		}
		name := payload["name"]
		reason := payload["reason"]
		m.notifications = append(m.notifications, fmt.Sprintf("[下线] %s (%s)", name, reason))
	}
}

func formatLatency(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%dµs", d.Microseconds())
	}
	return fmt.Sprintf("%dms", d.Milliseconds())
}

func StartInteractiveConsole(info PeerInfoProvider, sender MessageSender, connector PeerConnector, pingSender PingSender, statusSetter StatusSetter, n *notifier.Notifier) bool {
	m := newModel(info, sender, connector, pingSender, statusSetter, n)

	p := tea.NewProgram(m)

	ch := make(chan notifier.Notification, 100)
	token := n.Subscribe(func(notif notifier.Notification) {
		select {
		case ch <- notif:
		default:
		}
	})

	go func() {
		for notif := range ch {
			p.Send(notificationMsg{notification: notif})
		}
	}()

	_, err := p.Run()
	close(ch)
	n.Unsubscribe(token)
	return err == nil
}
