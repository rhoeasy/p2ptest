# CONTEXT.md — p2ptest

## 领域术语

### 通知 (Notification)

用户需要主动关注的系统事件。与日志（Log）的区别：日志是内部调试信息，用户不需要主动关注；通知是用户需要做出反应的事件。

通知类型：
- **message_received** — 收到来自其他节点的文本消息
- **peer_online** — 新节点加入网络
- **peer_offline** — 节点离开网络
- **peer_discovered** — 发现新节点，需要建立连接。由两种场景触发：(1) MembershipService.Handshake 收到新节点时发出 (2) MembershipService.NotifyNodeJoin 收到广播时发出。收到此通知后，Node 会向已连接 peer 广播 NotifyNodeJoin RPC，root.go 里的订阅会调 ConnectToPeers 建立消息流。

通知由 `Notifier` 发出，通过回调注册制推送到订阅者（CLI、Web）。

### 操作反馈 (Feedback)

用户主动执行命令后的直接结果。不属于通知，由 Console/UI 直接返回给用户。

例：消息发送成功 `[SUCCESS]`、发送失败 `[ERROR]`、用法提示。

### 日志 (Log)

内部调试和状态信息。用户无需主动关注。只走 zap 日志系统，不推送到前端。

例：启动完成、stream 建立/中断、心跳、gRPC 内部状态。

### 通知器 (Notifier)

独立的横切关注点组件（`internal/notifier/`），负责收集通知并分发给订阅者。不属于任何单一限界上下文。

职责：
- 维护回调列表（CLI、Web 各注册一个）
- 内存缓冲最近 N 条通知（供新连接的 Web 客户端回溯）
- 发出通知时遍历所有回调

### Web 管理面板 (Web Dashboard)

节点的浏览器端交互界面。与 CLI 是同一节点的两种前端。

通信方式：
- 命令操作 → HTTP API（REST）
- 通知推送 → WebSocket

与 CLI 同时使用，互不冲突。

### CLI 终端 (CLI Console)

节点的终端交互界面，基于 bubbletea TUI 框架。收到通知时重绘 prompt，避免打断用户输入。

## 功能缺口

按优先级排列，当前已识别但未实现的功能。

### P0 — 核心可用性

- ~~**心跳调度**~~：✅ 已恢复。`Node.startHeartbeatLoop()` 定时发心跳，`Node.startPeerCleaner()` 清理超时节点。
- ~~**消息回显**~~：✅ 已恢复。`recvPeerMessageLoop` 和 `messaging/service.go` 均通过 Notifier 推送 `message_received`，CLI/Web 均可收到。`from` 字段显示节点名称而非 IP:port。
- ~~**断线感知**~~：✅ 已恢复。stream 断开时 `recvPeerMessageLoop` 触发 `peer_offline` 通知、注销 peer、清理连接池。主动断开通过 `disconnect` 命令实现。
- ~~**尸代码清理**~~：✅ 已恢复。`FileChunk` 已从 proto 删除，.pb.go 已重新生成。
- ~~**节点发现广播**~~：✅ 已恢复。`NotifyNodeJoin` RPC + `Node.broadcastNodeJoin()` 实现新节点加入时通知已有 peer。

### P1 — 体验标配

- ~~**广播**~~：✅ 已恢复。CLI `broadcast <消息>` 向所有在线 peer 发送；Web `POST /api/broadcast` 同步支持。
- ~~**消息历史**~~：✅ 已实现。`GET /api/messages` 从 `notifier.History()` 过滤 `type=message_received` 返回专用消息历史。
- ~~**节点详情**~~：✅ 已恢复。`list` 显示名称、地址、最后心跳时间、在线时长、连接状态。
- ~~**CLI 命令补全**~~：✅ 已恢复。`help`（详细帮助）、`connect <ip:port>`（手动连接）、`disconnect <name>`（主动断开）。
- ~~**Web API 补全**~~：✅ 已恢复。`POST /api/connect`、`POST /api/disconnect`、`POST /api/broadcast`、`GET /api/messages`、`POST /api/ping` 均已实现。

### P2 — 架构已就位

- ~~**Gossip 发现**~~：✅ 已实现。`Node.startGossipLoop()` 每 30s 调用 `client.GossipWithPeer()` 扩散节点列表，发现新节点时发出 `peer_discovered` 通知。
- ~~**FindNode 调用**~~：✅ 已实现（descoped）。`client.FindNode()` 按 ID 查找节点，`Discovery.FindNode` gRPC 服务端已实现。当前无业务调用方（Oracle 标记为 dead code），保留作为未来 DHT 路由基础设施。
- ~~**NodeStatus 使用**~~：✅ 已实现。`UpdateStatus(uuid, status)` 加入 `PeerRegistry`；心跳用 `statusGetter()` 读取（线程安全，`statusMu` 保护）；断开/清理时设置 OFFLINE；CLI `status` 命令 + Web `GET/POST /api/status` 可查看/设置状态（online/busy）。
- ~~**Ping 实现**~~：✅ 已实现。`Envelope.Pong` + nonce 匹配实现 RTT 测量；CLI `ping` 命令 + Web `POST /api/ping` 端点；`client.SendPing()` 发送并等待 Pong 计算 RTT。

## 限界上下文

详见 AGENTS.md。

- **Discovery** — 节点发现
- **Membership** — 成员管理
- **Messaging** — 消息传递

通知是横切关注点，不属于任何单一限界上下文。
