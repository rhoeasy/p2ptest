# ADR 0001: 三层输出模型 + 通知器

## 状态

已接受 (2026-06-13)

## 上下文

p2ptest 节点在终端运行时，用户交互（命令反馈）和系统日志混杂输出。收到消息后终端没有显示，只写了 zap 日志。未来需要支持 Web 管理面板。

核心矛盾：业务逻辑用 zap 日志（stderr），用户交互用 fmt.Print（stdout），但终端里两者混在一起不可读。同时，部分用户需要关注的事件（收到消息、节点上下线）埋在日志里，用户看不到。

## 决策

将节点输出分为三层，各走各的通道：

### 1. 操作反馈（Feedback）

用户执行命令的直接结果。由 Console/UI 直接返回，不经过 Notifier。

例：`[SUCCESS] 消息已发送`、`[ERROR] 发送失败`

### 2. 通知（Notification）

用户需要主动关注的系统事件。由 `internal/notifier/` 独立包管理，通过回调注册制推送到 CLI 和 Web。

通知类型：`message_received`、`peer_online`、`peer_offline`（snake_case 命名）。

数据结构：
```go
type Notification struct {
    Type    string          // snake_case
    Time    time.Time
    Payload json.RawMessage // 按 Type 解析
}
```

Notifier 维护内存缓冲（最近 N 条），供新连接的 Web 客户端回溯历史。

### 3. 日志（Log）

内部调试信息，只走 zap。不推送到前端。

### CLI 实现

CLI 使用 bubbletea TUI 框架，收到通知时清除当前输入行 → 打印通知 → 重绘 prompt + 已输入内容，避免打断用户输入。

### Web 实现

- 命令操作 → HTTP REST API
- 通知推送 → WebSocket

### 进程组织

单进程，`--web :8080` 开关启用 Web 服务。CLI 和 Web 可同时使用。

## 考虑过的替代方案

### 通知传递机制

| 方案 | 优点 | 放弃原因 |
|---|---|---|
| Go channel 广播 | 惯用模式 | 需要管理 channel 复制和慢消费者，只有两个订阅者时过度设计 |
| 事件总线（topic 路由） | 最通用 | P2P 学习项目不需要 topic 路由，YAGNI |
| **回调注册制** ✅ | 简单，与现有 `onNodeStarted` 模式一致 | — |

### CLI prompt 不被打断

| 方案 | 优点 | 放弃原因 |
|---|---|---|
| 不管它 | 最简单 | 输入被拦腰截断，体验差 |
| 通知排队 | 不打断 | 延迟显示，用户错过实时消息 |
| **bubbletea 重绘** ✅ | 体验最好 | 实现复杂度可接受，Go 生态最成熟的 TUI |

### Web 通信

| 方案 | 优点 | 放弃原因 |
|---|---|---|
| 纯 WebSocket | 简单 | 所有操作要自设计协议，无法 curl 调试 |
| gRPC-Web | 复用 proto | 浏览器 streaming 支持有限，需要 Envoy 代理 |
| **HTTP API + WebSocket** ✅ | REST 可调试可文档，WS 只推通知 | — |

### 进程组织

| 方案 | 优点 | 放弃原因 |
|---|---|---|
| 独立 Web 网关 | 解耦 | 部署复杂度翻倍，P2P 学习项目不需要 |

## 后果

- `internal/console/` 需要用 bubbletea 重写
- 需要新建 `internal/notifier/` 包
- messaging、membership service 需要注入 Notifier 并在关键事件处调用 `Emit()`
- 需要新建 HTTP + WebSocket 服务层
- Node 不再参与通知流转，职责更清晰
