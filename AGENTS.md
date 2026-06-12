# AGENTS.md — p2ptest

> **项目质量评级**: 架构重构完成，使用 DDD 风格
> 
> 状态：2026-06-13 完成从 God Struct 到 DDD 限界上下文的迁移

## 架构演变

### 重构前（2026-06-12）

```
internal/peer/          # 上帝对象：Node 管理一切
  node.go               # gRPC 服务 + 注册表 + 连接池 + 心跳 + 广播
  node_rpc.go           # Join/Leave/Heartbeat/Stream
  node_heartbeat.go     # 心跳逻辑
  node_broadcast.go     # 广播逻辑
  registry.go           # peer 注册表
```

问题：
- God Struct：Node 同时管理 gRPC 服务端、peer 注册表、连接池、消息流、心跳、清理
- 包边界混乱：client 包直接调用 peer.Node 内部方法
- 过程式 proto：Join/NotifyNodeJoin/Leave 语义模糊

### 重构后（2026-06-13）

```
internal/
  discovery/            # 限界上下文：节点发现
    domain/
      registry.go       # PeerRegistry 聚合根
    application/
      service.go        # Discovery gRPC 服务
  membership/           # 限界上下文：成员管理
    application/
      service.go        # Membership gRPC 服务
  messaging/            # 限界上下文：消息传递
    application/
      service.go        # Messaging gRPC 服务
  transport/
    conn_pool.go        # 连接池（共享基础设施）
  node/                 # 聚合根
    node.go             # 协调三个上下文
    node_test.go        # 6 个测试
```

## 关键设计决策

### Proto 语义改进

| 旧设计（过程式） | 新设计（DDD） | 理由 |
|---|---|---|
| `Join` | `Handshake` | 双向确认，一次调用完成发现+成员建立 |
| `NotifyNodeJoin` | `HandshakeResp.known_peers` | 消除冗余 RPC |
| `Leave` | `Disconnect` | "我要断开"语义清晰 |
| `SendHeartbeat` | `Heartbeat` | 请求/响应模式 |
| `PeerMessageStream` | `Stream` | 统一信封 Envelope |
| 单一 `P2PPeerService` | `Discovery`+`Membership`+`Messaging` | 三个限界上下文 |

### 设计原则

1. **名词先行**：先找领域中的概念（Node, Membership, Discovery），再填动词
2. **主语清晰**：每个函数必须有明确的主语，`membership.Handshake` 而非 `Join`
3. **边界划分**：同一概念在不同场景下含义不同 → 分成不同上下文
4. **消除歧义**：好的命名不需要额外解释

## DDD 学习资源

详见 `docs/DDD_GUIDE.md`：
- 命名审查法
- 名词提取法
- 语义聚焦法
- 边界划分法
- 重构前后对比

## 快速开始

```bash
# 编译 + 启动种子节点（端口50051）
make run-seed

# 启动 node2（连接种子节点，端口50052）
make run-node2

# 启动 node3（端口50053）
make run-node3

# 仅编译
make build

# 重新生成 protobuf
cd proto && bash gen.sh
```

## 项目结构

```
p2ptest/
├── cmd/p2pnode/          # 入口点
│   └── main.go           # 仅调用 root.Execute()
│   └── root/root.go      # Cobra CLI + pprof 调试服务器
├── internal/
│   ├── discovery/        # 限界上下文：节点发现
│   │   ├── domain/       # 聚合根、值对象
│   │   └── application/  # gRPC 服务适配
│   ├── membership/       # 限界上下文：成员管理
│   │   └── application/  # gRPC 服务适配
│   ├── messaging/        # 限界上下文：消息传递
│   │   └── application/  # gRPC 服务适配
│   ├── transport/        # 共享基础设施
│   │   └── conn_pool.go  # gRPC 连接池
│   ├── node/             # 聚合根
│   │   ├── node.go       # 协调三个上下文
│   │   └── node_test.go  # 测试
│   ├── client/           # 客户端逻辑
│   ├── console/          # 交互式控制台
│   ├── grpcutil/         # gRPC 连接工具
│   ├── logger/           # Zap 日志初始化
│   ├── types/            # 配置常量和错误定义
│   └── helper/           # SplitInput
├── proto/
│   ├── p2p.proto         # DDD 风格 proto
│   └── p2p/              # 生成的 Go 代码
├── docs/
│   └── DDD_GUIDE.md      # DDD 设计指南
├── bin/                  # 编译输出目录
└── Makefile              # 核心开发命令
```

## 开发流程

```bash
# 1. 修改代码后先编译
go build ./cmd/p2pnode

# 2. 运行测试
make test

# 3. 手动测试（需要开多个终端）
# 终端1: make run-seed
# 终端2: make run-node2
# 终端3: make run-node3

# 4. 控制台命令（在运行中的节点里）
> list                          # 查看在线节点
> send <节点名/IP:Port> <消息>   # 发送文本消息
> exit                          # 退出

# 5. pprof 调试（启动时加 -d 或 --debug）
# http://127.0.0.1:6060/debug/pprof/
```

## Makefile 命令速查

| 命令 | 作用 |
|------|------|
| `make all` | proto + build |
| `make proto` | 编译 protobuf 生成 .pb.go |
| `make build` | 编译到 bin/p2pnode |
| `make run-seed` | 启动种子节点（-n seed -p 50051 -d） |
| `make run-node2` | 启动 node2 连种子 |
| `make run-node3` | 启动 node3 连种子 |
| `make test` | 运行单元测试 |
| `make test-race` | 竞态检测 |
| `make clean` | 清理编译产物和生成文件 |

## 配置和 CLI 参数

```bash
./bin/p2pnode \
  -n, --name <名称>       # 节点名称（默认: node）
  -i, --ip <IP>           # 监听IP（默认: 127.0.0.1）
  -p, --port <端口>       # 监听端口（默认: 50051）
      --peer-ip <IP>      # 要连接的目标节点IP
      --peer-port <端口>  # 要连接的目标节点端口
  -d, --debug             # 开启debug日志 + pprof
```

## 依赖

- Go 1.25.5
- protoc (protobuf 编译器)
- 主要库: grpc, zap, cobra, uuid

## 已知限制

- **无配置文件**: 所有配置通过 CLI flag，不支持文件/env
- **无持久化**: 节点重启后 peer 列表丢失
- **仅局域网**: NAT 穿透相关代码存在但无实际实现
- **无加密**: gRPC 使用 insecure 传输
- **无服务发现**: 依赖手动指定 --peer-ip/--peer-port

## 重构经验教训（2026-06-13）

### 错误：知识流失（Knowledge Loss）

**问题**：从旧架构（Join）迁移到新架构（Handshake）时，遗漏了旧代码的隐式行为。

旧 `JoinResp.Peers` 包含 seed 节点自己：
```protobuf
message JoinResp {
  repeated NodeInfo peers = 1;  // 包含 seed 自己 + 所有已知 peers
}
```

新 `HandshakeResp` 将两者分离：
```protobuf
message HandshakeResp {
  NodeInfo peer = 1;              // 这是 seed 自己
  repeated NodeInfo known_peers = 2;  // 不包含 seed 自己
}
```

重构时只返回了 `known_peers`，导致 seed 节点从未被建立 stream 连接，消息发送失败。

**根本原因**：
1. 没有识别旧代码中的隐式行为（peers 列表包含 seed）
2. 没有端到端测试验证消息发送功能
3. 错误假设新架构设计更清晰 = 实现更正确

### 预防措施

**1. 重构检查清单**

任何架构重构前必须完成：
- [ ] 列出旧代码的所有隐式行为和副作用
- [ ] 编写一个端到端测试，验证核心用户场景（如：两个节点互发消息）
- [ ] 确保新实现通过该测试后再删除旧代码
- [ ] 检查旧 bug 修复是否被保留（搜索 `git log --all --grep="fix"`）

**2. 隐式行为识别法**

读旧代码时，问自己：
- "这个返回值除了明显的内容，还包含什么？"
- "调用这个函数后，除了返回值，还修改了什么状态？"
- "如果我是第一次看这个接口，我会漏掉什么假设？"

**3. 端到端测试优先**

不要只写单元测试。在重构前写这个测试：
```go
func TestTwoNodesCanExchangeMessages(t *testing.T) {
    seed := startNode("seed", 50051)
    defer seed.Stop()
    
    node2 := startNode("node2", 50052, "127.0.0.1:50051")
    defer node2.Stop()
    
    // 验证 node2 能发消息给 seed
    err := node2.SendMessageTo("seed", "hello")
    if err != nil {
        t.Fatal(err)
    }
}
```

**4. 旧代码考古**

重构前查看 `git log --all --grep="fix"`，确认每个 bug 修复在新架构中是否有对应实现。
