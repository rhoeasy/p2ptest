# DDD 设计教程 — 以 p2ptest 五阶段演进为例

> 这份教程与你已有的 `docs/DDD_GUIDE.md` 是互补关系。
> DDD_GUIDE 讲的是**微观技巧**：命名审查法、名词提取法、语义聚焦法（"怎么命名"）。
> 本教程讲的是**宏观心智**：为什么这样设计、怎么一步步想、怎么用 TDD 守护它、以及这个项目真实走过的弯路。
>
> 阅读顺序建议：先读本文建立全局观 → 遇到具体命名问题回查 DDD_GUIDE → 想看某个决策的来龙去脉查 `docs/adr/`。

---

## 0. 这份教程要回答的核心问题

不是"DDD 是什么"（到处都有定义），而是：

> **下次我拿到一个新系统，我该怎么想，才能设计成 p2ptest 现在这个样子？**

所以这份教程是"想法的训练"，不是"概念词典"。每一章都会回到一句话：
**先有名词，再分边界，动词最后。**

---

## 1. 起点：为什么需要 DDD（God Struct 之痛）

要学会"好的设计"，先要看清"坏的设计"痛在哪。本项目第一版由 doubaoseed2.0 完成，commit `5a418ba init: first commit`。看当时的结构：

```
internal/peer/
  node.go            # ← 一个 struct 管所有事
  node_rpc.go        # gRPC 服务端实现
  node_heartbeat.go  # 心跳逻辑
  node_broadcast.go  # 广播逻辑
  node_conn.go       # 连接管理
  node_peermap.go    # peer 注册表
  helper.go
```

`peer.Node` 是一个**上帝对象（God Struct）**：它同时是 gRPC 服务端、peer 注册表、连接池持有者、心跳调度器、消息广播器。所有逻辑都挂在 `Node` 的方法上。

这种结构在"能跑"阶段没问题。但它有三个结构性缺陷，任何一个都会在演化时咬你：

**痛点 A：改一处，牵全身。** 想给心跳加个超时参数，你打开 `node_heartbeat.go`，但它读写的状态散落在 `node_peermap.go` 和 `node_conn.go`。你看不完整个包不敢动。

**痛点 B：包边界形同虚设。** `client` 包直接调用 `peer.Node` 的内部方法（见后来的 commit `199622e refactor(client): decouple from *node.Node using interfaces` 就是为修这个）。"包"只是文件夹，不是真正的边界。

**痛点 C：过程式 proto，主语缺失。** 看旧 proto：

```protobuf
service P2PPeerService {
  rpc Join(JoinReq) returns (JoinResp);
  rpc NotifyNodeJoin(NodeInfo) returns (Empty);
  rpc Leave(NodeID) returns (Empty);
  rpc SendHeartbeat(HeartbeatReq) returns (Empty);
}
```

`Join` — 谁 join 谁？`Leave` — 是我离开还是让你离开？动词是主角，名词（主体）不见了。这是过程式思维的典型症状：**按"操作流程"组织代码，而不是按"领域概念"组织**。

> **学会的第一个想法**：当你发现一个文件叫 `xxx_manager.go` / `xxx_helper.go` / `node.go(什么都有)`，先停下来问——这里到底有几个不同的**名词**混在一起了？

---

## 2. DDD 的核心心智模型（三个问题）

抛开所有术语，DDD 落到日常设计就三个问题，按顺序问：

1. **领域里有哪些名词？**（→ 聚合根 / 实体 / 值对象）
2. **哪些名词在不同场景下含义不同，该拆开？**（→ 限界上下文）
3. **什么东西不属于任何单个名词？**（→ 横切关注点）

问完这三个，**最后**才填动词（方法名）。

这就是 p2ptest 重构时 glm5.1 做的事（commit `e7994bd refactor(proto)` 起步）。下面三章分别展开这三问。

---

## 3. 第一问：名词提取 → 聚合根

### 怎么做

拿到需求，**先不要想功能，先列名词**。p2ptest 的领域名词浮现过程（对照 DDD_GUIDE 的"名词提取法"）：

```
网络里有：
  - 节点（Node）        有身份、有地址、有状态
  - 对等关系（Membership） 节点之间的关系
  - 发现（Discovery）   网络拓扑信息
  - 消息（Message）     通信内容
```

这些名词就是候选的**聚合根（Aggregate Root）**。

### 什么是聚合根（关键误区澄清）

很多人误以为"聚合根 = 数据库表映射的实体"。**错。** 聚合根的本质是**一致性边界**：它是一组必须一起变更的对象的"唯一入口"，外部只能通过它修改这组对象。

本项目最好的例子是 `internal/discovery/domain/registry.go` 的 `PeerRegistry`：

```go
// registry.go:30-37
type peerRegistry struct {
    onlinePeers  map[string]*pb.NodeInfo
    lastActive   map[string]time.Time
    registeredAt map[string]time.Time
    nameToAddrs  map[string][]string
    mu           sync.RWMutex
    selfUUID     string
}
```

它内部有 4 个 map + 1 把锁。这 4 个 map 必须一起变更（注册一个 peer 要同时写 onlinePeers/lastActive/registeredAt/nameToAddrs），否则状态不一致。所以它们被锁在 `peerRegistry` 后面，**外界只能通过 `Register/Unregister/Get/...` 这些方法碰它们**。这就是聚合根——把"必须一致"的东西圈起来，只留一个门。

对比：如果像旧代码那样 peer map 散落在 `node_peermap.go`，任何方法都能直接 `node.peers[uuid] = ...`，就没有一致性保证。

> **学会的第二个想法**：看到一组"必须一起改"的状态，第一反应是"把它们藏到一个对象后面，只留窄接口"。这就是聚合根本能。

### 为什么是 `PeerRegistry` 而不是 `DiscoveryService`

注意分层：`domain/PeerRegistry`（聚合根 + 业务规则）和 `application/DiscoveryService`（gRPC 适配层）是分开的。

```
discovery/
  domain/
    registry.go      # 聚合根：一致性边界、并发规则、防御性拷贝
  application/
    service.go       # gRPC service，薄薄一层，调 registry
```

判据很简单：**业务规则放 domain，技术适配放 application。** `GetStale`（判断哪些 peer 超时）是业务规则，归 registry；`GetPeers` RPC 怎么序列化响应是技术，归 service。这样哪天你把 gRPC 换成 HTTP，domain 一行不改。

---

## 4. 第二问：边界不同 → 限界上下文

这是 DDD 最有用、也最容易被忽视的一问。

### 判据：同一概念，含义不同，就拆

问自己：**这个名词在不同场景下，含义是否相同？** 不一样就拆成不同的**限界上下文（Bounded Context）**。

本项目最经典的例子是 `NodeInfo` 这同一个 proto 消息：

| 场景 | NodeInfo 的含义 | 归属 |
|------|----------------|------|
| 查询网络拓扑 | "我听说有这些节点" —— 路由线索 | Discovery |
| 建立对等关系 | "我要和你建立连接" —— 成员资格 | Membership |
| 收发消息 | "这条消息从哪来" —— 消息源标签 | Messaging |

同一个 `NodeInfo`，在三个语境里**语义不同**。这正是 DDD 说的"相同概念在不同上下文有不同模型"。于是 proto 被拆成三个 service（commit `e7994bd`）：

```protobuf
service Discovery  { rpc GetPeers(...); rpc FindNode(...); }
service Membership { rpc Handshake(...); rpc Heartbeat(...); rpc Disconnect(...); rpc NotifyNodeJoin(...); }
service Messaging  { rpc Stream(...); }
```

而旧设计是一个 `P2PPeerService` 把所有 RPC 塞一起——这恰恰是"没有边界意识"。

### 边界在代码里长什么样

限界上下文 = Go 的 package 边界。本项目用包来落地：

```
internal/
  discovery/    application import domain；domain 不 import 任何人
  membership/   同上
  messaging/    同上
```

**关键纪律**：上下文之间**不互相 import**。discovery 不知道 membership 存在。它们只通过 `node` 这个协调器（下一章）间接协作。谁违反这条（比如 membership 直接调 discovery 的方法），边界就破了。

> **学会的第三个想法**：每当你想把两个包的业务逻辑串起来，先问"它们是同一个名词吗？"。不是 → 别让它们互相 import，找个协调者。
> 第 12 章会看到，本项目重构时丢功能，根因之一就是边界画对了但协调没补上。

---

## 5. 第三问的延伸：协调者（Node）——不是上帝对象

既然三个上下文不互相 import，它们怎么协作？答案是 `internal/node/node.go` 的 `Node`。这里有个**极重要的区分**，理解了才算懂 DDD：

> **协调器（Coordinator）≠ 上帝对象（God Struct）。**

看 `Node` 实际做什么（commit `166220a` 起步，`b055641` 补全）：

```go
// node.go:30-55 —— 字段
type Node struct {
    cfg      *types.NodeConfig
    registry discoveryDomain.PeerRegistry   // ← 持有聚合根的接口，不是自己实现
    connPool *transport.ConnPool
    notifier *notifier.Notifier
    discoverySvc  *discoveryApp.DiscoveryService    // ← 持有三个 service
    membershipSvc *membershipApp.MembershipService
    messagingSvc  *messagingApp.MessagingService
    ...
}
```

`Node` 的方法分两类：

| 方法 | 性质 |
|------|------|
| `Start` / `Stop` | 装配三个 service 到 gRPC server，启停 |
| `startHeartbeatLoop` / `startPeerCleaner` / `startGossipLoop` | 跑 ticker，**编排**三个上下文 |
| `GetOnlinePeers` / `SendToStream` / ... | **转发**到 registry / connPool，本身不含业务规则 |

注意 `Node` 里**没有**业务规则。判断超时在 registry 的 `GetStale`；连接并发安全在 connPool 的 `lockedStream`；消息分派在 messaging 的 `handleEnvelope`。Node 只是把它们"接上线"、按时间节奏戳它们。

对比旧 God Struct：旧 `peer.Node` 自己写心跳逻辑、自己管 peer map、自己广播。**区别不在代码量，在于"规则住哪"**。规则住在各上下文里，Node 只是导演。

> **学会的第四个想法**：当你想往"顶层对象"里塞 if/else 业务逻辑时，先问"这条规则属于哪个名词？"。它多半该住到某个聚合根里，顶层对象只负责调用。

---

## 6. 横切关注点：Notifier 为什么不属于任何上下文

第三问是"什么东西不属于任何名词"。本项目给出一个教科书式的回答：`internal/notifier/`（commit `1a3552c`，决策记录见 ADR-0001）。

### 问题

`message_received`、`peer_online`、`peer_offline` 这些事件，**CLI 要订阅**（终端显示），**Web 也要订阅**（WebSocket 推浏览器）。这些事件由 membership/messaging 上下文产生，但消费者有两个、且会继续增加。

如果把这些事件塞进 membership 上下文，那 membership 就得知道 CLI 和 Web 的存在——边界立刻被污染。如果让 CLI 直接去 membership 里"拉"，又得每个前端各写一套分发。

### 解法：独立包 + fan-out

抽一个横切包 `notifier`，谁都不拥有它，谁都依赖它：

```go
// notifier.go —— 回调注册制，fan-out
func (n *Notifier) Subscribe(callback func(Notification)) SubscriptionToken
func (n *Notifier) Emit(notification Notification)
```

membership/messaging 只管 `Emit`，不关心谁订阅；CLI/Web 只管 `Subscribe`，不关心谁发。Notifier 是纯粹的**中间人**。

### 它做对的三个设计细节（都值得学）

1. **锁内拷贝回调、锁外调用**（notifier.go:46-67）：
   ```go
   func (n *Notifier) Emit(notification Notification) {
       n.mu.Lock()
       callbacks := make([]func(Notification), 0, len(n.callbacks))
       for _, cb := range n.callbacks { callbacks = append(callbacks, cb) }  // 拷贝
       // ...写 ring buffer...
       n.mu.Unlock()
       for _, callback := range callbacks { callback(notification) }  // 锁外调用
   }
   ```
   为什么？如果锁内调 callback，而 callback 里又调 Subscribe/Unsubscribe，就死锁；而且慢回调会阻塞所有 Emit。**拷贝快照 + 锁外调用**是观察者模式的标准正确写法，本项目一上来就做对了。

2. **ring buffer 历史**：新 Web 客户端连上时能回溯最近 N 条通知（`History()`）。解决了"晚连接的客户错过断网期间事件"的问题，且用定长环形数组零分配。

3. **明确不归任何上下文**：参见 CONTEXT.md "通知是横切关注点，不属于任何单一限界上下文"。这条**文档化**的声明本身就是设计动作——它防止后人手贱把 notifier 挪进 membership。

> **学会的第五个想法**：每当一个东西被两个以上上下文需要，且不属于其中任何一个，就独立成横切包。典型候选：日志、通知、鉴权、配置、指标。但每个都要写一句"为什么它不属于某上下文"，否则容易滥用。

---

## 7. 领域事件与事件总线：Notifier 的另一重身份

这一章是第 6 章的延续。第 6 章讲了 notifier 作为"横切关注点"的定位；但它还有一重身份没点破——**它就是一个简化版的事件总线，承载的是领域事件（Domain Event）。** 你读第 6 章时卡在"事件总线没弄明白"，根因就是这一步没接上。

### 7.1 什么是领域事件：描述"已经发生的事"

先区分两个容易混的概念：

| 概念 | 定义 | p2ptest 对应 |
|------|------|-------------|
| **命令 Command** | 描述"想要做某事"——可成功可失败 | HandshakeReq、DisconnectReq（RPC 请求） |
| **领域事件 Domain Event** | 描述"某事已经发生"——不可变，不可撤销 | message_received、peer_online、peer_offline、peer_discovered |

判据很直接：**命名是不是过去式。** 看本项目 notifier 的四个事件类型（notification.go 注释）：

```
message_received    ← "消息已被接收"
peer_online         ← "节点已上线"
peer_offline        ← "节点已下线"
peer_discovered     ← "节点已被发现"
```

全是过去式。这不是偶然，是领域事件的命名铁律。为什么？

- **命令**是对未来的请求，可能被拒（Handshake 接收方可以 `Accepted=false`）
- **事件**是对过去的记录，已板上钉钉。收到 peer_offline 就是"这个 peer 已经下线了"，不是"请你下线"

混了会出大问题：如果把你把事件命名成 `connect_peer`（命令式），订阅者就会以为是"请你连接这个 peer"，而它实际表达的是"这个 peer 已经被发现，你可以选择连或不连"——语义就歪了。本项目命名做对了，但如果你不知道这条规则，下次自己起名可能就写成 `new_peer` / `do_connect` 了。

> **学会的第九个想法**：凡是描述"系统里发生了什么"的通知，命名一律过去式。命令描述意图、事件描述事实。

### 7.2 事件总线的本质：观察者模式 + 不可变事件

第 6 章我们说 notifier 是"纯粹的中间人"。换个更精确的视角：**通知器 = 事件总线的内存版。** 对照三种相关模式：

| 模式 | 特征 | p2ptest 用的是 |
|------|------|---------------|
| **观察者模式** | 一个目标维护观察者列表，状态变化时通知 | notifier 的 Subscribe/Emit 就是 |
| **事件总线** | 观察者模式的增强：多对多、按类型路由、事件不可变 | notifier 是简化版（没按类型路由） |
| **消息队列** | 跨进程、持久化、ack/retry | notifier 不是（纯内存、fire-and-forget） |

notifier 介于两者之间：它继承了观察者模式的"目标自动通知观察者"，又向事件总线靠拢——**事件对象本身不可变**（`Notification.Type/Time/Payload` 在 Emit 后没人改，`Payload` 是 `json.RawMessage` 字节拷贝）。这正是生产级事件总线的核心约束：**事件一旦发出就不许改**，因为改了会让不同订阅者在不同时刻看到不同状态，是 bug 之源。

本项目靠"构造时 json.Marshal 成 bytes"天然实现了不可变——这个设计是正确的，但教程第 6 章没点破它为什么这么做。真正的理由是：**不可变事件是事件总线区别于普通回调链的本质。**

```go
// notification.go —— 事件构造即冻结
func NewMessageReceivedNotification(from string, content string) Notification {
    payload, _ := json.Marshal(map[string]string{"from": from, "content": content})
    return Notification{
        Type:    "message_received",   // 不可变
        Time:    time.Now(),            // 不可变
        Payload: payload,               // bytes，构造后没人能改
    }
}
```

### 7.3 事件驱动副作用链：notifier 最高级的用法

这是本章最重要的一节。光有"订阅-通知"只是观察者；**领域事件真正的威力在于：一个事件可以触发后续副作用，形成响应链，而生产方完全不知道下游是谁。**

本项目有一个教科书级例子——`peer_discovered` 事件引发的传播链。看 node.go:130-134：

```go
// node.go Start() 里
n.notifier.Subscribe(func(notif notifier.Notification) {
    if notif.Type == "peer_discovered" {
        n.broadcastNodeJoin(notif)   // ← 副作用：广播 NotifyNodeJoin RPC
    }
})
```

这里发生了什么？追踪完整链路：

```
场景A：Handshake 接收方处理新节点
  membership.Handshake()
    → notifier.Emit(peer_discovered)          [事件发布：发现了新节点]
        → 订阅回调触发 broadcastNodeJoin()
            → 遍历所有已连接 peer，并发发 NotifyNodeJoin RPC   [副作用链]
                → 远端 peer 的 membership.NotifyNodeJoin
                    → 再 Emit(peer_discovered)  [接力，见下方]

场景B：Gossip 轮询发现新节点
  node.performGossipRound()
    → 发现 registry 里没有的新 peer
    → notifier.Emit(peer_discovered)
        → 同上副作用链
```

关键洞察：**membership.Handshake 和 performGossipRound 都只负责"发现并 Emit 事件"，它们完全不知道 broadcastNodeJoin 的存在。** 把这个事件订阅删掉，Handshake 照样工作、注册照样成功——只是新发现的节点不会被二次传播给网络里其他 peer。

这就是松耦合。对比旧 God Struct 做法：`NotifyNodeJoin` 是一个 RPC 方法，调用方必须在 Handshake 逻辑里**主动**写"然后我要通知别人"——发现逻辑和传播逻辑粘在一起。新设计里，"发现"和"传播"通过事件解耦，成了可独立替换的两个环节。

### 7.4 事件链的风险：接力环路与无限递归

刚才那个链路里藏着一个必须警惕的问题。注意到 `NotifyNodeJoin` 处理端**也会 Emit `peer_discovered`**（membership/service.go:174 的 `s.notifier.Emit(notifier.NewPeerDiscoveredNotification(...))`）。如果那个节点的订阅回调又调 broadcastNodeJoin，理论上会无限接力。

本项目靠**幂等检查**断了环：

```go
// membership/service.go NotifyNodeJoin —— 先查重
if _, exists := s.registry.Get(req.NewNode.Id.Uuid); exists {
    s.registry.UpdateLastActive(req.NewNode.Id.Uuid)
    return &pb.NotifyNodeJoinResp{Acknowledged: true}, nil   // 已知，不再 Emit
}
```

收到一个已注册的节点 → 追加心跳但不 Emit peer_discovered。于是环在第 2 跳就断了。

> **学会的第十个想法**：事件驱动副作用链必须配**幂等性检查**或**去重机制**，否则环状网络必然死循环。本项目用 registry.Get 做"已知则不传播"，生产级系统常用：事件加去重 ID（trace_id/nonce）、订阅端带处理状态表、事件带版本号防乱序重放。

### 7.5 生产级事件总线还差什么（方向指引）

notifier 是教学版。对照工业级事件总线（NATS、Kafka、Go 的 EventBus 库），它有意省略了：

| 能力 | notifier | 工业级 | 本项目为什么能省 |
|------|----------|--------|-----------------|
| 按类型路由 | ❌ 全广播 | ✅ topic/queue | 只有本地两个订阅者，分发靠回调里 if |
| 持久化 | ❌ 纯内存 | ✅ 落盘/WAL | 节点重启本就接受状态丢失（ CONTEXT.md 已知限制） |
| ack/retry | ❌ fire-and-forget | ✅ 至少一次/精确一次 | 订阅者都是同进程，失败了重连即可 |
| 背压 | ❌ 256 缓冲满了 default 丢弃 | ✅ 生产者阻塞/降级 | localhost 学习项目，不会过载 |
| 有序保证 | ❌ 单 Emit 内串行 | ✅ 分区有序 | 事件量极低 |

但保留了一个**不该省**的能力——历史回溯（ring buffer + `History()`）。因为 Web 客户端晚连上时需要看"断连期间错过的事件"，这是状态恢复而非持久化。ADR-0001 记录了这个取舍。

### 7.6 一张大图：事件在系统里怎么流

把全项目的事件流画出来：

```
                          ┌─────────────────────────────┐
  gRPC 请求（命令） ──→   │ membership.Handshake        │
                          │   注册 peer                  │
                          │   Emit(peer_online)         │──┐
                          │   Emit(peer_discovered)     │──┼─→ notifier（事件总线）
                          └─────────────────────────────┘  │     │
                                                           │     │ fan-out
                          ┌─────────────────────────────┐  │     │
  gRPC 流消息   ──→       │ messaging.Stream            │  │     ▼
                          │   Emit(message_received)    │──┘  ┌────────────┐
                          └─────────────────────────────┘     │ 订阅者1：  │
                          ┌─────────────────────────────┐     │ CLI 控制台 │
  心跳超时      ──→       │ node.cleanupStalePeers      │     └────────────┘
                          │   Emit(peer_offline)        │────→┌────────────┐
                          └─────────────────────────────┘     │ 订阅者2：  │
                          ┌─────────────────────────────┐     │ Web WS    │
  Gossip 发现   ──→       │ node.performGossipRound      │     └────────────┘
                          │   Emit(peer_discovered)     │────→┌────────────────────┐
                          └─────────────────────────────┘     │ 订阅者3：node 自己  │
                                                              │  → broadcastNodeJoin │
                                                              └────────────────────┘
```

注意**node 既是生产者又是消费者**（订阅者3）——这是事件驱动架构的常见形态：聚合根之间通过事件协作，彼此不知道对方存在，只通过事件总线接火线。第 5 章说"上下文之间不互相 import"，靠什么协作？**答案终于完整了：靠事件总线**。协调器 Node 订阅事件并触发跨上下文动作，上下文本身只 Emit 事件不在乎谁听。

> **本章小结**：notifier 实质是一个内存事件总线，承载四类不可变领域事件（全过去式命名）。它最深的用法不是"通知前端"，而是"订阅者之一的 Node 自身"——通过响应 `peer_discovered` 触发 broadcastNodeJoin，把三上下文间接协作起来，同时用 registry 幂等性断环。光发通知只是观察者模式；用事件解耦副作用链才是事件驱动架构的起点。

---

## 8. 可测性设计：接口隔离 = 依赖倒置 = Deep Module

这一章把 DDD 的"边界"和 TDD 的"可测"接起来。本项目有一组非常漂亮的小接口（commit `199622e` 起步）。

### client 不再依赖 *node.Node

```go
// internal/client/interfaces.go —— PeerNode 接口
type PeerNode interface {
    NodeIdentity      // GetNodeID() / Cfg()
    HasStream(addr string) bool
    SendToStream(addr string, env *pb.Envelope) error
    SetPeerConn(addr string, conn *grpc.ClientConn)
    // ...
    Notifier() *notifier.Notifier
    HandlePongReceived(nonce string, pingTimestamp uint64)
}
```

旧代码 `client` 包直接 import 并调用 `*node.Node` 的方法。这有两个坏处：(1) client 想测试就得真起一个 Node；(2) client 和 node 双向耦合，谁也动不了。

抽成接口后，client 依赖**行为契约**而非具体类型。测试时塞个假的 PeerNode 即可（这正是本轮 TDD 修复能推进的基础）。

### web / console 的 Provider 接口族

`internal/web/server.go` 定义了五个窄接口：

```go
type PeerInfoProvider interface { GetOnlinePeers() []map[string]string; GetAddrByName(name string) (string, error) }
type MessageSender interface { SendTextMessage(targetAddr, content string) error; BroadcastMessage(content string) (int, int) }
type PeerConnector interface { ConnectToPeer(addr string) error; DisconnectPeer(name string) (string, error) }
type PingSender interface { SendPing(targetAddr string) (time.Duration, error) }
type StatusSetter interface { SetNodeStatus(status string) error; GetNodeStatus() string }
```

`Node` 同时实现这五个接口。Web 服务不 import node 包，只依赖这五个接口。于是 `internal/web/server_test.go` 用 mock 实现就能测全部 HTTP 端点，**完全不起 gRPC**。本轮修 `Server.Start` 的 bug 测试（`server_start_test.go`）能直接断言"端口冲突返回 error"，正因为有这套接口。

### 这就是 Deep Module

mattpocock TDD 技能里强调的"deep module"：**小接口 + 深实现**。`PeerInfoProvider` 两个方法，背后是整个 registry；`Notifier.Subscribe` 一个方法，背后是 fan-out + ring buffer。接口越小，测试越容易，实现越能藏复杂度。

反例是 shallow module：大接口 + 薄实现（纯转发）。本项目旧 `peer.Node` 暴露几十个方法就是 shallow 的反面教材。

> **学会的第六个想法**：好的边界天然可测，可测的边界天然是 deep module。当你发现"为了测它我得 mock 八个东西"，往往说明边界画错了，该抽接口了。

---

## 9. 防御性设计：不变性与 Clone 对称

这一章用一个**我们刚修的真实 bug** 讲设计语言。这个 bug 是本轮 commit `e430418` 修的。

### 不对称的防御

原 `registry` 读侧做了 `proto.Clone`：

```go
// registry.go Get/List（读侧，原本就有 Clone）
return proto.Clone(p).(*pb.NodeInfo), true
```

但写侧直接存外部指针：

```go
// registry.go Register（写侧，原 bug）
r.onlinePeers[uuid] = peer   // ← 存的是调用方的指针
```

测试暴露的问题（`registry_test.go`）：

```go
peer := newTestPeer("node2", "uuid-2")
r.Register(peer)
peer.Id.Name = "tampered"          // 调用方改自己的对象
got, _ := r.Get("uuid-2")
// got.Id.Name == "tampered"  ← 泄漏了！registry 内部状态被外部改动污染
```

### 设计语言：读写对称

这不止是"多写一个 Clone"，而是一种设计主张：**聚合根对内持有数据的主权，对外不泄漏可变别名。** 读侧 Clone 保证"给出去的副本不会被改回来影响我"，写侧 Clone 保证"收进来的不会被你改了影响我"。两侧必须对称，缺一就是洞。

非对称防御是常见隐患——人往往记得"不要把内部对象直接 return"（读侧），却忘了"也不要直接存外部传进来的"（写侧）。本项目这个 bug 潜伏了整个重构期，因为生产路径恰好传的是 gRPC 新解码的独立消息，没踩到；但只要哪天有人复用一个 `*pb.NodeInfo` 注册，就炸。

> **学会的第七个想法**：凡是用"集合存指针"的聚合根，问一句"读写两侧的防御对称吗？"。不对称就是债。

---

## 10. Deep Module 实战：把脆弱藏在深实现里

这一章讲两个"小接口藏大坑"的实例，都是本项目做对的（或我们刚补的）。

### 10.1 lockedStream：把 gRPC 并发约束藏起来

gRPC 规定**同一个 stream 不能并发 Send**。本项目用 `transport/conn_pool.go` 的 `lockedStream` 把这个约束封死（commit `a034dc4`）：

```go
// conn_pool.go:13-16
type lockedStream struct {
    mu     sync.Mutex
    stream pb.Messaging_StreamClient
}

// 对外只暴露 SendToStream：拿锁 → Send → 放锁
func (p *ConnPool) SendToStream(addr string, env *pb.Envelope) error {
    p.mu.RLock(); ls, ok := p.streams[addr]; p.mu.RUnlock()
    if !ok { return fmt.Errorf("no stream to %s", addr) }
    ls.mu.Lock(); defer ls.mu.Unlock()
    return ls.stream.Send(env)
}
```

调用方写 `connPool.SendToStream(addr, env)`，完全不知道有锁。如果哪天 broadcast（并发往所有 stream 发）忘了加锁，就是生产环境的随机 panic。**这道防线让"错误用法写不出来。**

### 10.2 parseHostPort：把 panic 危险藏成 ok 返回值

这是本轮 commit `632f6e5` 修的。原代码：

```go
// node.go broadcastNodeJoin（原 bug）
Addrs: []*pb.NodeAddr{{Ip: addr[:strings.LastIndex(addr, ":")], Port: uint32(mustParsePort(addr))}}
```

`strings.LastIndex` 在 addr 不含冒号时返回 -1，`addr[:-1]` 直接 panic。生产路径恰好都带冒号，没踩到——但这是**字面意义上的定时炸弹**，输入来自外部通知，迟早炸。

修复方式（TDD 先写测试）是抽成 deep module：

```go
// node.go —— 暴露 ok 风格三返回值，内部用 net.SplitHostPort
func parseHostPort(addr string) (host string, port uint32, ok bool) {
    h, pStr, err := net.SplitHostPort(addr)
    if err != nil { return "", 0, false }
    p, err := strconv.Atoi(pStr)
    if err != nil || p < 0 { return "", 0, false }
    return h, uint32(p), true
}
```

调用点变成 `host, port, ok := parseHostPort(addr); if !ok { return }`。**非法输入不再能触发 panic，行为从"可能崩"变成"可预测的跳过"。**

> **学会的第八个想法**：看到 `xxx[:strings.LastIndex(x, sep)]` 或任何"边界靠约定保证"的切片，第一反应是抽成一个返回 (值, ok) 的函数，用标准库的解析器，让非法输入走正常错误路径而非 panic。脆弱性属于实现层，不该暴露给调用方。

---

## 11. TDD 与 DDD 的配合（mattpocock 方法论）

这一章讲"为什么 DDD 之后 TDD 变得顺手"，并用本轮 5 个修复做复盘。

### 11.1 TDD 不是"先写所有测试"

最容易误解的一点：以为 RED 阶段 = 把所有测试写完，GREEN = 把所有实现写完。mattpocock 称之为**横向切片（horizontal slicing）**，是反模式：

- 一次性写一堆测试，测的是**想象的**行为，不是**真实**行为
- 你会去测数据结构的形状而不是用户能感知的行为
- 测试对真实变化不敏感：行为坏了它还过，行为没坏它反而挂

正确做法是**垂直切片（tracer bullet）**：一个测试 → 一段实现 → 下一个测试 → 下一段实现。每个测试回应上一个循环里学到的东西。

### 11.2 行为测试，通过公共接口

TDD 测**行为**不测**实现**。判据很直接：如果你重命名一个内部函数测试就挂，但行为没变——那测试测的是实现细节，是坏测试。

这和 DDD 完美契合：限界上下文 = 清晰的公共接口 = 天然的测试入口。比如本轮：

- `registry.Register` 是聚合根的公共方法 → 测"注册后改原指针不影响内部"是行为，合法
- `parseHostPort` 是抽出来的纯函数 → 测"各种输入返回什么"是行为，合法
- `Server.Start` 是公共方法 → 测"端口冲突返回 error"是行为，合法
- 而 `broadcastNodeJoin` 是私有方法、且要真实网络才能验证完整行为 → **不为它单独写测试**，而是把它依赖的脆弱纯逻辑抽成 `parseHostPort` 再测那个

这就是 10.2 的做法：**把不可测的私有逻辑，重构成可测的纯函数**。这是 TDD 驱动出的好设计，不是为测试而测试。

### 11.3 本轮五个修复的 RED-GREEN-REFACTOR 复盘

| Commit | Bug | RED（失败的测试） | GREEN（最小实现） | REFACTOR |
|--------|-----|------------------|------------------|----------|
| `e430418` | registry 写侧未 Clone | `RegisterDoesNotAliasCallerPointer`：注册后改原指针，断言内部不变 → 泄漏，FAIL | 写侧加 `proto.Clone` | 无（已对称） |
| `632f6e5` | 切片 panic | `TestParseHostPort` 表驱动，含无冒号边界 → 编译失败(函数不存在) | 实现 `parseHostPort` | `broadcastNodeJoin` 改用它、删 `mustParsePort`、删 `strings` import |
| `9c59398` | Start 吞 error | `TestServerStartReturnsErrorOnPortConflict`：占端口再 Start，断言返回 error → 返回 nil，FAIL | Start 改用 `net.Listen` 同步绑定 + Serve 日志 | 无 |
| `b810bf2` | 弃用 grpc API | （机械重构，靠现有 node 测试 + staticcheck SA1019 消失保护） | 换 `grpcutil.NewClientConn` | 无 |
| `a321bcc`/`a18c8a1` | 常量单位混用/风格 | （行为保持重构，靠现有测试 + staticcheck 保护） | 常量改 Duration、调用方简化、logger 合并声明 | 无 |

看模式了吗？**行为 bug 用失败测试驱动修复；纯机械重构靠现有测试 + 静态分析守护。** 两种都安全，但前提是"先有测试在前"。这就是为什么 AGENTS.md 里那条教训——"重构前先写端到端测试"——是用真实丢功能换来的。

### 11.4 mock 的边界

mattpocock 的原则：**只在系统边界 mock**（外部 API、DB、时间、网络）。不要 mock 自己的内部类。

本项目 `server_test.go` 的 mock 都在系统边界：`mockMessageSender` mock 的是 Node 这个"重型依赖"，`mockPeerConnector` 同理。而 registry 的测试不用任何 mock——registry 是纯内存聚合根，直接 new 一个测。这是正确的 mock 节制。

---

## 12. 五阶段时间脉络复盘（git 历史对照）

这一章把整个项目当案例复盘。每阶段给"做了什么 / 为什么 / 学到什么"。

### 阶段 0 — doubaoseed2.0：能跑的 God Struct（`5a418ba`）

**做了什么**：一把梭，`internal/peer/` 包，`Node` 上帝对象，过程式 proto。
**为什么**：第一版优先"能用"，这是合理的。**学到什么**：God Struct 的债不在第一版还，会在演化时加倍还。能在第一版就做名词提取固然好；做不到也要意识到"我欠了边界债"。

### 阶段 1 — glm5.1 DDD 重构（`e7994bd` → `94ea6c9`）

**做了什么**：proto 按三上下文重设计 → 抽 discovery/membership/messaging/transport 四包 → 建 Node 协调器 → 删旧 peer 包。
**为什么**：结构债已到影响演化的程度。
**学到什么（含惨痛教训）**：**结构对了 ≠ 功能对。** 重构时只关注"新设计更清晰"，却漏了旧代码的隐式行为。AGENTS.md 记录的典型：旧 `JoinResp.Peers` **包含 seed 自己**，新 `HandshakeResp` 把 seed 和 known_peers 分开，重构时只返回了 known_peers，导致 seed 从未被建立 stream 连接，消息发不出。— **隐式行为识别法**：读旧代码时问三个问题（来自 AGENTS.md）：
  1. 这个返回值除了明显内容，还包含什么？
  2. 调用后除了返回值，还改了什么状态？
  3. 第一次看这接口，我会漏掉什么假设？

### 阶段 2 — 找回功能（本轮 commit `9ffe3d3` → `ff9ffcc`）

**做了什么**：心跳、消息回显、断线感知、广播、Gossip、Ping——逐个补回。CONTEXT.md 的 P0-P2 清单就是找回功能的 todo。
**为什么**：阶段 1 丢的功能必须补。
**学到什么**：补功能时新增了横切 `notifier`（阶段 1 没设计它，因为当时只有 CLI）。这印证 DDD 是**演进式**的：新需求（Web 面板）逼出新的横切关注点，再回头抽象。**不要一次把所有上下文画死。**

### 阶段 3 — TDD 守护修复（本轮 `e430418` → `a18c8a1`，共 5 个修复 + 2 个重构）

**做了什么**：见第 11.3 节。每个行为 bug 先写失败测试，重构靠测试+staticcheck 守护。修完 staticcheck 从 3 项告警降到 0，race 检测全绿。
**为什么**：阶段 2 补功能时引入的脆弱点（写侧 alias、切片 panic、吞错、弃用 API、单位混用）需要系统性清理，且不能靠"改完祈祷"。
**学到什么**：DDD 给了清晰的边界，TDD 给了边界内的安全网。两者结合，改一类问题能确信没破坏别的。本轮验证：修完五个点，`go test ./internal/...` + `-race` 全绿、staticcheck 零告警。

### 阶段 4 — 文档化（`ebb8620`、`ff9ffcc`、本教程）

把决策、术语、教训写下来（AGENTS.md / CONTEXT.md / ADR / DDD_GUIDE / 本教程）。**文档是设计的一部分**：CONTEXT.md 里"通知 ≠ 日志"的界定，本身就是一次设计动作。

---

## 13. 常见误区（避坑）

1. **"DDD = 建一堆 domain/application 目录"** —— 错。关键是边界**语义**（同一概念不同含义才拆），不是目录层级。没有语义判断的分层只是包不动代码的搬运。

2. **"聚合根必须是数据库实体"** —— 错。本项目 `PeerRegistry` 是纯内存聚合根，照样是聚合根。判据是一致性边界，不是持久化。

3. **"横切关注点随便放个 util 包"** —— 错。notifier 独立成包 + ADR 写清"为什么不属于任何上下文"。每个横切包都该有这个"出身证明"，否则很快变成新的上帝包。

4. **"重构不带测试就是赌博"** —— 这是 AGENTS.md 用丢消息功能换来的教训。结构重构前必须先有端到端测试锁住核心场景（两节点互发消息）。本轮修 bug 全程 TDD 正是贯彻这条。

5. **"协调器等于上帝对象"** —— 见第 5 章。Node 协调、不持规则。混淆这两个就会"DDD 重构完又长出一个新 God Struct"。

6. **"读侧防御就够"** —— 见第 9 章。读写 Clone 必须对称。

7. **"TDD = 一次写完所有测试"** —— 见第 11.1 章。纵向切片，不是横向切片。

---

## 14. 动手练习（照着学）

### 练习 1（名词提取）
拿一个你最熟的 REST CRUD 服务（比如"订单系统"）。只列名词，不列功能。然后标注：哪些名词在不同场景含义不同（候选限界上下文）？哪些状态必须一起变更（候选聚合根）？

### 练习 2（边界判断）
在 p2ptest 里找：`ConnPool` 属于哪个上下文？为什么它独立成 `transport` 包而不是塞进 `messaging`？（提示：谁用 connPool？membership 的心跳也用。多上下文共享的基础设施 → 独立包，类似横切。）

### 练习 3（Deep Module）
为 `parseHostPort` 想两个更强的行为：IPv6 zone（`fe80::1%eth0:443`）、主机名而非 IP（`example.com:80`）。先写测试（表驱动），再改实现让它过。体会"小接口藏复杂度"。

### 练习 4（TDD 修待办）
本项目 `client.FindNode`（`internal/client/discovery.go`）经 CONTEXT.md 确认无业务调用方。用 TDD 方式：先写一个集成测试（起两个真节点，A 通过 B FindNode 查 C），RED，再写调用方让它在 gossip 场景里被用上，GREEN。这会逼你理解 DHT 路由雏形。

### 练习 5（防御对称审计）
通读 `internal/`，找出所有"集合存指针"的地方（`map[string]*Xxx`）。对每一处问：读侧 Clone 吗？写侧 Clone 吗？对称吗？把不对称的列成 issue。本轮已修 registry，还有没有别的？

---

## 附录 A — 审查报告优点提炼（可迁移到你别的项目）

以下是代码审查中发现的、本项目做得好的设计，每条都是可复用的思想：

1. **观察者正确性**：notifier 的"锁内拷贝回调、锁外调用"。任何 fan-out/事件总线都该这样写，否则死锁或慢消费者拖垮系统。

2. **分层锁粒度**：ConnPool 用 RWMutex 保护 map，再用 per-stream Mutex 保护单流 Send。不同临界区用不同锁，避免一把大锁串行所有 stream。这是并发设计的成熟标志。

3. **读写 Clone 对称**：聚合根对内主权、对外不泄漏别名（第 9 章，含修复）。

4. **协调器 vs 上帝对象**：Node 只编排不持规则（第 5 章）。

5. **接口族而非大接口**：web 的 5 个窄 Provider 接口，每个前端独立可测（第 8 章）。

6. **横切包有出身证明**：notifier + ADR-0001 明确声明"不属于任何上下文"（第 6 章）。

7. **把脆弱藏进 deep module**：lockedStream 封装并发约束、parseHostPort 封装解析安全（第 10 章）。

8. **常量带类型**：间隔统一 `time.Duration` 而非裸 int 毫秒，编译期消除单位误用（commit `a321bcc`）。

9. **失败要可见**：`Server.Start` 同步绑定让端口冲突立刻报错，而非后台吞错（commit `9c59398`）。

10. **文档即设计**：CONTEXT.md 的术语界定、ADR 的决策记录、AGENTS.md 的教训——都是设计动作，不是附庸。

---

## 附录 B — 术语对照表

| 术语 | 本项目对应 | 一句话理解 |
|------|-----------|-----------|
| 聚合根 Aggregate Root | `PeerRegistry` | 一致性边界的唯一入口 |
| 限界上下文 Bounded Context | discovery / membership / messaging 三包 | 同一概念的不同语义模型 |
| 横切关注点 Cross-cutting Concern | `notifier` | 不属于任何单个上下文的能力 |
| 协调器 Coordinator | `node.Node` | 编排上下文，自身不含业务规则 |
| Deep Module | `lockedStream` / `parseHostPort` | 小接口 + 深实现 |
| 依赖倒置 DIP | client/web 的 Provider 接口 | 依赖行为契约，不依赖具体类型 |
| 垂直切片 TDD | 本轮每个修复的 RED→GREEN | 一次一个测试→一段实现 |

---

## 15. 收尾实战：Ed25519 签名落地（P1-A 闭环）

本章是对"项目收尾"的记录。签名是 P1-A 的全部内容，也是 p2ptest 从"学习透镜"升级为"原设计意图完整兑现"的标志。它把 DDD、TDD、横切关注点、deep module 全串在一起——你照着这章读代码，会发现前面 14 章的概念都在这一个 feature 里用上了。

### 15.1 为什么 Ed25519 是"一石二鸟"的选择

AGENTS.md 的 P1-A 小节写明了选 Ed25519 的理由：Solana 同款签名算法。这不是凑巧——Ed25519 的 Go 标准库实现（`crypto/ed25519`）和 Rust 的 `ed25519-dalek` 是同一套曲线（Curve25519），API 几乎一一对应。你在 p2ptest 里学会的 sign/verify 模式，到 Solana 里写钱包验证时直接复用。

对照 phase-0 README 的"一石二鸟"表："认证鉴权 → 钱包签名验证"这一行，现在落地了。

### 15.2 crypto 包：教科书级 deep module

`internal/crypto/crypto.go` 只有 77 行，但它是一个完美的 deep module：

```
小接口（对外 3 个入口）：
  NewIdentity() → *Identity           生成密钥对
  (*Identity).Sign(data) → []byte     签名
  Verify(pk, data, sig) → error       验签

深实现（藏在内）：
  crypto/ed25519 标准库
  密钥生成、签名、验证的完整密码学
  HandshakeSignData / HeartbeatSignData 辅助函数（定义签名内容）
```

调用方（Membership/Messaging/Node/client）只需要知道"Sign 传数据拿签名、Verify 传公钥+数据+签名验真"——完全不需要知道 Curve25519 是什么。如果哪天换算法（比如上 BLS 聚合签名），只改 crypto 包内部，外部零改动。

这正是第 10 章"deep module"概念的真实应用：**小接口藏大坑——这里藏的是密码学。**

### 15.3 签名注入：三层防线

签名不是一道墙，是三层独立验证——每层保护不同的信任假设：

**层 1：Handshake 签名**（membership/service.go）
- 发起方 client 调 `n.Sign(HandshakeSignData(uuid))` 签名
- 接收方 membership 验签后才注册——**防伪造 Join**
- 验签失败返回 `Accepted=false, RejectReason: "invalid signature"`

**层 2：Heartbeat 签名**（node.go 签名 + membership 验签）
- Node 心跳循环调 `identity.Sign(HeartbeatSignData(uuid, timestamp))`
- 接收方 membership 验签——**防伪造心跳 + 防重放**（timestamp 绑定）
- 验签失败返回 `UNKNOWN` 状态

**层 3：Envelope 签名**（client 签名 + messaging 验签）
- client 计算 `SHA256(proto.Marshal(payload))` 得 content_hash，再 `n.Sign(content_hash)` 签名
- 接收方 messaging 先验 content_hash（防篡改），再验签名（防伪造）——**防篡改 + 防 Poison 消息**
- 验签失败静默丢弃

### 15.4 向后兼容策略：宽松验签

一个关键的设计决策——**验签只在签名存在时做**。看 membership 的 Handshake 验签：

```go
// service.go —— 宽松验签
if len(req.Signature) > 0 {        // 有签名才验
    if crypto.Verify(...) != nil {  // 验不过才拒
        return rejected
    }
}
// 无签名 → 照常处理（向后兼容旧客户端）
```

Messaging 也一样：

```go
// service.go verifyEnvelope
if len(env.Signature) == 0 || ... {
    return true   // 无签名 → 放行
}
```

为什么这么做？因为 p2ptest 是学习项目，不会强制所有节点同时升级。这样设计后：旧节点（无签名）和新节点（有签名）能共存，新节点之间的通信受签名保护，旧节点之间不受保护但不报错。

**这是"渐进式安全升级"的标准模式**——生产级系统从明文切 TLS 时也这么干（insecure + TLS 双栈过渡期）。

### 15.5 TDD 的第一个 RED

按 mattpocock 垂直切片，签名的第一个 RED 是 `TestIdentitySignAndVerify`：

```go
// crypto_test.go —— tracer bullet
func TestIdentitySignAndVerify(t *testing.T) {
    alice, err := NewIdentity()
    // ...
    sig := alice.Sign(data)
    if err := Verify(alice.PublicKey(), data, sig); err != nil {
        t.Errorf("Verify failed for valid signature: %v", err)
    }
}
```

编译失败（`NewIdentity` / `Verify` 不存在）→ 实现 77 行 crypto.go → GREEN。然后追加三个测试：篡改数据被拒、错误公钥被拒、公钥稳定——每个都是行为测试，不碰实现细节。

`crypto_test.go` 全部 4 个测试用真实 Ed25519 密钥对，零 mock。因为密码学验证天然是纯函数行为：输入确定、输出确定、无副作用——是 deep module 的理想测试对象。

### 15.6 架构图

`docs/architecture.html` 是一张交互式架构图（浏览器打开），用深色主题可视化整个系统：

- **绿色**（Backend）：三个限界上下文（Discovery / Membership / Messaging）+ domain.PeerRegistry 聚合根
- **橙色**（Event Bus）：notifier + Emit/Subscribe 箭头（事件流）
- **玫红**（Security）：crypto.Identity + Verify 箭头（签名验签流）
- **紫色**（Aggregate Root）：PeerRegistry
- **青色**（Frontend）：CLI/Web 通过 Provider 接口注入
- **灰虚线框**：Bounded Contexts 边界 + 共享基础设施（ConnPool）

图里能看到第 7 章讲的三件事：
1. 三上下文不互相 import（各自独立绿色块）
2. 事件总线（橙色箭头）连接所有上下文和前端
3. Node 既是协调器（顶部）又是 crypto 的持有者，签名向下注入各层

### 15.7 这一章串联的概念

| 概念 | 在签名实现里的体现 | 对应教程章节 |
|------|-------------------|-------------|
| Deep module | crypto 包: 3 方法藏密码学 | 第 10 章 |
| 聚合根 | PeerRegistry 存公钥用于验签 | 第 3 章 |
| 限界上下文 | 验签逻辑各 context 独立实现 | 第 4 章 |
| 横切关注点 | crypto 独立成包，所有上下文共享 | 第 6 章 |
| 领域事件 | 签名后的消息触发 message_received | 第 7 章 |
| 依赖倒置 | client 通过 Signer 接口调 Node.Sign | 第 8 章 |
| 防御对称 | 签名方 Sign + 验签方 Verify 对称 | 第 9 章 |
| 垂直切片 TDD | TestIdentitySignAndVerify 是 tracer bullet | 第 11 章 |
| 向后兼容 | 宽松验签策略 | 新概念 |

签名不是孤立功能——它是前面 14 章所有概念的综合应用。读懂这一章，就读懂了整个教程。

---

## 结语

DDD 不是一套目录模板，是一种**看系统的方式**：先看名词，再看边界，最后才填动词。
TDD 不是写测试的运动，是一种**改代码的纪律**：先写一个会失败的测试证明问题存在，再让它过，再重构。

这个项目花了一个 God Struct、一次丢功能、五次 TDD 修复、一次 Ed25519 签名收尾，才走到现在。每一步都能在 `git log` 里看到。把它当一面镜子：你下一个系统的第一版会不会有边界债？演化时能不能用测试守护？抽横切包时有没有写出身证明？安全功能能不能用 deep module 藏复杂度？

带着这些问题去看代码，比看任何概念书都有用。不懂的章节，随时问我。
