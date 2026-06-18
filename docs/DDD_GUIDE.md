# DDD 设计指南 — P2P网络重构实录

## 问题：从过程式到领域式

原始设计的 proto 使用过程式思维：
```protobuf
service P2PPeerService {
  rpc Join(JoinReq) returns (JoinResp);           // 我要加入
  rpc NotifyNodeJoin(NodeInfo) returns (Empty);   // 通知别人我加入了
  rpc Leave(NodeID) returns (Empty);              // 我要离开
  rpc SendHeartbeat(HeartbeatReq) returns (Empty); // 发送心跳
}
```

问题：动词是主角，主语缺失。
- `Join` — 谁 join 谁？是 A 加入 B，还是 B 加入 A？
- `NotifyNodeJoin` — 谁 notify 谁？为什么要单独一个 RPC？
- `Leave` — 谁 leave？是我离开，还是让你离开？

## 解决：名词先行，动词依附

DDD 思维顺序：
1. 先找**名词**（领域中有哪些概念）
2. 再找**关系**（这些概念之间有什么联系）
3. 最后填**动词**（这些概念能做什么）

### P2P 网络中的概念浮现

```
网络中有：
  - 节点（Node）
  - 节点之间有关系：对等关系（Membership）
  - 节点需要被发现：发现（Discovery）
  - 节点需要通信：消息传递（Messaging）

所以动词应该依附于这些概念：
  - Membership.Handshake — 建立对等关系
  - Membership.Heartbeat — 维持对等关系
  - Membership.Disconnect — 终止对等关系
  - Discovery.GetPeers — 查询已知节点
  - Discovery.FindNode — 查找特定节点
  - Messaging.Stream — 传递消息
```

## 核心原则

### 1. 命名审查法

写接口时问自己三个问题：
- 这个函数的**主语**是谁？
- 这个函数的**宾语**是谁？
- 结果产生了什么**领域对象**？

| 函数名 | 主语 | 宾语 | 评价 |
|--------|------|------|------|
| `Join(req)` | ??? | ??? | 主语缺失，语义模糊 |
| `Handshake(req)` | 发起方 | 接收方 | 双向确认，语义清晰 |
| `Leave(id)` | ??? | ??? | 是我离开还是让你离开？ |
| `Disconnect(req)` | 发起方 | 接收方 | "我要断开与你的连接" |

### 2. 名词提取法

拿到需求后，**先不要想功能，先列出名词**：

P2P 网络：
- 节点（Node）— 有身份、有地址、有状态
- 对等关系（Membership）— 节点之间的关系
- 发现（Discovery）— 网络拓扑信息
- 消息（Message）— 通信内容

这些名词就是**聚合根**（Aggregate Root）。代码结构应该围绕它们组织，而不是围绕功能流程组织。

### 3. 语义聚焦法

好的领域命名能**消除歧义**，不需要额外解释：

```protobuf
// 模糊：Join 后会发生什么？B 知不知道 A？A 知不知道 B 的 peers？
rpc Join(JoinReq) returns (JoinResp);

// 清晰：Handshake 是双向的，一次调用完成所有事
rpc Handshake(HandshakeReq) returns (HandshakeResp);
// HandshakeResp 包含：
//   - peer: 对方的 NodeInfo
//   - known_peers: 对方已知的其他节点（替代 NotifyNodeJoin！）
//   - accepted: 是否接受
```

### 4. 边界划分法（限界上下文）

问自己：**这个概念在不同场景下含义是否相同？**

| 概念 | 在 Discovery 中 | 在 Membership 中 |
|------|---------------|----------------|
| NodeInfo | 用于查询和路由 | 用于建立对等关系 |
| 节点列表 | 网络拓扑视图 | 成员资格证明 |

含义不同 → 分成不同的上下文（Bounded Context）

## 重构前后对比

### 目录结构

```
重构前（按功能分层）：
internal/
  peer/
    node.go          # 上帝对象：服务+注册表+连接池+心跳+广播
    node_rpc.go      # RPC 实现
    node_heartbeat.go # 心跳逻辑
    node_broadcast.go # 广播逻辑
    registry.go       # 注册表

重构后（按领域分上下文）：
internal/
  discovery/          # 限界上下文：节点发现
    domain/
      registry.go     # PeerRegistry 聚合根
    application/
      service.go      # Discovery gRPC 服务
  membership/         # 限界上下文：成员管理
    application/
      service.go      # Membership gRPC 服务
  messaging/          # 限界上下文：消息传递
    application/
      service.go      # Messaging gRPC 服务
  node/               # 聚合根
    node.go           # 协调三个上下文
```

### 接口设计

| 场景 | 过程式 | DDD |
|------|--------|-----|
| 新节点入网 | `Join` → `NotifyNodeJoin` | `Handshake`（一次调用完成） |
| 维持关系 | `SendHeartbeat` | `Heartbeat` |
| 退出网络 | `Leave` | `Disconnect` |
| 查询节点 | `FindNode`（在 P2PPeerService 里） | `Discovery.FindNode`（独立上下文） |
| 发送消息 | `PeerMessageStream`（扁平消息） | `Messaging.Stream`（信封+payload） |

## 一句话总结

> 过程式：动词开头，名词是参数
> DDD：名词开头，动词是方法

过程式思维：`Join(req)` —— 我要做什么？
DDD 思维：`membership.Handshake(req)` —— 领域对象能做什么？

## 练习建议

1. 拿一个你设计的系统，只列名词，画关系图
2. 检查每个函数名：主语是否清晰？
3. 把模糊的动词（Do/Process/Handle）换成具体的领域动词
4. 如果一个函数涉及多个领域概念，考虑拆分上下文
