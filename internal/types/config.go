package types

import "time"

// NodeConfig 节点基础配置（通用，服务端/客户端都需要）
type NodeConfig struct {
	NodeName   string // 节点可读名称
	ListenIP   string // 监听IP
	ListenPort uint32 // 监听端口
	ProtoVer   uint32 // 协议版本（固定为1）

}

// 通用常量（避免魔法值）
const (
	HeartbeatInterval = 5000  // 心跳间隔（毫秒）
	GossipInterval    = 30000 // Gossip 间隔（毫秒）
	DefaultProtoVer   = 1     // 默认协议版本

	HeartbeatTimeout = 30 * time.Second
	CleanInterval    = 5 * time.Second

	DefaultNodeVer string = "v0.1.0"
)
