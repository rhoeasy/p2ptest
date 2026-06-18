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
	// 间隔统一用 time.Duration，避免"裸毫秒 int 当 Duration 用"的语义陷阱。
	// 调用方可直接赋值给 time.Duration 字段，无需手动换算。
	HeartbeatInterval = 5 * time.Second
	GossipInterval    = 30 * time.Second
	HeartbeatTimeout  = 30 * time.Second
	CleanInterval     = 5 * time.Second

	DefaultProtoVer = 1            // 默认协议版本
	DefaultNodeVer  string = "v0.1.0"
)
