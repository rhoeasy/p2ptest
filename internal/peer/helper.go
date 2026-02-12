package peer

import (
	"fmt"
	"p2ptest/internal/types"
	"time"

	pb "p2ptest/proto"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func genNodeUUID() string {
	return uuid.NewString()
}

// genMsgID 生成消息唯一ID（仅节点模块使用）
func genMsgID() string {
	return uuid.NewString()
}

// getTimestampMs 获取当前时间戳（毫秒）
func getTimestampMs() uint64 {
	return uint64(time.Now().UnixMilli())
}

// toPbTimestamp 转换为Protobuf Timestamp类型
func toPbTimestamp() *timestamppb.Timestamp {
	return timestamppb.New(time.Now())
}

// buildNodeInfo 构建节点信息（封装重复逻辑）
func buildNodeInfo(cfg *types.NodeConfig) *pb.NodeInfo {
	return &pb.NodeInfo{
		Id: &pb.NodeID{
			Uuid: genNodeUUID(),
			Name: cfg.NodeName,
			Hash: []byte{},
		},
		Addrs: []*pb.NodeAddr{
			{
				Ip:       cfg.ListenIP,
				Port:     cfg.ListenPort,
				NatType:  pb.NatType_NAT_UNKNOWN,
				IsPublic: true,
				Protocol: "tcp",
			},
		},
		Status:            pb.NodeStatus_NODE_STATUS_ONLINE,
		HeartbeatInterval: types.HeartbeatInterval,
		Version:           types.DefaultNodeVer,
		LastActive:        toPbTimestamp(),
	}
}

func formatNodeAddr(ip string, port uint32) string {
	return fmt.Sprintf("%s:%d", ip, port)
}

// getPeerFirstAddr 标准Go风格：返回 地址+错误
func getPeerFirstAddr(peer *pb.NodeInfo) (string, error) {
	if peer == nil || len(peer.Addrs) == 0 {
		return "", types.ErrNoValidNodeAddress
	}
	return formatNodeAddr(peer.Addrs[0].Ip, peer.Addrs[0].Port), nil
}
