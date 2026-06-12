package application

import (
	"context"

	"p2ptest/internal/discovery/domain"
	"p2ptest/internal/logger"
	"p2ptest/internal/types"
	pb "p2ptest/proto/p2p"

	"go.uber.org/zap"
)

// MembershipService 实现 gRPC Membership 服务接口。
// 职责：管理节点间的对等关系（握手、心跳、断开）。
type MembershipService struct {
	pb.UnimplementedMembershipServer
	registry domain.PeerRegistry
	selfInfo *pb.NodeInfo
	cfg      *types.NodeConfig
}

func NewMembershipService(registry domain.PeerRegistry, selfInfo *pb.NodeInfo, cfg *types.NodeConfig) *MembershipService {
	return &MembershipService{
		registry: registry,
		selfInfo: selfInfo,
		cfg:      cfg,
	}
}

// Handshake 处理握手请求。
// 语义：接收方验证发起方，若接受则注册发起方并返回自己的信息和已知节点列表。
func (s *MembershipService) Handshake(ctx context.Context, req *pb.HandshakeReq) (*pb.HandshakeResp, error) {
	if req == nil || req.Self == nil || req.Self.Id == nil || req.Self.Id.Uuid == "" {
		return &pb.HandshakeResp{Accepted: false, RejectReason: "invalid handshake request"}, nil
	}

	logger.L().Info("[membership] received handshake",
		zap.String("node_name", req.Self.Id.Name),
		zap.String("uuid", req.Self.Id.Uuid),
	)

	// 注册对方到自己的 registry
	if err := s.registry.Register(req.Self); err != nil {
		logger.L().Warn("[membership] failed to register peer", zap.Error(err))
		return &pb.HandshakeResp{Accepted: false, RejectReason: err.Error()}, nil
	}

	logger.L().Info("[membership] handshake accepted",
		zap.String("node_name", req.Self.Id.Name),
		zap.String("uuid", req.Self.Id.Uuid),
	)

	// 返回自己的信息 + 已知节点列表
	knownPeers := s.registry.List()

	return &pb.HandshakeResp{
		Peer:       s.selfInfo,
		KnownPeers: knownPeers,
		Accepted:   true,
	}, nil
}

// Heartbeat 处理心跳请求。
// 语义：接收方更新发起方的 lastActive，返回自己的状态。
func (s *MembershipService) Heartbeat(ctx context.Context, req *pb.HeartbeatReq) (*pb.HeartbeatResp, error) {
	if req == nil || req.NodeId == nil {
		return &pb.HeartbeatResp{Status: pb.NodeStatus_UNKNOWN}, nil
	}

	s.registry.UpdateLastActive(req.NodeId.Uuid)

	logger.L().Debug("[membership] received heartbeat",
		zap.String("node", req.NodeId.Name),
		zap.String("uuid", req.NodeId.Uuid),
	)

	return &pb.HeartbeatResp{
		Status:    pb.NodeStatus_ONLINE,
		Timestamp: req.Timestamp,
	}, nil
}

// Disconnect 处理断开请求。
// 语义：接收方注销发起方，确认断开。
func (s *MembershipService) Disconnect(ctx context.Context, req *pb.DisconnectReq) (*pb.DisconnectResp, error) {
	if req == nil || req.NodeId == nil || req.NodeId.Uuid == "" {
		return &pb.DisconnectResp{Acknowledged: true}, nil
	}

	if err := s.registry.Unregister(req.NodeId.Uuid); err != nil {
		logger.L().Warn("[membership] failed to unregister peer", zap.Error(err))
	}

	logger.L().Info("[membership] node disconnected",
		zap.String("name", req.NodeId.Name),
		zap.String("uuid", req.NodeId.Uuid),
		zap.String("reason", req.Reason),
	)

	return &pb.DisconnectResp{Acknowledged: true}, nil
}

func (s *MembershipService) Registry() domain.PeerRegistry {
	return s.registry
}
