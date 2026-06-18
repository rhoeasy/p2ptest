package application

import (
	"context"
	"fmt"
	"sync/atomic"

	"p2ptest/internal/crypto"
	"p2ptest/internal/discovery/domain"
	"p2ptest/internal/logger"
	"p2ptest/internal/notifier"
	"p2ptest/internal/types"
	pb "p2ptest/proto/p2p"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// MembershipService 实现 gRPC Membership 服务接口。
// 职责：管理节点间的对等关系（握手、心跳、断开）。
type MembershipService struct {
	pb.UnimplementedMembershipServer
	registry     domain.PeerRegistry
	selfInfo     *pb.NodeInfo
	cfg          *types.NodeConfig
	notifier     *notifier.Notifier
	statusGetter atomic.Value // stores func() pb.NodeStatus
}

func NewMembershipService(registry domain.PeerRegistry, selfInfo *pb.NodeInfo, cfg *types.NodeConfig, notifier *notifier.Notifier) *MembershipService {
	svc := &MembershipService{
		registry: registry,
		selfInfo: selfInfo,
		cfg:      cfg,
		notifier: notifier,
	}
	if selfInfo != nil {
		svc.statusGetter.Store(func() pb.NodeStatus { return pb.NodeStatus_ONLINE })
	}
	return svc
}

func (s *MembershipService) SetStatusGetter(fn func() pb.NodeStatus) {
	s.statusGetter.Store(fn)
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

	// 验签：如果签名存在（新版本客户端），必须验证通过才接受
	if len(req.Signature) > 0 {
		if len(req.Self.Id.PublicKey) == 0 {
			return &pb.HandshakeResp{Accepted: false, RejectReason: "public_key required when signature present"}, nil
		}
		if err := crypto.Verify(req.Self.Id.PublicKey, crypto.HandshakeSignData(req.Self.Id.Uuid), req.Signature); err != nil {
			logger.L().Warn("[membership] handshake signature invalid", zap.String("uuid", req.Self.Id.Uuid))
			return &pb.HandshakeResp{Accepted: false, RejectReason: "invalid signature"}, nil
		}
	}

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

	if s.notifier != nil {
		peerAddr := ""
		if len(req.Self.Addrs) > 0 {
			peerAddr = fmt.Sprintf("%s:%d", req.Self.Addrs[0].Ip, req.Self.Addrs[0].Port)
		}
		s.notifier.Emit(notifier.NewPeerOnlineNotification(req.Self.Id.Name, peerAddr))

		if len(knownPeers) > 0 {
			s.notifier.Emit(notifier.NewPeerDiscoveredNotification(
				req.Self.Id.Name, peerAddr, req.Self.Id.Uuid,
			))
		}
	}

	return &pb.HandshakeResp{
		Peer:       proto.Clone(s.selfInfo).(*pb.NodeInfo),
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

	// 验签：如果签名存在，必须验证通过
	if len(req.Signature) > 0 {
		if len(req.NodeId.PublicKey) == 0 {
			return &pb.HeartbeatResp{Status: pb.NodeStatus_UNKNOWN}, nil
		}
		if err := crypto.Verify(req.NodeId.PublicKey, crypto.HeartbeatSignData(req.NodeId.Uuid, req.Timestamp), req.Signature); err != nil {
			logger.L().Debug("[membership] heartbeat signature invalid", zap.String("uuid", req.NodeId.Uuid))
			return &pb.HeartbeatResp{Status: pb.NodeStatus_UNKNOWN}, nil
		}
	}

	status := pb.NodeStatus_ONLINE
	if fn, ok := s.statusGetter.Load().(func() pb.NodeStatus); ok && fn != nil {
		status = fn()
	}

	return &pb.HeartbeatResp{
		Status:    status,
		Timestamp: req.Timestamp,
	}, nil
}

// Disconnect 处理断开请求。
// 语义：接收方将发起方状态设为 OFFLINE，然后注销，确认断开。
func (s *MembershipService) Disconnect(ctx context.Context, req *pb.DisconnectReq) (*pb.DisconnectResp, error) {
	if req == nil || req.NodeId == nil || req.NodeId.Uuid == "" {
		return &pb.DisconnectResp{Acknowledged: true}, nil
	}

	if _, exists := s.registry.Get(req.NodeId.Uuid); exists {
		_ = s.registry.UpdateStatus(req.NodeId.Uuid, pb.NodeStatus_OFFLINE)
	}

	if removed, _ := s.registry.Unregister(req.NodeId.Uuid); removed {
		logger.L().Info("[membership] node disconnected",
			zap.String("name", req.NodeId.Name),
			zap.String("uuid", req.NodeId.Uuid),
			zap.String("reason", req.Reason),
		)

		if s.notifier != nil {
			s.notifier.Emit(notifier.NewPeerOfflineNotification(req.NodeId.Name, req.Reason))
		}
	}

	return &pb.DisconnectResp{Acknowledged: true}, nil
}

func (s *MembershipService) NotifyNodeJoin(ctx context.Context, req *pb.NotifyNodeJoinReq) (*pb.NotifyNodeJoinResp, error) {
	if req == nil || req.NewNode == nil || req.NewNode.Id == nil || req.NewNode.Id.Uuid == "" {
		return &pb.NotifyNodeJoinResp{Acknowledged: true}, nil
	}

	if req.NewNode.Id.Uuid == s.selfInfo.Id.Uuid {
		return &pb.NotifyNodeJoinResp{Acknowledged: true}, nil
	}

	if _, exists := s.registry.Get(req.NewNode.Id.Uuid); exists {
		s.registry.UpdateLastActive(req.NewNode.Id.Uuid)
		return &pb.NotifyNodeJoinResp{Acknowledged: true}, nil
	}

	if err := s.registry.Register(req.NewNode); err != nil {
		logger.L().Warn("[membership] notify-node-join register failed", zap.Error(err))
		return &pb.NotifyNodeJoinResp{Acknowledged: false}, nil
	}

	peerAddr := ""
	if len(req.NewNode.Addrs) > 0 {
		peerAddr = fmt.Sprintf("%s:%d", req.NewNode.Addrs[0].Ip, req.NewNode.Addrs[0].Port)
	}

	logger.L().Info("[membership] notified of new node",
		zap.String("node_name", req.NewNode.Id.Name),
		zap.String("addr", peerAddr),
	)

	if s.notifier != nil {
		s.notifier.Emit(notifier.NewPeerDiscoveredNotification(
			req.NewNode.Id.Name, peerAddr, req.NewNode.Id.Uuid,
		))
	}

	return &pb.NotifyNodeJoinResp{Acknowledged: true}, nil
}
