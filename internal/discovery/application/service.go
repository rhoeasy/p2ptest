package application

import (
	"context"

	"p2ptest/internal/discovery/domain"
	pb "p2ptest/proto/p2p"
)

// DiscoveryService 实现 gRPC Discovery 服务接口。
// 职责：响应节点发现请求，不涉及成员关系变更。
type DiscoveryService struct {
	pb.UnimplementedDiscoveryServer
	registry domain.PeerRegistry
}

func NewDiscoveryService(registry domain.PeerRegistry) *DiscoveryService {
	return &DiscoveryService{registry: registry}
}

func (s *DiscoveryService) GetPeers(ctx context.Context, req *pb.GetPeersReq) (*pb.GetPeersResp, error) {
	peers := s.registry.List()

	if req != nil && req.Limit > 0 && int(req.Limit) < len(peers) {
		peers = peers[:req.Limit]
	}

	return &pb.GetPeersResp{Peers: peers}, nil
}

func (s *DiscoveryService) FindNode(ctx context.Context, req *pb.FindNodeReq) (*pb.FindNodeResp, error) {
	if req == nil || req.Target == nil {
		return &pb.FindNodeResp{Found: false}, nil
	}

	node, found := s.registry.Get(req.Target.Uuid)
	return &pb.FindNodeResp{
		Node:  node,
		Found: found,
	}, nil
}
