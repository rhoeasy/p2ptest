package peer

import (
	"context"
	"fmt"
	"p2ptest/internal/logger"
	pb "p2ptest/proto"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ====================== 服务端 API 实现 ======================

// Join 处理其他节点的加入请求，返回自身节点列表
func (n *Node) Join(ctx context.Context, req *pb.JoinReq) (*pb.JoinResp, error) {
	if req == nil || req.NodeInfo == nil || req.NodeInfo.Id == nil {
		return &pb.JoinResp{
			Success: false,
			Error: &pb.ErrorResp{
				Code: pb.ErrorCode_ERR_NODE_EXIST,
				Msg:  "invalid join request",
			},
			ProtoVersion: n.cfg.ProtoVer,
		}, nil
	}

	nodeID := req.NodeInfo.Id
	nodeAddr, err := getPeerFirstAddr(req.NodeInfo)
	// 无地址也允许Join（只打日志）
	if err != nil {
		logger.L().Info("[server] 节点无有效地址",
			zap.String("node_name", nodeID.Name),
			zap.String("uuid", nodeID.Uuid),
		)
	}

	logger.L().Info("[server] 收到Join请求",
		zap.String("node_name", nodeID.Name),
		zap.String("uuid", nodeID.Uuid),
		zap.String("addr", nodeAddr),
	)

	// 1. 短锁：清理同名+同IP旧节点
	n.mu.Lock()
	n.cleanOldPeerByNameAndAddrUnlocked(nodeID.Name, nodeAddr)
	n.mu.Unlock()

	// 2. 短锁：判断UUID是否已存在
	n.mu.RLock()
	_, exists := n.onlinePeers[nodeID.Uuid]
	n.mu.RUnlock()

	if exists {
		return &pb.JoinResp{
			Success: false,
			Error: &pb.ErrorResp{
				Code: pb.ErrorCode_ERR_NODE_EXIST,
				Msg:  fmt.Sprintf("node %v(%v) exist", nodeID.Name, nodeID.Uuid),
			},
			ProtoVersion: n.cfg.ProtoVer,
		}, nil
	}

	// 3. 添加新节点
	n.mu.Lock()
	n.onlinePeers[nodeID.Uuid] = req.NodeInfo
	n.lastActive[nodeID.Uuid] = time.Now()
	n.mu.Unlock()

	logger.L().Info("[server] 节点加入成功",
		zap.String("node_name", nodeID.Name),
		zap.String("uuid", nodeID.Uuid),
	)

	// 4. 获取节点列表并广播
	peers := n.getPeerList()
	go n.broadcastNodeJoin(req.NodeInfo)

	// 5. 返回响应
	return &pb.JoinResp{
		Success: true,
		Error: &pb.ErrorResp{
			Code:    pb.ErrorCode_ERR_SUCCESS,
			Msg:     "successfully get peers",
			Version: n.cfg.ProtoVer,
		},
		Peers:        peers,
		ProtoVersion: n.cfg.ProtoVer,
	}, nil
}

// cleanOldPeerByNameAndAddrUnlocked 清理同名+同IP旧节点（无锁）
func (n *Node) cleanOldPeerByNameAndAddrUnlocked(targetName string, targetAddr string) {
	for uuid, peer := range n.onlinePeers {
		if peer == nil || peer.Id == nil {
			continue
		}

		peerName := peer.Id.Name
		peerAddr, err := getPeerFirstAddr(peer)
		if err != nil {
			continue
		}

		// 同名+同地址 → 清理旧节点
		if peerName == targetName && peerAddr == targetAddr {
			n.cleanPeerResourceUnlocked(uuid)
			logger.L().Warn("[server] 清理同名同IP旧节点",
				zap.String("old_uuid", uuid),
				zap.String("name", targetName),
				zap.String("addr", targetAddr),
			)
		}
	}
}

// SendHeartbeat 处理其他节点的心跳请求
func (n *Node) SendHeartbeat(ctx context.Context, req *pb.HeartbeatReq) (*emptypb.Empty, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if _, exists := n.onlinePeers[req.NodeId.Uuid]; exists {
		n.lastActive[req.NodeId.Uuid] = time.Now()
		logger.L().Debug("[server] 收到心跳",
			zap.String("node", req.NodeId.Name),
			zap.String("status", req.Status.String()),
		)
	}

	return &emptypb.Empty{}, nil
}

// Leave 处理节点主动离开
func (n *Node) Leave(ctx context.Context, nodeID *pb.NodeID) (*emptypb.Empty, error) {
	if nodeID == nil || nodeID.Uuid == "" {
		return &emptypb.Empty{}, nil
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// 统一清理入口
	n.cleanPeerResourceUnlocked(nodeID.Uuid)

	logger.L().Info("[server] 节点离开",
		zap.String("name", nodeID.Name),
		zap.String("uuid", nodeID.Uuid),
	)

	return &emptypb.Empty{}, nil
}

// NotifyNodeJoin 处理新节点上线通知
func (n *Node) NotifyNodeJoin(ctx context.Context, newPeer *pb.NodeInfo) (*emptypb.Empty, error) {
	if newPeer == nil || newPeer.Id == nil || newPeer.Id.Uuid == "" {
		return &emptypb.Empty{}, nil
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// 跳过自身
	if newPeer.Id.Uuid == n.nodeID.Uuid {
		return &emptypb.Empty{}, nil
	}

	// 已存在则更新活跃时间
	if _, exists := n.onlinePeers[newPeer.Id.Uuid]; exists {
		n.lastActive[newPeer.Id.Uuid] = time.Now()
		return &emptypb.Empty{}, nil
	}

	// 添加新节点
	n.onlinePeers[newPeer.Id.Uuid] = newPeer
	n.lastActive[newPeer.Id.Uuid] = time.Now()

	// ✅ 正确判断：用 error 代替魔法字符串
	addr, err := getPeerFirstAddr(newPeer)
	if err == nil { // 只有地址合法时，才加映射
		n.addNameAddrMappingUnlocked(newPeer.Id.Name, addr)
	}

	logger.L().Info("[server] 收到新节点上线通知",
		zap.String("node_name", newPeer.Id.Name),
		zap.String("uuid", newPeer.Id.Uuid),
	)

	return &emptypb.Empty{}, nil
}

// PeerMessageStream 处理双向消息流
func (n *Node) PeerMessageStream(stream pb.P2PPeerService_PeerMessageStreamServer) error {
	for {
		msg, err := stream.Recv()
		if err != nil {
			// 正常关闭不打印警告
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.Canceled {
				logger.L().Debug("[stream] 流正常关闭")
			} else {
				logger.L().Warn("[stream] 流异常关闭", zap.Error(err))
			}
			return err
		}

		if msg == nil {
			continue
		}

		// 处理文本消息
		if msg.Type == pb.MessageType_MSG_TEXT {
			content := msg.GetText()
			logger.L().Info("[server] 收到消息",
				zap.String("from", msg.From.Name),
				zap.String("content", content),
			)

			// 构造回复
			reply := &pb.P2PMessage{
				MsgId:        genMsgID(),
				Type:         pb.MessageType_MSG_TEXT,
				From:         n.nodeID,
				ProtoVersion: n.cfg.ProtoVer,
				SendTime:     toPbTimestamp(),
				Content: &pb.P2PMessage_Text{
					Text: fmt.Sprintf("received: %s", content),
				},
				ContentHash: []byte{},
				Signature:   []byte{},
			}

			if err := stream.Send(reply); err != nil {
				logger.L().Error("[server] 回复消息失败", zap.Error(err))
				return err
			}
		}
	}
}
