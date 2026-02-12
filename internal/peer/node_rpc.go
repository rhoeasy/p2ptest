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

// Join 处理其他节点的加入请求，就是把自己的peer返回给请求节点，让它自己去建立连接
func (n *Node) Join(ctx context.Context, req *pb.JoinReq) (*pb.JoinResp, error) {
	if req == nil || req.NodeInfo == nil || req.NodeInfo.Id == nil || len(req.NodeInfo.Addrs) == 0 {
		return &pb.JoinResp{
			Success: false,
			Error: &pb.ErrorResp{
				Code: pb.ErrorCode_ERR_NODE_EXIST, // 用你已有的错误码
				Msg:  "invalid join request",
			},
			ProtoVersion: n.cfg.ProtoVer,
		}, nil
	}

	nodeID := req.NodeInfo.Id
	nodeAddr := fmt.Sprintf("%s:%d", req.NodeInfo.Addrs[0].Ip, req.NodeInfo.Addrs[0].Port)

	logger.L().Info("[server] 收到Join请求",
		zap.String("node_name", nodeID.Name),
		zap.String("uuid", nodeID.Uuid),
		zap.String("addr", nodeAddr),
	)

	// ======================
	// 1. 短锁：先清理【同名+同IP】的旧节点（解决脏数据）
	// ======================
	n.mu.Lock()
	n.cleanOldPeerByNameAndAddrUnlocked(nodeID.Name, nodeAddr)
	n.mu.Unlock()

	// ======================
	// 2. 短锁：判断UUID是否已存在
	// ======================
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

	n.mu.Lock()
	n.onlinePeers[nodeID.Uuid] = req.NodeInfo
	n.lastActive[nodeID.Uuid] = time.Now()
	n.mu.Unlock()

	logger.L().Info("[server side] node successfully join p2p",
		zap.String("node_name", nodeID.Name),
		zap.String("uuid", nodeID.Uuid),
	)

	peers := n.getPeerList()

	// 广播新节点上线
	go n.broadcastNodeJoin(req.NodeInfo)

	// 返回成功响应
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

// cleanOldPeerByNameAndAddrUnlocked 清理【同名+同IP】的旧节点（解决脏数据、旧conn、旧地址映射）
func (n *Node) cleanOldPeerByNameAndAddrUnlocked(targetName string, targetAddr string) {
	// 遍历所有在线节点，找到同名+同地址的旧节点，删除
	for uuid, peer := range n.onlinePeers {
		if peer == nil || peer.Id == nil || len(peer.Addrs) == 0 {
			continue
		}

		peerName := peer.Id.Name
		peerAddr := fmt.Sprintf("%s:%d", peer.Addrs[0].Ip, peer.Addrs[0].Port)

		// 同名 + 同地址 → 判定为旧节点，清理
		if peerName == targetName && peerAddr == targetAddr {
			// 🔥 统一清理，保证和Leave/超时清理逻辑完全一致
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
		logger.L().Debug("[server side] recv hb",
			zap.String("node", req.NodeId.Name),
			zap.String("status", req.Status.String()),
		)
	}
	// TODO 太久没心跳的自动断开连接，再加入重连队列

	return &emptypb.Empty{}, nil
}

func (n *Node) Leave(ctx context.Context, nodeID *pb.NodeID) (*emptypb.Empty, error) {
	if nodeID == nil || len(nodeID.Uuid) == 0 {
		return &emptypb.Empty{}, nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	n.cleanPeerResourceUnlocked(nodeID.Uuid)

	logger.L().Info("[server] node leave",
		zap.String("name", nodeID.Name),
		zap.String("uuid", nodeID.Uuid),
	)

	return &emptypb.Empty{}, nil
}

func (n *Node) NotifyNodeJoin(ctx context.Context, newPeer *pb.NodeInfo) (*emptypb.Empty, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if newPeer == nil || newPeer.Id == nil || newPeer.Id.Uuid == "" {
		return &emptypb.Empty{}, nil
	}

	// 跳过自己
	if newPeer.Id.Uuid == n.nodeID.Uuid {
		return &emptypb.Empty{}, nil
	}

	// 核心修复：已存在则更新活跃时间，不再重复添加
	if _, exists := n.onlinePeers[newPeer.Id.Uuid]; exists {
		n.lastActive[newPeer.Id.Uuid] = time.Now()
		return &emptypb.Empty{}, nil
	}

	// 更新本地在线节点列表
	n.onlinePeers[newPeer.Id.Uuid] = newPeer
	n.lastActive[newPeer.Id.Uuid] = time.Now()

	// 👇【关键修复】同步更新【节点名称→地址】映射表
	if len(newPeer.Addrs) > 0 {
		addr := fmt.Sprintf("%s:%d", newPeer.Addrs[0].Ip, newPeer.Addrs[0].Port)
		n.addNameAddrMappingUnlocked(newPeer.Id.Name, addr)
	}
	logger.L().Info("[server] notified new node join",
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
			// 🔥 正确判断 gRPC 正常退出（Canceled），不打印 WARN
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.Canceled {
				// 正常退出（节点Stop、主动关闭），只打Debug，不报警告
				logger.L().Debug("[stream] cosed successfully")
			} else {
				// 真正异常才打印 warn
				logger.L().Warn("[stream] closed unexpected", zap.Error(err))
			}
			return err
		}

		if msg == nil { // 额外防御空指针
			continue
		}

		if msg.Type == pb.MessageType_MSG_TEXT {
			content := msg.GetText()
			logger.L().Info("[server side] recv msg",
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
				logger.L().Error("[server side] reply failed", zap.Error(err))
				return err
			}
		}
	}
}
