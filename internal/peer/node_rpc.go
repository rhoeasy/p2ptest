package peer

import (
	"context"
	"fmt"
	"p2ptest/internal/logger"
	pb "p2ptest/proto"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ========== 服务端方法实现（对外提供的RPC接口） ==========
// Join 处理其他节点的加入请求，就是把自己的peer返回给请求节点，让它自己去建立连接
func (n *Node) Join(ctx context.Context, req *pb.JoinReq) (*pb.JoinResp, error) {
	n.mu.Lock()

	nodeID := req.NodeInfo.Id
	logger.L().Info("[server] 收到Join请求",
		zap.String("node_name", nodeID.Name),
		zap.String("uuid", nodeID.Uuid),
		zap.String("addr", fmt.Sprintf("%s:%d", req.NodeInfo.Addrs[0].Ip, req.NodeInfo.Addrs[0].Port)),
	)
	if _, exists := n.onlinePeers[nodeID.Uuid]; exists {
		return &pb.JoinResp{
			Success: false,
			Error: &pb.ErrorResp{
				Code: pb.ErrorCode_ERR_NODE_EXIST,
				Msg:  fmt.Sprintf("node %v(%v) exist", req.NodeInfo.Id.Name, nodeID.Uuid),
			},
			ProtoVersion: n.cfg.ProtoVer,
		}, nil
	}

	// 记录节点信息
	n.onlinePeers[nodeID.Uuid] = req.NodeInfo
	n.lastActive[nodeID.Uuid] = time.Now()
	n.mu.Unlock()

	logger.L().Info("[server side] node successfully join p2p",
		zap.String("node_name", req.NodeInfo.Id.Name),
		zap.String("uuid", nodeID.Uuid),
	)

	peers := n.getPeerList()

	// ==============================================
	// 👇 新增：广播新节点上线给【所有已经在线的节点】
	// ==============================================
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

	delete(n.lastActive, nodeID.Uuid)
	n.cleanNameAddrByUUIDUnlocked(nodeID.Uuid)
	delete(n.onlinePeers, nodeID.Uuid)

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
			// 客户端正常断开/流取消，只打日志并退出，不panic
			logger.L().Warn("[server side] stream closed", zap.Error(err))
			return err // 关键：出错直接退出，不继续执行
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
