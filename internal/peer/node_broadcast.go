package peer

import (
	"context"
	"fmt"
	"p2ptest/internal/logger"
	pb "p2ptest/proto"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// broadcastNodeJoin 向所有在线节点广播「新节点加入」通知
func (n *Node) broadcastNodeJoin(newNode *pb.NodeInfo) {

	select {
	case <-n.ctx.Done():
		return
	default:
	}

	// 短时间锁：只复制列表，不阻塞其他操作
	n.mu.RLock()
	allPeers := make([]*pb.NodeInfo, 0, len(n.onlinePeers))
	for _, p := range n.onlinePeers {
		allPeers = append(allPeers, p)
	}
	n.mu.RUnlock()

	for _, p := range allPeers {

		select {
		case <-n.ctx.Done():
			return
		default:
		}

		// 跳过自己 & 刚加入的节点
		if p.Id.Uuid == n.nodeID.Uuid || p.Id.Uuid == newNode.Id.Uuid {
			continue
		}
		if len(p.Addrs) == 0 {
			continue
		}

		addr := fmt.Sprintf("%s:%d", p.Addrs[0].Ip, p.Addrs[0].Port)
		go n.notifySinglePeerJoinWithCtx(addr, newNode)
	}
}

// notifySinglePeerJoinWithCtx 封装通知逻辑，自带 ctx 管控
func (n *Node) notifySinglePeerJoinWithCtx(peerAddr string, newNode *pb.NodeInfo) {
	// 🔥 从全局根 ctx 派生，节点停止自动取消
	ctx, cancel := context.WithTimeout(n.ctx, 3*time.Second)
	defer cancel()

	// 直接复用你原有逻辑
	conn, ok := n.GetPeerConn(peerAddr)
	if !ok {
		dialConn, err := grpc.DialContext(ctx, peerAddr, grpc.WithInsecure())
		if err != nil {
			logger.L().Error("[client] dial peer failed",
				zap.String("addr", peerAddr), zap.Error(err))
			return
		}
		n.SetPeerConn(peerAddr, dialConn)
		conn = dialConn
	}

	cli := pb.NewP2PPeerServiceClient(conn)
	_, err := cli.NotifyNodeJoin(ctx, newNode)
	if err != nil {
		logger.L().Error("[client] notify node join failed",
			zap.String("addr", peerAddr), zap.Error(err))
		return
	}

	logger.L().Info("[client] notify peer success",
		zap.String("to_addr", peerAddr),
		zap.String("new_node", newNode.Id.Name))
}

// broadcastNodeLeave 广播节点下线
func (n *Node) broadcastNodeLeave(nodeID *pb.NodeID) {
	n.mu.RLock()
	allPeers := make([]*pb.NodeInfo, 0, len(n.onlinePeers))
	for _, p := range n.onlinePeers {
		allPeers = append(allPeers, p)
	}
	n.mu.RUnlock()

	for _, p := range allPeers {
		if p.Id.Uuid == n.nodeID.Uuid || p.Id.Uuid == nodeID.Uuid {
			continue
		}
		if len(p.Addrs) == 0 {
			continue
		}
		addr := fmt.Sprintf("%s:%d", p.Addrs[0].Ip, p.Addrs[0].Port)

		go func(peerAddr string, leaveID *pb.NodeID) {
			ctx, cancel := context.WithTimeout(n.ctx, 3*time.Second)
			defer cancel()

			conn, ok := n.GetPeerConn(peerAddr)
			if !ok {
				dialConn, err := grpc.DialContext(ctx, peerAddr, grpc.WithInsecure())
				if err != nil {
					return
				}
				n.SetPeerConn(peerAddr, dialConn)
				conn = dialConn
			}

			cli := pb.NewP2PPeerServiceClient(conn)
			_, _ = cli.Leave(ctx, leaveID)
		}(addr, nodeID)
	}
}
