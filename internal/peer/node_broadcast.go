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
	// 短时间锁：只复制列表，不阻塞其他操作
	n.mu.RLock()
	allPeers := make([]*pb.NodeInfo, 0, len(n.onlinePeers))
	for _, p := range n.onlinePeers {
		allPeers = append(allPeers, p)
	}
	n.mu.RUnlock()

	for _, p := range allPeers {
		// 跳过自己 & 刚加入的节点
		if p.Id.Uuid == n.nodeID.Uuid || p.Id.Uuid == newNode.Id.Uuid {
			continue
		}
		if len(p.Addrs) == 0 {
			continue
		}

		addr := fmt.Sprintf("%s:%d", p.Addrs[0].Ip, p.Addrs[0].Port)

		// 👇 把 ctx 定义丢进协程内部，避免提前 cancel
		go func(peerAddr string, nodeInfo *pb.NodeInfo) {
			// 每个通知都用独立 ctx 做超时控制
			ctx, cancel := context.WithTimeout(n.ctx, 3*time.Second)
			defer cancel()

			n.notifySinglePeerJoin(ctx, peerAddr, nodeInfo)
		}(addr, newNode)
	}
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

// notifySinglePeerJoin 通知**单个**节点有新节点加入
func (n *Node) notifySinglePeerJoin(ctx context.Context, peerAddr string, newNode *pb.NodeInfo) {
	// 1. 获取/复用连接
	conn, ok := n.GetPeerConn(peerAddr)
	if !ok {
		// 用 ctx 控制拨号超时
		dialConn, err := grpc.DialContext(ctx, peerAddr, grpc.WithInsecure())
		if err != nil {
			logger.L().Error("[client] dial peer failed",
				zap.String("addr", peerAddr), zap.Error(err))
			return
		}
		n.SetPeerConn(peerAddr, dialConn)
		conn = dialConn
	}

	// 2. 调用远程 RPC
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
