package peer

import (
	"context"
	"p2ptest/internal/logger"
	pb "p2ptest/proto"
	"time"

	"go.uber.org/zap"
)

// broadcastNodeJoin 发送节点上线广播（无锁嵌套，安全）
func (n *Node) broadcastNodeJoin(newNode *pb.NodeInfo) {
	select {
	case <-n.ctx.Done():
		return
	default:
	}

	// 1. 短读锁：只拷贝数据，立刻释放
	n.mu.RLock()
	allPeers := make([]*pb.NodeInfo, 0, len(n.onlinePeers))
	for _, p := range n.onlinePeers {
		allPeers = append(allPeers, p)
	}
	n.mu.RUnlock()
	// 🔥 锁已完全释放，下面不会产生锁竞争

	for _, p := range allPeers {
		select {
		case <-n.ctx.Done():
			return
		default:
		}

		if p.Id == nil || newNode.Id == nil {
			continue
		}
		selfID := n.nodeID.Uuid
		currPID := p.Id.Uuid
		newPID := newNode.Id.Uuid

		if currPID == selfID || currPID == newPID {
			continue
		}

		// 用统一地址函数，不手写拼接
		addr, err := getPeerFirstAddr(p)
		if err != nil {
			continue
		}

		// 异步执行，不持有任何锁
		go n.notifySinglePeerJoin(addr, newNode)
	}
}

func (n *Node) notifySinglePeerJoin(peerAddr string, newNode *pb.NodeInfo) {
	if newNode == nil || newNode.Id == nil {
		return
	}

	ctx, cancel := context.WithTimeout(n.ctx, 3*time.Second)
	defer cancel()

	// 🔥 此时没有任何锁，GetOrCreatePeerClient 加写锁完全安全
	cli, err := n.GetOrCreatePeerClient(ctx, peerAddr)
	if err != nil {
		logger.L().Error("[client] get peer client failed",
			zap.String("addr", peerAddr), zap.Error(err))
		return
	}

	_, err = cli.NotifyNodeJoin(ctx, newNode)
	if err != nil {
		logger.L().Error("[client] notify node join failed",
			zap.String("addr", peerAddr), zap.Error(err))
		return
	}

	logger.L().Info("[client] notify node join success",
		zap.String("to_addr", peerAddr),
		zap.String("new_node_name", newNode.Id.Name))
}

// broadcastNodeLeave 发送节点下线广播（无锁嵌套，安全）
func (n *Node) broadcastNodeLeave(nodeID *pb.NodeID) {
	if nodeID == nil || nodeID.Uuid == "" {
		return
	}

	// 1. 短读锁：只拷贝，立刻释放
	n.mu.RLock()
	allPeers := make([]*pb.NodeInfo, 0, len(n.onlinePeers))
	for _, p := range n.onlinePeers {
		allPeers = append(allPeers, p)
	}
	n.mu.RUnlock()
	// 🔥 锁已完全释放

	for _, p := range allPeers {
		if p.Id == nil {
			continue
		}
		selfID := n.nodeID.Uuid
		targetID := nodeID.Uuid
		currPID := p.Id.Uuid

		if currPID == selfID || currPID == targetID {
			continue
		}

		addr, err := getPeerFirstAddr(p)
		if err != nil {
			continue
		}

		// 异步 + 无锁执行
		go func(addr string, id *pb.NodeID) {
			ctx, cancel := context.WithTimeout(n.ctx, 3*time.Second)
			defer cancel()

			// 无锁状态下创建client，绝对不死锁
			cli, err := n.GetOrCreatePeerClient(ctx, addr)
			if err != nil {
				return
			}
			_, _ = cli.Leave(ctx, id)
		}(addr, nodeID)
	}
}
