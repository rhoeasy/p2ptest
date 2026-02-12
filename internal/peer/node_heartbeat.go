package peer

import (
	"context"
	"p2ptest/internal/logger"
	"p2ptest/internal/types"
	pb "p2ptest/proto"
	"time"

	"go.uber.org/zap"
)

// 全局唯一心跳循环：只许 Start() 调用，不许别处调用
func (n *Node) startHeartbeatLoop() {
	go func() {
		hbInterval := time.Duration(types.HeartbeatInterval) * time.Millisecond
		ticker := time.NewTicker(hbInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				n.sendHeartbeatToAllPeers()
			case <-n.stopChan:
				logger.L().Info("心跳发送任务已停止")
				return
			case <-n.ctx.Done():
				logger.L().Info("心跳发送任务因 ctx 停止")
				return
			}
		}
	}()
}

func (n *Node) sendHeartbeatToAllPeers() {
	// 🔥 短读锁：拷贝 → 立即释放
	n.mu.RLock()
	peersCopy := make([]*pb.NodeInfo, 0, len(n.onlinePeers))
	for _, p := range n.onlinePeers {
		peersCopy = append(peersCopy, p)
	}
	n.mu.RUnlock()

	// 🔥 无锁状态下发送心跳
	for _, p := range peersCopy {
		addr, err := getPeerFirstAddr(p)
		if err != nil {
			continue
		}
		go n.sendHeartbeat(addr)
	}
}

// sendHeartbeat 向单个地址发送心跳（严格匹配proto）
func (n *Node) sendHeartbeat(addr string) {
	ctx, cancel := context.WithTimeout(n.ctx, 2*time.Second)
	defer cancel()

	// 获取gRPC客户端
	cli, err := n.GetOrCreatePeerClient(ctx, addr)
	if err != nil {
		logger.L().Warn("[heartbeat] get client failed",
			zap.String("addr", addr), zap.Error(err))
		return
	}

	req := &pb.HeartbeatReq{
		NodeId:    n.nodeID,
		Status:    pb.NodeStatus_NODE_STATUS_ONLINE,
		Timestamp: uint64(time.Now().UnixMilli()),
		Signature: nil, // 你后面可以加签名逻辑
	}

	_, err = cli.SendHeartbeat(ctx, req)
	if err != nil {
		logger.L().Warn("[heartbeat] send failed",
			zap.String("addr", addr), zap.Error(err))
		return
	}

	// 心跳成功 → 更新活跃时间
	n.mu.Lock()
	n.lastActive[n.nodeID.Uuid] = time.Now()
	n.mu.Unlock()
}

// 只做：超时节点清理
func (n *Node) startPeerCleaner() {
	go func() {
		cleanInterval := time.Duration(types.CleanInterval) * time.Millisecond
		ticker := time.NewTicker(cleanInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				n.cleanTimeoutPeers()
			case <-n.stopChan:
				logger.L().Info("节点清理任务已停止")
				return
			case <-n.ctx.Done():
				logger.L().Info("节点清理任务因 ctx 停止")
				return
			}
		}
	}()
}

// cleanTimeoutPeers 清理超时节点（逻辑不变，无需改动）
func (n *Node) cleanTimeoutPeers() {
	n.mu.Lock()
	defer n.mu.Unlock()

	now := time.Now()
	for uuid := range n.onlinePeers {
		last, exists := n.lastActive[uuid]
		if !exists || now.Sub(last) > types.HeartbeatTimeout {
			n.cleanPeerResourceUnlocked(uuid)
			logger.L().Warn("节点心跳超时/无活跃时间，已移除", zap.String("uuid", uuid))
		}
	}
}
