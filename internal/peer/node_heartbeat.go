package peer

import (
	"context"
	"fmt"
	"p2ptest/internal/logger"
	"p2ptest/internal/types"
	pb "p2ptest/proto"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
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
			case <-n.ctx.Done(): // 👇 新增：ctx 退出
				logger.L().Info("心跳发送任务因 ctx 停止")
				return
			}
		}
	}()
}

func (n *Node) sendHeartbeatToAllPeers() {
	n.mu.RLock()
	defer n.mu.RUnlock()

	// 按地址去重：同一个地址只发一次
	sentAddr := make(map[string]struct{})

	for _, p := range n.onlinePeers {
		if p.Id.Uuid == n.nodeID.Uuid || len(p.Addrs) == 0 {
			continue
		}

		addr := fmt.Sprintf("%s:%d", p.Addrs[0].Ip, p.Addrs[0].Port)
		if _, ok := sentAddr[addr]; ok {
			continue
		}
		sentAddr[addr] = struct{}{}

		go func(addr string) {
			ctx, cancel := context.WithTimeout(n.ctx, 3*time.Second)
			defer cancel()

			conn, ok := n.GetPeerConn(addr)
			if !ok {
				dial, err := grpc.DialContext(ctx, addr, grpc.WithInsecure())
				if err != nil {
					return
				}
				n.SetPeerConn(addr, dial)
				conn = dial
			}

			cli := pb.NewP2PPeerServiceClient(conn)
			_, _ = cli.SendHeartbeat(ctx, &pb.HeartbeatReq{
				NodeId: n.nodeID,
				Status: pb.NodeStatus_NODE_STATUS_ONLINE,
			})
		}(addr)
	}
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
				logger.L().Info("节点清理任务因 ctx 停止")
				return
			case <-n.ctx.Done(): // 👇 新增：ctx 退出
				logger.L().Info("心跳发送任务因 ctx 停止")
				return
			}
		}
	}()
}

// cleanTimeoutPeers 清理超时节点
func (n *Node) cleanTimeoutPeers() {
	n.mu.Lock()
	defer n.mu.Unlock()

	now := time.Now()
	// 🔥 修复：遍历【在线节点列表】，而不是遍历心跳时间
	for uuid := range n.onlinePeers {
		last, exists := n.lastActive[uuid]
		// 不存在活跃时间 或 超时 → 移除
		if !exists || now.Sub(last) > types.HeartbeatTimeout {
			n.cleanNameAddrByUUIDUnlocked(uuid)
			n.closePeerConnByUUIDUnlocked(uuid)
			delete(n.onlinePeers, uuid)
			delete(n.lastActive, uuid)
			logger.L().Warn("节点心跳超时/无活跃时间，已移除", zap.String("uuid", uuid))
		}
	}
}
