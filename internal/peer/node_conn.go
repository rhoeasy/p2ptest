package peer

import (
	"context"
	"fmt"
	"p2ptest/internal/logger"
	pb "p2ptest/proto"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func (n *Node) SetPeerConn(addr string, conn *grpc.ClientConn) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peerConns[addr] = conn
}

func (n *Node) DeletePeerConn(addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if conn, exists := n.peerConns[addr]; exists {
		conn.Close() // 关闭连接
		delete(n.peerConns, addr)
	}
}

func (n *Node) GetPeerConn(addr string) (*grpc.ClientConn, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	conn, exists := n.peerConns[addr]
	return conn, exists
}

// ConnectToPeerStream 主动和目标节点建立双向消息流（发送消息前必调用）
func (n *Node) ConnectToPeerStream(addr string) error {
	// 先看流是否已存在
	n.mu.RLock()
	_, exists := n.peerStreams[addr]
	n.mu.RUnlock()
	if exists {
		return nil
	}

	// 先获取/建立普通gRPC连接
	conn, ok := n.GetPeerConn(addr)
	if !ok {
		dialConn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		n.SetPeerConn(addr, dialConn)
		conn = dialConn
	}

	// 建立双向流
	cli := pb.NewP2PPeerServiceClient(conn)
	stream, err := cli.PeerMessageStream(context.Background())
	if err != nil {
		return err
	}

	// 保存流
	n.SetPeerStream(addr, stream)
	logger.L().Info("[client] 成功建立双向流", zap.String("addr", addr))

	// 启动协程接收对方回复
	go n.recvStreamLoop(addr, stream)

	return nil
}

// recvStreamLoop 循环接收对方发来的消息
func (n *Node) recvStreamLoop(addr string, stream pb.P2PPeerService_PeerMessageStreamClient) {
	for {
		// 👇 用 n.ctx 控制是否退出
		select {
		case <-n.ctx.Done():
			logger.L().Info("流接收协程退出", zap.String("addr", addr))
			return
		default:
		}

		msg, err := stream.Recv()
		if err != nil {
			logger.L().Warn("[client] 流断开", zap.String("addr", addr), zap.Error(err))
			n.DeletePeerStream(addr)
			n.DeletePeerConn(addr)
			return
		}
		logger.L().Info("[client] 收到消息",
			zap.String("from", msg.From.Name),
			zap.String("content", msg.GetText()),
		)
	}
}

// SetPeerStream 设置双向流（给客户端模块用）
func (n *Node) SetPeerStream(addr string, stream pb.P2PPeerService_PeerMessageStreamClient) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peerStreams[addr] = stream
}

// DeletePeerStream 删除双向流（给客户端模块用）
func (n *Node) DeletePeerStream(addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.peerStreams, addr)
}

// closePeerConnByUUIDUnlocked 根据UUID关闭对应节点的gRPC连接和流（无锁）
func (n *Node) closePeerConnByUUIDUnlocked(uuid string) {
	peer, ok := n.onlinePeers[uuid]
	if !ok {
		return
	}
	// 遍历节点地址，关闭对应连接和流
	for _, a := range peer.Addrs {
		addr := fmt.Sprintf("%s:%d", a.Ip, a.Port)

		delete(n.peerStreams, addr)

		// 关闭并删除连接
		if conn, connExists := n.peerConns[addr]; connExists {
			_ = conn.Close()
			delete(n.peerConns, addr)
		}
	}
}
