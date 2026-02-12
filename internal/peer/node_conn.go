package peer

import (
	"context"
	"p2ptest/internal/grpcutil"
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
		conn.Close()
		delete(n.peerConns, addr)
	}
}

func (n *Node) GetPeerConn(addr string) (*grpc.ClientConn, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	conn, exists := n.peerConns[addr]
	return conn, exists
}

// SetPeerStream 设置双向流
func (n *Node) SetPeerStream(addr string, stream pb.P2PPeerService_PeerMessageStreamClient) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peerStreams[addr] = stream
}

func (n *Node) DeletePeerStream(addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.peerStreams, addr)
}

func (n *Node) ConnectToPeerStream(addr string) error {

	n.mu.RLock()
	exists := n.peerStreams[addr] != nil
	n.mu.RUnlock()
	if exists {
		return nil
	}

	cli, err := n.GetOrCreatePeerClient(context.Background(), addr)
	if err != nil {
		return err
	}

	stream, err := grpcutil.ConnectPeerStream(context.Background(), getConnFromClient(cli))
	if err != nil {
		return err
	}

	n.SetPeerStream(addr, stream)
	logger.L().Info("[client] 成功建立双向流", zap.String("addr", addr))

	// 启动接收协程
	go n.recvStreamLoop(addr, stream)

	return nil
}

// recvStreamLoop 循环接收消息
func (n *Node) recvStreamLoop(addr string, stream pb.P2PPeerService_PeerMessageStreamClient) {
	for {
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

// closePeerConnByUUIDUnlocked 根据UUID关闭连接和流（无锁）
func (n *Node) closePeerConnByUUIDUnlocked(uuid string) {
	conn := n.peerConns[uuid]
	stream := n.peerStreams[uuid]

	// 用节点 ctx 管控生命周期，防止 goroutine 泄露
	go func(ctx context.Context) {
		// 只要节点停止，立刻放弃关闭
		select {
		case <-ctx.Done():
			return
		default:
		}

		if stream != nil {
			_ = stream.CloseSend()
		}
		if conn != nil {
			_ = conn.Close()
		}
	}(n.ctx)
}

// ------------------------------------------------------------------------------
// 🔥 全局唯一获取客户端方法：自动缓存 + 自动重连 + 全项目复用
// ------------------------------------------------------------------------------
func (n *Node) GetOrCreatePeerClient(
	ctx context.Context,
	addr string,
) (pb.P2PPeerServiceClient, error) {
	// 1. 先读缓存
	conn, ok := n.GetPeerConn(addr)
	if ok && conn != nil {
		return pb.NewP2PPeerServiceClient(conn), nil
	}

	// 2. 无缓存 → 用 grpcutil 标准新建连接
	conn, err := grpcutil.NewClientConn(ctx, addr)
	if err != nil {
		return nil, err
	}

	// 3. 写入缓存
	n.SetPeerConn(addr, conn)

	return pb.NewP2PPeerServiceClient(conn), nil
}

// ------------------------------------------------------------------------------
// 🔧 内部小工具：从 client 里取出 conn（辅助用）
// ------------------------------------------------------------------------------
func getConnFromClient(cli pb.P2PPeerServiceClient) *grpc.ClientConn {
	if cc, ok := cli.(interface{ ClientConn() *grpc.ClientConn }); ok {
		return cc.ClientConn()
	}
	return nil
}
