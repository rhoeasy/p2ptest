package client

import (
	"context"
	"fmt"
	"time"

	"p2ptest/internal/grpcutil"
	"p2ptest/internal/logger"
	pb "p2ptest/proto/p2p"

	"go.uber.org/zap"
)

// HandshakeSeedNode 与种子节点握手并获取已知节点列表。
func HandshakeSeedNode(seedAddr string, n PeerNode) ([]*pb.NodeInfo, error) {
	conn, err := grpcutil.NewClientConn(context.Background(), seedAddr)
	if err != nil {
		return nil, fmt.Errorf("connect to seed %s failed: %w", seedAddr, err)
	}
	defer conn.Close()

	logger.L().Info("[client] connected to seed", zap.String("addr", seedAddr))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cli := pb.NewMembershipClient(conn)
	req := buildHandshakeReq(n)

	logger.L().Debug("[client] handshake request",
		zap.String("name", req.Self.Id.Name),
		zap.String("uuid", req.Self.Id.Uuid))

	resp, err := cli.Handshake(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("call Handshake RPC failed: %w", err)
	}

	if !resp.Accepted {
		return nil, fmt.Errorf("handshake rejected: %s", resp.RejectReason)
	}

	logger.L().Info("[client] handshake accepted",
		zap.Int("known_peers", len(resp.KnownPeers)),
		zap.String("seed", seedAddr))

	if resp.Peer != nil {
		n.AddOnlinePeer(resp.Peer)
	}

	allPeers := resp.KnownPeers
	if resp.Peer != nil {
		allPeers = append(allPeers, resp.Peer)
	}

	return allPeers, nil
}

// ConnectToPeers 连接到 peer 列表中的节点。
func ConnectToPeers(n PeerNode, peers []*pb.NodeInfo) error {
	var errMsg string
	logger.L().Info("[client] peers to connect", zap.Int("count", len(peers)))

	selfAddr := fmt.Sprintf("%s:%d", n.Cfg().ListenIP, n.Cfg().ListenPort)

	for _, p := range peers {
		peerUUID := p.Id.Uuid
		peerName := p.Id.Name
		peerAddr := ""
		if len(p.Addrs) > 0 {
			peerAddr = fmt.Sprintf("%s:%d", p.Addrs[0].Ip, p.Addrs[0].Port)
		}

		logger.L().Info("[client] process peer",
			zap.String("name", peerName), zap.String("uuid", peerUUID), zap.String("addr", peerAddr))

		if peerAddr == selfAddr {
			logger.L().Info("skip self connection by addr", zap.String("addr", peerAddr))
			continue
		}
		if p.Id.Uuid == n.GetNodeID().Uuid {
			logger.L().Info("skip self connection by uuid", zap.String("uuid", peerUUID))
			continue
		}

		if peerAddr == "" {
			errMsg += fmt.Sprintf("peer %s has no valid addr; ", peerName)
			continue
		}

		if err := connectToSinglePeer(n, peerAddr); err != nil {
			errMsg += fmt.Sprintf("connect peer %s(%s) failed: %v; ", peerName, peerAddr, err)
			continue
		}
		n.AddOnlinePeer(p)
		logger.L().Info("[client] connect peer success", zap.String("name", peerName), zap.String("addr", peerAddr))
	}

	if errMsg != "" {
		return fmt.Errorf("some peers connect failed: %s", errMsg)
	}
	return nil
}

func connectToSinglePeer(n ConnPoolManager, peerAddr string) error {
	conn, err := grpcutil.NewClientConn(context.Background(), peerAddr)
	if err != nil {
		return fmt.Errorf("connect failed: %w", err)
	}

	n.SetPeerConn(peerAddr, conn)

	cli := pb.NewMessagingClient(conn)
	stream, err := cli.Stream(context.Background())
	if err != nil {
		n.DeletePeerConn(peerAddr)
		conn.Close()
		return fmt.Errorf("create stream failed: %w", err)
	}

	n.SetPeerStream(peerAddr, stream)
	go recvPeerMessageLoop(n, peerAddr, stream)

	return nil
}

func buildHandshakeReq(n NodeIdentity) *pb.HandshakeReq {
	selfInfo := &pb.NodeInfo{
		Id: n.GetNodeID(),
		Addrs: []*pb.NodeAddr{
			{Ip: n.Cfg().ListenIP, Port: n.Cfg().ListenPort},
		},
		Status: pb.NodeStatus_ONLINE,
	}
	return &pb.HandshakeReq{
		Self:         selfInfo,
		ProtoVersion: n.Cfg().ProtoVer,
	}
}

func recvPeerMessageLoop(n ConnPoolManager, peerAddr string, stream pb.Messaging_StreamClient) {
	for {
		env, err := stream.Recv()
		if err != nil {
			logger.L().Warn("[client] recv msg failed", zap.String("addr", peerAddr), zap.Error(err))
			n.DeletePeerConn(peerAddr)
			n.DeletePeerStream(peerAddr)
			return
		}

		if textMsg, ok := env.Payload.(*pb.Envelope_Text); ok {
			logger.L().Info("[client] recv msg",
				zap.String("addr", peerAddr),
				zap.String("text", textMsg.Text.Content))
		}
	}
}

// SendTextMessage 发送文本消息。
func SendTextMessage(n PeerNode, targetAddr string, content string) error {
	streams := n.GetPeerStreams()
	stream, exists := streams[targetAddr]
	if !exists {
		return fmt.Errorf("no stream to %s", targetAddr)
	}

	env := &pb.Envelope{
		MsgId:     generateMsgID(),
		From:      n.GetNodeID(),
		Timestamp: uint64(time.Now().UnixMilli()),
		Payload: &pb.Envelope_Text{
			Text: &pb.TextMessage{Content: content},
		},
	}

	if err := stream.Send(env); err != nil {
		return fmt.Errorf("send msg failed: %w", err)
	}

	logger.L().Info("[client] send msg", zap.String("addr", targetAddr), zap.String("content", content))
	return nil
}

// HandshakeAndConnect 与种子节点握手并连接所有返回的节点。
func HandshakeAndConnect(n PeerNode, seedAddr string) error {
	peers, err := HandshakeSeedNode(seedAddr, n)
	if err != nil {
		return fmt.Errorf("handshake seed failed: %w", err)
	}

	if err := ConnectToPeers(n, peers); err != nil {
		return fmt.Errorf("connect peers failed: %w", err)
	}

	return nil
}

func generateMsgID() string {
	return fmt.Sprintf("msg-%d", time.Now().UnixNano())
}
