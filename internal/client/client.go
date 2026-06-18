package client

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"p2ptest/internal/crypto"
	"p2ptest/internal/grpcutil"
	"p2ptest/internal/logger"
	"p2ptest/internal/notifier"
	pb "p2ptest/proto/p2p"
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

		if err := connectToSinglePeer(n, peerAddr, peerName, peerUUID); err != nil {
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

func connectToSinglePeer(n PeerNode, peerAddr, peerName, peerUUID string) error {
	if n.HasStream(peerAddr) {
		logger.L().Info("[client] already connected, skipping", zap.String("addr", peerAddr))
		return nil
	}

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
	go recvPeerMessageLoop(n, peerAddr, peerName, peerUUID, stream)

	return nil
}

func buildHandshakeReq(n PeerNode) *pb.HandshakeReq {
	selfInfo := &pb.NodeInfo{
		Id: n.GetNodeID(),
		Addrs: []*pb.NodeAddr{
			{Ip: n.Cfg().ListenIP, Port: n.Cfg().ListenPort},
		},
		Status: pb.NodeStatus_ONLINE,
	}
	sigData := crypto.HandshakeSignData(n.GetNodeID().Uuid)
	return &pb.HandshakeReq{
		Self:         selfInfo,
		ProtoVersion: n.Cfg().ProtoVer,
		Signature:    n.Sign(sigData),
	}
}

func recvPeerMessageLoop(n PeerNode, peerAddr, peerName, peerUUID string, stream pb.Messaging_StreamClient) {
	for {
		env, err := stream.Recv()
		if err != nil {
			logger.L().Warn("[client] recv msg failed, peer disconnected",
				zap.String("name", peerName), zap.String("addr", peerAddr), zap.Error(err))

			if removed, _ := n.RemoveOnlinePeer(peerUUID); removed {
				if ntfr := n.Notifier(); ntfr != nil {
					ntfr.Emit(notifier.NewPeerOfflineNotification(peerName, "stream disconnected"))
				}
			}

			n.DeletePeerConn(peerAddr)
			n.DeletePeerStream(peerAddr)
			return
		}

		if textMsg, ok := env.Payload.(*pb.Envelope_Text); ok {
			logger.L().Info("[client] recv msg",
				zap.String("name", peerName),
				zap.String("addr", peerAddr),
				zap.String("text", textMsg.Text.Content))

			// Emit notification with peer name (not IP:port)
			if ntfr := n.Notifier(); ntfr != nil {
				ntfr.Emit(notifier.NewMessageReceivedNotification(peerName, textMsg.Text.Content))
			}
		}

		if pongMsg, ok := env.Payload.(*pb.Envelope_Pong); ok {
			if env.From != nil {
				logger.L().Debug("[client] recv pong", zap.String("name", env.From.Name))
			}
			if pong := pongMsg.Pong; pong != nil {
				n.HandlePongReceived(string(pong.Nonce), pong.PingTimestamp)
			}
		}
	}
}

// SendTextMessage 发送文本消息。
func SendTextMessage(n PeerNode, targetAddr string, content string) error {
	if !n.HasStream(targetAddr) {
		return fmt.Errorf("no stream to %s", targetAddr)
	}

	payload := &pb.Envelope_Text{
		Text: &pb.TextMessage{Content: content},
	}
	payloadBytes, err := proto.Marshal(payload.Text)
	if err != nil {
		return fmt.Errorf("marshal payload failed: %w", err)
	}
	contentHash := sha256.Sum256(payloadBytes)

	env := &pb.Envelope{
		MsgId:       generateMsgID(),
		From:        n.GetNodeID(),
		Timestamp:   uint64(time.Now().UnixMilli()),
		Payload:     payload,
		ContentHash: contentHash[:],
		Signature:   n.Sign(contentHash[:]),
	}

	if err := n.SendToStream(targetAddr, env); err != nil {
		return fmt.Errorf("send msg failed: %w", err)
	}

	logger.L().Info("[client] send msg", zap.String("addr", targetAddr), zap.String("content", content))
	return nil
}

// BroadcastTextMessage 向所有在线 peer 广播文本消息，返回成功数和失败数。
func BroadcastTextMessage(n PeerNode, content string) (int, int) {
	addrs := n.StreamAddrs()
	success := 0
	failed := 0

	for _, addr := range addrs {
		payload := &pb.Envelope_Text{
			Text: &pb.TextMessage{Content: content},
		}
		payloadBytes, err := proto.Marshal(payload.Text)
		if err != nil {
			logger.L().Warn("[client] broadcast marshal failed",
				zap.String("addr", addr), zap.Error(err))
			failed++
			continue
		}
		contentHash := sha256.Sum256(payloadBytes)

		env := &pb.Envelope{
			MsgId:       generateMsgID(),
			From:        n.GetNodeID(),
			Timestamp:   uint64(time.Now().UnixMilli()),
			Payload:     payload,
			ContentHash: contentHash[:],
			Signature:   n.Sign(contentHash[:]),
		}

		if err := n.SendToStream(addr, env); err != nil {
			logger.L().Warn("[client] broadcast failed",
				zap.String("addr", addr), zap.Error(err))
			failed++
		} else {
			success++
			logger.L().Info("[client] broadcast sent",
				zap.String("addr", addr), zap.String("content", content))
		}
	}

	return success, failed
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
