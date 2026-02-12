package client

import (
	"context"
	"fmt"
	"log"
	"p2ptest/internal/grpcutil"
	"p2ptest/internal/peer"
	"p2ptest/internal/types"
	"time"

	pb "p2ptest/proto"
)

// JoinSeedNode send Join request to seed node and get peer list only
func JoinSeedNode(seedAddr string, node *peer.Node) ([]*pb.NodeInfo, error) {
	conn, err := grpcutil.NewClientConn(context.Background(), seedAddr)
	if err != nil {
		return nil, fmt.Errorf("connect to seed %s failed: %v", seedAddr, err)
	}
	defer conn.Close()

	log.Printf("[client] connected to seed %s", seedAddr)

	// RPC 超时控制
	rpcCtx, rpcCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer rpcCancel()

	cli := pb.NewP2PPeerServiceClient(conn)
	joinReq := buildJoinReq(node)

	log.Printf("[client-debug] Join request: name=%s, uuid=%s, addr=%s:%d",
		joinReq.NodeInfo.Id.Name, joinReq.NodeInfo.Id.Uuid,
		joinReq.NodeInfo.Addrs[0].Ip, joinReq.NodeInfo.Addrs[0].Port)

	resp, err := cli.Join(rpcCtx, joinReq)
	if err != nil {
		return nil, fmt.Errorf("call Join RPC failed: %v", err)
	}

	if !resp.Success {
		return nil, fmt.Errorf("Join failed: %s (code: %d)", resp.Error.Msg, resp.Error.Code)
	}

	log.Printf("[client] got %d online peers from seed %s", len(resp.Peers), seedAddr)
	for i, p := range resp.Peers {
		addrStr := ""
		if len(p.Addrs) > 0 {
			addrStr = fmt.Sprintf("%s:%d", p.Addrs[0].Ip, p.Addrs[0].Port)
		}
		log.Printf("[client] Peer[%d] name: %s | uuid: %s | addr: %s",
			i+1, p.Id.Name, p.Id.Uuid, addrStr)
	}

	return resp.Peers, nil
}

// ConnectToPeers connect to peers in list and save connections
func ConnectToPeers(node *peer.Node, peers []*pb.NodeInfo) error {
	var errMsg string
	log.Printf("[client] peers to connect: %+v", peers)

	selfAddr := fmt.Sprintf("%s:%d", node.Cfg().ListenIP, node.Cfg().ListenPort)

	for _, p := range peers {
		peerUUID := p.Id.Uuid
		peerName := p.Id.Name
		peerAddr := ""
		if len(p.Addrs) > 0 {
			peerAddr = fmt.Sprintf("%s:%d", p.Addrs[0].Ip, p.Addrs[0].Port)
		}

		log.Printf("[client] process peer: name=%s, uuid=%s, addr=%s", peerName, peerUUID, peerAddr)

		if peerAddr == selfAddr {
			log.Printf("skip self connection by addr: %s", peerAddr)
			continue
		}
		if p.Id.Uuid == node.GetNodeID().Uuid {
			log.Printf("skip self connection by uuid: %s", peerUUID)
			continue
		}

		if peerAddr == "" {
			errMsg += fmt.Sprintf("peer %s has no valid addr; ", peerName)
			continue
		}

		if err := connectToSinglePeer(node, peerAddr); err != nil {
			errMsg += fmt.Sprintf("connect peer %s(%s) failed: %v; ", peerName, peerAddr, err)
			continue
		}
		node.AddOnlinePeer(p)
		log.Printf("[client] connect peer %s(%s) success", peerName, peerAddr)
	}

	if errMsg != "" {
		return fmt.Errorf("some peers connect failed: %s", errMsg)
	}
	return nil
}

func connectToSinglePeer(node *peer.Node, peerAddr string) error {
	conn, err := grpcutil.NewClientConn(context.Background(), peerAddr)
	if err != nil {
		return fmt.Errorf("connect failed: %w", err)
	}

	node.SetPeerConn(peerAddr, conn)

	stream, err := grpcutil.ConnectPeerStream(context.Background(), conn)
	if err != nil {
		node.DeletePeerConn(peerAddr)
		conn.Close()
		return fmt.Errorf("create stream failed: %w", err)
	}

	node.SetPeerStream(peerAddr, stream)
	go recvPeerMessageLoop(node, peerAddr, stream)

	return nil
}

func buildJoinReq(node *peer.Node) *pb.JoinReq {
	return &pb.JoinReq{
		NodeInfo: &pb.NodeInfo{
			Id: node.GetNodeID(),
			Addrs: []*pb.NodeAddr{
				{
					Ip:       node.Cfg().ListenIP,
					Port:     node.Cfg().ListenPort,
					NatType:  pb.NatType_NAT_UNKNOWN,
					IsPublic: true,
					Protocol: "tcp",
				},
			},
			Status:            pb.NodeStatus_NODE_STATUS_ONLINE,
			HeartbeatInterval: types.HeartbeatInterval,
			Version:           types.DefaultNodeVer,
			LastActive:        peer.ToPbTimestamp(),
		},
		ProtoVersion: types.DefaultProtoVer,
		Signature:    []byte{},
	}
}

func recvPeerMessageLoop(node *peer.Node, peerAddr string, stream pb.P2PPeerService_PeerMessageStreamClient) {
	for {
		msg, err := stream.Recv()
		if err != nil {
			log.Printf("[client] recv msg from %s failed: %v", peerAddr, err)
			node.DeletePeerConn(peerAddr)
			node.DeletePeerStream(peerAddr)
			return
		}

		if msg.Type == pb.MessageType_MSG_TEXT {
			log.Printf("[client] recv from %s: %s", peerAddr, msg.GetText())
		}
	}
}

func SendTextMessage(node *peer.Node, targetAddr string, content string) error {
	if err := node.ConnectToPeerStream(targetAddr); err != nil {
		return err
	}

	streams := node.GetPeerStreams()
	stream, exists := streams[targetAddr]
	if !exists {
		return fmt.Errorf("no stream to %s", targetAddr)
	}

	msg := &pb.P2PMessage{
		MsgId:        peer.GenMsgID(),
		Type:         pb.MessageType_MSG_TEXT,
		From:         node.GetNodeID(),
		ProtoVersion: types.DefaultProtoVer,
		SendTime:     peer.ToPbTimestamp(),
		Content: &pb.P2PMessage_Text{
			Text: content,
		},
		ContentHash: []byte{},
		Signature:   []byte{},
	}

	if err := stream.Send(msg); err != nil {
		return fmt.Errorf("send msg failed: %v", err)
	}

	log.Printf("[client] send to %s: %s", targetAddr, content)
	return nil
}

// JoinAndConnect join seed node and connect to all returned peers
func JoinAndConnect(node *peer.Node, seedIP string, seedPort uint32) error {
	seedAddr := fmt.Sprintf("%s:%d", seedIP, seedPort)

	peers, err := JoinSeedNode(seedAddr, node)
	if err != nil {
		return fmt.Errorf("join seed failed: %v", err)
	}

	if err := ConnectToPeers(node, peers); err != nil {
		return fmt.Errorf("connect peers failed: %v", err)
	}

	return nil
}
