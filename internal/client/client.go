package client

import (
	"context"
	"fmt"
	"log"
	"p2ptest/internal/peer"
	"p2ptest/internal/types"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"

	pb "p2ptest/proto"
)

// JoinSeedNode 向种子节点发送Join请求，仅获取Peer列表（不建立连接）
func JoinSeedNode(seedAddr string, node *peer.Node) ([]*pb.NodeInfo, error) {
	// ========== 1. 拆分ctx：连接等待和RPC调用分开 ==========
	// 连接等待的ctx（10秒，只用于连接）
	connCtx, connCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer connCancel()

	// RPC调用的ctx（20秒，独立于连接，确保有足够时间）
	rpcCtx, rpcCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer rpcCancel()

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(resolver.Get("passthrough")),
		grpc.WithBlock(), // 低版本兼容，强制连接阻塞
	}

	// 2. 创建ClientConn
	conn, err := grpc.NewClient(seedAddr, opts...)
	if err != nil {
		return nil, fmt.Errorf("创建种子节点%s连接失败: %v", seedAddr, err)
	}
	defer conn.Close()

	// 3. 触发连接 + 循环等待就绪（只用connCtx）
	log.Printf("[client-debug] 触发连接种子节点%s，当前状态: %s", seedAddr, conn.GetState().String())
	conn.Connect()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	var connReady bool
	for {
		select {
		case <-connCtx.Done():
			return nil, fmt.Errorf("连接种子节点%s超时（10秒），最终状态: %s，错误: %v",
				seedAddr, conn.GetState().String(), connCtx.Err())
		case <-ticker.C:
			state := conn.GetState()
			log.Printf("[client-debug] 连接状态轮询: %s", state.String())

			if state == connectivity.Ready {
				connReady = true
				break
			}
			if state == connectivity.TransientFailure {
				return nil, fmt.Errorf("连接种子节点%s失败，状态: TransientFailure", seedAddr)
			}
		}
		if connReady {
			break
		}
	}

	log.Printf("[client-debug] 种子节点%s连接就绪，开始调用Join RPC", seedAddr)

	// ========== 4. RPC调用用独立的rpcCtx（20秒超时） ==========
	cli := pb.NewP2PPeerServiceClient(conn)
	joinReq := buildJoinReq(node)

	// 新增：打印请求内容，确认参数正确
	log.Printf("[client-debug] Join请求参数: 节点名称=%s, UUID=%s, 地址=%s:%d",
		joinReq.NodeInfo.Id.Name, joinReq.NodeInfo.Id.Uuid,
		joinReq.NodeInfo.Addrs[0].Ip, joinReq.NodeInfo.Addrs[0].Port)

	// 调用Join（用rpcCtx，而非connCtx）
	resp, err := cli.Join(rpcCtx, joinReq)
	if err != nil {
		return nil, fmt.Errorf("发送Join请求失败: %v", err)
	}

	if !resp.Success {
		return nil, fmt.Errorf("Join失败: %s（错误码：%d）", resp.Error.Msg, resp.Error.Code)
	}

	// 打印Peer列表
	log.Printf("[client] 从种子节点%s获取到%d个在线Peer", seedAddr, len(resp.Peers))
	for i, p := range resp.Peers {
		log.Printf("[client] Peer[%d] | 名称: %s | UUID: %s | 地址: %s:%d",
			i+1, p.Id.Name, p.Id.Uuid, p.Addrs[0].Ip, p.Addrs[0].Port)
	}

	return resp.Peers, nil
}

// ConnectToPeers 拿到Peer列表后，批量建立连接并保存
func ConnectToPeers(node *peer.Node, peers []*pb.NodeInfo) error {
	var errMsg string
	// 1. 打印拿到的Peer列表详情（关键排查日志）
	log.Printf("[client] 待连接的Peer列表详情：%+v", peers)

	// 2. 自己的地址（按地址过滤，更可靠）
	selfAddr := fmt.Sprintf("%s:%d", node.Cfg().ListenIP, node.Cfg().ListenPort)

	for _, p := range peers {
		// 3. 先打印当前Peer的信息（排查用）
		peerUUID := p.Id.Uuid
		peerName := p.Id.Name
		peerAddr := ""
		if len(p.Addrs) > 0 {
			peerAddr = fmt.Sprintf("%s:%d", p.Addrs[0].Ip, p.Addrs[0].Port)
		}
		log.Printf("[client] 处理Peer：name=%s, uuid=%s, addr=%s", peerName, peerUUID, peerAddr)

		// 4. 过滤自身（优先按地址，UUID为辅）
		if peerAddr == selfAddr {
			log.Printf("skip to establish with self (addr: %s)", peerAddr)
			continue
		}
		if p.Id.Uuid == node.GetNodeID().Uuid {
			log.Printf("skip to establish with self (uuid: %s)", peerUUID)
			continue
		}

		// 5. 无有效地址则跳过
		if peerAddr == "" {
			errMsg += fmt.Sprintf("Peer%s无有效地址；", peerName)
			continue
		}

		// 6. 建立连接
		if err := connectToSinglePeer(node, peerAddr); err != nil {
			errMsg += fmt.Sprintf("连接Peer%s(%s)失败: %v；", peerName, peerAddr, err)
			continue
		}
		node.AddOnlinePeer(p)
		log.Printf("[client] 成功连接Peer%s(%s)", peerName, peerAddr)
	}

	if errMsg != "" {
		return fmt.Errorf("部分Peer连接失败: %s", errMsg)
	}
	return nil
}

func connectToSinglePeer(node *peer.Node, peerAddr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 正确类型：[]grpc.DialOption
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(resolver.Get("passthrough")),
	}

	conn, err := grpc.NewClient(peerAddr, opts...)
	if err != nil {
		return fmt.Errorf("创建grpc连接失败: %v", err)
	}

	// 无返回值调用Connect
	conn.Connect()

	// 修复：用connectivity.Idle替代grpc.Idle
	if succ := conn.WaitForStateChange(ctx, connectivity.Idle); !succ {
		conn.Close()
		return fmt.Errorf("grpc连接%s未就绪: %v", peerAddr, err)
	}

	// 保存长期连接到node
	node.SetPeerConn(peerAddr, conn)

	cli := pb.NewP2PPeerServiceClient(conn)
	stream, err := cli.PeerMessageStream(context.Background())
	if err != nil {
		node.DeletePeerConn(peerAddr)
		return fmt.Errorf("建立双向流失败: %v", err)
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
			log.Printf("[client] 从%s接收消息失败: %v", peerAddr, err)
			node.DeletePeerConn(peerAddr)   // 清理连接
			node.DeletePeerStream(peerAddr) // 清理stream
			return
		}

		if msg.Type == pb.MessageType_MSG_TEXT {
			// 修复原代码msg.GetContent()错误，应为msg.GetText()
			log.Printf("[client] 收到%s消息: %s", peerAddr, msg.GetText())
		}
	}
}

func SendTextMessage(node *peer.Node, targetAddr string, content string) error {
	// ======================
	// 👇 自动建立流（这里才是正确位置）
	// ======================
	if err := node.ConnectToPeerStream(targetAddr); err != nil {
		return err
	}

	streams := node.GetPeerStreams()
	stream, exists := streams[targetAddr]
	if !exists {
		return fmt.Errorf("haven't established connection with %v", targetAddr)
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
		return fmt.Errorf("failed to send msg, err: %v", err)
	}

	log.Printf("[client side] send msg to %s: %s", targetAddr, content)

	return nil
}

// JoinAndConnect 向种子节点Join并批量连接返回的Peer（原JoinPeer的替代函数）
func JoinAndConnect(node *peer.Node, seedIP string, seedPort uint32) error {
	seedAddr := fmt.Sprintf("%s:%d", seedIP, seedPort)

	// 1. 第一步：调用Join获取Peer列表
	peers, err := JoinSeedNode(seedAddr, node)
	if err != nil {
		return fmt.Errorf("Join种子节点失败: %v", err)
	}

	// 2. 第二步：批量连接Peer列表
	if err := ConnectToPeers(node, peers); err != nil {
		return fmt.Errorf("批量连接Peer失败: %v", err)
	}

	return nil
}
