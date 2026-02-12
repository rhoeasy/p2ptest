package peer

import (
	"fmt"
	"p2ptest/internal/logger"
	"p2ptest/internal/types"
	pb "p2ptest/proto"
	"time"

	"go.uber.org/zap"
)

// AddOnlinePeer 主动添加在线节点到本地列表（线程安全）
func (n *Node) AddOnlinePeer(peerInfo *pb.NodeInfo) {
	if peerInfo == nil || peerInfo.Id == nil || peerInfo.Id.Uuid == "" {
		logger.L().Warn("[client] invalid peer info, skip add")
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// 跳过自己
	if peerInfo.Id.Uuid == n.nodeID.Uuid {
		return
	}

	// 核心修复：已存在则更新，不再重复添加
	if _, exists := n.onlinePeers[peerInfo.Id.Uuid]; exists {
		n.onlinePeers[peerInfo.Id.Uuid] = peerInfo // 更新最新信息
		n.lastActive[peerInfo.Id.Uuid] = time.Now()
		logger.L().Info("[client] update peer in local online list",
			zap.String("node_name", peerInfo.Id.Name),
			zap.String("uuid", peerInfo.Id.Uuid))
		return
	}

	// 不存在则新增
	n.onlinePeers[peerInfo.Id.Uuid] = peerInfo
	n.lastActive[peerInfo.Id.Uuid] = time.Now()
	logger.L().Info("[client] add peer to local online list",
		zap.String("node_name", peerInfo.Id.Name),
		zap.String("uuid", peerInfo.Id.Uuid),
		zap.String("addr", fmt.Sprintf("%s:%d", peerInfo.Addrs[0].Ip, peerInfo.Addrs[0].Port)),
	)

	// 同步更新名称映射（取第一个有效地址）
	if len(peerInfo.Addrs) > 0 {
		addr := fmt.Sprintf("%s:%d", peerInfo.Addrs[0].Ip, peerInfo.Addrs[0].Port)
		n.addNameAddrMappingUnlocked(peerInfo.Id.Name, addr) // 调用映射添加方法
	}
}

// GetOnlinePeers 获取所有在线节点（包含自己），返回格式化的节点信息列表
func (n *Node) GetOnlinePeers() []map[string]string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	peersInfo := make([]map[string]string, 0)

	// 1. 先添加自己的信息
	selfInfo := map[string]string{
		"节点名称": n.cfg.NodeName,
		"UUID": n.nodeID.Uuid,
		"监听地址": fmt.Sprintf("%s:%d", n.cfg.ListenIP, n.cfg.ListenPort),
		"节点类型": "本地节点（自身）",
		"状态":   "在线（ONLINE）",
	}
	peersInfo = append(peersInfo, selfInfo)

	// 2. 再添加其他在线节点
	for uuid, peer := range n.onlinePeers {
		// 跳过自己（避免重复）
		if uuid == n.nodeID.Uuid {
			continue
		}
		// 取第一个有效地址
		peerAddr := "无有效地址"
		if len(peer.Addrs) > 0 {
			peerAddr = fmt.Sprintf("%s:%d", peer.Addrs[0].Ip, peer.Addrs[0].Port)
		}
		peerInfo := map[string]string{
			"节点名称": peer.Id.Name,
			"UUID": uuid,
			"监听地址": peerAddr,
			"节点类型": "远程节点（Peer）",
			"状态":   pb.NodeStatus_name[int32(peer.Status)],
		}
		peersInfo = append(peersInfo, peerInfo)
	}

	return peersInfo
}

func (n *Node) getPeerList() []*pb.NodeInfo {
	// 1. 初始化列表，先加自己（seed）
	peerList := []*pb.NodeInfo{
		{
			Id: &pb.NodeID{
				Name: n.cfg.NodeName, // seed
				Uuid: n.nodeID.Uuid,  // seed的唯一UUID
			},
			Addrs: []*pb.NodeAddr{
				{
					Ip:       n.cfg.ListenIP,   // 127.0.0.1
					Port:     n.cfg.ListenPort, // 50051
					NatType:  pb.NatType_NAT_UNKNOWN,
					IsPublic: true,
					Protocol: "tcp",
				},
			},
			Status:            pb.NodeStatus_NODE_STATUS_ONLINE,
			HeartbeatInterval: types.HeartbeatInterval,
			Version:           types.DefaultNodeVer,
			LastActive:        ToPbTimestamp(),
		},
	}

	// 2. 打印自身信息（确认seed自己被加入列表）
	logger.L().Info("[server] getPeerList - 自身信息",
		zap.String("name", n.cfg.NodeName),
		zap.String("uuid", n.nodeID.Uuid),
		zap.String("addr", fmt.Sprintf("%s:%d", n.cfg.ListenIP, n.cfg.ListenPort)),
	)

	// 3. 再加其他在线节点（node2）
	n.mu.RLock()
	defer n.mu.RUnlock()
	for uuid, peer := range n.onlinePeers {
		// 跳过自己（防止重复）
		if uuid == n.nodeID.Uuid {
			continue
		}
		peerList = append(peerList, peer)
		logger.L().Info("[server] getPeerList - 新增在线节点",
			zap.String("name", peer.Id.Name),
			zap.String("uuid", uuid),
		)
	}

	// 4. 打印最终返回的列表（关键排查）
	logger.L().Info("[server] getPeerList - 最终返回Peer数量",
		zap.Int("count", len(peerList)),
	)
	for i, p := range peerList {
		logger.L().Info("[server] getPeerList - Peer详情",
			zap.Int("index", i+1),
			zap.String("name", p.Id.Name),
			zap.String("uuid", p.Id.Uuid),
			zap.String("addr", fmt.Sprintf("%s:%d", p.Addrs[0].Ip, p.Addrs[0].Port)),
		)
	}

	return peerList
}

// AddNameAddrMapping 外部调用的加锁版本
func (n *Node) AddNameAddrMapping(nodeName string, addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.addNameAddrMappingUnlocked(nodeName, addr) // 调用无锁内部方法
}

// addNameAddrMappingUnlocked 内部无锁版本（仅在已加锁时调用）
func (n *Node) addNameAddrMappingUnlocked(nodeName string, addr string) {
	// 去重：避免同一名称重复添加同一地址
	for _, existingAddr := range n.nameToAddrs[nodeName] {
		if existingAddr == addr {
			return
		}
	}
	n.nameToAddrs[nodeName] = append(n.nameToAddrs[nodeName], addr)
	logger.L().Info("新增节点名称映射",
		zap.String("node_name", nodeName),
		zap.String("addr", addr),
	)
}

// GetAddrByName 根据节点名称查找地址（返回第一个可用地址，处理同名）
func (n *Node) GetAddrByName(nodeName string) (string, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	// 1. 先查名称映射
	addrs, exists := n.nameToAddrs[nodeName]
	if !exists || len(addrs) == 0 {
		return "", fmt.Errorf("未找到节点名称「%s」对应的地址", nodeName)
	}

	// 2. 同名节点处理：返回第一个地址，同时提示
	if len(addrs) > 1 {
		logger.L().Warn("找到多个同名节点，使用第一个地址",
			zap.String("node_name", nodeName),
			zap.String("use_addr", addrs[0]),
			zap.Strings("all_addrs", addrs),
		)
	}
	return addrs[0], nil
}

func (n *Node) cleanNameAddrByUUIDUnlocked(uuid string) {
	toDeletePeer, ok := n.onlinePeers[uuid]
	if !ok {
		return
	}
	toDeletePeerName := toDeletePeer.Id.Name
	toDeletePeerAddrs := n.nameToAddrs[toDeletePeerName]

	newAddrs := make([]string, 0, len(toDeletePeerAddrs))
	for _, addr := range toDeletePeerAddrs {
		// 保留不是该节点的地址
		isThisNode := false
		for _, a := range toDeletePeer.Addrs {
			if fmt.Sprintf("%s:%d", a.Ip, a.Port) == addr {
				isThisNode = true
				break
			}
		}
		if !isThisNode {
			newAddrs = append(newAddrs, addr)
		}
	}

	if len(newAddrs) == 0 {
		delete(n.nameToAddrs, toDeletePeerName)
	} else {
		n.nameToAddrs[toDeletePeerName] = newAddrs
	}
}

// cleanPeerResourceUnlocked 统一清理单个节点的所有资源（唯一入口，不漏任何一项）
func (n *Node) cleanPeerResourceUnlocked(uuid string) {
	logger.L().Info("[clean] 真正要删除的UUID", zap.String("target_uuid", uuid))
	// 1. 清理名称→地址映射
	n.cleanNameAddrByUUIDUnlocked(uuid)
	// 2. 关闭并清理gRPC连接、流
	n.closePeerConnByUUIDUnlocked(uuid)
	// 3. 从在线列表移除
	delete(n.onlinePeers, uuid)

	logger.L().Info("[clean] 已从 onlinePeers 删除", zap.String("deleted_uuid", uuid))
	// 4. 移除活跃时间
	delete(n.lastActive, uuid)
}
