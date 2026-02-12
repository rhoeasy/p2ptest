package peer

import (
	"fmt"
	"p2ptest/internal/logger"
	"p2ptest/internal/types"
	pb "p2ptest/proto"
	"time"

	"go.uber.org/zap"
)

// AddOnlinePeer 主动添加/更新在线节点到本地列表（线程安全）
func (n *Node) AddOnlinePeer(peerInfo *pb.NodeInfo) {
	if peerInfo == nil || peerInfo.Id == nil || peerInfo.Id.Uuid == "" {
		logger.L().Warn("[peer] invalid peer info, skip add")
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// 跳过自身
	if peerInfo.Id.Uuid == n.nodeID.Uuid {
		return
	}

	uuid := peerInfo.Id.Uuid
	exists := n.onlinePeers[uuid] != nil

	// 存在则更新，不存在则新增
	n.onlinePeers[uuid] = peerInfo
	n.lastActive[uuid] = time.Now()

	if exists {
		logger.L().Info("[peer] update online peer",
			zap.String("name", peerInfo.Id.Name),
			zap.String("uuid", uuid))
		return
	}

	addr, err := getPeerFirstAddr(peerInfo)
	if err == nil {
		n.addNameAddrMappingUnlocked(peerInfo.Id.Name, addr)
	}

	logger.L().Info("[peer] add online peer",
		zap.String("name", peerInfo.Id.Name),
		zap.String("uuid", uuid),
		zap.String("addr", addr)) // err 不为空时 addr 为空字符串，正常打印
}

// GetOnlinePeers 获取所有在线节点（包含自身），用于控制台展示
func (n *Node) GetOnlinePeers() []map[string]string {
	n.mu.RLock()
	copyPeers := make(map[string]*pb.NodeInfo, len(n.onlinePeers))
	for k, v := range n.onlinePeers {
		copyPeers[k] = v
	}
	selfUUID := n.nodeID.Uuid
	selfCfg := n.cfg
	n.mu.RUnlock()

	var peersInfo []map[string]string

	// 加入自身节点
	selfAddr := formatNodeAddr(selfCfg.ListenIP, selfCfg.ListenPort)
	peersInfo = append(peersInfo, map[string]string{
		"节点名称": selfCfg.NodeName,
		"UUID": selfUUID,
		"监听地址": selfAddr,
		"节点类型": "本地节点（自身）",
		"状态":   "在线（ONLINE）",
	})

	// 遍历拷贝后的map，不持有锁
	for uuid, peer := range copyPeers {
		if uuid == selfUUID {
			continue
		}

		addr, err := getPeerFirstAddr(peer)
		if err != nil {
			addr = ""
		}

		peersInfo = append(peersInfo, map[string]string{
			"节点名称": peer.Id.Name,
			"UUID": uuid,
			"监听地址": addr,
			"节点类型": "远程节点（Peer）",
			"状态":   pb.NodeStatus_name[int32(peer.Status)],
		})
	}

	return peersInfo
}

// getPeerList 获取节点列表（用于Join接口返回，服务端内部使用）
func (n *Node) getPeerList() []*pb.NodeInfo {
	// 🔥 短读锁：只拷贝，立刻释放，绝不占锁
	n.mu.RLock()
	// 拷贝在线节点，避免遍历原map
	copyPeers := make(map[string]*pb.NodeInfo, len(n.onlinePeers))
	for k, v := range n.onlinePeers {
		copyPeers[k] = v
	}
	// 拷贝自身信息
	selfName := n.cfg.NodeName
	selfUUID := n.nodeID.Uuid
	selfIP := n.cfg.ListenIP
	selfPort := n.cfg.ListenPort
	n.mu.RUnlock()
	// 🔥 锁已释放，下面随便写逻辑

	selfAddr := formatNodeAddr(selfIP, selfPort)
	peerList := []*pb.NodeInfo{
		{
			Id: &pb.NodeID{
				Name: selfName,
				Uuid: selfUUID,
			},
			Addrs: []*pb.NodeAddr{{
				Ip:       selfIP,
				Port:     selfPort,
				NatType:  pb.NatType_NAT_UNKNOWN,
				IsPublic: true,
				Protocol: "tcp",
			}},
			Status:            pb.NodeStatus_NODE_STATUS_ONLINE,
			HeartbeatInterval: types.HeartbeatInterval,
			Version:           types.DefaultNodeVer,
			LastActive:        ToPbTimestamp(),
		},
	}

	logger.L().Info("[server] self info in peer list",
		zap.String("name", selfName),
		zap.String("uuid", selfUUID),
		zap.String("addr", selfAddr))

	// 遍历拷贝的map，不加锁
	for uuid, peer := range copyPeers {
		if uuid == selfUUID {
			continue
		}
		peerList = append(peerList, peer)
		logger.L().Info("[server] add peer to list",
			zap.String("name", peer.Id.Name),
			zap.String("uuid", uuid))
	}

	logger.L().Info("[server] total peers in list", zap.Int("count", len(peerList)))
	return peerList
}

// AddNameAddrMapping 对外：添加节点名称→地址映射（线程安全）
func (n *Node) AddNameAddrMapping(nodeName string, addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.addNameAddrMappingUnlocked(nodeName, addr)
}

// addNameAddrMappingUnlocked 内部无锁：维护名称-地址映射（去重）
func (n *Node) addNameAddrMappingUnlocked(nodeName string, addr string) {
	for _, existing := range n.nameToAddrs[nodeName] {
		if existing == addr {
			return
		}
	}
	n.nameToAddrs[nodeName] = append(n.nameToAddrs[nodeName], addr)
	logger.L().Info("[peer] add name-addr mapping",
		zap.String("name", nodeName),
		zap.String("addr", addr))
}

// GetAddrByName 按节点名查地址（自动处理同名节点）
func (n *Node) GetAddrByName(nodeName string) (string, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	addrs, exists := n.nameToAddrs[nodeName]
	if !exists || len(addrs) == 0 {
		return "", fmt.Errorf("node name [%s] not found", nodeName)
	}

	// 同名节点：使用第一个地址
	if len(addrs) > 1 {
		logger.L().Warn("[peer] multiple nodes with same name, use first",
			zap.String("name", nodeName),
			zap.String("use_addr", addrs[0]),
			zap.Strings("all_addrs", addrs))
	}

	return addrs[0], nil
}

// cleanNameAddrByUUIDUnlocked 无锁：按UUID清理名称映射
func (n *Node) cleanNameAddrByUUIDUnlocked(uuid string) {
	peer, ok := n.onlinePeers[uuid]
	if !ok {
		return
	}

	name := peer.Id.Name
	oldAddrs := n.nameToAddrs[name]
	var newAddrs []string

	for _, addr := range oldAddrs {
		keep := true
		for _, a := range peer.Addrs {
			if formatNodeAddr(a.Ip, a.Port) == addr {
				keep = false
				break
			}
		}
		if keep {
			newAddrs = append(newAddrs, addr)
		}
	}

	if len(newAddrs) == 0 {
		delete(n.nameToAddrs, name)
	} else {
		n.nameToAddrs[name] = newAddrs
	}
}

// cleanPeerResourceUnlocked 【全局唯一入口】清理节点所有资源
func (n *Node) cleanPeerResourceUnlocked(uuid string) {
	logger.L().Info("[clean] start clean peer resource", zap.String("uuid", uuid))

	// 1. 清理名称映射
	n.cleanNameAddrByUUIDUnlocked(uuid)
	// 2. 关闭gRPC连接与流
	n.closePeerConnByUUIDUnlocked(uuid)

	delete(n.onlinePeers, uuid)
	delete(n.lastActive, uuid)
	delete(n.peerConns, uuid)
	delete(n.peerStreams, uuid)

	logger.L().Info("[clean] finish clean peer resource", zap.String("uuid", uuid))
}
