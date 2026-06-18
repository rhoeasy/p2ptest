package domain

import (
	"fmt"
	"sync"
	"time"

	"p2ptest/internal/types"
	pb "p2ptest/proto/p2p"

	"google.golang.org/protobuf/proto"
)

// PeerRegistry 定义 peer 注册表的行为
type PeerRegistry interface {
	Register(peer *pb.NodeInfo) error
	Unregister(uuid string) (bool, error)
	Get(uuid string) (*pb.NodeInfo, bool)
	GetByName(name string) (*pb.NodeInfo, bool)
	List() []*pb.NodeInfo
	UpdateLastActive(uuid string)
	UpdateStatus(uuid string, status pb.NodeStatus) error
	GetLastActive(uuid string) (time.Time, bool)
	GetRegisteredAt(uuid string) (time.Time, bool)
	GetStale(threshold time.Duration) []string
	GetAddrByName(name string) (string, error)
}

// peerRegistry 是 PeerRegistry 的内存实现
type peerRegistry struct {
	onlinePeers map[string]*pb.NodeInfo
	lastActive  map[string]time.Time
	registeredAt map[string]time.Time
	nameToAddrs map[string][]string
	mu          sync.RWMutex
	selfUUID    string
}

// Verify peerRegistry implements PeerRegistry
var _ PeerRegistry = (*peerRegistry)(nil)

func NewPeerRegistry(selfUUID string) PeerRegistry {
	return &peerRegistry{
		onlinePeers:  make(map[string]*pb.NodeInfo),
		lastActive:   make(map[string]time.Time),
		registeredAt: make(map[string]time.Time),
		nameToAddrs:  make(map[string][]string),
		selfUUID:     selfUUID,
	}
}

func (r *peerRegistry) Register(peer *pb.NodeInfo) error {
	if peer == nil || peer.Id == nil || peer.Id.Uuid == "" {
		return types.ErrInvalidPeerInfo
	}
	if peer.Id.Uuid == r.selfUUID {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	uuid := peer.Id.Uuid
	exists := r.onlinePeers[uuid] != nil

	r.onlinePeers[uuid] = peer
	r.lastActive[uuid] = time.Now()

	if !exists {
		r.registeredAt[uuid] = time.Now()
		addr, err := getPeerFirstAddr(peer)
		if err == nil {
			r.addNameAddrUnlocked(peer.Id.Name, addr)
		}
	}

	return nil
}

func (r *peerRegistry) Unregister(uuid string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	peer, ok := r.onlinePeers[uuid]
	if !ok {
		return false, nil
	}

	r.cleanNameAddrUnlocked(peer)

	delete(r.onlinePeers, uuid)
	delete(r.lastActive, uuid)
	delete(r.registeredAt, uuid)

	return true, nil
}

func (r *peerRegistry) Get(uuid string) (*pb.NodeInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.onlinePeers[uuid]
	if !ok {
		return nil, false
	}
	return proto.Clone(p).(*pb.NodeInfo), true
}

func (r *peerRegistry) GetByName(name string) (*pb.NodeInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, p := range r.onlinePeers {
		if p.Id.Name == name {
			return proto.Clone(p).(*pb.NodeInfo), true
		}
	}
	return nil, false
}

func (r *peerRegistry) GetLastActive(uuid string) (time.Time, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.lastActive[uuid]
	return t, ok
}

func (r *peerRegistry) GetRegisteredAt(uuid string) (time.Time, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.registeredAt[uuid]
	return t, ok
}

func (r *peerRegistry) List() []*pb.NodeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	peers := make([]*pb.NodeInfo, 0, len(r.onlinePeers))
	for _, p := range r.onlinePeers {
		peers = append(peers, proto.Clone(p).(*pb.NodeInfo))
	}
	return peers
}

func (r *peerRegistry) UpdateLastActive(uuid string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.onlinePeers[uuid]; exists {
		r.lastActive[uuid] = time.Now()
	}
}

func (r *peerRegistry) UpdateStatus(uuid string, status pb.NodeStatus) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	peer, exists := r.onlinePeers[uuid]
	if !exists {
		return types.ErrPeerNotFound
	}
	peer.Status = status
	return nil
}

func (r *peerRegistry) GetStale(threshold time.Duration) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	now := time.Now()
	var stale []string
	for uuid := range r.onlinePeers {
		last, exists := r.lastActive[uuid]
		if !exists || now.Sub(last) > threshold {
			stale = append(stale, uuid)
		}
	}
	return stale
}

func (r *peerRegistry) GetAddrByName(name string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	addrs, exists := r.nameToAddrs[name]
	if !exists || len(addrs) == 0 {
		return "", types.ErrPeerNotFound
	}
	return addrs[0], nil
}

func (r *peerRegistry) addNameAddrUnlocked(name string, addr string) {
	for _, existing := range r.nameToAddrs[name] {
		if existing == addr {
			return
		}
	}
	r.nameToAddrs[name] = append(r.nameToAddrs[name], addr)
}

func (r *peerRegistry) cleanNameAddrUnlocked(peer *pb.NodeInfo) {
	name := peer.Id.Name
	oldAddrs := r.nameToAddrs[name]
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
		delete(r.nameToAddrs, name)
	} else {
		r.nameToAddrs[name] = newAddrs
	}
}

func formatNodeAddr(ip string, port uint32) string {
	return fmt.Sprintf("%s:%d", ip, port)
}

func getPeerFirstAddr(peer *pb.NodeInfo) (string, error) {
	if peer == nil || len(peer.Addrs) == 0 {
		return "", types.ErrNoValidNodeAddress
	}
	return formatNodeAddr(peer.Addrs[0].Ip, peer.Addrs[0].Port), nil
}
