package client

import (
	"testing"
	"time"

	"p2ptest/internal/logger"
	"p2ptest/internal/notifier"
	"p2ptest/internal/types"
	pb "p2ptest/proto/p2p"

	"google.golang.org/grpc"
)

func init() {
	logger.InitLogger(false)
}

type mockPeerNode struct {
	nodeID       *pb.NodeID
	cfg          *types.NodeConfig
	streams      map[string]pb.Messaging_StreamClient
	conns        map[string]*grpc.ClientConn
	onlinePeers  map[string]*pb.NodeInfo
	notifierImpl *notifier.Notifier
	pendingPings map[string]chan time.Duration
}

func newMockPeerNode(name string, port uint32) *mockPeerNode {
	return &mockPeerNode{
		nodeID: &pb.NodeID{Uuid: "mock-uuid-" + name, Name: name},
		cfg: &types.NodeConfig{
			NodeName:   name,
			ListenIP:   "127.0.0.1",
			ListenPort: port,
			ProtoVer:   types.DefaultProtoVer,
		},
		streams:      make(map[string]pb.Messaging_StreamClient),
		conns:        make(map[string]*grpc.ClientConn),
		onlinePeers:  make(map[string]*pb.NodeInfo),
		notifierImpl: notifier.NewNotifier(10),
		pendingPings: make(map[string]chan time.Duration),
	}
}

func (m *mockPeerNode) GetNodeID() *pb.NodeID       { return m.nodeID }
func (m *mockPeerNode) Cfg() *types.NodeConfig      { return m.cfg }
func (m *mockPeerNode) HasStream(addr string) bool  { _, ok := m.streams[addr]; return ok }
func (m *mockPeerNode) StreamAddrs() []string {
	addrs := make([]string, 0, len(m.streams))
	for addr := range m.streams {
		addrs = append(addrs, addr)
	}
	return addrs
}
func (m *mockPeerNode) SendToStream(addr string, env *pb.Envelope) error { return nil }
func (m *mockPeerNode) SetPeerConn(addr string, conn *grpc.ClientConn)   { m.conns[addr] = conn }
func (m *mockPeerNode) SetPeerStream(addr string, stream pb.Messaging_StreamClient) {
	m.streams[addr] = stream
}
func (m *mockPeerNode) DeletePeerConn(addr string)   { delete(m.conns, addr) }
func (m *mockPeerNode) DeletePeerStream(addr string) { delete(m.streams, addr) }
func (m *mockPeerNode) Notifier() *notifier.Notifier { return m.notifierImpl }
func (m *mockPeerNode) AddOnlinePeer(peer *pb.NodeInfo) error {
	m.onlinePeers[peer.Id.Uuid] = peer
	return nil
}
func (m *mockPeerNode) RemoveOnlinePeer(uuid string) (bool, error) {
	_, ok := m.onlinePeers[uuid]
	delete(m.onlinePeers, uuid)
	return ok, nil
}
func (m *mockPeerNode) RecordPingSent(nonce string) chan time.Duration {
	ch := make(chan time.Duration, 1)
	m.pendingPings[nonce] = ch
	return ch
}
func (m *mockPeerNode) CancelPendingPing(nonce string) {
	delete(m.pendingPings, nonce)
}
func (m *mockPeerNode) HandlePongReceived(nonce string, pingTimestamp uint64) {
	ch, ok := m.pendingPings[nonce]
	if !ok {
		return
	}
	delete(m.pendingPings, nonce)
	rtt := time.Since(time.UnixMilli(int64(pingTimestamp)))
	select {
	case ch <- rtt:
	default:
	}
}

func TestBuildHandshakeReq(t *testing.T) {
	n := newMockPeerNode("test", 50020)
	req := buildHandshakeReq(n)

	if req == nil || req.Self == nil || req.Self.Id == nil {
		t.Fatal("buildHandshakeReq should return valid request")
	}
	if req.Self.Id.Name != "test" {
		t.Fatalf("expected name 'test', got '%s'", req.Self.Id.Name)
	}
	if req.Self.Addrs[0].Port != 50020 {
		t.Fatal("handshake request should contain correct address")
	}
}

func TestConnectToPeersSkipSelf(t *testing.T) {
	n := newMockPeerNode("test", 50021)

	peers := []*pb.NodeInfo{
		{
			Id: &pb.NodeID{Uuid: n.GetNodeID().Uuid, Name: "test"},
			Addrs: []*pb.NodeAddr{{
				Ip: "127.0.0.1", Port: 50021,
			}},
			Status: pb.NodeStatus_ONLINE,
		},
	}

	err := ConnectToPeers(n, peers)
	if err != nil {
		t.Fatalf("ConnectToPeers should not error when skipping self: %v", err)
	}

	if len(n.streams) != 0 {
		t.Fatalf("expected 0 remote peers when skipping self, got %d", len(n.streams))
	}
}

func TestConnectToPeersEmptyAddr(t *testing.T) {
	n := newMockPeerNode("test", 50022)

	peers := []*pb.NodeInfo{
		{
			Id:     &pb.NodeID{Uuid: "peer-uuid-empty", Name: "empty-peer"},
			Addrs:  []*pb.NodeAddr{},
			Status: pb.NodeStatus_ONLINE,
		},
	}

	err := ConnectToPeers(n, peers)
	if err == nil {
		t.Fatal("ConnectToPeers should error when peer has no address")
	}
}
