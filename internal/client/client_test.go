package client

import (
	"testing"

	"p2ptest/internal/logger"
	"p2ptest/internal/node"
	"p2ptest/internal/types"
	pb "p2ptest/proto/p2p"
)

func init() {
	logger.InitLogger(false)
}

func newTestNode(name string, port uint32) *node.Node {
	cfg := &types.NodeConfig{
		NodeName:   name,
		ListenIP:   "127.0.0.1",
		ListenPort: port,
		ProtoVer:   types.DefaultProtoVer,
	}
	return node.NewNode(cfg)
}

func TestBuildHandshakeReq(t *testing.T) {
	n := newTestNode("test", 50020)
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
	n := newTestNode("test", 50021)

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

	onlinePeers := n.GetOnlinePeers()
	if len(onlinePeers) != 0 {
		t.Fatalf("expected 0 remote peers when skipping self, got %d", len(onlinePeers))
	}
}

func TestConnectToPeersEmptyAddr(t *testing.T) {
	n := newTestNode("test", 50022)

	peers := []*pb.NodeInfo{
		{
			Id:    &pb.NodeID{Uuid: "peer-uuid-empty", Name: "empty-peer"},
			Addrs: []*pb.NodeAddr{},
			Status: pb.NodeStatus_ONLINE,
		},
	}

	err := ConnectToPeers(n, peers)
	if err == nil {
		t.Fatal("ConnectToPeers should error when peer has no address")
	}
}
