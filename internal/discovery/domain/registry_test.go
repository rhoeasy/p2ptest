package domain

import (
	"testing"

	pb "p2ptest/proto/p2p"
)

// helper: build a minimal valid peer
func newTestPeer(name, uuid string) *pb.NodeInfo {
	return &pb.NodeInfo{
		Id:    &pb.NodeID{Uuid: uuid, Name: name},
		Addrs: []*pb.NodeAddr{{Ip: "127.0.0.1", Port: 50052}},
	}
}

// Register must not alias the caller's pointer: mutating the original after
// registration must not change what the registry returns.
func TestPeerRegistry_RegisterDoesNotAliasCallerPointer(t *testing.T) {
	r := NewPeerRegistry("self-uuid")

	peer := newTestPeer("node2", "uuid-2")
	if err := r.Register(peer); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// caller mutates its own copy after registration
	peer.Id.Name = "tampered"
	peer.Addrs[0].Port = 9999

	got, ok := r.Get("uuid-2")
	if !ok {
		t.Fatal("expected peer to be registered")
	}
	if got.Id.Name != "node2" {
		t.Errorf("registry leaked caller mutation: name = %q, want %q", got.Id.Name, "node2")
	}
	if got.Addrs[0].Port != 50052 {
		t.Errorf("registry leaked caller mutation: port = %d, want %d", got.Addrs[0].Port, 50052)
	}
}
