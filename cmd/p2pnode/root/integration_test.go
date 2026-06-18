package root

import (
	"context"
	"testing"
	"time"

	"p2ptest/internal/node"
	"p2ptest/internal/types"
)

func TestTwoNodesCanExchangeMessages(t *testing.T) {
	// 启动 seed 节点
	seedCfg := &types.NodeConfig{
		NodeName:   "seed",
		ListenIP:   "127.0.0.1",
		ListenPort: 50091,
		ProtoVer:   types.DefaultProtoVer,
	}
	seedApp := NewApp(seedCfg)
	seedCtx, seedCancel := context.WithCancel(context.Background())
	defer seedCancel()

	seedReady := make(chan struct{})
	seedApp.onNodeStarted = func(n *node.Node) {
		close(seedReady)
	}

	seedDone := make(chan error, 1)
	go func() {
		seedDone <- seedApp.Run(seedCtx)
	}()

	// 等待 seed 启动（通过回调信号）
	select {
	case <-seedReady:
	case <-time.After(2 * time.Second):
		t.Fatal("seed did not start in time")
	}

	// 启动 node2，连接到 seed
	node2Cfg := &types.NodeConfig{
		NodeName:   "node2",
		ListenIP:   "127.0.0.1",
		ListenPort: 50092,
		ProtoVer:   types.DefaultProtoVer,
	}
	node2App := NewApp(node2Cfg, WithSeedAddr("127.0.0.1:50091"))
	node2Ctx, node2Cancel := context.WithCancel(context.Background())
	defer node2Cancel()

	node2Ready := make(chan struct{})
	node2App.onNodeStarted = func(n *node.Node) {
		close(node2Ready)
	}

	node2Done := make(chan error, 1)
	go func() {
		node2Done <- node2App.Run(node2Ctx)
	}()

	// 等待 node2 启动
	select {
	case <-node2Ready:
	case <-time.After(2 * time.Second):
		t.Fatal("node2 did not start in time")
	}

	// 等待连接建立完成
	time.Sleep(200 * time.Millisecond)

	// 验证 node2 可以看到 seed
	peers := node2App.P2PNode.GetOnlinePeers()
	foundSeed := false
	for _, p := range peers {
		if p["name"] == "seed" {
			foundSeed = true
			break
		}
	}
	if !foundSeed {
		t.Fatalf("node2 should see seed in peer list, got: %v", peers)
	}

	// 验证 seed 可以看到 node2
	seedPeers := seedApp.P2PNode.GetOnlinePeers()
	foundNode2 := false
	for _, p := range seedPeers {
		if p["name"] == "node2" {
			foundNode2 = true
			break
		}
	}
	if !foundNode2 {
		t.Fatalf("seed should see node2 in peer list, got: %v", seedPeers)
	}

	// 清理
	node2Cancel()
	seedCancel()

	select {
	case <-node2Done:
	case <-time.After(2 * time.Second):
		t.Fatal("node2 did not stop in time")
	}

	select {
	case <-seedDone:
	case <-time.After(2 * time.Second):
		t.Fatal("seed did not stop in time")
	}
}
