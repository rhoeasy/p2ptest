package root

import (
	"context"
	"net/http"
	"testing"
	"time"

	"p2ptest/internal/node"
	"p2ptest/internal/types"
)

func TestNewApp(t *testing.T) {
	cfg := &types.NodeConfig{
		NodeName:   "test-node",
		ListenIP:   "127.0.0.1",
		ListenPort: 50051,
		ProtoVer:   types.DefaultProtoVer,
	}

	app := NewApp(cfg)

	if app == nil {
		t.Fatal("NewApp should return non-nil")
	}
	if app.config != cfg {
		t.Fatal("app.config should be the provided config")
	}
}

func TestNewAppWithOptions(t *testing.T) {
	cfg := &types.NodeConfig{
		NodeName:   "test-node",
		ListenIP:   "127.0.0.1",
		ListenPort: 50051,
	}

	app := NewApp(cfg, WithPprof("127.0.0.1:6060"))

	if app.pprofAddr != "127.0.0.1:6060" {
		t.Fatalf("expected pprofAddr to be set, got %s", app.pprofAddr)
	}
}

func TestAppDefaultConfig(t *testing.T) {
	cfg := &types.NodeConfig{}
	app := NewApp(cfg)

	if app.config.ProtoVer != types.DefaultProtoVer {
		t.Fatalf("expected default proto ver %d, got %d", types.DefaultProtoVer, app.config.ProtoVer)
	}
}

func TestAppRunInitializesNode(t *testing.T) {
	cfg := &types.NodeConfig{
		NodeName:   "test-run",
		ListenIP:   "127.0.0.1",
		ListenPort: 50061,
		ProtoVer:   types.DefaultProtoVer,
	}

	app := NewApp(cfg)

	// Use a channel to safely receive the node reference
	nodeReady := make(chan struct{})
	app.onNodeStarted = func(n *node.Node) {
		close(nodeReady)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- app.Run(ctx)
	}()

	// Wait for the node to be initialized (signaled via callback)
	select {
	case <-nodeReady:
		// Node is ready — safe to access app.P2PNode
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for node to start")
	}

	if app.P2PNode == nil {
		t.Fatal("app.P2PNode should be initialized after Run")
	}

	cancel()
	<-done
}

func TestAppRunWithPprof(t *testing.T) {
	cfg := &types.NodeConfig{
		NodeName:   "test-pprof",
		ListenIP:   "127.0.0.1",
		ListenPort: 50062,
		ProtoVer:   types.DefaultProtoVer,
	}

	app := NewApp(cfg, WithPprof("127.0.0.1:16060"))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- app.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get("http://127.0.0.1:16060/debug/pprof/")
	if err != nil {
		t.Fatalf("pprof should be accessible: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	cancel()
	<-done
}

func TestAppWithSeedAddr(t *testing.T) {
	cfg := &types.NodeConfig{
		NodeName:   "test-seed",
		ListenIP:   "127.0.0.1",
		ListenPort: 50063,
		ProtoVer:   types.DefaultProtoVer,
	}

	app := NewApp(cfg, WithSeedAddr("127.0.0.1:50051"))

	if app.seedAddr != "127.0.0.1:50051" {
		t.Fatalf("expected seedAddr to be set, got %s", app.seedAddr)
	}
}
