package root

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"p2ptest/internal/client"
	"p2ptest/internal/console"
	"p2ptest/internal/logger"
	"p2ptest/internal/node"
	"p2ptest/internal/notifier"
	"p2ptest/internal/types"
	"p2ptest/internal/web"

	pb "p2ptest/proto/p2p"
	"go.uber.org/zap"
)

type App struct {
	config        *types.NodeConfig
	pprofAddr     string
	seedAddr      string
	P2PNode       *node.Node
	onNodeStarted func(*node.Node)
}

type AppOption func(*App)

type nodeAdapter struct {
	node *node.Node
}

func (a *nodeAdapter) SendTextMessage(targetAddr string, content string) error {
	return client.SendTextMessage(a.node, targetAddr, content)
}

func (a *nodeAdapter) BroadcastMessage(content string) (int, int) {
	return client.BroadcastTextMessage(a.node, content)
}

func (a *nodeAdapter) ConnectToPeer(addr string) error {
	peers, err := client.HandshakeSeedNode(addr, a.node)
	if err != nil {
		return fmt.Errorf("连接 %s 失败: %w", addr, err)
	}
	return client.ConnectToPeers(a.node, peers)
}

func (a *nodeAdapter) DisconnectPeer(name string) (string, error) {
	return a.node.DisconnectPeer(name)
}

func (a *nodeAdapter) SendPing(targetAddr string) (time.Duration, error) {
	return client.SendPing(a.node, targetAddr)
}

func (a *nodeAdapter) SetNodeStatus(status string) error {
	switch status {
	case "online":
		a.node.SetNodeStatus(pb.NodeStatus_ONLINE)
	case "busy":
		a.node.SetNodeStatus(pb.NodeStatus_BUSY)
	default:
		return fmt.Errorf("invalid status: %s", status)
	}
	return nil
}

func (a *nodeAdapter) GetNodeStatus() string {
	return a.node.GetNodeStatus()
}

func NewApp(cfg *types.NodeConfig, opts ...AppOption) *App {
	if cfg.ProtoVer == 0 {
		cfg.ProtoVer = types.DefaultProtoVer
	}
	app := &App{config: cfg}
	for _, opt := range opts {
		opt(app)
	}
	return app
}

func WithPprof(addr string) AppOption {
	return func(a *App) {
		a.pprofAddr = addr
	}
}

func WithSeedAddr(addr string) AppOption {
	return func(a *App) {
		a.seedAddr = addr
	}
}

func (a *App) Run(ctx context.Context) error {
	logger.InitLogger(false)

	if a.pprofAddr != "" {
		go a.startPprof()
	}

	a.P2PNode = node.NewNode(a.config)
	if err := a.P2PNode.Start(); err != nil {
		return err
	}
	defer a.P2PNode.Stop()

	if a.onNodeStarted != nil {
		a.onNodeStarted(a.P2PNode)
	}

	if a.seedAddr != "" {
		if err := client.HandshakeAndConnect(a.P2PNode, a.seedAddr); err != nil {
			log.Printf("[WARN] connect seed failed: %v", err)
		}
	}

	<-ctx.Done()
	return ctx.Err()
}

func (a *App) startPprof() {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", http.HandlerFunc(pprof.Index))
	mux.HandleFunc("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
	mux.HandleFunc("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	mux.HandleFunc("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
	mux.HandleFunc("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))
	mux.HandleFunc("/debug/pprof/block", http.HandlerFunc(pprof.Handler("block").ServeHTTP))
	mux.HandleFunc("/debug/pprof/goroutine", http.HandlerFunc(pprof.Handler("goroutine").ServeHTTP))

	log.Printf("pprof started at http://%s/debug/pprof/", a.pprofAddr)
	if err := http.ListenAndServe(a.pprofAddr, mux); err != nil {
		log.Printf("pprof failed: %v", err)
	}
}

var (
	cfg      = &types.NodeConfig{ProtoVer: types.DefaultProtoVer}
	peerIP   string
	peerPort uint
	debug    bool
	webAddr  string
	rootCmd  = &cobra.Command{
		Use:   "p2pnode",
		Short: "P2P节点程序（基于gRPC+Cobra）",
		Long:  `轻量级P2P节点程序，支持节点加入、心跳、双向消息流`,
		Run:   runNode,
	}
)

func init() {
	rootCmd.Flags().StringVarP(&cfg.NodeName, "name", "n", "node", "节点可读名称")
	rootCmd.Flags().StringVarP(&cfg.ListenIP, "ip", "i", "127.0.0.1", "节点监听IP")
	rootCmd.Flags().Uint32VarP(&cfg.ListenPort, "port", "p", 50051, "节点监听端口")
	rootCmd.Flags().StringVarP(&peerIP, "peer-ip", "", "", "要连接的目标节点IP")
	rootCmd.Flags().UintVarP(&peerPort, "peer-port", "", 0, "要连接的目标节点端口")
	rootCmd.Flags().BoolVarP(&debug, "debug", "d", false, "开启debug日志模式")
	rootCmd.Flags().StringVar(&webAddr, "web", "", "Web管理面板地址（例: :8080）")
}

func runNode(cmd *cobra.Command, args []string) {
	var opts []AppOption
	if debug {
		opts = append(opts, WithPprof("127.0.0.1:6060"))
	}
	if peerIP != "" && peerPort > 0 {
		opts = append(opts, WithSeedAddr(fmt.Sprintf("%s:%d", peerIP, peerPort)))
	}

	app := NewApp(cfg, opts...)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	app.onNodeStarted = func(n *node.Node) {
		logger.RedirectToFile(fmt.Sprintf("%s.log", cfg.NodeName))

		n.Notifier().Subscribe(func(notif notifier.Notification) {
			if notif.Type == "peer_discovered" {
				var payload map[string]string
				if err := json.Unmarshal(notif.Payload, &payload); err != nil {
					return
				}
				addr := payload["addr"]
				peerUUID := payload["uuid"]
				if addr == "" || peerUUID == "" || peerUUID == n.GetNodeID().Uuid {
					return
				}
				selfAddr := fmt.Sprintf("%s:%d", n.Cfg().ListenIP, n.Cfg().ListenPort)
				if addr == selfAddr {
					return
				}
				peerInfo := &pb.NodeInfo{
					Id:    &pb.NodeID{Uuid: peerUUID, Name: payload["name"]},
					Addrs: []*pb.NodeAddr{{Ip: addr[:strings.LastIndex(addr, ":")], Port: uint32(mustParsePort(addr))}},
				}
				if err := client.ConnectToPeers(n, []*pb.NodeInfo{peerInfo}); err != nil {
					logger.L().Warn("[node] auto-connect failed", zap.String("addr", addr), zap.Error(err))
				}
			}
		})

		adapter := &nodeAdapter{node: n}

		go func() {
			console.StartInteractiveConsole(n, adapter, adapter, adapter, adapter, n.Notifier())
			cancel()
		}()

		if webAddr != "" {
			webServer := web.NewServer(webAddr, n, adapter, adapter, adapter, adapter, n.Notifier())
			if err := webServer.Start(); err != nil {
				log.Printf("[WARN] web server start failed: %v", err)
			} else {
				log.Printf("[INFO] web dashboard started at http://%s", webAddr)
			}
		}
	}

	if err := app.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("[ERROR] %v", err)
	}
}

func mustParsePort(addr string) int {
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return 0
	}
	p, _ := strconv.Atoi(portStr)
	return p
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("[ERROR] execute failed: %v", err)
	}
}
