package root

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/pprof"

	"github.com/spf13/cobra"

	"p2ptest/internal/client"
	"p2ptest/internal/console"
	"p2ptest/internal/logger"
	"p2ptest/internal/node"
	"p2ptest/internal/types"
)

type App struct {
	config        *types.NodeConfig
	pprofAddr     string
	seedAddr      string
	P2PNode       *node.Node
	onNodeStarted func(*node.Node)
}

type AppOption func(*App)

type nodeSender struct {
	node *node.Node
}

func (s *nodeSender) SendTextMessage(targetAddr string, content string) error {
	return client.SendTextMessage(s.node, targetAddr, content)
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

// 全局变量：仅存放Cobra参数和节点实例
var (
	cfg      = &types.NodeConfig{ProtoVer: types.DefaultProtoVer}
	peerIP   string
	peerPort uint
	debug    bool
	rootCmd  = &cobra.Command{
		Use:   "p2pnode",
		Short: "P2P节点程序（基于gRPC+Cobra）",
		Long:  `轻量级P2P节点程序，支持节点加入、心跳、双向消息流`,
		Run:   runNode,
	}
)

// init：仅注册CLI参数（无业务逻辑）
func init() {
	// 注册参数（支持短选项+长选项）
	rootCmd.Flags().StringVarP(&cfg.NodeName, "name", "n", "node", "节点可读名称")
	rootCmd.Flags().StringVarP(&cfg.ListenIP, "ip", "i", "127.0.0.1", "节点监听IP")
	rootCmd.Flags().Uint32VarP(&cfg.ListenPort, "port", "p", 50051, "节点监听端口")
	rootCmd.Flags().StringVarP(&peerIP, "peer-ip", "", "", "要连接的目标节点IP")
	rootCmd.Flags().UintVarP(&peerPort, "peer-port", "", 0, "要连接的目标节点端口")
	rootCmd.Flags().BoolVarP(&debug, "debug", "d", false, "开启debug日志模式")
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
		go func() {
			console.StartInteractiveConsole(n, &nodeSender{node: n})
			cancel()
		}()
	}

	if err := app.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("[ERROR] %v", err)
	}
}

// Execute：对外暴露的执行入口（给main.go调用）
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("[ERROR] execute failed: %v", err)
	}
}
