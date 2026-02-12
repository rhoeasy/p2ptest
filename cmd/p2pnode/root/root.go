package root

import (
	"log"

	"github.com/spf13/cobra"

	"p2ptest/internal/client"
	"p2ptest/internal/console"
	"p2ptest/internal/logger"
	"p2ptest/internal/peer"
	"p2ptest/internal/types"
)

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
		Run:   runNode, // 核心运行逻辑
	}
	node *peer.Node
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

// runNode：节点启动+连接逻辑（核心业务）
func runNode(cmd *cobra.Command, args []string) {

	logger.InitLogger(debug)

	// 1. 初始化并启动节点
	node = peer.NewNode(cfg)
	if err := node.Start(); err != nil {
		log.Fatalf("[ERROR] 启动节点失败：%v", err)
	}
	defer node.Stop()

	// 2. 连接目标节点（若指定）
	if peerIP != "" && peerPort > 0 {
		if err := client.JoinAndConnect(node, peerIP, uint32(peerPort)); err != nil {
			log.Printf("[WARN] 连接目标节点失败：%v", err)
		}
	}

	// 3. 启动控制台交互（委托给console包）
	console.StartInteractiveConsole(node)
}

// Execute：对外暴露的执行入口（给main.go调用）
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("[ERROR] 程序执行失败：%v", err)
	}
}
