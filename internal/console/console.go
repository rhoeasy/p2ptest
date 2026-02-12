package console

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"p2ptest/internal/client"
	"p2ptest/internal/helper"
	"p2ptest/internal/peer"
)

// StartInteractiveConsole：启动控制台交互（对外暴露的唯一方法）
func StartInteractiveConsole(node *peer.Node) {
	// 控制台提示
	fmt.Println("\n===== P2P节点控制台 =====")
	fmt.Println("send <节点名称/IP地址> <消息内容> - 发送文本消息（支持节点名称）")
	fmt.Println("list                     - 查看节点")
	fmt.Println("exit                     - 退出节点")
	fmt.Println("==========================")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		input := scanner.Text()
		if input == "" {
			continue
		}

		// 解析输入（委托给helper包）
		parts := helper.SplitInput(input)
		handleConsoleCommand(node, parts)
	}
}

// handleConsoleCommand：处理控制台命令（内部方法）
func handleConsoleCommand(node *peer.Node, parts []string) {
	switch parts[0] {
	case "exit":
		fmt.Println("[INFO] 退出节点...")
		node.Stop() // 假设你有节点停止方法
		os.Exit(0)

	case "list": // 新增list命令处理
		fmt.Println("\n===== 在线节点列表 =====")
		peers := node.GetOnlinePeers()
		if len(peers) == 0 {
			fmt.Println("暂无在线节点")
		} else {
			for i, peer := range peers {
				fmt.Printf("节点 %d:\n", i+1)
				for k, v := range peer {
					fmt.Printf("  %s: %s\n", k, v)
				}
				fmt.Println("------------------------")
			}
		}
		fmt.Println("========================")
	case "send":
		if len(parts) < 3 {
			fmt.Println("用法：send <节点名称/IP地址> <消息内容>")
			fmt.Println("示例：send seed 你好 （按名称发送）")
			fmt.Println("示例：send 127.0.0.1:50051 你好 （按IP发送）")
			return
		}

		// 解析目标（节点名称/IP地址）
		target := parts[1]
		content := strings.Join(parts[2:], " ")
		targetAddr := ""

		// 第一步：尝试按节点名称查找地址
		addr, err := node.GetAddrByName(target)
		if err == nil {
			targetAddr = addr
			fmt.Printf("[INFO] 解析节点名称「%s」→ 地址：%s\n", target, targetAddr)
		} else {
			// 第二步：降级为IP:Port处理（兼容原有用法）
			// 简单校验是否是IP:Port格式（包含:）
			if strings.Contains(target, ":") {
				targetAddr = target
				fmt.Printf("[INFO] 按IP地址模式处理：%s\n", targetAddr)
			} else {
				fmt.Printf("[ERROR] 发送失败：%s，且不是合法的IP:Port格式\n", err.Error())
				return
			}
		}

		// 发送消息（原有逻辑，用解析后的targetAddr）
		if err := client.SendTextMessage(node, targetAddr, content); err != nil {
			fmt.Printf("[ERROR] 发送失败：%v\n", err)
		} else {
			fmt.Printf("[SUCCESS] 消息已发送到 %s（名称：%s）: %s\n", targetAddr, target, content)
		}

	default:
		fmt.Println("未知命令，仅支持：send/exit")
	}
}
