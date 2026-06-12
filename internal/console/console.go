package console

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"p2ptest/internal/helper"
)

type PeerInfoProvider interface {
	GetOnlinePeers() []map[string]string
	GetAddrByName(name string) (string, error)
}

type MessageSender interface {
	SendTextMessage(targetAddr string, content string) error
}

// StartInteractiveConsole：启动控制台交互（对外暴露的唯一方法）
// 返回true表示用户请求退出
func StartInteractiveConsole(info PeerInfoProvider, sender MessageSender) bool {
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
		if handleConsoleCommand(info, sender, parts) {
			return true
		}
	}
	return false
}

// handleConsoleCommand：处理控制台命令（内部方法）
// 返回true表示请求退出
func handleConsoleCommand(info PeerInfoProvider, sender MessageSender, parts []string) bool {
	switch parts[0] {
	case "exit":
		fmt.Println("[INFO] 退出节点...")
		return true

	case "list":
		fmt.Println("\n===== 在线节点列表 =====")
		peers := info.GetOnlinePeers()
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
			return false
		}

		target := parts[1]
		content := strings.Join(parts[2:], " ")
		targetAddr := ""

		addr, err := info.GetAddrByName(target)
		if err == nil {
			targetAddr = addr
			fmt.Printf("[INFO] 解析节点名称「%s」→ 地址：%s\n", target, targetAddr)
		} else {
			if strings.Contains(target, ":") {
				targetAddr = target
				fmt.Printf("[INFO] 按IP地址模式处理：%s\n", targetAddr)
			} else {
				fmt.Printf("[ERROR] 发送失败：%s，且不是合法的IP:Port格式\n", err.Error())
				return false
			}
		}

		if err := sender.SendTextMessage(targetAddr, content); err != nil {
			fmt.Printf("[ERROR] 发送失败：%v\n", err)
		} else {
			fmt.Printf("[SUCCESS] 消息已发送到 %s（名称：%s）: %s\n", targetAddr, target, content)
		}

	default:
		fmt.Println("未知命令，仅支持：send/exit")
	}
	return false
}
