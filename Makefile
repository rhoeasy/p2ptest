# 项目基础配置
APP_NAME := p2pnode
BIN_DIR := ./bin
PROTO_FILE := ./proto/p2p.proto
PROTO_OUT := ./proto
GO_MOD := p2ptest

# 默认目标：先编译proto，再编译程序
all: proto build

# 编译Protobuf文件（生成pb.go和grpc.pb.go）
proto:
	@echo "编译Protobuf文件..."
	mkdir -p $(PROTO_OUT)/p2p
	protoc --go_out=. --go_opt=module=p2ptest \
		--go-grpc_out=. --go-grpc_opt=module=p2ptest \
		$(PROTO_FILE)
	@echo "Protobuf编译完成"

# 编译Go程序到bin目录
build:
	@echo "编译Go程序..."
	mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/$(APP_NAME) ./cmd/p2pnode
	@echo "程序编译完成，输出路径：$(BIN_DIR)/$(APP_NAME)"

# 清理生成的文件
clean:
	@echo "清理生成文件..."
	rm -rf $(BIN_DIR)
	rm -f $(PROTO_OUT)/p2p/*.pb.go
	@echo "清理完成"

# 快速启动种子节点（用Cobra短选项，避免解析错误）
run-seed: build
	@echo "启动种子节点（端口50051，Web面板:8081）..."
	$(BIN_DIR)/$(APP_NAME) -n seed -i 127.0.0.1 -p 50051 -d --web :8081

# 快速启动node2节点（连接种子节点，短+长选项结合）
run-node2: build
	@echo "启动node2节点（端口50052，Web面板:8082，连接种子节点）..."
	$(BIN_DIR)/$(APP_NAME) -n node2 -i 127.0.0.1 -p 50052 --peer-ip 127.0.0.1 --peer-port 50051 --web :8082

run-node3: build
	@echo "启动node3节点（端口50053，Web面板:8083，连接种子节点）..."
	$(BIN_DIR)/$(APP_NAME) -n node3 -i 127.0.0.1 -p 50053 --peer-ip 127.0.0.1 --peer-port 50051 --web :8083

# 单元测试
test:
	@echo "运行单元测试..."
	go test -v ./internal/...

# 🔥 竞态条件检测（Race Condition Test）
test-race:
	@echo "运行竞态条件检测（race detector）..."
	go test -race -v ./internal/...

# 伪目标（避免和同名文件冲突）
.PHONY: all proto build clean run-seed run-node2 run-node3 test test-race