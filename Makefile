# 项目基础配置
APP_NAME := p2pnode
BIN_DIR := ./bin
PROTO_FILE := ./proto/message.proto
PROTO_OUT := .
GO_MOD := p2p-demo

# 默认目标：先编译proto，再编译程序
all: proto build

# 编译Protobuf文件（生成pb.go和grpc.pb.go）
proto:
	@echo "编译Protobuf文件..."
	protoc --go_out=$(PROTO_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_OUT) --go-grpc_opt=paths=source_relative \
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
	rm -f $(PROTO_OUT)/message.pb.go $(PROTO_OUT)/message_grpc.pb.go
	@echo "清理完成"

# 快速启动种子节点（用Cobra短选项，避免解析错误）
run-seed: build
	@echo "启动种子节点（端口50051）..."
	$(BIN_DIR)/$(APP_NAME) -n seed -i 127.0.0.1 -p 50051 -d

# 快速启动node2节点（连接种子节点，短+长选项结合）
run-node2: build
	@echo "启动node2节点（端口50052，连接种子节点）..."
	$(BIN_DIR)/$(APP_NAME) -n node2 -i 127.0.0.1 -p 50052 --peer-ip 127.0.0.1 --peer-port 50051

run-node3: build
	@echo "启动node3节点（端口50053，连接种子节点）..."
	$(BIN_DIR)/$(APP_NAME) -n node3 -i 127.0.0.1 -p 50053 --peer-ip 127.0.0.1 --peer-port 50051

# 伪目标（避免和同名文件冲突）
.PHONY: all proto build clean