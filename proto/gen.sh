#!/usr/bin/env bash

set -x

mkdir -p p2p

protoc --go_out=./p2p --go_opt=paths=source_relative \
    --go-grpc_out=./p2p --go-grpc_opt=paths=source_relative \
    p2p.proto