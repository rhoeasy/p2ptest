package transport

import (
	"sync"

	pb "p2ptest/proto/p2p"
	"google.golang.org/grpc"
)

// ConnPool 管理 peer 的 gRPC 连接和流，线程安全。
type ConnPool struct {
	mu      sync.RWMutex
	conns   map[string]*grpc.ClientConn
	streams map[string]pb.Messaging_StreamClient
}

func NewConnPool() *ConnPool {
	return &ConnPool{
		conns:   make(map[string]*grpc.ClientConn),
		streams: make(map[string]pb.Messaging_StreamClient),
	}
}

func (p *ConnPool) SetConn(addr string, conn *grpc.ClientConn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.conns[addr] = conn
}

func (p *ConnPool) GetConn(addr string) (*grpc.ClientConn, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	conn, ok := p.conns[addr]
	return conn, ok
}

func (p *ConnPool) DeleteConn(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if conn, ok := p.conns[addr]; ok && conn != nil {
		_ = conn.Close()
	}
	delete(p.conns, addr)
}

func (p *ConnPool) SetStream(addr string, stream pb.Messaging_StreamClient) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.streams[addr] = stream
}

func (p *ConnPool) GetStream(addr string) (pb.Messaging_StreamClient, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	stream, ok := p.streams[addr]
	return stream, ok
}

func (p *ConnPool) DeleteStream(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.streams, addr)
}

func (p *ConnPool) GetStreamsCopy() map[string]pb.Messaging_StreamClient {
	p.mu.RLock()
	defer p.mu.RUnlock()
	copy := make(map[string]pb.Messaging_StreamClient, len(p.streams))
	for k, v := range p.streams {
		copy[k] = v
	}
	return copy
}

func (p *ConnPool) CloseByAddr(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if stream, ok := p.streams[addr]; ok && stream != nil {
		_ = stream.CloseSend()
	}
	delete(p.streams, addr)
	if conn, ok := p.conns[addr]; ok && conn != nil {
		_ = conn.Close()
	}
	delete(p.conns, addr)
}

func (p *ConnPool) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, stream := range p.streams {
		if stream != nil {
			_ = stream.CloseSend()
		}
	}
	for _, conn := range p.conns {
		if conn != nil {
			_ = conn.Close()
		}
	}
	p.streams = make(map[string]pb.Messaging_StreamClient)
	p.conns = make(map[string]*grpc.ClientConn)
}
