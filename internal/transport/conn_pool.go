package transport

import (
	"fmt"
	"sync"

	pb "p2ptest/proto/p2p"
	"google.golang.org/grpc"
)

// lockedStream wraps a gRPC stream with a send mutex.
// gRPC requires that Send() not be called concurrently on the same stream.
type lockedStream struct {
	mu     sync.Mutex
	stream pb.Messaging_StreamClient
}

// ConnPool 管理 peer 的 gRPC 连接和流，线程安全。
type ConnPool struct {
	mu      sync.RWMutex
	conns   map[string]*grpc.ClientConn
	streams map[string]*lockedStream
}

func NewConnPool() *ConnPool {
	return &ConnPool{
		conns:   make(map[string]*grpc.ClientConn),
		streams: make(map[string]*lockedStream),
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
	p.streams[addr] = &lockedStream{stream: stream}
}

func (p *ConnPool) GetStream(addr string) (pb.Messaging_StreamClient, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	ls, ok := p.streams[addr]
	if !ok {
		return nil, false
	}
	return ls.stream, ok
}

func (p *ConnPool) DeleteStream(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.streams, addr)
}

func (p *ConnPool) GetStreamsCopy() map[string]*lockedStream {
	p.mu.RLock()
	defer p.mu.RUnlock()
	copy := make(map[string]*lockedStream, len(p.streams))
	for k, v := range p.streams {
		copy[k] = v
	}
	return copy
}

func (p *ConnPool) StreamAddrs() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	addrs := make([]string, 0, len(p.streams))
	for addr := range p.streams {
		addrs = append(addrs, addr)
	}
	return addrs
}

func (p *ConnPool) HasStream(addr string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, ok := p.streams[addr]
	return ok
}

func (p *ConnPool) SendToStream(addr string, env *pb.Envelope) error {
	p.mu.RLock()
	ls, ok := p.streams[addr]
	p.mu.RUnlock()
	if !ok {
		return fmt.Errorf("no stream to %s", addr)
	}
	ls.mu.Lock()
	defer ls.mu.Unlock()
	return ls.stream.Send(env)
}

func (p *ConnPool) CloseByAddr(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if ls, ok := p.streams[addr]; ok && ls != nil {
		_ = ls.stream.CloseSend()
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
	for _, ls := range p.streams {
		if ls != nil {
			_ = ls.stream.CloseSend()
		}
	}
	for _, conn := range p.conns {
		if conn != nil {
			_ = conn.Close()
		}
	}
	p.streams = make(map[string]*lockedStream)
	p.conns = make(map[string]*grpc.ClientConn)
}
