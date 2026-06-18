package web

import (
	"net"
	"testing"

	"p2ptest/internal/notifier"
)

// Start must surface bind failures instead of swallowing them. Previously
// ListenAndServe ran in a goroutine and its error was discarded, so a port
// conflict failed silently.
func TestServerStartReturnsErrorOnPortConflict(t *testing.T) {
	n := notifier.NewNotifier(10)
	server := newTestServer(&mockPeerInfoProvider{}, &mockMessageSender{}, n)

	// Occupy a free port so the web server cannot bind it.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("setup listen failed: %v", err)
	}
	defer ln.Close()
	server.addr = ln.Addr().String()

	defer server.Stop()

	if err := server.Start(); err == nil {
		t.Fatal("Start should return an error when the port is already in use")
	}
}
