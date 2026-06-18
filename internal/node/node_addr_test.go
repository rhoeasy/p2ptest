package node

import "testing"

// parseHostPort splits a "host:port" address into its parts. It must never
// panic on malformed input: callers (e.g. broadcastNodeJoin) feed it values
// from notifications that are not guaranteed to contain a port.
func TestParseHostPort(t *testing.T) {
	cases := []struct {
		name   string
		addr   string
		host   string
		port   uint32
		ok     bool
	}{
		{"normal", "127.0.0.1:50051", "127.0.0.1", 50051, true},
		{"ipv6", "[::1]:50051", "::1", 50051, true},
		{"no_colon", "no-port", "", 0, false},
		{"empty", "", "", 0, false},
		{"port_not_numeric", "host:abc", "", 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// This must not panic for any input.
			host, port, ok := parseHostPort(c.addr)
			if ok != c.ok {
				t.Fatalf("ok = %v, want %v", ok, c.ok)
			}
			if ok {
				if host != c.host {
					t.Errorf("host = %q, want %q", host, c.host)
				}
				if port != c.port {
					t.Errorf("port = %d, want %d", port, c.port)
				}
			}
		})
	}
}
