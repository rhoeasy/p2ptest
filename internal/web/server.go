package web

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"p2ptest/internal/notifier"
	"github.com/gorilla/websocket"
)

type PeerInfoProvider interface {
	GetOnlinePeers() []map[string]string
	GetAddrByName(name string) (string, error)
}

type MessageSender interface {
	SendTextMessage(targetAddr string, content string) error
	BroadcastMessage(content string) (int, int)
}

type PeerConnector interface {
	ConnectToPeer(addr string) error
	DisconnectPeer(name string) (string, error)
}

type PingSender interface {
	SendPing(targetAddr string) (time.Duration, error)
}

type StatusSetter interface {
	SetNodeStatus(status string) error
	GetNodeStatus() string
}

type Server struct {
	addr         string
	info         PeerInfoProvider
	sender       MessageSender
	connector    PeerConnector
	pingSender   PingSender
	statusSetter StatusSetter
	notifier     *notifier.Notifier
	mux        *http.ServeMux
	httpServer *http.Server
	wsUpgrader websocket.Upgrader
	wsClients  map[*wsClient]struct{}
	mu         sync.Mutex
}

func NewServer(addr string, info PeerInfoProvider, sender MessageSender, connector PeerConnector, pingSender PingSender, statusSetter StatusSetter, n *notifier.Notifier) *Server {
	s := &Server{
		addr:         addr,
		info:         info,
		sender:       sender,
		connector:    connector,
		pingSender:   pingSender,
		statusSetter: statusSetter,
		notifier:     n,
		mux:        http.NewServeMux(),
		wsUpgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
		wsClients: make(map[*wsClient]struct{}),
	}

	s.setupRoutes()
	return s
}

func (s *Server) getHandler() http.Handler {
	return s.mux
}

func (s *Server) setupRoutes() {
	s.mux.HandleFunc("/api/peers", s.handleGetPeers)
	s.mux.HandleFunc("/api/send", s.handleSendMessage)
	s.mux.HandleFunc("/api/broadcast", s.handleBroadcastMessage)
	s.mux.HandleFunc("/api/connect", s.handleConnect)
	s.mux.HandleFunc("/api/disconnect", s.handleDisconnect)
	s.mux.HandleFunc("/api/notifications", s.handleGetNotifications)
	s.mux.HandleFunc("/api/messages", s.handleGetMessages)
	s.mux.HandleFunc("/api/ping", s.handlePing)
	s.mux.HandleFunc("/api/status", s.handleStatus)
	s.mux.HandleFunc("/ws", s.handleWebSocket)
}

func (s *Server) handleGetPeers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	peers := s.info.GetOnlinePeers()
	response := map[string]interface{}{"peers": peers}
	jsonBytes, err := json.Marshal(response)
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	w.Write(jsonBytes)
}

func (s *Server) Start() error {
	s.httpServer = &http.Server{
		Addr:    s.addr,
		Handler: s.mux,
	}

	go func() {
		s.httpServer.ListenAndServe()
	}()

	return nil
}

func (s *Server) Stop() error {
	if s.httpServer == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return s.httpServer.Shutdown(ctx)
}
