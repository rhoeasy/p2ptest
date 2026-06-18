package web

import (
	"encoding/json"
	"fmt"
	"net/http"

	"p2ptest/internal/notifier"
)

type sendRequest struct {
	Target  string `json:"target"`
	Content string `json:"content"`
}

type sendResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

type broadcastRequest struct {
	Content string `json:"content"`
}

type broadcastResponse struct {
	Success int  `json:"success"`
	Failed  int  `json:"failed"`
}

type connectRequest struct {
	Addr string `json:"addr"`
}

type connectResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

type disconnectRequest struct {
	Name string `json:"name"`
}

type disconnectResponse struct {
	Success bool   `json:"success"`
	Addr    string `json:"addr,omitempty"`
	Error   string `json:"error,omitempty"`
}

func (s *Server) handleSendMessage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req sendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if req.Target == "" || req.Content == "" {
		http.Error(w, "Missing required fields: target and content", http.StatusBadRequest)
		return
	}

	targetAddr := req.Target
	addr, err := s.info.GetAddrByName(req.Target)
	if err == nil && addr != "" {
		targetAddr = addr
	} else if !containsColon(req.Target) {
		response := sendResponse{Success: false, Error: fmt.Sprintf("cannot resolve target '%s'", req.Target)}
		json.NewEncoder(w).Encode(response)
		return
	}

	err = s.sender.SendTextMessage(targetAddr, req.Content)

	var response sendResponse
	if err != nil {
		response = sendResponse{Success: false, Error: err.Error()}
	} else {
		response = sendResponse{Success: true}
	}

	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleBroadcastMessage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req broadcastRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if req.Content == "" {
		http.Error(w, "Missing required field: content", http.StatusBadRequest)
		return
	}

	success, failed := s.sender.BroadcastMessage(req.Content)
	json.NewEncoder(w).Encode(broadcastResponse{Success: success, Failed: failed})
}

func (s *Server) handleConnect(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req connectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if req.Addr == "" {
		http.Error(w, "Missing required field: addr", http.StatusBadRequest)
		return
	}

	err := s.connector.ConnectToPeer(req.Addr)
	if err != nil {
		json.NewEncoder(w).Encode(connectResponse{Success: false, Error: err.Error()})
	} else {
		json.NewEncoder(w).Encode(connectResponse{Success: true})
	}
}

func (s *Server) handleDisconnect(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req disconnectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if req.Name == "" {
		http.Error(w, "Missing required field: name", http.StatusBadRequest)
		return
	}

	addr, err := s.connector.DisconnectPeer(req.Name)
	if err != nil {
		json.NewEncoder(w).Encode(disconnectResponse{Success: false, Error: err.Error()})
	} else {
		json.NewEncoder(w).Encode(disconnectResponse{Success: true, Addr: addr})
	}
}

func containsColon(s string) bool {
	for _, c := range s {
		if c == ':' {
			return true
		}
	}
	return false
}

type notificationsResponse struct {
	Notifications []notifier.Notification `json:"notifications"`
}

func (s *Server) handleGetNotifications(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	history := s.notifier.History()
	response := notificationsResponse{Notifications: history}
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleGetMessages(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	history := s.notifier.History()
	var messages []notifier.Notification
	for _, n := range history {
		if n.Type == "message_received" {
			messages = append(messages, n)
		}
	}
	if messages == nil {
		messages = []notifier.Notification{}
	}
	json.NewEncoder(w).Encode(map[string][]notifier.Notification{"messages": messages})
}

type pingRequest struct {
	Target string `json:"target"`
}

type pingResponse struct {
	Success   bool   `json:"success"`
	LatencyMs int64  `json:"latency_ms,omitempty"`
	Error     string `json:"error,omitempty"`
}

func (s *Server) handlePing(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req pingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if req.Target == "" {
		http.Error(w, "Missing required field: target", http.StatusBadRequest)
		return
	}

	targetAddr := req.Target
	addr, err := s.info.GetAddrByName(req.Target)
	if err == nil && addr != "" {
		targetAddr = addr
	}

	latency, err := s.pingSender.SendPing(targetAddr)
	if err != nil {
		json.NewEncoder(w).Encode(pingResponse{Success: false, Error: err.Error()})
	} else {
		json.NewEncoder(w).Encode(pingResponse{Success: true, LatencyMs: latency.Milliseconds()})
	}
}

type statusRequest struct {
	Status string `json:"status"`
}

type statusResponse struct {
	Success bool   `json:"success"`
	Status  string `json:"status"`
	Error   string `json:"error,omitempty"`
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodGet:
		status := s.statusSetter.GetNodeStatus()
		json.NewEncoder(w).Encode(statusResponse{Success: true, Status: status})

	case http.MethodPost:
		var req statusRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		if req.Status == "" {
			http.Error(w, "Missing required field: status", http.StatusBadRequest)
			return
		}
		if err := s.statusSetter.SetNodeStatus(req.Status); err != nil {
			json.NewEncoder(w).Encode(statusResponse{Success: false, Error: err.Error()})
			return
		}
		json.NewEncoder(w).Encode(statusResponse{Success: true, Status: req.Status})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}
