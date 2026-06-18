package web

import (
	"net/http"
	"time"

	"p2ptest/internal/notifier"
	"github.com/gorilla/websocket"
)

type wsClient struct {
	conn *websocket.Conn
	send chan notifier.Notification
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")

	conn, err := s.wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	client := &wsClient{
		conn: conn,
		send: make(chan notifier.Notification, 256),
	}

	s.mu.Lock()
	s.wsClients[client] = struct{}{}
	s.mu.Unlock()

	history := s.notifier.History()
	for _, notification := range history {
		client.send <- notification
	}

	stopChan := make(chan struct{})
	subToken := s.notifier.Subscribe(func(notification notifier.Notification) {
		select {
		case client.send <- notification:
		case <-stopChan:
			return
		default:
		}
	})

	go s.writePump(client, stopChan, subToken)
	go s.readPump(client, stopChan)
}

func (s *Server) writePump(client *wsClient, stopChan chan struct{}, subToken notifier.SubscriptionToken) {
	defer func() {
		s.notifier.Unsubscribe(subToken)
		client.conn.Close()
		close(client.send)

		s.mu.Lock()
		delete(s.wsClients, client)
		s.mu.Unlock()
	}()

	for {
		select {
		case notification, ok := <-client.send:
			if !ok {
				client.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			err := client.conn.WriteJSON(notification)
			if err != nil {
				return
			}
		case <-stopChan:
			return
		}
	}
}

func (s *Server) readPump(client *wsClient, stopChan chan struct{}) {
	defer func() {
		close(stopChan)
	}()

	client.conn.SetReadLimit(512)
	client.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	client.conn.SetPongHandler(func(string) error {
		client.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, _, err := client.conn.ReadMessage()
		if err != nil {
			break
		}
	}
}
