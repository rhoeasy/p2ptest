package client

import (
	"crypto/rand"
	"fmt"
	"time"

	pb "p2ptest/proto/p2p"
)

// SendPing sends a ping to targetAddr and waits for pong, returning RTT.
func SendPing(n PeerNode, targetAddr string) (time.Duration, error) {
	nonce := make([]byte, 8)
	if _, err := rand.Read(nonce); err != nil {
		return 0, fmt.Errorf("generate nonce failed: %w", err)
	}

	nonceKey := string(nonce)

	env := &pb.Envelope{
		MsgId:     generateMsgID(),
		From:      n.GetNodeID(),
		Timestamp: uint64(time.Now().UnixMilli()),
		Payload: &pb.Envelope_Ping{
			Ping: &pb.Ping{Nonce: nonce},
		},
	}

	// Register pending ping and get a channel to wait on
	pongCh := n.RecordPingSent(nonceKey)

	if err := n.SendToStream(targetAddr, env); err != nil {
		n.CancelPendingPing(nonceKey)
		return 0, fmt.Errorf("send ping failed: %w", err)
	}

	// Wait for pong or timeout
	select {
	case rtt := <-pongCh:
		return rtt, nil
	case <-time.After(5 * time.Second):
		n.CancelPendingPing(nonceKey)
		return 0, fmt.Errorf("ping timeout: %s", targetAddr)
	}
}
