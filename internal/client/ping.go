package client

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	pb "p2ptest/proto/p2p"
)

// SendPing sends a ping to targetAddr and waits for pong, returning RTT.
func SendPing(n PeerNode, targetAddr string) (time.Duration, error) {
	nonce := make([]byte, 8)
	if _, err := rand.Read(nonce); err != nil {
		return 0, fmt.Errorf("generate nonce failed: %w", err)
	}

	nonceKey := string(nonce)

	payload := &pb.Envelope_Ping{
		Ping: &pb.Ping{Nonce: nonce},
	}
	payloadBytes, err := proto.Marshal(payload.Ping)
	if err != nil {
		return 0, fmt.Errorf("marshal payload failed: %w", err)
	}
	contentHash := sha256.Sum256(payloadBytes)

	env := &pb.Envelope{
		MsgId:       generateMsgID(),
		From:        n.GetNodeID(),
		Timestamp:   uint64(time.Now().UnixMilli()),
		Payload:     payload,
		ContentHash: contentHash[:],
		Signature:   n.Sign(contentHash[:]),
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
