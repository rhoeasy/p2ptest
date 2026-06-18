// Package crypto provides Ed25519 identity and signing for p2ptest nodes.
//
// Each node generates an Identity (Ed25519 keypair) at startup. The public key
// travels in NodeID; the private key never leaves the node. Every Handshake,
// Heartbeat, and Envelope carries a signature so the receiver can prove the
// sender is who it claims to be — replacing the insecure transport that the
// original DDD refactor left in place.
package crypto

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"errors"
)

var ErrInvalidSignature = errors.New("invalid signature")

// Identity is a node's cryptographic identity: an Ed25519 private key plus
// its derived public key. The private key stays in-process; the public key
// is safe to share via proto messages.
type Identity struct {
	priv ed25519.PrivateKey
}

// NewIdentity generates a fresh Ed25519 keypair.
func NewIdentity() (*Identity, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return &Identity{priv: priv}, nil
}

// PublicKey returns the 32-byte Ed25519 public key, suitable for inclusion
// in a protobuf NodeID.public_key field. Deterministic: same key every call.
func (id *Identity) PublicKey() []byte {
	return id.priv.Public().(ed25519.PublicKey)
}

// Sign produces an Ed25519 signature over data. The signature is 64 bytes.
func (id *Identity) Sign(data []byte) []byte {
	return ed25519.Sign(id.priv, data)
}

// Verify checks that sig was produced by the holder of the private key
// corresponding to publicKey, over exactly data. Returns ErrInvalidSignature
// if the signature does not verify (tampered data, wrong key, bad signature).
func Verify(publicKey, data, sig []byte) error {
	if len(publicKey) != ed25519.PublicKeySize {
		return ErrInvalidSignature
	}
	if len(sig) != ed25519.SignatureSize {
		return ErrInvalidSignature
	}
	if !ed25519.Verify(ed25519.PublicKey(publicKey), data, sig) {
		return ErrInvalidSignature
	}
	return nil
}

// HandshakeSignData returns the bytes to sign/verify for a Handshake request.
// Signing just the UUID proves "I hold the private key for this public key"
// without depending on proto serialization.
func HandshakeSignData(uuid string) []byte {
	return []byte(uuid)
}

// HeartbeatSignData returns the bytes to sign/verify for a Heartbeat request.
// UUID + timestamp binds the signature to a specific point in time, preventing
// replay of old heartbeats.
func HeartbeatSignData(uuid string, timestamp uint64) []byte {
	data := []byte(uuid)
	ts := make([]byte, 8)
	binary.BigEndian.PutUint64(ts, timestamp)
	return append(data, ts...)
}
