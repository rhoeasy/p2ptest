package crypto

import (
	"bytes"
	"testing"
)

// Identity is a node's Ed25519 keypair — the cryptographic identity that
// proves "this message really came from the node claiming to have sent it".
// Sign produces a signature; Verify (a package function) checks it against
// a public key.

// Tracer bullet: sign-then-verify roundtrip on real data.
func TestIdentitySignAndVerify(t *testing.T) {
	alice, err := NewIdentity()
	if err != nil {
		t.Fatalf("NewIdentity failed: %v", err)
	}

	data := []byte("hello from alice")
	sig := alice.Sign(data)

	if err := Verify(alice.PublicKey(), data, sig); err != nil {
		t.Errorf("Verify failed for valid signature: %v", err)
	}
}

// Tampered data must be rejected — this is the whole point of signing.
func TestVerifyRejectsTamperedData(t *testing.T) {
	alice, _ := NewIdentity()
	sig := alice.Sign([]byte("original"))

	if err := Verify(alice.PublicKey(), []byte("tampered"), sig); err == nil {
		t.Error("Verify should reject tampered data, but it passed")
	}
}

// A signature from Alice must not verify against Bob's public key.
func TestVerifyRejectsWrongKey(t *testing.T) {
	alice, _ := NewIdentity()
	bob, _ := NewIdentity()

	sig := alice.Sign([]byte("from alice"))
	if err := Verify(bob.PublicKey(), []byte("from alice"), sig); err == nil {
		t.Error("Verify should reject signature verified against wrong key")
	}
}

// Public key must be deterministically derivable (so it can be sent in NodeInfo).
func TestPublicKeyIsStable(t *testing.T) {
	id, _ := NewIdentity()
	pk1 := id.PublicKey()
	pk2 := id.PublicKey()
	if !bytes.Equal(pk1, pk2) {
		t.Error("PublicKey() should return the same bytes on every call")
	}
}
