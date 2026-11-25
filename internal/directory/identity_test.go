package directory

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"testing"

	"github.com/wangzw/peers/internal/pairing"
)

var _ identitySource = (*pairing.Pairing)(nil)

type stubIdentitySource struct {
	fp  pairing.Fingerprint
	pub []byte
	key ed25519.PrivateKey
}

func (s *stubIdentitySource) Fingerprint() pairing.Fingerprint { return s.fp }
func (s *stubIdentitySource) PublicKey() []byte                { return append([]byte(nil), s.pub...) }
func (s *stubIdentitySource) Sign(data []byte) []byte          { return ed25519.Sign(s.key, data) }

func newTestSource(t *testing.T, seed byte) (*stubIdentitySource, ed25519.PublicKey) {
	t.Helper()
	var raw [ed25519.SeedSize]byte
	raw[0] = seed
	key := ed25519.NewKeyFromSeed(raw[:])
	pub := append(ed25519.PublicKey(nil), key.Public().(ed25519.PublicKey)...)
	return &stubIdentitySource{fp: pairing.FingerprintOf(pub), pub: pub, key: key}, pub
}

// TestNewIdentityMatchesSource checks that newIdentity returns an identity
// whose fingerprint, public key and signature match the source.
func TestNewIdentityMatchesSource(t *testing.T) {
	t.Parallel()
	src, pub := newTestSource(t, 1)

	id, err := newIdentity(src)
	if err != nil {
		t.Fatalf("newIdentity() error = %v, want nil", err)
	}
	if got := id.Fingerprint(); got != src.fp {
		t.Fatalf("Identity.Fingerprint() = %s, want %s", got, src.fp)
	}

	got := id.PublicKey()
	if len(got) != ed25519.PublicKeySize {
		t.Fatalf("Identity.PublicKey() length = %d, want %d", len(got), ed25519.PublicKeySize)
	}
	if !bytes.Equal(got, pub) {
		t.Fatalf("Identity.PublicKey() = %x, want %x", got, pub)
	}
	got[0] ^= 0xff
	if again := id.PublicKey(); !bytes.Equal(again, pub) {
		t.Fatal("mutating the key returned by PublicKey changed the next call")
	}

	data := []byte("signed record")
	if !ed25519.Verify(pub, data, id.Sign(data)) {
		t.Fatal("Signature does not verify with the source public key")
	}
}

// TestNewIdentityRejectsMismatch checks that newIdentity rejects a source whose
// public key does not match its fingerprint or is the wrong length.
func TestNewIdentityRejectsMismatch(t *testing.T) {
	t.Parallel()
	srcA, pubA := newTestSource(t, 2)
	_, pubB := newTestSource(t, 3)

	t.Run("different key", func(t *testing.T) {
		src := &stubIdentitySource{fp: srcA.fp, pub: pubB, key: srcA.key}
		if _, err := newIdentity(src); !errors.Is(err, ErrNoIdentity) {
			t.Fatalf("newIdentity() error = %v, want ErrNoIdentity", err)
		}
	})

	t.Run("wrong key length", func(t *testing.T) {
		src := &stubIdentitySource{fp: srcA.fp, pub: pubA[:ed25519.PublicKeySize-1], key: srcA.key}
		if _, err := newIdentity(src); !errors.Is(err, ErrNoIdentity) {
			t.Fatalf("newIdentity() error = %v, want ErrNoIdentity", err)
		}
	})
}

// TestNewIdentityWithoutIdentity checks that NewIdentity reports ErrNoIdentity
// when the pairing has no identity.
func TestNewIdentityWithoutIdentity(t *testing.T) {
	t.Parallel()
	for _, p := range []*pairing.Pairing{{}, nil} {
		id, err := NewIdentity(p)
		if err == nil {
			t.Fatalf("NewIdentity(%v) error = nil, want ErrNoIdentity", p)
		}
		if !errors.Is(err, ErrNoIdentity) {
			t.Fatalf("NewIdentity(%v) error = %v, want ErrNoIdentity", p, err)
		}
		if id != nil {
			t.Fatalf("NewIdentity(%v) = %v, want nil", p, id)
		}
	}
}
