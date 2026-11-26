package pairing

import (
	"bytes"
	"crypto/ed25519"
	"crypto/tls"
	"testing"
)

// TestPairingFingerprintMatchesIdentity checks that Fingerprint derives from the
// identity's public key.
func TestPairingFingerprintMatchesIdentity(t *testing.T) {
	p := hsNewPairing(t, newHSSettings(t), &hsTransport{}, newHSClock())

	got := p.Fingerprint()
	want := FingerprintOf(p.identity.publicKey())
	if got != want {
		t.Fatalf("Fingerprint() = %s, want %s", got, want)
	}
	if got.IsZero() {
		t.Fatal("Fingerprint() is zero for an opened instance")
	}
	if got.String() != fingerprintHex(want) {
		t.Fatalf("Fingerprint().String() = %q, want %q", got.String(), fingerprintHex(want))
	}
}

// TestZeroPairingFingerprint checks that a zero or empty Pairing returns the
// zero fingerprint.
func TestZeroPairingFingerprint(t *testing.T) {
	var zero Pairing
	if got := zero.Fingerprint(); !got.IsZero() {
		t.Fatalf("zero Pairing Fingerprint() = %s, want the zero fingerprint", got)
	}
	if got := (&Pairing{}).Fingerprint(); !got.IsZero() {
		t.Fatalf("empty Pairing Fingerprint() = %s, want the zero fingerprint", got)
	}
}

// TestPairingTLSConfig checks the TLS version, client auth, skipped
// verification and the presented certificate.
func TestPairingTLSConfig(t *testing.T) {
	p := hsNewPairing(t, newHSSettings(t), &hsTransport{}, newHSClock())

	cfg := p.TLSConfig()
	if cfg == nil {
		t.Fatal("TLSConfig() = nil for an opened instance")
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Fatalf("MinVersion = %#x, want TLS 1.3", cfg.MinVersion)
	}
	if cfg.ClientAuth != tls.RequestClientCert {
		t.Fatalf("ClientAuth = %v, want RequestClientCert", cfg.ClientAuth)
	}
	if !cfg.InsecureSkipVerify {
		t.Fatal("InsecureSkipVerify = false, want true: identity comes from the fingerprint check")
	}
	if len(cfg.Certificates) != 1 {
		t.Fatalf("Certificates has %d entries, want 1", len(cfg.Certificates))
	}
	if len(cfg.Certificates[0].Certificate) == 0 {
		t.Fatal("the presented certificate has no DER bytes")
	}
	if !bytes.Equal(cfg.Certificates[0].Certificate[0], p.identity.cert.Certificate[0]) {
		t.Fatal("TLSConfig did not present the identity certificate")
	}
	if cfg.Certificates[0].PrivateKey == nil {
		t.Fatal("the presented certificate has no private key")
	}
}

// TestPairingTLSConfigReturnsIsolatedCertificateBytes checks that each
// TLSConfig call returns its own copy of the certificate bytes.
func TestPairingTLSConfigReturnsIsolatedCertificateBytes(t *testing.T) {
	p := hsNewPairing(t, newHSSettings(t), &hsTransport{}, newHSClock())
	original := append([]byte(nil), p.identity.cert.Certificate[0]...)

	first := p.TLSConfig()
	first.Certificates[0].Certificate[0][0] ^= 0xff

	second := p.TLSConfig()
	if !bytes.Equal(second.Certificates[0].Certificate[0], original) {
		t.Fatal("mutating the certificate bytes returned by TLSConfig changed the next call: " +
			"the returned certificate must be a deep copy")
	}
	if !bytes.Equal(p.identity.cert.Certificate[0], original) {
		t.Fatal("mutating the certificate bytes returned by TLSConfig changed the identity")
	}
}

// TestZeroPairingTLSConfigIsNil checks that a zero Pairing returns a nil TLS
// configuration.
func TestZeroPairingTLSConfigIsNil(t *testing.T) {
	var zero Pairing
	if cfg := zero.TLSConfig(); cfg != nil {
		t.Fatalf("zero Pairing TLSConfig() = %+v, want nil", cfg)
	}
}

// TestPairingSign checks that Sign produces a verifiable, deterministic
// signature with the identity key.
func TestPairingSign(t *testing.T) {
	p := hsNewPairing(t, newHSSettings(t), &hsTransport{}, newHSClock())
	data := []byte("peers pairing signature")

	sig := p.Sign(data)
	if len(sig) != 64 {
		t.Fatalf("signature length = %d, want 64", len(sig))
	}
	if !Verify(p.identity.publicKey(), data, sig) {
		t.Fatal("the signature does not verify with the identity public key")
	}
	if Verify(p.identity.publicKey(), []byte("different data"), sig) {
		t.Fatal("the signature verifies for different data")
	}
	if again := p.Sign(data); !bytes.Equal(sig, again) {
		t.Fatal("Ed25519 signing is not deterministic")
	}
}

// TestZeroPairingSignIsNil checks that a zero Pairing returns a nil signature.
func TestZeroPairingSignIsNil(t *testing.T) {
	var zero Pairing
	if sig := zero.Sign([]byte("data")); sig != nil {
		t.Fatalf("zero Pairing Sign() = %x, want nil", sig)
	}
}

// TestPairingPublicKey checks that PublicKey matches the fingerprint, verifies
// signatures and returns a copy.
func TestPairingPublicKey(t *testing.T) {
	p := hsNewPairing(t, newHSSettings(t), &hsTransport{}, newHSClock())
	data := []byte("peers pairing public key")

	pub := p.PublicKey()
	if len(pub) != ed25519.PublicKeySize {
		t.Fatalf("public key length = %d, want %d", len(pub), ed25519.PublicKeySize)
	}
	if got, want := FingerprintOf(pub), p.Fingerprint(); got != want {
		t.Fatalf("FingerprintOf(PublicKey()) = %s, want %s", got, want)
	}
	if !ed25519.Verify(pub, data, p.Sign(data)) {
		t.Fatal("Sign does not verify with PublicKey")
	}

	before := append([]byte(nil), pub...)
	pub[0] ^= 0xff
	if again := p.PublicKey(); !bytes.Equal(again, before) {
		t.Fatal("mutating the key returned by PublicKey changed the next call: " +
			"the returned key must be a copy")
	}
}

// TestZeroPairingPublicKeyIsNil checks that a zero, nil or empty Pairing
// returns a nil public key.
func TestZeroPairingPublicKeyIsNil(t *testing.T) {
	var zero Pairing
	if got := zero.PublicKey(); got != nil {
		t.Fatalf("zero Pairing PublicKey() = %x, want nil", got)
	}
	if got := (*Pairing)(nil).PublicKey(); got != nil {
		t.Fatalf("nil Pairing PublicKey() = %x, want nil", got)
	}
	if got := (&Pairing{}).PublicKey(); got != nil {
		t.Fatalf("empty Pairing PublicKey() = %x, want nil", got)
	}
}
