package pairing

import (
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"time"
)

// Fingerprint is SHA-256 over a certificate's DER SubjectPublicKeyInfo. It is
// comparable and usable as a map key. A zero Fingerprint means "no identity".
type Fingerprint [32]byte

// String renders the fingerprint as 64 lowercase hexadecimal characters.
func (f Fingerprint) String() string { return hex.EncodeToString(f[:]) }

// IsZero reports whether f is the zero fingerprint.
func (f Fingerprint) IsZero() bool { return f == Fingerprint{} }

// Role is this Broker's role in one peer relationship.
type Role uint8

const (
	// RoleUnknown means the fingerprint is not a peer.
	RoleUnknown Role = iota
	// RoleDialer means this Broker initiated the pairing and redials.
	RoleDialer
	// RoleAcceptor means the peer initiated the pairing and this Broker waits.
	RoleAcceptor
)

// String returns "dialer", "acceptor" or "unknown".
func (r Role) String() string { return roleName(r) }

// AddressSource names where a candidate address came from.
type AddressSource uint8

const (
	// SourceAcceptor is an address the acceptor advertised.
	SourceAcceptor AddressSource = iota + 1
	// SourceCard is an address carried by the peer's directory card.
	SourceCard
	// SourcePairing is the address dialed during pairing.
	SourcePairing
)

// String returns "acceptor", "card" or "pairing".
func (s AddressSource) String() string {
	switch s {
	case SourceAcceptor:
		return "acceptor"
	case SourceCard:
		return "card"
	case SourcePairing:
		return "pairing"
	default:
		return "unknown"
	}
}

// Candidate is one dialable address and the source it came from.
type Candidate struct {
	Source AddressSource // where the address came from
	Addr   string        // a dialable host:port
}

// Peer is the exported view of one peer-table entry. Peers returns copies:
// mutating Candidates or PublicKey does not change the table.
type Peer struct {
	Fingerprint   Fingerprint // identifies the peer
	PublicKey     []byte      // raw 32-byte Ed25519 public key
	Alias         string      // self-reported alias
	LocalAlias    string      // empty when none was assigned
	Role          Role        // this Broker's role in the pair
	PairedAt      time.Time   // when this side's handshake succeeded
	Candidates    []Candidate // dialer peers only
	LastConnected string      // dialer peers only
}

// Sentinel errors. Callers match them with errors.Is.
//
// ErrNeedsLocalAlias is returned when a handshake needs a local alias the caller
// must supply, ErrNotListening when no listen address is configured, ErrIdentity
// when the identity file is rejected, ErrStore when the peer table or
// configuration cannot be read or written, and ErrProtocol when a handshake frame
// is malformed.
var (
	ErrNotOpen         = errors.New("the pairing is not open")
	ErrNoTransport     = errors.New("the transport is nil")
	ErrInvalidAddress  = errors.New("invalid address")
	ErrInvalidAlias    = errors.New("invalid alias")
	ErrNeedsLocalAlias = errors.New("a local alias is required")
	ErrPeerUnreachable = errors.New("the peer is unreachable")
	ErrNotListening    = errors.New("not listening")
	ErrIdentity        = errors.New("identity error")
	ErrStore           = errors.New("peer table error")
	ErrPairingRejected = errors.New("pairing rejected")
	ErrProtocol        = errors.New("protocol error")

	// ErrDialFailed marks an error caused by the dial itself: TCP connect, the
	// TLS handshake or the dial deadline. Transport implementations wrap such
	// errors with it; Connect maps only this sentinel to ErrPeerUnreachable.
	ErrDialFailed = errors.New("dial failed")
)

// RejectReason is the local reason a pairing was refused.
type RejectReason uint8

// RejectReason values. ReasonTimeout and ReasonNotConnected are local only and
// never travel on the wire.
const (
	ReasonKeyInvalid RejectReason = iota + 1
	ReasonVerificationFailed
	ReasonTryLater
	ReasonTimeout
	ReasonNotConnected
	ReasonKeyExpired
	ReasonVersionIncompatible
)

// PairingError is a refusal with a reason and a human-readable detail. Its
// message starts with "Not connected" and it unwraps to ErrPairingRejected, so
// brokers can map it to the pairing_rejected error code.
type PairingError struct {
	Reason RejectReason // the refusal kind
	Detail string       // the explanation shown after "Not connected: "
}

// Error implements error.
func (e *PairingError) Error() string { return "Not connected: " + e.Detail }

// Unwrap reports ErrPairingRejected so errors.Is(err, ErrPairingRejected) holds.
func (e *PairingError) Unwrap() error { return ErrPairingRejected }

// Pairing holds the identity, the peer table, the one-time key table and the
// injected transport. Open returns one instance; all stateful operations are
// methods on it. A zero Pairing answers read-only queries as if the peer table
// were empty and refuses operations that need the identity or the transport.
type Pairing struct {
	settings  settings
	transport Transport
	clock     clock
	identity  *identity
	peers     *peerTable
	keys      *keyTable

	storeOps fileOps
	randRead func([]byte) (int, error) // crypto/rand.Read, injectable only in tests
}

// Verify checks an Ed25519 signature made by the key behind pub. pub is either a
// raw 32-byte public key or a DER SubjectPublicKeyInfo. A wrong key or signature
// length is a false result, never a panic.
func Verify(pub, data, sig []byte) bool {
	if len(sig) != ed25519.SignatureSize {
		return false
	}
	switch len(pub) {
	case ed25519.PublicKeySize:
		return ed25519.Verify(pub, data, sig)
	case len(ed25519SubjectPublicKeyInfoPrefix) + ed25519.PublicKeySize:
		key, err := parseEd25519PublicKeyInfo(pub)
		if err != nil {
			return false
		}
		return ed25519.Verify(key, data, sig)
	default:
		return false
	}
}

// FingerprintOf derives a fingerprint from a raw 32-byte Ed25519 public key or a
// DER SubjectPublicKeyInfo. Any other length yields the zero Fingerprint.
func FingerprintOf(pub []byte) Fingerprint { return fingerprintOfPublicKey(pub) }
