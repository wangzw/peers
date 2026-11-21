package pairing

import (
	"context"

	"github.com/wangzw/peers/internal/pairing/pairingpb"
)

// PairConn is one TLS connection handed to pairing. Frames are passed raw;
// pairing interprets only pairing frames and never parses link frames.
//
// The implementation is supplied by link. It must provide the peer identity
// read from the certificate presented on this connection, must return from
// Send and Receive when the context is canceled or expires, and must allow
// Close to be called more than once.
type PairConn interface {
	// PeerFingerprint is the certificate fingerprint the peer presented on
	// this TLS connection.
	PeerFingerprint() Fingerprint

	// PeerPublicKey is the peer's raw 32-byte Ed25519 public key, taken from
	// the same certificate.
	PeerPublicKey() []byte

	// RemoteAddr is the peer's network address in host:port form. Pairing uses
	// only its host for the audit merge key.
	RemoteAddr() string

	// ProtocolVersion is the link protocol version this side speaks on the
	// connection. pairing sends it in PairRequest and compares it there.
	ProtocolVersion() uint32

	// Send writes one frame and flushes it. It must return when ctx is
	// canceled or expires, returning ctx.Err().
	Send(ctx context.Context, kind uint32, payload []byte) error

	// Receive returns the next frame in stream order. It must return when ctx
	// is canceled or expires, returning ctx.Err().
	Receive(ctx context.Context) (kind uint32, payload []byte, err error)

	// Unread puts a frame back at the head of the receive queue.
	Unread(kind uint32, payload []byte)

	// Close closes the connection. It is safe to call more than once.
	Close() error
}

// finalSender is implemented by a PairConn whose transport writes
// asynchronously and therefore has to keep the connection open after its last
// frame. link's PairConn adapter implements it with the same linger it uses
// for its own Reject. A transport that delivers synchronously needs no linger
// and falls back to Send followed by Close.
type finalSender interface {
	SendAndLinger(ctx context.Context, kind uint32, payload []byte) error
}

// Frame kinds used during a pairing handshake. Only kindHello belongs to the
// link protocol: pairing checks it to recognise the frame that hands the
// connection to link, and never interprets its payload. The other values
// mirror the pairingpb message set so a mismatch is a compile-time error.
const (
	kindHello         uint32 = 1
	kindPairRequest          = pairingpb.KindPairRequest
	kindPairChallenge        = pairingpb.KindPairChallenge
	kindPairConfirm          = pairingpb.KindPairConfirm
	kindPairDone             = pairingpb.KindPairDone
	kindPairReject           = pairingpb.KindPairReject
)
