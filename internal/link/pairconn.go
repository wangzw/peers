package link

import (
	"context"

	"github.com/wangzw/peers/internal/link/linkpb"
	"github.com/wangzw/peers/internal/pairing"
)

// pairConn adapts a conn to the interface pairing speaks during a handshake.
// Pairing gets the raw frames of one TLS connection; it never parses link
// frames.
type pairConn struct {
	c *conn
	// version is the link protocol version this Link speaks, including a test
	// override. Pairing sends it in PairRequest and compares it there.
	version uint32
	// linger is link's rejection linger, shared with rejectAndLinger. It sends
	// the pairing reject frame and keeps the connection open until the frame
	// has had time to leave the transport's write buffer.
	linger func(*conn, func(context.Context) error) error
}

var _ pairing.PairConn = (*pairConn)(nil)

// newPairConn wraps c for pairing. version is the Link's protocol version and
// linger must be the containing Link's lingerAfterSend.
func newPairConn(c *conn, version uint32, linger func(*conn, func(context.Context) error) error) pairing.PairConn {
	return &pairConn{c: c, version: version, linger: linger}
}

// PeerFingerprint returns the certificate fingerprint the peer presented.
func (p *pairConn) PeerFingerprint() pairing.Fingerprint {
	return p.c.id.fp
}

// PeerPublicKey returns a copy of the peer's raw Ed25519 public key.
func (p *pairConn) PeerPublicKey() []byte {
	return append([]byte(nil), p.c.id.pub...)
}

// RemoteAddr returns the peer's TCP address in host:port form.
func (p *pairConn) RemoteAddr() string {
	return p.c.id.remote
}

// ProtocolVersion returns the link protocol version this Link speaks on the
// connection.
func (p *pairConn) ProtocolVersion() uint32 {
	return p.version
}

// Send writes one unprompted frame. Pairing frames carry no request id and are
// not replies.
func (p *pairConn) Send(ctx context.Context, kind uint32, payload []byte) error {
	return p.c.sendDirect(ctx, &linkpb.Frame{Kind: kind, Payload: payload})
}

// SendAndLinger sends one unprompted frame and uses link's rejection linger:
// the connection stays open until the frame has left the transport's write
// buffer, and at most rejectLinger in any case. Pairing uses it for PairReject,
// which the peer must read instead of an abrupt close.
func (p *pairConn) SendAndLinger(ctx context.Context, kind uint32, payload []byte) error {
	return p.linger(p.c, func(sendCtx context.Context) error {
		return p.c.sendDirect(sendCtx, &linkpb.Frame{Kind: kind, Payload: payload})
	})
}

// Receive returns the next frame in stream order.
func (p *pairConn) Receive(ctx context.Context) (uint32, []byte, error) {
	f, err := p.c.recv(ctx)
	if err != nil {
		return 0, nil, err
	}
	return f.GetKind(), f.GetPayload(), nil
}

// Unread puts a frame back at the head of the receive queue.
func (p *pairConn) Unread(kind uint32, payload []byte) {
	p.c.unread(&linkpb.Frame{Kind: kind, Payload: payload})
}

// Close closes the connection and reports success; pairing may call it more
// than once.
func (p *pairConn) Close() error {
	p.c.closeAsync(FailProtocol)
	return nil
}
