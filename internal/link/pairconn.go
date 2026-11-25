package link

import (
	"context"

	"github.com/wangzw/peers/internal/link/linkpb"
	"github.com/wangzw/peers/internal/pairing"
)

type pairConn struct {
	c       *conn
	version uint32
	linger  func(*conn, func(context.Context) error) error
}

var _ pairing.PairConn = (*pairConn)(nil)

// newPairConn wraps c for pairing. linger must be the containing Link's
// lingerAfterSend.
func newPairConn(c *conn, version uint32, linger func(*conn, func(context.Context) error) error) pairing.PairConn {
	return &pairConn{c: c, version: version, linger: linger}
}

func (p *pairConn) PeerFingerprint() pairing.Fingerprint {
	return p.c.id.fp
}

func (p *pairConn) PeerPublicKey() []byte {
	return append([]byte(nil), p.c.id.pub...)
}

func (p *pairConn) RemoteAddr() string {
	return p.c.id.remote
}

func (p *pairConn) ProtocolVersion() uint32 {
	return p.version
}

func (p *pairConn) Send(ctx context.Context, kind uint32, payload []byte) error {
	return p.c.sendDirect(ctx, &linkpb.Frame{Kind: kind, Payload: payload})
}

// SendAndLinger sends one frame through link's rejection linger, so pairing's
// PairReject reaches the peer instead of an abrupt close.
func (p *pairConn) SendAndLinger(ctx context.Context, kind uint32, payload []byte) error {
	return p.linger(p.c, func(sendCtx context.Context) error {
		return p.c.sendDirect(sendCtx, &linkpb.Frame{Kind: kind, Payload: payload})
	})
}

func (p *pairConn) Receive(ctx context.Context) (uint32, []byte, error) {
	f, err := p.c.recv(ctx)
	if err != nil {
		return 0, nil, err
	}
	return f.GetKind(), f.GetPayload(), nil
}

func (p *pairConn) Unread(kind uint32, payload []byte) {
	p.c.unread(&linkpb.Frame{Kind: kind, Payload: payload})
}

// Close closes the connection. Pairing may call it more than once.
func (p *pairConn) Close() error {
	p.c.closeAsync(FailProtocol)
	return nil
}
