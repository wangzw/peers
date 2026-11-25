package link

import (
	"context"
	"fmt"
	"time"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/link/linkpb"
	"github.com/wangzw/peers/internal/pairing"
	"github.com/wangzw/peers/internal/pairing/pairingpb"
)

func (l *Link) nonce() ([]byte, error) {
	b := make([]byte, nonceSize)
	n, err := l.randRead(b)
	if err != nil {
		return nil, err
	}
	if n != len(b) {
		return nil, fmt.Errorf("the random source returned %d of %d bytes", n, len(b))
	}
	return b, nil
}

func (l *Link) timerCtx(d time.Duration) (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	stop := l.clock.AfterFunc(d, cancel)
	return ctx, func() {
		if stop != nil {
			stop()
		}
		cancel()
	}
}

// sendHalfCloser is the send half of a gRPC client stream. Closing it after the
// Reject writes the frames still buffered before the half-close; server streams
// have no send-side half-close.
type sendHalfCloser interface {
	CloseSend() error
}

// lingerAfterSend sends one frame and keeps the connection open for at most
// rejectLinger, the same deadline bounding the write, then closes it. Its
// goroutine must not outlive the connection or the Link and stops the timer
// when it exits. send writes exactly one frame.
func (l *Link) lingerAfterSend(c *conn, send func(context.Context) error) error {
	c.lingering.Store(true)
	ctx, cancel := context.WithCancel(l.rootCtx)
	stop := l.clock.AfterFunc(l.rejectLinger, cancel)
	go func() {
		select {
		case <-ctx.Done():
		case <-c.done():
		}
		if stop != nil {
			stop()
		}
		cancel()
		c.closeAsync(FailProtocol)
		if l.onRejectLingerExit != nil {
			l.onRejectLingerExit()
		}
	}()
	if err := send(ctx); err != nil {
		cancel()
		return err
	}
	// Half-close so the Reject ends with an orderly write, not the abrupt close
	// at the end of the linger; the connection stays up until the linger ends.
	if hc, ok := c.stream.(sendHalfCloser); ok {
		_ = hc.CloseSend()
	}
	return nil
}

// rejectAndLinger sends a Reject through the linger: a send returns once the
// frame is buffered, so closing right away can drop it.
func (l *Link) rejectAndLinger(c *conn, reason linkpb.RejectReason) {
	_ = l.lingerAfterSend(c, func(ctx context.Context) error {
		return c.sendDirect(ctx, encodeReject(reason))
	})
}

func (l *Link) rejectUnknown(c *conn, fp Fingerprint) {
	l.rejectAndLinger(c, linkpb.RejectReason_REJECT_REASON_NOT_A_PEER)
	event := config.Event{
		Time:   l.clock.Now(),
		Event:  config.EventConnectionRejected,
		Peer:   &config.PeerRef{Fingerprint: fp},
		Detail: "not a peer",
	}
	if source := sourcePrefix(c.id.remote); source != "" {
		event.MergeKey = source
		event.Detail = "not a peer from " + source
	}
	l.settings.Audit(event)
}

func (l *Link) auditPeerRejected(fp Fingerprint) {
	l.settings.Audit(config.Event{
		Time:  l.clock.Now(),
		Event: config.EventRejectedByPeer,
		Peer: &config.PeerRef{
			Fingerprint: fp,
			Alias:       l.peers.EffectiveAlias(fp),
		},
	})
	l.logger.Warn("link: peer no longer treats this Broker as a peer",
		"peer", fp.String())
}

// closeAfterFailedHandshake is a no-op when a rejection linger owns the close.
func (c *conn) closeAfterFailedHandshake(reason FailReason) {
	if c.lingering.Load() {
		return
	}
	c.closeAsync(reason)
}

func failedHandshakeReason(failure FailReason) FailReason {
	if failure == FailNone {
		return FailConnectionLost
	}
	return failure
}

func failFromReject(r linkpb.RejectReason) FailReason {
	switch r {
	case linkpb.RejectReason_REJECT_REASON_NOT_A_PEER:
		return FailNotAPeer
	case linkpb.RejectReason_REJECT_REASON_ROLE_MISMATCH:
		return FailRoleMismatch
	case linkpb.RejectReason_REJECT_REASON_PROTOCOL_VERSION_INCOMPATIBLE:
		return FailVersionIncompatible
	default:
		return FailProtocol
	}
}

func (l *Link) noteHandshaking(fp Fingerprint) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if rec := l.records[fp]; rec != nil {
		rec.phase = PhaseHandshaking
	}
}

func (l *Link) logVersionMismatch(fp Fingerprint, peerVersion uint32) {
	l.logger.Warn("link: protocol version mismatch", "peer", fp,
		"local", l.protocolVersion, "peer_version", peerVersion)
}

func (l *Link) noteVersionFailure(fp Fingerprint, peerVersion uint32) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if rec := l.records[fp]; rec != nil {
		rec.lastFailure = FailVersionIncompatible
		rec.peerVersion = peerVersion
	}
}

func joinSalt(dialerNonce, acceptorNonce []byte) []byte {
	salt := make([]byte, 0, len(dialerNonce)+len(acceptorNonce))
	salt = append(salt, dialerNonce...)
	return append(salt, acceptorNonce...)
}

// serveInbound reads and judges the first frame of an accepted connection. On
// success the caller must continue with runConn(c, delivered); on every other
// exit the connection is closed or already closing.
func (l *Link) serveInbound(c *conn, t *trackedConn) (<-chan struct{}, bool) {
	fp := c.id.fp

	f, err := c.recv(context.Background())
	if err != nil {
		return nil, false
	}
	if t != nil {
		t.firstFrameSeen()
	}

	// Pairing pushes the following Hello back, so the normal path below sees it.
	if f.GetKind() == pairingpb.KindPairRequest && classify(f) == classState {
		c.unread(f)
		if err := l.peers.Accept(newPairConn(c, l.protocolVersion, l.lingerAfterSend)); err != nil {
			// A pairing rejection starts its own linger and owns the close; a
			// plain handshake failure has no linger and closes here.
			c.closeAfterFailedHandshake(FailProtocol)
			return nil, false
		}
		if t != nil {
			t.authenticated()
		}
		ctx, release := l.timerCtx(l.helloTimeout)
		next, err := c.recv(ctx)
		release()
		if err != nil {
			c.closeAsync(FailProtocol)
			return nil, false
		}
		f = next
	}

	if f.GetKind() != linkpb.KindHello {
		if l.peers.Role(fp) == pairing.RoleUnknown {
			l.rejectUnknown(c, fp)
			return nil, false
		}
		l.logger.Warn("link: unexpected first frame from a known peer", "peer", fp, "kind", f.GetKind())
		c.closeAsync(FailProtocol)
		return nil, false
	}

	// An unknown fingerprint is refused before its Hello is decoded: a stranger
	// that sends a malformed Hello must still get the "not a peer" Reject and
	// leave the connection_rejected audit record.
	if l.peers.Role(fp) == pairing.RoleUnknown {
		l.rejectUnknown(c, fp)
		return nil, false
	}

	hello, err := decodeHello(f)
	if err != nil {
		c.closeAsync(FailProtocol)
		return nil, false
	}

	gen := l.dropGeneration(fp)
	role := l.peers.Role(fp)
	switch role {
	case pairing.RoleUnknown:
		l.rejectUnknown(c, fp)
		return nil, false
	case pairing.RoleDialer:
		l.logger.Warn("link: inbound connection from a peer this Broker dials", "peer", fp)
		l.rejectAndLinger(c, linkpb.RejectReason_REJECT_REASON_ROLE_MISMATCH)
		return nil, false
	}

	if hello.GetProtocolVersion() != l.protocolVersion {
		l.logVersionMismatch(fp, hello.GetProtocolVersion())
		l.noteVersionFailure(fp, hello.GetProtocolVersion())
		l.rejectAndLinger(c, linkpb.RejectReason_REJECT_REASON_PROTOCOL_VERSION_INCOMPATIBLE)
		return nil, false
	}

	// Leave the unauthenticated registry before answering, so the frames the
	// peer sends after it reads our Hello are not charged to the pre-authentication
	// read budget.
	if t != nil {
		t.authenticated()
	}

	l.noteHandshaking(fp)
	l.peers.ObserveAlias(fp, hello.GetAlias())

	nonce, err := l.nonce()
	if err != nil {
		l.logger.Error("link: cannot generate a nonce", "peer", fp, "error", err)
		c.closeAsync(FailProtocol)
		return nil, false
	}
	ctx, release := l.timerCtx(l.helloTimeout)
	sendErr := c.sendDirect(ctx, encodeHello(l.protocolVersion, l.settings.Alias(), nonce, l.settings.Advertised()))
	release()
	if sendErr != nil {
		c.closeAsync(FailProtocol)
		return nil, false
	}

	if l.afterHelloSent != nil {
		l.afterHelloSent()
	}

	salt := joinSalt(hello.GetNonce(), nonce)

	c.role = pairing.RoleAcceptor
	delivered, ok := l.install(fp, pairing.RoleAcceptor, c, salt, gen)
	if !ok {
		c.closeAsync(FailConnectionLost)
		return nil, false
	}
	return delivered, true
}

// dialerHandshake exchanges Hello frames on a dialed connection whose
// fingerprint was already checked. On a version mismatch it sends Reject
// through rejectAndLinger, which owns the close; on every other failure the
// caller closes the connection.
func (l *Link) dialerHandshake(c *conn) (salt []byte, failure FailReason, peerVersion uint32, err error) {
	fp := c.id.fp

	nonce, err := l.nonce()
	if err != nil {
		return nil, FailProtocol, 0, err
	}

	ctx, release := l.timerCtx(l.helloTimeout)
	defer release()
	if sendErr := c.sendDirect(ctx, encodeHello(l.protocolVersion, l.settings.Alias(), nonce, nil)); sendErr != nil {
		return nil, FailConnectionLost, 0, sendErr
	}

	f, err := c.recv(ctx)
	if err != nil {
		return nil, FailConnectionLost, 0, err
	}

	switch f.GetKind() {
	case linkpb.KindReject:
		reason, decodeErr := decodeReject(f)
		if decodeErr != nil {
			return nil, FailProtocol, 0, decodeErr
		}
		return nil, failFromReject(reason), 0, fmt.Errorf("the peer rejected the link: %v", reason)

	case linkpb.KindHello:
		hello, decodeErr := decodeHello(f)
		if decodeErr != nil {
			return nil, FailProtocol, 0, decodeErr
		}
		if hello.GetProtocolVersion() != l.protocolVersion {
			l.logVersionMismatch(fp, hello.GetProtocolVersion())
			// Go through the linger: a send returns once the frame is buffered,
			// not when it reaches the wire, so closing right away can drop the
			// Reject.
			l.rejectAndLinger(c, linkpb.RejectReason_REJECT_REASON_PROTOCOL_VERSION_INCOMPATIBLE)
			return nil, FailVersionIncompatible, hello.GetProtocolVersion(),
				fmt.Errorf("the peer protocol version is %d, want %d", hello.GetProtocolVersion(), l.protocolVersion)
		}
		// The acceptor's address list must reach the peer table before the
		// link-establishment notification, or a later dial uses the stale list.
		l.peers.ObserveAlias(fp, hello.GetAlias())
		l.peers.ObserveAcceptorAddresses(fp, addrsFromPB(hello.GetAdvertisedAddresses()))
		return joinSalt(nonce, hello.GetNonce()), FailNone, 0, nil

	default:
		return nil, FailProtocol, 0, fmt.Errorf("%w: unexpected handshake frame kind %d", errMalformed, f.GetKind())
	}
}
