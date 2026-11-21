package pairing

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

// bufferedLinger is the fake transport's linger window. It stands in for the
// one second link keeps a rejected connection open so the frame can leave the
// write buffer.
const bufferedLinger = time.Second

// bufferedPairConn models a transport whose Send only hands the frame to the
// write buffer and whose Close drops whatever is still buffered. A connection
// implementing finalSender flushes the buffer in SendAndLinger, waits until
// the peer has read the frame or the linger expires, and only then closes.
//
// eager selects the write path. The dialing side writes straight to its peer
// so the handshake can start; the accepting side buffers, which is the
// behaviour the rejection path has to survive.
type bufferedPairConn struct {
	peer    *bufferedPairConn
	eager   bool
	fp      Fingerprint
	pub     []byte
	remote  string
	version uint32

	mu     sync.Mutex
	queue  []hsFrame // frames this side may read
	buf    []hsFrame // frames Send accepted but has not written out
	closed bool
	wake   chan struct{}
	read   chan struct{} // signals that a queued frame was consumed
}

// newBufferedPair returns the dialing and accepting ends, each reporting the
// other's fingerprint and public key.
func newBufferedPair(dialFP Fingerprint, dialPub []byte, accFP Fingerprint, accPub []byte) (*bufferedPairConn, *bufferedPairConn) {
	dialer := &bufferedPairConn{
		fp:      accFP,
		pub:     append([]byte(nil), accPub...),
		remote:  "192.0.2.2:40002",
		version: hsProtocolVersion,
		eager:   true,
		wake:    make(chan struct{}, 1),
		read:    make(chan struct{}, 1),
	}
	acceptor := &bufferedPairConn{
		fp:      dialFP,
		pub:     append([]byte(nil), dialPub...),
		remote:  "192.0.2.1:40001",
		version: hsProtocolVersion,
		wake:    make(chan struct{}, 1),
		read:    make(chan struct{}, 1),
	}
	dialer.peer = acceptor
	acceptor.peer = dialer
	return dialer, acceptor
}

func (c *bufferedPairConn) notify() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *bufferedPairConn) PeerFingerprint() Fingerprint { return c.fp }
func (c *bufferedPairConn) PeerPublicKey() []byte        { return append([]byte(nil), c.pub...) }
func (c *bufferedPairConn) RemoteAddr() string           { return c.remote }
func (c *bufferedPairConn) ProtocolVersion() uint32      { return c.version }

func (c *bufferedPairConn) Send(ctx context.Context, kind uint32, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	frame := hsFrame{kind: kind, payload: append([]byte(nil), payload...)}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return io.ErrClosedPipe
	}
	if c.eager {
		peer := c.peer
		c.mu.Unlock()
		if peer == nil || !peer.deliver(frame) {
			return io.ErrClosedPipe
		}
		return nil
	}
	c.buf = append(c.buf, frame)
	c.mu.Unlock()
	return nil
}

// SendAndLinger is the finalSender path: it writes the buffered frame out and
// waits for the peer to read it, bounded by bufferedLinger, before closing.
func (c *bufferedPairConn) SendAndLinger(ctx context.Context, kind uint32, payload []byte) error {
	if err := c.Send(ctx, kind, payload); err != nil {
		return err
	}
	c.mu.Lock()
	frames := c.buf
	c.buf = nil
	peer := c.peer
	c.mu.Unlock()
	if peer == nil || !peer.deliver(frames...) {
		return c.Close()
	}
	timer := time.NewTimer(bufferedLinger)
	defer timer.Stop()
	select {
	case <-peer.read:
	case <-timer.C:
	case <-ctx.Done():
	}
	return c.Close()
}

// deliver queues frames for this side to read. A closed connection accepts
// nothing.
func (c *bufferedPairConn) deliver(frames ...hsFrame) bool {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return false
	}
	c.queue = append(c.queue, frames...)
	c.mu.Unlock()
	c.notify()
	return true
}

func (c *bufferedPairConn) Receive(ctx context.Context) (uint32, []byte, error) {
	for {
		c.mu.Lock()
		if len(c.queue) > 0 {
			frame := c.queue[0]
			c.queue = c.queue[1:]
			c.mu.Unlock()
			select {
			case c.read <- struct{}{}:
			default:
			}
			return frame.kind, frame.payload, nil
		}
		closed := c.closed
		c.mu.Unlock()
		if closed {
			return 0, nil, io.EOF
		}
		select {
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		case <-c.wake:
		}
	}
}

func (c *bufferedPairConn) Unread(kind uint32, payload []byte) {
	c.mu.Lock()
	c.queue = append([]hsFrame{{kind: kind, payload: append([]byte(nil), payload...)}}, c.queue...)
	c.mu.Unlock()
	c.notify()
}

// Close drops the frames that were never written out. Frames already delivered
// to the peer stay readable, so the peer reports the handshake failure after
// it has read them.
func (c *bufferedPairConn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.buf = nil
	peer := c.peer
	c.mu.Unlock()
	c.notify()
	if peer != nil {
		peer.mu.Lock()
		peer.closed = true
		peer.buf = nil
		peer.mu.Unlock()
		peer.notify()
	}
	return nil
}

// noLingerConn exposes only the PairConn method set, hiding SendAndLinger from
// pairing's finalSender assertion. The rejection then takes the Send-then-Close
// fallback.
type noLingerConn struct{ PairConn }

// runBufferedReject runs one Connect against one Accept over the buffered fake.
// linger selects whether the accepting end offers finalSender.
func runBufferedReject(t *testing.T, accP, initP *Pairing, key string, linger bool) (connectErr, acceptErr error) {
	t.Helper()
	dialConn, accConn := newBufferedPair(
		initP.identity.fingerprint(), initP.identity.publicKey(),
		accP.identity.fingerprint(), accP.identity.publicKey(),
	)
	tr, ok := initP.transport.(*hsTransport)
	if !ok {
		t.Fatalf("dialer transport is %T, want *hsTransport", initP.transport)
	}
	tr.raw = dialConn
	var accepted PairConn = accConn
	if !linger {
		accepted = &noLingerConn{PairConn: accConn}
	}
	acceptErrCh := make(chan error, 1)
	go func() { acceptErrCh <- accP.Accept(accepted) }()

	ctx, cancel := context.WithTimeout(context.Background(), testWaitBound)
	defer cancel()
	_, _, connectErr = initP.Connect(ctx, "127.0.0.1:17443", key, "")
	acceptErr = awaitAccept(acceptErrCh)
	return connectErr, acceptErr
}

func newRejectPairing(t *testing.T, alias string, clk *hsClock) *Pairing {
	t.Helper()
	s := newHSSettings(t)
	s.alias = alias
	return hsNewPairing(t, s, &hsTransport{}, clk)
}

func assertPairingError(t *testing.T, err error, reason RejectReason, detail string) {
	t.Helper()
	if !errors.Is(err, ErrPairingRejected) {
		t.Fatalf("error = %v, want it to wrap ErrPairingRejected", err)
	}
	var pairingErr *PairingError
	if !errors.As(err, &pairingErr) {
		t.Fatalf("error = %v (%T), want *PairingError", err, err)
	}
	if pairingErr.Reason != reason {
		t.Fatalf("reason = %v, want %v (error: %v)", pairingErr.Reason, reason, err)
	}
	if pairingErr.Detail != detail {
		t.Fatalf("detail = %q, want %q", pairingErr.Detail, detail)
	}
}

// TestRejectAcceptorLingerCarriesTheReason checks the finalSender path over a
// transport that only writes buffered frames: the acceptor lingers after the
// PairReject, so the dialer receives the reason and detail instead of a bare
// close.
func TestRejectAcceptorLingerCarriesTheReason(t *testing.T) {
	t.Run("wrong key", func(t *testing.T) {
		clk := newHSClock()
		accP := newRejectPairing(t, "acceptor", clk)
		initP := newRejectPairing(t, "dialer", clk)

		connectErr, acceptErr := runBufferedReject(t, accP, initP, "WRNG-WRNG-WRNG-WRNG", true)
		assertPairingError(t, connectErr, ReasonKeyInvalid, "pairing key is invalid")
		assertPairingError(t, acceptErr, ReasonKeyInvalid, "pairing key is invalid")
	})

	t.Run("expired key", func(t *testing.T) {
		clk := newHSClock()
		accP := newRejectPairing(t, "acceptor", clk)
		key, _, _, err := accP.CreateOneTimeKey()
		if err != nil {
			t.Fatalf("CreateOneTimeKey: %v", err)
		}
		clk.Advance(oneTimeKeyLifetime + time.Minute)
		initP := newRejectPairing(t, "dialer", clk)

		connectErr, acceptErr := runBufferedReject(t, accP, initP, key, true)
		want := rejectDetail(ReasonKeyExpired)
		assertPairingError(t, connectErr, ReasonKeyExpired, want)
		assertPairingError(t, acceptErr, ReasonKeyExpired, want)
	})
}

// TestBufferedTransportWithoutLingerLosesTheReason characterizes the old
// transport behaviour: Send only buffered the PairReject, Close dropped it, and
// the dialer saw a bare connection close. The fake really reproduces the
// defect, so the previous failure was not an artifact of the test harness.
func TestBufferedTransportWithoutLingerLosesTheReason(t *testing.T) {
	clk := newHSClock()
	accP := newRejectPairing(t, "acceptor", clk)
	initP := newRejectPairing(t, "dialer", clk)

	connectErr, acceptErr := runBufferedReject(t, accP, initP, "WRNG-WRNG-WRNG-WRNG", false)
	// The acceptor still knows why it refused.
	assertPairingError(t, acceptErr, ReasonKeyInvalid, "pairing key is invalid")
	// The buffered reject was dropped, so the dialer lost the reason.
	assertPairingError(t, connectErr, ReasonNotConnected, "connection closed during pairing")
}
