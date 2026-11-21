package link

import (
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/link/linkpb"
)

// bufferedSendStream models the gRPC client transport on a dialed connection.
// Send hands a frame to the transport and returns before it reaches the peer;
// the transport writes the buffered frames later in flush. Closing the
// connection before that write discards whatever is still buffered, which is
// how a Reject sent just before closeAsync can be lost.
type bufferedSendStream struct {
	incoming  chan *linkpb.Frame
	delivered chan *linkpb.Frame

	mu        sync.Mutex
	pending   []*linkpb.Frame
	aborted   bool
	closed    chan struct{}
	closeOnce sync.Once
}

func newBufferedSendStream() *bufferedSendStream {
	return &bufferedSendStream{
		incoming:  make(chan *linkpb.Frame, 1),
		delivered: make(chan *linkpb.Frame, 4),
		closed:    make(chan struct{}),
	}
}

func (s *bufferedSendStream) Send(f *linkpb.Frame) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.aborted {
		return errPipeClosed
	}
	s.pending = append(s.pending, f)
	return nil
}

func (s *bufferedSendStream) Recv() (*linkpb.Frame, error) {
	select {
	case f := <-s.incoming:
		return f, nil
	case <-s.closed:
		return nil, errPipeClosed
	}
}

// CloseSend models the transport's ordered half-close: buffered frames are
// written before the stream ends.
func (s *bufferedSendStream) CloseSend() error {
	s.flush()
	return nil
}

// flush is the transport writer: it hands the buffered frames to the peer.
func (s *bufferedSendStream) flush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.aborted {
		s.pending = nil
		return
	}
	for _, f := range s.pending {
		s.delivered <- f
	}
	s.pending = nil
}

func (s *bufferedSendStream) abort() {
	s.mu.Lock()
	s.aborted = true
	s.pending = nil
	s.mu.Unlock()
	s.closeOnce.Do(func() { close(s.closed) })
}

// TestDialerVersionMismatchLingersUntilTheRejectIsWritten reproduces the lost
// Reject without relying on the timing race of a real transport. The stream
// holds every frame until flush, which stands in for the gRPC writer running
// after the handshake returned. If the dialer closes the connection as soon as
// the version mismatch is seen, abort discards the buffered Reject and the
// accepting side never receives it.
func TestDialerVersionMismatchLingersUntilTheRejectIsWritten(t *testing.T) {
	h := newHandshakeHarness(t)
	fp := Fingerprint{0xB7}
	stream := newBufferedSendStream()
	c := newConn(stream, stream.abort, peerIdentity{fp: fp, pub: make([]byte, 32), remote: "127.0.0.1:1"}, 8)
	c.start()

	stream.incoming <- encodeHello(linkpb.ProtocolVersion+1, "acceptor", hsNonce(2), nil)

	res := waitHSDial(t, hsDial(t, h.l, c))
	if res.failure != FailVersionIncompatible {
		t.Fatalf("failure = %v, want FailVersionIncompatible (err: %v)", res.failure, res.err)
	}
	if res.peerVersion != linkpb.ProtocolVersion+1 {
		t.Fatalf("peer version = %d, want %d", res.peerVersion, linkpb.ProtocolVersion+1)
	}

	// This is the dial loop's failure path for a failed handshake.
	c.closeAfterFailedHandshake(FailVersionIncompatible)

	// The transport writer runs after the handshake. A connection that was
	// already aborted never gets its buffered Reject written.
	stream.flush()

	deadline := time.After(eventTimeout)
	for {
		select {
		case f := <-stream.delivered:
			if f.GetKind() == linkpb.KindHello {
				continue
			}
			if f.GetKind() != linkpb.KindReject {
				t.Fatalf("the dialer sent frame kind %d after the Hello, want Reject", f.GetKind())
			}
			reason, err := decodeReject(f)
			if err != nil {
				t.Fatalf("decode the dialer's Reject: %v", err)
			}
			if reason != linkpb.RejectReason_REJECT_REASON_PROTOCOL_VERSION_INCOMPATIBLE {
				t.Fatalf("reject reason = %v, want PROTOCOL_VERSION_INCOMPATIBLE", reason)
			}
			return
		case <-deadline:
			t.Fatal("the accepting side never received the dialer's Reject: the dialer closed before the transport wrote it")
		}
	}
}
