package link

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/link/linkpb"
)

// noLinger is a synchronous rejection send for the contract test: the frame is
// handed to the pipe and the connection closes at once. The contract test does
// not exercise the linger itself.
func noLinger(c *conn, send func(context.Context) error) error {
	if err := send(context.Background()); err != nil {
		return err
	}
	c.closeAsync(FailProtocol)
	return nil
}

// TestPairConnContract checks the pairing.PairConn behaviours pairing relies
// on: identity accessors, a frame pushed back being received first, Send
// producing an unprompted frame, ctx cancellation, and repeated Close.
func TestPairConnContract(t *testing.T) {
	a, b := newPipe(t)
	fp := Fingerprint{0xab, 0xcd}
	pub := make([]byte, 32)
	for i := range pub {
		pub[i] = byte(i)
	}
	id := peerIdentity{fp: fp, pub: pub, remote: "192.0.2.7:17443"}
	c := newConn(a, a.closeBoth, id, 4)
	c.start()
	pc := newPairConn(c, linkpb.ProtocolVersion, noLinger)

	if got := pc.PeerFingerprint(); got != fp {
		t.Fatalf("PeerFingerprint() = %v, want %v", got, fp)
	}
	gotPub := pc.PeerPublicKey()
	if string(gotPub) != string(pub) {
		t.Fatalf("PeerPublicKey() = %x, want %x", gotPub, pub)
	}
	gotPub[0] ^= 0xff
	if pc.PeerPublicKey()[0] != pub[0] {
		t.Fatal("PeerPublicKey returns a slice aliasing the identity")
	}
	if got := pc.RemoteAddr(); got != id.remote {
		t.Fatalf("RemoteAddr() = %q, want %q", got, id.remote)
	}
	if got := pc.ProtocolVersion(); got != linkpb.ProtocolVersion {
		t.Fatalf("ProtocolVersion() = %d, want %d", got, linkpb.ProtocolVersion)
	}

	// A frame pushed back is received before frames from the stream.
	pc.Unread(100, []byte("pair-request"))
	kind, payload, err := pc.Receive(context.Background())
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if kind != 100 || string(payload) != "pair-request" {
		t.Fatalf("Receive got kind=%d payload=%q, want kind=100 payload=pair-request", kind, payload)
	}

	// Send writes an unprompted frame.
	if err := pc.Send(context.Background(), 101, []byte("ok")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	f, err := b.Recv()
	if err != nil {
		t.Fatalf("recv sent frame: %v", err)
	}
	if f.GetKind() != 101 || string(f.GetPayload()) != "ok" {
		t.Fatalf("sent frame kind=%d payload=%q, want kind=101 payload=ok", f.GetKind(), f.GetPayload())
	}
	if f.GetRequestId() != 0 || f.GetIsReply() {
		t.Fatalf("sent frame id=%d isReply=%v, want id=0 isReply=false", f.GetRequestId(), f.GetIsReply())
	}

	// Send returns the context error while the peer's stream blocks.
	block := make(chan struct{})
	a.blockSend = block
	// The stream is blocked, so Send can return only through the deadline: the
	// deadline is the assertion, not a fallback, and no event must precede it.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := pc.Send(ctx, 103, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Send error = %v, want context.DeadlineExceeded", err)
	}
	close(block)

	// Close is idempotent.
	if err := pc.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := pc.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	select {
	case <-c.done():
	default:
		t.Fatal("Close left the connection open")
	}
}
