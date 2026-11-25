package link

import (
	"context"
	"testing"
)

// TestPairConnSendAndLingerHoldsTheConnection checks that SendAndLinger
// delivers the frame and keeps the connection open until the linger expires.
func TestPairConnSendAndLingerHoldsTheConnection(t *testing.T) {
	h := newHandshakeHarness(t)
	local, peer := newPipe(t)
	c := newConn(local, local.closeBoth, peerIdentity{
		fp:     Fingerprint{0x5A},
		pub:    make([]byte, 32),
		remote: "127.0.0.1:1",
	}, 8)
	c.start()
	pc := newPairConn(c, h.l.protocolVersion, h.l.lingerAfterSend).(*pairConn)

	payload := []byte("pair-reject")
	if err := pc.SendAndLinger(context.Background(), 104, payload); err != nil {
		t.Fatalf("SendAndLinger: %v", err)
	}

	f, err := peer.Recv()
	if err != nil {
		t.Fatalf("read the rejected frame: %v", err)
	}
	if f.GetKind() != 104 || string(f.GetPayload()) != string(payload) {
		t.Fatalf("frame kind=%d payload=%q, want kind=104 payload=%q", f.GetKind(), f.GetPayload(), payload)
	}

	hsAssertNotClosed(t, c)

	h.clock.Advance(h.l.rejectLinger)
	hsWaitClosed(t, c)
}
