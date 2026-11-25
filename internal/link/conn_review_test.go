package link

import (
	"testing"

	"github.com/wangzw/peers/internal/link/linkpb"
)

// TestPreAuthBacklogAtMostOneFrame checks that a connection that never
// authenticates buffers at most one frame before its Reject.
func TestPreAuthBacklogAtMostOneFrame(t *testing.T) {
	h := newHandshakeHarness(t)
	peer := Fingerprint{0xF5}
	c, _, p := h.newConn(t, peer)
	res := h.serve(c, nil)

	hsSend(t, p, encodeHello(linkpb.ProtocolVersion, "stranger", hsNonce(13), nil))
	for i := 0; i < connInQueueSize; i++ {
		hsSend(t, p, &linkpb.Frame{Kind: 200})
	}

	rej := hsRecv(t, p)
	if rej.GetKind() != linkpb.KindReject {
		t.Fatalf("first reply kind = %d, want reject", rej.GetKind())
	}
	if n := len(c.in); n > 1 {
		t.Fatalf("pre-authentication input backlog = %d frames, want at most 1", n)
	}

	h.clock.Advance(h.l.rejectLinger)
	hsWaitClosed(t, c)
	if r := waitHSResult(t, res); r.ok {
		t.Fatal("unknown fingerprint was installed")
	}
}
