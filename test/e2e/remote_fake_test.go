//go:build e2e

package e2e

import (
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/testkit/fakesession"
	"github.com/wangzw/peers/test/e2e/harness"
)

// containerFakeBound is the fallback for each event wait for the fake proxy.
// The waits are event-driven: a frame wait wakes when the fake records a
// frame, and the connection wait wakes when the fake drops its last
// connection. Both are reached while the node agent reads or closes a
// simulated session's socket, and no product timer participates. The bound is
// therefore a safety net for a handler that never reaches the event, not a
// scheduling allowance; it uses the shared fallback rather than a few seconds.
const containerFakeBound = harness.EventTimeout

// containerInboxFrame is the frame the tests write into a container session's
// inbox socket.
const containerInboxFrame = `{"msg_id":"m1","type":"message","message":{"content":"held"}}`

// TestContainerSessionFakeProxy drives a container session's fakesession.Fake
// from the host through the node control protocol. The session and its frames
// live in the container process, so every operation travels over
// /v1/session/fake and the frames come back over /v1/session/frames.
func TestContainerSessionFakeProxy(t *testing.T) {
	c := harness.Up(t, harness.TopoSingle)
	node := c.Node("hostA")

	t.Run("release unblocks a held frame", func(t *testing.T) {
		session := node.StartSession(t, harness.SessionSpec{Name: "held", Mode: fakesession.HoldOpen})
		defer session.End(t)

		writeContainerInboxFrame(t, node, session.Fake().SocketPath(), containerInboxFrame)

		// Wait for the frame, not for the connection count: the fake registers
		// the held connection and the frame under one lock, so a frame observed
		// here guarantees Release finds the hold. The kernel lists the accepted
		// socket as soon as the client connects, which is before the handler
		// reads the line; releasing on that count can arrive before the hold
		// exists and be lost.
		frame := session.WaitFrame(t, containerFakeBound)
		if frame.MsgID != "m1" || frame.Content != "held" {
			t.Fatalf("frame = {MsgID: %q, Content: %q}, want m1/held", frame.MsgID, frame.Content)
		}

		session.Fake().Release()

		// The handler leaves the hold only when Release reaches the container.
		// The fake wakes this wait when the released handler drops its
		// connection, so a release that was lost or never arrived fails here.
		if !session.Fake().WaitNoConnections(containerFakeBound) {
			t.Fatalf("the released connection did not close within %s", containerFakeBound)
		}
	})

	t.Run("release is not lost when the handler reads late", func(t *testing.T) {
		// The delay pins the race this case exists for: the kernel lists the
		// accepted connection at once, while the handler waits before reading
		// the frame and registering the hold. A Release sent on the connection
		// count alone therefore arrives before the hold exists and is
		// discarded, leaving the handler blocked. Waiting for the frame first
		// closes that window.
		session := node.StartSession(t, harness.SessionSpec{
			Name:      "late",
			Mode:      fakesession.HoldOpen,
			BusyDelay: 500 * time.Millisecond,
		})
		defer session.End(t)

		writeContainerInboxFrame(t, node, session.Fake().SocketPath(), containerInboxFrame)

		// The frame is recorded when the fake takes the connection and
		// registers the hold. Waiting for it, rather than for the connection
		// count, keeps Release from arriving before the hold exists.
		frame := session.WaitFrame(t, containerFakeBound)
		if frame.MsgID != "m1" || frame.Content != "held" {
			t.Fatalf("frame = {MsgID: %q, Content: %q}, want m1/held", frame.MsgID, frame.Content)
		}

		session.Fake().Release()

		if !session.Fake().WaitNoConnections(containerFakeBound) {
			t.Fatalf("the released connection did not close within %s", containerFakeBound)
		}
	})

	t.Run("set mode applies to the next connection", func(t *testing.T) {
		session := node.StartSession(t, harness.SessionSpec{Name: "mode", Mode: fakesession.CloseImmediately})
		defer session.End(t)

		// CloseImmediately discards the connection without recording a frame.
		// After this change the next connection must be held and recorded.
		session.Fake().SetMode(fakesession.HoldOpen)
		writeContainerInboxFrame(t, node, session.Fake().SocketPath(), containerInboxFrame)

		frame := session.WaitFrame(t, containerFakeBound)
		if frame.MsgID != "m1" || frame.Content != "held" {
			t.Fatalf("frame = {MsgID: %q, Content: %q}, want m1/held", frame.MsgID, frame.Content)
		}
		session.Fake().Release()
	})
}

// writeContainerInboxFrame writes one frame line to a session's inbox socket
// from inside the container. netcat waits for the receiver to close, so the
// shell backgrounds it and returns at once; -N makes it half-close its side
// when the frame is written, which is what lets a released HoldOpen handler
// drain to EOF and finish.
func writeContainerInboxFrame(t *testing.T, node *harness.Node, socket, frame string) {
	t.Helper()
	script := "printf '%s\\n' " + shellQuote(frame) + " | nc -N -U " + shellQuote(socket) + " >/dev/null 2>&1 &"
	res := node.Exec(t, []string{"sh", "-c", script}, nil)
	if res.ExitCode != 0 {
		t.Fatalf("writing a frame to %s: exit code %d: %s", socket, res.ExitCode, res.Stderr)
	}
}

// shellQuote quotes value for sh. The frame and socket contain no single
// quotes, but quoting handles them anyway.
func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}
