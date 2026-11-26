//go:build e2e

package e2e

import (
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/testkit/fakesession"
	"github.com/wangzw/peers/test/e2e/harness"
)

const containerFakeBound = harness.EventTimeout

const containerInboxFrame = `{"msg_id":"m1","type":"message","message":{"content":"held"}}`

// TestContainerSessionFakeProxy drives a container session's fakesession.Fake
// from the host through the node control protocol.
func TestContainerSessionFakeProxy(t *testing.T) {
	c := harness.Up(t, harness.TopoSingle)
	node := c.Node("hostA")

	t.Run("release unblocks a held frame", func(t *testing.T) {
		session := node.StartSession(t, harness.SessionSpec{Name: "held", Mode: fakesession.HoldOpen})
		defer session.End(t)

		writeContainerInboxFrame(t, node, session.Fake().SocketPath(), containerInboxFrame)

		// Wait for the frame, not the connection count: the fake records the
		// hold and the frame under one lock, while the kernel lists the
		// accepted socket before the handler reads the line.
		frame := session.WaitFrame(t, containerFakeBound)
		if frame.MsgID != "m1" || frame.Content != "held" {
			t.Fatalf("frame = {MsgID: %q, Content: %q}, want m1/held", frame.MsgID, frame.Content)
		}

		session.Fake().Release()

		if !session.Fake().WaitNoConnections(containerFakeBound) {
			t.Fatalf("the released connection did not close within %s", containerFakeBound)
		}
	})

	t.Run("release is not lost when the handler reads late", func(t *testing.T) {
		// The delay pins the race: the kernel lists the connection before the
		// handler registers the hold, so a Release sent on the connection count
		// alone would be lost.
		session := node.StartSession(t, harness.SessionSpec{
			Name:      "late",
			Mode:      fakesession.HoldOpen,
			BusyDelay: 500 * time.Millisecond,
		})
		defer session.End(t)

		writeContainerInboxFrame(t, node, session.Fake().SocketPath(), containerInboxFrame)

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

		session.Fake().SetMode(fakesession.HoldOpen)
		writeContainerInboxFrame(t, node, session.Fake().SocketPath(), containerInboxFrame)

		frame := session.WaitFrame(t, containerFakeBound)
		if frame.MsgID != "m1" || frame.Content != "held" {
			t.Fatalf("frame = {MsgID: %q, Content: %q}, want m1/held", frame.MsgID, frame.Content)
		}
		session.Fake().Release()
	})
}

// writeContainerInboxFrame backgrounds nc because it waits for the receiver to
// close; -N half-closes when the frame is written so a released HoldOpen
// handler drains to EOF.
func writeContainerInboxFrame(t *testing.T, node *harness.Node, socket, frame string) {
	t.Helper()
	script := "printf '%s\\n' " + shellQuote(frame) + " | nc -N -U " + shellQuote(socket) + " >/dev/null 2>&1 &"
	res := node.Exec(t, []string{"sh", "-c", script}, nil)
	if res.ExitCode != 0 {
		t.Fatalf("writing a frame to %s: exit code %d: %s", socket, res.ExitCode, res.Stderr)
	}
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}
