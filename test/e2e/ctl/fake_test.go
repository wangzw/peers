package ctl_test

import (
	"context"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/test/e2e/ctl"
)

const inboxFrame = `{"msg_id":"m1","type":"message","message":{"content":"held"}}` + "\n"

// TestCtlFakeUnsupportedOperation checks that an operation the node agent
// cannot carry fails the request with a clear message and leaves the agent
// serving.
func TestCtlFakeUnsupportedOperation(t *testing.T) {
	_, client := newTestAgent(t)
	ctx := context.Background()

	info, err := client.StartSession(ctx, ctl.SessionStartRequest{Name: "unsupported"})
	if err != nil {
		t.Fatalf("starting a session: %v", err)
	}
	defer func() { _ = client.End(ctx, ctl.SessionIDRequest{ID: info.ID}) }()

	err = client.Fake(ctx, ctl.SessionFakeRequest{ID: info.ID, Op: "close"})
	if err == nil {
		t.Fatal("Fake accepted the unsupported operation close")
	}
	if !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("error = %q, want it to say the operation is unsupported", err)
	}

	if _, err := client.Frames(ctx, ctl.SessionFramesRequest{ID: info.ID}); err != nil {
		t.Fatalf("reading frames after the failed operation: %v", err)
	}
}

// TestCtlSessionErrors checks that a background failure recorded in a
// session's fake reaches the caller through the control endpoint, and that a
// second request does not return it again.
func TestCtlSessionErrors(t *testing.T) {
	_, client := newTestAgent(t)
	ctx := context.Background()

	info, err := client.StartSession(ctx, ctl.SessionStartRequest{Name: "errors"})
	if err != nil {
		t.Fatalf("starting a session: %v", err)
	}
	defer func() { _ = client.End(ctx, ctl.SessionIDRequest{ID: info.ID}) }()

	if err := client.Fake(ctx, ctl.SessionFakeRequest{
		ID: info.ID, Op: "inject_background_error", Arg: "injected failure",
	}); err != nil {
		t.Fatalf("injecting a background failure: %v", err)
	}

	res, err := client.Errors(ctx, ctl.SessionIDRequest{ID: info.ID})
	if err != nil {
		t.Fatalf("reading session failures: %v", err)
	}
	if len(res.Errors) != 1 || res.Errors[0] != "injected failure" {
		t.Fatalf("errors = %v, want the injected failure", res.Errors)
	}

	again, err := client.Errors(ctx, ctl.SessionIDRequest{ID: info.ID})
	if err != nil {
		t.Fatalf("reading session failures again: %v", err)
	}
	if len(again.Errors) != 0 {
		t.Fatalf("errors after the first read = %v, want none", again.Errors)
	}
}

// TestCtlFakeRelease checks that release ends a HoldOpen connection and that
// the frame it held was recorded.
func TestCtlFakeRelease(t *testing.T) {
	_, client := newTestAgent(t)
	ctx := context.Background()

	info, err := client.StartSession(ctx, ctl.SessionStartRequest{Name: "held", Mode: "hold-open"})
	if err != nil {
		t.Fatalf("starting a session: %v", err)
	}
	defer func() { _ = client.End(ctx, ctl.SessionIDRequest{ID: info.ID}) }()

	conn, err := net.Dial("unix", info.SocketPath)
	if err != nil {
		t.Fatalf("dialing the inbox socket: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write([]byte(inboxFrame)); err != nil {
		t.Fatalf("writing a frame: %v", err)
	}

	frames, err := client.Frames(ctx, ctl.SessionFramesRequest{ID: info.ID, WaitN: 1, TimeoutMS: 3000})
	if err != nil {
		t.Fatalf("waiting for the frame: %v", err)
	}
	if !frames.OK || len(frames.Frames) != 1 {
		t.Fatalf("frames = %+v, want the held frame", frames)
	}
	if frames.Frames[0].MsgID != "m1" {
		t.Fatalf("frame msg_id = %q, want m1", frames.Frames[0].MsgID)
	}

	if err := client.Fake(ctx, ctl.SessionFakeRequest{ID: info.ID, Op: "release"}); err != nil {
		t.Fatalf("releasing the held connection: %v", err)
	}

	// Release lets the handler return; the connection still ends only once the sender closes its side, so the test closes it here.
	if err := conn.Close(); err != nil {
		t.Fatalf("closing the inbox connection: %v", err)
	}
	// InboxConnections reads /proc/net/unix, so the count is observable on Linux only.
	// The release and frame checks above have already run; only the count is skipped.
	if runtime.GOOS != "linux" {
		t.Skip("InboxConnections reads /proc/net/unix")
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		n, err := client.InboxConnections(ctx, ctl.SessionIDRequest{ID: info.ID})
		if err != nil {
			t.Fatalf("counting inbox connections: %v", err)
		}
		if n.N == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("InboxConnections() = %d after release, want 0", n.N)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestCtlFakeSetMode checks that set_mode changes how the next connection is
// handled: a CloseImmediately session switched to HoldOpen must record a frame.
func TestCtlFakeSetMode(t *testing.T) {
	_, client := newTestAgent(t)
	ctx := context.Background()

	info, err := client.StartSession(ctx, ctl.SessionStartRequest{Name: "mode", Mode: "close-immediately"})
	if err != nil {
		t.Fatalf("starting a session: %v", err)
	}
	defer func() { _ = client.End(ctx, ctl.SessionIDRequest{ID: info.ID}) }()

	if err := client.Fake(ctx, ctl.SessionFakeRequest{ID: info.ID, Op: "set_mode", Arg: "hold-open"}); err != nil {
		t.Fatalf("changing the session mode: %v", err)
	}

	conn, err := net.Dial("unix", info.SocketPath)
	if err != nil {
		t.Fatalf("dialing the inbox socket: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write([]byte(inboxFrame)); err != nil {
		t.Fatalf("writing a frame: %v", err)
	}

	frames, err := client.Frames(ctx, ctl.SessionFramesRequest{ID: info.ID, WaitN: 1, TimeoutMS: 3000})
	if err != nil {
		t.Fatalf("waiting for the frame: %v", err)
	}
	if !frames.OK || len(frames.Frames) != 1 {
		t.Fatalf("frames = %+v, want one recorded after the mode change", frames)
	}
	if err := client.Fake(ctx, ctl.SessionFakeRequest{ID: info.ID, Op: "release"}); err != nil {
		t.Fatalf("releasing the held connection: %v", err)
	}
}
