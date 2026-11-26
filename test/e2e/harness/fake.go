package harness

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/testkit/fakesession"
	"github.com/wangzw/peers/test/e2e/ctl"
)

// fakeAnswerSlack is added to a frame wait's own timeout so the node agent's
// timeout fires before the client's.
const fakeAnswerSlack = 5 * time.Second

// FakeSession is the simulated session surface a test drives. A local session
// returns the real *fakesession.Fake; a container session returns a proxy that
// forwards each operation to the node agent. Methods the control protocol
// cannot express fail the test.
type FakeSession interface {
	// ConfigDir returns the configuration directory holding the registry record.
	ConfigDir() string
	// PID returns the session process ID.
	PID() int
	// RecordPath returns the path of the registry record.
	RecordPath() string
	// SocketPath returns the inbox socket path named by the record; it is empty
	// when no socket was bound.
	SocketPath() string
	// SetName changes the session name in the record.
	SetName(name string)
	// Rename is an alias for SetName.
	Rename(name string)
	// SetStatus changes the status field in the record.
	SetStatus(status string)
	// SetBusy writes "busy" when busy is true and "idle" otherwise.
	SetBusy(busy bool)
	// SetSessionID changes the session identifier in the record.
	SetSessionID(sessionID string)
	// RemoveRecord deletes the registry record but leaves the socket bound.
	RemoveRecord()
	// RestoreRecord writes the record again from the current fields.
	RestoreRecord()
	// WritePartialRecord writes exactly prefix to the record, for testing the
	// partial-read path.
	WritePartialRecord(prefix string)
	// SetRecordModTime sets the record's modification time; later writes still
	// advance past it.
	SetRecordModTime(mod time.Time)
	// SetMode changes how subsequent connections are handled.
	SetMode(mode fakesession.Mode)
	// SetBusyDelay changes how long each accepted connection waits before its
	// frames are read.
	SetBusyDelay(delay time.Duration)
	// Release ends one hold created by HoldOpen.
	Release()
	// Frames returns the message frames received so far, in arrival order.
	Frames() []fakesession.Frame
	// WaitFrame waits for the next frame not yet returned by WaitFrame or
	// WaitFrameN, and reports false when the frame did not arrive within timeout
	// or the local fake was closed first.
	WaitFrame(timeout time.Duration) (fakesession.Frame, bool)
	// WaitFrameN waits for the nth frame, counting from 1, and marks every frame
	// up to n as returned. It reports false when the frame did not arrive within
	// timeout or the local fake was closed first.
	WaitFrameN(n int, timeout time.Duration) (fakesession.Frame, bool)
	// WaitNoConnections waits until the fake tracks no connection, up to
	// timeout, and reports false when a connection remains at the deadline or
	// the local fake is closed while one remains.
	WaitNoConnections(timeout time.Duration) bool
	// TakeErrors returns the background failures recorded since the previous
	// call and forgets them.
	TakeErrors() []error
	// InjectBackgroundError records err as a background failure.
	InjectBackgroundError(err error)
	// Close stops the listener, releases held connections and removes the
	// socket; it is idempotent.
	Close()
	// RemoveSocket stops accepting connections and removes the bound socket while
	// leaving the registry record in place.
	RemoveSocket()
}

var _ FakeSession = (*fakesession.Fake)(nil)

type remoteFake struct {
	tb     testing.TB
	name   string
	client *ctl.Client
	info   ctl.SessionInfo

	mu     sync.Mutex
	cursor int
}

func (f *remoteFake) ConfigDir() string { return f.info.ConfigDir }

func (f *remoteFake) PID() int { return f.info.FakePID }

func (f *remoteFake) RecordPath() string { return f.info.RecordPath }

func (f *remoteFake) SocketPath() string { return f.info.SocketPath }

func (f *remoteFake) SetName(name string) { f.apply("rename", name) }

func (f *remoteFake) Rename(name string) { f.SetName(name) }

func (f *remoteFake) SetStatus(status string) { f.apply("set_status", status) }

func (f *remoteFake) SetBusy(busy bool) { f.apply("set_busy", busy) }

func (f *remoteFake) SetSessionID(sessionID string) { f.apply("set_session_id", sessionID) }

func (f *remoteFake) RemoveRecord() { f.apply("remove_record", nil) }

func (f *remoteFake) RestoreRecord() { f.apply("restore_record", nil) }

func (f *remoteFake) RemoveSocket() { f.apply("remove_socket", nil) }

func (f *remoteFake) SetMode(mode fakesession.Mode) { f.apply("set_mode", modeName(mode)) }

func (f *remoteFake) SetBusyDelay(delay time.Duration) {
	f.apply("set_busy_delay", delay.Milliseconds())
}

func (f *remoteFake) Release() { f.apply("release", nil) }

func (f *remoteFake) TakeErrors() []error {
	f.tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testWaitBound)
	defer cancel()
	res, err := f.client.Errors(ctx, ctl.SessionIDRequest{ID: f.info.ID})
	if err != nil {
		f.tb.Fatalf("harness: session %s: reading simulated session failures: %v", f.name, err)
		return nil
	}
	errs := make([]error, 0, len(res.Errors))
	for _, message := range res.Errors {
		errs = append(errs, errors.New(message))
	}
	return errs
}

func (f *remoteFake) InjectBackgroundError(err error) {
	if err == nil {
		return
	}
	f.apply("inject_background_error", err.Error())
}

func (f *remoteFake) WritePartialRecord(string) { f.unsupported("WritePartialRecord") }

func (f *remoteFake) SetRecordModTime(time.Time) { f.unsupported("SetRecordModTime") }

func (f *remoteFake) Close() { f.unsupported("Close") }

func (f *remoteFake) Frames() []fakesession.Frame {
	f.tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testWaitBound)
	defer cancel()
	res, err := f.client.Frames(ctx, ctl.SessionFramesRequest{ID: f.info.ID})
	if err != nil {
		f.tb.Fatalf("harness: session %s: reading frames: %v", f.name, err)
		return nil
	}
	return remoteFrames(res.Frames)
}

func (f *remoteFake) WaitFrame(timeout time.Duration) (fakesession.Frame, bool) {
	f.tb.Helper()
	f.mu.Lock()
	n := f.cursor + 1
	f.mu.Unlock()
	return f.waitFrameN(n, timeout)
}

func (f *remoteFake) WaitFrameN(n int, timeout time.Duration) (fakesession.Frame, bool) {
	f.tb.Helper()
	if n < 1 {
		f.tb.Fatalf("harness: session %s: WaitFrameN index %d, want at least 1", f.name, n)
		return fakesession.Frame{}, false
	}
	return f.waitFrameN(n, timeout)
}

func (f *remoteFake) waitFrameN(n int, timeout time.Duration) (fakesession.Frame, bool) {
	f.tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout+fakeAnswerSlack)
	defer cancel()
	res, err := f.client.Frames(ctx, ctl.SessionFramesRequest{
		ID:        f.info.ID,
		WaitN:     n,
		TimeoutMS: timeout.Milliseconds(),
	})
	if err != nil {
		f.tb.Fatalf("harness: session %s: waiting for a frame: %v", f.name, err)
		return fakesession.Frame{}, false
	}
	if !res.OK || len(res.Frames) < n {
		return fakesession.Frame{}, false
	}
	f.mu.Lock()
	if f.cursor < n {
		f.cursor = n
	}
	f.mu.Unlock()
	return remoteFrame(res.Frames[n-1]), true
}

func (f *remoteFake) WaitNoConnections(timeout time.Duration) bool {
	f.tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout+fakeAnswerSlack)
	defer cancel()
	res, err := f.client.WaitNoConnections(ctx, ctl.SessionWaitNoConnectionsRequest{
		ID:        f.info.ID,
		TimeoutMS: timeout.Milliseconds(),
	})
	if err != nil {
		f.tb.Fatalf("harness: session %s: waiting for no inbox connections: %v", f.name, err)
		return false
	}
	return res.OK
}

func (f *remoteFake) apply(op string, arg any) {
	f.tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testWaitBound)
	defer cancel()
	if err := f.client.Fake(ctx, ctl.SessionFakeRequest{ID: f.info.ID, Op: op, Arg: arg}); err != nil {
		f.tb.Fatalf("harness: session %s: Fake operation %s: %v", f.name, op, err)
	}
}

func (f *remoteFake) unsupported(method string) {
	f.tb.Helper()
	f.tb.Fatalf("harness: session %s: Fake.%s is not supported for a session in a container", f.name, method)
}

func remoteFrames(frames []ctl.SessionFrame) []fakesession.Frame {
	out := make([]fakesession.Frame, 0, len(frames))
	for _, frame := range frames {
		out = append(out, remoteFrame(frame))
	}
	return out
}

// remoteFrame keeps the local fake's invariants: AuthRaw is nil unless an
// authentication line preceded the frame, and Fields is always non-nil.
func remoteFrame(frame ctl.SessionFrame) fakesession.Frame {
	var authRaw []byte
	if frame.Auth && frame.AuthRaw != "" {
		authRaw = []byte(frame.AuthRaw)
	}
	fields := make(map[string]any, len(frame.Fields))
	for key, value := range frame.Fields {
		fields[key] = value
	}
	return fakesession.Frame{
		Raw:       []byte(frame.Raw),
		Auth:      frame.Auth,
		AuthRaw:   authRaw,
		MsgID:     frame.MsgID,
		Type:      frame.Type,
		FromName:  frame.FromName,
		From:      frame.From,
		SessionID: frame.SessionID,
		Priority:  frame.Priority,
		Content:   frame.Content,
		Fields:    fields,
	}
}
