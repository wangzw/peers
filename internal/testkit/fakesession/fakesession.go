package fakesession

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Mode selects how a Fake handles an accepted connection.
type Mode int

const (
	// CloseAfterRead reads one message line and closes the connection once the
	// sender half-closes. A line that is not a frame or that names another
	// session is not recorded. It is the zero value.
	CloseAfterRead Mode = iota

	// HoldOpen reads and records one message line and keeps the connection open
	// until Release or Close; after Release it reads until the sender half-closes.
	// Only a recorded frame holds the connection open; a line that is not a frame
	// or that names another session is drained and the connection closed.
	HoldOpen

	// CloseImmediately closes the connection without reading anything.
	CloseImmediately
)

// Options configures New.
type Options struct {
	// ConfigDir is the Claude configuration directory; empty means a new
	// temporary directory, and the registry is written under
	// <ConfigDir>/sessions.
	ConfigDir string

	// PID is the session process ID and the registry filename; zero assigns a
	// value unique within the process, and no real process is required.
	PID int

	// Name is the session name written to the record.
	Name string

	// Status is the session status written to the record; empty is allowed.
	Status string

	// SessionID is the simulated session identifier; empty derives one from PID,
	// and SetSessionID changes it later. A frame whose session_id differs from it
	// is not recorded.
	SessionID string

	// NoBind writes only the registry record and binds no socket; SocketPath
	// chooses the path the record names.
	NoBind bool

	// SocketPath is the inbox socket path. When it is empty and NoBind is unset,
	// a path is generated under a short temporary directory and bound. With NoBind
	// and an empty SocketPath no path is generated and the record's path is empty;
	// with NoBind and a path, the record names it but no socket is bound.
	SocketPath string

	// Mode selects how accepted connections are handled.
	Mode Mode

	// Extra adds to or overrides the fields written to the registry record; it
	// is applied last.
	Extra map[string]any

	// BusyDelay delays reading each accepted connection; zero reads immediately.
	BusyDelay time.Duration
}

// Frame is one message frame received by a Fake.
type Frame struct {
	// Raw is the frame line with the trailing newline removed.
	Raw []byte

	// Auth reports whether an authentication line preceded this frame on the
	// same connection.
	Auth bool

	// AuthRaw is the authentication line without its trailing newline; nil when
	// Auth is false.
	AuthRaw []byte

	// MsgID is the frame's msg_id.
	MsgID string

	// Type is the frame's type.
	Type string

	// FromName is the sender name decoded from the attribution envelope inside
	// message.content; empty when the content carries no envelope or cleaning
	// the name would change it.
	FromName string

	// From is the frame's top-level from field.
	From string

	// SessionID is the frame's session_id field.
	SessionID string

	// Priority is the frame's priority field.
	Priority string

	// Content is message.content verbatim, including any attribution envelope.
	Content string

	// Fields is the whole decoded frame, always non-nil for a recorded frame.
	Fields map[string]any
}

// Fake is a simulated Claude Code session.
type Fake struct {
	t testing.TB

	configDir       string
	sessionsDir     string
	pid             int
	recordPath      string
	socketPath      string
	boundSocketPath string
	ownedTempDir    string
	listener        net.Listener

	mu            sync.Mutex
	name          string
	status        string
	sessionID     string
	extra         map[string]any
	recordPresent bool
	lastMod       time.Time
	mode          Mode
	busyDelay     time.Duration
	closed        bool
	stopped       bool
	frames        []Frame
	frameCursor   int
	held          []*heldConn
	conns         map[net.Conn]struct{}
	closedConns   int
	bgErr         error

	done    chan struct{}
	frameCh chan struct{}
	connCh  chan struct{}
}

type heldConn struct {
	release chan struct{}
}

var autoPID atomic.Int64

func nextPID() int {
	return int(autoPID.Add(1)) + 99999
}

// New creates a Fake, writes its registry record, and binds its socket unless
// NoBind is set. It registers Close with t.Cleanup.
func New(t testing.TB, opts Options) *Fake {
	t.Helper()

	f := &Fake{
		t:         t,
		configDir: opts.ConfigDir,
		name:      opts.Name,
		status:    opts.Status,
		sessionID: opts.SessionID,
		mode:      opts.Mode,
		busyDelay: opts.BusyDelay,
		conns:     make(map[net.Conn]struct{}),
		done:      make(chan struct{}),
		frameCh:   make(chan struct{}, 1),
		connCh:    make(chan struct{}, 1),
	}

	if f.configDir == "" {
		f.configDir = t.TempDir()
	}
	// Register Close after t.TempDir so Close runs before the temporary
	// directory's RemoveAll.
	t.Cleanup(f.Close)
	if f.pid = opts.PID; f.pid == 0 {
		f.pid = nextPID()
	}
	if f.sessionID == "" {
		f.sessionID = fmt.Sprintf("session-%d", f.pid)
	}
	f.extra = make(map[string]any, len(opts.Extra))
	for k, v := range opts.Extra {
		f.extra[k] = v
	}

	f.sessionsDir = filepath.Join(f.configDir, "sessions")
	if err := os.MkdirAll(f.sessionsDir, 0o700); err != nil {
		t.Fatalf("fakesession: creating %s: %v", f.sessionsDir, err)
	}
	f.recordPath = filepath.Join(f.sessionsDir, fmt.Sprintf("%d.json", f.pid))

	switch {
	case opts.SocketPath != "":
		f.socketPath = opts.SocketPath
		if !opts.NoBind {
			if err := f.bindAt(opts.SocketPath); err != nil {
				t.Fatalf("fakesession: binding %s: %v", opts.SocketPath, err)
			}
		}
	case opts.NoBind:
	default:
		if err := f.bindAuto(); err != nil {
			t.Fatalf("fakesession: binding socket: %v", err)
		}
	}

	f.mu.Lock()
	f.recordPresent = true
	err := f.writeRecordLocked()
	f.mu.Unlock()
	if err != nil {
		t.Fatalf("fakesession: writing session record: %v", err)
	}
	return f
}

// ConfigDir returns the configuration directory holding the registry record.
func (f *Fake) ConfigDir() string { return f.configDir }

// PID returns the session process ID.
func (f *Fake) PID() int { return f.pid }

// RecordPath returns the path of the registry record.
func (f *Fake) RecordPath() string { return f.recordPath }

// SocketPath returns the inbox socket path named by the record. It is empty
// when the fake was created with NoBind and no SocketPath.
func (f *Fake) SocketPath() string { return f.socketPath }

// SetName changes the session name in the record.
func (f *Fake) SetName(name string) {
	f.t.Helper()
	f.updateRecord(func() { f.name = name })
}

// Rename is an alias for SetName that reads like a session rename.
func (f *Fake) Rename(name string) {
	f.t.Helper()
	f.SetName(name)
}

// SetStatus changes the status field in the record.
func (f *Fake) SetStatus(status string) {
	f.t.Helper()
	f.updateRecord(func() { f.status = status })
}

// SetBusy writes "busy" when busy is true and "idle" otherwise; it changes only
// the record's status, while BusyDelay models a session that delays reading.
func (f *Fake) SetBusy(busy bool) {
	f.t.Helper()
	if busy {
		f.SetStatus("busy")
		return
	}
	f.SetStatus("idle")
}

// SetSessionID changes the session identifier in the record. A frame that carries
// a session_id is recorded only when it matches this identifier; a frame without
// one is recorded as usual.
func (f *Fake) SetSessionID(sessionID string) {
	f.t.Helper()
	f.updateRecord(func() { f.sessionID = sessionID })
}

// RemoveRecord deletes the registry record but leaves the socket bound.
func (f *Fake) RemoveRecord() {
	f.t.Helper()
	f.mu.Lock()
	f.recordPresent = false
	path := f.recordPath
	f.mu.Unlock()
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		f.t.Fatalf("fakesession: removing %s: %v", path, err)
	}
}

// RestoreRecord writes the record again from the current fields.
func (f *Fake) RestoreRecord() {
	f.t.Helper()
	f.mu.Lock()
	f.recordPresent = true
	err := f.writeRecordLocked()
	f.mu.Unlock()
	if err != nil {
		f.t.Fatalf("fakesession: writing session record: %v", err)
	}
}

// WritePartialRecord writes exactly prefix to the record, leaving whatever
// JSON it contains in place. A truncated prefix produces an unparseable record
// for testing the partial-read path.
func (f *Fake) WritePartialRecord(prefix string) {
	f.t.Helper()
	f.mu.Lock()
	f.recordPresent = true
	err := f.writeRecordBytesLocked([]byte(prefix))
	f.mu.Unlock()
	if err != nil {
		f.t.Fatalf("fakesession: writing partial session record: %v", err)
	}
}

// SetRecordModTime sets the record's modification time. Later writes still
// advance past this value.
func (f *Fake) SetRecordModTime(mod time.Time) {
	f.t.Helper()
	f.mu.Lock()
	f.lastMod = mod
	var err error
	if f.recordPresent {
		err = os.Chtimes(f.recordPath, mod, mod)
	}
	f.mu.Unlock()
	if err != nil {
		f.t.Fatalf("fakesession: setting record modification time: %v", err)
	}
}

// SetMode changes how subsequent connections are handled.
func (f *Fake) SetMode(mode Mode) {
	f.t.Helper()
	f.mu.Lock()
	f.mode = mode
	f.mu.Unlock()
}

// SetBusyDelay changes how long each accepted connection waits before its
// frames are read.
func (f *Fake) SetBusyDelay(delay time.Duration) {
	f.t.Helper()
	f.mu.Lock()
	f.busyDelay = delay
	f.mu.Unlock()
}

// Release ends one hold created by HoldOpen; the connection stays open for
// reading until the sender half-closes.
func (f *Fake) Release() {
	f.t.Helper()
	f.mu.Lock()
	var hc *heldConn
	if n := len(f.held); n > 0 {
		hc = f.held[0]
		f.held = f.held[1:]
	}
	f.mu.Unlock()
	if hc != nil {
		close(hc.release)
	}
}

// Frames returns the message frames received so far, in arrival order.
func (f *Fake) Frames() []Frame {
	f.t.Helper()
	f.mu.Lock()
	out := make([]Frame, len(f.frames))
	for i := range f.frames {
		out[i] = copyFrame(f.frames[i])
	}
	f.mu.Unlock()
	return out
}

// WaitFrame waits for the next message frame that has not been returned yet by
// WaitFrame or WaitFrameN, so repeated calls walk the frames in arrival order.
// It reports false when the frame did not arrive in time or the fake was closed
// first.
func (f *Fake) WaitFrame(timeout time.Duration) (Frame, bool) {
	f.t.Helper()
	f.mu.Lock()
	n := f.frameCursor + 1
	f.mu.Unlock()
	return f.waitFrameN(n, timeout)
}

// WaitFrameN waits for the nth message frame, counting from 1, and returns it.
// It also marks every frame up to n as returned, so a later WaitFrame continues
// after the highest frame observed. It reports false when the frame did not
// arrive in time or the fake was closed first.
func (f *Fake) WaitFrameN(n int, timeout time.Duration) (Frame, bool) {
	f.t.Helper()
	if n < 1 {
		f.t.Fatalf("fakesession: WaitFrameN index %d, want at least 1", n)
	}
	return f.waitFrameN(n, timeout)
}

func (f *Fake) waitFrameN(n int, timeout time.Duration) (Frame, bool) {
	f.t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		f.mu.Lock()
		if len(f.frames) >= n {
			fr := copyFrame(f.frames[n-1])
			if f.frameCursor < n {
				f.frameCursor = n
			}
			f.mu.Unlock()
			return fr, true
		}
		f.mu.Unlock()

		select {
		case <-f.frameCh:
		case <-timer.C:
			return Frame{}, false
		case <-f.done:
			f.mu.Lock()
			have := len(f.frames)
			f.mu.Unlock()
			if have < n {
				return Frame{}, false
			}
		}
	}
}

// WaitNoConnections waits until the fake tracks no connection, up to timeout. A
// HoldOpen connection still counts after Release until the sender half-closes,
// and counts until Close if the sender does not. It reports false when a
// connection remains at the deadline.
func (f *Fake) WaitNoConnections(timeout time.Duration) bool {
	f.t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		f.mu.Lock()
		empty := len(f.conns) == 0
		f.mu.Unlock()
		if empty {
			return true
		}
		select {
		case <-f.connCh:
		case <-timer.C:
			// A close that raced with the deadline still counts.
			f.mu.Lock()
			empty := len(f.conns) == 0
			f.mu.Unlock()
			return empty
		case <-f.done:
			f.mu.Lock()
			empty := len(f.conns) == 0
			f.mu.Unlock()
			return empty
		}
	}
}

// Close stops the listener, releases held connections, removes the socket, and
// removes any temporary directory the fake created for it. It is idempotent.
func (f *Fake) Close() {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return
	}
	f.closed = true
	f.stopped = true
	close(f.done)
	listener := f.listener
	f.listener = nil
	path := f.boundSocketPath
	f.boundSocketPath = ""
	held := f.held
	f.held = nil
	conns := make([]net.Conn, 0, len(f.conns))
	for conn := range f.conns {
		conns = append(conns, conn)
	}
	f.conns = make(map[net.Conn]struct{})
	tempDir := f.ownedTempDir
	f.ownedTempDir = ""
	f.mu.Unlock()

	f.signalConns()

	if listener != nil {
		_ = listener.Close()
	}
	for _, hc := range held {
		close(hc.release)
	}
	for _, conn := range conns {
		_ = conn.Close()
	}
	if path != "" {
		_ = os.Remove(path)
	}
	if tempDir != "" {
		_ = os.RemoveAll(tempDir)
	}
}

// updateRecord calls Fatalf on a write error after releasing the lock.
func (f *Fake) updateRecord(mutate func()) {
	f.mu.Lock()
	mutate()
	var err error
	if f.recordPresent {
		err = f.writeRecordLocked()
	}
	f.mu.Unlock()
	if err != nil {
		f.t.Fatalf("fakesession: writing session record: %v", err)
	}
}

// The caller holds f.mu.
func (f *Fake) writeRecordLocked() error {
	record := map[string]any{
		"pid":                 f.pid,
		"name":                f.name,
		"status":              f.status,
		"messagingSocketPath": f.socketPath,
		"sessionId":           f.sessionID,
		"nameSource":          "derived",
		"kind":                "interactive",
		"entrypoint":          "cli",
		"peerProtocol":        1,
	}
	for key, value := range f.extra {
		record[key] = value
	}
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encoding record: %w", err)
	}
	return f.writeRecordBytesLocked(data)
}

// writeRecordBytesLocked replaces the record content in place and advances the
// modification time past the previous write. The caller holds f.mu.
func (f *Fake) writeRecordBytesLocked(data []byte) error {
	file, err := os.OpenFile(f.recordPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	mod := f.nextModTimeLocked()
	if err := os.Chtimes(f.recordPath, mod, mod); err != nil {
		return err
	}
	f.lastMod = mod
	return nil
}

// The caller holds f.mu.
func (f *Fake) nextModTimeLocked() time.Time {
	now := time.Now()
	if !now.After(f.lastMod) {
		now = f.lastMod.Add(time.Millisecond)
	}
	return now
}

// TakeErrors returns the background failures recorded since the previous call
// and clears them; recording never calls a testing method, so the fake's owner
// reports them.
func (f *Fake) TakeErrors() []error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.bgErr == nil {
		return nil
	}
	err := f.bgErr
	f.bgErr = nil
	return []error{err}
}

// InjectBackgroundError records err as a background failure, as an unexpected
// accept-loop failure would.
func (f *Fake) InjectBackgroundError(err error) {
	if err == nil {
		return
	}
	f.setBackgroundError(err)
}
