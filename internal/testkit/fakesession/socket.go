package fakesession

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// maxFrameLineBytes is the receiver's per-line limit: a longer line is
// discarded and the connection closed.
const maxFrameLineBytes = 1 << 20

// maxFromNameRunes is the receiver's rune limit for a from-name.
const maxFromNameRunes = 64

// maxSocketPathBytes keeps the generated socket path inside the Unix sun_path
// limit on the supported platforms.
const maxSocketPathBytes = 100

var errLineTooLong = errors.New("fakesession: frame line exceeds the 1 MiB limit")

// fromNameAttrRe extracts the from-name attribute from an envelope tag. It is
// applied only to the opening tag, never to the body.
var fromNameAttrRe = regexp.MustCompile(`(?:^|\s)from-name="((?:\\.|[^"\\])*)"`)

// bindAuto binds a socket under a fresh short temporary directory and starts
// the accept loop.
func (f *Fake) bindAuto() error {
	dir, err := f.newSocketTempDir()
	if err != nil {
		return err
	}
	sockDir := filepath.Join(dir, "sock")
	if err := os.Mkdir(sockDir, 0o700); err != nil {
		_ = os.RemoveAll(dir)
		return err
	}
	path := filepath.Join(sockDir, fmt.Sprintf("%d.sock", f.pid))
	listener, err := bindUnix(path)
	if err != nil {
		_ = os.RemoveAll(dir)
		return err
	}
	f.ownedTempDir = dir
	f.boundSocketPath = path
	f.socketPath = path
	f.listener = listener
	go f.serve(listener)
	return nil
}

// bindAt binds at a caller-provided path and starts the accept loop.
func (f *Fake) bindAt(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	listener, err := bindUnix(path)
	if err != nil {
		return err
	}
	f.boundSocketPath = path
	f.listener = listener
	go f.serve(listener)
	return nil
}

// bindUnix binds a Unix socket and restricts it to the owning user.
func bindUnix(path string) (net.Listener, error) {
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = listener.Close()
		return nil, err
	}
	return listener, nil
}

// newSocketTempDir returns a directory short enough for a socket path,
// preferring the process temporary directory and falling back to /tmp.
func (f *Fake) newSocketTempDir() (string, error) {
	dir, err := os.MkdirTemp("", "fakesession-")
	if err != nil {
		return "", err
	}
	if socketPathLen(dir, f.pid) <= maxSocketPathBytes {
		return dir, nil
	}
	_ = os.RemoveAll(dir)

	dir, err = os.MkdirTemp("/tmp", "fakesession-")
	if err != nil {
		return "", err
	}
	if socketPathLen(dir, f.pid) > maxSocketPathBytes {
		_ = os.RemoveAll(dir)
		return "", fmt.Errorf("temporary socket path under %s is too long", dir)
	}
	return dir, nil
}

func socketPathLen(dir string, pid int) int {
	return len(filepath.Join(dir, "sock", fmt.Sprintf("%d.sock", pid)))
}

// RemoveSocket stops accepting connections and removes the bound socket file
// while leaving the registry record in place. It models a session whose inbox
// socket disappeared. It is idempotent.
func (f *Fake) RemoveSocket() {
	f.t.Helper()
	f.mu.Lock()
	listener := f.listener
	f.listener = nil
	path := f.boundSocketPath
	f.boundSocketPath = ""
	f.stopped = true
	f.mu.Unlock()

	if listener != nil {
		_ = listener.Close()
	}
	if path != "" {
		_ = os.Remove(path)
	}
}

// serve accepts connections until the listener is closed.
func (f *Fake) serve(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			f.mu.Lock()
			stopped := f.stopped
			f.mu.Unlock()
			if !stopped {
				f.setBackgroundError(fmt.Errorf("accepting connection: %w", err))
			}
			return
		}
		go f.handle(conn)
	}
}

// setBackgroundError stores the first unexpected background failure so a test
// goroutine can report it. It never calls a testing method itself.
func (f *Fake) setBackgroundError(err error) {
	f.mu.Lock()
	if f.bgErr == nil {
		f.bgErr = err
	}
	f.mu.Unlock()
}

// trackConn registers a connection so Close can interrupt a blocked read. It
// reports false when the fake is already closed.
func (f *Fake) trackConn(conn net.Conn) bool {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return false
	}
	f.conns[conn] = struct{}{}
	f.mu.Unlock()
	f.signalConns()
	return true
}

// markConnClosed records that the handler closed conn and wakes anyone waiting
// for a close. The caller closes the connection before calling this, so a
// waiter that observes the new count also observes EOF.
func (f *Fake) markConnClosed(conn net.Conn) {
	f.mu.Lock()
	delete(f.conns, conn)
	f.closedConns++
	f.mu.Unlock()
	f.signalConns()
}

// closedConnCount returns how many connections the fake's handlers have closed.
func (f *Fake) closedConnCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closedConns
}

// waitClosedSince waits until the fake has closed a connection after base. It is
// event-driven: a handler closing a connection advances closedConns and wakes
// the wait, so the returned bool does not depend on how long reading one line
// takes. timeout only stops a fake that never closes the connection from
// hanging the test.
func (f *Fake) waitClosedSince(base int, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		f.mu.Lock()
		closed := f.closedConns
		f.mu.Unlock()
		if closed > base {
			return true
		}
		select {
		case <-f.connCh:
		case <-timer.C:
			f.mu.Lock()
			closed := f.closedConns
			f.mu.Unlock()
			return closed > base
		}
	}
}

// signalConns wakes WaitNoConnections after a connection was added or dropped.
func (f *Fake) signalConns() {
	select {
	case f.connCh <- struct{}{}:
	default:
	}
}

// handle serves one accepted connection according to the current mode.
func (f *Fake) handle(conn net.Conn) {
	if !f.trackConn(conn) {
		_ = conn.Close()
		return
	}
	// Close before recording the close so a waiter that sees the count advance
	// also sees EOF on its end of the connection.
	defer func() {
		_ = conn.Close()
		f.markConnClosed(conn)
	}()

	f.mu.Lock()
	mode := f.mode
	delay := f.busyDelay
	f.mu.Unlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-f.done:
			return
		}
	}
	if mode == CloseImmediately {
		return
	}

	reader := bufio.NewReaderSize(conn, 4096)
	var auth bool
	var authRaw []byte
	line, err := readLine(reader, maxFrameLineBytes)
	if err != nil {
		return
	}
	if isAuthLine(line) {
		auth = true
		authRaw = line
		line, err = readLine(reader, maxFrameLineBytes)
		if err != nil {
			return
		}
	}
	if len(line) == 0 {
		return
	}
	frame, ok := parseFrame(line, auth, authRaw)
	if !ok {
		// The line is not a JSON object, so it is not a frame. The receiver
		// discards it and closes the connection once the sender half-closes.
		drainUntilEOF(conn)
		return
	}
	if !f.acceptsSession(frame.SessionID) {
		// The frame is addressed to another session. The receiver drops it
		// silently; the sender only observes the connection closing.
		drainUntilEOF(conn)
		return
	}

	if mode == HoldOpen {
		held := &heldConn{release: make(chan struct{})}
		f.mu.Lock()
		if f.closed {
			f.mu.Unlock()
			return
		}
		// Register the held connection and the frame under one lock so a test
		// that sees the frame can always release the connection.
		f.held = append(f.held, held)
		f.frames = append(f.frames, frame)
		f.mu.Unlock()
		f.signalFrame()
		select {
		case <-held.release:
		case <-f.done:
			return
		}
		// Release ends the hold, but the socket stays open for reading until
		// the sender half-closes. Closing it outright would make a sender
		// whose half-close is delayed (the vendored client waits 150 ms on
		// darwin) fail that CloseWrite with ENOTCONN.
		drainUntilEOF(conn)
		return
	}

	f.addFrame(frame)
	// Keep the connection open until the sender half-closes. Closing right
	// after the frame would make a delayed CloseWrite fail with ENOTCONN on
	// darwin, which the sender has to report as an uncertain delivery.
	drainUntilEOF(conn)
}

// drainUntilEOF reads and discards whatever the sender writes until it
// half-closes or the connection fails. It lets a sender whose half-close is
// delayed complete it while the socket is still connected.
func drainUntilEOF(conn net.Conn) {
	buf := make([]byte, 4096)
	for {
		if _, err := conn.Read(buf); err != nil {
			return
		}
	}
}

// acceptsSession reports whether a frame addressed to sessionID belongs to this
// fake. A frame without a session id is accepted; a frame carrying any other id
// is silently dropped, matching the receiver.
func (f *Fake) acceptsSession(sessionID string) bool {
	if sessionID == "" {
		return true
	}
	f.mu.Lock()
	current := f.sessionID
	f.mu.Unlock()
	return sessionID == current
}

// addFrame records a message frame and wakes WaitFrame.
func (f *Fake) addFrame(frame Frame) {
	f.mu.Lock()
	f.frames = append(f.frames, frame)
	f.mu.Unlock()
	f.signalFrame()
}

// copyFrame detaches a frame from the fake's internal storage.
func copyFrame(frame Frame) Frame {
	frame.Raw = append([]byte(nil), frame.Raw...)
	if frame.AuthRaw != nil {
		frame.AuthRaw = append([]byte(nil), frame.AuthRaw...)
	}
	if frame.Fields != nil {
		fields := make(map[string]any, len(frame.Fields))
		for key, value := range frame.Fields {
			fields[key] = value
		}
		frame.Fields = fields
	}
	return frame
}

func (f *Fake) signalFrame() {
	select {
	case f.frameCh <- struct{}{}:
	default:
	}
}

// readLine reads one newline-terminated line, removing the line ending. A line
// longer than max bytes is rejected. A final line without a newline is
// accepted as a line.
func readLine(r *bufio.Reader, max int) ([]byte, error) {
	var buf []byte
	for {
		chunk, err := r.ReadSlice('\n')
		if len(chunk) > 0 {
			buf = append(buf, chunk...)
			if len(buf) > max+1 {
				return nil, errLineTooLong
			}
		}
		switch {
		case err == nil:
			return trimLineEnding(buf), nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			if len(buf) == 0 {
				return nil, io.EOF
			}
			if len(buf) > max {
				return nil, errLineTooLong
			}
			return trimLineEnding(buf), nil
		default:
			return nil, err
		}
	}
}

func trimLineEnding(line []byte) []byte {
	line = bytes.TrimSuffix(line, []byte("\n"))
	return bytes.TrimSuffix(line, []byte("\r"))
}

// isAuthLine reports whether line is the optional authentication frame.
func isAuthLine(line []byte) bool {
	if bytes.HasPrefix(line, []byte(`{"type":"auth"`)) {
		return true
	}
	var probe struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(line, &probe); err != nil {
		return false
	}
	return probe.Type == "auth"
}

// parseFrame decodes one message line into a Frame. It reports false when the
// line is not a JSON object; such a line is not a frame and is not recorded.
func parseFrame(raw []byte, auth bool, authRaw []byte) (Frame, bool) {
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return Frame{}, false
	}
	frame := Frame{
		Raw:    append([]byte(nil), raw...),
		Auth:   auth,
		Fields: fields,
	}
	if authRaw != nil {
		frame.AuthRaw = append([]byte(nil), authRaw...)
	}
	frame.MsgID = stringField(fields, "msg_id")
	frame.Type = stringField(fields, "type")
	frame.From = stringField(fields, "from")
	frame.SessionID = stringField(fields, "session_id")
	frame.Priority = stringField(fields, "priority")
	if message, ok := fields["message"].(map[string]any); ok {
		if content, ok := message["content"].(string); ok {
			frame.Content = content
			frame.FromName = envelopeFromName(content)
		}
	}
	return frame, true
}

func stringField(fields map[string]any, key string) string {
	value, _ := fields[key].(string)
	return value
}

// envelopeFromName returns the from-name attribute of the attribution
// envelope at the start of content. It returns "" when content has no
// envelope, or when the receiver would not recognize the envelope because
// cleaning the name would change it. Only the opening tag is inspected, so a
// forged tag in the body cannot supply the name.
func envelopeFromName(content string) string {
	if !strings.HasPrefix(content, "<cross-session-message") {
		return ""
	}
	end := strings.IndexByte(content, '>')
	if end < 0 {
		return ""
	}
	match := fromNameAttrRe.FindStringSubmatch(content[:end+1])
	if match == nil {
		return ""
	}
	name, err := strconv.Unquote(`"` + match[1] + `"`)
	if err != nil {
		return ""
	}
	if cleanFromName(name) != name {
		return ""
	}
	return name
}

// cleanFromName applies the receiver's from-name cleaning: quotes, angle
// brackets and invisible characters are dropped, surrounding whitespace is
// trimmed, and the result is truncated to 64 runes.
func cleanFromName(name string) string {
	name = strings.Map(func(r rune) rune {
		switch r {
		case '"', '<', '>':
			return -1
		}
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return -1
		}
		return r
	}, name)
	name = strings.TrimSpace(name)
	if runes := []rune(name); len(runes) > maxFromNameRunes {
		name = string(runes[:maxFromNameRunes])
	}
	return name
}
