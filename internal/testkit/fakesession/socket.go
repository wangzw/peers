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

// maxFrameLineBytes matches the receiver's per-line limit; a longer line is
// discarded and the connection closed.
const maxFrameLineBytes = 1 << 20

// maxFromNameRunes matches the receiver's rune limit for a from-name.
const maxFromNameRunes = 64

// maxSocketPathBytes keeps the generated socket path inside the Unix sun_path
// limit on the supported platforms.
const maxSocketPathBytes = 100

var errLineTooLong = errors.New("fakesession: frame line exceeds the 1 MiB limit")

// fromNameAttrRe matches the from-name attribute of an envelope opening tag;
// never apply it to the body.
var fromNameAttrRe = regexp.MustCompile(`(?:^|\s)from-name="((?:\\.|[^"\\])*)"`)

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
// while leaving the registry record in place. It is idempotent.
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

func (f *Fake) setBackgroundError(err error) {
	f.mu.Lock()
	if f.bgErr == nil {
		f.bgErr = err
	}
	f.mu.Unlock()
}

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

func (f *Fake) markConnClosed(conn net.Conn) {
	f.mu.Lock()
	delete(f.conns, conn)
	f.closedConns++
	f.mu.Unlock()
	f.signalConns()
}

func (f *Fake) closedConnCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closedConns
}

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

func (f *Fake) signalConns() {
	select {
	case f.connCh <- struct{}{}:
	default:
	}
}

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
		drainUntilEOF(conn)
		return
	}
	if !f.acceptsSession(frame.SessionID) {
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
		drainUntilEOF(conn)
		return
	}

	f.addFrame(frame)
	// Read to EOF before closing: closing first makes a sender's delayed
	// half-close fail with ENOTCONN on darwin.
	drainUntilEOF(conn)
}

func drainUntilEOF(conn net.Conn) {
	buf := make([]byte, 4096)
	for {
		if _, err := conn.Read(buf); err != nil {
			return
		}
	}
}

func (f *Fake) acceptsSession(sessionID string) bool {
	if sessionID == "" {
		return true
	}
	f.mu.Lock()
	current := f.sessionID
	f.mu.Unlock()
	return sessionID == current
}

func (f *Fake) addFrame(frame Frame) {
	f.mu.Lock()
	f.frames = append(f.frames, frame)
	f.mu.Unlock()
	f.signalFrame()
}

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

// cleanFromName mirrors the receiver's from-name cleaning.
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
