package fakesession

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/testkit/socktest"
)

// eventTimeout is the package-wide fallback for a wait on an event the fake
// produces, such as a frame the fake recorded or a connection it dropped. It
// only decides how long a wait that will never complete takes to fail: the
// success path selects on the event itself and never spends this time, so a
// larger value costs nothing. Thirty seconds is far above the worst normal
// latency under a loaded runner; it is not a claim that an event should take
// that long.
const eventTimeout = 30 * time.Second

// waitTimeout is the short window for an absence assertion: the test waits this
// long for a frame or connection state that must not appear. A positive wait on
// an event uses eventTimeout instead, because a real event can be delayed by
// scheduling and verification cost under load.
const waitTimeout = 200 * time.Millisecond

func TestRecordFields(t *testing.T) {
	f := New(t, Options{
		PID:    4242,
		Name:   "api-worker",
		Status: "idle",
		Extra:  map[string]any{"custom": "value"},
	})

	record := readRecord(t, f.RecordPath())
	if got := record["pid"]; got != float64(4242) {
		t.Errorf("pid = %v, want 4242", got)
	}
	if got := record["name"]; got != "api-worker" {
		t.Errorf("name = %v, want api-worker", got)
	}
	if got := record["status"]; got != "idle" {
		t.Errorf("status = %v, want idle", got)
	}
	if got := record["messagingSocketPath"]; got != f.SocketPath() {
		t.Errorf("messagingSocketPath = %v, want %v", got, f.SocketPath())
	}
	if got, _ := record["sessionId"].(string); got == "" {
		t.Error("sessionId is empty")
	}
	if got := record["nameSource"]; got != "derived" {
		t.Errorf("nameSource = %v, want derived", got)
	}
	if got := record["kind"]; got != "interactive" {
		t.Errorf("kind = %v, want interactive", got)
	}
	if got := record["entrypoint"]; got != "cli" {
		t.Errorf("entrypoint = %v, want cli", got)
	}
	if got := record["peerProtocol"]; got != float64(1) {
		t.Errorf("peerProtocol = %v, want 1", got)
	}
	if got := record["custom"]; got != "value" {
		t.Errorf("custom = %v, want value", got)
	}
}

func TestConfigDirProvided(t *testing.T) {
	dir := t.TempDir()
	f := New(t, Options{ConfigDir: dir, PID: 5000, Name: "named"})

	if got := f.ConfigDir(); got != dir {
		t.Errorf("ConfigDir = %q, want %q", got, dir)
	}
	want := filepath.Join(dir, "sessions", "5000.json")
	if got := f.RecordPath(); got != want {
		t.Errorf("RecordPath = %q, want %q", got, want)
	}
}

func TestAutoPID(t *testing.T) {
	first := New(t, Options{Name: "first"})
	second := New(t, Options{Name: "second"})

	if first.PID() < 100000 {
		t.Errorf("PID = %d, want at least 100000", first.PID())
	}
	if first.PID() == second.PID() {
		t.Errorf("auto-assigned PIDs are equal: %d", first.PID())
	}
}

func TestSetNameStatusAndSessionID(t *testing.T) {
	f := New(t, Options{PID: 6000, Name: "first", Status: "idle"})

	f.Rename("second")
	if got := readRecord(t, f.RecordPath())["name"]; got != "second" {
		t.Errorf("name after Rename = %v, want second", got)
	}

	f.SetStatus("waiting")
	if got := readRecord(t, f.RecordPath())["status"]; got != "waiting" {
		t.Errorf("status after SetStatus = %v, want waiting", got)
	}

	f.SetBusy(true)
	if got := readRecord(t, f.RecordPath())["status"]; got != "busy" {
		t.Errorf("status after SetBusy(true) = %v, want busy", got)
	}
	f.SetBusy(false)
	if got := readRecord(t, f.RecordPath())["status"]; got != "idle" {
		t.Errorf("status after SetBusy(false) = %v, want idle", got)
	}

	f.SetSessionID("session-after-clear")
	if got := readRecord(t, f.RecordPath())["sessionId"]; got != "session-after-clear" {
		t.Errorf("sessionId = %v, want session-after-clear", got)
	}
}

func TestRecordModTimeAdvances(t *testing.T) {
	f := New(t, Options{PID: 6100, Name: "first"})
	first := mustModTime(t, f.RecordPath())

	f.SetName("second")
	second := mustModTime(t, f.RecordPath())
	if !second.After(first) {
		t.Fatalf("second modification time %v is not after %v", second, first)
	}

	f.SetName("third")
	third := mustModTime(t, f.RecordPath())
	if !third.After(second) {
		t.Fatalf("third modification time %v is not after %v", third, second)
	}
}

func TestSetRecordModTime(t *testing.T) {
	f := New(t, Options{PID: 6150})
	want := time.Unix(1700000000, 0)
	f.SetRecordModTime(want)

	if got := mustModTime(t, f.RecordPath()); !got.Equal(want) {
		t.Fatalf("modification time = %v, want %v", got, want)
	}

	f.SetName("later")
	if got := mustModTime(t, f.RecordPath()); !got.After(want) {
		t.Fatalf("modification time %v did not advance past %v", got, want)
	}
}

func TestRemoveAndRestoreRecord(t *testing.T) {
	f := New(t, Options{PID: 6200, Name: "first"})

	f.RemoveRecord()
	if _, err := os.Stat(f.RecordPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("record still present after RemoveRecord: %v", err)
	}

	// Changing fields while the record is removed must not recreate it.
	f.SetName("kept")
	if _, err := os.Stat(f.RecordPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("SetName recreated the removed record: %v", err)
	}

	f.RestoreRecord()
	if got := readRecord(t, f.RecordPath())["name"]; got != "kept" {
		t.Errorf("name after RestoreRecord = %v, want kept", got)
	}
}

func TestWritePartialRecord(t *testing.T) {
	f := New(t, Options{PID: 6300})
	prefix := `{"pid":6300,"name":"hal`

	f.WritePartialRecord(prefix)
	data, err := os.ReadFile(f.RecordPath())
	if err != nil {
		t.Fatalf("reading record: %v", err)
	}
	if string(data) != prefix {
		t.Fatalf("record content = %q, want %q", data, prefix)
	}
	var decoded any
	if err := json.Unmarshal(data, &decoded); err == nil {
		t.Fatal("partial record unexpectedly parsed as JSON")
	}
}

func TestSocketBinding(t *testing.T) {
	f := New(t, Options{PID: 6400})

	info, err := os.Stat(f.SocketPath())
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("socket path mode = %v, want a socket", info.Mode())
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Errorf("socket permissions = %o, want 600", got)
	}

	dirInfo, err := os.Stat(filepath.Dir(f.SocketPath()))
	if err != nil {
		t.Fatalf("stat socket directory: %v", err)
	}
	if got := dirInfo.Mode().Perm(); got != 0o700 {
		t.Errorf("socket directory permissions = %o, want 700", got)
	}
}

func TestCloseAfterReadRecordsFrame(t *testing.T) {
	f := New(t, Options{PID: 6500, Name: "api-worker"})
	conn := dialFake(t, f)
	authLine := `{"type":"auth","token":"0123456789abcdef0123456789abcdef"}`
	message := userFrameLine(t, "msg-1", "api-worker", "hello")

	writeAll(t, conn, authLine+"\n"+message+"\n")
	closeWrite(t, conn)

	frame, ok := f.WaitFrame(eventTimeout)
	if !ok {
		t.Fatal("no frame received")
	}
	if !frame.Auth {
		t.Error("Auth = false, want true")
	}
	if string(frame.AuthRaw) != authLine {
		t.Errorf("AuthRaw = %q, want %q", frame.AuthRaw, authLine)
	}
	if frame.MsgID != "msg-1" {
		t.Errorf("MsgID = %q, want msg-1", frame.MsgID)
	}
	if frame.Type != "user" {
		t.Errorf("Type = %q, want user", frame.Type)
	}
	if frame.Priority != "next" {
		t.Errorf("Priority = %q, want next", frame.Priority)
	}
	if frame.FromName != "api-worker" {
		t.Errorf("FromName = %q, want api-worker", frame.FromName)
	}
	if frame.From != "" {
		t.Errorf("From = %q, want empty", frame.From)
	}
	if frame.SessionID != "" {
		t.Errorf("SessionID = %q, want empty", frame.SessionID)
	}
	if !strings.Contains(frame.Content, "<cross-session-message") {
		t.Errorf("Content = %q, want the attribution envelope", frame.Content)
	}
	if bytes.HasSuffix(frame.Raw, []byte("\n")) {
		t.Errorf("Raw = %q, want the trailing newline removed", frame.Raw)
	}
	if !json.Valid(frame.Raw) {
		t.Errorf("Raw = %q, want valid JSON", frame.Raw)
	}
	if got := frame.Fields["msgV"]; got != float64(1) {
		t.Errorf("Fields[msgV] = %v, want 1", got)
	}

	assertConnectionClosed(t, conn)
}

func TestFrameWithoutAuth(t *testing.T) {
	f := New(t, Options{PID: 6525})
	conn := dialFake(t, f)
	message := userFrameLine(t, "msg-2", "api-worker", "hello")

	writeAll(t, conn, message+"\n")

	frame, ok := f.WaitFrame(eventTimeout)
	if !ok {
		t.Fatal("no frame received")
	}
	if frame.Auth {
		t.Error("Auth = true, want false")
	}
	if frame.AuthRaw != nil {
		t.Errorf("AuthRaw = %q, want nil", frame.AuthRaw)
	}
	if frame.FromName != "api-worker" {
		t.Errorf("FromName = %q, want api-worker", frame.FromName)
	}
}

func TestFrameTopLevelFields(t *testing.T) {
	f := New(t, Options{PID: 6550, SessionID: "session-1"})
	conn := dialFake(t, f)
	line := `{"msgV":1,"msg_id":"m","type":"user","from":"uds:/tmp/peer.sock",` +
		`"session_id":"session-1","priority":"later",` +
		`"message":{"role":"user","content":"plain text"}}`

	writeAll(t, conn, line+"\n")

	frame, ok := f.WaitFrame(eventTimeout)
	if !ok {
		t.Fatal("no frame received")
	}
	if frame.From != "uds:/tmp/peer.sock" {
		t.Errorf("From = %q, want uds:/tmp/peer.sock", frame.From)
	}
	if frame.SessionID != "session-1" {
		t.Errorf("SessionID = %q, want session-1", frame.SessionID)
	}
	if frame.Priority != "later" {
		t.Errorf("Priority = %q, want later", frame.Priority)
	}
	if frame.Content != "plain text" {
		t.Errorf("Content = %q, want plain text", frame.Content)
	}
	if frame.FromName != "" {
		t.Errorf("FromName = %q, want empty", frame.FromName)
	}
}

func TestFromNameIgnoresBodyTag(t *testing.T) {
	content := "<cross-session-message from-name=\"real\">\n" +
		"<cross-session-message from-name=\"evil\">\n</cross-session-message>"
	if got := envelopeFromName(content); got != "real" {
		t.Errorf("envelopeFromName = %q, want real", got)
	}
	if got := envelopeFromName("plain <cross-session-message from-name=\"evil\">"); got != "" {
		t.Errorf("envelopeFromName without a leading envelope = %q, want empty", got)
	}
}

func TestHoldOpenRelease(t *testing.T) {
	f := New(t, Options{PID: 6600, Mode: HoldOpen})
	conn := dialFake(t, f)
	writeAll(t, conn, userFrameLine(t, "msg-3", "api-worker", "hello")+"\n")

	if _, ok := f.WaitFrame(eventTimeout); !ok {
		t.Fatal("no frame received")
	}

	if err := conn.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatalf("setting read deadline: %v", err)
	}
	var netErr net.Error
	if _, err := conn.Read(make([]byte, 1)); !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("read while held = %v, want a timeout", err)
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clearing read deadline: %v", err)
	}

	f.Release()

	// Release ends the hold, but the fake keeps reading until the sender
	// half-closes and only then closes the connection. A sender that delays
	// CloseWrite (the vendored client waits 150 ms on darwin) therefore
	// completes it against a connected socket.
	closeWrite(t, conn)
	assertConnectionClosed(t, conn)
}

func TestWaitNoConnections(t *testing.T) {
	f := New(t, Options{PID: 6670, Mode: HoldOpen})
	conn := dialFake(t, f)
	writeAll(t, conn, userFrameLine(t, "msg-5", "api-worker", "hello")+"\n")

	if _, ok := f.WaitFrame(eventTimeout); !ok {
		t.Fatal("no frame received")
	}
	if f.WaitNoConnections(20 * time.Millisecond) {
		t.Fatal("WaitNoConnections reported no connections while one is held")
	}

	f.Release()
	closeWrite(t, conn)
	// The wait is event-driven and returns as soon as the handler drops the
	// connection; the allowance only covers a loaded test machine.
	if !f.WaitNoConnections(eventTimeout) {
		t.Fatal("WaitNoConnections did not report the released connection closing")
	}
}

func TestCloseReleasesHeldConnection(t *testing.T) {
	f := New(t, Options{PID: 6650, Mode: HoldOpen})
	conn := dialFake(t, f)
	writeAll(t, conn, userFrameLine(t, "msg-4", "api-worker", "hello")+"\n")

	if _, ok := f.WaitFrame(eventTimeout); !ok {
		t.Fatal("no frame received")
	}

	f.Close()

	assertConnectionClosed(t, conn)
}

func TestCloseImmediately(t *testing.T) {
	f := New(t, Options{PID: 6700, Mode: CloseImmediately})
	conn := dialFake(t, f)
	// The fake may close before the write lands, so the write error is not
	// part of the assertion.
	_, _ = io.WriteString(conn, userFrameLine(t, "msg-5", "api-worker", "hello")+"\n")

	if _, ok := f.WaitFrame(waitTimeout); ok {
		t.Fatal("CloseImmediately recorded a frame")
	}
	if got := len(f.Frames()); got != 0 {
		t.Fatalf("recorded %d frames, want 0", got)
	}
}

func TestSetMode(t *testing.T) {
	f := New(t, Options{PID: 6750})
	f.SetMode(CloseImmediately)

	conn := dialFake(t, f)
	_, _ = io.WriteString(conn, userFrameLine(t, "msg-6", "api-worker", "hello")+"\n")

	if _, ok := f.WaitFrame(waitTimeout); ok {
		t.Fatal("CloseImmediately set with SetMode recorded a frame")
	}
}

func TestNoBindWithPath(t *testing.T) {
	missing := socktest.Path(t, "missing.sock")
	f := New(t, Options{PID: 6800, NoBind: true, SocketPath: missing})

	if got := f.SocketPath(); got != missing {
		t.Errorf("SocketPath = %q, want %q", got, missing)
	}
	if got := readRecord(t, f.RecordPath())["messagingSocketPath"]; got != missing {
		t.Errorf("messagingSocketPath = %v, want %q", got, missing)
	}
	if _, err := net.Dial("unix", missing); err == nil {
		t.Fatal("dialing an unbound path succeeded")
	}
}

func TestNoBindEmptyPath(t *testing.T) {
	f := New(t, Options{PID: 6900, NoBind: true})

	if got := f.SocketPath(); got != "" {
		t.Errorf("SocketPath = %q, want empty", got)
	}
	if got := readRecord(t, f.RecordPath())["messagingSocketPath"]; got != "" {
		t.Errorf("messagingSocketPath = %v, want empty", got)
	}
}

// Linux refuses a regular file with ECONNREFUSED and macOS with ENOTSOCK, so both are accepted.
func TestSocketRefusesConnections(t *testing.T) {
	path := filepath.Join(socktest.Dir(t), "regular-file")
	if err := os.WriteFile(path, []byte("not a socket"), 0o600); err != nil {
		t.Fatalf("writing regular file: %v", err)
	}
	f := New(t, Options{PID: 6950, NoBind: true, SocketPath: path})

	if got := readRecord(t, f.RecordPath())["messagingSocketPath"]; got != path {
		t.Errorf("messagingSocketPath = %v, want %q", got, path)
	}
	_, err := net.Dial("unix", path)
	if err == nil {
		t.Fatal("dialing a regular file succeeded")
	}
	if !errors.Is(err, syscall.ECONNREFUSED) && !errors.Is(err, syscall.ENOTSOCK) && !strings.Contains(err.Error(), "not a socket") {
		t.Fatalf("dialing a regular file: %v, want ECONNREFUSED, ENOTSOCK, or a not-a-socket error", err)
	}
}

func TestRemoveSocket(t *testing.T) {
	f := New(t, Options{PID: 7000})
	path := f.SocketPath()

	f.RemoveSocket()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket still present after RemoveSocket: %v", err)
	}
	if _, err := net.Dial("unix", path); err == nil {
		t.Fatal("dialing a removed socket succeeded")
	}
	if got := readRecord(t, f.RecordPath())["messagingSocketPath"]; got != path {
		t.Errorf("messagingSocketPath = %v, want the original path %q", got, path)
	}

	f.RemoveSocket() // idempotent
}

func TestBusyDelay(t *testing.T) {
	f := New(t, Options{PID: 7100, BusyDelay: 100 * time.Millisecond})
	f.SetBusyDelay(100 * time.Millisecond)

	conn := dialFake(t, f)
	writeAll(t, conn, userFrameLine(t, "msg-7", "api-worker", "hello")+"\n")

	if _, ok := f.WaitFrame(40 * time.Millisecond); ok {
		t.Fatal("frame arrived before the busy delay elapsed")
	}
	if _, ok := f.WaitFrame(eventTimeout); !ok {
		t.Fatal("frame did not arrive after the busy delay")
	}
}

func TestTwoFakesSameConfigDir(t *testing.T) {
	dir := t.TempDir()
	first := New(t, Options{ConfigDir: dir, PID: 7201, Name: "first"})
	second := New(t, Options{ConfigDir: dir, PID: 7202, Name: "second"})

	for _, fake := range []*Fake{first, second} {
		if _, err := os.Stat(fake.RecordPath()); err != nil {
			t.Fatalf("record %s: %v", fake.RecordPath(), err)
		}
	}
	if got := readRecord(t, first.RecordPath())["name"]; got != "first" {
		t.Errorf("first record name = %v, want first", got)
	}
	if got := readRecord(t, second.RecordPath())["name"]; got != "second" {
		t.Errorf("second record name = %v, want second", got)
	}
}

func TestWaitFrameTimeout(t *testing.T) {
	f := New(t, Options{PID: 7300})

	if _, ok := f.WaitFrame(20 * time.Millisecond); ok {
		t.Fatal("WaitFrame returned a frame with no connection")
	}
	if got := len(f.Frames()); got != 0 {
		t.Fatalf("recorded %d frames, want 0", got)
	}
}

func TestCloseIdempotent(t *testing.T) {
	f := New(t, Options{PID: 7400})
	path := f.SocketPath()
	tempDir := filepath.Dir(filepath.Dir(path))

	f.Close()
	f.Close()

	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket still present after Close: %v", err)
	}
	if _, err := os.Stat(tempDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary socket directory still present after Close: %v", err)
	}
	if _, ok := f.WaitFrame(10 * time.Millisecond); ok {
		t.Fatal("WaitFrame returned a frame after Close")
	}
}

func TestExtraOverridesFields(t *testing.T) {
	f := New(t, Options{
		PID:   7600,
		Name:  "base",
		Extra: map[string]any{"name": "override", "custom": 42},
	})

	record := readRecord(t, f.RecordPath())
	if got := record["name"]; got != "override" {
		t.Errorf("name = %v, want override", got)
	}
	if got := record["custom"]; got != float64(42) {
		t.Errorf("custom = %v, want 42", got)
	}
}

func TestFrameLineTooLong(t *testing.T) {
	f := New(t, Options{PID: 7500})
	conn := dialFake(t, f)
	// The write deadline only guards against a hang: a load-related stall must not
	// turn into a short timeout, and a real timeout means the write never
	// finished, so it fails rather than pass.
	if err := conn.SetWriteDeadline(time.Now().Add(eventTimeout)); err != nil {
		t.Fatalf("setting write deadline: %v", err)
	}

	line := bytes.Repeat([]byte("a"), maxFrameLineBytes+2)
	_, err := conn.Write(line)
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		t.Fatal("writing the oversized line timed out before it finished")
	}
	if err != nil {
		// Closing the connection once the oversized line is seen can make the
		// write fail; either outcome must leave no frame.
		t.Logf("writing oversized line returned %v", err)
	}
	// Negative window: an oversized frame that was recorded still shows up here.
	if _, ok := f.WaitFrame(waitTimeout); ok {
		t.Fatal("an oversized frame was recorded")
	}
	if got := len(f.Frames()); got != 0 {
		t.Fatalf("recorded %d frames, want 0", got)
	}
}

// readRecord reads and decodes a registry record.
func readRecord(t *testing.T, path string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading record %s: %v", path, err)
	}
	var record map[string]any
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatalf("decoding record %s: %v", path, err)
	}
	return record
}

func mustModTime(t *testing.T, path string) time.Time {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.ModTime()
}

// fakeConn is a client connection to a Fake. It records how many connections
// the fake had closed when it was dialed, so the test can wait for this
// connection's own close without assuming a latency bound.
type fakeConn struct {
	net.Conn
	fake       *Fake
	priorClose int
}

func dialFake(t *testing.T, f *Fake) *fakeConn {
	t.Helper()
	// The count is sampled before dialing: this connection cannot close before
	// it exists, so any later advance belongs to it. The tests using dialFake
	// keep one connection per fake in flight at a time.
	prior := f.closedConnCount()
	raw, err := net.Dial("unix", f.SocketPath())
	if err != nil {
		t.Fatalf("dialing %s: %v", f.SocketPath(), err)
	}
	conn := &fakeConn{Conn: raw, fake: f, priorClose: prior}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func writeAll(t *testing.T, conn net.Conn, data string) {
	t.Helper()
	if _, err := io.WriteString(conn, data); err != nil {
		t.Fatalf("writing to the fake socket: %v", err)
	}
}

func closeWrite(t *testing.T, conn net.Conn) {
	t.Helper()
	if fc, ok := conn.(*fakeConn); ok {
		conn = fc.Conn
	}
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		t.Fatalf("connection type = %T, want *net.UnixConn", conn)
	}
	if err := unixConn.CloseWrite(); err != nil {
		t.Fatalf("half-closing the connection: %v", err)
	}
}

// userFrameLine builds a user frame in the wire format the socket transport
// library produces.
func userFrameLine(t *testing.T, msgID, fromName, body string) string {
	t.Helper()
	content := body
	if fromName != "" {
		escaped := strings.ReplaceAll(body, "</cross-session-message", `<\/cross-session-message`)
		content = fmt.Sprintf("<cross-session-message from-name=%q>\n%s\n</cross-session-message>", fromName, escaped)
	}
	frame := map[string]any{
		"msgV":     1,
		"msg_id":   msgID,
		"type":     "user",
		"message":  map[string]any{"role": "user", "content": content},
		"priority": "next",
	}
	data, err := json.Marshal(frame)
	if err != nil {
		t.Fatalf("encoding frame: %v", err)
	}
	return string(data)
}
