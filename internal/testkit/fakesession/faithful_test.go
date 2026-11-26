package fakesession

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

func frameLineWithSessionID(t *testing.T, msgID, fromName, body, sessionID string) string {
	t.Helper()
	line := userFrameLine(t, msgID, fromName, body)
	if sessionID == "" {
		return line
	}
	var fields map[string]any
	if err := json.Unmarshal([]byte(line), &fields); err != nil {
		t.Fatalf("decoding frame: %v", err)
	}
	fields["session_id"] = sessionID
	data, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("encoding frame: %v", err)
	}
	return string(data)
}

// The fake records a frame before closing the connection, so once sendFrameLine
// returns any recorded frame is already visible to Frames and WaitFrame.
func sendFrameLine(t *testing.T, f *Fake, line string) {
	t.Helper()
	conn := dialFake(t, f)
	writeAll(t, conn, line+"\n")
	closeWrite(t, conn)
	assertConnectionClosed(t, conn)
}

func assertConnectionClosed(t *testing.T, conn *fakeConn) {
	t.Helper()
	if !conn.fake.waitClosedSince(conn.priorClose, eventTimeout) {
		t.Fatal("the fake did not close the connection")
	}
	if err := conn.SetReadDeadline(time.Now().Add(eventTimeout)); err != nil {
		t.Fatalf("setting read deadline: %v", err)
	}
	if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("reading after the frame = %v, want EOF", err)
	}
}

// TestFrameSessionIDFiltering checks that frames with no session id or the
// current id are recorded while a frame addressed to another id is dropped,
// including after the fake's id changes.
func TestFrameSessionIDFiltering(t *testing.T) {
	f := New(t, Options{PID: 8001, SessionID: "current-session"})

	sendFrameLine(t, f, frameLineWithSessionID(t, "matching", "sender", "one", "current-session"))
	sendFrameLine(t, f, userFrameLine(t, "absent", "sender", "two"))
	sendFrameLine(t, f, frameLineWithSessionID(t, "stale", "sender", "three", "old-session"))

	f.SetSessionID("new-session")

	sendFrameLine(t, f, frameLineWithSessionID(t, "previous", "sender", "four", "current-session"))
	sendFrameLine(t, f, frameLineWithSessionID(t, "new", "sender", "five", "new-session"))

	want := []string{"matching", "absent", "new"}
	frames := f.Frames()
	if len(frames) != len(want) {
		t.Fatalf("recorded %d frames, want %d: %+v", len(frames), len(want), frames)
	}
	for i, id := range want {
		if frames[i].MsgID != id {
			t.Fatalf("frame %d msg id = %q, want %q", i+1, frames[i].MsgID, id)
		}
	}
}

// TestFrameWithMismatchedSessionIDIsDropped checks that a frame with a
// mismatched session_id is neither recorded nor returned by WaitFrame.
func TestFrameWithMismatchedSessionIDIsDropped(t *testing.T) {
	f := New(t, Options{PID: 8002, SessionID: "current-session"})

	conn := dialFake(t, f)
	writeAll(t, conn, frameLineWithSessionID(t, "dropped", "sender", "hello", "other-session")+"\n")
	closeWrite(t, conn)
	assertConnectionClosed(t, conn)

	if got := len(f.Frames()); got != 0 {
		t.Fatalf("recorded %d frames, want 0", got)
	}
	if frame, ok := f.WaitFrame(20 * time.Millisecond); ok {
		t.Fatalf("dropped frame surfaced from WaitFrame: %+v", frame)
	}
}

// TestFromNameCleaning checks that a from-name cleaning leaves unchanged is
// reported while one cleaning would change leaves FromName empty.
func TestFromNameCleaning(t *testing.T) {
	cases := []struct {
		name string
		from string
		want string
	}{
		{"plain", "api-worker@hostA", "api-worker@hostA"},
		{"quote", `a"b`, ""},
		{"less-than", "a<b", ""},
		{"greater-than", "a>b", ""},
		{"sixty-four-runes", strings.Repeat("a", 64), strings.Repeat("a", 64)},
		{"sixty-five-runes", strings.Repeat("a", 65), ""},
		{"zero-width", "a" + string(rune(0x200b)) + "b", ""},
		{"tab", "a\tb", ""},
		{"newline", "a\nb", ""},
		{"leading-space", " a", ""},
		{"trailing-space", "a ", ""},
	}

	f := New(t, Options{PID: 8010})
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sendFrameLine(t, f, userFrameLine(t, fmt.Sprintf("m-%d", i), tc.from, "hello"))
			frames := f.Frames()
			if len(frames) != i+1 {
				t.Fatalf("recorded %d frames, want %d", len(frames), i+1)
			}
			frame := frames[i]
			if frame.FromName != tc.want {
				t.Fatalf("FromName = %q, want %q", frame.FromName, tc.want)
			}
			if !strings.Contains(frame.Content, "<cross-session-message") {
				t.Fatalf("Content = %q, want the attribution envelope preserved", frame.Content)
			}
		})
	}
}

// TestNonJSONLineIsNotRecorded checks that a line which is not a JSON object is
// not recorded or reported as a frame.
func TestNonJSONLineIsNotRecorded(t *testing.T) {
	lines := map[string]string{
		"garbage":     "this is not json",
		"json-string": `"just a string"`,
		"json-array":  `[1,2,3]`,
		"json-null":   `null`,
	}
	f := New(t, Options{PID: 8020})
	for name, line := range lines {
		t.Run(name, func(t *testing.T) {
			sendFrameLine(t, f, line)
			if got := len(f.Frames()); got != 0 {
				t.Fatalf("non-frame line was recorded: %d frames", got)
			}
		})
	}
	if frame, ok := f.WaitFrame(20 * time.Millisecond); ok {
		t.Fatalf("non-frame line surfaced from WaitFrame: %+v", frame)
	}
}

// TestWaitFrameReturnsFramesInOrder checks that repeated WaitFrame calls walk
// the recorded frames in order.
func TestWaitFrameReturnsFramesInOrder(t *testing.T) {
	f := New(t, Options{PID: 8030})

	sendFrameLine(t, f, userFrameLine(t, "first", "sender", "one"))
	sendFrameLine(t, f, userFrameLine(t, "second", "sender", "two"))

	first, ok := f.WaitFrame(eventTimeout)
	if !ok {
		t.Fatal("first frame not received")
	}
	if first.MsgID != "first" {
		t.Fatalf("first frame msg id = %q, want first", first.MsgID)
	}
	second, ok := f.WaitFrame(eventTimeout)
	if !ok {
		t.Fatal("second frame not received")
	}
	if second.MsgID != "second" {
		t.Fatalf("second frame msg id = %q, want second", second.MsgID)
	}
	if frame, ok := f.WaitFrame(20 * time.Millisecond); ok {
		t.Fatalf("WaitFrame returned an extra frame: %+v", frame)
	}
}
