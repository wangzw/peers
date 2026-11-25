//go:build linux

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// frameChildTimeout bounds one frame written by a freshly executed subprocess,
// where process cold start dominates the wait.
const frameChildTimeout = 10 * time.Second

func decodeFrame(t *testing.T, raw []byte) verbatimFrame {
	t.Helper()
	if !bytes.HasSuffix(raw, []byte("\n")) {
		t.Fatalf("frame does not end in a newline: %q", raw)
	}
	var frame verbatimFrame
	if err := json.Unmarshal(bytes.TrimSuffix(raw, []byte("\n")), &frame); err != nil {
		t.Fatalf("decoding frame: %v", err)
	}
	return frame
}

// TestVerbatimFrameKeepsNameBytes checks that a name survives byte for byte in
// verbatim mode and the frame fields keep their order.
func TestVerbatimFrameKeepsNameBytes(t *testing.T) {
	// A U+2028 and a quote both survive byte for byte; verbatim mode tests
	// names the Broker would have cleaned.
	name := "a b\"c"
	frame, err := buildVerbatimFrame(name, "hello", "msg-1")
	if err != nil {
		t.Fatalf("buildVerbatimFrame: %v", err)
	}
	wantPrefix := `{"msgV":1,"msg_id":"msg-1","type":"user","message":{"role":"user","content":`
	if !strings.HasPrefix(string(frame), wantPrefix) {
		t.Errorf("frame does not start with the userFrame fields in order:\n got %q", frame)
	}

	decoded := decodeFrame(t, frame)
	if decoded.MsgV != 1 || decoded.MsgID != "msg-1" || decoded.Type != "user" ||
		decoded.Message.Role != "user" || decoded.Priority != "next" {
		t.Errorf("unexpected frame fields: %+v", decoded)
	}
	want := "<cross-session-message from-name=\"a b\"c\">\nhello\n</cross-session-message>"
	if decoded.Message.Content != want {
		t.Errorf("content = %q, want %q", decoded.Message.Content, want)
	}
}

// TestLibraryModeQuotesName checks that library mode quotes a backslash in the
// sender name.
func TestLibraryModeQuotesName(t *testing.T) {
	path, lines := listenOnce(t)
	var stdout strings.Builder
	err := runFrame([]string{
		"--socket", path,
		"--from-name", `a\b`,
		"--body", "hello",
		"--mode", "library",
	}, &stdout)
	if err != nil {
		t.Fatalf("runFrame: %v", err)
	}
	if !strings.Contains(stdout.String(), " ok ") {
		t.Errorf("result line does not report success: %q", stdout.String())
	}

	line := receiveLine(t, lines)
	var frame verbatimFrame
	if err := json.Unmarshal([]byte(line), &frame); err != nil {
		t.Fatalf("decoding library frame: %v", err)
	}
	// The library sanitizes and then quotes the name with %q, so one backslash
	// on the wire becomes two inside the attribute.
	if !strings.Contains(frame.Message.Content, `from-name="a\\b"`) {
		t.Errorf("content %q does not carry the library-quoted name", frame.Message.Content)
	}
}

// TestBodyClosingTagEscaped checks that a closing envelope tag in the body
// cannot end the envelope early.
func TestBodyClosingTagEscaped(t *testing.T) {
	frame, err := buildVerbatimFrame("n", "seen </cross-session-message> here", "msg-2")
	if err != nil {
		t.Fatalf("buildVerbatimFrame: %v", err)
	}
	decoded := decodeFrame(t, frame)
	want := "<cross-session-message from-name=\"n\">\nseen <\\/cross-session-message> here\n</cross-session-message>"
	if decoded.Message.Content != want {
		t.Errorf("content = %q, want %q", decoded.Message.Content, want)
	}
}

// TestForkEachUsesDistinctPids checks that --fork-each writes each frame from a
// different child process.
func TestForkEachUsesDistinctPids(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "rsprobe")
	build := exec.Command("go", "build", "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building rsprobe: %v\n%s", err, out)
	}

	const count = 3
	path := filepath.Join(t.TempDir(), "inbox.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	pids := make(chan int, count)
	go func() {
		for i := 0; i < count; i++ {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			pid, pidErr := peerPIDChecked(conn)
			_, _ = io.Copy(io.Discard, conn)
			_ = conn.Close()
			if pidErr == nil {
				pids <- pid
			}
		}
	}()

	cmd := exec.Command(bin, "frame",
		"--socket", path,
		"--from-name", "n",
		"--body", "m{i}",
		"--count", fmt.Sprint(count),
		"--fork-each")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("rsprobe frame --fork-each: %v\n%s", err, out)
	}

	seen := make(map[int]bool, count)
	for i := 0; i < count; i++ {
		select {
		case pid := <-pids:
			if pid == os.Getpid() {
				t.Errorf("frame %d was written by the test process itself", i)
			}
			if seen[pid] {
				t.Errorf("frame %d reused writer pid %d", i, pid)
			}
			seen[pid] = true
		case <-time.After(frameChildTimeout):
			t.Fatalf("received only %d of %d frames", i, count)
		}
	}
	if len(seen) != count {
		t.Errorf("saw %d distinct writer pids, want %d", len(seen), count)
	}
}

// listenOnce starts a unix listener and returns its path and a channel
// receiving the first line written to it. The listener closes the connection
// after reading the newline, which the frame writer waits for.
func listenOnce(t *testing.T) (string, <-chan string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "inbox.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	lines := make(chan string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		line, err := bufio.NewReader(conn).ReadString('\n')
		if err == nil {
			lines <- line
		}
	}()
	return path, lines
}

func receiveLine(t *testing.T, lines <-chan string) string {
	t.Helper()
	select {
	case line := <-lines:
		return line
	case <-time.After(eventTimeout):
		t.Fatal("timed out waiting for a frame")
		return ""
	}
}

// peerPIDChecked reads the connecting process's pid with SO_PEERCRED and
// reports why it could not.
func peerPIDChecked(conn net.Conn) (int, error) {
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return 0, fmt.Errorf("connection is %T, not a unix socket", conn)
	}
	raw, err := unixConn.SyscallConn()
	if err != nil {
		return 0, err
	}
	var (
		cred    *unix.Ucred
		credErr error
	)
	if err := raw.Control(func(fd uintptr) {
		cred, credErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return 0, err
	}
	if credErr != nil {
		return 0, credErr
	}
	return int(cred.Pid), nil
}
