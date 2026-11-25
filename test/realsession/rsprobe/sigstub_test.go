package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// sigstubHelperEnv marks the re-executed test binary as the sigstub child; its
// value is the log path.
const sigstubHelperEnv = "RSPROBE_SIGSTUB_LOG"

// TestSigstubHelper serves "rsprobe sigstub" as the child half of
// TestSigstubLogsAndIgnoresSignals, and skips otherwise.
func TestSigstubHelper(t *testing.T) {
	logPath := os.Getenv(sigstubHelperEnv)
	if logPath == "" {
		t.Skip("sigstub child process only")
	}
	if err := cmdSigstub([]string{"--log", logPath}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// TestSigstubLogsAndIgnoresSignals checks that the stub logs start and signal
// events and stays alive after them.
func TestSigstubLogsAndIgnoresSignals(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "sigstub.jsonl")
	cmd := exec.Command(os.Args[0], "-test.run=^TestSigstubHelper$")
	cmd.Env = append(os.Environ(),
		sigstubHelperEnv+"="+logPath,
		"CLAUDE_CODE_MESSAGING_SOCKET=/tmp/rs/sigstub.sock",
	)
	// Keep standard input open: a closed stdin would make the stub log
	// stdin-eof before the signals and the test wants the signal sequence only.
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin pipe: %v", err)
	}
	defer func() { _ = stdin.Close() }()
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sigstub: %v", err)
	}

	exited := make(chan struct{})
	var waitErr error
	go func() {
		waitErr = cmd.Wait()
		close(exited)
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case <-exited:
		case <-time.After(eventTimeout):
			t.Errorf("sigstub did not exit after SIGKILL")
		}
	})

	// The start line is written after the signal handlers are installed, so its
	// appearance also tells us that sending SIGINT at once is safe.
	if !waitUntil(5*time.Second, func() bool {
		events := readSigstubEvents(logPath)
		return len(events) > 0 && events[0].Event == "start"
	}) {
		t.Fatalf("no start record in %s", logPath)
	}
	start := readSigstubEvents(logPath)[0]
	if start.PID != cmd.Process.Pid {
		t.Errorf("start pid = %d, want %d", start.PID, cmd.Process.Pid)
	}
	if start.PPID != os.Getpid() {
		t.Errorf("start ppid = %d, want %d", start.PPID, os.Getpid())
	}
	if start.MessagingSock == nil || *start.MessagingSock != "/tmp/rs/sigstub.sock" {
		t.Errorf("start messaging_socket = %v, want /tmp/rs/sigstub.sock", start.MessagingSock)
	}
	if start.TMonNs != 0 {
		t.Errorf("start t_mono_ns = %d, want 0", start.TMonNs)
	}

	for _, sig := range []syscall.Signal{syscall.SIGINT, syscall.SIGTERM} {
		if err := cmd.Process.Signal(sig); err != nil {
			t.Fatalf("send %s: %v", signalName(sig), err)
		}
		want := signalName(sig)
		if !waitUntil(5*time.Second, func() bool {
			return countSignals(readSigstubEvents(logPath), want) > 0
		}) {
			t.Fatalf("no %s record in %s", want, logPath)
		}
	}

	// A stub that exits on a signal would have closed exited by now.
	select {
	case <-exited:
		t.Fatalf("sigstub exited after signals: %v", waitErr)
	case <-time.After(500 * time.Millisecond):
	}

	sigs := signalEvents(readSigstubEvents(logPath))
	if len(sigs) != 2 {
		t.Fatalf("got %d signal records, want 2: %+v", len(sigs), sigs)
	}
	if sigs[0].Sig != "SIGINT" || sigs[1].Sig != "SIGTERM" {
		t.Fatalf("signal order = %q, %q, want SIGINT, SIGTERM", sigs[0].Sig, sigs[1].Sig)
	}
	if sigs[0].TMonNs > sigs[1].TMonNs {
		t.Errorf("signal times %d, %d are not monotonic", sigs[0].TMonNs, sigs[1].TMonNs)
	}
}

func waitUntil(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

func readSigstubEvents(path string) []sigstubEvent {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer func() { _ = f.Close() }()

	var events []sigstubEvent
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var ev sigstubEvent
		if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
			continue
		}
		events = append(events, ev)
	}
	return events
}

func signalEvents(events []sigstubEvent) []sigstubEvent {
	var out []sigstubEvent
	for _, ev := range events {
		if ev.Event == "signal" {
			out = append(out, ev)
		}
	}
	return out
}

func countSignals(events []sigstubEvent, name string) int {
	n := 0
	for _, ev := range signalEvents(events) {
		if ev.Sig == name {
			n++
		}
	}
	return n
}
