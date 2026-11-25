package helper

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
)

// TestHelperStderrTagsUIDAndFlushes checks that a complete line is logged with the uid and an unterminated one only on flush.
func TestHelperStderrTagsUIDAndFlushes(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	w := newHelperStderr(log, 4242, nil)

	if _, err := w.Write([]byte("first line\nsecond")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got := buf.String(); !strings.Contains(got, "first line") {
		t.Fatalf("log %q does not contain the complete line", got)
	}
	if got := buf.String(); strings.Contains(got, "second") {
		t.Fatalf("log %q contains an unterminated line before flush", got)
	}

	w.flush()
	out := buf.String()
	for _, want := range []string{"uid=4242", "msg=account_helper_stderr", "second"} {
		if !strings.Contains(out, want) {
			t.Fatalf("log %q does not contain %q", out, want)
		}
	}
}

// TestHelperStderrContextCanceledDuringStopIsDebug checks that a cancellation line written while the Broker stops is logged at DEBUG.
func TestHelperStderrContextCanceledDuringStopIsDebug(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	w := newHelperStderr(log, 4242, func() bool { return true })

	if _, err := w.Write([]byte("peers internal account-helper: context canceled\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	out := buf.String()
	if strings.Contains(out, "level=WARN") {
		t.Fatalf("a context-canceled stderr line during a stop was logged at WARN: %q", out)
	}
	for _, want := range []string{"level=DEBUG", "msg=account_helper_stderr", "uid=4242"} {
		if !strings.Contains(out, want) {
			t.Fatalf("log %q does not contain %q", out, want)
		}
	}
}

// TestHelperStderrContextCanceledOutsideStopStaysWarn checks that a cancellation line outside a stop is logged at WARN.
func TestHelperStderrContextCanceledOutsideStopStaysWarn(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	w := newHelperStderr(log, 4242, func() bool { return false })

	if _, err := w.Write([]byte("peers internal account-helper: context canceled\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "level=WARN") {
		t.Fatalf("a context-canceled stderr line outside a stop = %q, want WARN", out)
	}
}

// TestHelperStderrOtherLineDuringStopStaysWarn checks that a non-cancellation line is logged at WARN even during a stop.
func TestHelperStderrOtherLineDuringStopStaysWarn(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	w := newHelperStderr(log, 4242, func() bool { return true })

	if _, err := w.Write([]byte("helper: writing to the registry failed\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if out := buf.String(); !strings.Contains(out, "level=WARN") {
		t.Fatalf("a non-cancellation stderr line during a stop = %q, want WARN", out)
	}
}

// TestHelperStderrTruncatesLongUnterminatedLine checks that an overlong unterminated line is logged truncated and the next line starts fresh.
func TestHelperStderrTruncatesLongUnterminatedLine(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	w := newHelperStderr(log, 4242, nil)

	line := strings.Repeat("x", maxHelperStderrLine+10)
	n, err := w.Write([]byte(line))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len(line) {
		t.Fatalf("Write = %d, want %d", n, len(line))
	}
	out := buf.String()
	if !strings.Contains(out, "(truncated)") {
		t.Fatalf("log %q does not contain the truncation marker", out)
	}
	if !strings.Contains(out, strings.Repeat("x", maxHelperStderrLine)) {
		t.Fatalf("log does not hold the first %d bytes of the line", maxHelperStderrLine)
	}
	if strings.Contains(out, strings.Repeat("x", maxHelperStderrLine+1)) {
		t.Fatal("log holds more than the line limit")
	}

	if _, err := w.Write([]byte("next line\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got := buf.String(); !strings.Contains(got, "next line") {
		t.Fatalf("log %q does not contain the line after a truncated one", got)
	}
}

// TestProcessHelperStderrIsTaggedWithAccount checks that a real helper child's stderr is logged with its account uid.
func TestProcessHelperStderrIsTaggedWithAccount(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	acct, err := LookupAccount(uint32(os.Getuid()))
	if err != nil {
		t.Fatalf("LookupAccount: %v", err)
	}

	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	proc, err := startHelperProcessArgs(exe, []string{"internal", "account-helper", "stderr-marker"}, acct, log, nil)
	if err != nil {
		t.Fatalf("startHelperProcessArgs: %v", err)
	}
	client := NewClient(proc.Stdout, proc.Stdin)
	ctx := testContext(t)
	if _, err := client.WaitReady(ctx); err != nil {
		_ = proc.Kill()
		_ = proc.Wait()
		t.Fatalf("WaitReady: %v", err)
	}
	_ = client.Close()
	if err := proc.Wait(); err != nil {
		t.Fatalf("helper exited with %v, want a clean exit", err)
	}

	out := buf.String()
	for _, want := range []string{"helper stderr marker", fmt.Sprintf("uid=%d", acct.UID)} {
		if !strings.Contains(out, want) {
			t.Fatalf("helper stderr log %q does not contain %q", out, want)
		}
	}
}
