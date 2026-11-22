package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

func newMCPApp(s *stubDeps) (*app, *bytes.Buffer, *bytes.Buffer) {
	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)
	return &app{stdout: stdout, stderr: stderr, version: "test", deps: s.deps()}, stdout, stderr
}

func TestMCPWritesNothingToStdout(t *testing.T) {
	stub := &stubDeps{}
	a, stdout, stderr := newMCPApp(stub)

	if got := a.mcpCmd(context.Background(), nil); got != exitOK {
		t.Fatalf("mcpCmd returned %d, want %d", got, exitOK)
	}
	if stub.mcpCalls != 1 {
		t.Fatalf("runMCP called %d times, want 1", stub.mcpCalls)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestMCPContextCancelledBySignalContext(t *testing.T) {
	ctx, stop := newSignalContext(context.Background())
	defer stop()

	cancelled := make(chan struct{})
	stub := &stubDeps{
		runMCP: func(ctx context.Context) error {
			<-ctx.Done()
			close(cancelled)
			return nil
		},
	}
	a, _, _ := newMCPApp(stub)

	done := make(chan int, 1)
	go func() {
		done <- a.run(ctx, []string{"mcp"})
	}()

	//procsig:allow sends SIGTERM to this test's own process
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("sending SIGTERM to self: %v", err)
	}

	select {
	case <-cancelled:
	case <-time.After(processTimeout):
		t.Fatalf("runMCP did not observe the context cancellation within %s", processTimeout)
	}
	select {
	case code := <-done:
		if code != exitOK {
			t.Fatalf("run returned %d, want %d", code, exitOK)
		}
	case <-time.After(processTimeout):
		t.Fatalf("run did not return within %s after the context was cancelled", processTimeout)
	}
}

func TestMCPRejectsArguments(t *testing.T) {
	cases := []struct {
		args     []string
		wantLast string
	}{
		{[]string{"x"}, `peers mcp: unexpected argument "x"`},
		{[]string{"--foo"}, "peers mcp: flag provided but not defined: -foo"},
	}
	for _, tc := range cases {
		stub := &stubDeps{}
		a, _, stderr := newMCPApp(stub)

		if got := a.mcpCmd(context.Background(), tc.args); got != exitUsage {
			t.Fatalf("mcpCmd(%q) returned %d, want %d", tc.args, got, exitUsage)
		}
		if stub.mcpCalls != 0 {
			t.Fatalf("mcpCmd(%q) called runMCP %d times, want 0", tc.args, stub.mcpCalls)
		}
		if !strings.HasPrefix(stderr.String(), mcpUsage) {
			t.Fatalf("stderr = %q, want the usage before the reason", stderr.String())
		}
		if got := brokerLastLine(stderr.String()); got != tc.wantLast {
			t.Fatalf("last stderr line = %q, want %q", got, tc.wantLast)
		}
	}
}

func TestMCPErrorExitsOne(t *testing.T) {
	stub := &stubDeps{mcpErr: errors.New("boom")}
	a, stdout, stderr := newMCPApp(stub)

	if got := a.mcpCmd(context.Background(), nil); got != exitFailure {
		t.Fatalf("mcpCmd returned %d, want %d", got, exitFailure)
	}
	if want := "peers mcp: boom\n"; stderr.String() != want {
		t.Fatalf("stderr = %q, want %q", stderr.String(), want)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
}

func TestMCPHelp(t *testing.T) {
	for _, flagName := range []string{"-h", "--help"} {
		stub := &stubDeps{}
		a, stdout, stderr := newMCPApp(stub)

		if got := a.mcpCmd(context.Background(), []string{flagName}); got != exitOK {
			t.Fatalf("mcpCmd(%q) returned %d, want %d", flagName, got, exitOK)
		}
		if stub.mcpCalls != 0 {
			t.Fatalf("mcpCmd(%q) called runMCP %d times, want 0", flagName, stub.mcpCalls)
		}
		if want := "usage: peers mcp\n"; stdout.String() != want {
			t.Fatalf("stdout = %q, want %q", stdout.String(), want)
		}
		if stderr.Len() != 0 {
			t.Fatalf("stderr = %q, want empty", stderr.String())
		}
	}
}
