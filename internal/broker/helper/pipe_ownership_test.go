package helper

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

// assertPipeOpen fails t unless f is still open.
func assertPipeOpen(t *testing.T, name string, f *os.File) {
	t.Helper()
	if _, err := f.Stat(); err != nil {
		t.Errorf("%s was closed before the child was reaped: %v", name, err)
	}
}

// TestAccessCheckStartStartFailureClosesPipes checks that a failed Start closes both ends of both pipes it created.
func TestAccessCheckStartStartFailureClosesPipes(t *testing.T) {
	rec := stubHelperPipe(t, 0, nil)
	exe := filepath.Join(t.TempDir(), "missing-peers")

	proc, err := accessCheckStart(exe)(context.Background(), Account{UID: uint32(os.Geteuid())})
	if proc != nil {
		t.Fatal("accessCheckStart returned a process after Start failed")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("accessCheckStart error = %v, want the exec failure", err)
	}
	if len(rec.pipes) != 2 {
		t.Fatalf("created %d pipes, want 2", len(rec.pipes))
	}
	assertPipeClosed(t, "stdin read end", rec.pipes[0][0])
	assertPipeClosed(t, "stdin writer", rec.pipes[0][1])
	assertPipeClosed(t, "stdout read end", rec.pipes[1][0])
	assertPipeClosed(t, "stdout writer", rec.pipes[1][1])
}

// TestAccessCheckStartClosesChildEnds checks that a started check closes the child ends of both pipes and, after Wait, the parent ends.
func TestAccessCheckStartClosesChildEnds(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	rec := stubHelperPipe(t, 0, nil)
	acct := Account{UID: uint32(os.Geteuid()), GID: uint32(os.Getegid())}

	proc, err := accessCheckStart(exe)(context.Background(), acct)
	if err != nil {
		t.Fatalf("accessCheckStart: %v", err)
	}
	t.Cleanup(func() {
		_ = proc.Kill()
		_ = proc.Wait()
	})
	if len(rec.pipes) != 2 {
		t.Fatalf("created %d pipes, want 2", len(rec.pipes))
	}
	stdin, stdout := rec.pipes[0], rec.pipes[1]
	assertPipeClosed(t, "stdin read end", stdin[0])
	assertPipeClosed(t, "stdout writer", stdout[1])
	assertPipeOpen(t, "stdin writer", stdin[1])
	assertPipeOpen(t, "stdout read end", stdout[0])

	_ = proc.Kill()
	_ = proc.Wait()
	assertPipeClosed(t, "stdin writer", stdin[1])
	assertPipeClosed(t, "stdout read end", stdout[0])
}

// TestStartHelperProcessArgsStartFailureClosesPipes checks that a failed Start closes both ends of both pipes.
func TestStartHelperProcessArgsStartFailureClosesPipes(t *testing.T) {
	rec := stubHelperPipe(t, 0, nil)
	exe := filepath.Join(t.TempDir(), "missing-peers")

	proc, err := startHelperProcessArgs(exe, nil, Account{UID: uint32(os.Geteuid())},
		slog.New(slog.NewTextHandler(io.Discard, nil)), func() bool { return false })
	if proc != nil {
		t.Fatal("startHelperProcessArgs returned a process after Start failed")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("startHelperProcessArgs error = %v, want the exec failure", err)
	}
	if len(rec.pipes) != 2 {
		t.Fatalf("created %d pipes, want 2", len(rec.pipes))
	}
	assertPipeClosed(t, "stdin read end", rec.pipes[0][0])
	assertPipeClosed(t, "stdin writer", rec.pipes[0][1])
	assertPipeClosed(t, "stdout read end", rec.pipes[1][0])
	assertPipeClosed(t, "stdout writer", rec.pipes[1][1])
}

// TestStartHelperProcessArgsClosesChildEnds checks that a started helper closes the child ends of both pipes and, after Wait, the parent ends.
func TestStartHelperProcessArgsClosesChildEnds(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	acct, err := LookupAccount(uint32(os.Getuid()))
	if err != nil {
		t.Fatalf("LookupAccount: %v", err)
	}
	rec := stubHelperPipe(t, 0, nil)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	proc, err := startHelperProcessArgs(exe, []string{"internal", "account-helper"}, acct, log, nil)
	if err != nil {
		t.Fatalf("startHelperProcessArgs: %v", err)
	}
	t.Cleanup(func() {
		_ = proc.Kill()
		_ = proc.Wait()
	})
	if len(rec.pipes) != 2 {
		t.Fatalf("created %d pipes, want 2", len(rec.pipes))
	}
	stdin, stdout := rec.pipes[0], rec.pipes[1]
	assertPipeClosed(t, "stdin read end", stdin[0])
	assertPipeClosed(t, "stdout writer", stdout[1])
	assertPipeOpen(t, "stdin writer", stdin[1])
	assertPipeOpen(t, "stdout read end", stdout[0])

	_ = proc.Kill()
	_ = proc.Wait()
	assertPipeClosed(t, "stdin writer", stdin[1])
	assertPipeClosed(t, "stdout read end", stdout[0])
}
