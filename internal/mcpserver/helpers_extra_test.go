package mcpserver

import (
	"context"
	"errors"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// TestNormalizeRemoteParseError reports an unparsable remote as no topic.
func TestNormalizeRemoteParseError(t *testing.T) {
	if got, ok := normalizeRemote("http://[::1"); ok || got != "" {
		t.Fatalf("normalizeRemote(invalid URL) = (%q, %v), want (\"\", false)", got, ok)
	}
}

// TestOpeningPhraseForUpdate prefixes an update failure with its own phrase.
func TestOpeningPhraseForUpdate(t *testing.T) {
	if got := openingPhrase("update_broker", callUpdate); got != textUpdateFailed {
		t.Fatalf("openingPhrase(update) = %q, want %q", got, textUpdateFailed)
	}
}

// TestSystemPeerUIDRejectsNonUnixConn rejects a connection that is not a Unix
// socket.
func TestSystemPeerUIDRejectsNonUnixConn(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	if _, err := systemPeerUID(client); err == nil || !strings.Contains(err.Error(), "not a Unix socket") {
		t.Fatalf("systemPeerUID(pipe) error = %v, want it to reject a non-Unix connection", err)
	}
}

// TestPeerUIDFromFDRejectsBadDescriptor reports EBADF for a bad descriptor and
// the account for a real socket.
func TestPeerUIDFromFDRejectsBadDescriptor(t *testing.T) {
	if _, err := peerUIDFromFD(-1); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("peerUIDFromFD(-1) = %v, want EBADF", err)
	}

	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	defer func() {
		_ = syscall.Close(fds[0])
		_ = syscall.Close(fds[1])
	}()
	uid, err := peerUIDFromFD(fds[0])
	if err != nil {
		t.Fatalf("peerUIDFromFD(socket) = %v, want nil", err)
	}
	if uid != uint32(os.Geteuid()) {
		t.Fatalf("peerUIDFromFD(socket) = %d, want %d", uid, os.Geteuid())
	}
}

// writeFakeGit puts an executable named git with body on PATH.
func writeFakeGit(t *testing.T, body string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "git")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0o755); err != nil {
		t.Fatalf("writing the fake git: %v", err)
	}
	t.Setenv("PATH", dir)
}

// TestGitOriginFailsWithoutStderr returns the git exit status and the directory
// when git prints nothing on standard error.
func TestGitOriginFailsWithoutStderr(t *testing.T) {
	dir := t.TempDir()
	writeFakeGit(t, "exit 1\n")
	_, err := gitOrigin(context.Background(), dir)
	if err == nil {
		t.Fatal("gitOrigin succeeded although git failed")
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
		t.Fatalf("gitOrigin error = %v, want an exec exit status of 1", err)
	}
	if !strings.Contains(err.Error(), dir) {
		t.Fatalf("gitOrigin error = %v, want it to name %s", err, dir)
	}
	if !strings.HasSuffix(err.Error(), ": exit status 1") {
		t.Fatalf("gitOrigin error = %v, want the bare exit status without stderr text", err)
	}
}

// TestGitOriginRejectsEmptyURL reports an empty remote.origin.url.
func TestGitOriginRejectsEmptyURL(t *testing.T) {
	writeFakeGit(t, "exit 0\n")
	_, err := gitOrigin(context.Background(), t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "empty remote.origin.url") {
		t.Fatalf("gitOrigin error = %v, want the empty URL failure", err)
	}
}

// TestLookupDefaultTopicsNilLogger joins a default topic from a usable origin
// and returns a settled empty result when there is none, both with the default
// logger.
func TestLookupDefaultTopicsNilLogger(t *testing.T) {
	dir := t.TempDir()
	got := lookupDefaultTopics(context.Background(), dir,
		func(context.Context, string) (string, error) {
			return "https://github.com/acme/repo.git", nil
		}, nil)
	if !got.settled || len(got.topics) != 1 || got.topics[0] != "github.com/acme/repo" || got.projectAutoJoinOff {
		t.Fatalf("lookupDefaultTopics(origin) = %+v, want the topic github.com/acme/repo", got)
	}

	got = lookupDefaultTopics(context.Background(), dir,
		func(context.Context, string) (string, error) { return "", errors.New("no origin") }, nil)
	if !got.settled || len(got.topics) != 0 || got.projectAutoJoinOff {
		t.Fatalf("lookupDefaultTopics(no origin) = %+v, want a settled empty result", got)
	}
}

// TestAccountLockHeldUnlockedFile reports the lock as held while this process
// holds it and free for an unlocked or missing lock file.
func TestAccountLockHeldUnlockedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "broker.lock")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("writing the lock file: %v", err)
	}
	if accountLockHeld(dir) {
		t.Fatal("accountLockHeld reported an unlocked file as held")
	}
	if accountLockHeld(filepath.Join(t.TempDir(), "missing")) {
		t.Fatal("accountLockHeld reported a missing lock file as held")
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("opening the lock file: %v", err)
	}
	defer func() { _ = f.Close() }()
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatalf("locking the file: %v", err)
	}
	if !accountLockHeld(dir) {
		t.Fatal("accountLockHeld reported a held lock as free")
	}
}

// TestRuntimeDirCandidatesRoot gives root only the system runtime directory.
func TestRuntimeDirCandidatesRoot(t *testing.T) {
	got := runtimeDirCandidates(0)
	want := []string{systemRuntimeDir(runtime.GOOS)}
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("runtimeDirCandidates(0) = %v, want %v", got, want)
	}
}

// TestRuntimeDirCandidatesKeepsPreferredAfterFallback selects a usable fallback
// first and keeps the occupied preferred directory after it.
func TestRuntimeDirCandidatesKeepsPreferredAfterFallback(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root has a single runtime directory")
	}
	preferred := filepath.Join(t.TempDir(), "preferred-file")
	if err := os.WriteFile(preferred, nil, 0o600); err != nil {
		t.Fatalf("writing the occupied preferred path: %v", err)
	}
	base := t.TempDir()
	if err := os.Chmod(base, 0o700); err != nil {
		t.Fatalf("chmod base: %v", err)
	}

	oldPreferred, oldBase := preferredRuntimeDir, fallbackRuntimeBase
	preferredRuntimeDir = func(int) (string, error) { return preferred, nil }
	fallbackRuntimeBase = func(int) (string, error) { return base, nil }
	t.Cleanup(func() { preferredRuntimeDir, fallbackRuntimeBase = oldPreferred, oldBase })

	if !fallbackBaseUsable(base, os.Geteuid()) {
		t.Fatal("fallbackBaseUsable rejected the fallback directory this test relies on")
	}
	got := runtimeDirCandidates(os.Geteuid())
	want := []string{filepath.Join(base, "peers"), preferred}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("runtimeDirCandidates = %v, want %v", got, want)
	}
}

// TestFallbackBaseUsableRejectsNonDirectory rejects a regular file and accepts a
// usable directory.
func TestFallbackBaseUsableRejectsNonDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("writing the non-directory: %v", err)
	}
	if fallbackBaseUsable(path, os.Geteuid()) {
		t.Fatal("fallbackBaseUsable accepted a regular file")
	}
	base := t.TempDir()
	if err := os.Chmod(base, 0o700); err != nil {
		t.Fatalf("chmod base: %v", err)
	}
	if !fallbackBaseUsable(base, os.Geteuid()) {
		t.Fatal("fallbackBaseUsable rejected a usable directory")
	}
}

// TestFallbackBaseUsableRejectsForeignOwner rejects a directory owned by another
// account and accepts the same directory when this account owns it.
func TestFallbackBaseUsableRejectsForeignOwner(t *testing.T) {
	base := t.TempDir()
	if err := os.Chmod(base, 0o700); err != nil {
		t.Fatalf("chmod base: %v", err)
	}
	if fallbackBaseUsable(base, os.Geteuid()+1) {
		t.Fatal("fallbackBaseUsable accepted a directory owned by another account")
	}
	if !fallbackBaseUsable(base, os.Geteuid()) {
		t.Fatal("fallbackBaseUsable rejected a directory owned by this account")
	}
}

// TestDarwinFallbackBaseRejectsEmptyOutput reports an empty getconf answer.
func TestDarwinFallbackBaseRejectsEmptyOutput(t *testing.T) {
	const euid = 987654
	fallbackBaseCache.mu.Lock()
	delete(fallbackBaseCache.dirs, euid)
	fallbackBaseCache.mu.Unlock()
	t.Cleanup(func() {
		fallbackBaseCache.mu.Lock()
		delete(fallbackBaseCache.dirs, euid)
		fallbackBaseCache.mu.Unlock()
	})

	old := getconfOutput
	getconfOutput = func(context.Context) ([]byte, error) { return []byte("  \n"), nil }
	t.Cleanup(func() { getconfOutput = old })

	_, err := darwinFallbackBase(euid)
	if err == nil || !strings.Contains(err.Error(), "empty per-user temporary directory") {
		t.Fatalf("darwinFallbackBase error = %v, want the empty-output failure", err)
	}
}

// TestProjectOwnerUIDMissingFile reports fs.ErrNotExist for a missing file and
// this account for a file it owns.
func TestProjectOwnerUIDMissingFile(t *testing.T) {
	if _, err := projectOwnerUID(filepath.Join(t.TempDir(), "missing")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("projectOwnerUID(missing) = %v, want fs.ErrNotExist", err)
	}
	path := filepath.Join(t.TempDir(), "present")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("writing the file: %v", err)
	}
	uid, err := projectOwnerUID(path)
	if err != nil {
		t.Fatalf("projectOwnerUID(present) = %v, want nil", err)
	}
	if uid != uint32(os.Geteuid()) {
		t.Fatalf("projectOwnerUID(present) = %d, want %d", uid, os.Geteuid())
	}
}

// TestCheckProjectConfigOwnerMissingFile reports the owner lookup failure for a
// missing file.
func TestCheckProjectConfigOwnerMissingFile(t *testing.T) {
	err := checkProjectConfigOwner(filepath.Join(t.TempDir(), "missing.yaml"))
	if err == nil || !strings.Contains(err.Error(), "cannot determine the owner") {
		t.Fatalf("checkProjectConfigOwner error = %v, want the owner lookup failure", err)
	}
}

// TestCheckProjectConfigOwnerDirectoryError reports the directory owner lookup
// failure after the file passes.
func TestCheckProjectConfigOwnerDirectoryError(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, ".peers.yaml")
	if err := os.WriteFile(file, []byte("topics: []\n"), 0o600); err != nil {
		t.Fatalf("writing the project file: %v", err)
	}

	old := projectOwnerUID
	projectOwnerUID = func(path string) (uint32, error) {
		if path == file {
			return uint32(os.Geteuid()), nil
		}
		return 0, errors.New("stat failed")
	}
	t.Cleanup(func() { projectOwnerUID = old })

	err := checkProjectConfigOwner(file)
	if err == nil || !strings.Contains(err.Error(), "cannot determine the owner of") {
		t.Fatalf("checkProjectConfigOwner error = %v, want the directory owner lookup failure", err)
	}
}

// TestAgentStatusNameNames names each agent status in words.
func TestAgentStatusNameNames(t *testing.T) {
	tests := []struct {
		name string
		in   localpb.AgentStatus
		want string
	}{
		{"idle", localpb.AgentStatus_AGENT_STATUS_IDLE, "idle"},
		{"busy", localpb.AgentStatus_AGENT_STATUS_BUSY, "busy"},
		{"waiting", localpb.AgentStatus_AGENT_STATUS_WAITING, "waiting"},
		{"shell", localpb.AgentStatus_AGENT_STATUS_SHELL, "shell"},
		{"unspecified", localpb.AgentStatus_AGENT_STATUS_UNSPECIFIED, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := agentStatusName(tt.in); got != tt.want {
				t.Fatalf("agentStatusName(%v) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
