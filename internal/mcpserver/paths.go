package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// preferredRuntimeDir returns the runtime directory of the account euid before
// the fallback rule. It is the seam the peers_testenv build replaces; production
// code never replaces it.
//
// The runtime directory does not depend on any environment variable. The
// account is taken by effective uid, matching the path table in
// design-config.md: the system Broker runs as root, every other account gets
// its own directory under /tmp.
var preferredRuntimeDir = func(euid int) (string, error) {
	if euid == 0 {
		return systemRuntimeDir(runtime.GOOS), nil
	}
	return "/tmp/peers-" + strconv.Itoa(euid), nil
}

// fallbackRuntimeBase returns the per-account base directory whose "peers"
// child is the fallback runtime directory. It is a seam tests replace;
// production code never replaces it.
var fallbackRuntimeBase = defaultFallbackRuntimeBase

// runtimeDir returns the runtime directory of the account euid. It is the seam
// the peers_testenv build no longer replaces; the hook redirects
// preferredRuntimeDir instead, so the fallback rule still applies under the test
// root.
var runtimeDir = func(euid int) string {
	preferred, err := preferredRuntimeDir(euid)
	if err != nil || euid == 0 {
		return preferred
	}
	if !runtimeDirOccupied(preferred, euid) {
		return preferred
	}
	if base, err := fallbackRuntimeBase(euid); err == nil && fallbackBaseUsable(base, euid) {
		return filepath.Join(base, "peers")
	}
	return preferred
}

// runtimeDirCandidates returns the runtime directories this account's Broker may
// run in: the one the location rule selects first, then the other candidate.
// The list depends only on the account and the platform paths, so selection can
// probe both even while the preferred directory's state changes under it.
func runtimeDirCandidates(euid int) []string {
	paths := []string{runtimeDir(euid)}
	if euid == 0 {
		return paths
	}
	if preferred, err := preferredRuntimeDir(euid); err == nil && !slices.Contains(paths, preferred) {
		paths = append(paths, preferred)
	}
	if base, err := fallbackRuntimeBase(euid); err == nil {
		if fallback := filepath.Join(base, "peers"); !slices.Contains(paths, fallback) {
			paths = append(paths, fallback)
		}
	}
	return paths
}

// userSocketCandidates returns the local socket of every runtime directory the
// account's Broker may use.
func userSocketCandidates(euid int) []string {
	dirs := runtimeDirCandidates(euid)
	sockets := make([]string, 0, len(dirs))
	for _, dir := range dirs {
		sockets = append(sockets, socketPath(dir))
	}
	return sockets
}

// socketPath returns the local Broker socket inside dir.
func socketPath(dir string) string {
	return filepath.Join(dir, "broker.sock")
}

// runtimeDirOccupied reports whether another account has taken path. A path
// that does not exist yet is not occupied. Only a symbolic link, a
// non-directory or a directory owned by another account counts, matching the
// rule in internal/config.
func runtimeDirOccupied(path string, euid int) bool {
	info, err := os.Lstat(path)
	if err != nil {
		return false
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return true
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	return ok && int(st.Uid) != euid
}

// fallbackBaseUsable reports whether base may hold the fallback runtime
// directory: it must be a directory this account owns with no group or other
// access, and it must not be a symbolic link.
func fallbackBaseUsable(base string, euid int) bool {
	info, err := os.Lstat(base)
	if err != nil {
		return false
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return false
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(st.Uid) != euid {
		return false
	}
	return info.Mode().Perm()&0o077 == 0
}

// fallbackBaseProbeTimeout bounds the getconf call on macOS, so a stalled
// getconf cannot delay every runtime-directory lookup. It is a variable so
// tests can shorten it.
var fallbackBaseProbeTimeout = 5 * time.Second

// getconfOutput runs getconf. It is a seam tests replace; production code never
// replaces it.
var getconfOutput = func(ctx context.Context) ([]byte, error) {
	return exec.CommandContext(ctx, "/usr/bin/getconf", "DARWIN_USER_TEMP_DIR").Output()
}

// fallbackBaseCache keeps a successful getconf result for the life of the
// process. The runtime directory is resolved on every lookup, and asking
// getconf each time would spawn a process per call. A failure is not cached, so
// a transient failure can recover on the next lookup.
var fallbackBaseCache = struct {
	mu   sync.Mutex
	dirs map[int]string
}{dirs: make(map[int]string)}

// defaultFallbackRuntimeBase names the platform's per-account base directory:
// the logind runtime directory on Linux, and the per-user temporary directory
// that getconf reports on macOS.
func defaultFallbackRuntimeBase(euid int) (string, error) {
	if runtime.GOOS != "darwin" {
		return fmt.Sprintf("/run/user/%d", euid), nil
	}
	return darwinFallbackBase(euid)
}

// darwinFallbackBase asks getconf once per process and account, with a bounded
// wait.
func darwinFallbackBase(euid int) (string, error) {
	fallbackBaseCache.mu.Lock()
	if dir, ok := fallbackBaseCache.dirs[euid]; ok {
		fallbackBaseCache.mu.Unlock()
		return dir, nil
	}
	fallbackBaseCache.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), fallbackBaseProbeTimeout)
	defer cancel()
	out, err := getconfOutput(ctx)
	if err != nil {
		return "", fmt.Errorf("cannot ask getconf for the per-user temporary directory: %w", err)
	}
	dir := strings.TrimSpace(string(out))
	if dir == "" {
		return "", errors.New("getconf returned an empty per-user temporary directory")
	}
	fallbackBaseCache.mu.Lock()
	fallbackBaseCache.dirs[euid] = dir
	fallbackBaseCache.mu.Unlock()
	return dir, nil
}

// systemRuntimeDir returns the runtime directory of the root account on goos.
// The platform suffix is passed in rather than read from runtime.GOOS so the
// rule is testable on every host.
func systemRuntimeDir(goos string) string {
	if goos == "darwin" {
		return "/var/run/peers"
	}
	return "/run/peers"
}

// userSocketPath is the local socket of the Broker that runs as the account euid.
func userSocketPath(euid int) string {
	return socketPath(runtimeDir(euid))
}

// systemSocketPath is the local socket of the system Broker, which runs as root.
func systemSocketPath() string {
	return socketPath(runtimeDir(0))
}
