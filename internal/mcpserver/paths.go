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

// preferredRuntimeDir returns the account's runtime directory before the
// fallback rule; the peers_testenv build replaces it, and production never does.
var preferredRuntimeDir = func(euid int) (string, error) {
	if euid == 0 {
		return systemRuntimeDir(runtime.GOOS), nil
	}
	return "/tmp/peers-" + strconv.Itoa(euid), nil
}

// fallbackRuntimeBase returns the per-account base directory whose "peers"
// child is the fallback runtime directory; tests replace it, and production
// never does.
var fallbackRuntimeBase = defaultFallbackRuntimeBase

// runtimeDir returns the runtime directory of the account euid; the
// peers_testenv hook redirects preferredRuntimeDir, so the fallback rule still
// applies under the test root.
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

func userSocketCandidates(euid int) []string {
	dirs := runtimeDirCandidates(euid)
	sockets := make([]string, 0, len(dirs))
	for _, dir := range dirs {
		sockets = append(sockets, socketPath(dir))
	}
	return sockets
}

func socketPath(dir string) string {
	return filepath.Join(dir, "broker.sock")
}

// runtimeDirOccupied reports whether another account has taken path. Only a
// symbolic link, a non-directory or a directory owned by another account counts,
// matching the rule in internal/config; a path that does not exist yet is not
// occupied.
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
// directory: a directory this account owns, with no group or other access, not
// a symbolic link.
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

// fallbackBaseProbeTimeout bounds the getconf call on macOS; tests shorten it.
var fallbackBaseProbeTimeout = 5 * time.Second

// getconfOutput runs getconf; tests replace it, and production never does.
var getconfOutput = func(ctx context.Context) ([]byte, error) {
	return exec.CommandContext(ctx, "/usr/bin/getconf", "DARWIN_USER_TEMP_DIR").Output()
}

// fallbackBaseCache caches a successful getconf result per process; a failure
// is not cached, so a transient failure can recover on the next lookup.
var fallbackBaseCache = struct {
	mu   sync.Mutex
	dirs map[int]string
}{dirs: make(map[int]string)}

func defaultFallbackRuntimeBase(euid int) (string, error) {
	if runtime.GOOS != "darwin" {
		return fmt.Sprintf("/run/user/%d", euid), nil
	}
	return darwinFallbackBase(euid)
}

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

func systemRuntimeDir(goos string) string {
	if goos == "darwin" {
		return "/var/run/peers"
	}
	return "/run/peers"
}

func userSocketPath(euid int) string {
	return socketPath(runtimeDir(euid))
}

func systemSocketPath() string {
	return socketPath(runtimeDir(0))
}
