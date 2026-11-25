package config

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func (c *core) resolvePaths() (PathSet, error) {
	euid := c.euid()
	choice, err := chooseRuntimeDir(euid)
	if err != nil {
		return PathSet{}, err
	}
	runtimeDir := choice.path
	switch c.mode {
	case ModeUser:
		home, err := c.homeDir()
		if err != nil {
			return PathSet{}, fmt.Errorf("cannot determine the home directory: %w", err)
		}
		if home == "" {
			return PathSet{}, errors.New("the account has no home directory")
		}
		dataDir := filepath.Join(home, ".peers")
		return PathSet{
			RuntimeDir:         runtimeDir,
			RuntimeDirFallback: choice.fallback,
			ConfigFile:         filepath.Join(dataDir, "config.yaml"),
			DataDir:            dataDir,
			AuditLog:           filepath.Join(dataDir, "audit.log"),
		}, nil
	case ModeSystem:
		if c.systemPaths != nil {
			return c.systemPaths(), nil
		}
		return PathSet{
			RuntimeDir:         runtimeDir,
			RuntimeDirFallback: choice.fallback,
			ConfigFile:         "/etc/peers/config.yaml",
			DataDir:            "/var/lib/peers",
			AuditLog:           "/var/log/peers/audit.log",
		}, nil
	default:
		return PathSet{}, fmt.Errorf("unknown deployment mode %d", c.mode)
	}
}

// runtimeRoot is a seam tests replace to place the runtime directory under a
// temporary root; production code never replaces it.
var runtimeRoot = func(euid int) string {
	if euid == 0 {
		if runtime.GOOS == "darwin" {
			return "/var/run/peers"
		}
		return "/run/peers"
	}
	return fmt.Sprintf("/tmp/peers-%d", euid)
}

// runtimeFallbackBase is a seam tests replace; production code never replaces
// it.
var runtimeFallbackBase = defaultRuntimeFallbackBase

// fallbackBaseProbeTimeout bounds the getconf call so a stalled getconf cannot
// delay every runtime-directory lookup; tests shorten it.
var fallbackBaseProbeTimeout = 5 * time.Second

// getconfOutput is a seam tests replace; production code never replaces it.
var getconfOutput = func(ctx context.Context) ([]byte, error) {
	return exec.CommandContext(ctx, "/usr/bin/getconf", "DARWIN_USER_TEMP_DIR").Output()
}

// fallbackBaseCache caches a successful getconf result per account; a failure
// is not cached so a transient failure can recover on the next lookup.
var fallbackBaseCache = struct {
	mu   sync.Mutex
	dirs map[int]string
}{dirs: make(map[int]string)}

func defaultRuntimeFallbackBase(euid int) (string, error) {
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

type runtimeDirOccupant struct {
	taken  RuntimeDirFallback
	reason string
}

// dirOwnerUID is a seam tests replace to simulate an entry owned by another
// account without needing root; production reads the uid from the stat data.
var dirOwnerUID = func(info os.FileInfo) (int, bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return int(st.Uid), true
}

func occupiedRuntimeDir(path string, euid int) (runtimeDirOccupant, bool) {
	info, err := os.Lstat(path)
	if err != nil {
		return runtimeDirOccupant{}, false
	}
	taken := RuntimeDirFallback{TakenPath: path, OwnerUID: -1}
	if info.Mode()&os.ModeSymlink != 0 {
		return runtimeDirOccupant{taken: taken, reason: "it is a symbolic link"}, true
	}
	if !info.IsDir() {
		return runtimeDirOccupant{taken: taken, reason: "it is not a directory"}, true
	}
	owner, ok := dirOwnerUID(info)
	if !ok {
		return runtimeDirOccupant{taken: taken, reason: "it is not owned by this account"}, true
	}
	if owner != euid {
		taken.OwnerUID = owner
		return runtimeDirOccupant{taken: taken, reason: fmt.Sprintf("it is owned by uid %d, not %d", owner, euid)}, true
	}
	return runtimeDirOccupant{}, false
}

func usableFallbackBase(base string, euid int) bool {
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

type runtimeDirChoice struct {
	path     string
	fallback *RuntimeDirFallback
}

func chooseRuntimeDir(euid int) (runtimeDirChoice, error) {
	preferred := runtimeRoot(euid)
	if euid == 0 {
		return runtimeDirChoice{path: preferred}, nil
	}
	occ, occupied := occupiedRuntimeDir(preferred, euid)
	if !occupied {
		return runtimeDirChoice{path: preferred}, nil
	}
	if base, err := runtimeFallbackBase(euid); err == nil && usableFallbackBase(base, euid) {
		taken := occ.taken
		return runtimeDirChoice{path: filepath.Join(base, "peers"), fallback: &taken}, nil
	}
	return runtimeDirChoice{path: preferred}, fmt.Errorf("cannot use %s: %s, and no other runtime directory is available", preferred, occ.reason)
}

func runtimeDirPath(euid int) string {
	choice, _ := chooseRuntimeDir(euid)
	return choice.path
}

func accountLockPath(euid int) string {
	return filepath.Join(runtimeDirPath(euid), "broker.lock")
}

func runtimeDirCandidates(euid int) []string {
	preferred := runtimeRoot(euid)
	if euid == 0 {
		return []string{preferred}
	}
	paths := []string{preferred}
	if base, err := runtimeFallbackBase(euid); err == nil {
		if fallback := filepath.Join(base, "peers"); fallback != preferred {
			paths = append(paths, fallback)
		}
	}
	return paths
}

// AccountLockCandidates returns the account lock path at every runtime
// directory this account's Broker may use, so a running instance at either
// location keeps a second one from starting.
func AccountLockCandidates() []string {
	dirs := runtimeDirCandidates(os.Geteuid())
	paths := make([]string, 0, len(dirs))
	for _, dir := range dirs {
		paths = append(paths, filepath.Join(dir, "broker.lock"))
	}
	return paths
}

func ensureRuntimeDir(euid int) (string, error) {
	choice, err := chooseRuntimeDir(euid)
	if err != nil {
		return "", err
	}
	mode := os.FileMode(0o700)
	if euid == 0 {
		mode = 0o755
	}
	if err := ensureDir(choice.path, mode, euid, permRuntime); err != nil {
		// The preferred directory may have been taken between resolution and
		// creation, so re-resolve once to use the fallback instead of failing
		// startup.
		var mismatch *dirOwnerMismatchError
		if choice.fallback == nil && errors.As(err, &mismatch) {
			if next, nextErr := chooseRuntimeDir(euid); nextErr == nil && next.path != choice.path {
				if ensureErr := ensureDir(next.path, mode, euid, permRuntime); ensureErr == nil {
					return next.path, nil
				}
			}
		}
		return "", err
	}
	return choice.path, nil
}

type permRule int

const (
	permRuntime permRule = iota
	permData
	permShared
)

func ensureDir(path string, mode os.FileMode, euid int, rule permRule) error {
	if err := os.MkdirAll(path, mode); err != nil {
		return fmt.Errorf("cannot create %s: %w", path, err)
	}
	if err := validateDir(path, euid, rule); err != nil {
		return err
	}
	return nil
}

type dirOwnerMismatchError struct {
	path  string
	owner int
	want  int
}

func (e *dirOwnerMismatchError) Error() string {
	return fmt.Sprintf("cannot use %s: it is owned by uid %d, not %d", e.path, e.owner, e.want)
}

func validateDir(path string, euid int, rule permRule) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("cannot inspect %s: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("cannot use %s: it is a symbolic link", path)
	}
	if !info.IsDir() {
		return fmt.Errorf("cannot use %s: it is not a directory", path)
	}
	f, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("cannot open %s: %w", path, err)
	}
	file := os.NewFile(uintptr(f), path)
	st, err := file.Stat()
	_ = file.Close()
	if err != nil {
		return fmt.Errorf("cannot inspect %s: %w", path, err)
	}
	if owner, ok := dirOwnerUID(st); ok && owner != euid {
		return &dirOwnerMismatchError{path: path, owner: owner, want: euid}
	}
	if err := checkDirPermission(st.Mode().Perm(), euid, rule); err != nil {
		return fmt.Errorf("cannot use %s: %w", path, err)
	}
	return nil
}

func checkDirPermission(perm os.FileMode, euid int, rule permRule) error {
	switch rule {
	case permRuntime:
		if euid == 0 {
			if perm&0o022 != 0 {
				return errors.New("must not be group or world writable")
			}
		} else if perm&0o077 != 0 {
			return errors.New("must not be accessible by group or other")
		}
	case permData:
		if perm&0o077 != 0 {
			return errors.New("must not be accessible by group or other")
		}
	case permShared:
		if perm&0o022 != 0 {
			return errors.New("must not be group or world writable")
		}
	}
	return nil
}

func (c *core) prepareDataDirs(paths PathSet) error {
	euid := c.euid()
	switch c.mode {
	case ModeUser:
		if err := ensureDir(paths.DataDir, 0o700, euid, permData); err != nil {
			return err
		}
	case ModeSystem:
		if euid != 0 {
			return errors.New("a system Broker must run as root")
		}
		if err := ensureDir(filepath.Dir(paths.ConfigFile), 0o755, euid, permShared); err != nil {
			return err
		}
		if err := ensureDir(paths.DataDir, 0o700, euid, permData); err != nil {
			return err
		}
		if err := ensureDir(filepath.Dir(paths.AuditLog), 0o755, euid, permShared); err != nil {
			return err
		}
	}
	return nil
}

var accountInit struct {
	mu  sync.Mutex
	err error
}

func takeAccountInitErr() error {
	accountInit.mu.Lock()
	defer accountInit.mu.Unlock()
	err := accountInit.err
	accountInit.err = nil
	return err
}

func setAccountInitErr(err error) {
	accountInit.mu.Lock()
	defer accountInit.mu.Unlock()
	accountInit.err = err
}

// RuntimeDir returns the runtime directory of the account euid, applying the
// fallback rule.
func RuntimeDir(euid int) string {
	return runtimeDirPath(euid)
}

// CheckRuntimeDir creates and checks the runtime directory's owner and
// permission, before the account lock is taken.
func CheckRuntimeDir() error {
	return checkRuntimeDir(os.Geteuid())
}

func checkRuntimeDir(euid int) error {
	_, err := ensureRuntimeDir(euid)
	return err
}

// AccountLock returns the path of the account lock. It attempts to create and
// check the runtime directory, remembering a failure for the next Load.
func AccountLock() string {
	euid := os.Geteuid()
	if _, err := ensureRuntimeDir(euid); err != nil {
		setAccountInitErr(err)
	}
	return accountLockPath(euid)
}
