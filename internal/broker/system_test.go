package broker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
)

var (
	_                                                 = config.ModeSystem
	_ func(string, func(old, new config.Value) error) = config.Validate
)

func socketPerms(t *testing.T, path string) (os.FileMode, uint32) {
	t.Helper()
	fi, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("Lstat %s: %v", path, err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("Lstat %s: no owner information", path)
	}
	return fi.Mode().Perm(), st.Gid
}

func currentGroup(t *testing.T) *user.Group {
	t.Helper()
	g, err := user.LookupGroupId(strconv.Itoa(os.Getgid()))
	if err != nil {
		t.Fatalf("LookupGroupId(%d): %v", os.Getgid(), err)
	}
	return g
}

// TestRunSystemNonRootStillNotRoot checks that a system run as a non-root
// account exits before loading the configuration.
func TestRunSystemNonRootStillNotRoot(t *testing.T) {
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.System = true
		rt.opts.Deps.Geteuid = func() int { return 1000 }
	})
	rt.start()

	err := rt.waitErr()
	if !errors.Is(err, ErrNotRoot) {
		t.Fatalf("Run error = %v, want ErrNotRoot", err)
	}
	if calls := rt.cfg.LoadCalls(); len(calls) != 0 {
		t.Fatalf("Load called %d times before the root check, want 0", len(calls))
	}
	if _, err := os.Stat(rt.socketPath()); !os.IsNotExist(err) {
		t.Errorf("broker.sock exists, want no socket (stat error %v)", err)
	}
}

// TestRunSystemLoadsSystemMode checks that the system form loads the
// configuration in system mode.
func TestRunSystemLoadsSystemMode(t *testing.T) {
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.System = true
		rt.opts.Deps.Geteuid = func() int { return 0 }
		rt.opts.Assemblers = []Assembler{&configWiring{}}
		rt.cfg.setValue("broker.local_socket_group", config.Value{Kind: config.KindString})
		rt.cfg.setValue("broker.local_socket_mode", config.Value{Kind: config.KindString, String: "0600"})
	})
	rt.start()

	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q, want ok", got)
	}
	calls := rt.cfg.LoadCalls()
	if len(calls) != 1 {
		t.Fatalf("Load called %d times, want 1", len(calls))
	}
	if calls[0].Mode != config.ModeSystem {
		t.Errorf("Load mode = %v, want config.ModeSystem", calls[0].Mode)
	}

	rt.cancel()
	if err := rt.waitErr(); err != nil {
		t.Fatalf("Run returned %v after cancellation, want nil", err)
	}
}

// TestRunSystemWithoutConfigWiringFails checks that the system form refuses to
// start without the config assembler.
func TestRunSystemWithoutConfigWiringFails(t *testing.T) {
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.System = true
		rt.opts.Deps.Geteuid = func() int { return 0 }
		rt.cfg.setValue("broker.local_socket_group", config.Value{Kind: config.KindString})
		rt.cfg.setValue("broker.local_socket_mode", config.Value{Kind: config.KindString, String: "0600"})
	})
	rt.start()

	if got := rt.readyLine(); got == "ok\n" {
		t.Fatal("readiness line is ok without the config assembler")
	}
	err := rt.waitErr()
	if err == nil {
		t.Fatal("Run succeeded without the config assembler")
	}
	if !strings.Contains(err.Error(), "config assembler") {
		t.Errorf("Run error = %q, want it to name the missing config assembler", err)
	}
	if _, err := os.Stat(rt.socketPath()); !os.IsNotExist(err) {
		t.Errorf("broker.sock exists, want no socket (stat error %v)", err)
	}
}

// TestRunSystemWithoutAccessFails checks that the system form refuses to start
// without an assembly access checker.
func TestRunSystemWithoutAccessFails(t *testing.T) {
	clearAccess := &fakeAssembler{name: "clear-access", fn: func(_ context.Context, _ ConfigSource, _ *Deps, asm *Assembly) ([]Component, error) {
		asm.Access = nil
		return nil, nil
	}}
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.System = true
		rt.opts.Deps.Geteuid = func() int { return 0 }
		rt.opts.Assemblers = []Assembler{&configWiring{}, clearAccess}
		rt.cfg.setValue("broker.local_socket_group", config.Value{Kind: config.KindString})
		rt.cfg.setValue("broker.local_socket_mode", config.Value{Kind: config.KindString, String: "0600"})
	})
	rt.start()

	if got := rt.readyLine(); got == "ok\n" {
		t.Fatal("readiness line is ok without an assembly access checker")
	}
	err := rt.waitErr()
	if err == nil {
		t.Fatal("Run succeeded without an assembly access checker")
	}
	if !strings.Contains(err.Error(), "access checker") {
		t.Errorf("Run error = %q, want it to name the missing access checker", err)
	}
}

// TestRunSystemRejectsReplacedAccess checks that the system form refuses to
// start when a later assembler replaces the access checker.
func TestRunSystemRejectsReplacedAccess(t *testing.T) {
	replaceAccess := &fakeAssembler{name: "replace-access", fn: func(_ context.Context, _ ConfigSource, _ *Deps, asm *Assembly) ([]Component, error) {
		asm.Access = &wiringChecker{granted: true}
		return nil, nil
	}}
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.System = true
		rt.opts.Deps.Geteuid = func() int { return 0 }
		rt.opts.Assemblers = []Assembler{&configWiring{}, replaceAccess}
		rt.cfg.setValue("broker.local_socket_group", config.Value{Kind: config.KindString})
		rt.cfg.setValue("broker.local_socket_mode", config.Value{Kind: config.KindString, String: "0600"})
	})
	rt.start()

	if got := rt.readyLine(); got == "ok\n" {
		t.Fatal("readiness line is ok after a later assembler replaced the checker")
	}
	err := rt.waitErr()
	if err == nil {
		t.Fatal("Run succeeded after a later assembler replaced the checker")
	}
	if !strings.Contains(err.Error(), "effective access checker") {
		t.Errorf("Run error = %q, want it to name the effective checker", err)
	}
}

// TestRunSystemAppliesSocketPerms checks that the system form applies the
// configured socket group and mode and follows a later mode change.
func TestRunSystemAppliesSocketPerms(t *testing.T) {
	group := currentGroup(t)
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.System = true
		rt.opts.Deps.Geteuid = func() int { return 0 }
		rt.opts.Assemblers = []Assembler{&configWiring{}}
		rt.cfg.setValue("broker.local_socket_group", config.Value{Kind: config.KindString, String: group.Name})
		rt.cfg.setValue("broker.local_socket_mode", config.Value{Kind: config.KindString, String: "0660"})
	})
	rt.start()

	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q, want ok", got)
	}
	mode, gid := socketPerms(t, rt.socketPath())
	if mode != 0o660 {
		t.Errorf("broker.sock mode = %04o, want 0660", mode)
	}
	if gid != uint32(os.Getgid()) {
		t.Errorf("broker.sock gid = %d, want %d", gid, os.Getgid())
	}

	rt.cfg.setValue("broker.local_socket_mode", config.Value{Kind: config.KindString, String: "0640"})
	waitForReg(t, "the socket mode to follow the configuration", func() bool {
		mode, _ := socketPerms(t, rt.socketPath())
		return mode == 0o640
	})

	rt.cancel()
	if err := rt.waitErr(); err != nil {
		t.Fatalf("Run returned %v after cancellation, want nil", err)
	}
}

// TestRunSystemUnknownGroupFails checks that the system form exits when
// broker.local_socket_group names a group that does not exist.
func TestRunSystemUnknownGroupFails(t *testing.T) {
	name := "peers-no-such-group-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.System = true
		rt.opts.Deps.Geteuid = func() int { return 0 }
		rt.opts.Assemblers = []Assembler{&configWiring{}}
		rt.cfg.setValue("broker.local_socket_group", config.Value{Kind: config.KindString, String: name})
		rt.cfg.setValue("broker.local_socket_mode", config.Value{Kind: config.KindString, String: "0660"})
	})
	rt.start()

	err := rt.waitErr()
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Run error = %v, want ErrInvalidConfig", err)
	}
	if !strings.Contains(err.Error(), "broker.local_socket_group") {
		t.Errorf("Run error = %q, want it to name broker.local_socket_group", err)
	}
	if _, err := os.Stat(rt.socketPath()); !os.IsNotExist(err) {
		t.Errorf("broker.sock exists, want no socket (stat error %v)", err)
	}
}

// TestApplyLocalSocketPermsGroupAndMode checks that an explicit group and mode
// are applied to the socket file.
func TestApplyLocalSocketPermsGroupAndMode(t *testing.T) {
	path := shortSocketDir(t, "broker.sock")
	dir := filepath.Dir(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen on %s: %v", path, err)
	}
	defer func() { _ = ln.Close() }()

	group := currentGroup(t)
	if err := applyLocalSocketPerms(path, dir, group.Name, "0660"); err != nil {
		t.Fatalf("applyLocalSocketPerms: %v", err)
	}
	mode, gid := socketPerms(t, path)
	if mode != 0o660 {
		t.Errorf("socket mode = %04o, want 0660", mode)
	}
	if gid != uint32(os.Getgid()) {
		t.Errorf("socket gid = %d, want %d", gid, os.Getgid())
	}
}

// TestApplyLocalSocketPermsDefaultsToRuntimeDirGroup checks that an empty group
// takes the runtime directory's group.
func TestApplyLocalSocketPermsDefaultsToRuntimeDirGroup(t *testing.T) {
	path := shortSocketDir(t, "broker.sock")
	dir := filepath.Dir(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen on %s: %v", path, err)
	}
	defer func() { _ = ln.Close() }()

	if err := applyLocalSocketPerms(path, dir, "", "0600"); err != nil {
		t.Fatalf("applyLocalSocketPerms: %v", err)
	}
	_, gid := socketPerms(t, path)
	dirInfo, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("Stat %s: %v", dir, err)
	}
	dirSt, ok := dirInfo.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("Stat %s: no owner information", dir)
	}
	if gid != dirSt.Gid {
		t.Errorf("socket gid = %d, want the runtime directory gid %d", gid, dirSt.Gid)
	}
}

// TestValidateSocketGroup checks that an empty or existing group is accepted and
// a missing one is refused.
func TestValidateSocketGroup(t *testing.T) {
	if err := validateSocketGroup(config.Value{}, config.Value{Kind: config.KindString}); err != nil {
		t.Errorf("validateSocketGroup(\"\") = %v, want nil", err)
	}
	group := currentGroup(t)
	if err := validateSocketGroup(config.Value{}, config.Value{Kind: config.KindString, String: group.Name}); err != nil {
		t.Errorf("validateSocketGroup(%q) = %v, want nil", group.Name, err)
	}
	if err := validateSocketGroup(config.Value{}, config.Value{Kind: config.KindString, String: "peers-no-such-group"}); err == nil {
		t.Fatal("validateSocketGroup accepted a group that does not exist")
	}
}

// TestSocketGroupTransientLookupFailureIsAFailedCheck checks that an account
// database failure is reported as a failed check, not a missing group.
func TestSocketGroupTransientLookupFailureIsAFailedCheck(t *testing.T) {
	old := lookupGroupFromDB
	t.Cleanup(func() { lookupGroupFromDB = old })
	lookupGroupFromDB = func(string) (uint32, error) {
		return 0, errors.New("account database unavailable")
	}

	err := validateSocketGroup(config.Value{}, config.Value{Kind: config.KindString, String: "peers-users"})
	if err == nil {
		t.Fatal("validateSocketGroup succeeded while the account database failed")
	}
	if strings.Contains(err.Error(), "does not exist") {
		t.Errorf("validateSocketGroup error = %q, want a failed check, not a missing group", err)
	}

	path := shortSocketDir(t, "broker.sock")
	ln, listenErr := net.Listen("unix", path)
	if listenErr != nil {
		t.Fatalf("listen on %s: %v", path, listenErr)
	}
	defer func() { _ = ln.Close() }()

	permErr := applyLocalSocketPerms(path, filepath.Dir(path), "peers-users", "0660")
	if permErr == nil {
		t.Fatal("applyLocalSocketPerms succeeded while the account database failed")
	}
	if strings.Contains(permErr.Error(), "does not exist") {
		t.Errorf("applyLocalSocketPerms error = %q, want a failed check, not a missing group", permErr)
	}
}

// TestWatchSocketPermsReappliesOnEitherKey checks that a change to either socket
// key reapplies the permissions.
func TestWatchSocketPermsReappliesOnEitherKey(t *testing.T) {
	cfg := newFakeConfig(t.TempDir())
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	calls := 0
	watchSocketPerms(cfg, func() error {
		calls++
		return nil
	}, logger)

	cfg.setValue("broker.local_socket_group", config.Value{Kind: config.KindString, String: "groupA"})
	cfg.setValue("broker.local_socket_mode", config.Value{Kind: config.KindString, String: "0640"})
	if calls != 2 {
		t.Fatalf("apply called %d times, want 2", calls)
	}

	failing := newFakeConfig(t.TempDir())
	watchSocketPerms(failing, func() error { return errors.New("cannot chmod") }, logger)
	failing.setValue("broker.local_socket_mode", config.Value{Kind: config.KindString, String: "0600"})
	if calls != 2 {
		t.Fatalf("apply called %d times through the working watcher, want 2", calls)
	}
}

// TestRunUserModeUnchanged checks that a user Broker keeps mode 0600 and
// registers no socket-group validator.
func TestRunUserModeUnchanged(t *testing.T) {
	rt := newRunTest(t)
	rt.start()

	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q, want ok", got)
	}
	mode, _ := socketPerms(t, rt.socketPath())
	if mode != 0o600 {
		t.Errorf("broker.sock mode = %04o, want 0600", mode)
	}
	calls := rt.cfg.LoadCalls()
	if len(calls) != 1 || calls[0].Mode != config.ModeUser {
		t.Fatalf("Load calls = %+v, want one config.ModeUser", calls)
	}
	rt.cfg.mu.Lock()
	_, hasGroupValidator := rt.cfg.validators["broker.local_socket_group"]
	rt.cfg.mu.Unlock()
	if hasGroupValidator {
		t.Error("user mode registered a broker.local_socket_group validator")
	}

	rt.cancel()
	if err := rt.waitErr(); err != nil {
		t.Fatalf("Run returned %v after cancellation, want nil", err)
	}
}
