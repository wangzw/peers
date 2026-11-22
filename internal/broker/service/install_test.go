package service

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
)

// baseOptions returns user Linux options rooted in a temporary directory.
// Probe reports no Broker and ReadyTimeout keeps the readiness wait short; a
// test that exercises the start overrides both.
func baseOptions(t *testing.T) Options {
	t.Helper()
	return Options{
		GOOS:     "linux",
		EUID:     1000,
		UID:      1000,
		Exe:      "/usr/local/bin/peers",
		Home:     "/home/alice",
		TempDir:  "/tmp",
		Root:     t.TempDir(),
		LookPath: func(file string) (string, error) { return filepath.Join("/bin", file), nil },
		Run:      func(string, ...string) ([]byte, error) { return nil, nil },
		// The administration group already exists with a fixed id, so tests that
		// do not exercise group creation emit no group commands.
		GroupGID: func(string) (int, bool, error) { return 999, true, nil },
		// The group lists no members, so reusing it needs no confirmation. A test
		// that exercises the member lookup sets this to nil and drives Run.
		GroupMembers: func(string) ([]string, error) { return nil, nil },
		// No account has the group as its primary group, so the member lookup
		// needs no account list. A test that exercises it replaces this.
		PrimaryGroupAccounts: func(int) ([]string, error) { return nil, nil },
		Probe:                func(string) (int, bool) { return 0, false },
		// Most tests are not about degraded running; report a healthy Broker so
		// the status check after readiness passes.
		StatusProbe: func(string) (DegradedState, bool, error) { return DegradedState{}, false, nil },
		// Most tests do not exercise the listen precheck; report every address
		// free so they never bind the template's real port. The precheck tests
		// replace this with config.ProbeListen or a stand-in.
		ListenProbe: func(string) error { return nil },
		Owner:       fakeOwner(nil),
		EvalSymlinks: func(path string) (string, error) {
			return path, nil
		},
		ReadyTimeout: 20 * time.Millisecond,
	}
}

type fakeOwnerEntry struct {
	uid  uint32
	mode fs.FileMode
}

func fakeOwner(overrides map[string]fakeOwnerEntry) func(string) (uint32, fs.FileMode, error) {
	return func(path string) (uint32, fs.FileMode, error) {
		if entry, ok := overrides[path]; ok {
			return entry.uid, entry.mode, nil
		}
		return 0, 0o755, nil
	}
}

func recordCommands(cmds *[]string) func(string, ...string) ([]byte, error) {
	return func(name string, args ...string) ([]byte, error) {
		*cmds = append(*cmds, strings.Join(append([]string{name}, args...), " "))
		return nil, nil
	}
}

// recordCommandsUntil records every command and sets started when trigger
// appears in the arguments, standing in for the service manager starting the
// Broker.
func recordCommandsUntil(cmds *[]string, started *bool, trigger string) func(string, ...string) ([]byte, error) {
	return func(name string, args ...string) ([]byte, error) {
		*cmds = append(*cmds, strings.Join(append([]string{name}, args...), " "))
		if slices.Contains(args, "show") {
			return []byte("4242\n"), nil
		}
		if slices.Contains(args, trigger) {
			*started = true
		}
		return nil, nil
	}
}

// probeAfterStart reports no Broker before the service starts and the given pid
// once it has.
func probeAfterStart(started *bool, pid int) func(string) (int, bool) {
	return func(string) (int, bool) {
		if *started {
			return pid, true
		}
		return 0, false
	}
}

// exitError returns the error of a process that exited with code, so the
// launchctl exit-code handling is exercised by a real *exec.ExitError.
func exitError(t *testing.T, code int) error {
	t.Helper()
	err := exec.Command("sh", "-c", fmt.Sprintf("exit %d", code)).Run()
	if err == nil {
		t.Fatalf("sh -c 'exit %d' succeeded", code)
	}
	return err
}

func mustReadFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func assertMode(t *testing.T, path string, want fs.FileMode) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if got := fi.Mode().Perm(); got != want {
		t.Errorf("%s mode = %#o, want %#o", path, got, want)
	}
}

func assertOwner(t *testing.T, path string, want uint32) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("%s has no owner information", path)
	}
	if got := st.Uid; got != want {
		t.Errorf("%s owner = %d, want %d", path, got, want)
	}
}

func assertNoEntries(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir %s: %v", dir, err)
	}
	if len(entries) != 0 {
		names := make([]string, len(entries))
		for i, entry := range entries {
			names[i] = entry.Name()
		}
		t.Errorf("%s is not empty: %v", dir, names)
	}
}

func TestInstallLinuxUser(t *testing.T) {
	opts := baseOptions(t)
	opts.XDGConfigHome = ""
	var cmds []string
	started := false
	opts.Run = recordCommandsUntil(&cmds, &started, "restart")
	opts.Probe = probeAfterStart(&started, 4242)

	res, err := Install(opts)
	if err != nil {
		t.Fatalf("Install: %v", err)
	}

	path := filepath.Join(opts.Root, "home/alice/.config/systemd/user/peers.service")
	want, err := SystemdUserUnit(opts.Exe)
	if err != nil {
		t.Fatal(err)
	}
	if got := mustReadFile(t, path); got != want {
		t.Errorf("unit content mismatch:\n got %q\nwant %q", got, want)
	}
	assertMode(t, path, 0o644)

	wantCmds := []string{
		"systemctl --user daemon-reload",
		"systemctl --user enable peers.service",
		"systemctl --user restart peers.service",
		"systemctl --user show --property MainPID --value peers.service",
	}
	if !slices.Equal(cmds, wantCmds) {
		t.Errorf("commands = %q, want %q", cmds, wantCmds)
	}
	if !slices.Equal(res.Commands, wantCmds[:3]) {
		t.Errorf("Result.Commands = %q, want %q", res.Commands, wantCmds[:3])
	}
	if res.UnitPath != "/home/alice/.config/systemd/user/peers.service" {
		t.Errorf("UnitPath = %q", res.UnitPath)
	}
	if res.Domain != "" {
		t.Errorf("Domain = %q, want empty on Linux", res.Domain)
	}
	configPath := filepath.Join(opts.Root, "home/alice/.peers/config.yaml")
	if got, want := mustReadFile(t, configPath), string(config.Template(config.ModeUser)); got != want {
		t.Errorf("config content = %q, want the user template", got)
	}
	assertMode(t, configPath, 0o600)
	assertOwner(t, configPath, uint32(os.Geteuid()))
	assertMode(t, filepath.Dir(configPath), 0o700)
	if !res.ConfigNew {
		t.Error("ConfigNew = false, want true")
	}
	if res.ConfigPath != "/home/alice/.peers/config.yaml" {
		t.Errorf("ConfigPath = %q", res.ConfigPath)
	}
	if want := "/home/alice/.peers/audit.log"; res.AuditLog != want {
		t.Errorf("AuditLog = %q, want %q", res.AuditLog, want)
	}
}

func TestInstallLinuxUserHonoursXDGConfigHome(t *testing.T) {
	install := func(t *testing.T, opts Options) {
		t.Helper()
		var cmds []string
		started := false
		opts.Run = recordCommandsUntil(&cmds, &started, "restart")
		opts.Probe = probeAfterStart(&started, 4242)
		if _, err := Install(opts); err != nil {
			t.Fatalf("Install: %v", err)
		}
	}

	t.Run("absolute", func(t *testing.T) {
		opts := baseOptions(t)
		opts.XDGConfigHome = "/x/cfg"
		install(t, opts)
		path := filepath.Join(opts.Root, "x/cfg/systemd/user/peers.service")
		if _, err := os.Stat(path); err != nil {
			t.Errorf("unit not written under XDG_CONFIG_HOME: %v", err)
		}
	})

	t.Run("relative falls back", func(t *testing.T) {
		opts := baseOptions(t)
		opts.XDGConfigHome = "rel"
		install(t, opts)
		path := filepath.Join(opts.Root, "home/alice/.config/systemd/user/peers.service")
		if _, err := os.Stat(path); err != nil {
			t.Errorf("unit not written under Home/.config: %v", err)
		}
	})
}

func TestInstallLinuxSystem(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	var cmds []string
	started := false
	opts.Run = recordCommandsUntil(&cmds, &started, "restart")
	opts.Probe = probeAfterStart(&started, 4242)

	res, err := Install(opts)
	if err != nil {
		t.Fatalf("Install: %v", err)
	}

	unitPath := filepath.Join(opts.Root, "etc/systemd/system/peers.service")
	want, err := SystemdSystemUnit(opts.Exe)
	if err != nil {
		t.Fatal(err)
	}
	if got := mustReadFile(t, unitPath); got != want {
		t.Errorf("unit content mismatch:\n got %q\nwant %q", got, want)
	}

	configPath := filepath.Join(opts.Root, "etc/peers/config.yaml")
	if got := mustReadFile(t, configPath); got != string(config.Template(config.ModeSystem)) {
		t.Errorf("config content = %q, want the system template", got)
	}
	assertMode(t, configPath, 0o660)
	if !res.ConfigNew {
		t.Error("ConfigNew = false, want true")
	}
	if res.ConfigPath != "/etc/peers/config.yaml" {
		t.Errorf("ConfigPath = %q", res.ConfigPath)
	}
	if want := "/var/log/peers/audit.log"; res.AuditLog != want {
		t.Errorf("AuditLog = %q, want %q", res.AuditLog, want)
	}
	if res.AdminGroup != DefaultAdminGroup {
		t.Errorf("AdminGroup = %q, want %q", res.AdminGroup, DefaultAdminGroup)
	}

	wantCmds := []string{
		"systemctl daemon-reload",
		"systemctl enable peers.service",
		"systemctl restart peers.service",
		"systemctl show --property MainPID --value peers.service",
	}
	if !slices.Equal(cmds, wantCmds) {
		t.Errorf("commands = %q, want %q", cmds, wantCmds)
	}
}

func TestInstallLinuxStepFailures(t *testing.T) {
	cases := []struct {
		name    string
		trigger string
		failing string
		want    []string
	}{
		{
			name:    "daemon-reload",
			trigger: "daemon-reload",
			failing: "systemctl --user daemon-reload",
			want: []string{
				"systemctl --user daemon-reload",
				"systemctl --user disable --now peers.service",
			},
		},
		{
			name:    "enable",
			trigger: "enable",
			failing: "systemctl --user enable peers.service",
			want: []string{
				"systemctl --user daemon-reload",
				"systemctl --user enable peers.service",
				"systemctl --user disable --now peers.service",
			},
		},
		{
			name:    "restart",
			trigger: "restart",
			failing: "systemctl --user restart peers.service",
			want: []string{
				"systemctl --user daemon-reload",
				"systemctl --user enable peers.service",
				"systemctl --user restart peers.service",
				"systemctl --user disable --now peers.service",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := baseOptions(t)
			var cmds []string
			opts.Run = func(name string, args ...string) ([]byte, error) {
				cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
				if slices.Contains(args, tc.trigger) {
					return []byte("failed step"), errors.New("exit status 1")
				}
				return nil, nil
			}

			res, err := Install(opts)
			if err == nil {
				t.Fatalf("Install error = nil, want the %s failure", tc.trigger)
			}
			// The error must name the reason, not only that a step failed: the
			// command that ran, its exit status and the output it printed.
			for _, want := range []string{tc.failing, "exit status 1", "failed step"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("Install error = %q, want it to contain %q", err, want)
				}
			}
			if !slices.Equal(cmds, tc.want) {
				t.Errorf("commands = %q, want %q", cmds, tc.want)
			}
			if res.UnitPath == "" {
				t.Error("Result.UnitPath is empty after a failed start")
			}
		})
	}
}

func TestInstallKeepsExistingSystemConfig(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	var cmds []string
	started := false
	opts.Run = recordCommandsUntil(&cmds, &started, "restart")
	opts.Probe = probeAfterStart(&started, 4242)
	configPath := filepath.Join(opts.Root, "etc/peers/config.yaml")
	if err := os.MkdirAll(filepath.Dir(configPath), 0o755); err != nil {
		t.Fatal(err)
	}
	const existing = "broker:\n  alias: x\n"
	if err := os.WriteFile(configPath, []byte(existing), 0o600); err != nil {
		t.Fatal(err)
	}
	// A group and mode the new installation must not touch.
	if err := os.Chmod(configPath, 0o640); err != nil {
		t.Fatal(err)
	}
	opts.Chown = func(string, int, int) error {
		t.Error("Install changed the owner of an existing configuration file")
		return nil
	}
	opts.Chmod = func(string, fs.FileMode) error {
		t.Error("Install changed the mode of an existing configuration file")
		return nil
	}

	res, err := Install(opts)
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	if got := mustReadFile(t, configPath); got != existing {
		t.Errorf("config was overwritten: %q", got)
	}
	assertMode(t, configPath, 0o640)
	if res.ConfigNew {
		t.Error("ConfigNew = true, want false")
	}
	if res.ConfigPath != "/etc/peers/config.yaml" {
		t.Errorf("ConfigPath = %q", res.ConfigPath)
	}
	if res.AdminGroup != DefaultAdminGroup {
		t.Errorf("AdminGroup = %q, want %q", res.AdminGroup, DefaultAdminGroup)
	}
}

// systemInstallWithGroupRun builds a Linux system install whose command runner
// answers the group database and the service manager. created reports whether
// ensureAdminGroup's groupadd has run; gid is the id getent reports once it has.
func systemInstallWithGroupRun(t *testing.T, opts *Options, gid int) (cmds *[]string, created *bool) {
	t.Helper()
	cmds = &[]string{}
	created = new(bool)
	started := false
	opts.Run = func(name string, args ...string) ([]byte, error) {
		*cmds = append(*cmds, strings.Join(append([]string{name}, args...), " "))
		switch {
		case name == "getent" && !*created:
			return []byte(""), exitError(t, 2)
		case name == "getent":
			return []byte(fmt.Sprintf("%s:x:%d:\n", args[1], gid)), nil
		case name == "groupadd":
			*created = true
			return nil, nil
		case slices.Contains(args, "show"):
			return []byte("4242\n"), nil
		case slices.Contains(args, "restart"):
			started = true
		}
		return nil, nil
	}
	opts.Probe = probeAfterStart(&started, 4242)
	return cmds, created
}

func TestInstallSystemCreatesAdminGroup(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	opts.GroupGID = nil // exercise the platform lookup through the command runner
	const gid = 999
	cmds, created := systemInstallWithGroupRun(t, &opts, gid)
	var chowns []string
	var chmods []string
	opts.Chown = func(path string, uid, gid int) error {
		chowns = append(chowns, fmt.Sprintf("%s %d:%d", path, uid, gid))
		return nil
	}
	opts.Chmod = func(path string, mode fs.FileMode) error {
		chmods = append(chmods, fmt.Sprintf("%s %#o", path, mode))
		return os.Chmod(path, mode)
	}

	res, err := Install(opts)
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	if !*created {
		t.Error("the administration group was not created")
	}
	wantCmds := []string{
		"getent group peers",
		"groupadd --system peers",
		"getent group peers",
		"systemctl daemon-reload",
		"systemctl enable peers.service",
		"systemctl restart peers.service",
		"systemctl show --property MainPID --value peers.service",
	}
	if !slices.Equal(*cmds, wantCmds) {
		t.Errorf("commands = %q, want %q", *cmds, wantCmds)
	}
	if want := "groupadd --system peers"; !slices.Contains(res.Commands, want) {
		t.Errorf("Result.Commands = %q, want %q", res.Commands, want)
	}
	if res.AdminGroup != DefaultAdminGroup {
		t.Errorf("AdminGroup = %q, want %q", res.AdminGroup, DefaultAdminGroup)
	}

	configPath := filepath.Join(opts.Root, "etc/peers/config.yaml")
	want := fmt.Sprintf("%s 0:%d", configPath, gid)
	if !slices.Equal(chowns, []string{want}) {
		t.Errorf("chown calls = %q, want %q", chowns, want)
	}
	wantChmod := fmt.Sprintf("%s %#o", configPath, 0o660)
	if !slices.Equal(chmods, []string{wantChmod}) {
		t.Errorf("chmod calls = %q, want %q", chmods, wantChmod)
	}
	assertMode(t, configPath, 0o660)
}

func TestInstallSystemRecordsACreatedGroup(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	opts.GroupGID = nil
	systemInstallWithGroupRun(t, &opts, 999)

	res, err := Install(opts)
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	if !res.AdminGroupCreated {
		t.Error("AdminGroupCreated = false, want true")
	}
	record := filepath.Join(opts.Root, adminGroupRecordPath)
	data, err := os.ReadFile(record)
	if err != nil {
		t.Fatalf("reading the group record %s: %v", record, err)
	}
	if got := strings.TrimSpace(string(data)); got != DefaultAdminGroup {
		t.Errorf("record = %q, want %q", got, DefaultAdminGroup)
	}
}

func TestInstallSystemDoesNotRecordAnExistingGroup(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	opts.GroupGID = func(string) (int, bool, error) { return 777, true, nil }
	var cmds []string
	started := false
	opts.Run = recordCommandsUntil(&cmds, &started, "restart")
	opts.Probe = probeAfterStart(&started, 4242)

	res, err := Install(opts)
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	if res.AdminGroupCreated {
		t.Error("AdminGroupCreated = true for a group that already existed")
	}
	if _, err := os.Stat(filepath.Join(opts.Root, adminGroupRecordPath)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a group that already existed was recorded: %v", err)
	}
}

func TestInstallSystemUsesAnExistingGroup(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	const gid = 777
	opts.GroupGID = func(name string) (int, bool, error) {
		if name != DefaultAdminGroup {
			t.Errorf("looked up group %q, want %q", name, DefaultAdminGroup)
		}
		return gid, true, nil
	}
	var cmds []string
	started := false
	opts.Run = recordCommandsUntil(&cmds, &started, "restart")
	opts.Probe = probeAfterStart(&started, 4242)
	var gotGID int
	opts.Chown = func(_ string, uid, gid int) error {
		if uid != 0 {
			t.Errorf("chown uid = %d, want 0", uid)
		}
		gotGID = gid
		return nil
	}

	res, err := Install(opts)
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	for _, cmd := range cmds {
		if strings.HasPrefix(cmd, "groupadd") || strings.HasPrefix(cmd, "getent") {
			t.Errorf("an existing group still ran %q", cmd)
		}
	}
	if gotGID != gid {
		t.Errorf("configuration group = %d, want %d", gotGID, gid)
	}
	if got, want := mustReadFile(t, filepath.Join(opts.Root, "etc/peers/config.yaml")), string(config.Template(config.ModeSystem)); got != want {
		t.Errorf("config content = %q, want the system template", got)
	}
	if res.AdminGroup != DefaultAdminGroup {
		t.Errorf("AdminGroup = %q, want %q", res.AdminGroup, DefaultAdminGroup)
	}
}

func TestInstallSystemFailsWhenTheAdminGroupCannotBeCreated(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	opts.GroupGID = nil
	var cmds []string
	started := false
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		switch {
		case name == "getent":
			return []byte(""), exitError(t, 2)
		case name == "groupadd":
			return []byte("groupadd: cannot create group"), errors.New("exit status 9")
		case slices.Contains(args, "restart"):
			started = true
		}
		return nil, nil
	}
	opts.Probe = probeAfterStart(&started, 4242)
	opts.Chown = func(string, int, int) error {
		t.Error("chown ran after a failed group creation")
		return nil
	}
	opts.Chmod = func(string, fs.FileMode) error {
		t.Error("chmod ran after a failed group creation")
		return nil
	}

	_, err := Install(opts)
	if err == nil || !strings.Contains(err.Error(), "cannot create group") {
		t.Fatalf("Install error = %v, want the groupadd failure", err)
	}
	if want := []string{"getent group peers", "groupadd --system peers"}; !slices.Equal(cmds, want) {
		t.Errorf("commands = %q, want %q", cmds, want)
	}
	assertNoEntries(t, opts.Root)
}

func TestInstallSystemHonoursAdminGroup(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	opts.AdminGroup = "broker-ops"
	const gid = 555
	var lookedUp []string
	opts.GroupGID = func(name string) (int, bool, error) {
		lookedUp = append(lookedUp, name)
		return gid, true, nil
	}
	var cmds []string
	started := false
	opts.Run = recordCommandsUntil(&cmds, &started, "restart")
	opts.Probe = probeAfterStart(&started, 4242)
	var gotGID int
	opts.Chown = func(_ string, _, gid int) error {
		gotGID = gid
		return nil
	}

	res, err := Install(opts)
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	if !slices.Equal(lookedUp, []string{"broker-ops"}) {
		t.Errorf("group lookups = %q, want [broker-ops]", lookedUp)
	}
	if gotGID != gid {
		t.Errorf("configuration group = %d, want %d", gotGID, gid)
	}
	if res.AdminGroup != "broker-ops" {
		t.Errorf("AdminGroup = %q, want broker-ops", res.AdminGroup)
	}
}

func TestInstallSystemCreatesTheNamedAdminGroup(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	opts.AdminGroup = "broker-ops"
	opts.GroupGID = nil
	const gid = 556
	cmds, _ := systemInstallWithGroupRun(t, &opts, gid)

	if _, err := Install(opts); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if want := "groupadd --system broker-ops"; !slices.Contains(*cmds, want) {
		t.Errorf("commands = %q, want %q", *cmds, want)
	}
}

func TestInstallKeepsExistingUserConfig(t *testing.T) {
	opts := baseOptions(t)
	var cmds []string
	started := false
	opts.Run = recordCommandsUntil(&cmds, &started, "restart")
	opts.Probe = probeAfterStart(&started, 4242)
	configPath := filepath.Join(opts.Root, "home/alice/.peers/config.yaml")
	if err := os.MkdirAll(filepath.Dir(configPath), 0o700); err != nil {
		t.Fatal(err)
	}
	const existing = "broker:\n  alias: x\n"
	if err := os.WriteFile(configPath, []byte(existing), 0o600); err != nil {
		t.Fatal(err)
	}

	res, err := Install(opts)
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	if got := mustReadFile(t, configPath); got != existing {
		t.Errorf("config was overwritten: %q", got)
	}
	if res.ConfigNew {
		t.Error("ConfigNew = true, want false")
	}
	if res.ConfigPath != "/home/alice/.peers/config.yaml" {
		t.Errorf("ConfigPath = %q", res.ConfigPath)
	}
}

// TestCreateConfigRemovesPartialFileOnWriteFailure checks that a file created
// by this run is removed when writing the template fails, so the next install
// does not mistake an incomplete file for an existing configuration.
func TestCreateConfigRemovesPartialFileOnWriteFailure(t *testing.T) {
	opts := baseOptions(t)
	res := Result{}
	open := func(path string) (*os.File, error) {
		if err := os.WriteFile(path, []byte("partial"), 0o600); err != nil {
			return nil, err
		}
		return os.Open(path) // read-only, so Write fails
	}
	err := createConfigWith(opts, &res, -1, 0o600, open)
	if err == nil {
		t.Fatal("createConfigWith error = nil, want the write failure")
	}
	path := filepath.Join(opts.Root, "home/alice/.peers/config.yaml")
	// The error must name the reason, not only that the step failed: the path
	// the write targeted and the write error the read-only handle produced.
	for _, want := range []string{"cannot write the configuration file", path, "bad file descriptor"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("createConfigWith error = %q, want it to contain %q", err, want)
		}
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, fs.ErrNotExist) {
		t.Errorf("the partial configuration file survived the write failure: %v", statErr)
	}
}

func TestInstallSystemRequiresRoot(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 1000
	called := false
	opts.Run = func(string, ...string) ([]byte, error) {
		called = true
		return nil, nil
	}

	_, err := Install(opts)
	const want = "service install --system must run as root"
	if err == nil || err.Error() != want {
		t.Fatalf("Install error = %v, want %q", err, want)
	}
	assertNoEntries(t, opts.Root)
	if called {
		t.Error("Install ran a command before the root check")
	}
}

func TestInstallUserRejectsRoot(t *testing.T) {
	opts := baseOptions(t)
	opts.System = false
	opts.EUID = 0
	called := false
	opts.Run = func(string, ...string) ([]byte, error) {
		called = true
		return nil, nil
	}

	_, err := Install(opts)
	const want = "service install without --system must not run as root; the user unit belongs to one account"
	if err == nil || err.Error() != want {
		t.Fatalf("Install error = %v, want %q", err, want)
	}
	assertNoEntries(t, opts.Root)
	if called {
		t.Error("Install ran a command before the root check")
	}
}

func TestInstallSystemRejectsWritableBinary(t *testing.T) {
	t.Run("directory owned by another account", func(t *testing.T) {
		opts := baseOptions(t)
		opts.System = true
		opts.EUID = 0
		opts.Owner = fakeOwner(map[string]fakeOwnerEntry{
			"/usr/local/bin": {uid: 1000, mode: 0o755},
		})
		_, err := Install(opts)
		if err == nil || !strings.Contains(err.Error(), "/usr/local/bin") {
			t.Fatalf("Install error = %v, want one naming /usr/local/bin", err)
		}
	})

	t.Run("binary group writable", func(t *testing.T) {
		opts := baseOptions(t)
		opts.System = true
		opts.EUID = 0
		opts.Owner = fakeOwner(map[string]fakeOwnerEntry{
			opts.Exe: {uid: 0, mode: 0o775},
		})
		_, err := Install(opts)
		if err == nil || !strings.Contains(err.Error(), opts.Exe) {
			t.Fatalf("Install error = %v, want one naming %s", err, opts.Exe)
		}
	})

	t.Run("safe binary succeeds", func(t *testing.T) {
		opts := baseOptions(t)
		opts.System = true
		opts.EUID = 0
		var cmds []string
		started := false
		opts.Run = recordCommandsUntil(&cmds, &started, "restart")
		opts.Probe = probeAfterStart(&started, 4242)
		if _, err := Install(opts); err != nil {
			t.Fatalf("Install: %v", err)
		}
	})
}

func TestInstallSystemChecksResolvedPath(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	const resolved = "/home/alice/bin/peers"
	opts.EvalSymlinks = func(string) (string, error) { return resolved, nil }
	opts.Owner = fakeOwner(map[string]fakeOwnerEntry{
		resolved: {uid: 1000, mode: 0o755},
	})

	_, err := Install(opts)
	if err == nil || !strings.Contains(err.Error(), resolved) {
		t.Fatalf("Install error = %v, want one naming %s", err, resolved)
	}
}

func TestInstallDarwinSystemWritesResolvedExecutable(t *testing.T) {
	const (
		link     = "/Users/alice/bin/peers"
		resolved = "/opt/peers/peers"
	)
	darwinSystem := func(t *testing.T) Options {
		t.Helper()
		opts := baseOptions(t)
		opts.GOOS = "darwin"
		opts.System = true
		opts.EUID = 0
		opts.UID = 0
		opts.Exe = link
		opts.EvalSymlinks = func(string) (string, error) { return resolved, nil }
		var cmds []string
		started := false
		opts.Run = func(name string, args ...string) ([]byte, error) {
			cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
			switch {
			case len(args) == 2 && args[0] == "print":
				if started {
					return []byte("state = running\n\tpid = 4242\n"), nil
				}
				return []byte("Could not find service"), exitError(t, 113)
			case len(args) > 0 && args[0] == "bootout":
				return nil, exitError(t, 3)
			case len(args) > 0 && args[0] == "bootstrap":
				started = true
			}
			return nil, nil
		}
		opts.Probe = probeAfterStart(&started, 4242)
		return opts
	}

	t.Run("plist names the resolved path", func(t *testing.T) {
		opts := darwinSystem(t)
		if _, err := Install(opts); err != nil {
			t.Fatalf("Install: %v", err)
		}
		plist := filepath.Join(opts.Root, "Library/LaunchDaemons/io.github.wangzw.peers.plist")
		want, err := LaunchDaemonPlist(resolved)
		if err != nil {
			t.Fatal(err)
		}
		if got := mustReadFile(t, plist); got != want {
			t.Errorf("plist content mismatch:\n got %q\nwant %q", got, want)
		}
		if got := mustReadFile(t, plist); strings.Contains(got, link) {
			t.Errorf("plist names the unresolved link %q:\n%s", link, got)
		}
	})

	t.Run("resolved target outside root is refused", func(t *testing.T) {
		opts := darwinSystem(t)
		opts.Owner = fakeOwner(map[string]fakeOwnerEntry{
			resolved: {uid: 1000, mode: 0o755},
		})
		_, err := Install(opts)
		if err == nil || !strings.Contains(err.Error(), resolved) {
			t.Fatalf("Install error = %v, want one naming %s", err, resolved)
		}
		assertNoEntries(t, opts.Root)
	})
}

func TestInstallRejectsTempBinary(t *testing.T) {
	for _, system := range []struct {
		name string
		set  bool
	}{{"user", false}, {"system", true}} {
		t.Run(system.name, func(t *testing.T) {
			opts := baseOptions(t)
			opts.Exe = "/tmp/go-build123/exe/peers"
			opts.TempDir = "/tmp"
			opts.System = system.set
			if system.set {
				opts.EUID = 0
			}
			_, err := Install(opts)
			if err == nil || !strings.Contains(err.Error(), "temporary") {
				t.Fatalf("Install error = %v, want a temporary-directory rejection", err)
			}
		})
	}
}

func TestInstallLinuxWithoutSystemctl(t *testing.T) {
	opts := baseOptions(t)
	opts.LookPath = func(string) (string, error) { return "", errors.New("not found") }

	_, err := Install(opts)
	if err == nil {
		t.Fatal("Install succeeded without systemctl")
	}
	if !strings.Contains(err.Error(), "cannot find systemctl (is systemd installed?)") {
		t.Errorf("error %q does not carry the parenthesized hint", err)
	}
	assertNoEntries(t, opts.Root)
}

func TestInstallDarwinUser(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.System = false
	opts.EUID = 501
	opts.UID = 501
	opts.Home = "/Users/alice"
	var cmds []string
	started := false
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		switch {
		case len(args) == 2 && args[0] == "print" && args[1] == "gui/501":
			return []byte("com.apple.launchd\n"), nil
		case len(args) == 2 && args[0] == "print":
			if started {
				return []byte("state = running\n\tpid = 4242\n"), nil
			}
			return []byte("Could not find service"), exitError(t, 113)
		case len(args) > 0 && args[0] == "bootout":
			return nil, exitError(t, 3)
		case len(args) > 0 && args[0] == "bootstrap":
			started = true
		}
		return nil, nil
	}
	opts.Probe = probeAfterStart(&started, 4242)

	res, err := Install(opts)
	if err != nil {
		t.Fatalf("Install: %v", err)
	}

	plist := filepath.Join(opts.Root, "Users/alice/Library/LaunchAgents/io.github.wangzw.peers.plist")
	want, err := LaunchAgentPlist(opts.Exe, "/Users/alice/Library/Logs/peers/broker.log")
	if err != nil {
		t.Fatal(err)
	}
	if got := mustReadFile(t, plist); got != want {
		t.Errorf("plist content mismatch:\n got %q\nwant %q", got, want)
	}

	logDir := filepath.Join(opts.Root, "Users/alice/Library/Logs/peers")
	fi, err := os.Stat(logDir)
	if err != nil {
		t.Fatalf("log directory missing: %v", err)
	}
	if !fi.IsDir() {
		t.Errorf("%s is not a directory", logDir)
	}
	assertMode(t, logDir, 0o700)

	wantCmds := []string{
		"launchctl print gui/501",
		"launchctl print gui/501/io.github.wangzw.peers",
		"launchctl bootout gui/501/io.github.wangzw.peers",
		"launchctl print gui/501/io.github.wangzw.peers",
		"launchctl enable gui/501/io.github.wangzw.peers",
		"launchctl bootstrap gui/501 /Users/alice/Library/LaunchAgents/io.github.wangzw.peers.plist",
		"launchctl print gui/501/io.github.wangzw.peers",
	}
	if !slices.Equal(cmds, wantCmds) {
		t.Errorf("commands = %q, want %q", cmds, wantCmds)
	}
	if !slices.Equal(res.Commands, []string{
		"launchctl bootout gui/501/io.github.wangzw.peers",
		"launchctl enable gui/501/io.github.wangzw.peers",
		"launchctl bootstrap gui/501 /Users/alice/Library/LaunchAgents/io.github.wangzw.peers.plist",
	}) {
		t.Errorf("Result.Commands = %q, want the bootout, enable and bootstrap", res.Commands)
	}
	if res.Domain != "gui/501" {
		t.Errorf("Domain = %q, want gui/501", res.Domain)
	}
}

func TestInstallDarwinWithoutGUIDomain(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.EUID = 501
	opts.UID = 501
	opts.Home = "/Users/alice"
	opts.Run = func(string, ...string) ([]byte, error) {
		return []byte("Could not find domain"), exitError(t, 113)
	}

	_, err := Install(opts)
	if err == nil {
		t.Fatal("Install succeeded without a gui domain")
	}
	for _, want := range []string{"graphical login session", "--system"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	assertNoEntries(t, opts.Root)
}

func TestInstallDarwinBootoutFailure(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.EUID = 501
	opts.UID = 501
	opts.Home = "/Users/alice"
	// A loaded service has a plist on disk, so this failure must keep it and
	// disable the label.
	plist := filepath.Join(opts.Root, "Users/alice/Library/LaunchAgents/io.github.wangzw.peers.plist")
	if err := os.MkdirAll(filepath.Dir(plist), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(plist, []byte("old plist"), 0o644); err != nil {
		t.Fatal(err)
	}
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		switch {
		case len(args) == 2 && args[0] == "print" && args[1] == "gui/501":
			return []byte("com.apple.launchd\n"), nil
		case len(args) == 2 && args[0] == "print":
			return []byte("state = running\n\tpid = 4242\n"), nil
		case len(args) > 0 && args[0] == "bootout":
			return []byte("Boot-out failed: 5: Input/output error"), exitError(t, 5)
		}
		return nil, nil
	}

	_, err := Install(opts)
	if err == nil || !strings.Contains(err.Error(), "bootout") {
		t.Fatalf("Install error = %v, want the bootout failure", err)
	}
	for _, want := range []string{"could not be unloaded", "label was disabled", "run service install again"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	wantCmds := []string{
		"launchctl print gui/501",
		"launchctl print gui/501/io.github.wangzw.peers",
		"launchctl bootout gui/501/io.github.wangzw.peers",
		"launchctl disable gui/501/io.github.wangzw.peers",
	}
	if !slices.Equal(cmds, wantCmds) {
		t.Errorf("commands = %q, want %q", cmds, wantCmds)
	}
	if _, err := os.Stat(plist); err != nil {
		t.Errorf("plist after a failed bootout: %v", err)
	}
}

// TestInstallDarwinReportsADisableFailure checks that a disable that fails is
// reported as such: the error must not claim the label was disabled, because the
// kept plist may then load and start a Broker at the next login.
func TestInstallDarwinReportsADisableFailure(t *testing.T) {
	opts := darwinUserReinstall(t)
	opts.Run = func(name string, args ...string) ([]byte, error) {
		switch {
		case len(args) == 2 && args[0] == "print" && args[1] == "gui/501":
			return []byte("com.apple.launchd\n"), nil
		case len(args) == 2 && args[0] == "print":
			return []byte("state = running\n\tpid = 4242\n"), nil
		case len(args) > 0 && args[0] == "bootout":
			return []byte("Boot-out failed: 5: Input/output error"), exitError(t, 5)
		case len(args) > 0 && args[0] == "disable":
			return []byte("disable failed"), errors.New("exit status 1")
		}
		return nil, nil
	}

	_, err := Install(opts)
	if err == nil {
		t.Fatal("Install succeeded although bootout failed")
	}
	for _, want := range []string{"could not be disabled", "may start automatically", "disable failed"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "label was disabled") {
		t.Errorf("error %q claims the label was disabled although disable failed", err)
	}
}

func TestInstallDarwinBootstrapFailureRollsBackThePlist(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.EUID = 501
	opts.UID = 501
	opts.Home = "/Users/alice"
	// A fresh installation has no old service, so this exercises the rollback
	// after one bootstrap attempt rather than the exit-5 retry loop.
	opts.BootstrapTimeout = time.Nanosecond
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		switch {
		case len(args) == 2 && args[0] == "print" && args[1] == "gui/501":
			return []byte("com.apple.launchd\n"), nil
		case len(args) == 2 && args[0] == "print":
			return []byte("Could not find service"), exitError(t, 113)
		case len(args) > 0 && args[0] == "bootout":
			return nil, exitError(t, 3)
		case len(args) > 0 && args[0] == "bootstrap":
			return []byte("Bootstrap failed: 5: Input/output error"), exitError(t, 5)
		}
		return nil, nil
	}

	_, err := Install(opts)
	if err == nil || !strings.Contains(err.Error(), "Bootstrap failed") {
		t.Fatalf("Install error = %v, want the bootstrap failure", err)
	}
	wantCmds := []string{
		"launchctl print gui/501",
		"launchctl print gui/501/io.github.wangzw.peers",
		"launchctl bootout gui/501/io.github.wangzw.peers",
		"launchctl print gui/501/io.github.wangzw.peers",
		"launchctl enable gui/501/io.github.wangzw.peers",
		"launchctl bootstrap gui/501 /Users/alice/Library/LaunchAgents/io.github.wangzw.peers.plist",
	}
	if !slices.Equal(cmds, wantCmds) {
		t.Errorf("commands = %q, want %q", cmds, wantCmds)
	}
	plist := filepath.Join(opts.Root, "Users/alice/Library/LaunchAgents/io.github.wangzw.peers.plist")
	if _, err := os.Stat(plist); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("plist was not rolled back: %v", err)
	}
}

func TestInstallDarwinBootstrapFailureReportsTheOutput(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.EUID = 501
	opts.UID = 501
	opts.Home = "/Users/alice"
	opts.BootstrapTimeout = time.Nanosecond
	opts.Run = func(name string, args ...string) ([]byte, error) {
		switch {
		case len(args) == 2 && args[0] == "print" && args[1] == "gui/501":
			return []byte("com.apple.launchd\n"), nil
		case len(args) == 2 && args[0] == "print":
			return []byte("Could not find service"), exitError(t, 113)
		case len(args) > 0 && args[0] == "bootout":
			return nil, exitError(t, 3)
		case len(args) > 0 && args[0] == "bootstrap":
			return []byte("Bootstrap failed: 5: Input/output error"), exitError(t, 5)
		}
		return nil, nil
	}

	_, err := Install(opts)
	if err == nil {
		t.Fatal("Install succeeded after a failed bootstrap")
	}
	for _, want := range []string{"Bootstrap failed: 5: Input/output error", "launchctl bootstrap"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

func TestInstallDarwinBootstrapFailureReportsTheOutputWhenRollbackFails(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.EUID = 501
	opts.UID = 501
	opts.Home = "/Users/alice"
	opts.BootstrapTimeout = time.Nanosecond
	plist := filepath.Join(opts.Root, "Users/alice/Library/LaunchAgents/io.github.wangzw.peers.plist")
	opts.Run = func(name string, args ...string) ([]byte, error) {
		switch {
		case len(args) == 2 && args[0] == "print" && args[1] == "gui/501":
			return []byte("com.apple.launchd\n"), nil
		case len(args) == 2 && args[0] == "print":
			return []byte("Could not find service"), exitError(t, 113)
		case len(args) > 0 && args[0] == "bootout":
			return nil, exitError(t, 3)
		case len(args) > 0 && args[0] == "bootstrap":
			// Replace the plist with a non-empty directory so the rollback
			// cannot remove it.
			_ = os.Remove(plist)
			if err := os.Mkdir(plist, 0o755); err != nil {
				t.Fatalf("creating %s: %v", plist, err)
			}
			if err := os.WriteFile(filepath.Join(plist, "keep"), []byte("x"), 0o644); err != nil {
				t.Fatalf("writing marker: %v", err)
			}
			return []byte("Bootstrap failed: 5: Input/output error"), exitError(t, 5)
		}
		return nil, nil
	}

	_, err := Install(opts)
	if err == nil {
		t.Fatal("Install succeeded after a failed bootstrap")
	}
	for _, want := range []string{"Bootstrap failed", "could not be cleaned up", plist} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

func TestInstallDarwinSystem(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.System = true
	opts.EUID = 0
	opts.UID = 0
	opts.Home = "/var/root"
	var cmds []string
	started := false
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		switch {
		case len(args) == 2 && args[0] == "print":
			if started {
				return []byte("state = running\n\tpid = 4242\n"), nil
			}
			return []byte("Could not find service"), exitError(t, 113)
		case len(args) > 0 && args[0] == "bootout":
			return nil, exitError(t, 3)
		case len(args) > 0 && args[0] == "bootstrap":
			started = true
		}
		return nil, nil
	}
	opts.Probe = probeAfterStart(&started, 4242)

	res, err := Install(opts)
	if err != nil {
		t.Fatalf("Install: %v", err)
	}

	plist := filepath.Join(opts.Root, "Library/LaunchDaemons/io.github.wangzw.peers.plist")
	want, err := LaunchDaemonPlist(opts.Exe)
	if err != nil {
		t.Fatal(err)
	}
	if got := mustReadFile(t, plist); got != want {
		t.Errorf("plist content mismatch:\n got %q\nwant %q", got, want)
	}

	logDir := filepath.Join(opts.Root, "var/log/peers")
	if fi, err := os.Stat(logDir); err != nil || !fi.IsDir() {
		t.Errorf("log directory %s: %v", logDir, err)
	}
	assertMode(t, logDir, 0o750)

	configPath := filepath.Join(opts.Root, "etc/peers/config.yaml")
	if got := mustReadFile(t, configPath); got != string(config.Template(config.ModeSystem)) {
		t.Errorf("config content = %q, want the system template", got)
	}
	assertMode(t, configPath, 0o660)

	wantCmds := []string{
		"launchctl print system/io.github.wangzw.peers",
		"launchctl bootout system/io.github.wangzw.peers",
		"launchctl print system/io.github.wangzw.peers",
		"launchctl enable system/io.github.wangzw.peers",
		"launchctl bootstrap system /Library/LaunchDaemons/io.github.wangzw.peers.plist",
		"launchctl print system/io.github.wangzw.peers",
	}
	if !slices.Equal(cmds, wantCmds) {
		t.Errorf("commands = %q, want %q", cmds, wantCmds)
	}
	if !slices.Equal(res.Commands, []string{
		"launchctl bootout system/io.github.wangzw.peers",
		"launchctl enable system/io.github.wangzw.peers",
		"launchctl bootstrap system /Library/LaunchDaemons/io.github.wangzw.peers.plist",
	}) {
		t.Errorf("Result.Commands = %q, want the bootout, enable and bootstrap", res.Commands)
	}
	if res.Domain != "system" {
		t.Errorf("Domain = %q, want system", res.Domain)
	}
}

func TestInstallRejectsForeignBroker(t *testing.T) {
	t.Run("linux service-managed Broker is replaced", func(t *testing.T) {
		opts := baseOptions(t)
		const pid = 4242
		opts.Probe = func(string) (int, bool) { return pid, true }
		var cmds []string
		opts.Run = func(name string, args ...string) ([]byte, error) {
			cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
			if slices.Contains(args, "show") {
				return []byte(fmt.Sprintf("%d\n", pid)), nil
			}
			return nil, nil
		}

		res, err := Install(opts)
		if err != nil {
			t.Fatalf("Install: %v", err)
		}
		wantCmds := []string{
			"systemctl --user show --property MainPID --value peers.service",
			"systemctl --user daemon-reload",
			"systemctl --user enable peers.service",
			"systemctl --user restart peers.service",
			"systemctl --user show --property MainPID --value peers.service",
		}
		if !slices.Equal(cmds, wantCmds) {
			t.Errorf("commands = %q, want %q", cmds, wantCmds)
		}
		if res.UnitPath == "" {
			t.Error("UnitPath is empty")
		}
	})

	t.Run("linux service pid differs from the socket peer", func(t *testing.T) {
		opts := baseOptions(t)
		opts.Probe = func(string) (int, bool) { return 4242, true }
		opts.Run = func(name string, args ...string) ([]byte, error) {
			if slices.Contains(args, "show") {
				return []byte("9999\n"), nil
			}
			return nil, nil
		}

		_, err := Install(opts)
		if err == nil || !strings.Contains(err.Error(), "not started by the peers service") {
			t.Fatalf("Install error = %v, want the foreign-Broker rejection", err)
		}
		for _, want := range []string{"service pid 9999", "socket peer pid 4242", "stop it before installing"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not contain %q", err, want)
			}
		}
		assertNoEntries(t, opts.Root)
	})

	t.Run("linux unit not running", func(t *testing.T) {
		opts := baseOptions(t)
		opts.Probe = func(string) (int, bool) { return 4242, true }
		opts.Run = func(name string, args ...string) ([]byte, error) {
			if slices.Contains(args, "show") {
				return []byte("0\n"), nil
			}
			return nil, nil
		}

		_, err := Install(opts)
		if err == nil || !strings.Contains(err.Error(), "not started by the peers service") {
			t.Fatalf("Install error = %v, want the foreign-Broker rejection", err)
		}
		assertNoEntries(t, opts.Root)
	})

	t.Run("linux system pid differs", func(t *testing.T) {
		opts := baseOptions(t)
		opts.System = true
		opts.EUID = 0
		opts.Probe = func(path string) (int, bool) {
			if path != "/run/peers/broker.sock" {
				t.Errorf("probe path = %q", path)
			}
			return 4242, true
		}
		opts.Run = func(name string, args ...string) ([]byte, error) {
			if slices.Contains(args, "show") {
				return []byte("9999\n"), nil
			}
			return nil, nil
		}

		_, err := Install(opts)
		if err == nil || !strings.Contains(err.Error(), "system Broker") {
			t.Fatalf("Install error = %v, want the foreign system Broker rejection", err)
		}
	})

	t.Run("macOS service-managed Broker is replaced", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "darwin"
		opts.EUID = 501
		opts.UID = 501
		opts.Home = "/Users/alice"
		const pid = 4242
		opts.Probe = func(string) (int, bool) { return pid, true }
		var cmds []string
		bootedOut := false
		started := false
		opts.Run = func(name string, args ...string) ([]byte, error) {
			cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
			switch {
			case len(args) == 2 && args[0] == "print" && args[1] == "gui/501":
				return []byte("com.apple.launchd\n"), nil
			case len(args) == 2 && args[0] == "print":
				if bootedOut && !started {
					return []byte("Could not find service"), exitError(t, 113)
				}
				return []byte(fmt.Sprintf("state = running\n\tpid = %d\n", pid)), nil
			case len(args) > 0 && args[0] == "bootout":
				bootedOut = true
				return nil, exitError(t, 3)
			case len(args) > 0 && args[0] == "bootstrap":
				started = true
			}
			return nil, nil
		}

		if _, err := Install(opts); err != nil {
			t.Fatalf("Install: %v", err)
		}
		wantCmds := []string{
			"launchctl print gui/501",
			"launchctl print gui/501/io.github.wangzw.peers",
			"launchctl print gui/501/io.github.wangzw.peers",
			"launchctl bootout gui/501/io.github.wangzw.peers",
			"launchctl print gui/501/io.github.wangzw.peers",
			"launchctl enable gui/501/io.github.wangzw.peers",
			"launchctl bootstrap gui/501 /Users/alice/Library/LaunchAgents/io.github.wangzw.peers.plist",
			"launchctl print gui/501/io.github.wangzw.peers",
		}
		if !slices.Equal(cmds, wantCmds) {
			t.Errorf("commands = %q, want %q", cmds, wantCmds)
		}
	})

	t.Run("macOS service pid differs", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "darwin"
		opts.EUID = 501
		opts.UID = 501
		opts.Home = "/Users/alice"
		opts.Probe = func(string) (int, bool) { return 4242, true }
		opts.Run = func(name string, args ...string) ([]byte, error) {
			if len(args) == 2 && args[1] == "gui/501/io.github.wangzw.peers" {
				return []byte("state = running\n\tpid = 9999\n"), nil
			}
			return nil, nil
		}

		_, err := Install(opts)
		if err == nil || !strings.Contains(err.Error(), "not started by the peers service") {
			t.Fatalf("Install error = %v, want the foreign-Broker rejection", err)
		}
		for _, want := range []string{"service pid 9999", "socket peer pid 4242"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not contain %q", err, want)
			}
		}
	})

	t.Run("macOS unit not loaded but the socket answers", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "darwin"
		opts.EUID = 501
		opts.UID = 501
		opts.Home = "/Users/alice"
		opts.Probe = func(string) (int, bool) { return 4242, true }
		opts.Run = func(name string, args ...string) ([]byte, error) {
			if len(args) == 2 && args[1] == "gui/501/io.github.wangzw.peers" {
				return []byte("Could not find service"), exitError(t, 113)
			}
			return nil, nil
		}

		_, err := Install(opts)
		if err == nil || !strings.Contains(err.Error(), "not started by the peers service") {
			t.Fatalf("Install error = %v, want the foreign-Broker rejection", err)
		}
		if !strings.Contains(err.Error(), "service pid 0") {
			t.Errorf("error %q does not name the missing service pid", err)
		}
	})

	t.Run("peer pid unknown", func(t *testing.T) {
		opts := baseOptions(t)
		opts.Probe = func(string) (int, bool) { return 0, true }
		opts.Run = func(name string, args ...string) ([]byte, error) {
			if slices.Contains(args, "show") {
				return []byte("4242\n"), nil
			}
			return nil, nil
		}

		_, err := Install(opts)
		if err == nil || !strings.Contains(err.Error(), "not started by the peers service") {
			t.Fatalf("Install error = %v, want the foreign-Broker rejection", err)
		}
		if !strings.Contains(err.Error(), "socket peer pid 0") {
			t.Errorf("error %q does not name the unknown peer pid", err)
		}
	})
}

func TestInstallRejectsAnAnsweringForeignBrokerAtReady(t *testing.T) {
	cases := []struct {
		name    string
		mainPID string
	}{
		{name: "service pid zero", mainPID: "0"},
		{name: "service pid differs", mainPID: "9999"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := baseOptions(t)
			probes := 0
			opts.Probe = func(string) (int, bool) {
				probes++
				if probes == 1 {
					// The pre-install check sees no Broker.
					return 0, false
				}
				// A foreign Broker answers after the service starts.
				return 6666, true
			}
			opts.Run = func(name string, args ...string) ([]byte, error) {
				if slices.Contains(args, "show") {
					return []byte(tc.mainPID + "\n"), nil
				}
				return nil, nil
			}
			opts.ReadyTimeout = 30 * time.Millisecond

			_, err := Install(opts)
			if err == nil {
				t.Fatal("Install succeeded while a foreign Broker answered on the socket")
			}
			for _, want := range []string{"not the service's process", "socket peer pid 6666", "journalctl --user -u peers.service"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not contain %q", err, want)
				}
			}
		})
	}
}

func TestInstallWaitsUntilTheServiceOwnsTheSocket(t *testing.T) {
	opts := baseOptions(t)
	probes := 0
	opts.Probe = func(string) (int, bool) {
		probes++
		if probes <= 3 {
			return 0, false
		}
		return 4242, true
	}
	opts.Run = func(name string, args ...string) ([]byte, error) {
		if slices.Contains(args, "show") {
			return []byte("4242\n"), nil
		}
		return nil, nil
	}
	opts.ReadyTimeout = time.Second

	if _, err := Install(opts); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if probes < 4 {
		t.Errorf("probe calls = %d, want the readiness loop to retry", probes)
	}
}

func TestInstallWaitsForTheBroker(t *testing.T) {
	t.Run("linux user", func(t *testing.T) {
		opts := baseOptions(t)
		var cmds []string
		started := false
		opts.Run = recordCommandsUntil(&cmds, &started, "restart")
		opts.Probe = func(string) (int, bool) { return 0, false }
		opts.ReadyTimeout = 30 * time.Millisecond

		_, err := Install(opts)
		if err == nil || !strings.Contains(err.Error(), "cannot reach the Broker") {
			t.Fatalf("Install error = %v, want the readiness timeout", err)
		}
		for _, want := range []string{
			"within 1 second",
			"check the service log: journalctl --user -u peers.service",
		} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not contain %q", err, want)
			}
		}
		if !slices.Contains(cmds, "systemctl --user restart peers.service") {
			t.Errorf("commands = %q, want the restart", cmds)
		}
	})

	t.Run("darwin system", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "darwin"
		opts.System = true
		opts.EUID = 0
		opts.UID = 0
		opts.Home = "/var/root"
		opts.Probe = func(string) (int, bool) { return 0, false }
		opts.ReadyTimeout = 30 * time.Millisecond
		opts.Run = func(name string, args ...string) ([]byte, error) {
			if len(args) == 2 && args[0] == "print" {
				return []byte("Could not find service"), exitError(t, 113)
			}
			if len(args) > 0 && args[0] == "bootout" {
				return nil, exitError(t, 3)
			}
			return nil, nil
		}

		_, err := Install(opts)
		if err == nil || !strings.Contains(err.Error(), "cannot reach the Broker") {
			t.Fatalf("Install error = %v, want the readiness timeout", err)
		}
		if !strings.Contains(err.Error(), "/var/log/peers/broker.log") {
			t.Errorf("error %q does not name the system log", err)
		}
	})

	t.Run("linux system", func(t *testing.T) {
		opts := baseOptions(t)
		opts.System = true
		opts.EUID = 0
		opts.Probe = func(string) (int, bool) { return 0, false }
		opts.ReadyTimeout = 30 * time.Millisecond

		_, err := Install(opts)
		if err == nil || !strings.Contains(err.Error(), "cannot reach the Broker") {
			t.Fatalf("Install error = %v, want the readiness timeout", err)
		}
		if !strings.Contains(err.Error(), "journalctl -u peers.service") {
			t.Errorf("error %q does not name the system service log", err)
		}
	})

	t.Run("darwin user", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "darwin"
		opts.EUID = 501
		opts.UID = 501
		opts.Home = "/Users/alice"
		opts.Probe = func(string) (int, bool) { return 0, false }
		opts.ReadyTimeout = 30 * time.Millisecond
		opts.Run = func(name string, args ...string) ([]byte, error) {
			if len(args) == 2 && args[0] == "print" && args[1] == "gui/501" {
				return []byte("com.apple.launchd\n"), nil
			}
			if len(args) == 2 && args[0] == "print" {
				return []byte("Could not find service"), exitError(t, 113)
			}
			if len(args) > 0 && args[0] == "bootout" {
				return nil, exitError(t, 3)
			}
			return nil, nil
		}

		_, err := Install(opts)
		if err == nil || !strings.Contains(err.Error(), "cannot reach the Broker") {
			t.Fatalf("Install error = %v, want the readiness timeout", err)
		}
		if !strings.Contains(err.Error(), "/Users/alice/Library/Logs/peers/broker.log") {
			t.Errorf("error %q does not name the user log", err)
		}
	})
}

// writeListenConfig writes a configuration file for opts with broker.listen set
// to addr, as if an earlier install had generated it.
func writeListenConfig(t *testing.T, opts Options, addr string) {
	t.Helper()
	path := rooted(opts, configFilePath(opts))
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	body := "broker:\n  listen: \"" + addr + "\"\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func commandIndex(cmds []string, want string) int {
	return slices.Index(cmds, want)
}

func commandCount(cmds []string, want string) int {
	count := 0
	for _, cmd := range cmds {
		if cmd == want {
			count++
		}
	}
	return count
}

func TestInstallRefusesAnOccupiedListenAddress(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	addr := ln.Addr().String()

	opts := baseOptions(t)
	writeListenConfig(t, opts, addr)
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		if slices.Contains(args, "show") {
			return []byte("4242\n"), nil
		}
		return nil, nil
	}
	opts.Probe = func(string) (int, bool) { return 0, false }
	opts.ListenProbe = config.ProbeListen
	opts.ReadyTimeout = 20 * time.Millisecond

	_, err = Install(opts)
	if err == nil {
		t.Fatal("Install succeeded while broker.listen was held by another process")
	}
	for _, want := range []string{addr, "already in use", "/home/alice/.peers/config.yaml", "broker.listen"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	if !strings.Contains(err.Error(), "leave it empty") {
		t.Errorf("error %q does not suggest an empty broker.listen", err)
	}
	for _, verb := range []string{"stop", "disable", "enable", "restart"} {
		if cmd := "systemctl --user " + verb + " " + UnitName; slices.Contains(cmds, cmd) {
			t.Errorf("commands = %q, want no %q before the listen precheck passes", cmds, cmd)
		}
	}
	// The unit is written only after the precheck, so a fresh installation that
	// cannot use its address leaves no service definition to start at boot.
	unit := rooted(opts, "/home/alice/.config/systemd/user/"+UnitName)
	if _, statErr := os.Stat(unit); !errors.Is(statErr, fs.ErrNotExist) {
		t.Errorf("unit file %s exists after the precheck failed: %v", unit, statErr)
	}
}

// TestInstallDarwinLeavesNoAutostartServiceAfterAPrecheckFailure checks that a
// failed macOS precheck writes no plist. launchd loads every plist in the
// LaunchAgents and LaunchDaemons directories at login and boot, so a plist
// left behind would start a Broker that cannot bind.
func TestInstallDarwinLeavesNoAutostartServiceAfterAPrecheckFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	addr := ln.Addr().String()

	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.EUID = 501
	opts.UID = 501
	opts.Home = "/Users/alice"
	writeListenConfig(t, opts, addr)
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		switch {
		case len(args) == 2 && args[0] == "print" && args[1] == "gui/501":
			return []byte("com.apple.launchd\n"), nil
		case len(args) == 2 && args[0] == "print" && args[1] == "gui/501/"+Label:
			return []byte("Could not find service"), exitError(t, 113)
		}
		return nil, nil
	}
	opts.Probe = func(string) (int, bool) { return 0, false }
	opts.ListenProbe = config.ProbeListen

	_, err = Install(opts)
	if err == nil {
		t.Fatal("Install succeeded while broker.listen was held by another process")
	}
	plist := rooted(opts, "/Users/alice/Library/LaunchAgents/"+Label+".plist")
	if _, statErr := os.Stat(plist); !errors.Is(statErr, fs.ErrNotExist) {
		t.Errorf("plist %s exists after the precheck failed: %v", plist, statErr)
	}
	for _, cmd := range cmds {
		if strings.Contains(cmd, "bootstrap") || strings.Contains(cmd, "bootout") {
			t.Errorf("commands = %q, want no launchd state change after the precheck failed", cmds)
		}
	}
}

// TestInstallDisablesTheServiceWhenThePrecheckStopFails checks the case where
// the Linux precheck cannot stop the service holding broker.listen. The failed
// installation must still disable the service, and the error must say that it
// will not start automatically and give the command that restores it. Without
// both texts the operator finds a disabled service and is not told why or how
// to bring it back. macOS does not stop the service in the precheck; its
// bootout failure during the replacement is covered by
// TestInstallDarwinBootoutFailure.
func TestInstallDisablesTheServiceWhenThePrecheckStopFails(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	addr := ln.Addr().String()

	opts := baseOptions(t)
	writeListenConfig(t, opts, addr)
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		switch {
		case slices.Contains(args, "show"):
			return []byte("4242\n"), nil
		case slices.Contains(args, "stop"):
			return []byte("Job for peers.service failed"), errors.New("exit status 1")
		}
		return nil, nil
	}
	opts.Probe = func(string) (int, bool) { return 4242, true }
	opts.ListenProbe = config.ProbeListen

	_, err = Install(opts)
	if err == nil {
		t.Fatal("Install succeeded although the service holding broker.listen could not be stopped")
	}
	for _, want := range []string{
		"cannot stop the existing service",
		"will not start automatically",
		"systemctl --user enable --now " + UnitName,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	if got := commandCount(cmds, "systemctl --user disable --now "+UnitName); got != 1 {
		t.Errorf("disable commands = %d, want 1 in %q", got, cmds)
	}
}

// TestInstallRefusesAnExternalOccupantWithoutStoppingTheService checks the
// occupant identification: a same-named service may be running while another
// process holds broker.listen. The installation must reject the address and
// leave the running service alone, because stopping it would take down a
// healthy Broker that is not the occupant.
func TestInstallRefusesAnExternalOccupantWithoutStoppingTheService(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	addr := ln.Addr().String()

	opts := baseOptions(t)
	writeListenConfig(t, opts, addr)
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		if slices.Contains(args, "show") {
			// The service manager reports a running process, but that Broker is
			// not the one holding the address.
			return []byte("4242\n"), nil
		}
		return nil, nil
	}
	opts.Probe = func(string) (int, bool) { return 0, false }
	opts.ListenProbe = config.ProbeListen

	_, err = Install(opts)
	if err == nil {
		t.Fatal("Install succeeded while an external process held broker.listen")
	}
	if !strings.Contains(err.Error(), "already in use") {
		t.Errorf("error %q does not say the address is already in use", err)
	}
	for _, cmd := range cmds {
		for _, verb := range []string{"stop", "disable", "enable", "restart", "bootout"} {
			if strings.Contains(cmd, " "+verb+" ") || strings.HasSuffix(cmd, " "+verb) {
				t.Errorf("commands = %q, want no %q while the occupant is not this service", cmds, cmd)
			}
		}
	}
}

// TestListenErrorReportsTheOccupant checks that an address already in use is
// reported together with the pid and executable of the process holding it. The
// lookup reads a stand-in /proc tree, so the test does not depend on what
// listens on the host.
func TestListenErrorReportsTheOccupant(t *testing.T) {
	procRoot := t.TempDir()
	var table strings.Builder
	table.WriteString("  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n")
	// A non-listening socket on the same port must not be taken for the occupant.
	table.WriteString("   0: 0100007F:4423 0100007F:9999 01 00000000:00000000 00:00000000 00000000  1000        0 111111 1\n")
	table.WriteString("   1: 0100007F:4423 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 424242 1\n")
	writeProcFile(t, procRoot, "net/tcp", table.String())
	writeProcFile(t, procRoot, "net/tcp6", "")

	fdDir := filepath.Join(procRoot, "1234", "fd")
	if err := os.MkdirAll(fdDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("socket:[424242]", filepath.Join(fdDir, "3")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/usr/local/bin/agent-peer", filepath.Join(procRoot, "1234", "exe")); err != nil {
		t.Fatal(err)
	}

	opts := baseOptions(t)
	opts.ProcRoot = procRoot
	err := listenError(opts, "/etc/peers/config.yaml", "127.0.0.1:17443", syscall.EADDRINUSE)
	for _, want := range []string{
		`already in use by "/usr/local/bin/agent-peer" (pid 1234)`,
		"127.0.0.1:17443",
		"/etc/peers/config.yaml",
		"broker.listen",
		"leave it empty",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

// TestListenErrorFallsBackWithoutAnOccupant checks that an address in use whose
// holder cannot be identified still gets the plain message, with no pid or
// executable and no second failure.
func TestListenErrorFallsBackWithoutAnOccupant(t *testing.T) {
	opts := baseOptions(t)
	opts.ProcRoot = t.TempDir()
	err := listenError(opts, "/etc/peers/config.yaml", "127.0.0.1:17443", syscall.EADDRINUSE)
	if err == nil {
		t.Fatal("listenError returned nil for an address in use")
	}
	for _, want := range []string{"already in use", "127.0.0.1:17443", "/etc/peers/config.yaml", "broker.listen"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	for _, unwanted := range []string{"(pid", " by "} {
		if strings.Contains(err.Error(), unwanted) {
			t.Errorf("error %q names an occupant although none was found", err)
		}
	}
}

// TestListenErrorQuotesTheOccupantPath keeps the kernel's " (deleted)" mark
// from the /proc exe link outside the quotes: the path is quoted so a path with
// spaces or shell metacharacters cannot change how the sentence reads, and the
// mark stays visible so a replaced executable is recognizable as such.
func TestListenErrorQuotesTheOccupantPath(t *testing.T) {
	procRoot := t.TempDir()
	writeProcListener(t, procRoot, "tcp", "0100007F", 17443, 1234, "424242", "/usr/local/bin/agent-peer (deleted)")

	opts := baseOptions(t)
	opts.ProcRoot = procRoot
	err := listenError(opts, "/etc/peers/config.yaml", "127.0.0.1:17443", syscall.EADDRINUSE)
	want := `already in use by "/usr/local/bin/agent-peer" (deleted) (pid 1234)`
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %v, want it to contain %q", err, want)
	}
}

// TestOccupantFromProcFindsTCP6Listener checks that a listener in
// /proc/net/tcp6 is matched by its address, not only by its port: the listening
// and requested addresses differ in every case, so a lookup that ignored the
// address would report the wrong answer.
func TestOccupantFromProcFindsTCP6Listener(t *testing.T) {
	const wildcard = "00000000000000000000000000000000"      // ::
	const documentation = "b80d0120000000000000000001000000" // 2001:db8::1
	cases := []struct {
		name    string
		listen  string
		want    net.IP
		matches bool
	}{
		{name: "wildcard holds the specific address", listen: wildcard, want: net.ParseIP("::1"), matches: true},
		{name: "another specific address does not", listen: documentation, want: net.ParseIP("::1"), matches: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			procRoot := t.TempDir()
			writeProcListener(t, procRoot, "tcp6", tc.listen, 17443, 4321, "515151", "/opt/peers/bin/peers")
			got := occupantFromProc(procRoot, tc.want, 17443)
			if tc.matches != (len(got) == 1) {
				t.Fatalf("occupantFromProc = %+v, want matched=%v", got, tc.matches)
			}
			if tc.matches && (got[0].pid != 4321 || got[0].path != "/opt/peers/bin/peers") {
				t.Fatalf("occupantFromProc = %+v, want pid 4321 at /opt/peers/bin/peers", got)
			}
		})
	}
}

// TestOccupantFromProcMatchesListenAddress checks the wildcard rules: a
// listener on 0.0.0.0 holds a specific IPv4 address on the same port, a
// listener on another specific address does not, a wildcard request matches a
// specific listener, and a wildcard request matches a listener of the other
// address family, because Go binds 0.0.0.0 with a dual-stack socket.
func TestOccupantFromProcMatchesListenAddress(t *testing.T) {
	cases := []struct {
		name    string
		table   string
		local   string
		want    net.IP
		matches bool
	}{
		{name: "wildcard listener holds the specific address", table: "tcp", local: "00000000", want: net.ParseIP("127.0.0.1"), matches: true},
		{name: "another specific address does not", table: "tcp", local: "0200007F", want: net.ParseIP("127.0.0.1"), matches: false},
		{name: "a wildcard request matches a specific listener", table: "tcp", local: "0100007F", want: nil, matches: true},
		{name: "a wildcard IPv4 request matches an IPv6 listener", table: "tcp6", local: "00000000000000000000000001000000", want: net.ParseIP("0.0.0.0"), matches: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			procRoot := t.TempDir()
			writeProcListener(t, procRoot, tc.table, tc.local, 17443, 1234, "424242", "/usr/local/bin/peers")
			got := occupantFromProc(procRoot, tc.want, 17443)
			if tc.matches != (len(got) == 1) {
				t.Fatalf("occupantFromProc = %+v, want matched=%v", got, tc.matches)
			}
		})
	}
}

// TestOccupantFromProcSkipsUnreadableDescriptors checks that a process whose fd
// directory cannot be read is skipped without hiding another process that holds
// the same socket and can be read.
func TestOccupantFromProcSkipsUnreadableDescriptors(t *testing.T) {
	procRoot := t.TempDir()
	const header = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"
	line := "   0: 0100007F:4423 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 424242 1\n"
	writeProcFile(t, procRoot, "net/tcp", header+line)
	writeProcFile(t, procRoot, "net/tcp6", header)
	// The first process directory exists but its fd directory does not, as
	// when /proc/<pid>/fd cannot be read.
	if err := os.MkdirAll(filepath.Join(procRoot, "1234"), 0o755); err != nil {
		t.Fatal(err)
	}
	// A second process holds the same socket and can be read.
	fdDir := filepath.Join(procRoot, "5678", "fd")
	if err := os.MkdirAll(fdDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("socket:[424242]", filepath.Join(fdDir, "3")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/usr/local/bin/peers", filepath.Join(procRoot, "5678", "exe")); err != nil {
		t.Fatal(err)
	}

	got := occupantFromProc(procRoot, net.ParseIP("127.0.0.1"), 17443)
	if len(got) != 1 || got[0].pid != 5678 || got[0].path != "/usr/local/bin/peers" {
		t.Fatalf("occupantFromProc = %+v, want the readable process 5678 at /usr/local/bin/peers", got)
	}
}

// TestOccupantFromProcReportsEveryOccupant checks that every process holding
// the listening socket is named, in pid order, instead of the first one found.
func TestOccupantFromProcReportsEveryOccupant(t *testing.T) {
	procRoot := t.TempDir()
	const header = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"
	line := "   0: 0100007F:4423 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 424242 1\n"
	writeProcFile(t, procRoot, "net/tcp", header+line)
	writeProcFile(t, procRoot, "net/tcp6", header)
	for _, holder := range []struct {
		pid string
		exe string
	}{
		{"5678", "/usr/local/bin/peers"},
		{"1234", "/opt/old/bin/agent-peer"},
	} {
		fdDir := filepath.Join(procRoot, holder.pid, "fd")
		if err := os.MkdirAll(fdDir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("socket:[424242]", filepath.Join(fdDir, "3")); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(holder.exe, filepath.Join(procRoot, holder.pid, "exe")); err != nil {
			t.Fatal(err)
		}
	}

	got := occupantFromProc(procRoot, net.ParseIP("127.0.0.1"), 17443)
	want := []listenOccupant{
		{pid: 1234, path: "/opt/old/bin/agent-peer"},
		{pid: 5678, path: "/usr/local/bin/peers"},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("occupantFromProc = %+v, want every holder in pid order %+v", got, want)
	}
	if desc := describeOccupants(got); desc != ` by "/opt/old/bin/agent-peer" (pid 1234) and "/usr/local/bin/peers" (pid 5678)` {
		t.Errorf("description = %q, want both holders joined without an Oxford comma", desc)
	}
}

// TestJoinAnd checks the user-facing list join: no Oxford comma.
func TestJoinAnd(t *testing.T) {
	cases := []struct {
		in   []string
		want string
	}{
		{[]string{"a"}, "a"},
		{[]string{"a", "b"}, "a and b"},
		{[]string{"a", "b", "c"}, "a, b and c"},
	}
	for _, tc := range cases {
		if got := joinAnd(tc.in); got != tc.want {
			t.Errorf("joinAnd(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestLsofPID checks that every process id column of lsof's table output is
// read, in order and without duplicates.
func TestLsofPID(t *testing.T) {
	out := "COMMAND  PID USER   FD   TYPE             DEVICE SIZE/OFF NODE NAME\n" +
		"peers   4321 alice  5u  IPv4 0x1234567890abcdef      0t0  TCP *:17443 (LISTEN)\n" +
		"peers   8765 bob    6u  IPv4 0x1234567890abcdef      0t0  TCP *:17443 (LISTEN)\n" +
		"peers   4321 alice  7u  IPv6 0x1234567890abcdef      0t0  TCP *:17443 (LISTEN)\n"
	if pids := lsofPID(out); !slices.Equal(pids, []int{4321, 8765}) {
		t.Fatalf("lsofPID = %v, want [4321 8765]", pids)
	}
}

// TestLsofTextPath checks the executable path in lsof's field output.
func TestLsofTextPath(t *testing.T) {
	cases := []struct {
		name string
		out  string
		want string
	}{
		{"executable", "p4321\nf7\nn/usr/local/bin/peers\n", "/usr/local/bin/peers"},
		{"no executable", "p4321\nf7\n", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := lsofTextPath(tc.out); got != tc.want {
				t.Errorf("lsofTextPath = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestOccupantFromLsofParsesExecutablePath checks that the macOS path uses
// absolute lsof paths, asks lsof for the executable with -d txt -Fn, and parses
// only its standard output.
func TestOccupantFromLsofParsesExecutablePath(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	var calls [][]string
	opts.RunOutput = func(name string, args ...string) ([]byte, error) {
		calls = append(calls, append([]string{name}, args...))
		if len(args) > 0 && args[0] == "-nP" {
			return []byte("COMMAND  PID USER   FD   TYPE             DEVICE SIZE/OFF NODE NAME\n" +
				"peers   4321 alice  5u  IPv4 0x1234567890abcdef      0t0  TCP *:17443 (LISTEN)\n"), nil
		}
		return []byte("p4321\nf7\nn/usr/local/bin/peers\n"), nil
	}

	got := findListenOccupant(opts, "127.0.0.1:17443")
	if len(got) != 1 || got[0].pid != 4321 || got[0].path != "/usr/local/bin/peers" {
		t.Fatalf("findListenOccupant = %+v, want pid 4321 at /usr/local/bin/peers", got)
	}
	if len(calls) != 2 {
		t.Fatalf("commands = %v, want two lsof runs", calls)
	}
	for _, call := range calls {
		if call[0] != "/usr/sbin/lsof" {
			t.Errorf("command = %q, want the absolute lsof path", call[0])
		}
	}
	wantArgs := []string{"-a", "-p", "4321", "-d", "txt", "-Fn"}
	if !slices.Equal(calls[1][1:], wantArgs) {
		t.Errorf("second lsof args = %v, want %v", calls[1][1:], wantArgs)
	}
}

// TestOccupantFromLsofFallsBackWithoutTheExecutable checks that a failed
// executable lookup still reports the pid and never fails the installation.
func TestOccupantFromLsofFallsBackWithoutTheExecutable(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.RunOutput = func(_ string, args ...string) ([]byte, error) {
		if len(args) > 0 && args[0] == "-nP" {
			return []byte("COMMAND  PID USER   FD   TYPE             DEVICE SIZE/OFF NODE NAME\n" +
				"peers   4321 alice  5u  IPv4 0x1234567890abcdef      0t0  TCP *:17443 (LISTEN)\n"), nil
		}
		return nil, errors.New("lsof failed")
	}

	got := findListenOccupant(opts, "127.0.0.1:17443")
	if len(got) != 1 || got[0].pid != 4321 || got[0].path != "" {
		t.Fatalf("findListenOccupant = %+v, want pid 4321 without a path", got)
	}
	if desc := describeOccupants(got); desc != " by the process with pid 4321" {
		t.Errorf("description = %q, want the pid without a path", desc)
	}
}

// TestOccupantFromLsofReportsEveryListener checks that every pid lsof lists as
// listening on the port is reported with its executable, not only the first.
func TestOccupantFromLsofReportsEveryListener(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.RunOutput = func(_ string, args ...string) ([]byte, error) {
		if len(args) > 0 && args[0] == "-nP" {
			return []byte("COMMAND  PID USER   FD   TYPE             DEVICE SIZE/OFF NODE NAME\n" +
				"peers   4321 alice  5u  IPv4 0x1234567890abcdef      0t0  TCP *:17443 (LISTEN)\n" +
				"old     8765 bob    6u  IPv4 0x1234567890abcdef      0t0  TCP *:17443 (LISTEN)\n"), nil
		}
		for i, arg := range args {
			if arg != "-p" || i+1 >= len(args) {
				continue
			}
			switch args[i+1] {
			case "4321":
				return []byte("p4321\nf7\nn/usr/local/bin/peers\n"), nil
			case "8765":
				return []byte("p8765\nf7\nn/opt/old/bin/agent-peer\n"), nil
			}
		}
		return nil, errors.New("unexpected lsof call")
	}

	got := findListenOccupant(opts, "127.0.0.1:17443")
	want := []listenOccupant{
		{pid: 4321, path: "/usr/local/bin/peers"},
		{pid: 8765, path: "/opt/old/bin/agent-peer"},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("findListenOccupant = %+v, want every listener %+v", got, want)
	}
}

// TestHumanDuration checks that the timeout lines read as whole seconds instead
// of printing a time.Duration, rounding any part of a second up to 1 second.
func TestHumanDuration(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{20 * time.Millisecond, "1 second"},
		{time.Second, "1 second"},
		{1500 * time.Millisecond, "2 seconds"},
		{20 * time.Second, "20 seconds"},
	}
	for _, tc := range cases {
		if got := humanDuration(tc.d); got != tc.want {
			t.Errorf("humanDuration(%v) = %q, want %q", tc.d, got, tc.want)
		}
	}
}

// writeProcListener writes a stand-in /proc tree with one listening socket in
// table (tcp or tcp6) at localHex:port, held by pid through inode.
func writeProcListener(t *testing.T, procRoot, table, localHex string, port, pid int, inode, exe string) {
	t.Helper()
	const header = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"
	line := fmt.Sprintf("   0: %s:%04X 00000000000000000000000000000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 %s 1\n", localHex, port, inode)
	writeProcFile(t, procRoot, "net/"+table, header+line)
	other := "tcp6"
	if table == "tcp6" {
		other = "tcp"
	}
	writeProcFile(t, procRoot, "net/"+other, header)
	fdDir := filepath.Join(procRoot, strconv.Itoa(pid), "fd")
	if err := os.MkdirAll(fdDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("socket:["+inode+"]", filepath.Join(fdDir, "3")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(exe, filepath.Join(procRoot, strconv.Itoa(pid), "exe")); err != nil {
		t.Fatal(err)
	}
}

// writeProcFile writes one file into a stand-in /proc tree.
func writeProcFile(t *testing.T, procRoot, name, content string) {
	t.Helper()
	path := filepath.Join(procRoot, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestInstallReinstallsOverItsOwnRunningService(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	addr := ln.Addr().String()

	opts := baseOptions(t)
	writeListenConfig(t, opts, addr)
	var cmds []string
	held := true
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		switch {
		case slices.Contains(args, "stop"):
			if held {
				_ = ln.Close()
				held = false
			}
		case slices.Contains(args, "show"):
			return []byte("4242\n"), nil
		}
		return nil, nil
	}
	// The service's own Broker answers on the local socket with the pid the
	// service manager reports, so the occupied address is this service's.
	opts.Probe = func(string) (int, bool) { return 4242, true }
	opts.ListenProbe = config.ProbeListen

	if _, err := Install(opts); err != nil {
		t.Fatalf("reinstall over a running service: %v", err)
	}
	stop := commandIndex(cmds, "systemctl --user stop "+UnitName)
	if stop < 0 {
		t.Fatalf("commands = %q, want the running service stopped before the listen precheck", cmds)
	}
	if enable := commandIndex(cmds, "systemctl --user enable "+UnitName); enable >= 0 && stop > enable {
		t.Errorf("commands = %q, want stop before enable", cmds)
	}
	if held {
		t.Error("the occupying listener was not stopped")
	}
}

// TestInstallDarwinReinstallWaitsForTheOldServiceToExit checks that a macOS
// reinstall waits for its own Broker to release broker.listen after launchd has
// unloaded the job. bootout returns before the old process has exited; the wait
// must outlast that delay instead of mistaking the old Broker for a foreign
// occupant.
func TestInstallDarwinReinstallWaitsForTheOldServiceToExit(t *testing.T) {
	opts := darwinUserReinstall(t)
	writeListenConfig(t, opts, "127.0.0.1:17448")
	// The fake keeps the old Broker's address busy for releaseDelay. The stop
	// budget must outlast that with the package fallback, so a loaded runner
	// cannot expire the budget before the release is observed; the delay, not the
	// budget, is what the test asserts.
	const releaseDelay = 1500 * time.Millisecond
	opts.StopTimeout = releaseDelay + processExitWait

	var cmds []string
	var bootedOut time.Time
	held := true
	started := false
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		switch {
		case len(args) == 2 && args[0] == "print" && args[1] == "gui/501":
			return []byte("com.apple.launchd\n"), nil
		case len(args) == 2 && args[0] == "print":
			switch {
			case started:
				return []byte("state = running\n\tpid = 5252\n"), nil
			case bootedOut.IsZero():
				return []byte("state = running\n\tpid = 4242\n"), nil
			}
			return []byte("Could not find service"), exitError(t, 113)
		case len(args) > 0 && args[0] == "bootout":
			if bootedOut.IsZero() {
				bootedOut = time.Now()
			}
		case len(args) > 0 && args[0] == "bootstrap":
			started = true
		}
		return nil, nil
	}
	opts.Probe = func(string) (int, bool) {
		if started {
			return 5252, true
		}
		return 4242, true
	}
	opts.ListenProbe = func(addr string) error {
		if held && !bootedOut.IsZero() && time.Since(bootedOut) >= releaseDelay {
			held = false
		}
		if held {
			return fmt.Errorf("cannot bind %q: %w", addr, syscall.EADDRINUSE)
		}
		return nil
	}

	if _, err := Install(opts); err != nil {
		t.Fatalf("reinstall after the old Broker needed 1.5s to release the address: %v", err)
	}
	if bootedOut.IsZero() {
		t.Fatalf("commands = %q, want the reinstall to boot out the running service", cmds)
	}
	if got := countCommandBefore(cmds, darwinBootoutCmd, darwinBootstrapCmd); got != 1 {
		t.Errorf("bootout calls before the bootstrap = %d, want 1; commands = %q", got, cmds)
	}
	if held {
		t.Error("the reinstall did not wait for the old Broker to release broker.listen")
	}
}

// TestInstallReportsAStoppedServiceWhenTheAddressStaysBusy checks the message
// of the one remaining path that stops a running service and still fails: the
// service owned the address, but it did not release it within the bound. The
// error must say the existing service was stopped, give a recovery command,
// and the failure must leave the unit unable to start at boot.
func TestInstallReportsAStoppedServiceWhenTheAddressStaysBusy(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	addr := ln.Addr().String()

	opts := baseOptions(t)
	writeListenConfig(t, opts, addr)
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		if slices.Contains(args, "show") {
			return []byte("4242\n"), nil
		}
		return nil, nil
	}
	opts.Probe = func(string) (int, bool) { return 4242, true }
	opts.ListenProbe = config.ProbeListen
	opts.StopTimeout = 30 * time.Millisecond

	_, err = Install(opts)
	if err == nil {
		t.Fatal("Install succeeded while broker.listen stayed occupied")
	}
	for _, want := range []string{
		"already in use",
		"the existing service was stopped",
		"systemctl --user enable --now peers.service",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	if cmd := "systemctl --user disable --now " + UnitName; !slices.Contains(cmds, cmd) {
		t.Errorf("commands = %q, want %q so the stopped service does not start at boot", cmds, cmd)
	}
}

func TestInstallReportsTheStartupFailureOnTimeout(t *testing.T) {
	const reason = "listen tcp :17443: bind: address already in use"
	fakeRun := func(name string, args ...string) ([]byte, error) {
		switch {
		case name == "journalctl":
			return []byte(fmt.Sprintf("{\"time\":%q,\"event\":\"startup_failed\",\"error\":%q}\n", time.Now().UTC().Format(time.RFC3339Nano), reason)), nil
		case slices.Contains(args, "show"):
			return []byte("4242\n"), nil
		}
		return nil, nil
	}

	t.Run("linux user", func(t *testing.T) {
		opts := baseOptions(t)
		opts.Run = fakeRun
		opts.Probe = func(string) (int, bool) { return 0, false }
		opts.ReadyTimeout = 20 * time.Millisecond

		_, err := Install(opts)
		if err == nil || !strings.Contains(err.Error(), "cannot reach the Broker") {
			t.Fatalf("Install error = %v, want the readiness timeout", err)
		}
		if !strings.Contains(err.Error(), reason) {
			t.Errorf("error %q does not carry the last startup_failed reason %q", err, reason)
		}
	})

	t.Run("darwin user log file", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "darwin"
		opts.EUID = 501
		opts.UID = 501
		opts.Home = "/Users/alice"
		logPath := filepath.Join(opts.Root, "Users/alice/Library/Logs/peers/broker.log")
		if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
			t.Fatal(err)
		}
		opts.Run = func(name string, args ...string) ([]byte, error) {
			switch {
			case len(args) == 2 && args[0] == "print" && args[1] == "gui/501":
				return []byte("com.apple.launchd\n"), nil
			case len(args) == 2 && args[0] == "print":
				return []byte("Could not find service"), exitError(t, 113)
			case len(args) > 0 && args[0] == "bootout":
				return nil, exitError(t, 3)
			case len(args) > 0 && args[0] == "bootstrap":
				// The Broker writes the record while it starts, after the
				// installation began.
				now := time.Now().UTC().Format(time.RFC3339Nano)
				line := fmt.Sprintf("{\"time\":%q,\"event\":\"broker_started\"}\n{\"time\":%q,\"event\":\"startup_failed\",\"error\":%q}\n", now, now, reason)
				if err := os.WriteFile(logPath, []byte(line), 0o600); err != nil {
					t.Fatalf("writing %s: %v", logPath, err)
				}
			}
			return nil, nil
		}
		opts.Probe = func(string) (int, bool) { return 0, false }
		opts.ReadyTimeout = 20 * time.Millisecond

		_, err := Install(opts)
		if err == nil || !strings.Contains(err.Error(), reason) {
			t.Fatalf("Install error = %v, want the readiness failure with %q", err, reason)
		}
	})
}

// TestInstallIgnoresStartupFailuresBeforeTheInstallStarted checks that a
// readiness timeout reports only records written after this installation
// began. An old startup_failed record describes an earlier run and would send
// the operator after a cause that no longer applies.
func TestInstallIgnoresStartupFailuresBeforeTheInstallStarted(t *testing.T) {
	const oldReason = "listen tcp :17443: bind: address already in use (earlier run)"
	old := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)

	t.Run("linux user", func(t *testing.T) {
		opts := baseOptions(t)
		opts.Run = func(name string, args ...string) ([]byte, error) {
			switch {
			case name == "journalctl":
				if !slices.Contains(args, "--since") {
					t.Errorf("journalctl arguments = %q, want a --since bound", args)
				}
				return []byte(fmt.Sprintf("{\"time\":%q,\"event\":\"startup_failed\",\"error\":%q}\n", old, oldReason)), nil
			case slices.Contains(args, "show"):
				return []byte("4242\n"), nil
			}
			return nil, nil
		}
		opts.Probe = func(string) (int, bool) { return 0, false }
		opts.ReadyTimeout = 20 * time.Millisecond

		_, err := Install(opts)
		if err == nil || !strings.Contains(err.Error(), "cannot reach the Broker") {
			t.Fatalf("Install error = %v, want the readiness timeout", err)
		}
		if strings.Contains(err.Error(), oldReason) {
			t.Errorf("error %q carries a startup_failed record from before the install", err)
		}
	})

	t.Run("darwin user log file", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "darwin"
		opts.EUID = 501
		opts.UID = 501
		opts.Home = "/Users/alice"
		logPath := filepath.Join(opts.Root, "Users/alice/Library/Logs/peers/broker.log")
		if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
			t.Fatal(err)
		}
		line := fmt.Sprintf("{\"time\":%q,\"event\":\"startup_failed\",\"error\":%q}\n", old, oldReason)
		if err := os.WriteFile(logPath, []byte(line), 0o600); err != nil {
			t.Fatal(err)
		}
		opts.Run = func(name string, args ...string) ([]byte, error) {
			switch {
			case len(args) == 2 && args[0] == "print" && args[1] == "gui/501":
				return []byte("com.apple.launchd\n"), nil
			case len(args) == 2 && args[0] == "print":
				return []byte("Could not find service"), exitError(t, 113)
			case len(args) > 0 && args[0] == "bootout":
				return nil, exitError(t, 3)
			}
			return nil, nil
		}
		opts.Probe = func(string) (int, bool) { return 0, false }
		opts.ReadyTimeout = 20 * time.Millisecond

		_, err := Install(opts)
		if err == nil || !strings.Contains(err.Error(), "cannot reach the Broker") {
			t.Fatalf("Install error = %v, want the readiness timeout", err)
		}
		if strings.Contains(err.Error(), oldReason) {
			t.Errorf("error %q carries a startup_failed record from before the install", err)
		}
	})
}

func TestInstallDisablesAfterAReadinessFailure(t *testing.T) {
	t.Run("linux user", func(t *testing.T) {
		opts := baseOptions(t)
		var cmds []string
		opts.Run = func(name string, args ...string) ([]byte, error) {
			cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
			if slices.Contains(args, "show") {
				return []byte("4242\n"), nil
			}
			return nil, nil
		}
		opts.Probe = func(string) (int, bool) { return 0, false }
		opts.ReadyTimeout = 20 * time.Millisecond

		_, err := Install(opts)
		if err == nil || !strings.Contains(err.Error(), "cannot reach the Broker") {
			t.Fatalf("Install error = %v, want the readiness timeout", err)
		}
		for _, want := range []string{"stopped and disabled", "run service install again"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not contain %q", err, want)
			}
		}
		if cmd := "systemctl --user disable --now " + UnitName; !slices.Contains(cmds, cmd) {
			t.Errorf("commands = %q, want %q after the readiness failure", cmds, cmd)
		}
		unit := rooted(opts, "/home/alice/.config/systemd/user/"+UnitName)
		if _, statErr := os.Stat(unit); statErr != nil {
			t.Errorf("unit file was not kept after the failure: %v", statErr)
		}
		configPath := rooted(opts, configFilePath(opts))
		if _, statErr := os.Stat(configPath); statErr != nil {
			t.Errorf("configuration file was not kept after the failure: %v", statErr)
		}
	})

	t.Run("darwin user", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "darwin"
		opts.EUID = 501
		opts.UID = 501
		opts.Home = "/Users/alice"
		bootouts := 0
		disables := 0
		var cmds []string
		opts.Run = func(name string, args ...string) ([]byte, error) {
			cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
			switch {
			case len(args) == 2 && args[0] == "print" && args[1] == "gui/501":
				return []byte("com.apple.launchd\n"), nil
			case len(args) == 2 && args[0] == "print":
				return []byte("Could not find service"), exitError(t, 113)
			case len(args) > 0 && args[0] == "bootout":
				bootouts++
				return nil, exitError(t, 3)
			case len(args) > 0 && args[0] == "disable":
				disables++
			}
			return nil, nil
		}
		opts.Probe = func(string) (int, bool) { return 0, false }
		opts.ReadyTimeout = 20 * time.Millisecond

		_, err := Install(opts)
		if err == nil || !strings.Contains(err.Error(), "stopped and disabled") {
			t.Fatalf("Install error = %v, want the readiness failure with the disabled note", err)
		}
		bootstrapIndex := slices.Index(cmds, darwinBootstrapCmd)
		if bootstrapIndex < 0 {
			t.Fatalf("commands = %q, want the replacement bootstrap", cmds)
		}
		if got := countCommand(cmds[:bootstrapIndex], darwinBootoutCmd); got != 1 {
			t.Errorf("bootout calls before the bootstrap = %d, want 1; commands = %q", got, cmds)
		}
		if bootouts != 2 {
			t.Errorf("bootout calls = %d, want one before the start and one after the failure; commands = %q", bootouts, cmds)
		}
		// launchd reloads every plist under LaunchAgents at login, so a kept
		// plist must be disabled or the failed install starts a Broker that
		// cannot bind.
		if disables != 1 {
			t.Errorf("launchctl disable calls = %d, want 1 after the failure", disables)
		}
		cleanup := cmds[bootstrapIndex+1:]
		bootoutRel := slices.Index(cleanup, darwinBootoutCmd)
		disableRel := slices.Index(cleanup, darwinDisableCmd)
		if bootoutRel < 0 || disableRel < 0 || bootoutRel > disableRel {
			t.Errorf("commands = %q, want the failure cleanup to boot out and then disable", cmds)
		}
		plist := rooted(opts, "/Users/alice/Library/LaunchAgents/"+Label+".plist")
		if _, statErr := os.Stat(plist); statErr != nil {
			t.Errorf("plist was not kept after the failure: %v", statErr)
		}
	})
}

// TestInstallReportsACleanupFailure checks the error of abandonInstall when
// disabling the service it registered also fails. The installation must say
// that the service may still start automatically instead of implying it is
// safe from autostart.
func TestInstallReportsACleanupFailure(t *testing.T) {
	opts := baseOptions(t)
	opts.Run = func(name string, args ...string) ([]byte, error) {
		switch {
		case slices.Contains(args, "show"):
			return []byte("4242\n"), nil
		case slices.Contains(args, "disable"):
			return []byte("disable failed"), errors.New("exit status 1")
		}
		return nil, nil
	}
	opts.Probe = func(string) (int, bool) { return 0, false }
	opts.ReadyTimeout = 20 * time.Millisecond

	_, err := Install(opts)
	if err == nil || !strings.Contains(err.Error(), "cannot reach the Broker") {
		t.Fatalf("Install error = %v, want the readiness timeout", err)
	}
	for _, want := range []string{"could not be disabled", "may start automatically", "disable failed"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

// TestInstallReportsANonAddressInUseProbeError checks the listenError branch
// for a bind failure that is not an occupied address, such as a privileged
// port or a malformed interface. The error must name the bind failure and the
// advice, and must not touch the service.
func TestInstallReportsANonAddressInUseProbeError(t *testing.T) {
	opts := baseOptions(t)
	writeListenConfig(t, opts, "127.0.0.1:17448")
	opts.ListenProbe = func(string) error { return errors.New("permission denied") }
	opts.Probe = func(string) (int, bool) { return 0, false }
	var cmds []string
	opts.Run = recordCommands(&cmds)

	_, err := Install(opts)
	if err == nil {
		t.Fatal("Install succeeded although broker.listen could not be bound")
	}
	for _, want := range []string{
		"cannot bind broker.listen",
		"permission denied",
		"change broker.listen to another port",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	if len(cmds) != 0 {
		t.Errorf("commands = %q, want none after a non-conflict bind failure", cmds)
	}
}

func TestInstallOverwritesUnit(t *testing.T) {
	opts := baseOptions(t)
	var cmds []string
	started := false
	opts.Run = recordCommandsUntil(&cmds, &started, "restart")
	opts.Probe = probeAfterStart(&started, 4242)
	path := filepath.Join(opts.Root, "home/alice/.config/systemd/user/peers.service")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("stale unit"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := Install(opts); err != nil {
		t.Fatalf("Install: %v", err)
	}
	want, err := SystemdUserUnit(opts.Exe)
	if err != nil {
		t.Fatal(err)
	}
	if got := mustReadFile(t, path); got != want {
		t.Errorf("stale unit was not replaced: %q", got)
	}
}

// TestInstallReportsDegradedAfterReady checks the check that runs once the
// service answers: a Broker that reports a degraded state leaves the service
// installed and running, returns *DegradedError with a complete Result, and is
// never disabled or stopped.
func TestInstallReportsDegradedAfterReady(t *testing.T) {
	cases := []struct {
		name  string
		state DegradedState
	}{
		{
			name: "local only",
			state: DegradedState{
				Level:  localpb.DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY,
				Step:   "listen",
				Reason: "listen tcp :17443: bind: address already in use",
				Fix:    "change broker.listen to another port",
			},
		},
		{
			name: "diagnostics only",
			state: DegradedState{
				Level:  localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY,
				Step:   "audit",
				Reason: "the audit log is not writable",
				Fix:    "fix the permissions of ~/.peers/audit.log",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := baseOptions(t)
			var cmds []string
			started := false
			opts.Run = recordCommandsUntil(&cmds, &started, "restart")
			opts.Probe = probeAfterStart(&started, 4242)
			opts.StatusProbe = func(string) (DegradedState, bool, error) {
				return tc.state, true, nil
			}

			res, err := Install(opts)
			var degraded *DegradedError
			if !errors.As(err, &degraded) {
				t.Fatalf("Install error = %v, want *DegradedError", err)
			}
			if degraded.State != tc.state {
				t.Errorf("DegradedError.State = %+v, want %+v", degraded.State, tc.state)
			}
			if res.UnitPath != "/home/alice/.config/systemd/user/peers.service" {
				t.Errorf("UnitPath = %q", res.UnitPath)
			}
			if res.ConfigPath != "/home/alice/.peers/config.yaml" {
				t.Errorf("ConfigPath = %q", res.ConfigPath)
			}
			if res.Binary != opts.Exe {
				t.Errorf("Binary = %q, want %q", res.Binary, opts.Exe)
			}
			if want := "journalctl --user -u peers.service"; res.RunLog != want {
				t.Errorf("RunLog = %q, want %q", res.RunLog, want)
			}
			if want := "/home/alice/.peers/audit.log"; res.AuditLog != want {
				t.Errorf("AuditLog = %q, want %q", res.AuditLog, want)
			}
			for _, cmd := range cmds {
				for _, verb := range []string{"disable", "stop"} {
					if slices.Contains(strings.Fields(cmd), verb) {
						t.Errorf("commands = %q, want the running service left alone after the degraded answer", cmds)
					}
				}
			}
		})
	}
}

// TestInstallDarwinReportsDegradedAfterReady checks the same result on macOS:
// the domain and the level's log paths are filled, and no cleanup disable runs.
func TestInstallDarwinReportsDegradedAfterReady(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.EUID = 501
	opts.UID = 501
	opts.Home = "/Users/alice"
	var cmds []string
	started := false
	bootedOut := false
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		switch {
		case len(args) == 2 && args[0] == "print" && args[1] == "gui/501":
			return []byte("com.apple.launchd\n"), nil
		case len(args) == 2 && args[0] == "print" && args[1] == "gui/501/"+Label:
			if bootedOut && !started {
				return []byte("Could not find service \"" + Label + "\" in domain for user"), exitError(t, 113)
			}
			return []byte("state = running\n\tpid = 4242\n"), nil
		case len(args) > 0 && args[0] == "bootout":
			bootedOut = true
			return nil, exitError(t, 3)
		case len(args) > 0 && args[0] == "bootstrap":
			started = true
		}
		return nil, nil
	}
	opts.Probe = probeAfterStart(&started, 4242)
	state := DegradedState{Level: localpb.DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY, Reason: "listener unavailable", Fix: "fix broker.listen"}
	opts.StatusProbe = func(string) (DegradedState, bool, error) { return state, true, nil }

	res, err := Install(opts)
	var degraded *DegradedError
	if !errors.As(err, &degraded) {
		t.Fatalf("Install error = %v, want *DegradedError", err)
	}
	if res.Domain != "gui/501" {
		t.Errorf("Domain = %q, want gui/501", res.Domain)
	}
	if res.Binary != opts.Exe {
		t.Errorf("Binary = %q, want %q", res.Binary, opts.Exe)
	}
	if want := "/Users/alice/Library/Logs/peers/broker.log"; res.RunLog != want {
		t.Errorf("RunLog = %q, want %q", res.RunLog, want)
	}
	if want := "/Users/alice/.peers/audit.log"; res.AuditLog != want {
		t.Errorf("AuditLog = %q, want %q", res.AuditLog, want)
	}
	if cmd := "launchctl disable gui/501/" + Label; slices.Contains(cmds, cmd) {
		t.Errorf("commands = %q, want no %q after the degraded answer", cmds, cmd)
	}
}

// TestInstallRetriesTheStatusProbe checks that a status probe that fails right
// after readiness is retried within the readiness bound instead of failing the
// installation on its first attempt.
func TestInstallRetriesTheStatusProbe(t *testing.T) {
	opts := baseOptions(t)
	var cmds []string
	started := false
	opts.Run = recordCommandsUntil(&cmds, &started, "restart")
	opts.Probe = probeAfterStart(&started, 4242)
	opts.ReadyTimeout = time.Second
	calls := 0
	opts.StatusProbe = func(string) (DegradedState, bool, error) {
		calls++
		if calls < 3 {
			return DegradedState{}, false, errors.New("the status is not available yet")
		}
		return DegradedState{}, false, nil
	}

	if _, err := Install(opts); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if calls < 3 {
		t.Errorf("status probes = %d, want the check to retry", calls)
	}
}

// TestInstallFailsWhenTheStatusCannotBeRead checks the failure that is not a
// degraded state: the service answers but its status cannot be read. The
// installation fails without disabling the running service, and the error tells
// the operator the service is running and where to look.
func TestInstallFailsWhenTheStatusCannotBeRead(t *testing.T) {
	opts := baseOptions(t)
	var cmds []string
	started := false
	opts.Run = recordCommandsUntil(&cmds, &started, "restart")
	opts.Probe = probeAfterStart(&started, 4242)
	opts.ReadyTimeout = 20 * time.Millisecond
	opts.StatusProbe = func(string) (DegradedState, bool, error) {
		return DegradedState{}, false, errors.New("connection reset")
	}

	res, err := Install(opts)
	if err == nil {
		t.Fatal("Install succeeded although the status could not be read")
	}
	var degraded *DegradedError
	if errors.As(err, &degraded) {
		t.Fatalf("Install error = %v, want a plain error, not *DegradedError", err)
	}
	for _, want := range []string{"running service", "cannot read the status", "peers service status", "connection reset"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	if res.Binary == "" || res.AuditLog == "" {
		t.Errorf("Result after a status failure = %+v, want the completed fields", res)
	}
	for _, cmd := range cmds {
		if slices.Contains(strings.Fields(cmd), "disable") {
			t.Errorf("commands = %q, want the answered service left running", cmds)
		}
	}
}

// TestInstallReportsTheServiceManagerStateOnTimeout checks the readiness
// timeout when the service log has no startup_failed record written by this
// installation. The error must carry what the service manager reports about
// the service instead of only the silent socket.
func TestInstallReportsTheServiceManagerStateOnTimeout(t *testing.T) {
	t.Run("linux user", func(t *testing.T) {
		opts := baseOptions(t)
		opts.Run = func(name string, args ...string) ([]byte, error) {
			switch {
			case slices.Contains(args, "ActiveState"):
				return []byte("ActiveState=failed\nSubState=failed\nExecMainStatus=1\n"), nil
			case slices.Contains(args, "show"):
				return []byte("4242\n"), nil
			case name == "journalctl":
				return nil, nil
			}
			return nil, nil
		}
		opts.Probe = func(string) (int, bool) { return 0, false }
		opts.ReadyTimeout = 20 * time.Millisecond

		_, err := Install(opts)
		if err == nil || !strings.Contains(err.Error(), "cannot reach the Broker") {
			t.Fatalf("Install error = %v, want the readiness timeout", err)
		}
		for _, want := range []string{"ActiveState=failed", "SubState=failed", "ExecMainStatus=1"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not carry %q", err, want)
			}
		}
	})

	t.Run("darwin user", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "darwin"
		opts.EUID = 501
		opts.UID = 501
		opts.Home = "/Users/alice"
		bootedOut := false
		started := false
		opts.Run = func(name string, args ...string) ([]byte, error) {
			switch {
			case len(args) == 2 && args[0] == "print" && args[1] == "gui/501":
				return []byte("com.apple.launchd\n"), nil
			case len(args) == 2 && args[0] == "print" && args[1] == "gui/501/"+Label:
				if bootedOut && !started {
					return []byte("Could not find service \"" + Label + "\" in domain for user"), exitError(t, 113)
				}
				return []byte("state = exited\n\tlast exit code = 1\n"), nil
			case len(args) > 0 && args[0] == "bootout":
				bootedOut = true
				return nil, exitError(t, 3)
			case len(args) > 0 && args[0] == "bootstrap":
				started = true
			}
			return nil, nil
		}
		opts.Probe = func(string) (int, bool) { return 0, false }
		opts.ReadyTimeout = 20 * time.Millisecond

		_, err := Install(opts)
		if err == nil || !strings.Contains(err.Error(), "cannot reach the Broker") {
			t.Fatalf("Install error = %v, want the readiness timeout", err)
		}
		for _, want := range []string{"state = exited", "last exit code = 1"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not carry %q", err, want)
			}
		}
	})

	t.Run("query failure", func(t *testing.T) {
		opts := baseOptions(t)
		opts.Run = func(name string, args ...string) ([]byte, error) {
			switch {
			case slices.Contains(args, "ActiveState"):
				return []byte("Failed to connect to bus: No such file or directory"), errors.New("exit status 1")
			case slices.Contains(args, "show"):
				return []byte("4242\n"), nil
			case name == "journalctl":
				return nil, nil
			}
			return nil, nil
		}
		opts.Probe = func(string) (int, bool) { return 0, false }
		opts.ReadyTimeout = 20 * time.Millisecond

		_, err := Install(opts)
		if err == nil || !strings.Contains(err.Error(), "cannot reach the Broker") {
			t.Fatalf("Install error = %v, want the readiness timeout", err)
		}
		for _, want := range []string{"service manager state could not be read", "ActiveState", "Failed to connect to bus"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not carry %q", err, want)
			}
		}
	})
}

// TestServiceManagerStateQueryArguments checks the query the readiness timeout
// makes: the user installation asks systemd for this account's unit with --user, and
// the system installation asks about the system unit without it.
func TestServiceManagerStateQueryArguments(t *testing.T) {
	cases := []struct {
		name     string
		system   bool
		wantUser bool
	}{
		{name: "user installation", system: false, wantUser: true},
		{name: "system installation", system: true, wantUser: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := baseOptions(t)
			opts.System = tc.system
			if tc.system {
				opts.EUID = 0
			}
			var activeArgs []string
			opts.Run = func(name string, args ...string) ([]byte, error) {
				switch {
				case slices.Contains(args, "ActiveState"):
					activeArgs = append([]string(nil), args...)
					return []byte("ActiveState=failed\nSubState=failed\nExecMainStatus=1\n"), nil
				case slices.Contains(args, "show"):
					return []byte("4242\n"), nil
				case name == "journalctl":
					return nil, nil
				}
				return nil, nil
			}
			opts.Probe = func(string) (int, bool) { return 0, false }
			opts.ReadyTimeout = 20 * time.Millisecond

			_, err := Install(opts)
			if err == nil || !strings.Contains(err.Error(), "the service manager reports") {
				t.Fatalf("Install error = %v, want the readiness timeout with the service manager state", err)
			}
			if activeArgs == nil {
				t.Fatal("the readiness timeout did not query the service manager state")
			}
			if got := slices.Contains(activeArgs, "--user"); got != tc.wantUser {
				t.Errorf("systemctl state query arguments = %q, --user present = %v, want %v", activeArgs, got, tc.wantUser)
			}
		})
	}
}

func TestInstallUnsupportedOS(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "freebsd"
	_, err := Install(opts)
	const want = `cannot install the service on platform "freebsd"`
	if err == nil || err.Error() != want {
		t.Fatalf("Install error = %v, want %q", err, want)
	}
}

// darwinUserReinstall returns darwin user options for a service that is
// already installed and loaded. A test's Run decides when launchd finishes
// unloading it.
func darwinUserReinstall(t *testing.T) Options {
	t.Helper()
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.EUID = 501
	opts.UID = 501
	opts.Home = "/Users/alice"
	// A reinstall finds the plist an earlier install wrote. The install records
	// that it existed, so a failure keeps it and disables its label.
	plist := filepath.Join(opts.Root, "Users/alice/Library/LaunchAgents/io.github.wangzw.peers.plist")
	if err := os.MkdirAll(filepath.Dir(plist), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(plist, []byte("previous plist"), 0o644); err != nil {
		t.Fatal(err)
	}
	return opts
}

const (
	darwinPrintOldCmd  = "launchctl print gui/501/io.github.wangzw.peers"
	darwinBootoutCmd   = "launchctl bootout gui/501/io.github.wangzw.peers"
	darwinDisableCmd   = "launchctl disable gui/501/io.github.wangzw.peers"
	darwinBootstrapCmd = "launchctl bootstrap gui/501 /Users/alice/Library/LaunchAgents/io.github.wangzw.peers.plist"
)

func countCommand(cmds []string, want string) int {
	n := 0
	for _, cmd := range cmds {
		if cmd == want {
			n++
		}
	}
	return n
}

// countCommandBefore counts want in cmds before the first stop command. The
// replacement segment ends at the bootstrap; commands after it belong to the
// failure cleanup, which may boot out again.
func countCommandBefore(cmds []string, want, stop string) int {
	limit := slices.Index(cmds, stop)
	if limit < 0 {
		limit = len(cmds)
	}
	return countCommand(cmds[:limit], want)
}

// TestInstallDarwinWaitsForTheOldServiceToUnload reproduces the user report of
// 2026-11-20: launchctl bootout returns before launchd has torn the job down,
// and a bootstrap in that window fails with exit 5. The fake keeps reporting
// the old service as loaded for a few print calls and fails any bootstrap
// attempted while it does. It also checks that the replacement boots the job
// out exactly once, before the wait.
func TestInstallDarwinWaitsForTheOldServiceToUnload(t *testing.T) {
	opts := darwinUserReinstall(t)
	// The fake reports the unload after a few polls, so the budget only needs to
	// outlast scheduling, not a real delay.
	opts.StopTimeout = processExitWait

	const loadedPolls = 3
	bootedOut := false
	var unloadPolls int
	bootstrapped := false
	started := false
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		switch {
		case len(args) == 2 && args[0] == "print" && args[1] == "gui/501":
			return []byte("com.apple.launchd\n"), nil
		case len(args) == 2 && args[0] == "print":
			switch {
			case started:
				return []byte("state = running\n\tpid = 5252\n"), nil
			case !bootedOut:
				return []byte("state = running\n\tpid = 4242\n"), nil
			}
			unloadPolls++
			if unloadPolls <= loadedPolls {
				return []byte("state = running\n\tpid = 4242\n"), nil
			}
			return []byte("Could not find service"), exitError(t, 113)
		case len(args) > 0 && args[0] == "bootout":
			bootedOut = true
		case len(args) > 0 && args[0] == "bootstrap":
			if !bootedOut || unloadPolls <= loadedPolls {
				return []byte("Bootstrap failed: 5: Input/output error"), exitError(t, 5)
			}
			bootstrapped = true
			started = true
		}
		return nil, nil
	}
	opts.Probe = probeAfterStart(&started, 5252)

	if _, err := Install(opts); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if !bootstrapped {
		t.Fatalf("the service was never bootstrapped; commands = %q", cmds)
	}
	if unloadPolls <= loadedPolls {
		t.Errorf("bootstrap ran after %d unload polls, want more than the %d that still reported the old service", unloadPolls, loadedPolls)
	}
	if got := countCommandBefore(cmds, darwinBootoutCmd, darwinBootstrapCmd); got != 1 {
		t.Errorf("bootout calls before the bootstrap = %d, want 1; commands = %q", got, cmds)
	}
	bootoutIndex := slices.Index(cmds, darwinBootoutCmd)
	bootstrapIndex := slices.Index(cmds, darwinBootstrapCmd)
	if bootoutIndex < 0 || bootstrapIndex < 0 || bootoutIndex > bootstrapIndex {
		t.Errorf("commands = %q, want bootout before bootstrap", cmds)
	}
	// The unload wait must be an explicit launchctl print between bootout and
	// bootstrap, not merely a side effect of waiting for the address.
	polled := false
	for _, cmd := range cmds[bootoutIndex+1 : bootstrapIndex] {
		if cmd == darwinPrintOldCmd {
			polled = true
			break
		}
	}
	if !polled {
		t.Errorf("commands = %q, want a launchctl print between bootout and bootstrap", cmds)
	}
}

// TestInstallDarwinWaitsForUnloadAfterBootoutExit3 checks that the unload wait
// does not depend on bootout's exit status. launchctl reports exit 3 for a
// service it considers unloaded, but launchd may still be tearing the job down;
// the installation must still wait, then retry the bootstrap that raced the
// teardown.
func TestInstallDarwinWaitsForUnloadAfterBootoutExit3(t *testing.T) {
	opts := darwinUserReinstall(t)
	opts.StopTimeout = processExitWait

	const loadedPolls = 2
	bootedOut := false
	var unloadPolls int
	bootstraps := 0
	earlyBootstrap := false
	started := false
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		switch {
		case len(args) == 2 && args[0] == "print" && args[1] == "gui/501":
			return []byte("com.apple.launchd\n"), nil
		case len(args) == 2 && args[0] == "print":
			switch {
			case started:
				return []byte("state = running\n\tpid = 5252\n"), nil
			case !bootedOut:
				return []byte("state = running\n\tpid = 4242\n"), nil
			}
			unloadPolls++
			if unloadPolls <= loadedPolls {
				return []byte("state = running\n\tpid = 4242\n"), nil
			}
			return []byte("Could not find service"), exitError(t, 113)
		case len(args) > 0 && args[0] == "bootout":
			bootedOut = true
			return nil, exitError(t, 3)
		case len(args) > 0 && args[0] == "bootstrap":
			bootstraps++
			if !bootedOut || unloadPolls <= loadedPolls {
				earlyBootstrap = true
				return []byte("Bootstrap failed: 5: Input/output error"), exitError(t, 5)
			}
			if bootstraps == 1 {
				return []byte("Bootstrap failed: 5: Input/output error"), exitError(t, 5)
			}
			started = true
		}
		return nil, nil
	}
	opts.Probe = probeAfterStart(&started, 5252)

	if _, err := Install(opts); err != nil {
		t.Fatalf("Install after a bootout that reported exit 3: %v", err)
	}
	if earlyBootstrap {
		t.Errorf("bootstrap ran before launchd unloaded the old service; commands = %q", cmds)
	}
	if unloadPolls <= loadedPolls {
		t.Errorf("unload polls = %d, want more than %d", unloadPolls, loadedPolls)
	}
	if bootstraps != 2 {
		t.Errorf("bootstrap calls = %d, want exit 5 then success", bootstraps)
	}
	if got := countCommandBefore(cmds, darwinBootoutCmd, darwinBootstrapCmd); got != 1 {
		t.Errorf("bootout calls before the bootstrap = %d, want 1; commands = %q", got, cmds)
	}
}

// TestInstallDarwinRetriesAPrintErrorThatDoesNotProveUnloaded checks that only
// exit status 113 or "Could not find service" count as unloaded. Any other
// print failure is retried until the service is reported gone.
func TestInstallDarwinRetriesAPrintErrorThatDoesNotProveUnloaded(t *testing.T) {
	opts := darwinUserReinstall(t)
	opts.StopTimeout = processExitWait

	bootedOut := false
	var otherPolls int
	started := false
	opts.Run = func(name string, args ...string) ([]byte, error) {
		switch {
		case len(args) == 2 && args[0] == "print" && args[1] == "gui/501":
			return []byte("com.apple.launchd\n"), nil
		case len(args) == 2 && args[0] == "print":
			switch {
			case started:
				return []byte("state = running\n\tpid = 5252\n"), nil
			case !bootedOut:
				return []byte("state = running\n\tpid = 4242\n"), nil
			}
			if otherPolls < 2 {
				otherPolls++
				return []byte("launchctl: Resource temporarily unavailable"), errors.New("exit status 70")
			}
			return []byte("Could not find service"), exitError(t, 113)
		case len(args) > 0 && args[0] == "bootout":
			bootedOut = true
		case len(args) > 0 && args[0] == "bootstrap":
			started = true
		}
		return nil, nil
	}
	opts.Probe = probeAfterStart(&started, 5252)

	if _, err := Install(opts); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if otherPolls != 2 {
		t.Errorf("print errors retried = %d, want 2 before the missing-service report", otherPolls)
	}
}

// TestInstallDarwinDoesNotTreatAPrintErrorAsUnloaded checks the timeout when
// launchctl print keeps failing with an error that does not prove the job is
// gone: the installation must not bootstrap over it. The failed reinstall keeps
// the plist and disables the label, so launchd does not start it at login.
func TestInstallDarwinDoesNotTreatAPrintErrorAsUnloaded(t *testing.T) {
	opts := darwinUserReinstall(t)
	opts.StopTimeout = 30 * time.Millisecond

	bootstrapped := false
	bootedOut := false
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		switch {
		case len(args) == 2 && args[0] == "print" && args[1] == "gui/501":
			return []byte("com.apple.launchd\n"), nil
		case len(args) == 2 && args[0] == "print":
			if bootedOut {
				return []byte("launchctl: Resource temporarily unavailable"), errors.New("exit status 70")
			}
			return []byte("state = running\n\tpid = 4242\n"), nil
		case len(args) > 0 && args[0] == "bootout":
			bootedOut = true
		case len(args) > 0 && args[0] == "bootstrap":
			bootstrapped = true
		}
		return nil, nil
	}

	_, err := Install(opts)
	if err == nil {
		t.Fatal("Install succeeded while launchctl print kept failing")
	}
	for _, want := range []string{
		"cannot unload the old service in",
		"within 1 second",
		"Resource temporarily unavailable",
		"run the install again",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	if bootstrapped {
		t.Error("Install bootstrapped while launchctl print did not report the service gone")
	}
	if !slices.Contains(cmds, darwinDisableCmd) {
		t.Errorf("commands = %q, want %q so the kept plist does not start at login", cmds, darwinDisableCmd)
	}
	plist := filepath.Join(opts.Root, "Users/alice/Library/LaunchAgents/io.github.wangzw.peers.plist")
	if _, statErr := os.Stat(plist); statErr != nil {
		t.Errorf("plist was removed while the old service may still be loaded: %v", statErr)
	}
}

// TestInstallDarwinRetriesBootstrapExit5 checks that a bootstrap that still
// returns exit 5 after the old service is gone is retried.
func TestInstallDarwinRetriesBootstrapExit5(t *testing.T) {
	opts := darwinUserReinstall(t)
	opts.BootstrapTimeout = processExitWait

	const failedBootstraps = 2
	var bootstraps int
	started := false
	opts.Run = func(name string, args ...string) ([]byte, error) {
		switch {
		case len(args) == 2 && args[0] == "print" && args[1] == "gui/501":
			return []byte("com.apple.launchd\n"), nil
		case len(args) == 2 && args[0] == "print":
			if started {
				return []byte("state = running\n\tpid = 5252\n"), nil
			}
			return []byte("Could not find service"), exitError(t, 113)
		case len(args) > 0 && args[0] == "bootout":
			return nil, nil
		case len(args) > 0 && args[0] == "bootstrap":
			bootstraps++
			if bootstraps <= failedBootstraps {
				return []byte("Bootstrap failed: 5: Input/output error"), exitError(t, 5)
			}
			started = true
		}
		return nil, nil
	}
	opts.Probe = probeAfterStart(&started, 5252)

	if _, err := Install(opts); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if want := failedBootstraps + 1; bootstraps != want {
		t.Errorf("bootstrap calls = %d, want %d", bootstraps, want)
	}
}

// TestInstallDarwinFailsAtOnceOnAnotherBootstrapExit checks that only exit 5 is
// retried: another exit status fails the installation on the first attempt.
func TestInstallDarwinFailsAtOnceOnAnotherBootstrapExit(t *testing.T) {
	opts := darwinUserReinstall(t)
	opts.BootstrapTimeout = processExitWait

	bootstraps := 0
	opts.Run = func(name string, args ...string) ([]byte, error) {
		switch {
		case len(args) == 2 && args[0] == "print" && args[1] == "gui/501":
			return []byte("com.apple.launchd\n"), nil
		case len(args) == 2 && args[0] == "print":
			return []byte("Could not find service"), exitError(t, 113)
		case len(args) > 0 && args[0] == "bootout":
			return nil, nil
		case len(args) > 0 && args[0] == "bootstrap":
			bootstraps++
			return []byte("Bootstrap failed: 37: Operation already in progress"), exitError(t, 37)
		}
		return nil, nil
	}

	_, err := Install(opts)
	if err == nil || !strings.Contains(err.Error(), "Operation already in progress") {
		t.Fatalf("Install error = %v, want the exit 37 failure", err)
	}
	if bootstraps != 1 {
		t.Errorf("bootstrap calls = %d, want 1", bootstraps)
	}
}

// TestInstallDarwinReinstallBootstrapFailureKeepsThePlist checks the failure a
// reinstall reports when bootstrap never succeeds: the old service is gone, no
// Broker runs, and the new plist stays disabled so it can be loaded by hand or
// by running the install again.
func TestInstallDarwinReinstallBootstrapFailureKeepsThePlist(t *testing.T) {
	opts := darwinUserReinstall(t)
	opts.BootstrapTimeout = 30 * time.Millisecond

	bootedOut := false
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		switch {
		case len(args) == 2 && args[0] == "print" && args[1] == "gui/501":
			return []byte("com.apple.launchd\n"), nil
		case len(args) == 2 && args[0] == "print":
			if bootedOut {
				return []byte("Could not find service"), exitError(t, 113)
			}
			return []byte("state = running\n\tpid = 4242\n"), nil
		case len(args) > 0 && args[0] == "bootout":
			bootedOut = true
		case len(args) > 0 && args[0] == "bootstrap":
			return []byte("Bootstrap failed: 5: Input/output error"), exitError(t, 5)
		}
		return nil, nil
	}

	_, err := Install(opts)
	if err == nil {
		t.Fatal("Install succeeded after a bootstrap that always fails")
	}
	for _, want := range []string{
		"the old service was unloaded",
		"no Broker is running",
		"peers service install",
		"launchctl enable gui/501/io.github.wangzw.peers && launchctl bootstrap gui/501 /Users/alice/Library/LaunchAgents/io.github.wangzw.peers.plist",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	if !slices.Contains(cmds, darwinDisableCmd) {
		t.Errorf("commands = %q, want %q so the kept plist does not start at login", cmds, darwinDisableCmd)
	}
	if got := countCommandBefore(cmds, darwinBootoutCmd, darwinBootstrapCmd); got != 1 {
		t.Errorf("bootout calls before the bootstrap = %d, want 1; commands = %q", got, cmds)
	}
	plist := filepath.Join(opts.Root, "Users/alice/Library/LaunchAgents/io.github.wangzw.peers.plist")
	if _, statErr := os.Stat(plist); statErr != nil {
		t.Errorf("reinstall failure removed the plist: %v", statErr)
	}
}

// TestInstallDarwinBootstrapFailureKeepsAnUnloadedPlist checks a plist that
// existed before the install whose service was not loaded, such as one left
// disabled by an earlier failure. The install must keep that plist and disable
// its label; only a plist this run wrote is removed.
func TestInstallDarwinBootstrapFailureKeepsAnUnloadedPlist(t *testing.T) {
	opts := darwinUserReinstall(t)
	opts.BootstrapTimeout = 30 * time.Millisecond

	plist := filepath.Join(opts.Root, "Users/alice/Library/LaunchAgents/io.github.wangzw.peers.plist")
	if err := os.MkdirAll(filepath.Dir(plist), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(plist, []byte("old plist"), 0o644); err != nil {
		t.Fatal(err)
	}

	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		switch {
		case len(args) == 2 && args[0] == "print" && args[1] == "gui/501":
			return []byte("com.apple.launchd\n"), nil
		case len(args) == 2 && args[0] == "print":
			return []byte("Could not find service"), exitError(t, 113)
		case len(args) > 0 && args[0] == "bootout":
			return nil, exitError(t, 3)
		case len(args) > 0 && args[0] == "bootstrap":
			return []byte("Bootstrap failed: 5: Input/output error"), exitError(t, 5)
		}
		return nil, nil
	}

	_, err := Install(opts)
	if err == nil {
		t.Fatal("Install succeeded after a bootstrap that always fails")
	}
	if got := countCommand(cmds, darwinDisableCmd); got != 1 {
		t.Errorf("disable calls = %d, want 1 for a kept plist; commands = %q", got, cmds)
	}
	if _, statErr := os.Stat(plist); statErr != nil {
		t.Errorf("a pre-existing plist was removed by a failed install: %v", statErr)
	}
}

// TestInstallDarwinPrecheckBootsOutOnce checks the replacement order: the
// precheck only identifies the occupant, and the whole installation boots the
// old job out exactly once, before the unload wait and the bootstrap.
func TestInstallDarwinPrecheckBootsOutOnce(t *testing.T) {
	opts := darwinUserReinstall(t)
	writeListenConfig(t, opts, "127.0.0.1:17448")

	var cmds []string
	held := true
	bootedOut := false
	started := false
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		switch {
		case len(args) == 2 && args[0] == "print" && args[1] == "gui/501":
			return []byte("com.apple.launchd\n"), nil
		case len(args) == 2 && args[0] == "print":
			switch {
			case started:
				return []byte("state = running\n\tpid = 5252\n"), nil
			case !bootedOut:
				return []byte("state = running\n\tpid = 4242\n"), nil
			}
			return []byte("Could not find service"), exitError(t, 113)
		case len(args) > 0 && args[0] == "bootout":
			bootedOut = true
			held = false
		case len(args) > 0 && args[0] == "bootstrap":
			started = true
		}
		return nil, nil
	}
	opts.Probe = func(string) (int, bool) {
		if started {
			return 5252, true
		}
		return 4242, true
	}
	opts.ListenProbe = func(addr string) error {
		if held {
			return fmt.Errorf("cannot bind %q: %w", addr, syscall.EADDRINUSE)
		}
		return nil
	}

	if _, err := Install(opts); err != nil {
		t.Fatalf("Install over a running service: %v", err)
	}
	if got := countCommandBefore(cmds, darwinBootoutCmd, darwinBootstrapCmd); got != 1 {
		t.Fatalf("bootout calls before the bootstrap = %d, want 1; commands = %q", got, cmds)
	}
	bootoutIndex := slices.Index(cmds, darwinBootoutCmd)
	bootstrapIndex := slices.Index(cmds, darwinBootstrapCmd)
	if bootoutIndex > bootstrapIndex {
		t.Errorf("commands = %q, want bootout before bootstrap", cmds)
	}
}

// TestInstallDarwinSharesOneStopBudget checks that the unload wait and the
// address wait take the same deadline. A deadline that has already passed makes
// each wait probe once instead of granting itself a fresh budget.
func TestInstallDarwinSharesOneStopBudget(t *testing.T) {
	opts := darwinUserReinstall(t)
	opts.Run = func(string, ...string) ([]byte, error) {
		return []byte("state = running\n\tpid = 4242\n"), nil
	}

	deadline := time.Now()
	if err := waitServiceUnloaded(opts, "gui/501", deadline, time.Millisecond, unloadAdvice(opts, "gui/501", "the install")); err == nil {
		t.Fatal("waitServiceUnloaded succeeded with a past deadline")
	}

	probes := 0
	probe := func(string) error {
		probes++
		return errors.New("still busy")
	}
	if err := waitListenReleased(probe, "127.0.0.1:17448", deadline); err == nil {
		t.Fatal("waitListenReleased succeeded with a past deadline")
	}
	if probes != 1 {
		t.Errorf("waitListenReleased probes = %d, want 1 with the caller's deadline", probes)
	}
}

// TestInstallDarwinBootstrapsAfterTheUnloadWaitUsesTheStopBudget checks that the
// exit-5 retries have their own bound. The unload wait runs past the stop
// budget's deadline and the first bootstrap still reports exit 5; the retry
// must run and succeed instead of inheriting the spent stop deadline.
func TestInstallDarwinBootstrapsAfterTheUnloadWaitUsesTheStopBudget(t *testing.T) {
	opts := darwinUserReinstall(t)
	// Scaled down: two 100ms unload polls run past this deadline.
	opts.StopTimeout = 190 * time.Millisecond
	opts.BootstrapTimeout = processExitWait

	const loadedPolls = 2
	bootedOut := false
	unloadPolls := 0
	bootstraps := 0
	started := false
	opts.Run = func(name string, args ...string) ([]byte, error) {
		switch {
		case len(args) == 2 && args[0] == "print" && args[1] == "gui/501":
			return []byte("com.apple.launchd\n"), nil
		case len(args) == 2 && args[0] == "print":
			switch {
			case started:
				return []byte("state = running\n\tpid = 5252\n"), nil
			case !bootedOut:
				return []byte("state = running\n\tpid = 4242\n"), nil
			}
			unloadPolls++
			if unloadPolls <= loadedPolls {
				return []byte("state = running\n\tpid = 4242\n"), nil
			}
			return []byte("Could not find service"), exitError(t, 113)
		case len(args) > 0 && args[0] == "bootout":
			bootedOut = true
		case len(args) > 0 && args[0] == "bootstrap":
			bootstraps++
			if bootstraps == 1 {
				return []byte("Bootstrap failed: 5: Input/output error"), exitError(t, 5)
			}
			started = true
		}
		return nil, nil
	}
	opts.Probe = probeAfterStart(&started, 5252)

	if _, err := Install(opts); err != nil {
		t.Fatalf("Install after the unload wait used the stop budget: %v", err)
	}
	if unloadPolls <= loadedPolls {
		t.Fatalf("unload polls = %d, want more than %d", unloadPolls, loadedPolls)
	}
	if bootstraps != 2 {
		t.Errorf("bootstrap calls = %d, want the exit 5 attempt and the retry", bootstraps)
	}
}
