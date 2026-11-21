package service

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/procsig"
)

// processExitWait is the package-wide fallback for the wait for
// waitProcessGone to notice a SIGKILLed child. It only decides how long a wait
// that will never complete takes to fail: the success path observes the child
// disappearing and never spends this time, so a larger value costs nothing.
// Thirty seconds is far above the worst normal latency under load; it is not a
// claim that a child should take that long.
const processExitWait = 30 * time.Second

// TestPostUpdateLinuxRewritesTheUnit checks both levels: the main unit is
// replaced with this binary's format, a drop-in beside it is left untouched,
// and the only command run is daemon-reload. The Broker is not restarted.
func TestPostUpdateLinuxRewritesTheUnit(t *testing.T) {
	cases := []struct {
		name    string
		system  bool
		unit    func(string) (string, error)
		path    string
		wantCmd string
	}{
		{
			name:    "user",
			system:  false,
			unit:    SystemdUserUnit,
			path:    "home/alice/.config/systemd/user/peers.service",
			wantCmd: "systemctl --user daemon-reload",
		},
		{
			name:    "system",
			system:  true,
			unit:    SystemdSystemUnit,
			path:    "etc/systemd/system/peers.service",
			wantCmd: "systemctl daemon-reload",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := baseOptions(t)
			opts.System = tc.system
			if tc.system {
				opts.EUID = 0
			}
			var cmds []string
			opts.Run = recordCommands(&cmds)

			unitFile := filepath.Join(opts.Root, tc.path)
			if err := os.MkdirAll(filepath.Dir(unitFile), 0o755); err != nil {
				t.Fatalf("create unit directory: %v", err)
			}
			if err := os.WriteFile(unitFile, []byte("stale unit\n"), 0o644); err != nil {
				t.Fatalf("write stale unit: %v", err)
			}
			const dropIn = "[Service]\nEnvironment=HTTPS_PROXY=http://proxy.example:8080\n"
			dropInPath := filepath.Join(filepath.Dir(unitFile), UnitName+".d", "proxy.conf")
			if err := os.MkdirAll(filepath.Dir(dropInPath), 0o755); err != nil {
				t.Fatalf("create drop-in directory: %v", err)
			}
			if err := os.WriteFile(dropInPath, []byte(dropIn), 0o644); err != nil {
				t.Fatalf("write drop-in: %v", err)
			}

			if err := PostUpdate(opts); err != nil {
				t.Fatalf("PostUpdate: %v", err)
			}

			want, err := tc.unit(opts.Exe)
			if err != nil {
				t.Fatalf("render unit: %v", err)
			}
			if got := mustReadFile(t, unitFile); got != want {
				t.Errorf("unit content = %q, want %q", got, want)
			}
			if got := mustReadFile(t, dropInPath); got != dropIn {
				t.Errorf("drop-in content = %q, want it left untouched as %q", got, dropIn)
			}
			if wantCmds := []string{tc.wantCmd}; !slices.Equal(cmds, wantCmds) {
				t.Errorf("commands = %q, want %q (no restart)", cmds, wantCmds)
			}
			assertNoInstallTempFiles(t, filepath.Dir(unitFile))
		})
	}
}

// TestPostUpdateLinuxRestoresTheUnitWhenDaemonReloadFails checks the first
// phase's rollback of the service definition: the unit this run replaced is
// written back and the unit database is reloaded again, so the binary rollback
// that follows does not leave systemd holding the next version's format.
func TestPostUpdateLinuxRestoresTheUnitWhenDaemonReloadFails(t *testing.T) {
	opts := baseOptions(t)
	const oldUnit = "old unit\n"
	unitFile := filepath.Join(opts.Root, "home/alice/.config/systemd/user/"+UnitName)
	if err := os.MkdirAll(filepath.Dir(unitFile), 0o755); err != nil {
		t.Fatalf("create unit directory: %v", err)
	}
	if err := os.WriteFile(unitFile, []byte(oldUnit), 0o644); err != nil {
		t.Fatalf("write old unit: %v", err)
	}
	var cmds []string
	reloads := 0
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		if slices.Contains(args, "daemon-reload") {
			reloads++
			if reloads == 1 {
				return []byte("unit reload failed"), os.ErrPermission
			}
		}
		return nil, nil
	}

	err := PostUpdate(opts)
	if err == nil {
		t.Fatal("PostUpdate succeeded although daemon-reload failed")
	}
	for _, want := range []string{"permission denied", "previous service definition was restored"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want it to mention %q", err, want)
		}
	}
	if got := mustReadFile(t, unitFile); got != oldUnit {
		t.Errorf("unit content = %q, want the old definition %q", got, oldUnit)
	}
	wantCmds := []string{"systemctl --user daemon-reload", "systemctl --user daemon-reload"}
	if !slices.Equal(cmds, wantCmds) {
		t.Errorf("commands = %q, want a daemon-reload before and after the restore %q", cmds, wantCmds)
	}
}

// TestPostUpdateLinuxRemovesANewUnitWhenDaemonReloadFails checks the restore of
// a definition this run created: there was no old main unit, so the failed run
// leaves none behind.
func TestPostUpdateLinuxRemovesANewUnitWhenDaemonReloadFails(t *testing.T) {
	opts := baseOptions(t)
	unitFile := filepath.Join(opts.Root, "home/alice/.config/systemd/user/"+UnitName)
	reloads := 0
	opts.Run = func(string, ...string) ([]byte, error) {
		reloads++
		if reloads == 1 {
			return nil, os.ErrPermission
		}
		return nil, nil
	}

	err := PostUpdate(opts)
	if err == nil {
		t.Fatal("PostUpdate succeeded although daemon-reload failed")
	}
	if !strings.Contains(err.Error(), "previous service definition was restored") {
		t.Errorf("error = %q, want it to say the definition was restored", err)
	}
	if _, statErr := os.Stat(unitFile); !errors.Is(statErr, fs.ErrNotExist) {
		t.Errorf("the failed run left a unit behind: %v", statErr)
	}
}

// TestPostUpdateLinuxReportsAndAuditsAFailedRestore checks the restore's own
// failure: the file is back but the second daemon-reload failed, so the error
// names that separately from the original failure and an update_failed record
// says the unit and the service manager may disagree.
func TestPostUpdateLinuxReportsAndAuditsAFailedRestore(t *testing.T) {
	opts := baseOptions(t)
	const oldUnit = "old unit\n"
	unitFile := filepath.Join(opts.Root, "home/alice/.config/systemd/user/"+UnitName)
	if err := os.MkdirAll(filepath.Dir(unitFile), 0o755); err != nil {
		t.Fatalf("create unit directory: %v", err)
	}
	if err := os.WriteFile(unitFile, []byte(oldUnit), 0o644); err != nil {
		t.Fatalf("write old unit: %v", err)
	}
	opts.Run = func(string, ...string) ([]byte, error) {
		return []byte("reload failed"), os.ErrPermission
	}

	err := PostUpdate(opts)
	if err == nil {
		t.Fatal("PostUpdate succeeded although daemon-reload failed")
	}
	if !strings.Contains(err.Error(), "service definition was restored but the service manager did not reload it") {
		t.Errorf("error = %q, want the failed restore named separately", err)
	}
	if got := mustReadFile(t, unitFile); got != oldUnit {
		t.Errorf("unit content = %q, want the old definition %q", got, oldUnit)
	}

	auditPath := rooted(opts, auditLogPath(opts))
	data, readErr := os.ReadFile(auditPath)
	if readErr != nil {
		t.Fatalf("read the audit log %s: %v", auditPath, readErr)
	}
	for _, want := range []string{`"event":"update_failed"`, "did not reload it"} {
		if !strings.Contains(string(data), want) {
			t.Errorf("audit log = %q, want it to contain %q", data, want)
		}
	}
}

// TestPostUpdateLaunchdRewritesThePlistAndStartsADetachedHelper checks both
// levels: the plist is replaced with this binary's format, and the reload is
// deferred to a helper that runs in its own session with its standard streams
// on the post-update log and an explicitly opened /dev/null as standard input.
// Nothing is booted out synchronously, because that would stop the job that
// runs the Broker and this process.
func TestPostUpdateLaunchdRewritesThePlistAndStartsADetachedHelper(t *testing.T) {
	cases := []struct {
		name       string
		system     bool
		level      string
		domain     string
		plistPath  string
		auditPath  string
		helperLog  string
		plist      func(string, string) (string, error)
		domainCmds []string
	}{
		{
			name:      "user",
			system:    false,
			level:     "user",
			domain:    "gui/501",
			plistPath: "/Users/alice/Library/LaunchAgents/" + Label + ".plist",
			auditPath: "/Users/alice/.peers/audit.log",
			helperLog: "/Users/alice/Library/Logs/peers/" + LaunchdHelperLogName,
			plist:     LaunchAgentPlist,
			domainCmds: []string{
				"launchctl print gui/501",
			},
		},
		{
			name:      "system",
			system:    true,
			level:     "system",
			domain:    "system",
			plistPath: "/Library/LaunchDaemons/" + Label + ".plist",
			auditPath: "/var/log/peers/audit.log",
			helperLog: "/var/log/peers/" + LaunchdHelperLogName,
			plist:     func(exe, _ string) (string, error) { return LaunchDaemonPlist(exe) },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts, fake := darwinPostUpdateOptions(t)
			opts.System = tc.system
			if tc.system {
				opts.EUID = 0
			}
			opts.Owner = ownLogDirAs(opts)
			writeInstalledUnit(t, opts)
			opts.PPID = func() int { return 4242 }
			var spawned *exec.Cmd
			opts.StartDetached = func(cmd *exec.Cmd) error {
				spawned = cmd
				return nil
			}

			if err := PostUpdate(opts); err != nil {
				t.Fatalf("PostUpdate: %v", err)
			}

			plistPath := filepath.Join(opts.Root, tc.plistPath)
			logPath := filepath.Join(opts.Home, "Library", "Logs", "peers", "broker.log")
			want, err := tc.plist(opts.Exe, logPath)
			if err != nil {
				t.Fatalf("render plist: %v", err)
			}
			if got := mustReadFile(t, plistPath); got != want {
				t.Errorf("plist content = %q, want %q", got, want)
			}
			if !slices.Equal(fake.cmds, tc.domainCmds) {
				t.Errorf("synchronous commands = %q, want only %q", fake.cmds, tc.domainCmds)
			}
			for _, cmd := range fake.cmds {
				if strings.Contains(cmd, "bootout") || strings.Contains(cmd, "bootstrap") {
					t.Errorf("PostUpdate ran %q synchronously; the reload must be deferred", cmd)
				}
			}

			if spawned == nil {
				t.Fatal("PostUpdate did not start the detached helper")
			}
			if spawned.SysProcAttr == nil || !spawned.SysProcAttr.Setsid {
				t.Error("the helper is not started in a new session (SysProcAttr.Setsid is false)")
			}
			stdin, ok := spawned.Stdin.(*os.File)
			if !ok {
				t.Fatalf("the helper's stdin is %T, want an open file on %s", spawned.Stdin, os.DevNull)
			}
			if got := stdin.Name(); got != os.DevNull {
				t.Errorf("the helper's stdin is %s, want %s", got, os.DevNull)
			}
			for name, writer := range map[string]any{"stdout": spawned.Stdout, "stderr": spawned.Stderr} {
				file, ok := writer.(*os.File)
				if !ok {
					t.Fatalf("the helper's %s is %T, want the post-update log file", name, writer)
				}
				if got, want := file.Name(), filepath.Join(opts.Root, tc.helperLog); got != want {
					t.Errorf("the helper's %s goes to %s, want %s", name, got, want)
				}
			}
			id := mustExecutableID(t, filepath.Join(opts.Root, opts.Exe))
			wantArgs := []string{
				opts.Exe, "internal", "launchd-reload",
				"--level", tc.level,
				"--domain", tc.domain,
				"--plist", tc.plistPath,
				"--pid", "4242",
				"--exe-dev", strconv.FormatUint(id.Dev, 10),
				"--exe-ino", strconv.FormatUint(id.Ino, 10),
				"--audit", tc.auditPath,
			}
			if !slices.Equal(spawned.Args, wantArgs) {
				t.Errorf("helper args = %q, want %q", spawned.Args, wantArgs)
			}
		})
	}
}

// TestPostUpdateLaunchdPreservesEnvironmentVariables checks that a rewrite
// keeps the EnvironmentVariables an operator added to the plist. launchd has no
// drop-in mechanism, so this dictionary is the only place a proxy setting can
// live across an update.
func TestPostUpdateLaunchdPreservesEnvironmentVariables(t *testing.T) {
	cases := []struct {
		name      string
		system    bool
		plistPath string
		plist     func(string) (string, error)
	}{
		{
			name:      "user",
			system:    false,
			plistPath: "/Users/alice/Library/LaunchAgents/" + Label + ".plist",
			plist: func(exe string) (string, error) {
				return LaunchAgentPlist(exe, "/Users/alice/Library/Logs/peers/broker.log")
			},
		},
		{
			name:      "system",
			system:    true,
			plistPath: "/Library/LaunchDaemons/" + Label + ".plist",
			plist:     LaunchDaemonPlist,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts, _ := darwinPostUpdateOptions(t)
			opts.System = tc.system
			if tc.system {
				opts.EUID = 0
			}
			opts.Owner = ownLogDirAs(opts)
			opts.StartDetached = func(*exec.Cmd) error { return nil }

			rendered, err := tc.plist(opts.Exe)
			if err != nil {
				t.Fatalf("render plist: %v", err)
			}
			const env = "\t<key>EnvironmentVariables</key>\n\t<dict>\n" +
				"\t\t<key>HTTPS_PROXY</key>\n\t\t<string>http://proxy.example:8080</string>\n" +
				"\t\t<key>NO_PROXY</key>\n\t\t<string>localhost,127.0.0.1</string>\n" +
				"\t</dict>\n"
			oldPlist := strings.Replace(rendered, "</dict>\n</plist>", env+"</dict>\n</plist>", 1)
			oldPath := rooted(opts, tc.plistPath)
			if err := os.MkdirAll(filepath.Dir(oldPath), 0o755); err != nil {
				t.Fatalf("create plist directory: %v", err)
			}
			if err := os.WriteFile(oldPath, []byte(oldPlist), 0o644); err != nil {
				t.Fatalf("write the old plist: %v", err)
			}

			if err := PostUpdate(opts); err != nil {
				t.Fatalf("PostUpdate: %v", err)
			}

			got := mustReadFile(t, oldPath)
			for _, want := range []string{
				"<key>HTTPS_PROXY</key>\n\t\t<string>http://proxy.example:8080</string>",
				"<key>NO_PROXY</key>\n\t\t<string>localhost,127.0.0.1</string>",
			} {
				if !strings.Contains(got, want) {
					t.Errorf("the rewritten plist lost %q:\n%s", want, got)
				}
			}
			if n := strings.Count(got, "<key>EnvironmentVariables</key>"); n != 1 {
				t.Errorf("the rewritten plist has %d EnvironmentVariables keys, want 1:\n%s", n, got)
			}
			if !strings.Contains(got, "broker") {
				t.Errorf("the rewritten plist is not this package's definition:\n%s", got)
			}
		})
	}
}

// TestWithPlistEnvironmentMergesGeneratedAndKeptEntries checks the rewrite
// writes one dictionary when the template itself sets EnvironmentVariables: a
// generated entry wins over a kept entry of the same name, the kept entries it
// does not override survive, and the plist keeps its other keys.
func TestWithPlistEnvironmentMergesGeneratedAndKeptEntries(t *testing.T) {
	rendered, err := LaunchDaemonPlist("/usr/local/bin/peers")
	if err != nil {
		t.Fatalf("render the plist: %v", err)
	}
	const generated = "\t<key>EnvironmentVariables</key>\n\t<dict>\n" +
		"\t\t<key>BOTH</key>\n\t\t<string>new</string>\n" +
		"\t\t<key>GENERATED</key>\n\t\t<string>only-new</string>\n" +
		"\t</dict>\n"
	content := strings.Replace(rendered, "</dict>\n</plist>", generated+"</dict>\n</plist>", 1)

	got := withPlistEnvironment(content, []plistEnvironmentEntry{
		{Name: "BOTH", Value: "old"},
		{Name: "KEPT", Value: "only-old"},
	})
	if n := strings.Count(got, "<key>EnvironmentVariables</key>"); n != 1 {
		t.Fatalf("the merged plist has %d EnvironmentVariables keys, want 1:\n%s", n, got)
	}
	for _, want := range []string{
		"<key>BOTH</key>\n\t\t<string>new</string>",
		"<key>GENERATED</key>\n\t\t<string>only-new</string>",
		"<key>KEPT</key>\n\t\t<string>only-old</string>",
		"<key>ProgramArguments</key>",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("the merged plist lost %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "<string>old</string>") {
		t.Errorf("a generated entry did not win over the kept entry of the same name:\n%s", got)
	}
}

// TestPostUpdateLaunchdRefusesASymlinkedPostUpdateLog checks the log file is
// opened without following a symbolic link at its path.
func TestPostUpdateLaunchdRefusesASymlinkedPostUpdateLog(t *testing.T) {
	opts, _ := darwinPostUpdateOptions(t)
	opts.System = true
	opts.EUID = 0
	opts.Owner = ownLogDirAs(opts)
	started := false
	opts.StartDetached = func(*exec.Cmd) error {
		started = true
		return nil
	}

	logPath := rooted(opts, filepath.Join(systemLogDir, LaunchdHelperLogName))
	if err := os.MkdirAll(filepath.Dir(logPath), 0o750); err != nil {
		t.Fatalf("create log directory: %v", err)
	}
	target := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(target, []byte("not the log"), 0o600); err != nil {
		t.Fatalf("write the symlink target: %v", err)
	}
	if err := os.Symlink(target, logPath); err != nil {
		t.Fatalf("create the symlink: %v", err)
	}

	err := PostUpdate(opts)
	if err == nil {
		t.Fatal("PostUpdate followed a symlink at the post-update log path")
	}
	if !strings.Contains(err.Error(), "post-update log") {
		t.Errorf("error = %q, want it to name the post-update log", err)
	}
	if started {
		t.Error("PostUpdate started the helper although the log path is a symlink")
	}
}

// TestPostUpdateLaunchdRefusesAnUnsafeLogDirectory checks the directory that
// holds the post-update log: it must belong to the account running the helper
// and not be writable by group or others.
func TestPostUpdateLaunchdRefusesAnUnsafeLogDirectory(t *testing.T) {
	cases := []struct {
		name string
		uid  uint32
		mode fs.FileMode
		want string
	}{
		{name: "owned by another account", uid: 501, mode: fs.ModeDir | 0o700, want: "owned by uid 501"},
		{name: "group writable", uid: 0, mode: fs.ModeDir | 0o770, want: "writable by group or others"},
		{name: "others writable", uid: 0, mode: fs.ModeDir | 0o707, want: "writable by group or others"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts, _ := darwinPostUpdateOptions(t)
			opts.System = true
			opts.EUID = 0
			started := false
			opts.StartDetached = func(*exec.Cmd) error {
				started = true
				return nil
			}
			opts.Owner = func(string) (uint32, fs.FileMode, error) {
				return tc.uid, tc.mode, nil
			}

			err := PostUpdate(opts)
			if err == nil {
				t.Fatal("PostUpdate accepted an unsafe post-update log directory")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to mention %q", err, tc.want)
			}
			if started {
				t.Error("PostUpdate started the helper although the log directory is unsafe")
			}
		})
	}
}

// TestLaunchdReloadWaitsForTheCallerThenBootsOutAndBootstraps checks the three
// properties the deferred reload needs: no launchctl command runs before the
// caller exits, the first is the bootout, and the bootstrap follows the unload
// wait. A gate holds the caller-exit wait open to prove the ordering.
func TestLaunchdReloadWaitsForTheCallerThenBootsOutAndBootstraps(t *testing.T) {
	opts, fake, plistPath, id := launchdReloadFixture(t)
	fake.loaded = true
	fake.pid = 111

	waited := make(chan struct{})
	gate := make(chan struct{})
	var waitedPID int
	opts.WaitProcessExit = func(pid int, _ time.Duration) error {
		waitedPID = pid
		close(waited)
		<-gate
		return nil
	}

	auditPath := filepath.Join(t.TempDir(), "audit.log")
	done := make(chan error, 1)
	go func() {
		done <- LaunchdReload(opts, "system", plistPath, 4242, id, auditPath)
	}()

	<-waited
	if len(fake.cmds) != 0 {
		t.Fatalf("launchctl ran %q before the caller exited", fake.cmds)
	}
	close(gate)
	if err := <-done; err != nil {
		t.Fatalf("LaunchdReload: %v", err)
	}
	if waitedPID != 4242 {
		t.Errorf("waited for pid %d, want 4242", waitedPID)
	}

	want := []string{
		"launchctl bootout system/" + Label,
		"launchctl print system/" + Label,
		"launchctl bootstrap system " + plistPath,
	}
	if !slices.Equal(fake.cmds, want) {
		t.Errorf("commands = %q, want %q", fake.cmds, want)
	}
}

// TestLaunchdReloadSkipsTheWaitWhenTheCallerIsAlreadyGone checks the case the
// helper's own parent pid reaches: os.Getppid returns 1 once the Broker exits
// and the helper is reparented to init. Pid 1 is init, not the Broker, so
// waiting on it would time out; the reload must proceed at once.
func TestLaunchdReloadSkipsTheWaitWhenTheCallerIsAlreadyGone(t *testing.T) {
	opts, fake, plistPath, id := launchdReloadFixture(t)
	fake.loaded = true
	fake.pid = 111
	opts.WaitProcessExit = func(pid int, _ time.Duration) error {
		t.Errorf("waited for pid %d, want no wait when the caller is already gone", pid)
		return errors.New("process is still running")
	}

	auditPath := filepath.Join(t.TempDir(), "audit.log")
	if err := LaunchdReload(opts, "system", plistPath, 1, id, auditPath); err != nil {
		t.Fatalf("LaunchdReload: %v", err)
	}

	want := []string{
		"launchctl bootout system/" + Label,
		"launchctl print system/" + Label,
		"launchctl bootstrap system " + plistPath,
	}
	if !slices.Equal(fake.cmds, want) {
		t.Errorf("commands = %q, want %q", fake.cmds, want)
	}
}

// TestLaunchdReloadAbandonsTheReloadWhenTheBrokerDoesNotExit checks that a
// Broker which does not exit within the bound leaves the service loaded. The
// update may have been rolled back, and booting the job out would stop the
// Broker that is serving the old version instead of loading the new definition.
func TestLaunchdReloadAbandonsTheReloadWhenTheBrokerDoesNotExit(t *testing.T) {
	opts, fake, plistPath, id := launchdReloadFixture(t)
	fake.loaded = true
	fake.pid = 111
	opts.WaitProcessExit = func(int, time.Duration) error {
		return errors.New("process is still running")
	}
	var logs []string
	opts.Logf = func(format string, args ...any) {
		logs = append(logs, fmt.Sprintf(format, args...))
	}
	auditPath := filepath.Join(t.TempDir(), "audit.log")

	err := LaunchdReload(opts, "system", plistPath, 4242, id, auditPath)
	if err == nil {
		t.Fatal("LaunchdReload succeeded although the Broker did not exit")
	}
	if !strings.Contains(err.Error(), "did not exit within") {
		t.Errorf("error = %q, want it to name the wait timeout", err)
	}
	if len(fake.cmds) != 0 {
		t.Errorf("commands = %q, want no launchctl command after a timed-out wait", fake.cmds)
	}
	if len(logs) != 1 || !strings.Contains(logs[0], "did not exit within") {
		t.Errorf("logs = %q, want one line about the Broker that did not exit", logs)
	}
	assertAuditDetail(t, auditPath, "did not exit within")
}

// TestLaunchdReloadAbandonsTheReloadWhenTheBinaryWasRolledBack checks the
// reload's precondition: the plist must name the binary that started the
// helper. A rolled-back update leaves the previous binary at that path, and the
// helper must not boot the service out then.
func TestLaunchdReloadAbandonsTheReloadWhenTheBinaryWasRolledBack(t *testing.T) {
	opts, fake, plistPath, id := launchdReloadFixture(t)
	fake.loaded = true
	fake.pid = 111
	opts.WaitProcessExit = func(int, time.Duration) error { return nil }
	auditPath := filepath.Join(t.TempDir(), "audit.log")

	exePath := rooted(opts, opts.Exe)
	// Rename a freshly written file over the path, as a binary rollback does.
	// Removing the new binary first could hand the same inode to the file
	// written next, which would hide the change the helper must notice.
	previous := filepath.Join(filepath.Dir(exePath), "previous-binary")
	if err := os.WriteFile(previous, []byte("old binary"), 0o755); err != nil {
		t.Fatalf("write the old binary: %v", err)
	}
	if err := os.Rename(previous, exePath); err != nil {
		t.Fatalf("put the old binary back: %v", err)
	}

	err := LaunchdReload(opts, "system", plistPath, 4242, id, auditPath)
	if err == nil {
		t.Fatal("LaunchdReload succeeded although the binary was rolled back")
	}
	if !strings.Contains(err.Error(), "not the binary that started this helper") {
		t.Errorf("error = %q, want it to name the mismatched executable", err)
	}
	if len(fake.cmds) != 0 {
		t.Errorf("commands = %q, want no launchctl command after a rollback", fake.cmds)
	}
	assertAuditDetail(t, auditPath, "not the binary that started this helper")
}

// TestLaunchdReloadRecordsAFailedBootstrapInTheAudit checks the second phase's
// failure path: the caller already has the success response, so the failure is
// recorded in the audit log and reported on standard error (the post-update
// log) instead of being returned to a caller. The Detail marks the phase.
func TestLaunchdReloadRecordsAFailedBootstrapInTheAudit(t *testing.T) {
	opts, fake, plistPath, id := launchdReloadFixture(t)
	fake.loaded = true
	fake.pid = 111
	fake.bootstrapFailures = 1
	opts.BootstrapTimeout = time.Nanosecond
	opts.WaitProcessExit = func(int, time.Duration) error { return nil }
	auditPath := filepath.Join(t.TempDir(), "audit.log")

	err := LaunchdReload(opts, "system", plistPath, 4242, id, auditPath)
	if err == nil {
		t.Fatal("LaunchdReload succeeded although bootstrap failed")
	}
	for _, want := range []string{"launchctl bootstrap system", "Input/output error"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want it to mention %q", err, want)
		}
	}

	assertAuditDetail(t, auditPath,
		"second phase: reloading the service definition after the update failed",
		"launchctl bootstrap")
}

// TestPostUpdateLinuxReportsAReloadFailure checks that a failing daemon-reload
// is reported with the command that failed, so the caller can name the reason.
func TestPostUpdateLinuxReportsAReloadFailure(t *testing.T) {
	opts := baseOptions(t)
	opts.Run = func(string, ...string) ([]byte, error) {
		return []byte("unit reload failed"), os.ErrPermission
	}

	err := PostUpdate(opts)
	if err == nil {
		t.Fatal("PostUpdate succeeded although daemon-reload failed")
	}
	for _, want := range []string{"systemctl --user daemon-reload", "permission denied"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want it to mention %q", err, want)
		}
	}
}

// TestPostUpdateOptionsSystemNeedsNoHome covers the systemd case: the system
// service runs with no HOME, and rewriting its definition must not read the
// account home. The user installation still reads it.
func TestPostUpdateOptionsSystemNeedsNoHome(t *testing.T) {
	t.Setenv("HOME", "")
	opts, err := PostUpdateOptions(true)
	if err != nil {
		t.Fatalf("PostUpdateOptions(true): %v", err)
	}
	if !opts.System {
		t.Error("System = false, want the system installation")
	}
	if opts.Home != "" {
		t.Errorf("Home = %q, want empty: the system installation must not read the account home", opts.Home)
	}
	if opts.Exe == "" {
		t.Error("Exe is empty, so the rewritten definition would not name a binary")
	}
	if opts.Run == nil {
		t.Error("Run is nil, so the rewrite could not reload the unit database")
	}

	if _, err := PostUpdateOptions(false); err == nil {
		t.Fatal("PostUpdateOptions(false) succeeded without a home directory")
	}
}

// TestPostUpdateRejectsAnUnsupportedPlatform keeps the platform behaviour the
// other service operations have.
func TestPostUpdateRejectsAnUnsupportedPlatform(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "windows"
	if err := PostUpdate(opts); err == nil {
		t.Fatal("PostUpdate accepted an unsupported platform")
	}
}

// TestStartDetachedProcessStartsANewSession checks the real process: the
// helper must run in a session of its own, because the launchd job's bootout
// stops the job's process group.
func TestStartDetachedProcessStartsANewSession(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("reads the session id from /proc")
	}
	cmd := exec.Command("sleep", "10")
	cmd.SysProcAttr = detachedSysProcAttr()
	pid, err := startDetachedProcess(cmd)
	if err != nil {
		t.Fatalf("startDetachedProcess: %v", err)
	}
	t.Cleanup(func() { _ = procsig.SendSignal(pid, syscall.SIGKILL) })

	session := procSession(t, pid)
	if session == procSession(t, os.Getpid()) {
		t.Errorf("the helper's session %d is the test's own session", session)
	}
	if session != pid {
		t.Errorf("the helper's session = %d, want its own pid %d", session, pid)
	}
}

// TestStartDetachedProcessLeavesTheReleasedPidAtMinusOne checks the property
// behind the accident: Release sets cmd.Process.Pid to -1, so reading it after
// the call and passing it to kill means kill(-1, SIGKILL), which signals every
// process the account may signal. The pid must come from the return value, and
// this test sends no signal through cmd.Process.
func TestStartDetachedProcessLeavesTheReleasedPidAtMinusOne(t *testing.T) {
	cmd := exec.Command("sleep", "10")
	pid, err := startDetachedProcess(cmd)
	if err != nil {
		t.Fatalf("startDetachedProcess: %v", err)
	}
	if pid <= 1 {
		t.Fatalf("startDetachedProcess returned pid %d, want the child's pid", pid)
	}
	t.Cleanup(func() { _ = procsig.SendSignal(pid, syscall.SIGKILL) })

	if got := cmd.Process.Pid; got != -1 {
		t.Errorf("cmd.Process.Pid after startDetachedProcess = %d, want -1 (Release clears it)", got)
	}
}

// TestWaitProcessGoneReturnsAfterTheProcessExits checks the real wait: it
// returns once the process is gone instead of polling until the bound.
func TestWaitProcessGoneReturnsAfterTheProcessExits(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("uses the session id from /proc")
	}
	cmd := exec.Command("sleep", "10")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start the child: %v", err)
	}
	pid := cmd.Process.Pid
	done := make(chan error, 1)
	go func() { done <- waitProcessGone(pid, 10*time.Second) }()

	time.Sleep(2 * processExitPollInterval)
	if err := procsig.SendSignal(pid, syscall.SIGKILL); err != nil {
		t.Fatalf("kill the child: %v", err)
	}
	go func() { _ = cmd.Wait() }()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("waitProcessGone: %v", err)
		}
	case <-time.After(processExitWait):
		t.Fatal("waitProcessGone did not return after the process exited")
	}
}

// darwinPostUpdateOptions returns darwin options whose directory owner matches
// the account that runs the helper, and creates the executable the plist names.
func darwinPostUpdateOptions(t *testing.T) (Options, *darwinFake) {
	t.Helper()
	opts, fake := darwinControlOptions(t)
	opts.Owner = ownLogDirAs(opts)
	writeRootedFile(t, opts, opts.Exe, "new binary", 0o755)
	return opts, fake
}

// ownLogDirAs makes the post-update log directory look owned by the account the
// options name. Call it after setting EUID: Options is passed by value, so a
// closure made before the change would keep the old value.
func ownLogDirAs(opts Options) func(string) (uint32, fs.FileMode, error) {
	return func(string) (uint32, fs.FileMode, error) {
		return uint32(opts.EUID), fs.ModeDir | 0o700, nil
	}
}

// launchdReloadFixture returns options, the fake launchctl, the plist path and
// the executable identity the helper expects.
func launchdReloadFixture(t *testing.T) (Options, *darwinFake, string, ExecutableID) {
	t.Helper()
	opts, fake := darwinPostUpdateOptions(t)
	opts.System = true
	opts.EUID = 0
	opts.Owner = ownLogDirAs(opts)
	plistPath := "/Library/LaunchDaemons/" + Label + ".plist"
	content, err := LaunchDaemonPlist(opts.Exe)
	if err != nil {
		t.Fatalf("render the plist: %v", err)
	}
	writeRootedFile(t, opts, plistPath, content, 0o644)
	return opts, fake, plistPath, mustExecutableID(t, rooted(opts, opts.Exe))
}

// writeRootedFile writes content at a production path under the test root.
func writeRootedFile(t *testing.T, opts Options, path, content string, mode fs.FileMode) string {
	t.Helper()
	fsPath := rooted(opts, path)
	if err := os.MkdirAll(filepath.Dir(fsPath), 0o755); err != nil {
		t.Fatalf("create the directory of %s: %v", path, err)
	}
	if err := os.WriteFile(fsPath, []byte(content), mode); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return fsPath
}

// mustExecutableID records the identity of an existing file.
func mustExecutableID(t *testing.T, path string) ExecutableID {
	t.Helper()
	id, err := executableID(path)
	if err != nil {
		t.Fatalf("identify %s: %v", path, err)
	}
	return id
}

// assertAuditDetail reads the audit log and checks it contains an update_failed
// record with every wanted fragment.
func assertAuditDetail(t *testing.T, path string, want ...string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the audit log %s: %v", path, err)
	}
	wants := append([]string{`"event":"update_failed"`}, want...)
	for _, fragment := range wants {
		if !strings.Contains(string(data), fragment) {
			t.Errorf("audit log = %q, want it to contain %q", data, fragment)
		}
	}
}

// procSession returns the session id of pid from /proc.
func procSession(t *testing.T, pid int) int {
	t.Helper()
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		t.Fatalf("read /proc/%d/stat: %v", pid, err)
	}
	text := string(data)
	end := strings.LastIndexByte(text, ')')
	if end < 0 {
		t.Fatalf("/proc/%d/stat has no closing parenthesis: %q", pid, text)
	}
	fields := strings.Fields(text[end+1:])
	if len(fields) < 4 {
		t.Fatalf("/proc/%d/stat has %d fields after the command name, want at least 4", pid, len(fields))
	}
	session, err := strconv.Atoi(fields[3])
	if err != nil {
		t.Fatalf("parse the session id of %d: %v", pid, err)
	}
	return session
}

// assertNoInstallTempFiles checks that no atomic-write temporary file survived
// the rewrite.
func assertNoInstallTempFiles(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir %s: %v", dir, err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".peers-install-") {
			t.Errorf("temporary file %s survived the rewrite", entry.Name())
		}
	}
}
