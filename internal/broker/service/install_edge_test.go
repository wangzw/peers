package service

import (
	"errors"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
)

// TestDefaultOptionsFromTheRunningProcess checks that DefaultOptions fills the
// fields and callbacks Install needs from the running process: the executable
// path, the account directories, and the command, owner, and socket helpers.
func TestDefaultOptionsFromTheRunningProcess(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", "/xdg-default-options")

	opts, err := DefaultOptions(false)
	if err != nil {
		t.Fatalf("DefaultOptions: %v", err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	wantExe, err := filepath.Abs(exe)
	if err != nil {
		t.Fatalf("filepath.Abs: %v", err)
	}
	if opts.System {
		t.Error("DefaultOptions(false) set System")
	}
	if opts.GOOS != runtime.GOOS {
		t.Errorf("GOOS = %q, want %q", opts.GOOS, runtime.GOOS)
	}
	if opts.EUID != os.Geteuid() || opts.UID != os.Getuid() {
		t.Errorf("EUID/UID = %d/%d, want %d/%d", opts.EUID, opts.UID, os.Geteuid(), os.Getuid())
	}
	if opts.Exe != wantExe {
		t.Errorf("Exe = %q, want %q", opts.Exe, wantExe)
	}
	if opts.Home != home {
		t.Errorf("Home = %q, want %q", opts.Home, home)
	}
	if opts.XDGConfigHome != "/xdg-default-options" {
		t.Errorf("XDGConfigHome = %q, want the environment value", opts.XDGConfigHome)
	}
	if opts.TempDir != os.TempDir() {
		t.Errorf("TempDir = %q, want %q", opts.TempDir, os.TempDir())
	}
	if opts.LookPath == nil || opts.Run == nil || opts.RunOutput == nil || opts.Owner == nil ||
		opts.EvalSymlinks == nil || opts.StatusProbe == nil || opts.Probe == nil {
		t.Fatal("DefaultOptions left a required callback nil")
	}

	if out, err := opts.Run("sh", "-c", "printf hi"); err != nil || string(out) != "hi" {
		t.Errorf("Run(printf hi) = %q, %v, want \"hi\", nil", out, err)
	}
	if _, err := opts.Run("sh", "-c", "exit 3"); err == nil {
		t.Error("Run(exit 3) succeeded, want the command failure")
	}

	file := filepath.Join(t.TempDir(), "owned")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	uid, mode, err := opts.Owner(file)
	if err != nil {
		t.Fatalf("Owner: %v", err)
	}
	if uid != uint32(os.Getuid()) {
		t.Errorf("Owner uid = %d, want %d", uid, os.Getuid())
	}
	if mode.Perm() != 0o600 {
		t.Errorf("Owner mode = %#o, want 0600", mode.Perm())
	}
	if _, _, err := opts.Owner(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Error("Owner of a missing file succeeded, want an error")
	}

	sock := shortProbeSocket(t)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	pid, answering := opts.Probe(sock)
	if !answering || pid != os.Getpid() {
		t.Errorf("Probe(live socket) = %d, %v, want %d, true", pid, answering, os.Getpid())
	}
	if pid, answering := opts.Probe(filepath.Join(t.TempDir(), "absent.sock")); answering || pid != 0 {
		t.Errorf("Probe(absent socket) = %d, %v, want 0, false", pid, answering)
	}
}

// TestDefaultOptionsWithoutHome checks that an account whose home cannot be
// located is reported instead of installing with an empty home directory.
func TestDefaultOptionsWithoutHome(t *testing.T) {
	t.Setenv("HOME", "")
	if _, err := DefaultOptions(false); err == nil || !strings.Contains(err.Error(), "cannot locate the home directory") {
		t.Fatalf("DefaultOptions error = %v, want the home directory failure", err)
	}
}

// TestInstallRejectsRelativeExecutable checks that a relative executable path is
// refused before anything is written.
func TestInstallRejectsRelativeExecutable(t *testing.T) {
	opts := baseOptions(t)
	opts.Exe = "relative/peers"
	_, err := Install(opts)
	if err == nil || !strings.Contains(err.Error(), "is not an absolute path") {
		t.Fatalf("Install error = %v, want the relative executable rejection", err)
	}
}

// TestInstallReportsAnUnresolvedExecutable checks that a symlink resolution
// failure is reported.
func TestInstallReportsAnUnresolvedExecutable(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	linkErr := errors.New("symbolic link loop")
	opts.EvalSymlinks = func(string) (string, error) { return "", linkErr }
	_, err := Install(opts)
	if err == nil || !errors.Is(err, linkErr) || !strings.Contains(err.Error(), "cannot resolve the executable") {
		t.Fatalf("Install error = %v, want the symlink resolution failure", err)
	}
}

// TestInstallSystemFailsWhenTheAdminGroupRecordCannotBeWritten checks that a
// recorded group creation that cannot be written stops the installation before
// any configuration file exists.
func TestInstallSystemFailsWhenTheAdminGroupRecordCannotBeWritten(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	groupExists := false
	opts.GroupGID = func(string) (int, bool, error) {
		if groupExists {
			return 4242, true, nil
		}
		return 0, false, nil
	}
	opts.Run = func(name string, args ...string) ([]byte, error) {
		if name == "groupadd" {
			groupExists = true
		}
		return nil, nil
	}
	if err := os.MkdirAll(filepath.Join(opts.Root, "var"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(opts.Root, "var", "lib"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	res, err := Install(opts)
	if err == nil || !strings.Contains(err.Error(), "cannot create directory "+systemDataDir) {
		t.Fatalf("Install error = %v, want the group record directory failure", err)
	}
	if !res.AdminGroupCreated {
		t.Error("Result.AdminGroupCreated = false, want true for the created group")
	}
	if _, statErr := os.Stat(rooted(opts, configFilePath(opts))); !errors.Is(statErr, fs.ErrNotExist) {
		t.Errorf("configuration file exists after the group record failed: %v", statErr)
	}
}

// TestInstallFailsWhenTheConfigDirectoryCannotBeCreated checks that a
// configuration directory that cannot be created fails the installation.
func TestInstallFailsWhenTheConfigDirectoryCannotBeCreated(t *testing.T) {
	opts := baseOptions(t)
	alice := filepath.Join(opts.Root, "home", "alice")
	if err := os.MkdirAll(alice, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(alice, ".peers"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Install(opts)
	if err == nil || !strings.Contains(err.Error(), "cannot create the configuration directory") {
		t.Fatalf("Install error = %v, want the configuration directory failure", err)
	}
}

// TestInstallDarwinReportsAnUnreadablePlistPath checks that a plist path whose
// parent cannot be inspected fails the installation instead of being written
// over.
func TestInstallDarwinReportsAnUnreadablePlistPath(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.EUID = 501
	opts.UID = 501
	opts.Home = "/Users/alice"
	opts.Run = func(name string, args ...string) ([]byte, error) {
		if name == "launchctl" && len(args) == 2 && args[0] == "print" && args[1] == "gui/501" {
			return []byte("domain\n"), nil
		}
		return nil, nil
	}
	alice := filepath.Join(opts.Root, "Users", "alice")
	if err := os.MkdirAll(alice, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(alice, "Library"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Install(opts)
	plist := filepath.Join(opts.Home, "Library", "LaunchAgents", Label+".plist")
	if err == nil || !strings.Contains(err.Error(), "cannot inspect "+plist) {
		t.Fatalf("Install error = %v, want the plist inspection failure naming %s", err, plist)
	}
}

// TestInstallDarwinFailsWhenTheLogDirectoryCannotBeCreated checks that a log
// directory that cannot be created fails the darwin installation naming its
// path.
func TestInstallDarwinFailsWhenTheLogDirectoryCannotBeCreated(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.EUID = 501
	opts.UID = 501
	opts.Home = "/Users/alice"
	opts.Run = func(name string, args ...string) ([]byte, error) {
		if name == "launchctl" && len(args) == 2 && args[0] == "print" && args[1] == "gui/501" {
			return []byte("domain\n"), nil
		}
		return nil, nil
	}
	library := filepath.Join(opts.Root, "Users", "alice", "Library")
	if err := os.MkdirAll(library, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(library, "Logs"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Install(opts)
	logDir := filepath.Join(opts.Home, "Library", "Logs", "peers")
	if err == nil || !strings.Contains(err.Error(), "cannot create directory "+logDir) {
		t.Fatalf("Install error = %v, want the log directory failure naming %s", err, logDir)
	}
}

// TestInstallLinuxFailsWhenTheUnitDirectoryCannotBeCreated checks that a unit
// directory that cannot be created fails the installation before systemctl runs.
func TestInstallLinuxFailsWhenTheUnitDirectoryCannotBeCreated(t *testing.T) {
	opts := baseOptions(t)
	alice := filepath.Join(opts.Root, "home", "alice")
	if err := os.MkdirAll(alice, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(alice, ".config"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	var cmds []string
	opts.Run = recordCommands(&cmds)

	_, err := Install(opts)
	unitDir := filepath.Join(opts.Home, ".config", "systemd", "user")
	if err == nil || !strings.Contains(err.Error(), "cannot create directory "+unitDir) {
		t.Fatalf("Install error = %v, want the unit directory failure naming %s", err, unitDir)
	}
	if slices.ContainsFunc(cmds, func(cmd string) bool { return strings.Contains(cmd, "systemctl") }) {
		t.Errorf("commands = %q, want no systemctl call after the unit write failed", cmds)
	}
}

// TestInstallLinuxAbandonsAfterAStopWhenTheUnitCannotBeWritten checks that an
// installation which stopped the running service to free broker.listen and then
// failed to write the unit still disables the service and tells the operator
// how to restore it.
func TestInstallLinuxAbandonsAfterAStopWhenTheUnitCannotBeWritten(t *testing.T) {
	opts := baseOptions(t)
	writeListenConfig(t, opts, "127.0.0.1:17443")
	alice := filepath.Join(opts.Root, "home", "alice")
	if err := os.MkdirAll(alice, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(alice, ".config"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	stopped := false
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		switch {
		case slices.Contains(args, "show"):
			return []byte("4242\n"), nil
		case slices.Contains(args, "stop"):
			stopped = true
			return nil, nil
		}
		return nil, nil
	}
	opts.Probe = func(string) (int, bool) { return 4242, true }
	opts.ListenProbe = func(string) error {
		if stopped {
			return nil
		}
		return syscall.EADDRINUSE
	}

	_, err := Install(opts)
	if err == nil {
		t.Fatal("Install succeeded although the unit directory could not be created")
	}
	for _, want := range []string{"the existing service was stopped", "will not start automatically", "systemctl --user enable --now " + UnitName} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	if got := commandCount(cmds, "systemctl --user disable --now "+UnitName); got != 1 {
		t.Errorf("disable commands = %d, want 1 in %q", got, cmds)
	}
}

// TestUnitPathRejectsAnUnknownPlatform checks that a platform without a unit
// location is refused.
func TestUnitPathRejectsAnUnknownPlatform(t *testing.T) {
	_, err := unitPath(Options{GOOS: "freebsd"})
	if err == nil || !strings.Contains(err.Error(), `cannot find the unit path on platform "freebsd"`) {
		t.Fatalf("unitPath error = %v, want the platform rejection", err)
	}
}

// TestPrecheckListenReportsAnInvalidConfig checks that an unreadable
// broker.listen fails the precheck without probing.
func TestPrecheckListenReportsAnInvalidConfig(t *testing.T) {
	opts := baseOptions(t)
	writeListenConfig(t, opts, "not an address")
	probed := false
	opts.ListenProbe = func(string) error {
		probed = true
		return nil
	}
	res := Result{}
	check, err := precheckListen(opts, "", &res)
	if err == nil || !strings.Contains(err.Error(), "cannot read broker.listen from") {
		t.Fatalf("precheckListen error = %v, want the configuration read failure", err)
	}
	if check != (listenCheck{}) {
		t.Errorf("listenCheck = %+v, want the zero value", check)
	}
	if probed {
		t.Error("the listen precheck probed an address that could not be read")
	}
}

// TestPrecheckListenSkipsAnEmptyAddress checks that an empty broker.listen
// skips the probe.
func TestPrecheckListenSkipsAnEmptyAddress(t *testing.T) {
	opts := baseOptions(t)
	writeListenConfig(t, opts, "")
	probed := false
	opts.ListenProbe = func(string) error {
		probed = true
		return nil
	}
	res := Result{}
	check, err := precheckListen(opts, "", &res)
	if err != nil {
		t.Fatalf("precheckListen: %v", err)
	}
	if check != (listenCheck{}) {
		t.Errorf("listenCheck = %+v, want the zero value", check)
	}
	if probed {
		t.Error("the listen precheck probed an empty broker.listen")
	}
}

// TestInstallReportsWhenTheStoppedServiceCannotBeDisabled checks that a
// precheck stop that fails together with its disable reports both: the service
// may start again automatically and the operator must be told.
func TestInstallReportsWhenTheStoppedServiceCannotBeDisabled(t *testing.T) {
	opts := baseOptions(t)
	writeListenConfig(t, opts, "127.0.0.1:17443")
	opts.Run = func(name string, args ...string) ([]byte, error) {
		switch {
		case slices.Contains(args, "show"):
			return []byte("4242\n"), nil
		case slices.Contains(args, "stop"):
			return []byte("stop failed"), errors.New("exit status 1")
		case slices.Contains(args, "disable"):
			return []byte("disable failed"), errors.New("exit status 1")
		}
		return nil, nil
	}
	opts.Probe = func(string) (int, bool) { return 4242, true }
	opts.ListenProbe = func(string) error { return syscall.EADDRINUSE }

	_, err := Install(opts)
	if err == nil {
		t.Fatal("Install succeeded although the service could not be stopped")
	}
	if !strings.Contains(err.Error(), "could not be disabled either and may start automatically") {
		t.Errorf("error %q does not report the failed disable", err)
	}
	if strings.Contains(err.Error(), "restore it with") {
		t.Errorf("error %q tells the operator to restore a service that could not be disabled", err)
	}
}

// TestServiceOwnsOccupiedAddressWithoutAProbe checks that an address is not
// treated as this service's without a probe.
func TestServiceOwnsOccupiedAddressWithoutAProbe(t *testing.T) {
	if serviceOwnsOccupiedAddress(Options{GOOS: "linux"}, "") {
		t.Error("serviceOwnsOccupiedAddress = true without a probe")
	}
}

// TestListenOccupantDescribe checks the description of each listen occupant
// shape.
func TestListenOccupantDescribe(t *testing.T) {
	cases := []struct {
		name string
		in   listenOccupant
		want string
	}{
		{"nothing to name", listenOccupant{}, ""},
		{"path", listenOccupant{pid: 4321, path: "/usr/local/bin/peers"}, `"/usr/local/bin/peers" (pid 4321)`},
		{"deleted path", listenOccupant{pid: 4321, path: "/usr/local/bin/peers (deleted)"}, `"/usr/local/bin/peers" (deleted) (pid 4321)`},
		{"pid only", listenOccupant{pid: 42}, "the process with pid 42"},
		{"non-positive pid", listenOccupant{pid: -1}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.in.describe(); got != tc.want {
				t.Errorf("describe() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestDescribeOccupantsWithoutDescribableParts checks that occupants with
// nothing to name produce an empty description.
func TestDescribeOccupantsWithoutDescribableParts(t *testing.T) {
	occupants := []listenOccupant{{}, {pid: 0}}
	if got := describeOccupants(occupants); got != "" {
		t.Errorf("describeOccupants = %q, want an empty description", got)
	}
}

// TestFindListenOccupantIgnoresMalformedAddresses checks that malformed
// addresses and an unknown platform yield no occupant while a matching listener
// is found.
func TestFindListenOccupantIgnoresMalformedAddresses(t *testing.T) {
	const header = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"
	procRoot := t.TempDir()
	writeProcFile(t, procRoot, "net/tcp", header+
		"   0: 0100007F:4423 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 424242 1\n")
	writeProcFile(t, procRoot, "net/tcp6", header)
	for _, holder := range []struct{ pid, inode string }{
		{"4101", "424242"},
	} {
		fdDir := filepath.Join(procRoot, holder.pid, "fd")
		if err := os.MkdirAll(fdDir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("socket:["+holder.inode+"]", filepath.Join(fdDir, "3")); err != nil {
			t.Fatal(err)
		}
	}
	opts := Options{GOOS: "linux", ProcRoot: procRoot}

	got := findListenOccupant(opts, "127.0.0.1:17443")
	if len(got) != 1 || got[0].pid != 4101 {
		t.Fatalf("findListenOccupant(127.0.0.1:17443) = %+v, want the listener with pid 4101", got)
	}
	for _, addr := range []string{"nocolon", "127.0.0.1:notaport", "127.0.0.1:70000", ":0"} {
		if got := findListenOccupant(opts, addr); got != nil {
			t.Errorf("findListenOccupant(%q) = %+v, want nil", addr, got)
		}
	}
	unknown := Options{GOOS: "freebsd"}
	if got := findListenOccupant(unknown, "127.0.0.1:17443"); got != nil {
		t.Errorf("findListenOccupant on an unknown platform = %+v, want nil", got)
	}
}

// TestOccupantFromProcWithoutAProcess checks that a listening socket with no
// matching process yields no occupant.
func TestOccupantFromProcWithoutAProcess(t *testing.T) {
	procRoot := t.TempDir()
	const header = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"
	writeProcFile(t, procRoot, "net/tcp", header+
		"   0: 0100007F:4423 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 424242 1\n")
	writeProcFile(t, procRoot, "net/tcp6", header)
	if got := occupantFromProc(procRoot, net.ParseIP("127.0.0.1"), 17443); got != nil {
		t.Errorf("occupantFromProc = %+v, want nil without a matching process", got)
	}
}

// TestListeningInodesSkipsAForeignLine checks that a malformed table line is
// skipped while the matching inode is returned.
func TestListeningInodesSkipsAForeignLine(t *testing.T) {
	procRoot := t.TempDir()
	const header = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"
	table := header +
		"   0: nonsense 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 111111 1\n" +
		"   1: 0100007F:4423 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 424242 1\n"
	writeProcFile(t, procRoot, "net/tcp", table)
	got := listeningInodes(filepath.Join(procRoot, "net", "tcp"), net.ParseIP("127.0.0.1"), 17443)
	if !slices.Equal(got, []string{"424242"}) {
		t.Errorf("listeningInodes = %v, want [424242]", got)
	}
}

// TestProcAddressRejectsMalformedFields checks that malformed kernel address
// fields are rejected.
func TestProcAddressRejectsMalformedFields(t *testing.T) {
	if ip, ok := procAddress("zz"); ok || ip != nil {
		t.Errorf("procAddress(zz) = %v, %v, want nil, false", ip, ok)
	}
	if ip, ok := procAddress("0000"); ok || ip != nil {
		t.Errorf("procAddress(0000) = %v, %v, want nil, false", ip, ok)
	}
	if ip, ok := procAddress("0100007F"); !ok || ip.String() != "127.0.0.1" {
		t.Errorf("procAddress(0100007F) = %v, %v, want 127.0.0.1, true", ip, ok)
	}
}

// TestListenAddressConflictsBetweenFamilies checks which addresses conflict
// across the IPv4 and IPv6 families.
func TestListenAddressConflictsBetweenFamilies(t *testing.T) {
	cases := []struct {
		name  string
		local net.IP
		want  net.IP
		wantR bool
	}{
		{"IPv4 request, IPv6 listener", net.ParseIP("::"), net.ParseIP("127.0.0.1"), true},
		{"IPv4 request, specific IPv6 listener", net.ParseIP("::1"), net.ParseIP("127.0.0.1"), false},
		{"IPv6 request, IPv4 listener", net.ParseIP("127.0.0.1"), net.ParseIP("::1"), false},
		{"IPv6 request, wildcard IPv6 listener", net.ParseIP("::"), net.ParseIP("::1"), true},
		{"IPv6 request, another IPv6 listener", net.ParseIP("2001:db8::1"), net.ParseIP("::1"), false},
		{"wildcard request", net.ParseIP("10.0.0.1"), nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := listenAddressConflicts(tc.local, tc.want); got != tc.wantR {
				t.Errorf("listenAddressConflicts(%v, %v) = %v, want %v", tc.local, tc.want, got, tc.wantR)
			}
		})
	}
}

// TestOccupantFromLsofWithoutARunner checks that occupantFromLsof returns
// nothing without a command runner.
func TestOccupantFromLsofWithoutARunner(t *testing.T) {
	if got := occupantFromLsof(Options{GOOS: "darwin"}, 17443); got != nil {
		t.Errorf("occupantFromLsof = %+v, want nil without a command runner", got)
	}
}

// TestOccupantFromLsofSkipsWithoutLsof checks that occupantFromLsof runs
// nothing when lsof is not installed.
func TestOccupantFromLsofSkipsWithoutLsof(t *testing.T) {
	ran := false
	opts := Options{
		GOOS:      "darwin",
		LookPath:  func(string) (string, error) { return "", errors.New("not found") },
		RunOutput: func(string, ...string) ([]byte, error) { ran = true; return nil, nil },
	}
	if got := occupantFromLsof(opts, 17443); got != nil {
		t.Errorf("occupantFromLsof = %+v, want nil without lsof", got)
	}
	if ran {
		t.Error("occupantFromLsof ran lsof although it could not be found")
	}
}

// TestOccupantFromLsofIgnoresAFailedQuery checks that a failed lsof query
// yields nothing while a successful one returns the listener.
func TestOccupantFromLsofIgnoresAFailedQuery(t *testing.T) {
	const table = "COMMAND   PID USER   FD   TYPE DEVICE SIZE/OFF NODE NAME\n" +
		"peers    4242 alice  5u  IPv4 0x1234      0t0  TCP 127.0.0.1:17443 (LISTEN)\n"
	query := func(err error) func(string, ...string) ([]byte, error) {
		return func(_ string, args ...string) ([]byte, error) {
			if slices.Contains(args, "-iTCP:17443") {
				return []byte(table), err
			}
			return []byte("f3\nn/usr/local/bin/peers\n"), nil
		}
	}
	opts := Options{GOOS: "darwin", RunOutput: query(errors.New("lsof failed"))}
	if got := occupantFromLsof(opts, 17443); got != nil {
		t.Errorf("occupantFromLsof = %+v, want nil for a failed query", got)
	}

	opts.RunOutput = query(nil)
	got := occupantFromLsof(opts, 17443)
	if len(got) != 1 || got[0].pid != 4242 || got[0].path != "/usr/local/bin/peers" {
		t.Fatalf("occupantFromLsof = %+v, want pid 4242 with its executable from the same output", got)
	}
}

// TestRunOccupantCommandReturnsStandardOutput checks that runOccupantCommand
// returns standard output and the command failure.
func TestRunOccupantCommandReturnsStandardOutput(t *testing.T) {
	out, err := runOccupantCommand("sh", "-c", "printf hi")
	if err != nil || string(out) != "hi" {
		t.Errorf("runOccupantCommand = %q, %v, want \"hi\", nil", out, err)
	}
	if out, err := runOccupantCommand("sh", "-c", "exit 3"); err == nil || len(out) != 0 {
		t.Errorf("runOccupantCommand(exit 3) = %q, %v, want the failure with no output", out, err)
	}
}

// TestLsofPIDWithoutAPIDColumn checks that lsof output without a PID column
// yields no pids.
func TestLsofPIDWithoutAPIDColumn(t *testing.T) {
	out := "COMMAND USER FD\nalice 5u IPv4\n"
	if pids := lsofPID(out); pids != nil {
		t.Errorf("lsofPID = %v, want nil without a PID column", pids)
	}
}

// TestAbandonInstallReportsAStopFailureWithoutADisableFailure checks that a
// failed stop and a successful disable are both reported.
func TestAbandonInstallReportsAStopFailureWithoutADisableFailure(t *testing.T) {
	opts := Options{
		GOOS:   "darwin",
		UID:    501,
		Home:   "/Users/alice",
		System: false,
		EUID:   501,
		Run: func(name string, args ...string) ([]byte, error) {
			if slices.Contains(args, "bootout") {
				return []byte("bootout failed"), errors.New("exit status 1")
			}
			return nil, nil
		},
	}
	res := Result{}
	err := abandonInstall(opts, "gui/501", &res, false, errors.New("startup failed"))
	if err == nil {
		t.Fatal("abandonInstall returned nil")
	}
	for _, want := range []string{"the service could not be stopped", "it is disabled and will not start automatically"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	if !slices.Contains(res.Commands, "launchctl disable gui/501/"+Label) {
		t.Errorf("Commands = %q, want the disable command", res.Commands)
	}
}

// TestStopAndDisableOnAnUnknownPlatform checks that stopAndDisable does nothing
// on an unsupported platform.
func TestStopAndDisableOnAnUnknownPlatform(t *testing.T) {
	stopErr, disableErr := stopAndDisable(Options{GOOS: "freebsd"}, "", &Result{})
	if stopErr != nil || disableErr != nil {
		t.Errorf("stopAndDisable = %v, %v, want nil, nil", stopErr, disableErr)
	}
}

// TestRecoveryCommandCoversPlatformsAndLevels checks the recovery command for
// each platform and service level.
func TestRecoveryCommandCoversPlatformsAndLevels(t *testing.T) {
	cases := []struct {
		name string
		opts Options
		want string
	}{
		{
			"linux system",
			Options{GOOS: "linux", System: true},
			"sudo systemctl enable --now " + UnitName,
		},
		{
			"linux user",
			Options{GOOS: "linux", System: false},
			"systemctl --user enable --now " + UnitName,
		},
		{
			"darwin system",
			Options{GOOS: "darwin", System: true},
			"sudo launchctl enable system/" + Label + " && sudo launchctl bootstrap system /Library/LaunchDaemons/" + Label + ".plist",
		},
		{
			"darwin user",
			Options{GOOS: "darwin", UID: 501, Home: "/Users/alice"},
			"launchctl enable gui/501/" + Label + " && launchctl bootstrap gui/501 /Users/alice/Library/LaunchAgents/" + Label + ".plist",
		},
		{"unknown platform", Options{GOOS: "freebsd"}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := recoveryCommand(tc.opts, serviceDomain(tc.opts.System, tc.opts.UID)); got != tc.want {
				t.Errorf("recoveryCommand = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestServiceMainPIDFailures checks that a failed query, a missing pid and a
// zero pid each report no main pid.
func TestServiceMainPIDFailures(t *testing.T) {
	linuxErr := Options{GOOS: "linux", Run: func(string, ...string) ([]byte, error) {
		return []byte("4242\n"), errors.New("systemctl failed")
	}}
	if pid, ok := serviceMainPID(linuxErr, ""); ok || pid != 0 {
		t.Errorf("serviceMainPID(linux failure) = %d, %v, want 0, false", pid, ok)
	}

	noPID := Options{GOOS: "darwin", Run: func(string, ...string) ([]byte, error) {
		return []byte("state = running\n"), nil
	}}
	if pid, ok := serviceMainPID(noPID, "gui/501"); ok || pid != 0 {
		t.Errorf("serviceMainPID(no pid line) = %d, %v, want 0, false", pid, ok)
	}

	zeroPID := Options{GOOS: "darwin", Run: func(string, ...string) ([]byte, error) {
		return []byte("pid = 0\n"), nil
	}}
	if pid, ok := serviceMainPID(zeroPID, "gui/501"); ok || pid != 0 {
		t.Errorf("serviceMainPID(pid 0) = %d, %v, want 0, false", pid, ok)
	}

	if pid, ok := serviceMainPID(Options{GOOS: "freebsd"}, ""); ok || pid != 0 {
		t.Errorf("serviceMainPID(unknown platform) = %d, %v, want 0, false", pid, ok)
	}
}

// TestInstallDarwinReportsAnAddressThatStaysOccupied checks that when the old
// job unloads but broker.listen is still held, the installation reports the
// occupied address and removes the plist it wrote.
func TestInstallDarwinReportsAnAddressThatStaysOccupied(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.EUID = 501
	opts.UID = 501
	opts.Home = "/Users/alice"
	opts.StopTimeout = 5 * time.Millisecond
	writeListenConfig(t, opts, "127.0.0.1:17443")
	bootedOut := false
	opts.Run = func(name string, args ...string) ([]byte, error) {
		if name != "launchctl" {
			return nil, nil
		}
		switch {
		case len(args) == 2 && args[0] == "print" && args[1] == "gui/501":
			return []byte("domain\n"), nil
		case len(args) == 2 && args[0] == "print" && args[1] == "gui/501/"+Label:
			if bootedOut {
				return []byte("Could not find service"), exitError(t, 113)
			}
			return []byte("pid = 4242\n"), nil
		case slices.Contains(args, "bootout"):
			bootedOut = true
		}
		return nil, nil
	}
	opts.Probe = func(string) (int, bool) { return 4242, true }
	opts.ListenProbe = func(string) error { return syscall.EADDRINUSE }

	_, err := Install(opts)
	if err == nil {
		t.Fatal("Install succeeded while broker.listen stayed occupied")
	}
	for _, want := range []string{"already in use", "the old service was unloaded and no Broker is running now", "the plist was removed"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	plist := rooted(opts, "/Users/alice/Library/LaunchAgents/"+Label+".plist")
	if _, statErr := os.Stat(plist); !errors.Is(statErr, fs.ErrNotExist) {
		t.Errorf("plist %s exists after the failure: %v", plist, statErr)
	}
}

// TestInstallDarwinReportsAnEnableFailure checks that a failed launchctl enable
// is reported and the plist is removed.
func TestInstallDarwinReportsAnEnableFailure(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.EUID = 501
	opts.UID = 501
	opts.Home = "/Users/alice"
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		if name != "launchctl" {
			return nil, nil
		}
		switch {
		case len(args) == 2 && args[0] == "print" && args[1] == "gui/501":
			return []byte("domain\n"), nil
		case len(args) == 2 && args[0] == "print" && args[1] == "gui/501/"+Label:
			return []byte("Could not find service"), exitError(t, 113)
		case slices.Contains(args, "enable"):
			return []byte("enable failed"), errors.New("exit status 1")
		}
		return nil, nil
	}
	opts.Probe = func(string) (int, bool) { return 0, false }

	_, err := Install(opts)
	if err == nil {
		t.Fatal("Install succeeded although launchctl enable failed")
	}
	for _, want := range []string{"cannot run launchctl enable", "no Broker is running now", "the plist was removed"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	if slices.ContainsFunc(cmds, func(cmd string) bool { return strings.Contains(cmd, "bootstrap") }) {
		t.Errorf("commands = %q, want no bootstrap after enable failed", cmds)
	}
}

// TestLaunchctlDetail checks the detail launchctlDetail extracts from output
// and errors.
func TestLaunchctlDetail(t *testing.T) {
	cases := []struct {
		name string
		err  error
		out  []byte
		want string
	}{
		{"error without output", errors.New("boom"), nil, "boom"},
		{"error with blank output", errors.New("boom"), []byte("  \n"), "boom"},
		{"first output line", nil, []byte("first line\nsecond line\n"), "first line"},
		{"no detail", nil, []byte("   "), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := launchctlDetail(tc.err, tc.out); got != tc.want {
				t.Errorf("launchctlDetail = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestInstallHintForTheSystemLevel checks the install hint for the system and
// user levels.
func TestInstallHintForTheSystemLevel(t *testing.T) {
	if got, want := installHint(Options{System: true}), "sudo peers service install --system"; got != want {
		t.Errorf("installHint(system) = %q, want %q", got, want)
	}
	if got, want := installHint(Options{}), "peers service install"; got != want {
		t.Errorf("installHint(user) = %q, want %q", got, want)
	}
}

// TestListenProbeDefaultsToConfigProbeListen checks that listenProbe falls back
// to config.ProbeListen.
func TestListenProbeDefaultsToConfigProbeListen(t *testing.T) {
	got := listenProbe(Options{})
	if reflect.ValueOf(got).Pointer() != reflect.ValueOf(config.ProbeListen).Pointer() {
		t.Error("listenProbe without a callback did not return config.ProbeListen")
	}
}

// TestWaitReadyUsesTheDefaultBound checks that waitReady uses the default
// readiness bound without an override.
func TestWaitReadyUsesTheDefaultBound(t *testing.T) {
	opts := baseOptions(t)
	opts.ReadyTimeout = 0
	opts.Probe = func(string) (int, bool) { return 4242, true }
	opts.Run = func(name string, args ...string) ([]byte, error) {
		if slices.Contains(args, "show") {
			return []byte("4242\n"), nil
		}
		return nil, nil
	}
	deadline, err := waitReady(opts, "", time.Now())
	if err != nil {
		t.Fatalf("waitReady: %v", err)
	}
	if remaining := time.Until(deadline); remaining < readyBound/2 || remaining > readyBound+time.Second {
		t.Errorf("readiness deadline is %v away, want about %v", remaining, readyBound)
	}
}

// TestWaitStatusUsesTheDefaultProbe checks that waitStatus reports a failed
// default probe naming the socket.
func TestWaitStatusUsesTheDefaultProbe(t *testing.T) {
	runDir := filepath.Join(t.TempDir(), "run")
	old := runtimeDirFor
	runtimeDirFor = func(int) string { return runDir }
	t.Cleanup(func() { runtimeDirFor = old })

	opts := baseOptions(t)
	opts.StatusProbe = nil
	state, degraded, err := waitStatus(opts, time.Now().Add(-time.Millisecond))
	socket := filepath.Join(runDir, "broker.sock")
	if err == nil || !strings.Contains(err.Error(), socket) {
		t.Fatalf("waitStatus = %+v, %v, nil, want the probe failure naming %s", state, degraded, socket)
	}
	if degraded {
		t.Error("waitStatus reported degraded after a failed probe")
	}
}

// TestServiceManagerStateReportsMissingFields checks that a state output
// without fields and an unsupported platform are each reported.
func TestServiceManagerStateReportsMissingFields(t *testing.T) {
	darwin := Options{GOOS: "darwin", Run: func(string, ...string) ([]byte, error) {
		return []byte("nothing useful\n"), nil
	}}
	if _, err := serviceManagerState(darwin, "gui/501"); err == nil || !strings.Contains(err.Error(), "printed no state or last exit code") {
		t.Errorf("serviceManagerState(darwin) error = %v, want the missing-fields failure", err)
	}
	if _, err := serviceManagerState(Options{GOOS: "freebsd"}, ""); err == nil || !strings.Contains(err.Error(), "cannot read the service manager state") {
		t.Errorf("serviceManagerState(unknown) error = %v, want the platform failure", err)
	}
}

// TestStartupFailureReasonIgnoresAJournalFailure checks that a journal query
// that fails yields no reason, while the same output without the error yields
// the record's cause.
func TestStartupFailureReasonIgnoresAJournalFailure(t *testing.T) {
	since := time.Now().Add(-time.Minute)
	record := `{"time":"` + time.Now().Format(time.RFC3339Nano) + `","event":"startup_failed","error":"the real cause"}`
	var called bool
	opts := Options{GOOS: "linux", Run: func(name string, args ...string) ([]byte, error) {
		if name != "journalctl" {
			t.Errorf("Run(%q), want journalctl", name)
		}
		called = true
		return []byte(record), errors.New("journalctl failed")
	}}
	if got := startupFailureReason(opts, since); got != "" {
		t.Errorf("startupFailureReason = %q, want an empty reason when the query fails", got)
	}
	if !called {
		t.Error("startupFailureReason did not query the journal")
	}

	opts.Run = func(string, ...string) ([]byte, error) {
		return []byte(record), nil
	}
	if got := startupFailureReason(opts, since); got != "the real cause" {
		t.Errorf("startupFailureReason = %q, want the record's cause", got)
	}
}

// TestLastStartupFailureSkipsUnusableRecords checks that unusable records are
// skipped and the most recent usable cause is returned.
func TestLastStartupFailureSkipsUnusableRecords(t *testing.T) {
	since := time.Now()
	at := func(offset time.Duration) string {
		return since.Add(offset).Format(time.RFC3339Nano)
	}
	log := strings.Join([]string{
		`{"time":"` + at(3*time.Second) + `","event":"startup_failed","error":"the real cause"}`,
		`{"startup_failed"`,
		`{"time":"` + at(2*time.Second) + `","event":"startup_ok","error":"wrong event"}`,
		`{"time":"` + at(time.Second) + `","event":"startup_failed"}`,
	}, "\n")
	if got := lastStartupFailure(log, since); got != "the real cause" {
		t.Errorf("lastStartupFailure = %q, want the most recent usable record", got)
	}
}

// TestRootedWithoutARoot checks that rooted prefixes a path only when a root is
// set.
func TestRootedWithoutARoot(t *testing.T) {
	if got := rooted(Options{}, "/etc/peers/config.yaml"); got != "/etc/peers/config.yaml" {
		t.Errorf("rooted without a root = %q, want the path unchanged", got)
	}
	if got := rooted(Options{Root: "/tmp/root"}, "/etc/peers/config.yaml"); got != "/tmp/root/etc/peers/config.yaml" {
		t.Errorf("rooted with a root = %q, want the path under the root", got)
	}
}

// TestWriteUnitRejectsAnUnsupportedPlatform checks that writeUnit refuses a
// platform without a unit path.
func TestWriteUnitRejectsAnUnsupportedPlatform(t *testing.T) {
	if _, err := writeUnit(Options{GOOS: "freebsd"}); err == nil || !strings.Contains(err.Error(), `cannot find the unit path on platform "freebsd"`) {
		t.Fatalf("writeUnit error = %v, want the unit path failure", err)
	}
}

// TestWriteUnitReportsAnInvalidExecutable checks that an executable with a
// control character is refused.
func TestWriteUnitReportsAnInvalidExecutable(t *testing.T) {
	opts := baseOptions(t)
	opts.Exe = "/usr/local/bin/bad\x01name"
	if _, err := writeUnit(opts); err == nil || !strings.Contains(err.Error(), "control character") {
		t.Fatalf("writeUnit error = %v, want the control character rejection", err)
	}
}

// TestWriteUnitReportsAnUnreadablePlist checks that an unreadable existing
// plist is reported.
func TestWriteUnitReportsAnUnreadablePlist(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.Home = "/Users/alice"
	opts.Exe = "/usr/local/bin/peers"
	plist := rooted(opts, filepath.Join(opts.Home, "Library", "LaunchAgents", Label+".plist"))
	if err := os.MkdirAll(plist, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := writeUnit(opts); err == nil || !strings.Contains(err.Error(), "cannot read the environment variables") {
		t.Fatalf("writeUnit error = %v, want the plist read failure", err)
	}
}

// TestWriteUnitReportsAnUnwritableDefinition checks that a unit path that
// cannot be written is reported.
func TestWriteUnitReportsAnUnwritableDefinition(t *testing.T) {
	opts := baseOptions(t)
	path, err := unitPath(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(rooted(opts, path), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := writeUnit(opts); err == nil || !strings.Contains(err.Error(), "cannot install") {
		t.Fatalf("writeUnit error = %v, want the atomic write failure", err)
	}
}

// TestCreateConfigWithFailures checks the directory and open failures of
// createConfigWith.
func TestCreateConfigWithFailures(t *testing.T) {
	t.Run("directory", func(t *testing.T) {
		opts := baseOptions(t)
		alice := filepath.Join(opts.Root, "home", "alice")
		if err := os.MkdirAll(alice, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(alice, ".peers"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		opened := false
		open := func(path string) (*os.File, error) {
			opened = true
			return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		}
		res := Result{}
		err := createConfigWith(opts, &res, -1, 0o600, open)
		if err == nil || !strings.Contains(err.Error(), "cannot create the configuration directory") {
			t.Fatalf("createConfigWith error = %v, want the directory failure", err)
		}
		if opened {
			t.Error("createConfigWith opened the file after the directory failed")
		}
	})

	t.Run("open", func(t *testing.T) {
		opts := baseOptions(t)
		openErr := errors.New("open exploded")
		open := func(string) (*os.File, error) { return nil, openErr }
		res := Result{}
		err := createConfigWith(opts, &res, -1, 0o600, open)
		if err == nil || !errors.Is(err, openErr) || !strings.Contains(err.Error(), "cannot create the configuration file") {
			t.Fatalf("createConfigWith error = %v, want the open failure", err)
		}
		if got, want := res.ConfigPath, configFilePath(opts); got != want {
			t.Errorf("ConfigPath = %q, want %q", got, want)
		}
	})
}

// TestCreateConfigRemovesTheFileWhenOwnershipFails checks that a failed
// ownership change removes the new configuration file.
func TestCreateConfigRemovesTheFileWhenOwnershipFails(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.Chown = func(string, int, int) error { return errors.New("chown refused") }
	open := func(path string) (*os.File, error) {
		return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o660)
	}
	res := Result{}
	err := createConfigWith(opts, &res, 4242, 0o660, open)
	if err == nil || !strings.Contains(err.Error(), "cannot set the owner of the configuration file") {
		t.Fatalf("createConfigWith error = %v, want the ownership failure", err)
	}
	if _, statErr := os.Stat(rooted(opts, configFilePath(opts))); !errors.Is(statErr, fs.ErrNotExist) {
		t.Errorf("configuration file exists after the ownership failure: %v", statErr)
	}
}

// TestCreateConfigRemovesTheFileWhenTheModeFails checks that a failed mode
// change removes the new configuration file.
func TestCreateConfigRemovesTheFileWhenTheModeFails(t *testing.T) {
	opts := baseOptions(t)
	opts.Chmod = func(string, fs.FileMode) error { return errors.New("chmod refused") }
	open := func(path string) (*os.File, error) {
		return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	}
	res := Result{}
	err := createConfigWith(opts, &res, -1, 0o600, open)
	if err == nil || !strings.Contains(err.Error(), "cannot set the mode of the configuration file") {
		t.Fatalf("createConfigWith error = %v, want the mode failure", err)
	}
	if _, statErr := os.Stat(rooted(opts, configFilePath(opts))); !errors.Is(statErr, fs.ErrNotExist) {
		t.Errorf("configuration file exists after the mode failure: %v", statErr)
	}
}

// TestSetConfigOwnerUsesTheRealChown checks that the default owner change goes
// through os.Chown, which a process without root is refused.
func TestSetConfigOwnerUsesTheRealChown(t *testing.T) {
	skipUnlessPermissionDenied(t)
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := setConfigOwner(Options{}, path, os.Getgid())
	if !errors.Is(err, fs.ErrPermission) {
		t.Errorf("setConfigOwner error = %v, want a permission failure", err)
	}
}

// TestCheckTempBinaryWithoutComparablePaths checks that checkTempBinary passes
// without a temp dir or with incomparable paths.
func TestCheckTempBinaryWithoutComparablePaths(t *testing.T) {
	if err := checkTempBinary(Options{}); err != nil {
		t.Errorf("checkTempBinary without a temp dir = %v, want nil", err)
	}
	if err := checkTempBinary(Options{TempDir: "/tmp", Exe: "relative/peers"}); err != nil {
		t.Errorf("checkTempBinary with incomparable paths = %v, want nil", err)
	}
}

// TestCheckSystemBinaryFailures checks each failure of checkSystemBinary and
// that a root-owned executable passes.
func TestCheckSystemBinaryFailures(t *testing.T) {
	ownerErr := errors.New("owner lookup failed")
	failing := Options{Exe: "/usr/local/bin/peers", Owner: func(string) (uint32, fs.FileMode, error) {
		return 0, 0, ownerErr
	}}
	if err := checkSystemBinary(failing); err == nil || !errors.Is(err, ownerErr) || !strings.Contains(err.Error(), `cannot inspect "/usr/local/bin/peers"`) {
		t.Errorf("checkSystemBinary(owner failure) = %v, want the owner lookup failure", err)
	}

	notExecutable := Options{Exe: "/usr/local/bin/peers", Owner: func(string) (uint32, fs.FileMode, error) {
		return 0, 0o640, nil
	}}
	if err := checkSystemBinary(notExecutable); err == nil || !strings.Contains(err.Error(), "not executable by other accounts") {
		t.Errorf("checkSystemBinary(mode 0640) = %v, want the executable bit failure", err)
	}

	safe := Options{Exe: "/usr/local/bin/peers", Owner: func(string) (uint32, fs.FileMode, error) {
		return 0, 0o755, nil
	}}
	if err := checkSystemBinary(safe); err != nil {
		t.Errorf("checkSystemBinary(0755) = %v, want nil", err)
	}
}

// TestWriteFileAtomicReportsAnUnusableDirectory checks that writeFileAtomic
// reports a directory it cannot write in.
func TestWriteFileAtomicReportsAnUnusableDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "file")
	err := writeFileAtomic(path, "content", 0o644)
	if err == nil || !strings.Contains(err.Error(), "cannot create a temporary file") {
		t.Fatalf("writeFileAtomic error = %v, want the temporary file failure", err)
	}
}
