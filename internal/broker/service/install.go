package service

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/wangzw/peers/internal/config"
)

const (
	systemConfigPath = "/etc/peers/config.yaml"
	systemdUnitDir   = "/etc/systemd/system"
	launchDaemonDir  = "/Library/LaunchDaemons"
	systemLogDir     = "/var/log/peers"

	// lsofPath is where macOS keeps lsof. The occupant lookup uses the
	// absolute path so it never depends on the caller's PATH.
	lsofPath = "/usr/sbin/lsof"
	// occupantCommandTimeout bounds one lsof run. The lookup is best effort and
	// must not delay a failed installation.
	occupantCommandTimeout = 2 * time.Second

	// readyBound bounds the wait for the local socket to answer after the
	// service starts. It covers the Broker's own startup and launchd's default
	// ThrottleInterval of 10 seconds, which launchd applies before relaunching
	// a process that exited too quickly.
	readyBound        = 20 * time.Second
	readyPollInterval = 100 * time.Millisecond
	// probeTimeout bounds one dial of the local socket.
	probeTimeout = 500 * time.Millisecond

	// serviceStopBound bounds one stop-and-replace sequence. On Linux the
	// listen precheck stops the service and waits this long for broker.listen.
	// On macOS the same budget covers the wait for launchd to finish tearing
	// the old job down and the wait for broker.listen; the two waits share one
	// deadline, so a slow unload does not add a second full budget. systemctl
	// stop and launchctl bootout return while the Broker is still shutting
	// down: systemd signals the process and launchd tears the job down
	// asynchronously, and the Broker then closes its listeners. The bound
	// covers launchd's exit timeout, the Broker's SIGTERM shutdown and
	// launchd's 10-second ThrottleInterval, with the same margin readyBound
	// leaves.
	serviceStopBound = 20 * time.Second
	// launchdUnloadPollInterval separates two launchctl print probes that wait
	// for the old job to disappear.
	launchdUnloadPollInterval = 100 * time.Millisecond
	// bootstrapTransientExit is the exit status launchctl reports when bootstrap
	// races the teardown of the same label.
	bootstrapTransientExit = 5
	// bootstrapRetryBound bounds the retries of a bootstrap that failed with
	// bootstrapTransientExit. It starts after the unload wait, so it must not
	// take the rest of serviceStopBound: when the old job takes nearly the
	// whole stop budget to disappear, that deadline is already spent and the
	// retry launchd still needs would fail at once. The separate bound states
	// the worst case explicitly: serviceStopBound 20s + bootstrapRetryBound
	// 10s + readyBound 20s = 50s for one reinstall, below the end-to-end
	// reinstall bound of 75s. 10s is launchd's ThrottleInterval, the pause the
	// stop budget's rationale already accounts for before the same label loads
	// again.
	bootstrapRetryBound = 10 * time.Second
	// bootstrapRetryAttempts caps the retries of a bootstrap that failed with
	// bootstrapTransientExit. bootstrapRetryBound normally stops the loop
	// first; the cap only bounds a clock that advances slower than the sleep.
	bootstrapRetryAttempts = 100
	bootstrapRetryInterval = 200 * time.Millisecond
)

// launchdPIDPattern matches the "pid = <n>" line of launchctl print.
var launchdPIDPattern = regexp.MustCompile(`(?m)^\s*pid = (\d+)$`)

// launchdStatePattern and launchdLastExitCodePattern match the two fields of
// launchctl print that explain a service that did not start.
var (
	launchdStatePattern        = regexp.MustCompile(`(?m)^\s*state = (.+)$`)
	launchdLastExitCodePattern = regexp.MustCompile(`(?m)^\s*last exit code = (.+)$`)
)

// Options is everything Install needs. DefaultOptions fills it from the running
// process; tests fill it by hand and set Root to a temporary directory.
type Options struct {
	System               bool
	GOOS                 string                                                                  // runtime.GOOS
	EUID                 int                                                                     // os.Geteuid()
	UID                  int                                                                     // os.Getuid(), used in the start hint
	Exe                  string                                                                  // absolute path of os.Executable()
	Home                 string                                                                  // account home directory
	XDGConfigHome        string                                                                  // XDG_CONFIG_HOME
	TempDir              string                                                                  // os.TempDir()
	Root                 string                                                                  // filesystem prefix for tests; empty in production
	ProcRoot             string                                                                  // Linux: /proc tree used to find a port's occupant; empty uses /proc
	AdminGroup           string                                                                  // group that administers a system Broker; empty uses DefaultAdminGroup
	LookPath             func(file string) (string, error)                                       // exec.LookPath
	Run                  func(name string, args ...string) ([]byte, error)                       // run a command, returning combined output
	RunOutput            func(name string, args ...string) ([]byte, error)                       // run a command, returning only its standard output; nil uses Run
	GroupGID             func(name string) (gid int, exists bool, err error)                     // group id lookup; nil queries the platform through Run
	GroupMembers         func(name string) ([]string, error)                                     // group member list; nil queries the platform through Run
	PrimaryGroupAccounts func(gid int) ([]string, error)                                         // accounts whose primary group is gid; nil queries the platform through Run
	Owner                func(path string) (uid uint32, mode fs.FileMode, err error)             // owner and permission bits, not following symlinks
	Chown                func(path string, uid, gid int) error                                   // ownership setter for the configuration file; nil uses os.Chown in production
	Chmod                func(path string, mode fs.FileMode) error                               // permission setter for the configuration file; nil uses os.Chmod
	EvalSymlinks         func(path string) (string, error)                                       // filepath.EvalSymlinks
	Probe                func(socketPath string) (pid int, answering bool)                       // peer pid of the Broker answering on the local socket
	ListenProbe          func(addr string) error                                                 // whether broker.listen can be bound; nil uses config.ProbeListen
	StatusProbe          func(socketPath string) (state DegradedState, degraded bool, err error) // Status over the local socket; nil uses the default set by DefaultOptions
	ReadyTimeout         time.Duration                                                           // wait for the local socket after start; zero uses readyBound
	StopTimeout          time.Duration                                                           // one stop-and-replace budget; zero uses serviceStopBound
	BootstrapTimeout     time.Duration                                                           // wait for a bootstrap that keeps failing with exit 5; zero uses bootstrapRetryBound

	PPID            func() int                               // caller pid for the launchd helper; nil: os.Getppid
	StartDetached   func(cmd *exec.Cmd) error                // start the launchd helper and release it; nil: start it in its own session
	WaitProcessExit func(pid int, bound time.Duration) error // wait for a process to exit; nil: poll kill(2)
	Logf            func(format string, args ...any)         // write one helper log line; nil: drop it
}

// Result describes what Install or Uninstall did. On error it lists the steps
// that already finished.
type Result struct {
	UnitPath   string // written or removed unit file or plist
	ConfigPath string // configuration file for the install's level
	ConfigNew  bool   // this run created the configuration file
	AdminGroup string // system: group delegated administration; empty for the user installation
	// AdminGroupCreated reports that this run created the administration
	// group, as opposed to reusing one that already existed.
	AdminGroupCreated bool
	// AdminGroupMembers lists the members the account database reports for the
	// administration group, sorted by name: supplementary members and, on
	// macOS, nested groups, plus accounts whose primary group is the
	// administration group. It is empty when the group was created by this run,
	// lists no members, or was not queried.
	AdminGroupMembers []string
	// AdminGroupMembersQueried reports whether this run read the member list.
	// Naming the group with --admin-group or keeping an existing configuration
	// file settles the decision without it, so the list is not read.
	AdminGroupMembersQueried bool
	Commands                 []string // commands that changed state, in order
	Domain                   string   // macOS: the launchd domain the service was loaded into or out of
	Removed                  bool     // Uninstall: a unit file or plist was removed

	Binary   string // Install: the executable the service definition starts
	RunLog   string // Install: where the Broker's run log is read, as logHint gives it
	AuditLog string // Install: the level's audit log path
}

// DefaultOptions fills Options from the running process.
func DefaultOptions(system bool) (Options, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Options{}, fmt.Errorf("cannot locate the home directory: %w", err)
	}
	return defaultOptions(system, home)
}

// PostUpdateOptions fills the Options PostUpdate needs for the level a process
// of this host runs at. The system installation needs no home directory, so a system
// service whose environment has no HOME can still rewrite its definition; the
// user installation reads the account home.
func PostUpdateOptions(system bool) (Options, error) {
	home := ""
	if !system {
		var err error
		home, err = os.UserHomeDir()
		if err != nil {
			return Options{}, fmt.Errorf("cannot locate the home directory: %w", err)
		}
	}
	return defaultOptions(system, home)
}

// defaultOptions builds the default Options for a level and an account home.
// home is empty for the system installation, whose paths never use it.
func defaultOptions(system bool, home string) (Options, error) {
	exe, err := os.Executable()
	if err != nil {
		return Options{}, fmt.Errorf("cannot locate the executable: %w", err)
	}
	exe, err = filepath.Abs(exe)
	if err != nil {
		return Options{}, fmt.Errorf("cannot make the executable path absolute: %w", err)
	}
	return Options{
		System:        system,
		GOOS:          runtime.GOOS,
		EUID:          os.Geteuid(),
		UID:           os.Getuid(),
		Exe:           exe,
		Home:          home,
		XDGConfigHome: os.Getenv("XDG_CONFIG_HOME"),
		TempDir:       os.TempDir(),
		LookPath:      exec.LookPath,
		Run: func(name string, args ...string) ([]byte, error) {
			return exec.Command(name, args...).CombinedOutput()
		},
		RunOutput: runOccupantCommand,
		Owner: func(path string) (uint32, fs.FileMode, error) {
			fi, err := os.Lstat(path)
			if err != nil {
				return 0, 0, err
			}
			st, ok := fi.Sys().(*syscall.Stat_t)
			if !ok {
				return 0, 0, fmt.Errorf("no owner information for %s", path)
			}
			return st.Uid, fi.Mode(), nil
		},
		EvalSymlinks: filepath.EvalSymlinks,
		StatusProbe:  defaultStatusProbe,
		Probe: func(socketPath string) (int, bool) {
			conn, err := net.DialTimeout("unix", socketPath, probeTimeout)
			if err != nil {
				return 0, false
			}
			defer func() { _ = conn.Close() }()
			unixConn, ok := conn.(*net.UnixConn)
			if !ok {
				return 0, true
			}
			pid, err := socketPeerPID(unixConn)
			if err != nil {
				// A Broker is answering even though its pid is unknown; report
				// it so the caller does not take it for the service's process.
				return 0, true
			}
			return pid, true
		},
	}, nil
}

// Install performs the installation; on error the Result lists what was
// already done.
func Install(opts Options) (Result, error) {
	var res Result
	// Records older than this describe an earlier run. The readiness timeout
	// only reports a startup_failed record written after this point.
	installStart := time.Now()

	switch opts.GOOS {
	case "linux", "darwin":
	default:
		return res, fmt.Errorf("cannot install the service on platform %q", opts.GOOS)
	}
	if opts.System && opts.EUID != 0 {
		return res, errors.New("service install --system must run as root")
	}
	if !opts.System && opts.EUID == 0 {
		return res, errors.New("service install without --system must not run as root; the user unit belongs to one account")
	}
	if !filepath.IsAbs(opts.Exe) {
		return res, fmt.Errorf("cannot install the service: the executable %q is not an absolute path", opts.Exe)
	}
	// A system unit must name the resolved executable. On macOS os.Executable
	// does not resolve symbolic links, so writing the path as given could point
	// the root-owned daemon at a link inside an account's directory, which that
	// account can retarget after the ownership check.
	if opts.System {
		real, err := opts.EvalSymlinks(opts.Exe)
		if err != nil {
			return res, fmt.Errorf("cannot resolve the executable %q: %w", opts.Exe, err)
		}
		opts.Exe = real
	}
	if err := checkTempBinary(opts); err != nil {
		return res, err
	}
	if opts.System {
		if err := checkSystemBinary(opts); err != nil {
			return res, err
		}
	}
	if opts.GOOS == "linux" {
		if _, err := opts.LookPath("systemctl"); err != nil {
			return res, fmt.Errorf("cannot find systemctl (is systemd installed?): %w", err)
		}
	}

	// The launchd domain decides both where the service is loaded and which
	// instance counts as this service's own when a Broker is already answering.
	domain := ""
	if opts.GOOS == "darwin" {
		var err error
		domain, err = launchdDomain(opts)
		if err != nil {
			return res, err
		}
	}
	if err := checkExistingBroker(opts, domain); err != nil {
		return res, err
	}

	// The administration group is resolved before any file is written, so a
	// group that cannot be created fails the installation without leaving a
	// configuration file behind.
	adminGID := -1
	if opts.System {
		gid, err := ensureAdminGroup(opts, &res)
		if err != nil {
			return res, err
		}
		adminGID = gid
		res.AdminGroup = adminGroupName(opts)
		if res.AdminGroupCreated {
			// Record the group before anything else: uninstall may only
			// remove a group whose creation this installation recorded.
			if err := recordAdminGroup(opts, res.AdminGroup); err != nil {
				return res, err
			}
		}
	}

	// The configuration file comes first: the listen precheck reads it, and a
	// failure later in the installation keeps it for the next run.
	if err := createConfig(opts, &res, adminGID); err != nil {
		return res, err
	}

	// macOS writes the plist before the precheck, so the replacement boots out
	// and bootstraps the same definition. Linux writes the unit after the
	// precheck, which there stops the service synchronously.
	unit := ""
	plistExisted := false
	if opts.GOOS == "darwin" {
		path, err := unitPath(opts)
		if err != nil {
			return res, err
		}
		if _, statErr := os.Lstat(rooted(opts, path)); statErr == nil {
			plistExisted = true
		} else if !errors.Is(statErr, fs.ErrNotExist) {
			return res, fmt.Errorf("cannot inspect %s: %w", path, statErr)
		}
		if _, err := writeUnit(opts); err != nil {
			return res, err
		}
		unit = path
		res.UnitPath = path
	}

	// The address is checked before the service is registered or started, so an
	// address that cannot be used fails the installation without leaving a
	// service that restarts forever or a plist that launchd loads at the next
	// login. The precheck identifies an occupant; only Linux stops the service
	// here, because on macOS the replacement below boots it out once.
	check, err := precheckListen(opts, domain, &res)
	if err != nil {
		if opts.GOOS == "darwin" && !plistExisted {
			// The plist did not exist before this run, so removing it restores
			// the state the operator had and leaves nothing to load at login. A
			// pre-existing plist is left as it was.
			if rmErr := os.Remove(rooted(opts, unit)); rmErr != nil && !errors.Is(rmErr, fs.ErrNotExist) {
				return res, fmt.Errorf("%w; also could not remove %s: %v", err, unit, rmErr)
			}
			res.UnitPath = ""
		}
		return res, err
	}

	if opts.GOOS == "linux" {
		path, err := writeUnit(opts)
		if err != nil {
			if check.stopped {
				return res, abandonInstall(opts, domain, &res, true, err)
			}
			return res, err
		}
		unit = path
		res.UnitPath = path
	}

	var replaced bool
	switch opts.GOOS {
	case "linux":
		err = runSystemctl(opts, &res)
	case "darwin":
		replaced, err = runLaunchctl(opts, domain, unit, plistExisted, check, &res)
	}
	if err != nil {
		// runLaunchctl has already removed the plist or disabled the label, so
		// the generic cleanup is only for the Linux path.
		if opts.GOOS == "darwin" {
			return res, err
		}
		return res, abandonInstall(opts, domain, &res, check.stopped, err)
	}
	deadline, err := waitReady(opts, domain, installStart)
	if err != nil {
		stopped := check.stopped
		if opts.GOOS == "darwin" {
			stopped = replaced
		}
		return res, abandonInstall(opts, domain, &res, stopped, err)
	}

	// The service answers, so it is installed and running from here on. Fill
	// what the summary prints even when the Broker turns out to be degraded.
	res.Domain = domain
	res.Binary = opts.Exe
	res.RunLog = logHint(opts)
	res.AuditLog = auditLogPath(opts)

	state, degraded, err := waitStatus(opts, deadline)
	if err != nil {
		// The service is running but does not answer Status. Leave it alone:
		// disabling it would take down a Broker that is reachable.
		return res, fmt.Errorf("cannot read the status of the running service: %w; %s", err, statusHint(opts))
	}
	if degraded {
		return res, &DegradedError{State: state}
	}
	return res, nil
}

// unitPath returns the production path of the unit file or plist for opts.
func unitPath(opts Options) (string, error) {
	switch {
	case opts.GOOS == "linux" && !opts.System:
		return filepath.Join(configHome(opts), "systemd", "user", UnitName), nil
	case opts.GOOS == "linux" && opts.System:
		return filepath.Join(systemdUnitDir, UnitName), nil
	case opts.GOOS == "darwin" && !opts.System:
		return filepath.Join(opts.Home, "Library", "LaunchAgents", Label+".plist"), nil
	case opts.GOOS == "darwin" && opts.System:
		return filepath.Join(launchDaemonDir, Label+".plist"), nil
	default:
		return "", fmt.Errorf("cannot find the unit path on platform %q", opts.GOOS)
	}
}

// runtimeDirFor names the runtime directory of one account. Production uses
// config.RuntimeDir, so the service follows the same rule as the Broker and the
// MCP server, including the fallback location. Tests replace it to pin that
// localSocketPath follows the resolved directory rather than a fixed path.
var runtimeDirFor = config.RuntimeDir

// localSocketPath returns the local socket a running Broker of this deployment
// level binds. It matches the runtime directory config picks for the level,
// including the fallback location when another account has taken the preferred
// directory. The runtime directory is keyed by the effective account.
func localSocketPath(opts Options) string {
	if opts.System {
		if opts.GOOS == "darwin" {
			return "/var/run/peers/broker.sock"
		}
		return "/run/peers/broker.sock"
	}
	return filepath.Join(runtimeDirFor(opts.EUID), "broker.sock")
}

// checkExistingBroker refuses the installation when a Broker outside the
// peers service already answers on this level's local socket. Replacing a
// service-managed Broker is fine: the start step restarts it with the new
// binary. The Broker is the service's own only when the pid the service
// manager reports equals the socket's peer pid; a loaded unit alone proves
// nothing about which process owns the socket.
func checkExistingBroker(opts Options, domain string) error {
	path := localSocketPath(opts)
	brokerPID, answering := opts.Probe(path)
	if !answering {
		return nil
	}
	servicePID, managed := serviceMainPID(opts, domain)
	if managed && servicePID == brokerPID {
		return nil
	}
	level := "user Broker for this account"
	if opts.System {
		level = "system Broker"
	}
	return fmt.Errorf("cannot install the service: a %s is already answering on %s and was not started by the peers service (service pid %d, socket peer pid %d); stop it before installing", level, path, servicePID, brokerPID)
}

// listenCheck is what the listen precheck learned about this install's address.
type listenCheck struct {
	addr     string // broker.listen this install would bind; empty when it does not listen
	occupied bool   // this service's own Broker holds addr
	stopped  bool   // the precheck stopped the service to free addr (Linux)
}

// precheckListen refuses the installation when the address this install's
// configuration makes the Broker listen on cannot be bound. It reads the
// effective broker.listen from the configuration file, including a template
// this run just generated, and binds the address once through ListenProbe,
// which releases it again. An empty value means the Broker does not listen and
// passes without a probe.
//
// When the address is already in use, it first identifies the occupant. An
// install is also a reinstall, so the occupant may be this service's own
// running Broker; the service manager's main pid and the pid answering on the
// local socket identify that case, the same comparison the pre-install check
// and the update path use. Any other occupant fails the installation with the
// running service untouched.
//
// On Linux the precheck then stops the service and waits for the address, and
// reports that it stopped it. On macOS it stops nothing: the replacement boots
// the old job out once, after bootout, and waiting for the address before that
// would either boot it out twice or wait on a service that is still loaded.
func precheckListen(opts Options, domain string, res *Result) (listenCheck, error) {
	path := configFilePath(opts)
	addr, err := config.ListenFromFile(rooted(opts, path))
	if err != nil {
		return listenCheck{}, fmt.Errorf("cannot read broker.listen from %s: %w", path, err)
	}
	if addr == "" {
		return listenCheck{}, nil
	}
	probe := opts.ListenProbe
	if probe == nil {
		probe = config.ProbeListen
	}
	err = probe(addr)
	if err == nil {
		return listenCheck{addr: addr}, nil
	}
	if !errors.Is(err, syscall.EADDRINUSE) {
		return listenCheck{}, listenError(opts, path, addr, err)
	}
	if !serviceOwnsOccupiedAddress(opts, domain) {
		return listenCheck{}, listenError(opts, path, addr, err)
	}

	check := listenCheck{addr: addr, occupied: true}
	if opts.GOOS != "linux" {
		return check, nil
	}

	if err := runSystemctlCommand(opts, res, "stop", UnitName); err != nil {
		// The stop failed, but a failed installation must still not leave the
		// service enabled for the next boot or login. Stopping it again is best
		// effort; the disable is what the error must report, because that is
		// what keeps the service from starting automatically.
		_, disableErr := stopAndDisable(opts, domain, res)
		msg := fmt.Sprintf("cannot stop the existing service holding %q: %v", addr, err)
		if disableErr != nil {
			msg += fmt.Sprintf("; it could not be disabled either and may start automatically: %v", disableErr)
			return listenCheck{}, errors.New(msg)
		}
		msg += "; it will not start automatically"
		if recovery := recoveryCommand(opts, domain); recovery != "" {
			msg += "; restore it with `" + recovery + "` after fixing the problem"
		}
		return listenCheck{}, errors.New(msg)
	}
	if err := waitListenReleased(probe, addr, time.Now().Add(stopBound(opts))); err != nil {
		// The service this install replaces was stopped and still holds the
		// address: report the occupied address, say the existing service was
		// stopped, and make sure it cannot start at boot.
		return listenCheck{}, abandonInstall(opts, domain, res, true, listenError(opts, path, addr, err))
	}
	check.stopped = true
	return check, nil
}

// serviceOwnsOccupiedAddress reports whether the process holding broker.listen
// is this service's own running Broker. The service manager must report a
// running main pid and the Broker answering on this level's local socket must
// have that same pid. A Broker that could not bind broker.listen fails to
// start, so a service that is merely restarting does not answer on the socket
// and is not mistaken for the occupant.
func serviceOwnsOccupiedAddress(opts Options, domain string) bool {
	if opts.Probe == nil {
		return false
	}
	servicePID, managed := serviceMainPID(opts, domain)
	if !managed {
		return false
	}
	brokerPID, answering := opts.Probe(localSocketPath(opts))
	return answering && brokerPID == servicePID
}

// waitListenReleased polls addr until it can be bound, returning the last probe
// error when the deadline expires. The caller has already stopped or booted out
// the service that owned the address; the wait covers the Broker's shutdown and
// the service manager's asynchronous teardown.
func waitListenReleased(probe func(string) error, addr string, deadline time.Time) error {
	var last error
	for {
		last = probe(addr)
		if last == nil {
			return nil
		}
		if !time.Now().Before(deadline) {
			return last
		}
		time.Sleep(readyPollInterval)
	}
}

// stopBound returns one stop-and-replace budget.
func stopBound(opts Options) time.Duration {
	if opts.StopTimeout > 0 {
		return opts.StopTimeout
	}
	return serviceStopBound
}

// listenError explains why the installation cannot use this install's
// broker.listen and what the operator can change. For an address that is in
// use, it names the process holding the port when the platform can tell.
func listenError(opts Options, path, addr string, err error) error {
	advice := "change broker.listen to another port, or leave it empty to run without a TCP listener, and run service install again"
	if errors.Is(err, syscall.EADDRINUSE) {
		msg := fmt.Sprintf("cannot use broker.listen %q from %s: the address is already in use", addr, path)
		if occupants := findListenOccupant(opts, addr); len(occupants) > 0 {
			msg += describeOccupants(occupants)
		}
		return fmt.Errorf("%s; %s", msg, advice)
	}
	return fmt.Errorf("cannot bind broker.listen %q from %s: %w; %s", addr, path, err, advice)
}

// listenOccupant is the process that holds a listening TCP port, as far as the
// platform can tell. path is empty when the executable could not be read.
type listenOccupant struct {
	pid  int
	path string
}

// describe renders the occupant as one item of the already-in-use sentence.
// The path is quoted, so a path with spaces, quotes or shell metacharacters
// cannot change how the sentence reads. A " (deleted)" suffix that the kernel
// adds to the exe link is kept outside the quotes: it says the running image no
// longer exists at that path, and putting it inside the quotes would read as
// part of the path.
func (o listenOccupant) describe() string {
	switch {
	case o.path != "":
		path, deleted := strings.CutSuffix(o.path, " (deleted)")
		desc := fmt.Sprintf("%q", path)
		if deleted {
			desc += " (deleted)"
		}
		return fmt.Sprintf("%s (pid %d)", desc, o.pid)
	case o.pid > 0:
		return fmt.Sprintf("the process with pid %d", o.pid)
	}
	return ""
}

// describeOccupants renders every occupant as the end of the already-in-use
// sentence. More than one process can hold the same listening socket, and each
// is named rather than picked by directory order.
func describeOccupants(occupants []listenOccupant) string {
	parts := make([]string, 0, len(occupants))
	for _, o := range occupants {
		if desc := o.describe(); desc != "" {
			parts = append(parts, desc)
		}
	}
	switch len(parts) {
	case 0:
		return ""
	case 1:
		return " by " + parts[0]
	default:
		return " by " + joinAnd(parts)
	}
}

// joinAnd joins items the way the user-facing text does: no Oxford comma.
func joinAnd(parts []string) string {
	if len(parts) == 1 {
		return parts[0]
	}
	return strings.Join(parts[:len(parts)-1], ", ") + " and " + parts[len(parts)-1]
}

// findListenOccupant identifies the processes listening on the TCP port in
// addr. It returns nil when the address has no TCP port, the platform cannot
// tell, or the lookup fails; the caller then keeps the plain message. The
// lookup never fails an installation and is never treated as evidence about
// which product the process belongs to.
func findListenOccupant(opts Options, addr string) []listenOccupant {
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return nil
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port <= 0 || port > 65535 {
		return nil
	}
	// A host name cannot be compared with the kernel's numeric addresses, so it
	// leaves want nil: every listener on the port is then a candidate, the
	// conservative answer for a conflict.
	var want net.IP
	if host != "" {
		want = net.ParseIP(host)
	}
	switch opts.GOOS {
	case "linux":
		root := opts.ProcRoot
		if root == "" {
			root = "/proc"
		}
		return occupantFromProc(root, want, port)
	case "darwin":
		return occupantFromLsof(opts, port)
	}
	return nil
}

// occupantFromProc finds the pids and executables that listen on port by
// matching the listening sockets in the kernel's TCP tables to processes' file
// descriptors. want limits the match to the addresses that conflict with the
// installer's own bind. procRoot is a parameter so a test can supply a
// stand-in /proc tree.
func occupantFromProc(procRoot string, want net.IP, port int) []listenOccupant {
	inodes := make(map[string]bool)
	for _, table := range []string{"tcp", "tcp6"} {
		for _, inode := range listeningInodes(filepath.Join(procRoot, "net", table), want, port) {
			inodes[inode] = true
		}
	}
	if len(inodes) == 0 {
		return nil
	}
	entries, err := os.ReadDir(procRoot)
	if err != nil {
		return nil
	}
	byPID := make(map[int]listenOccupant)
	for _, entry := range entries {
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue
		}
		fdDir := filepath.Join(procRoot, entry.Name(), "fd")
		fds, err := os.ReadDir(fdDir)
		if err != nil {
			// Another account's process, or one that exited: its fds cannot be
			// read, but that says nothing about the other candidates.
			continue
		}
		for _, fd := range fds {
			target, err := os.Readlink(filepath.Join(fdDir, fd.Name()))
			if err != nil {
				continue
			}
			inode, ok := strings.CutPrefix(target, "socket:[")
			if !ok || !strings.HasSuffix(inode, "]") {
				continue
			}
			if !inodes[strings.TrimSuffix(inode, "]")] {
				continue
			}
			pid, _ := strconv.Atoi(entry.Name())
			exe, _ := os.Readlink(filepath.Join(procRoot, entry.Name(), "exe"))
			byPID[pid] = listenOccupant{pid: pid, path: exe}
			break
		}
	}
	if len(byPID) == 0 {
		return nil
	}
	occupants := make([]listenOccupant, 0, len(byPID))
	for _, occupant := range byPID {
		occupants = append(occupants, occupant)
	}
	sort.Slice(occupants, func(i, j int) bool { return occupants[i].pid < occupants[j].pid })
	return occupants
}

// listeningInodes returns the inodes of the listening sockets in one
// /proc/net/tcp table whose local address can explain an address-in-use error
// for want and port. The table's local address field is HEXADDRESS:HEXPORT and
// state 0A is TCP_LISTEN. want nil means any address.
func listeningInodes(tablePath string, want net.IP, port int) []string {
	data, err := os.ReadFile(tablePath)
	if err != nil {
		return nil
	}
	var inodes []string
	lines := strings.Split(string(data), "\n")
	for _, line := range lines[1:] {
		fields := strings.Fields(line)
		if len(fields) < 10 || fields[3] != "0A" {
			continue
		}
		colon := strings.LastIndexByte(fields[1], ':')
		if colon < 0 {
			continue
		}
		listenPort, err := strconv.ParseInt(fields[1][colon+1:], 16, 32)
		if err != nil || int(listenPort) != port {
			continue
		}
		local, ok := procAddress(fields[1][:colon])
		if !ok || !listenAddressConflicts(local, want) {
			continue
		}
		inodes = append(inodes, fields[9])
	}
	return inodes
}

// procAddress parses one /proc/net/tcp or tcp6 local address field. The kernel
// writes each 32-bit word in host byte order, so every four bytes are reversed
// before they form the IP.
func procAddress(field string) (net.IP, bool) {
	raw, err := hex.DecodeString(field)
	if err != nil {
		return nil, false
	}
	switch len(raw) {
	case net.IPv4len:
		return net.IPv4(raw[3], raw[2], raw[1], raw[0]), true
	case net.IPv6len:
		ip := make(net.IP, net.IPv6len)
		for word := 0; word < net.IPv6len; word += 4 {
			ip[word] = raw[word+3]
			ip[word+1] = raw[word+2]
			ip[word+2] = raw[word+1]
			ip[word+3] = raw[word]
		}
		return ip, true
	}
	return nil, false
}

// listenAddressConflicts reports whether a socket bound to local explains an
// address-in-use error for a bind to want. A wildcard bind conflicts with every
// socket on its port in any family, because Go binds a wildcard with a
// dual-stack socket, so the rules are deliberately conservative: naming a
// listener that is not the actual occupant is better than missing the one that
// is. want nil matches every address.
func listenAddressConflicts(local, want net.IP) bool {
	if want == nil || want.IsUnspecified() {
		return true
	}
	if want4 := want.To4(); want4 != nil {
		if local4 := local.To4(); local4 != nil {
			return local4.IsUnspecified() || local4.Equal(want4)
		}
		return local.IsUnspecified()
	}
	if local.To4() != nil {
		return false
	}
	return local.IsUnspecified() || local.Equal(want)
}

// occupantFromLsof uses lsof to find the pids listening on port on macOS, then
// asks lsof for each process's executable. Both commands are best effort: any
// failure means the caller falls back to what it could establish.
func occupantFromLsof(opts Options, port int) []listenOccupant {
	run := opts.RunOutput
	if run == nil {
		run = opts.Run
	}
	if run == nil {
		return nil
	}
	if opts.LookPath != nil {
		if _, err := opts.LookPath(lsofPath); err != nil {
			return nil
		}
	}
	out, err := run(lsofPath, "-nP", "-iTCP:"+strconv.Itoa(port), "-sTCP:LISTEN")
	if err != nil {
		return nil
	}
	pids := lsofPID(string(out))
	if len(pids) == 0 {
		return nil
	}
	occupants := make([]listenOccupant, 0, len(pids))
	for _, pid := range pids {
		path := ""
		if txt, err := run(lsofPath, "-a", "-p", strconv.Itoa(pid), "-d", "txt", "-Fn"); err == nil {
			path = lsofTextPath(string(txt))
		}
		occupants = append(occupants, listenOccupant{pid: pid, path: path})
	}
	sort.Slice(occupants, func(i, j int) bool { return occupants[i].pid < occupants[j].pid })
	return occupants
}

// lsofTextPath reads the executable path from `lsof -a -p <pid> -d txt -Fn`
// field output: each open file starts with an f line and names its path on the
// following n line.
func lsofTextPath(out string) string {
	for _, line := range strings.Split(out, "\n") {
		if path, ok := strings.CutPrefix(line, "n"); ok && path != "" {
			return path
		}
	}
	return ""
}

// runOccupantCommand runs one occupant lookup command with a timeout, keeping
// only its standard output.
func runOccupantCommand(name string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), occupantCommandTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	err := cmd.Run()
	return stdout.Bytes(), err
}

// humanDuration renders d as the timeout lines read it, such as "20 seconds",
// rather than as the time.Duration String method writes it. Any part of a
// second rounds up, so a short timeout still reads as a whole second.
func humanDuration(d time.Duration) string {
	if d < time.Second {
		return "1 second"
	}
	seconds := int64(d / time.Second)
	if d%time.Second != 0 {
		seconds++
	}
	if seconds == 1 {
		return "1 second"
	}
	return fmt.Sprintf("%d seconds", seconds)
}

// lsofPID reads every process id from lsof's table output, in the order they
// appear, without duplicates.
func lsofPID(out string) []int {
	lines := strings.Split(out, "\n")
	if len(lines) < 2 {
		return nil
	}
	header := strings.Fields(lines[0])
	pidColumn := -1
	for i, field := range header {
		if field == "PID" {
			pidColumn = i
			break
		}
	}
	if pidColumn < 0 {
		return nil
	}
	var pids []int
	seen := make(map[int]bool)
	for _, line := range lines[1:] {
		fields := strings.Fields(line)
		if len(fields) <= pidColumn {
			continue
		}
		pid, err := strconv.Atoi(fields[pidColumn])
		if err != nil || pid <= 0 || seen[pid] {
			continue
		}
		seen[pid] = true
		pids = append(pids, pid)
	}
	return pids
}

// abandonInstall handles a failed installation after the service definition
// was written or an existing service was stopped. The unit or plist and the
// configuration file are kept, so fixing the cause and running service install
// again finds them. The service is disabled, so the failed installation never
// leaves a Broker that starts at the next boot or login: a systemd unit must
// not stay enabled, and a kept launchd plist must not stay loadable. stopped
// reports that this install stopped the service that was already running, so
// the error tells the operator what changed and how to restore it.
func abandonInstall(opts Options, domain string, res *Result, stopped bool, cause error) error {
	stopErr, disableErr := stopAndDisable(opts, domain, res)
	msg := cause.Error()
	if stopped {
		msg += "; the existing service was stopped"
	}
	switch {
	case disableErr != nil:
		msg += fmt.Sprintf("; the service could not be disabled and may start automatically at the next boot or login: %v", disableErr)
	case stopped:
		msg += " and will not start automatically"
		if recovery := recoveryCommand(opts, domain); recovery != "" {
			msg += "; restore it with `" + recovery + "` after fixing the problem"
		}
	case stopErr != nil:
		msg += fmt.Sprintf("; the service could not be stopped: %v; it is disabled and will not start automatically", stopErr)
	default:
		msg += "; the service was stopped and disabled"
	}
	msg += "; fix the problem and run service install again"
	return errors.New(msg)
}

// stopAndDisable stops the service and makes sure it cannot start at the next
// boot or login. It returns the stop error and the disable error separately:
// macOS can fail to unload the job while still recording the label's disabled
// state, which is what keeps launchd from loading it at login. Linux performs
// both in one `systemctl disable --now`, which removes the enable symlink and
// stops the unit, so the two errors are the same. The plist is kept so the user
// has the service definition, and install re-enables the label before it
// bootstraps again.
func stopAndDisable(opts Options, domain string, res *Result) (stopErr, disableErr error) {
	switch opts.GOOS {
	case "linux":
		err := runSystemctlCommand(opts, res, "disable", "--now", UnitName)
		return err, err
	case "darwin":
		return bootoutService(opts, domain, res), runLaunchctlCommand(opts, domain, res, "disable")
	}
	return nil, nil
}

// recoveryCommand names the command that brings back a service this install
// stopped before it failed.
func recoveryCommand(opts Options, domain string) string {
	switch opts.GOOS {
	case "linux":
		if opts.System {
			return "sudo systemctl enable --now " + UnitName
		}
		return "systemctl --user enable --now " + UnitName
	case "darwin":
		path, err := unitPath(opts)
		if err != nil {
			return ""
		}
		target := domain + "/" + Label
		prefix := ""
		if opts.System {
			prefix = "sudo "
		}
		return prefix + "launchctl enable " + target + " && " + prefix + "launchctl bootstrap " + domain + " " + path
	}
	return ""
}

// runLaunchctlCommand runs one launchctl command on domain/label, records it
// and reports its output on failure.
func runLaunchctlCommand(opts Options, domain string, res *Result, verb string) error {
	target := domain + "/" + Label
	cmdline := "launchctl " + verb + " " + target
	res.Commands = append(res.Commands, cmdline)
	out, err := opts.Run("launchctl", verb, target)
	if err != nil {
		if msg := strings.TrimSpace(string(out)); msg != "" {
			return fmt.Errorf("cannot run %s: %w: %s", cmdline, err, msg)
		}
		return fmt.Errorf("cannot run %s: %w", cmdline, err)
	}
	return nil
}

// serviceMainPID returns the pid the service manager reports for the unit and
// whether it reports a running one.
func serviceMainPID(opts Options, domain string) (int, bool) {
	switch opts.GOOS {
	case "linux":
		args := []string{"show", "--property", "MainPID", "--value", UnitName}
		if !opts.System {
			args = append([]string{"--user"}, args...)
		}
		out, err := opts.Run("systemctl", args...)
		if err != nil {
			return 0, false
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(out)))
		if err != nil || pid == 0 {
			return 0, false
		}
		return pid, true
	case "darwin":
		out, err := opts.Run("launchctl", "print", domain+"/"+Label)
		if err != nil {
			return 0, false
		}
		match := launchdPIDPattern.FindStringSubmatch(string(out))
		if match == nil {
			return 0, false
		}
		pid, err := strconv.Atoi(match[1])
		if err != nil || pid == 0 {
			return 0, false
		}
		return pid, true
	}
	return 0, false
}

// launchdDomain picks the launchd domain for a user service. launchd
// exposes gui/<uid> only to an account with a graphical login session. Probe
// 44498306091 showed that user/<uid> accepts print but rejects bootstrap with
// exit 5, so installing there would leave a loaded-looking, stopped service;
// the installation refuses before writing anything instead.
func launchdDomain(opts Options) (string, error) {
	if opts.System {
		return "system", nil
	}
	gui := fmt.Sprintf("gui/%d", opts.UID)
	if launchctlDomainExists(opts, gui) {
		return gui, nil
	}
	return "", fmt.Errorf("cannot start the service: launchd has no %s domain for uid %d; install the user service from a graphical login session, or use --system on a host without one", gui, opts.UID)
}

// launchctlDomainExists reports whether launchd knows the domain.
func launchctlDomainExists(opts Options, domain string) bool {
	_, err := opts.Run("launchctl", "print", domain)
	return err == nil
}

// runLaunchctl replaces the loaded service, if any, with the freshly written
// plist. It boots the old job out once before the bootstrap, waits for launchd
// to unload it, waits for broker.listen to be released, enables the label and
// bootstraps the plist, retrying a bootstrap that fails with exit 5. Every
// failure removes a plist this run wrote or, when the plist existed before this
// run, keeps it and disables the label, so a failed installation leaves nothing
// that launchd starts at the next login. plistExisted is what the caller saw
// before writing the plist, so a pre-existing file is never removed. It reports
// whether an old service was loaded, so a later readiness failure can say what
// was replaced.
//
// The unload wait does not depend on bootout's exit status: an unloaded service
// makes the first launchctl print fail, so waiting costs almost nothing. The
// one deadline shared by the unload wait and the address wait keeps those two
// phases within one stop budget instead of adding a separate wait for each. The
// bootstrap retries start after both phases and run on their own
// bootstrapRetryBound.
func runLaunchctl(opts Options, domain, plistPath string, plistExisted bool, check listenCheck, res *Result) (bool, error) {
	replaced := launchctlDomainLoaded(opts, domain)
	if err := bootoutService(opts, domain, res); err != nil {
		return replaced, abandonLaunchd(opts, domain, plistPath, res, plistExisted, replaced, false, err)
	}

	budget := stopBound(opts)
	deadline := time.Now().Add(budget)
	if err := waitServiceUnloaded(opts, domain, deadline, budget, unloadAdvice(opts, domain, "the install")); err != nil {
		return replaced, abandonLaunchd(opts, domain, plistPath, res, plistExisted, replaced, false, err)
	}
	if check.occupied {
		probe := opts.ListenProbe
		if probe == nil {
			probe = config.ProbeListen
		}
		if err := waitListenReleased(probe, check.addr, deadline); err != nil {
			return replaced, abandonLaunchd(opts, domain, plistPath, res, plistExisted, replaced, true,
				listenError(opts, configFilePath(opts), check.addr, err))
		}
	}

	// A failed earlier installation disables the label so it cannot start at
	// login. Enabling it here is what lets that plist load again; without it a
	// rerun after a failure would leave the service disabled for good.
	if err := runLaunchctlCommand(opts, domain, res, "enable"); err != nil {
		return replaced, abandonLaunchd(opts, domain, plistPath, res, plistExisted, replaced, true, err)
	}

	cmdline := "launchctl bootstrap " + domain + " " + plistPath
	res.Commands = append(res.Commands, cmdline)
	out, err := bootstrapService(opts, domain, plistPath)
	if err == nil {
		return replaced, nil
	}
	failure := fmt.Errorf("cannot run %s: %w", cmdline, err)
	if msg := strings.TrimSpace(string(out)); msg != "" {
		failure = fmt.Errorf("cannot run %s: %w: %s", cmdline, err, msg)
	}
	return replaced, abandonLaunchd(opts, domain, plistPath, res, plistExisted, replaced, true, failure)
}

// abandonLaunchd cleans up a failed replacement. Only a plist this run wrote is
// removed, which restores the state the operator had. A plist that existed
// before the run is kept, whether or not its service was loaded, and its label
// is disabled so launchd does not load it at the next login; the user can
// recover with enable followed by bootstrap. A disable that fails is reported
// as such instead of claiming the label is disabled. replaced reports that an
// old service was loaded before this run and unloaded reports that it is gone,
// so the error can say what happened to it.
func abandonLaunchd(opts Options, domain, plistPath string, res *Result, plistExisted, replaced, unloaded bool, cause error) error {
	msg := cause.Error()
	var cleanupErr error
	removed := false
	if plistExisted {
		cleanupErr = runLaunchctlCommand(opts, domain, res, "disable")
	} else {
		rmErr := os.Remove(rooted(opts, plistPath))
		if rmErr == nil || errors.Is(rmErr, fs.ErrNotExist) {
			removed = true
		} else {
			cleanupErr = rmErr
		}
	}
	if cleanupErr != nil {
		msg += fmt.Sprintf("; the failed installation could not be cleaned up: %v", cleanupErr)
	}
	switch {
	case replaced && unloaded:
		msg += "; the old service was unloaded and no Broker is running now"
		if plistExisted {
			if recovery := recoveryCommand(opts, domain); recovery != "" {
				msg += "; run `" + installHint(opts) + "` again, or load the kept plist with `" + recovery + "`"
			}
		}
	case unloaded:
		msg += "; no Broker is running now"
	case replaced:
		msg += "; the old service could not be unloaded"
	}
	switch {
	case plistExisted && cleanupErr != nil:
		msg += "; the plist was kept but its label could not be disabled, so the service may start automatically at the next login"
	case plistExisted:
		msg += "; the plist was kept and its label was disabled so it does not start at the next login"
	case removed:
		msg += "; the plist was removed"
	default:
		msg += "; the plist could not be removed and may start automatically at the next login"
	}
	msg += "; fix the problem and run service install again"
	return errors.New(msg)
}

// bootstrapService loads the plist and retries while launchctl reports
// bootstrapTransientExit, the status of a bootstrap that raced the old job's
// teardown. The retry window is bootstrapRetryBound, independent of the stop
// budget: it begins after the old job is gone, so an unload wait that used most
// of the stop budget must not leave the retry without time. The attempt cap
// only backstops a clock that advances slower than the sleep.
func bootstrapService(opts Options, domain, plistPath string) ([]byte, error) {
	bound := opts.BootstrapTimeout
	if bound <= 0 {
		bound = bootstrapRetryBound
	}
	deadline := time.Now().Add(bound)
	var (
		out []byte
		err error
	)
	for attempt := 1; ; attempt++ {
		out, err = opts.Run("launchctl", "bootstrap", domain, plistPath)
		if err == nil {
			return out, nil
		}
		if exitStatus(err) != bootstrapTransientExit || attempt >= bootstrapRetryAttempts || !time.Now().Before(deadline) {
			return out, err
		}
		time.Sleep(bootstrapRetryInterval)
	}
}

// waitServiceUnloaded waits until launchd no longer knows the service in
// domain. Only exit status 113 or the "Could not find service" message report
// an unloaded service; any other print failure is transient or unrelated and is
// retried until the deadline, because treating it as unloaded would bootstrap
// over a job launchd still holds. tail ends the timeout message: the caller
// names the command to retry, which differs between installation, stop and
// restart.
func waitServiceUnloaded(opts Options, domain string, deadline time.Time, budget time.Duration, tail string) error {
	target := domain + "/" + Label
	var last string
	for {
		out, err := opts.Run("launchctl", "print", target)
		if launchdUnloaded(err, out) {
			return nil
		}
		last = launchctlDetail(err, out)
		if !time.Now().Before(deadline) {
			break
		}
		time.Sleep(launchdUnloadPollInterval)
	}
	detail := ""
	if last != "" {
		detail = fmt.Sprintf(" (launchctl still reports %q)", last)
	}
	return fmt.Errorf("cannot unload the old service in %s within %s%s%s",
		domain, humanDuration(budget), detail, tail)
}

// unloadAdvice is the tail of waitServiceUnloaded's timeout message. It names
// the bootout that unloads the job by hand and the command to run afterwards.
func unloadAdvice(opts Options, domain, retry string) string {
	target := domain + "/" + Label
	return fmt.Sprintf("; unload it with `%s` and run %s again", privileged(opts, "launchctl bootout "+target), retry)
}

// launchdUnloaded reports whether launchctl print says the job is gone. launchd
// reports an unknown job with exit status 113 and "Could not find service";
// other failures do not prove the job is gone.
func launchdUnloaded(err error, out []byte) bool {
	if err == nil {
		return false
	}
	return exitStatus(err) == 113 || strings.Contains(string(out), "Could not find service")
}

// launchctlDetail describes what a print probe reported, for a timeout message.
func launchctlDetail(err error, out []byte) string {
	if msg := strings.TrimSpace(string(out)); msg != "" {
		return firstLine(msg)
	}
	if err != nil {
		return err.Error()
	}
	return ""
}

// firstLine returns the first non-empty line of s, trimmed.
func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return strings.TrimSpace(s[:i])
	}
	return s
}

// installHint names the command that installs this level's service.
func installHint(opts Options) string {
	if opts.System {
		return "sudo peers service install --system"
	}
	return "peers service install"
}

// statusHint names the command that shows the status of this level's service.
func statusHint(opts Options) string {
	if opts.System {
		return "check it with `peers service status --system`"
	}
	return "check it with `peers service status`"
}

// privileged prefixes cmd with sudo when the command belongs to a system
// service.
func privileged(opts Options, cmd string) string {
	if opts.System {
		return "sudo " + cmd
	}
	return cmd
}

// bootoutService unloads the service before a replacement or removal. launchctl
// exits 3 with "No such process" when the service is not loaded (probe
// 44498306091); that is the only bootout failure ignored. Any other failure is
// returned, because the old service may still own the account lock or the
// socket.
func bootoutService(opts Options, domain string, res *Result) error {
	target := domain + "/" + Label
	cmdline := "launchctl bootout " + target
	res.Commands = append(res.Commands, cmdline)
	out, err := opts.Run("launchctl", "bootout", target)
	if err == nil {
		return nil
	}
	if exitStatus(err) == 3 {
		return nil
	}
	if msg := strings.TrimSpace(string(out)); msg != "" {
		return fmt.Errorf("cannot run %s: %w: %s", cmdline, err, msg)
	}
	return fmt.Errorf("cannot run %s: %w", cmdline, err)
}

// exitStatus returns the exit code of a command error, or -1 when the error
// did not come from an exited process.
func exitStatus(err error) int {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}

// waitReady waits until the Broker answering on this level's local socket is
// the service's own process. It applies the same test as the pre-install
// check: the service manager's pid must equal the socket peer pid. An answer
// from any other process is not readiness, because the service cannot take over
// that process's account lock. A timeout reports the log location, since the
// service manager owns the process and its standard error. On success it
// returns the readiness deadline, which bounds the degraded-status check that
// follows within the same wait.
func waitReady(opts Options, domain string, since time.Time) (time.Time, error) {
	bound := opts.ReadyTimeout
	if bound <= 0 {
		bound = readyBound
	}
	path := localSocketPath(opts)
	deadline := time.Now().Add(bound)
	var (
		peerPID    int
		answering  bool
		servicePID int
		managed    bool
	)
	for {
		peerPID, answering = opts.Probe(path)
		if answering {
			servicePID, managed = serviceMainPID(opts, domain)
			if managed && servicePID == peerPID {
				return deadline, nil
			}
		}
		if time.Now().After(deadline) {
			if answering {
				return deadline, fmt.Errorf("cannot use the Broker answering on %s: it is not the service's process (service pid %d, socket peer pid %d); stop it and check the service log: %s", path, servicePID, peerPID, logHint(opts))
			}
			// The precheck can pass and the Broker still fail to start; the
			// service log records the cause. Include it so the timeout names
			// the real problem instead of only the silent socket.
			reason := startupFailureReason(opts, since)
			if reason != "" {
				return deadline, fmt.Errorf("cannot reach the Broker on %s within %s; check the service log: %s; its last startup_failed record says: %s", path, humanDuration(bound), logHint(opts), reason)
			}
			// No failure record was written after this install began. The
			// service manager still knows whether the unit failed and with what
			// exit code, which is the next best description of the cause.
			state, stateErr := serviceManagerState(opts, domain)
			if stateErr != nil {
				return deadline, fmt.Errorf("cannot reach the Broker on %s within %s; check the service log: %s; the service manager state could not be read: %w", path, humanDuration(bound), logHint(opts), stateErr)
			}
			return deadline, fmt.Errorf("cannot reach the Broker on %s within %s; check the service log: %s; the service manager reports %s", path, humanDuration(bound), logHint(opts), state)
		}
		time.Sleep(readyPollInterval)
	}
}

// waitStatus asks the ready Broker for its status. A probe error means Status
// could not be read yet, so it is retried until the readiness deadline; the
// Broker answered on the socket a moment earlier and may still be serving the
// call. The returned error is the last probe error.
func waitStatus(opts Options, deadline time.Time) (DegradedState, bool, error) {
	probe := opts.StatusProbe
	if probe == nil {
		probe = defaultStatusProbe
	}
	path := localSocketPath(opts)
	for {
		state, degraded, err := probe(path)
		if err == nil {
			return state, degraded, nil
		}
		if !time.Now().Before(deadline) {
			return DegradedState{}, false, err
		}
		time.Sleep(readyPollInterval)
	}
}

// serviceManagerState returns the state and last exit status the service
// manager reports for the unit or job, for a readiness timeout whose log has no
// startup_failed record. It names the query in its own error and includes its
// output, so a failing service manager does not turn into another silent
// failure.
func serviceManagerState(opts Options, domain string) (string, error) {
	switch opts.GOOS {
	case "linux":
		args := []string{"show", "-p", "ActiveState", "-p", "SubState", "-p", "ExecMainStatus", UnitName}
		if !opts.System {
			args = append([]string{"--user"}, args...)
		}
		cmdline := strings.Join(append([]string{"systemctl"}, args...), " ")
		out, err := opts.Run("systemctl", args...)
		if err != nil {
			if msg := strings.TrimSpace(string(out)); msg != "" {
				return "", fmt.Errorf("%s: %w: %s", cmdline, err, msg)
			}
			return "", fmt.Errorf("%s: %w", cmdline, err)
		}
		state := strings.Join(strings.Fields(string(out)), " ")
		if state == "" {
			return "", fmt.Errorf("%s printed no state", cmdline)
		}
		return state, nil
	case "darwin":
		target := domain + "/" + Label
		cmdline := "launchctl print " + target
		out, err := opts.Run("launchctl", "print", target)
		if err != nil {
			if msg := strings.TrimSpace(string(out)); msg != "" {
				return "", fmt.Errorf("%s: %w: %s", cmdline, err, msg)
			}
			return "", fmt.Errorf("%s: %w", cmdline, err)
		}
		var parts []string
		if m := launchdStatePattern.FindStringSubmatch(string(out)); m != nil {
			parts = append(parts, "state = "+strings.TrimSpace(m[1]))
		}
		if m := launchdLastExitCodePattern.FindStringSubmatch(string(out)); m != nil {
			parts = append(parts, "last exit code = "+strings.TrimSpace(m[1]))
		}
		if len(parts) == 0 {
			return "", fmt.Errorf("%s printed no state or last exit code", cmdline)
		}
		return strings.Join(parts, ", "), nil
	}
	return "", fmt.Errorf("cannot read the service manager state on platform %q", opts.GOOS)
}

// logHint names where the service manager writes the Broker's standard error.
func logHint(opts Options) string {
	if path := logFile(opts); path != "" {
		return path
	}
	if opts.System {
		return "journalctl -u " + UnitName
	}
	return "journalctl --user -u " + UnitName
}

// logFile returns the service log file macOS launchd writes, or an empty
// string on a platform whose service manager reads the log from journald.
func logFile(opts Options) string {
	if opts.GOOS != "darwin" {
		return ""
	}
	if opts.System {
		return "/var/log/peers/broker.log"
	}
	return filepath.Join(opts.Home, "Library", "Logs", "peers", "broker.log")
}

// startupFailureReason returns the error of the last startup_failed record
// written after since, or an empty string when there is none or the log cannot
// be read. It never fails an installation: the record is only additional
// context for a readiness timeout. Records from an earlier run describe a cause
// that may no longer apply, so they are ignored.
func startupFailureReason(opts Options, since time.Time) string {
	if path := logFile(opts); path != "" {
		data, err := readLogTail(rooted(opts, path))
		if err != nil {
			return ""
		}
		return lastStartupFailure(string(data), since)
	}
	args := []string{"-u", UnitName, "-n", "100", "--no-pager", "-o", "cat", "--since", "@" + strconv.FormatInt(since.Unix(), 10)}
	if !opts.System {
		args = append([]string{"--user"}, args...)
	}
	out, err := opts.Run("journalctl", args...)
	if err != nil {
		return ""
	}
	return lastStartupFailure(string(out), since)
}

// logTailBytes bounds how much of the service log startupFailureReason reads
// from the end. The record that matters is the last one, and the log may hold
// weeks of runtime records.
const logTailBytes = 64 << 10

// readLogTail returns at most the last logTailBytes of the file. A cut line at
// the start is harmless because lastStartupFailure ignores records it cannot
// parse.
func readLogTail(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if start := info.Size() - logTailBytes; start > 0 {
		if _, err := f.Seek(start, io.SeekStart); err != nil {
			return nil, err
		}
	}
	return io.ReadAll(f)
}

// lastStartupFailure scans the log from the end and returns the error field of
// the last startup_failed JSON record written at or after since. The Broker
// writes the time field as RFC 3339 with nanoseconds; a record without a
// parseable time cannot be shown to come from this installation and is ignored.
func lastStartupFailure(log string, since time.Time) string {
	lines := strings.Split(log, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line == "" || !strings.Contains(line, `"startup_failed"`) {
			continue
		}
		var record struct {
			Time  string `json:"time"`
			Event string `json:"event"`
			Error string `json:"error"`
		}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			continue
		}
		if record.Event != "startup_failed" || record.Error == "" {
			continue
		}
		at, err := time.Parse(time.RFC3339Nano, record.Time)
		if err != nil || at.Before(since) {
			continue
		}
		return record.Error
	}
	return ""
}

// configHome returns the base directory for user configuration.
func configHome(opts Options) string {
	if opts.XDGConfigHome != "" && filepath.IsAbs(opts.XDGConfigHome) {
		return opts.XDGConfigHome
	}
	return filepath.Join(opts.Home, ".config")
}

// rooted maps a production path under the test root prefix.
func rooted(opts Options, path string) string {
	if opts.Root == "" {
		return path
	}
	return filepath.Join(opts.Root, path)
}

// writeUnit writes the unit file or plist and returns its production path.
func writeUnit(opts Options) (string, error) {
	path, err := unitPath(opts)
	if err != nil {
		return "", err
	}
	var (
		content string
		dirs    []string
	)
	switch {
	case opts.GOOS == "linux" && !opts.System:
		content, err = SystemdUserUnit(opts.Exe)
		dirs = []string{filepath.Dir(path)}
	case opts.GOOS == "linux" && opts.System:
		content, err = SystemdSystemUnit(opts.Exe)
		dirs = []string{systemdUnitDir}
	case opts.GOOS == "darwin" && !opts.System:
		logDir := filepath.Join(opts.Home, "Library", "Logs", "peers")
		logPath := filepath.Join(logDir, "broker.log")
		content, err = LaunchAgentPlist(opts.Exe, logPath)
		dirs = []string{logDir, filepath.Dir(path)}
	case opts.GOOS == "darwin" && opts.System:
		content, err = LaunchDaemonPlist(opts.Exe)
		dirs = []string{systemLogDir, launchDaemonDir}
	default:
		return "", fmt.Errorf("cannot write the service definition on platform %q", opts.GOOS)
	}
	if err != nil {
		return "", err
	}
	// launchd has no drop-in mechanism, so a variable an operator added to the
	// plist is the only place a proxy setting can live. Keep that dictionary
	// across a rewrite; an entry this binary's template generates wins over a
	// kept entry of the same name.
	if opts.GOOS == "darwin" {
		env, err := plistEnvironment(rooted(opts, path))
		if err != nil {
			return "", fmt.Errorf("cannot read the environment variables of %s: %w", path, err)
		}
		content = withPlistEnvironment(content, env)
	}
	for _, dir := range dirs {
		mode := fs.FileMode(0o755)
		switch {
		case opts.GOOS == "darwin" && !opts.System && dir == filepath.Join(opts.Home, "Library", "Logs", "peers"):
			mode = 0o700
		case opts.GOOS == "darwin" && opts.System && dir == systemLogDir:
			// launchd opens the daemon's log files itself, so their mode is not
			// governed by the job's Umask. Keep accounts outside root's group out
			// of the directory instead; 0750 makes the files unreachable.
			mode = 0o750
		}
		if err := os.MkdirAll(rooted(opts, dir), mode); err != nil {
			return "", fmt.Errorf("cannot create directory %s: %w", dir, err)
		}
	}
	if err := writeFileAtomic(rooted(opts, path), content, 0o644); err != nil {
		return "", err
	}
	return path, nil
}

// configFilePath returns the production path of the configuration file for the
// install's level.
func configFilePath(opts Options) string {
	if opts.System {
		return systemConfigPath
	}
	return filepath.Join(opts.Home, ".peers", "config.yaml")
}

// auditLogPath returns the production path of the audit log for the install's
// level. It follows the same path table as configFilePath.
func auditLogPath(opts Options) string {
	if opts.System {
		return filepath.Join(systemLogDir, "audit.log")
	}
	return filepath.Join(opts.Home, ".peers", "audit.log")
}

// createConfig creates the configuration file for the install's level if it is
// absent and reports what happened in res. The file is created exclusively, so
// an existing file is never modified: its content, group and permissions stay
// as they are. The system file belongs to root and to the administration
// group, mode 0660; the user file belongs to the installing account, mode
// 0600. The content is the template the config package generates for the level.
// adminGID is the administration group for the system installation and ignored
// otherwise.
func createConfig(opts Options, res *Result, adminGID int) error {
	fileMode := fs.FileMode(0o600)
	if opts.System {
		fileMode = 0o660
	}
	return createConfigWith(opts, res, adminGID, fileMode, func(path string) (*os.File, error) {
		return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, fileMode)
	})
}

// createConfigWith implements createConfig; fileMode is the mode the new file
// must end up with, and open lets a test force a write failure after the file
// was created.
func createConfigWith(opts Options, res *Result, adminGID int, fileMode fs.FileMode, open func(string) (*os.File, error)) error {
	path := configFilePath(opts)
	res.ConfigPath = path

	dirMode := fs.FileMode(0o755)
	if !opts.System {
		// The user file lives in the account's data directory, which the
		// config package keeps private to the account.
		dirMode = 0o700
	}
	if err := os.MkdirAll(rooted(opts, filepath.Dir(path)), dirMode); err != nil {
		return fmt.Errorf("cannot create the configuration directory: %w", err)
	}

	fsPath := rooted(opts, path)
	f, err := open(fsPath)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			res.ConfigNew = false
			return nil
		}
		return fmt.Errorf("cannot create the configuration file: %w", err)
	}
	// A file this run created is removed when a later step fails, so the next
	// install does not mistake a partial file for an existing configuration.
	removePartial := func() {
		_ = os.Remove(fsPath)
	}
	mode := config.ModeUser
	if opts.System {
		mode = config.ModeSystem
	}
	if _, err := f.Write(config.Template(mode)); err != nil {
		_ = f.Close()
		removePartial()
		return fmt.Errorf("cannot write the configuration file %s: %w", path, err)
	}
	if opts.System {
		if err := setConfigOwner(opts, fsPath, adminGID); err != nil {
			_ = f.Close()
			removePartial()
			return fmt.Errorf("cannot set the owner of the configuration file: %w", err)
		}
	}
	if err := setConfigMode(opts, fsPath, fileMode); err != nil {
		_ = f.Close()
		removePartial()
		return fmt.Errorf("cannot set the mode of the configuration file: %w", err)
	}
	if err := f.Close(); err != nil {
		removePartial()
		return fmt.Errorf("cannot close the configuration file: %w", err)
	}
	res.ConfigNew = true
	return nil
}

// setConfigOwner makes the system configuration file root-owned and gives it
// the administration group. Options.Chown replaces the operation in a test; a
// test that roots the filesystem and supplies no Chown cannot change ownership
// without root, so the step is skipped there.
func setConfigOwner(opts Options, path string, gid int) error {
	if opts.Chown != nil {
		return opts.Chown(path, 0, gid)
	}
	if opts.Root != "" {
		return nil
	}
	return os.Chown(path, 0, gid)
}

// setConfigMode sets the configuration file's permission bits, through
// Options.Chmod when a test supplies it.
func setConfigMode(opts Options, path string, mode fs.FileMode) error {
	if opts.Chmod != nil {
		return opts.Chmod(path, mode)
	}
	return os.Chmod(path, mode)
}

// runSystemctl reloads the unit database, enables the unit and starts it.
// restart also starts a unit that is not running yet, and replaces the process
// when the unit was already running from an older binary.
func runSystemctl(opts Options, res *Result) error {
	for _, args := range [][]string{{"daemon-reload"}, {"enable", UnitName}, {"restart", UnitName}} {
		if err := runSystemctlCommand(opts, res, args...); err != nil {
			return err
		}
	}
	return nil
}

// runSystemctlCommand runs one systemctl command, records it and reports its
// output on failure.
func runSystemctlCommand(opts Options, res *Result, args ...string) error {
	argv := append([]string{}, args...)
	if !opts.System {
		argv = append([]string{"--user"}, argv...)
	}
	cmdline := strings.Join(append([]string{"systemctl"}, argv...), " ")
	res.Commands = append(res.Commands, cmdline)
	out, err := opts.Run("systemctl", argv...)
	if err != nil {
		if msg := strings.TrimSpace(string(out)); msg != "" {
			return fmt.Errorf("cannot run %s: %w: %s", cmdline, err, msg)
		}
		return fmt.Errorf("cannot run %s: %w", cmdline, err)
	}
	return nil
}

// checkTempBinary refuses binaries under the temporary directory.
func checkTempBinary(opts Options) error {
	if opts.TempDir == "" {
		return nil
	}
	rel, err := filepath.Rel(opts.TempDir, opts.Exe)
	if err != nil {
		return nil // different volumes: not under the temporary directory
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil
	}
	return fmt.Errorf("cannot install a service for %q: it is inside the temporary directory %q", opts.Exe, opts.TempDir)
}

// checkSystemBinary verifies that the executable and every directory above it
// are owned by root and unwritable by group or others. Install resolves the
// executable first, so the check and the written unit name the same file. A
// system service must execute a file no other account can replace.
func checkSystemBinary(opts Options) error {
	for path := opts.Exe; ; {
		uid, mode, err := opts.Owner(path)
		if err != nil {
			return fmt.Errorf("cannot inspect %q: %w", path, err)
		}
		switch {
		case uid != 0:
			return fmt.Errorf("cannot install the service: %q is owned by uid %d, not root", path, uid)
		case mode&0o022 != 0:
			return fmt.Errorf("cannot install the service: %q is writable by group or others (%#o)", path, mode.Perm())
		case mode&0o001 == 0:
			return fmt.Errorf("cannot install the service: %q is not executable by other accounts", path)
		}
		parent := filepath.Dir(path)
		if parent == path {
			break
		}
		path = parent
	}
	return nil
}

// writeFileAtomic writes content to a temporary file in the target directory
// and renames it over path, replacing any existing file.
func writeFileAtomic(path, content string, mode fs.FileMode) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".peers-install-*")
	if err != nil {
		return fmt.Errorf("cannot create a temporary file in %s: %w", dir, err)
	}
	tmp := f.Name()
	defer func() { _ = os.Remove(tmp) }()
	if err := f.Chmod(mode); err != nil {
		_ = f.Close()
		return fmt.Errorf("cannot set the mode of %s: %w", tmp, err)
	}
	if _, err := f.WriteString(content); err != nil {
		_ = f.Close()
		return fmt.Errorf("cannot write %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("cannot close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("cannot install %s: %w", path, err)
	}
	return nil
}
