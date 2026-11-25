//go:build e2e

package syscontainer

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/node"
)

const (
	// peersBinary is the peers build in the base image.
	peersBinary = "/usr/local/bin/peers"
	// systemConfigPath is the system Broker's configuration file.
	systemConfigPath = "/etc/peers/config.yaml"
	// systemSocketPath is the system Broker's local socket.
	systemSocketPath = "/run/peers/broker.sock"
	// accountPath is the environment of every account process.
	accountPath = "/usr/bin:/bin:/usr/sbin:/sbin"
	// procStopBound is how long a Broker process gets to exit after SIGTERM,
	// and again after SIGKILL. A Broker stops its listeners and links in well
	// under a second; the bound leaves room for a loaded container's scheduler.
	procStopBound = 5 * time.Second
)

// Proc is one Broker process started by a test.
type Proc struct {
	Cmd    *exec.Cmd
	PID    int
	Stdout *bytes.Buffer
	Stderr *bytes.Buffer

	exited  chan struct{}
	waitErr error
}

// Result is the outcome of one process run to completion.
type Result struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

// ProcInfo is one process found by Pids.
type ProcInfo struct {
	PID  int
	PPID int
	UID  uint32
	// Args is the process's argv, as /proc/<pid>/cmdline reports it.
	Args []string
}

// PeersBinary returns the path of the peers build the session helpers start.
func PeersBinary() string { return peersBinary }

// StartSystemBroker writes configYAML to the system configuration file when it
// is non-empty, runs "peers service run --system" as root, and waits for
// /run/peers/broker.sock. The process is terminated when the test ends.
func StartSystemBroker(t *testing.T, configYAML string) *Proc {
	t.Helper()
	return StartSystemBrokerEnv(t, configYAML, nil)
}

// StartSystemBrokerEnv is StartSystemBroker with extra environment entries,
// such as the variables a shell inherits from the process that opened it.
func StartSystemBrokerEnv(t *testing.T, configYAML string, env []string) *Proc {
	t.Helper()
	if !InContainer() {
		t.Fatalf("syscontainer: StartSystemBrokerEnv runs inside the container")
	}
	if configYAML != "" {
		writeSystemConfig(t, configYAML)
	}
	cmd := Command("root", env, peersBinary, "service", "run", "--system")
	return startAndWait(t, cmd, systemSocketPath, "the system Broker")
}

// StartUserBroker runs "peers service run --user <args>" as account and waits
// for that account's user socket. The process is terminated when the
// test ends.
func StartUserBroker(t *testing.T, account string, args ...string) *Proc {
	t.Helper()
	return StartUserBrokerPath(t, peersBinary, account, nil, args...)
}

// StartUserBrokerPath is StartUserBroker with an explicit executable and
// environment. It is the entry point for a test that runs a binary built at
// another version, such as the update scenario.
func StartUserBrokerPath(t *testing.T, exe, account string, env []string, args ...string) *Proc {
	t.Helper()
	if !InContainer() {
		t.Fatalf("syscontainer: StartUserBrokerPath runs inside the container")
	}
	socket := UserSocketPath(t, account)
	cmd := Command(account, env, exe, append([]string{"service", "run", "--user"}, args...)...)
	return startAndWait(t, cmd, socket, "the "+account+" user Broker")
}

// UserSocketPath returns the local socket path a user Broker of account
// binds.
func UserSocketPath(t *testing.T, account string) string {
	t.Helper()
	uid := AccountUID(t, account)
	return filepath.Join(fmt.Sprintf("/tmp/peers-%d", uid), "broker.sock")
}

// startAndWait starts cmd and waits for socket to appear. A process that exits
// before the socket exists fails the test with its standard error.
func startAndWait(t *testing.T, cmd *exec.Cmd, socket, what string) *Proc {
	t.Helper()
	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("syscontainer: starting %s: %v", what, err)
	}

	p := &Proc{
		Cmd:    cmd,
		PID:    cmd.Process.Pid,
		Stdout: stdout,
		Stderr: stderr,
		exited: make(chan struct{}),
	}
	go func() {
		p.waitErr = cmd.Wait()
		close(p.exited)
	}()
	t.Cleanup(func() { p.stop(t) })

	WaitFor(t, what+" socket "+socket, func() bool {
		if _, err := os.Stat(socket); err == nil {
			return true
		}
		select {
		case <-p.exited:
			t.Fatalf("syscontainer: %s exited before creating %s: %v\n%s",
				what, socket, p.waitErr, strings.TrimSpace(stderr.String()))
		default:
		}
		return false
	})

	return p
}

// stop terminates the process with SIGTERM and escalates to SIGKILL after five
// seconds.
func (p *Proc) stop(t *testing.T) {
	t.Helper()
	select {
	case <-p.exited:
		return
	default:
	}
	if err := p.Cmd.Process.Signal(syscall.SIGTERM); err != nil {
		_ = p.Cmd.Process.Kill()
	}
	select {
	case <-p.exited:
		return
	case <-time.After(procStopBound):
	}
	_ = p.Cmd.Process.Kill()
	select {
	case <-p.exited:
	case <-time.After(procStopBound):
		t.Logf("syscontainer: process %d did not exit after SIGKILL", p.PID)
	}
}

// Stop terminates the process and waits for it to exit. It is idempotent.
func (p *Proc) Stop(t *testing.T) {
	t.Helper()
	p.stop(t)
}

// AccountUID returns the uid of account from the account database.
func AccountUID(t *testing.T, account string) uint32 {
	t.Helper()
	_, uid, _ := accountIDs(t, account)
	return uid
}

// accountIDs looks account up in the account database and returns the entry
// with its primary uid and gid.
func accountIDs(t *testing.T, account string) (*user.User, uint32, uint32) {
	t.Helper()
	u, uid, gid, err := systemAccount(account)
	if err != nil {
		t.Fatalf("syscontainer: looking up account %q: %v", account, err)
	}
	return u, uid, gid
}

// systemAccount looks account up in the account database. The test binary is
// built without cgo as well, so os/user sees only /etc/passwd; when it misses,
// getent reaches the other NSS sources the image configures.
func systemAccount(account string) (*user.User, uint32, uint32, error) {
	u, err := user.Lookup(account)
	if err != nil {
		u, err = getentUser(account)
		if err != nil {
			return nil, 0, 0, err
		}
	}
	uid, err := strconv.ParseUint(u.Uid, 10, 32)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("account %q has a non-numeric uid %q", account, u.Uid)
	}
	gid, err := strconv.ParseUint(u.Gid, 10, 32)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("account %q has a non-numeric gid %q", account, u.Gid)
	}
	return u, uint32(uid), uint32(gid), nil
}

// getentUser reads one passwd entry through getent.
func getentUser(account string) (*user.User, error) {
	out, err := exec.Command("getent", "passwd", account).Output()
	if err != nil {
		return nil, fmt.Errorf("getent passwd %s: %w", account, err)
	}
	line := strings.TrimSuffix(string(out), "\n")
	fields := strings.Split(line, ":")
	if strings.Contains(line, "\n") || len(fields) != 7 || fields[0] != account {
		return nil, fmt.Errorf("getent passwd %s: unexpected output %q", account, line)
	}
	return &user.User{
		Username: fields[0],
		Uid:      fields[2],
		Gid:      fields[3],
		Name:     fields[4],
		HomeDir:  fields[5],
	}, nil
}

// RunAs runs the release peers binary as account ("root" is allowed) and
// returns its output and exit code.
func RunAs(t *testing.T, account string, env []string, args ...string) Result {
	t.Helper()
	if len(args) == 0 {
		t.Fatalf("syscontainer: RunAs needs a command")
	}
	return runCommand(t, account, env, peersBinary, args...)
}

// runCommand runs name as account and returns its output and exit code. A
// process that cannot be started fails the test; a non-zero exit is returned.
func runCommand(t *testing.T, account string, env []string, name string, args ...string) Result {
	t.Helper()
	cmd := Command(account, env, name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()

	result := Result{Stdout: stdout.String(), Stderr: stderr.String()}
	if err == nil {
		return result
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("syscontainer: running %s as %s: %v", name, account, err)
	}
	result.ExitCode = exitErr.ExitCode()
	return result
}

// Command builds a command that runs as account. The environment is built from
// an empty one: HOME, USER, LOGNAME, PATH, then env. Under coverage the
// coverage directory is added as GOCOVERDIR, unless env already names one. The
// working directory is the account's home. The credential is set from the
// account database, and it is left alone when the target account is the
// current one, because changing the group list then fails.
func Command(account string, env []string, name string, args ...string) *exec.Cmd {
	u, uid, gid := lookupAccount(account)
	cmd := exec.Command(name, args...)
	cmd.Dir = u.HomeDir
	cmd.Env = append([]string{
		"HOME=" + u.HomeDir,
		"USER=" + u.Username,
		"LOGNAME=" + u.Username,
		"PATH=" + accountPath,
	}, env...)
	if coverDir := harness.CoverageDir(); coverDir != "" && !hasEnv(env, "GOCOVERDIR") {
		cmd.Env = append(cmd.Env, "GOCOVERDIR="+coverDir)
	}
	if euid := os.Geteuid(); uint32(euid) != uid {
		cmd.SysProcAttr = &syscall.SysProcAttr{
			Credential: &syscall.Credential{
				Uid:    uid,
				Gid:    gid,
				Groups: accountGroups(u),
			},
		}
	}
	return cmd
}

// hasEnv reports whether env names the variable.
func hasEnv(env []string, name string) bool {
	prefix := name + "="
	for _, entry := range env {
		if strings.HasPrefix(entry, prefix) {
			return true
		}
	}
	return false
}

// lookupAccount is Command's account lookup. Command has no *testing.T, so a
// missing account panics; every account it is used with comes from the image.
func lookupAccount(account string) (*user.User, uint32, uint32) {
	u, uid, gid, err := systemAccount(account)
	if err != nil {
		panic(fmt.Sprintf("syscontainer: looking up account %q: %v", account, err))
	}
	return u, uid, gid
}

// accountGroups returns the supplementary groups of u.
func accountGroups(u *user.User) []uint32 {
	ids, err := u.GroupIds()
	if err != nil {
		ids = getentGroupIDs(u.Username)
	}
	groups := make([]uint32, 0, len(ids))
	for _, id := range ids {
		n, err := strconv.ParseUint(id, 10, 32)
		if err != nil {
			continue
		}
		groups = append(groups, uint32(n))
	}
	return groups
}

// getentGroupIDs reads an account's group list through getent, for a test
// binary whose os/user cannot see the extrausers source.
func getentGroupIDs(name string) []string {
	out, err := exec.Command("getent", "initgroups", name).Output()
	if err != nil {
		return nil
	}
	fields := strings.Fields(string(out))
	if len(fields) == 0 || fields[0] != name {
		return nil
	}
	return fields[1:]
}

// writeSystemConfig writes configYAML to the system configuration file, owned
// by root with mode 0600.
func writeSystemConfig(t *testing.T, configYAML string) {
	t.Helper()
	dir := filepath.Dir(systemConfigPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("syscontainer: creating %s: %v", dir, err)
	}
	if err := os.WriteFile(systemConfigPath, []byte(configYAML), 0o600); err != nil {
		t.Fatalf("syscontainer: writing %s: %v", systemConfigPath, err)
	}
	if err := os.Chmod(systemConfigPath, 0o600); err != nil {
		t.Fatalf("syscontainer: chmod %s: %v", systemConfigPath, err)
	}
	if err := os.Chown(systemConfigPath, 0, 0); err != nil {
		t.Fatalf("syscontainer: chown %s: %v", systemConfigPath, err)
	}
}

// Pids returns the processes whose command line contains every word. The scan
// is node.ListProcs, so this package does not read /proc itself.
func Pids(words ...string) []ProcInfo {
	procs, err := node.ListProcs(func(p node.Proc) bool {
		cmdline := strings.Join(p.Args, " ")
		for _, word := range words {
			if !strings.Contains(cmdline, word) {
				return false
			}
		}
		return true
	})
	if err != nil {
		return nil
	}
	out := make([]ProcInfo, 0, len(procs))
	for _, p := range procs {
		uid := uint32(0)
		if p.UID >= 0 {
			uid = uint32(p.UID)
		}
		out = append(out, ProcInfo{PID: p.PID, PPID: p.PPID, UID: uid, Args: p.Args})
	}
	return out
}
