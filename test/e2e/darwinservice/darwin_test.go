//go:build e2e_darwin

package darwinservice

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/test/e2e/node"
)

// TestSystemService installs the system Broker with the real subcommand,
// checks that launchd runs it, kills it to see launchd start a new process, and
// uninstalls it. The system and user tests run serially.
func TestSystemService(t *testing.T) {
	resetSystemService(t)
	t.Cleanup(func() { resetSystemService(t) })

	res := run(t, "sudo", "-n", installedBinary, "service", "install", "--system")
	if res.exitCode != 0 {
		t.Fatalf("service install --system: exit %d\nstdout: %s\nstderr: %s",
			res.exitCode, res.stdout, res.stderr)
	}
	if _, err := os.Stat(daemonPlist); err != nil {
		t.Fatalf("plist %s after install: %v", daemonPlist, err)
	}

	pid := waitForPID(t, "system", serviceStartBound)
	t.Logf("launchd started the system Broker with pid %d", pid)

	killPID(t, pid, true)
	restarted := waitForNewPID(t, "system", pid, restartBound)
	t.Logf("launchd restarted the system Broker with pid %d", restarted)

	res = run(t, "sudo", "-n", installedBinary, "service", "remove", "--system")
	if res.exitCode != 0 {
		t.Fatalf("service remove --system: exit %d\nstdout: %s\nstderr: %s",
			res.exitCode, res.stdout, res.stderr)
	}
	waitForGone(t, "system", serviceStopBound)
	if processAlive(restarted) {
		t.Errorf("pid %d still runs after uninstall", restarted)
	}
	if _, err := os.Stat(daemonPlist); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("plist %s still exists after uninstall: %v", daemonPlist, err)
	}
}

// TestUserService installs the user Broker in the account's launchd
// domain, checks that it runs and answers `peers service status`, and uninstalls
// it.
func TestUserService(t *testing.T) {
	resetUserService(t)
	t.Cleanup(func() { resetUserService(t) })

	domain := userDomain(t)
	t.Logf("using the launchd domain %s", domain)

	res := run(t, installedBinary, "service", "install")
	if res.exitCode != 0 {
		t.Fatalf("service install: exit %d\nstdout: %s\nstderr: %s",
			res.exitCode, res.stdout, res.stderr)
	}
	plist := filepath.Join(os.Getenv("HOME"), "Library", "LaunchAgents", serviceLabel+".plist")
	if _, err := os.Stat(plist); err != nil {
		t.Fatalf("plist %s after install: %v", plist, err)
	}

	pid := waitForPID(t, domain, serviceStartBound)
	t.Logf("launchd started the user Broker with pid %d", pid)

	socket := fmt.Sprintf("/tmp/peers-%d/broker.sock", os.Getuid())
	waitFor(t, serviceStartBound, "the user Broker socket "+socket, func() (bool, string) {
		_, err := os.Stat(socket)
		return err == nil, fmt.Sprint(err)
	})

	var status result
	waitFor(t, serviceStartBound, "peers service status to connect to the user Broker", func() (bool, string) {
		status = run(t, installedBinary, "service", "status")
		return status.exitCode == 0, fmt.Sprintf("exit %d\nstdout: %s\nstderr: %s", status.exitCode, status.stdout, status.stderr)
	})
	t.Logf("peers service status connected to the user Broker: %s", status.stdout)

	res = run(t, installedBinary, "service", "remove")
	if res.exitCode != 0 {
		t.Fatalf("service remove: exit %d\nstdout: %s\nstderr: %s",
			res.exitCode, res.stdout, res.stderr)
	}
	waitForGone(t, domain, serviceStopBound)
	if processAlive(pid) {
		t.Errorf("pid %d still runs after uninstall", pid)
	}
	if _, err := os.Stat(plist); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("plist %s still exists after uninstall: %v", plist, err)
	}
}

// userDomain returns the launchd domain the user service uses. The
// product installs only into gui/<uid>; a runner without that domain cannot
// install a user service at all.
func userDomain(t *testing.T) string {
	t.Helper()
	uid := os.Getuid()
	domain := fmt.Sprintf("gui/%d", uid)
	if _, err := exec.Command("launchctl", "print", domain).Output(); err != nil {
		t.Fatalf("the runner has no %s, so a user install must fail", domain)
	}
	return domain
}

// resetSystemService removes any system installation left by an earlier
// run. It is called before the test and from t.Cleanup.
func resetSystemService(t *testing.T) {
	t.Helper()
	_ = sudo(installedBinary, "service", "remove", "--system")
	bootout("system")
	_ = sudo("rm", "-f", daemonPlist)
}

// resetUserService removes any user installation left by an earlier run.
func resetUserService(t *testing.T) {
	t.Helper()
	_ = exec.Command(installedBinary, "service", "remove").Run()
	bootout(fmt.Sprintf("gui/%d", os.Getuid()))
	_ = os.Remove(filepath.Join(os.Getenv("HOME"), "Library", "LaunchAgents", serviceLabel+".plist"))
}

const (
	// reloginUserPort is the user Broker's listen port in the relogin test.
	reloginUserPort = "17445"
	// reloginPeerPort is the directly run system Broker's listen port. A
	// separate port avoids the service tests' default and any launchd label.
	reloginPeerPort = "17446"
	// reloginRecoveryBound bounds the wait for the restarted user Broker's
	// link. It covers installation readiness, launchd's default 10-second
	// ThrottleInterval, and the user Broker's 2-second reconnect maximum.
	reloginRecoveryBound = 30 * time.Second
	// mutationEnv selects a negative control for the relogin test.
	mutationEnv = "PEERS_E2E_MUTATION"
)

// TestUserServiceRestoresLinkAfterRelogin installs the user service,
// pairs it with a system Broker that the test runs directly, and then reloads
// the single user plist with bootout and bootstrap. That reload is what launchd
// does at login. The test asserts a new Broker pid and a CONNECTED link.
func TestUserServiceRestoresLinkAfterRelogin(t *testing.T) {
	resetUserService(t)
	t.Cleanup(func() { resetUserService(t) })
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		logPath := filepath.Join(os.Getenv("HOME"), "Library", "Logs", "peers", "broker.log")
		if data, err := os.ReadFile(logPath); err == nil {
			t.Logf("user Broker log %s:\n%s", logPath, data)
		}
	})

	// Start with empty peer tables so a rerun in the same job pairs afresh.
	_ = os.Remove(filepath.Join(os.Getenv("HOME"), ".peers", "peers.json"))
	_ = sudo("rm", "-f", "/var/lib/peers/peers.json")
	writeUserConfig(t, ":"+reloginUserPort)
	peer := startSystemPeer(t)

	res := run(t, installedBinary, "service", "install")
	if res.exitCode != 0 {
		t.Fatalf("service install: exit %d\nstdout: %s\nstderr: %s",
			res.exitCode, res.stdout, res.stderr)
	}
	userSocket := fmt.Sprintf("/tmp/peers-%d/broker.sock", os.Getuid())
	waitFor(t, serviceStartBound, "the user Broker socket "+userSocket, func() (bool, string) {
		_, err := os.Stat(userSocket)
		return err == nil, fmt.Sprint(err)
	})

	key := createSystemPairingKey(t)
	alias := connectUserBroker(t, userSocket, "127.0.0.1:"+reloginPeerPort, key, "system")
	waitForLink(t, userSocket, alias, reloginRecoveryBound)

	domain := userDomain(t)
	beforePID := waitForPID(t, domain, serviceStartBound)
	t.Logf("user Broker pid before the simulated login: %d", beforePID)

	mutation := os.Getenv(mutationEnv)
	if mutation == "break-link" {
		peer.stop(t)
		t.Logf("mutation break-link: the peer Broker was stopped")
	}

	plist := filepath.Join(os.Getenv("HOME"), "Library", "LaunchAgents", serviceLabel+".plist")
	bootout(domain)
	switch mutation {
	case "skip-bootstrap":
		t.Logf("mutation skip-bootstrap: the plist was not bootstrapped")
	default:
		if err := exec.Command("launchctl", "bootstrap", domain, plist).Run(); err != nil {
			t.Fatalf("launchctl bootstrap %s %s: %v", domain, plist, err)
		}
	}

	afterPID := waitForNewPID(t, domain, beforePID, reloginRecoveryBound)
	t.Logf("user Broker pid after the simulated login: %d", afterPID)
	waitForLink(t, userSocket, alias, reloginRecoveryBound)
}

// writeUserConfig writes the user Broker's configuration with the relogin
// test's listen port and fast reconnect timers.
func writeUserConfig(t *testing.T, listen string) {
	t.Helper()
	dir := filepath.Join(os.Getenv("HOME"), ".peers")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("creating %s: %v", dir, err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatalf("chmod %s: %v", dir, err)
	}
	content := fmt.Sprintf("broker:\n  listen: %q\nlink:\n  reconnect_min: 1s\n  reconnect_max: 2s\n", listen)
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(content), 0o600); err != nil {
		t.Fatalf("writing the user configuration: %v", err)
	}
}

// systemPeer is a system Broker started directly by the test rather than
// through a LaunchDaemon, so it has no launchd label to collide with.
type systemPeer struct {
	pid     int
	done    chan struct{}
	log     *os.File
	stopped bool
}

// startSystemPeer runs the system Broker as root on reloginPeerPort and waits
// for its socket and TCP listener.
func startSystemPeer(t *testing.T) *systemPeer {
	t.Helper()
	if _, err := unixPeerPID(systemSocketPath); err == nil {
		t.Fatalf("a system Broker already answers on %s; stop it before this test", systemSocketPath)
	}
	ensureSystemConfig(t)
	logFile, err := os.CreateTemp("", "peers-system-broker-*.log")
	if err != nil {
		t.Fatalf("creating the peer log: %v", err)
	}
	cmd := exec.Command("sudo", "-n", installedBinary, "service", "run", "--system", "--listen", ":"+reloginPeerPort)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatalf("starting the peer Broker: %v", err)
	}
	peer := &systemPeer{pid: cmd.Process.Pid, done: make(chan struct{}), log: logFile}
	go func() { _ = cmd.Wait(); close(peer.done) }()
	t.Cleanup(func() { peer.stop(t) })

	waitFor(t, serviceStartBound, "the peer Broker socket "+systemSocketPath, func() (bool, string) {
		_, err := os.Stat(systemSocketPath)
		return err == nil, fmt.Sprint(err)
	})
	waitFor(t, serviceStartBound, "the peer Broker to listen on 127.0.0.1:"+reloginPeerPort, func() (bool, string) {
		conn, err := net.DialTimeout("tcp", "127.0.0.1:"+reloginPeerPort, 200*time.Millisecond)
		if err != nil {
			return false, err.Error()
		}
		_ = conn.Close()
		return true, ""
	})
	// Prefer the process that owns the socket; sudo normally execs the command,
	// so this is the same pid.
	if pid, err := unixPeerPID(systemSocketPath); err == nil {
		peer.pid = pid
	}
	t.Logf("the peer Broker runs with pid %d on port %s", peer.pid, reloginPeerPort)
	return peer
}

// ensureSystemConfig creates the system Broker's configuration file when this
// test runs without the system service test before it. The system Broker's
// access check reads this file; a missing file is a permission denial.
func ensureSystemConfig(t *testing.T) {
	t.Helper()
	const path = "/etc/peers/config.yaml"
	if _, err := os.Stat(path); err == nil {
		return
	}
	if err := sudo("mkdir", "-p", filepath.Dir(path)); err != nil {
		t.Fatalf("creating the system configuration directory: %v", err)
	}
	tmp, err := os.CreateTemp("", "peers-system-config-*")
	if err != nil {
		t.Fatalf("creating the system configuration: %v", err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.WriteString("# system Broker configuration for the end-to-end test\n"); err != nil {
		_ = tmp.Close()
		t.Fatalf("writing the system configuration: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("closing the system configuration: %v", err)
	}
	if err := sudo("install", "-o", "root", "-g", "wheel", "-m", "0600", tmp.Name(), path); err != nil {
		t.Fatalf("installing the system configuration: %v", err)
	}
}

// stop terminates the peer Broker and reports its log when the test failed.
func (p *systemPeer) stop(t *testing.T) {
	t.Helper()
	if p.stopped {
		return
	}
	p.stopped = true
	if err := sudo("kill", "-TERM", strconv.Itoa(p.pid)); err != nil {
		t.Logf("stopping the peer Broker: %v", err)
	}
	// These are the failure-path escalation from SIGTERM to SIGKILL, not a wait
	// a correct run depends on: if SIGTERM does not converge in ten seconds the
	// process is killed, and if it still does not exit within five the test only
	// logs it. Neither bound can fail the test, so their fixed intervals stay.
	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
		_ = sudo("kill", "-9", strconv.Itoa(p.pid))
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			t.Logf("the peer Broker did not exit")
		}
	}
	_ = p.log.Close()
	if t.Failed() {
		if data, err := os.ReadFile(p.log.Name()); err == nil {
			t.Logf("peer Broker log:\n%s", data)
		}
	}
}

// unixPeerPID returns the pid at the other end of a connection to the Unix
// socket at path.
func unixPeerPID(path string) (int, error) {
	conn, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close() }()
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return 0, fmt.Errorf("%s is not a Unix socket", path)
	}
	raw, err := unixConn.SyscallConn()
	if err != nil {
		return 0, err
	}
	var (
		pid     int
		credErr error
	)
	if err := raw.Control(func(fd uintptr) {
		pid, credErr = unix.GetsockoptInt(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERPID)
	}); err != nil {
		return 0, err
	}
	if credErr != nil {
		return 0, credErr
	}
	if pid == 0 {
		return 0, fmt.Errorf("%s has no peer pid", path)
	}
	return pid, nil
}

// createSystemPairingKey creates a one-time key on the system Broker by running
// the test binary as root, where the system configuration file is writable.
func createSystemPairingKey(t *testing.T) string {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatalf("locating the test binary: %v", err)
	}
	cmd := exec.Command("sudo", "-n", "env", roleEnv+"="+systemPairKeyRole, binary)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("creating a system pairing key: %v\n%s", err, stderr.String())
	}
	key := strings.TrimSpace(string(out))
	if key == "" {
		t.Fatal("the system pairing key is empty")
	}
	return key
}

// connectUserBroker pairs the user Broker with address and returns the alias
// the user Broker uses for the peer.
func connectUserBroker(t *testing.T, socket, address, key, localAlias string) string {
	t.Helper()
	oracle, err := node.DialOracle(socket)
	if err != nil {
		t.Fatalf("dialing the user Broker: %v", err)
	}
	defer func() { _ = oracle.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	resp, err := oracle.Connect(ctx, address, key, localAlias)
	if err != nil {
		t.Fatalf("pairing with %s: %v", address, err)
	}
	alias := resp.GetAlias()
	if alias == "" {
		t.Fatal("the pairing returned an empty alias")
	}
	t.Logf("paired with %s as %q", address, alias)
	return alias
}

// waitForLink waits until the user Broker reports alias as CONNECTED.
func waitForLink(t *testing.T, socket, alias string, bound time.Duration) {
	t.Helper()
	waitFor(t, bound, "the link to "+alias+" to be connected", func() (bool, string) {
		// One ListBrokers sample gets five seconds. A transport error is retried
		// by the outer waitFor within bound, so a slow sample costs a retry
		// rather than failing the wait.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		oracle, err := node.DialOracle(socket)
		if err != nil {
			return false, err.Error()
		}
		defer func() { _ = oracle.Close() }()
		resp, err := oracle.ListBrokers(ctx)
		if err != nil {
			return false, err.Error()
		}
		for _, broker := range resp.GetBrokers() {
			if broker.GetAlias() == alias {
				return broker.GetStatus() == localpb.BrokerStatus_BROKER_STATUS_CONNECTED,
					fmt.Sprintf("status = %s, link = %s", broker.GetStatus(), broker.GetLinkState())
			}
		}
		return false, "alias " + alias + " is not listed"
	})
}
