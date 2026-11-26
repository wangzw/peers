//go:build e2e

package e2e

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/syscontainer"
)

const (
	serviceControlUserPort   = "17448"
	serviceControlSystemPort = "17449"
	// degradedObservationWindow is how long a degraded Broker is watched for a
	// restart; the unit's RestartSec is 5 seconds, so 30 seconds is six intervals.
	degradedObservationWindow = 30 * time.Second
)

// TestServiceControl checks that stop, start and restart act on one service
// level and that a degraded start keeps the Broker running.
func TestServiceControl(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	t.Run("stop and start act on one level only", func(t *testing.T) {
		userEnv := serviceControlSetup(t)
		uid := strconv.FormatUint(uint64(syscontainer.AccountUID(t, "peer")), 10)
		userSocket := "/tmp/peers-" + uid + "/broker.sock"

		systemBefore, ok := systemServiceMainPID(t)
		if !ok || systemBefore == 0 {
			t.Fatalf("the system service MainPID = %d (found %v), want a running Broker", systemBefore, ok)
		}
		userBefore := userServiceMainPID(t, userEnv)
		if userBefore == 0 {
			t.Fatal("the user service MainPID is zero after the install")
		}

		res := runWithEnv(t, "peer", userEnv, releaseBinary, "service", "stop")
		if res.ExitCode != 0 {
			t.Fatalf("service stop as peer: exit %d\nstdout: %s\nstderr: %s",
				res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
		}
		if want := "The user Broker service has stopped.\n"; res.Stdout != want {
			t.Errorf("stdout = %q, want %q", res.Stdout, want)
		}
		harness.Eventually(t, 10*time.Second, "the user service to stop", func() (bool, string) {
			pid := userServiceMainPID(t, userEnv)
			return pid == 0, fmt.Sprintf("user service MainPID = %d", pid)
		})
		if procs := peerBrokers(syscontainer.AccountUID(t, "peer")); len(procs) != 0 {
			t.Errorf("the peer Broker still runs after service stop: %v", procs)
		}
		if !systemServiceIsActive(t) {
			t.Error("the system service is not active after stopping the user service")
		}
		if pid, ok := systemServiceMainPID(t); !ok || pid != systemBefore {
			t.Errorf("the system service MainPID = %d (found %v), want the unchanged %d", pid, ok, systemBefore)
		}

		res = syscontainer.RunAs(t, "root", nil, "service", "restart")
		if res.ExitCode != 0 {
			t.Fatalf("service restart as root: exit %d\nstdout: %s\nstderr: %s",
				res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
		}
		if want := "The system Broker service has restarted.\n"; res.Stdout != want {
			t.Errorf("stdout = %q, want %q", res.Stdout, want)
		}
		harness.Eventually(t, 20*time.Second, "the system service to run a new process", func() (bool, string) {
			pid, ok := systemServiceMainPID(t)
			if !ok {
				return false, "systemctl show did not report a MainPID"
			}
			return pid != 0 && pid != systemBefore, fmt.Sprintf("system service MainPID = %d, want a pid different from %d", pid, systemBefore)
		})
		if pid := userServiceMainPID(t, userEnv); pid != 0 {
			t.Errorf("the user service MainPID = %d after restarting the system service, want 0", pid)
		}

		res = runWithEnv(t, "peer", userEnv, releaseBinary, "service", "start")
		if res.ExitCode != 0 {
			t.Fatalf("service start as peer: exit %d\nstdout: %s\nstderr: %s",
				res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
		}
		if want := "The user Broker service has started.\n"; res.Stdout != want {
			t.Errorf("stdout = %q, want %q", res.Stdout, want)
		}
		harness.Eventually(t, 20*time.Second, "the user service to run again", func() (bool, string) {
			pid := userServiceMainPID(t, userEnv)
			return pid != 0, fmt.Sprintf("user service MainPID = %d", pid)
		})
		waitForSocket(t, userSocket)
	})

	t.Run("a user command with only the system service names sudo", func(t *testing.T) {
		userEnv := serviceControlReset(t)
		writeSystemListenConfig(t, "127.0.0.1:"+serviceControlSystemPort)
		installSystemService(t)
		syscontainer.WaitFor(t, "the system Broker socket "+systemSocket, func() bool {
			_, err := os.Stat(systemSocket)
			return err == nil
		})

		res := runWithEnv(t, "peer", userEnv, releaseBinary, "service", "start")
		if res.ExitCode != 1 {
			t.Fatalf("service start as peer with only a system install: exit %d, want 1\nstdout: %s\nstderr: %s",
				res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
		}
		for _, want := range []string{"no user service is installed", "but the system service is", "sudo peers service start --system"} {
			if !strings.Contains(res.Stderr, want) {
				t.Errorf("stderr = %q, want it to contain %q", strings.TrimSpace(res.Stderr), want)
			}
		}
	})

	t.Run("no installed service names the level", func(t *testing.T) {
		serviceControlReset(t)

		res := syscontainer.RunAs(t, "root", nil, "service", "start")
		if res.ExitCode != 1 {
			t.Fatalf("service start as root with nothing installed: exit %d, want 1\nstdout: %s\nstderr: %s",
				res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
		}
		for _, want := range []string{"no system service is installed", "sudo peers service install --system"} {
			if !strings.Contains(res.Stderr, want) {
				t.Errorf("stderr = %q, want it to contain %q", strings.TrimSpace(res.Stderr), want)
			}
		}
	})

	t.Run("start reports a degraded Broker and keeps the service running", func(t *testing.T) {
		userEnv := serviceControlSetup(t)
		unitPath := userUnitPath(t)
		if unit := readFile(t, unitPath); !strings.Contains(unit, "--allow-degraded") {
			t.Fatalf("the user unit %s does not pass --allow-degraded:\n%s", unitPath, unit)
		}
		if unit := readFile(t, systemUnitPath); !strings.Contains(unit, "--allow-degraded") {
			t.Fatalf("the system unit %s does not pass --allow-degraded:\n%s", systemUnitPath, unit)
		}

		res := runWithEnv(t, "peer", userEnv, releaseBinary, "service", "stop")
		if res.ExitCode != 0 {
			t.Fatalf("service stop as peer: exit %d\nstdout: %s\nstderr: %s",
				res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
		}
		harness.Eventually(t, 10*time.Second, "the user service to stop before the port is taken", func() (bool, string) {
			pid := userServiceMainPID(t, userEnv)
			return pid == 0, fmt.Sprintf("user service MainPID = %d", pid)
		})

		listener, err := net.Listen("tcp", "127.0.0.1:"+serviceControlUserPort)
		if err != nil {
			t.Fatalf("holding the user Broker's port: %v", err)
		}
		t.Cleanup(func() { _ = listener.Close() })

		res = runWithEnv(t, "peer", userEnv, releaseBinary, "service", "start")
		if res.ExitCode != 3 {
			t.Fatalf("service start with the port taken: exit %d, want 3\nstdout: %s\nstderr: %s",
				res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
		}
		if want := "The user Broker service has started.\n"; res.Stdout != want {
			t.Errorf("stdout = %q, want %q", res.Stdout, want)
		}
		status := runWithEnv(t, "peer", userEnv, releaseBinary, "service", "status")
		if status.ExitCode != 0 {
			t.Fatalf("peers service status in local-only degradation: exit %d\nstdout: %s\nstderr: %s",
				status.ExitCode, strings.TrimSpace(status.Stdout), strings.TrimSpace(status.Stderr))
		}
		for _, want := range []string{"Broker: degraded (local only):", "Fix:"} {
			if !strings.Contains(status.Stdout, want) {
				t.Errorf("status stdout = %q, want it to contain %q", strings.TrimSpace(status.Stdout), want)
			}
		}

		if !userServiceIsActive(t, userEnv) {
			t.Fatal("the user service is not active after the degraded start")
		}
		pid := userServiceMainPID(t, userEnv)
		if pid == 0 {
			t.Fatal("the user service MainPID is zero after the degraded start")
		}
		t.Logf("the degraded user Broker runs with MainPID %d", pid)
		harness.Consistently(t, degradedObservationWindow, "the degraded Broker to keep the same pid", func() (bool, string) {
			current := userServiceMainPID(t, userEnv)
			return current == pid, fmt.Sprintf("MainPID = %d, want the unchanged %d", current, pid)
		})
	})
}

func serviceControlSetup(t *testing.T) []string {
	t.Helper()
	userEnv := serviceControlReset(t)

	writeAccountFile(t, "peer", "/home/peer/.peers/config.yaml",
		fmt.Sprintf("broker:\n  listen: \"127.0.0.1:%s\"\n", serviceControlUserPort))
	writeSystemListenConfig(t, "127.0.0.1:"+serviceControlSystemPort)

	installSystemService(t)
	syscontainer.WaitFor(t, "the system Broker socket "+systemSocket, func() bool {
		_, err := os.Stat(systemSocket)
		return err == nil
	})

	install := runWithEnv(t, "peer", userEnv, releaseBinary, "service", "install")
	if install.ExitCode != 0 {
		t.Fatalf("service install as peer: exit %d\nstdout: %s\nstderr: %s",
			install.ExitCode, strings.TrimSpace(install.Stdout), strings.TrimSpace(install.Stderr))
	}
	uid := strconv.FormatUint(uint64(syscontainer.AccountUID(t, "peer")), 10)
	waitForSocket(t, "/tmp/peers-"+uid+"/broker.sock")
	return userEnv
}

func serviceControlReset(t *testing.T) []string {
	t.Helper()

	peerUID := syscontainer.AccountUID(t, "peer")
	uid := strconv.FormatUint(uint64(peerUID), 10)
	userEnv := []string{
		"XDG_RUNTIME_DIR=/run/user/" + uid,
		"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/" + uid + "/bus",
	}

	mustRun(t, "root", nil, "loginctl", "enable-linger", "peer")
	mustRun(t, "root", nil, "systemctl", "start", "user@"+uid+".service")
	waitUserManager(t, peerUID)

	reset := func() {
		_ = runWithEnv(t, "peer", userEnv, releaseBinary, "service", "remove")
		_ = runWithEnv(t, "peer", userEnv, "systemctl", "--user", "stop", "peers.service")
		_ = runWithEnv(t, "peer", userEnv, "systemctl", "--user", "disable", "peers.service")
		_ = os.Remove(userUnitPath(t))
		resetSystemServiceState(t)
	}
	reset()
	t.Cleanup(reset)
	return userEnv
}

func waitForSocket(t *testing.T, path string) {
	t.Helper()
	syscontainer.WaitFor(t, "the Broker socket "+path, func() bool {
		_, err := os.Stat(path)
		return err == nil
	})
}

func systemServiceIsActive(t *testing.T) bool {
	t.Helper()
	res := runCommandAs(t, "root", "systemctl", "is-active", "peers.service")
	return strings.TrimSpace(res.Stdout) == "active"
}

func userServiceIsActive(t *testing.T, userEnv []string) bool {
	t.Helper()
	res := runWithEnv(t, "peer", userEnv, "systemctl", "--user", "is-active", "peers.service")
	return strings.TrimSpace(res.Stdout) == "active"
}
