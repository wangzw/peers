//go:build e2e

package e2e

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/test/e2e/syscontainer"
)

// TestSystemServiceInstallListenPortConflict checks that an occupied listen address
// fails the install before it registers anything and that reinstalling over the
// running service still succeeds.
func TestSystemServiceInstallListenPortConflict(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	t.Run("an occupied port fails the install and registers nothing", func(t *testing.T) {
		resetSystemServiceState(t)
		t.Cleanup(func() { resetSystemServiceState(t) })

		addr := occupiedListenAddr(t)
		writeSystemListenConfig(t, addr)

		res := syscontainer.RunAs(t, "root", nil, "service", "install", "--system")
		combined := res.Stdout + res.Stderr
		if res.ExitCode == 0 {
			t.Fatalf("service install --system accepted broker.listen %s held by another process\nstdout: %s\nstderr: %s",
				addr, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
		}
		for _, want := range []string{addr, "already in use", systemConfigPath, "broker.listen"} {
			if !strings.Contains(combined, want) {
				t.Errorf("service install --system error does not mention %q (exit %d)\nstdout: %s\nstderr: %s",
					want, res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
			}
		}
		assertSystemServiceNotRegistered(t)
	})

	t.Run("reinstalling while the service runs succeeds", func(t *testing.T) {
		resetSystemServiceState(t)
		t.Cleanup(func() { resetSystemServiceState(t) })

		addr := "127.0.0.1:" + reinstallListenPort
		writeSystemListenConfig(t, addr)

		install := syscontainer.RunAs(t, "root", nil, "service", "install", "--system")
		if install.ExitCode != 0 {
			t.Fatalf("first service install --system on %s: exit %d\nstdout: %s\nstderr: %s",
				addr, install.ExitCode, strings.TrimSpace(install.Stdout), strings.TrimSpace(install.Stderr))
		}
		syscontainer.WaitFor(t, "the system Broker socket "+systemSocket, func() bool {
			_, err := os.Stat(systemSocket)
			return err == nil
		})

		// The service's own Broker holds addr, so the preflight must recognize
		// that occupant.
		again := syscontainer.RunAs(t, "root", nil, "service", "install", "--system")
		if again.ExitCode != 0 {
			t.Fatalf("reinstalling service --system over the running service: exit %d\nstdout: %s\nstderr: %s",
				again.ExitCode, strings.TrimSpace(again.Stdout), strings.TrimSpace(again.Stderr))
		}
		enabled := runCommandAs(t, "root", "systemctl", "is-enabled", "peers.service")
		if state := strings.TrimSpace(enabled.Stdout); state != "enabled" {
			t.Errorf("systemctl is-enabled peers.service after the reinstall = %q (exit %d), want %q\nstderr: %s",
				state, enabled.ExitCode, "enabled", strings.TrimSpace(enabled.Stderr))
		}
		syscontainer.WaitFor(t, "the reinstalled system Broker to answer on "+addr, func() bool {
			conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
			if err != nil {
				return false
			}
			_ = conn.Close()
			return true
		})
	})
}

// reinstallListenPort is outside the kernel's ephemeral range and unused by the
// other system tests.
const reinstallListenPort = "17447"

func occupiedListenAddr(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("holding a loopback port: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener.Addr().String()
}

func writeSystemListenConfig(t *testing.T, addr string) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(systemConfigPath), 0o755); err != nil {
		t.Fatalf("creating %s: %v", filepath.Dir(systemConfigPath), err)
	}
	content := fmt.Sprintf("broker:\n  listen: %q\n", addr)
	if err := os.WriteFile(systemConfigPath, []byte(content), 0o600); err != nil {
		t.Fatalf("writing %s: %v", systemConfigPath, err)
	}
	if err := os.Chmod(systemConfigPath, 0o600); err != nil {
		t.Fatalf("chmod %s: %v", systemConfigPath, err)
	}
	if err := os.Chown(systemConfigPath, 0, 0); err != nil {
		t.Fatalf("chown %s: %v", systemConfigPath, err)
	}
}

func resetSystemServiceState(t *testing.T) {
	t.Helper()

	_ = syscontainer.RunAs(t, "root", nil, "service", "remove", "--system")
	_ = runCommandAs(t, "root", "systemctl", "stop", "peers.service")
	_ = runCommandAs(t, "root", "systemctl", "disable", "peers.service")
	_ = runCommandAs(t, "root", "systemctl", "daemon-reload")
	_ = os.Remove(systemUnitPath)
}

func assertSystemServiceNotRegistered(t *testing.T) {
	t.Helper()

	enabled := runCommandAs(t, "root", "systemctl", "is-enabled", "peers.service")
	if state := strings.TrimSpace(enabled.Stdout); state == "enabled" {
		t.Errorf("systemctl is-enabled peers.service = %q after the failed installation, want it not enabled\nstderr: %s",
			state, strings.TrimSpace(enabled.Stderr))
	}
	active := runCommandAs(t, "root", "systemctl", "is-active", "peers.service")
	switch state := strings.TrimSpace(active.Stdout); state {
	case "active", "activating", "reloading", "deactivating":
		t.Errorf("systemctl is-active peers.service = %q after the failed installation, want it inactive", state)
	}
	if procs := syscontainer.Pids("peers", "service", "run", "--system"); len(procs) != 0 {
		t.Errorf("the system Broker still runs after the failed installation: %v", procs)
	}
	// The unit is written only after the listen precheck, so a failed install
	// leaves no unit file.
	if _, err := os.Stat(systemUnitPath); err == nil {
		t.Errorf("the unit file %s exists after the precheck failed", systemUnitPath)
	}
}
