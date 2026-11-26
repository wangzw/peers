//go:build e2e_darwin

package darwinservice

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestUserServiceInstallRefusesAnOccupiedListenPort checks that a user service
// install refuses a broker.listen address another process holds and names the
// address, the configuration file and the key at fault.
func TestUserServiceInstallRefusesAnOccupiedListenPort(t *testing.T) {
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

	configPath := preserveUserConfig(t)
	addr := occupiedListenAddr(t)
	writeUserConfig(t, addr)

	res := run(t, installedBinary, "service", "install")
	combined := res.stdout + res.stderr
	if res.exitCode == 0 {
		t.Fatalf("service install accepted broker.listen %s held by another process\nstdout: %s\nstderr: %s",
			addr, res.stdout, res.stderr)
	}
	for _, want := range []string{addr, "already in use", configPath, "broker.listen"} {
		if !strings.Contains(combined, want) {
			t.Errorf("service install error does not mention %q (exit %d)\nstdout: %s\nstderr: %s",
				want, res.exitCode, res.stdout, res.stderr)
		}
	}

	domain := userDomain(t)
	if out, loaded := launchctlPrint(domain); loaded {
		t.Errorf("launchd still has %s/%s loaded after the failed installation:\n%s",
			domain, serviceLabel, out)
	}
	plist := filepath.Join(os.Getenv("HOME"), "Library", "LaunchAgents", serviceLabel+".plist")
	if _, err := os.Stat(plist); err == nil {
		t.Errorf("plist %s exists after the failed installation", plist)
	}
}

func occupiedListenAddr(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("holding a loopback port: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener.Addr().String()
}

func preserveUserConfig(t *testing.T) string {
	t.Helper()

	path := filepath.Join(os.Getenv("HOME"), ".peers", "config.yaml")
	data, err := os.ReadFile(path)
	existed := err == nil
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reading %s: %v", path, err)
	}
	t.Cleanup(func() {
		if !existed {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Errorf("removing %s: %v", path, err)
			}
			return
		}
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Errorf("restoring %s: %v", path, err)
		}
	})
	return path
}
