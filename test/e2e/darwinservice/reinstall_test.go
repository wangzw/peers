//go:build e2e_darwin

package darwinservice

import (
	"testing"
	"time"
)

const (
	// reinstallCommandBound covers the worst case of the 20-second stop budget,
	// the 10-second bootstrap retry and the 20-second readiness wait, 50 seconds
	// in all, plus margin for a loaded runner.
	reinstallCommandBound = 75 * time.Second
)

// TestSystemServiceReinstall checks that installing the system service while it
// runs replaces the running Broker.
func TestSystemServiceReinstall(t *testing.T) {
	resetSystemService(t)
	t.Cleanup(func() { resetSystemService(t) })

	reinstallRunningService(t, "system",
		"sudo", "-n", installedBinary, "service", "install", "--system")
}

// TestUserServiceReinstall checks the same reinstall in the account's gui
// domain.
func TestUserServiceReinstall(t *testing.T) {
	resetUserService(t)
	t.Cleanup(func() { resetUserService(t) })

	reinstallRunningService(t, userDomain(t),
		installedBinary, "service", "install")
}

func reinstallRunningService(t *testing.T, domain, name string, args ...string) {
	t.Helper()

	res := runWithin(t, reinstallCommandBound, name, args...)
	if res.exitCode != 0 {
		t.Fatalf("first service install: exit %d\nstdout: %s\nstderr: %s",
			res.exitCode, res.stdout, res.stderr)
	}
	first := waitForPID(t, domain, serviceStartBound)
	t.Logf("launchd started the first Broker with pid %d in %s", first, domain)

	if out, loaded := launchctlPrint(domain); loaded {
		t.Logf("before the reinstall, %s/%s is loaded:\n%s", domain, serviceLabel, out)
	} else {
		t.Logf("before the reinstall, %s/%s is not loaded:\n%s", domain, serviceLabel, out)
	}

	res = runWithin(t, reinstallCommandBound, name, args...)
	if res.exitCode != 0 {
		out, loaded := launchctlPrint(domain)
		t.Fatalf("service install over the running service: exit %d\nstdout: %s\nstderr: %s\nlaunchctl print %s/%s (loaded=%t):\n%s",
			res.exitCode, res.stdout, res.stderr, domain, serviceLabel, loaded, out)
	}
	second := waitForNewPID(t, domain, first, restartBound)
	t.Logf("the reinstall replaced pid %d with pid %d", first, second)

	statusArgs := argsForStatus(domain)
	status := run(t, statusArgs[0], statusArgs[1:]...)
	if status.exitCode != 0 {
		t.Fatalf("peers service status after the reinstall: exit %d\nstdout: %s\nstderr: %s",
			status.exitCode, status.stdout, status.stderr)
	}
}

func argsForStatus(domain string) []string {
	if domain == "system" {
		return []string{"sudo", "-n", installedBinary, "service", "status"}
	}
	return []string{installedBinary, "service", "status"}
}
