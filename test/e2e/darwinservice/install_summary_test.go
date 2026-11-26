//go:build e2e_darwin

package darwinservice

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const systemAuditPath = "/var/log/peers/audit.log"

func userConfigPath() string {
	return filepath.Join(os.Getenv("HOME"), ".peers", "config.yaml")
}

func userAuditPath() string {
	return filepath.Join(os.Getenv("HOME"), ".peers", "audit.log")
}

func userRunLogPath() string {
	return filepath.Join(os.Getenv("HOME"), "Library", "Logs", "peers", "broker.log")
}

// TestREQBrokerConfig050_UserServiceInstallPrintsSummary checks the summary a
// user installation prints and that the paths it prints exist.
func TestREQBrokerConfig050_UserServiceInstallPrintsSummary(t *testing.T) {
	resetUserService(t)
	t.Cleanup(func() { resetUserService(t) })

	configPath := preserveUserConfig(t)
	if err := os.Remove(configPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("removing %s: %v", configPath, err)
	}

	res := run(t, installedBinary, "service", "install")
	if res.exitCode != 0 {
		t.Fatalf("service install: exit %d\nstdout: %s\nstderr: %s", res.exitCode, res.stdout, res.stderr)
	}
	assertInstallSummary(t, res.stdout, "user service", "login", map[string]string{
		"binary:":    installedBinary,
		"config:":    configPath + " (generated)",
		"run log:":   userRunLogPath(),
		"audit log:": userAuditPath(),
		"start:":     "peers service start",
		"stop:":      "peers service stop",
		"restart:":   "peers service restart",
		"uninstall:": "peers uninstall",
	})

	waitForFile(t, userAuditPath(), serviceStartBound)
	waitForFile(t, userRunLogPath(), serviceStartBound)
	for _, path := range []string{installedBinary, configPath, userAuditPath()} {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("the summary prints %s, but it does not exist: %v", path, err)
		}
	}

	again := run(t, installedBinary, "service", "install")
	if again.exitCode != 0 {
		t.Fatalf("reinstalling the user service: exit %d\nstdout: %s\nstderr: %s", again.exitCode, again.stdout, again.stderr)
	}
	assertInstallSummary(t, again.stdout, "user service", "login", map[string]string{
		"config:": configPath + " (kept)",
	})

	t.Run("the printed commands exist and run", func(t *testing.T) {
		for _, label := range []string{"start:", "stop:", "restart:", "uninstall:"} {
			args := printedCommandArgs(t, summaryValue(t, again.stdout, label))
			// The test has no terminal, so uninstall needs --yes; the printed
			// summary keeps its form without it.
			note := ""
			if label == "uninstall:" {
				args = append(args, "--yes")
				note = " (with --yes, since the test has no terminal)"
			}
			res := run(t, installedBinary, args...)
			if res.exitCode != 0 {
				t.Fatalf("%s command from the summary%s: exit %d\nstdout: %s\nstderr: %s",
					strings.TrimSuffix(label, ":"), note, res.exitCode, res.stdout, res.stderr)
			}
		}
	})
}

// TestREQBrokerConfig050_SystemServiceInstallPrintsSummary checks the same
// summary for a system installation: the sudo prefix, starts at boot, and
// the system paths.
func TestREQBrokerConfig050_SystemServiceInstallPrintsSummary(t *testing.T) {
	resetSystemService(t)
	t.Cleanup(func() { resetSystemService(t) })
	preserveSystemFile(t, systemConfigPath)

	if err := sudo("rm", "-f", systemConfigPath); err != nil {
		t.Fatalf("removing %s: %v", systemConfigPath, err)
	}

	res := runSudo(t, installedBinary, "service", "install", "--system")
	if res.exitCode != 0 {
		t.Fatalf("service install --system: exit %d\nstdout: %s\nstderr: %s", res.exitCode, res.stdout, res.stderr)
	}
	assertInstallSummary(t, res.stdout, "system service", "boot", map[string]string{
		"binary:":    installedBinary,
		"config:":    systemConfigPath + " (generated)",
		"run log:":   "/var/log/peers/broker.log",
		"audit log:": systemAuditPath,
		"start:":     "sudo peers service start",
		"stop:":      "sudo peers service stop",
		"restart:":   "sudo peers service restart",
		"uninstall:": "peers uninstall",
	})

	waitForRootFile(t, systemAuditPath, serviceStartBound)
	waitForRootFile(t, "/var/log/peers/broker.log", serviceStartBound)
	for _, path := range []string{installedBinary, systemConfigPath, systemAuditPath} {
		if err := sudo("test", "-e", path); err != nil {
			t.Errorf("the summary prints %s, but it does not exist: %v", path, err)
		}
	}

	again := runSudo(t, installedBinary, "service", "install", "--system")
	if again.exitCode != 0 {
		t.Fatalf("reinstalling the system service: exit %d\nstdout: %s\nstderr: %s", again.exitCode, again.stdout, again.stderr)
	}
	assertInstallSummary(t, again.stdout, "system service", "boot", map[string]string{
		"config:": systemConfigPath + " (kept)",
	})

	t.Run("the printed commands exist and run", func(t *testing.T) {
		for _, label := range []string{"start:", "stop:", "restart:", "uninstall:"} {
			args := printedCommandArgs(t, summaryValue(t, again.stdout, label))
			// The test has no terminal, so uninstall needs --yes; the printed
			// summary keeps its form without it.
			note := ""
			if label == "uninstall:" {
				args = append(args, "--yes")
				note = " (with --yes, since the test has no terminal)"
			}
			res := runSudo(t, installedBinary, args...)
			if res.exitCode != 0 {
				t.Fatalf("%s command from the summary%s: exit %d\nstdout: %s\nstderr: %s",
					strings.TrimSuffix(label, ":"), note, res.exitCode, res.stdout, res.stderr)
			}
		}
	})
}

// TestREQBrokerConfig064_ReinstallReportsDegradedAuditLog checks that
// reinstalling after making the audit log unusable keeps the service running
// degraded at both levels.
func TestREQBrokerConfig064_ReinstallReportsDegradedAuditLog(t *testing.T) {
	t.Run("user installation", func(t *testing.T) {
		resetUserService(t)
		preserveUserAudit(t)
		t.Cleanup(func() { resetUserService(t) })

		writeUserConfig(t, "127.0.0.1:17449")
		install := run(t, installedBinary, "service", "install")
		if install.exitCode != 0 {
			t.Fatalf("service install: exit %d\nstdout: %s\nstderr: %s", install.exitCode, install.stdout, install.stderr)
		}
		domain := userDomain(t)
		beforePID := waitForPID(t, domain, serviceStartBound)

		breakAuditPath(t, userAuditPath(), false)

		res := run(t, installedBinary, "service", "install")
		if res.exitCode != 3 {
			t.Fatalf("reinstalling with an unusable audit log: exit %d, want 3\nstdout: %s\nstderr: %s",
				res.exitCode, res.stdout, res.stderr)
		}
		assertInstallSummary(t, res.stdout, "user service", "login", map[string]string{
			"binary:":    installedBinary,
			"config:":    userConfigPath() + " (kept)",
			"audit log:": userAuditPath(),
		})
		assertDegradedOutput(t, res.stderr, userAuditPath())

		afterPID := waitForPID(t, domain, serviceStartBound)
		if afterPID == 0 || !processAlive(afterPID) {
			t.Errorf("the user service did not survive the degraded reinstall; pid before %d, after %d", beforePID, afterPID)
		}

		status := run(t, installedBinary, "service", "status")
		if status.exitCode == 0 {
			t.Errorf("peers service status exited 0 for a diagnostics-only Broker\nstdout: %s\nstderr: %s", status.stdout, status.stderr)
		}
		if combined := status.stdout + status.stderr; !strings.Contains(combined, "degraded") {
			t.Errorf("peers service status output = %q, want it to report the degraded run", combined)
		}
	})

	t.Run("system installation", func(t *testing.T) {
		resetSystemService(t)
		preserveSystemFile(t, systemAuditPath)
		t.Cleanup(func() { resetSystemService(t) })

		install := runSudo(t, installedBinary, "service", "install", "--system")
		if install.exitCode != 0 {
			t.Fatalf("service install --system: exit %d\nstdout: %s\nstderr: %s", install.exitCode, install.stdout, install.stderr)
		}
		beforePID := waitForPID(t, "system", serviceStartBound)

		breakAuditPath(t, systemAuditPath, true)

		res := runSudo(t, installedBinary, "service", "install", "--system")
		if res.exitCode != 3 {
			t.Fatalf("reinstalling the system service with an unusable audit log: exit %d, want 3\nstdout: %s\nstderr: %s",
				res.exitCode, res.stdout, res.stderr)
		}
		assertInstallSummary(t, res.stdout, "system service", "boot", map[string]string{
			"binary:":    installedBinary,
			"audit log:": systemAuditPath,
		})
		assertDegradedOutput(t, res.stderr, systemAuditPath)

		afterPID := waitForPID(t, "system", serviceStartBound)
		if afterPID == 0 || !processAlive(afterPID) {
			t.Errorf("the system service did not survive the degraded reinstall; pid before %d, after %d", beforePID, afterPID)
		}

		status := runSudo(t, installedBinary, "service", "status")
		if status.exitCode == 0 {
			t.Errorf("peers service status as root exited 0 for a diagnostics-only Broker\nstdout: %s\nstderr: %s", status.stdout, status.stderr)
		}
		if combined := status.stdout + status.stderr; !strings.Contains(combined, "degraded") {
			t.Errorf("peers service status output = %q, want it to report the degraded run", combined)
		}
	})
}

func assertDegradedOutput(t *testing.T, stderr, auditPath string) {
	t.Helper()

	degradedLine := summaryValue(t, stderr, "Broker:")
	if !strings.Contains(degradedLine, "degraded (diagnostics only)") {
		t.Errorf("degraded line = %q, want the diagnostics-only level", degradedLine)
	}
	if !strings.Contains(degradedLine, "audit") {
		t.Errorf("degraded line = %q, want the audit log reason", degradedLine)
	}
	fixLine := summaryValue(t, stderr, "Fix:")
	if !strings.Contains(fixLine, auditPath) {
		t.Errorf("fix line = %q, want the audit log path %s", fixLine, auditPath)
	}
	if !strings.Contains(fixLine, "peers service restart") {
		t.Errorf("fix line = %q, want the service restart command", fixLine)
	}
}

// breakAuditPath replaces path with a directory, which cannot be opened for
// appending even by root.
func breakAuditPath(t *testing.T, path string, root bool) {
	t.Helper()

	if root {
		if err := sudo("rm", "-f", path); err != nil {
			t.Fatalf("removing %s: %v", path, err)
		}
		if err := sudo("mkdir", "-m", "0755", path); err != nil {
			t.Fatalf("replacing %s with a directory: %v", path, err)
		}
		return
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("removing %s: %v", path, err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatalf("replacing %s with a directory: %v", path, err)
	}
}

func preserveUserAudit(t *testing.T) {
	t.Helper()

	path := userAuditPath()
	data, err := os.ReadFile(path)
	existed := err == nil
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reading %s: %v", path, err)
	}
	t.Cleanup(func() {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Errorf("removing %s: %v", path, err)
		}
		if existed {
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Errorf("restoring %s: %v", path, err)
			}
		}
	})
}

func preserveSystemFile(t *testing.T, path string) {
	t.Helper()

	backup := path + ".e2e-backup"
	if err := sudo("cp", "-p", path, backup); err != nil {
		t.Cleanup(func() { _ = sudo("rm", "-rf", path) })
		return
	}
	t.Cleanup(func() {
		_ = sudo("rm", "-rf", path)
		if err := sudo("mv", backup, path); err != nil {
			t.Errorf("restoring %s: %v", path, err)
		}
	})
}

func assertInstallSummary(t *testing.T, stdout, level, start string, wants map[string]string) {
	t.Helper()

	lines := strings.Split(strings.TrimRight(stdout, "\n"), "\n")
	if len(lines) == 0 || strings.TrimSpace(lines[0]) == "" {
		t.Fatalf("the install printed no summary:\n%s", stdout)
	}
	first := strings.TrimSpace(lines[0])
	suffix := " installed (" + level + ", starts at " + start + ")"
	if !strings.HasPrefix(first, "peers ") || !strings.HasSuffix(first, suffix) {
		t.Errorf("summary first line = %q, want %q", first, "peers <version>"+suffix)
	}
	if version := strings.TrimSuffix(strings.TrimPrefix(first, "peers "), suffix); strings.TrimSpace(version) == "" || strings.Contains(version, " ") {
		t.Errorf("summary first line = %q, want one version word", first)
	}

	for label, want := range wants {
		if got := summaryValue(t, stdout, label); got != want {
			t.Errorf("summary %s = %q, want %q", label, got, want)
		}
	}
}

func printedCommandArgs(t *testing.T, command string) []string {
	t.Helper()

	words := strings.Fields(command)
	if len(words) > 0 && words[0] == "sudo" {
		words = words[1:]
	}
	if len(words) == 0 || words[0] != "peers" {
		t.Fatalf("summary command = %q, want it to name peers", command)
	}
	return words[1:]
}

func summaryValue(t *testing.T, output, label string) string {
	t.Helper()

	for _, line := range strings.Split(output, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, label) {
			return strings.TrimSpace(strings.TrimPrefix(trimmed, label))
		}
	}
	t.Fatalf("output has no %q line:\n%s", label, output)
	return ""
}

func waitForFile(t *testing.T, path string, bound time.Duration) {
	t.Helper()

	waitFor(t, bound, path, func() (bool, string) {
		_, err := os.Stat(path)
		return err == nil, errDetail(err)
	})
}

func waitForRootFile(t *testing.T, path string, bound time.Duration) {
	t.Helper()

	waitFor(t, bound, path, func() (bool, string) {
		err := sudo("test", "-e", path)
		return err == nil, errDetail(err)
	})
}

func errDetail(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func runSudo(t *testing.T, name string, args ...string) result {
	t.Helper()

	cmd := exec.Command("sudo", append([]string{"-n", name}, args...)...)
	out, err := cmd.Output()
	res := result{stdout: string(out)}
	if err == nil {
		return res
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("running sudo %s %v: %v", name, args, err)
	}
	res.exitCode = exitErr.ExitCode()
	res.stderr = string(exitErr.Stderr)
	return res
}
