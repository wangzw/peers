//go:build e2e

package e2e

import (
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/wangzw/peers/test/e2e/syscontainer"
)

const systemAuditPath = "/var/log/peers/audit.log"

// TestREQBrokerConfig050_SystemServiceInstallPrintsSummary checks the summary a
// system installation prints and that the paths and commands it names work,
// including (kept) on a second installation.
func TestREQBrokerConfig050_SystemServiceInstallPrintsSummary(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	resetSystemServiceState(t)
	// The printed uninstall removes the binary, so reset only when it still
	// exists.
	t.Cleanup(func() {
		if _, err := os.Stat(releaseBinary); err == nil {
			resetSystemServiceState(t)
		}
	})

	res := syscontainer.RunAs(t, "root", nil, "service", "install", "--system")
	if res.ExitCode != 0 {
		t.Fatalf("service install --system: exit %d\nstdout: %s\nstderr: %s",
			res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
	}
	assertInstallSummary(t, res.Stdout, "system service", "boot", true, map[string]string{
		"binary:":    releaseBinary,
		"config:":    systemConfigPath + " (generated)",
		"run log:":   "journalctl -u peers.service",
		"audit log:": systemAuditPath,
		"start:":     "sudo peers service start",
		"stop:":      "sudo peers service stop",
		"restart:":   "sudo peers service restart",
		"uninstall:": "peers uninstall",
	})

	syscontainer.WaitFor(t, "the system Broker socket "+systemSocket, func() bool {
		_, err := os.Stat(systemSocket)
		return err == nil
	})
	for _, path := range []string{releaseBinary, systemConfigPath, systemAuditPath} {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("the summary prints %s, but it does not exist: %v", path, err)
		}
	}

	again := syscontainer.RunAs(t, "root", nil, "service", "install", "--system")
	if again.ExitCode != 0 {
		t.Fatalf("reinstalling service --system: exit %d\nstdout: %s\nstderr: %s",
			again.ExitCode, strings.TrimSpace(again.Stdout), strings.TrimSpace(again.Stderr))
	}
	assertInstallSummary(t, again.Stdout, "system service", "boot", true, map[string]string{
		"config:": systemConfigPath + " (kept)",
	})

	t.Run("the printed commands run as printed", func(t *testing.T) {
		for _, label := range []string{"start:", "stop:", "restart:", "uninstall:"} {
			args := printedCommandArgs(t, summaryValue(t, again.Stdout, label))
			// The summary prints "peers uninstall" for a person at a terminal;
			// the test passes --yes to skip the question.
			if label == "uninstall:" {
				args = append(args, "--yes")
			}
			res := syscontainer.RunAs(t, "root", nil, args...)
			if res.ExitCode != 0 {
				t.Fatalf("%s command from the summary: exit %d\nstdout: %s\nstderr: %s",
					strings.TrimSuffix(label, ":"), res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
			}
		}
	})
}

// TestREQBrokerConfig050_UserServiceInstallPrintsSummary checks the same summary
// for a user installation: no sudo prefix, starts at login, the user's
// configuration file and log commands.
func TestREQBrokerConfig050_UserServiceInstallPrintsSummary(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	peerUID := syscontainer.AccountUID(t, "peer")
	uid := strconv.FormatUint(uint64(peerUID), 10)
	userEnv := []string{
		"XDG_RUNTIME_DIR=/run/user/" + uid,
		"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/" + uid + "/bus",
	}
	configPath := "/home/peer/.peers/config.yaml"
	auditPath := "/home/peer/.peers/audit.log"
	socket := "/tmp/peers-" + uid + "/broker.sock"

	mustRun(t, "root", nil, "loginctl", "enable-linger", "peer")
	mustRun(t, "root", nil, "systemctl", "start", "user@"+uid+".service")
	waitUserManager(t, peerUID)
	t.Cleanup(func() {
		_ = runWithEnv(t, "peer", userEnv, releaseBinary, "service", "remove")
	})

	res := runWithEnv(t, "peer", userEnv, releaseBinary, "service", "install")
	if res.ExitCode != 0 {
		t.Fatalf("service install as peer: exit %d\nstdout: %s\nstderr: %s",
			res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
	}
	assertInstallSummary(t, res.Stdout, "user service", "login", true, map[string]string{
		"binary:":    releaseBinary,
		"config:":    configPath + " (generated)",
		"run log:":   "journalctl --user -u peers.service",
		"audit log:": auditPath,
		"start:":     "peers service start",
		"stop:":      "peers service stop",
		"restart:":   "peers service restart",
		"uninstall:": "peers uninstall",
	})

	syscontainer.WaitFor(t, "the peer Broker socket "+socket, func() bool {
		_, err := os.Stat(socket)
		return err == nil
	})
	for _, path := range []string{releaseBinary, configPath, auditPath} {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("the summary prints %s, but it does not exist: %v", path, err)
		}
	}

	again := runWithEnv(t, "peer", userEnv, releaseBinary, "service", "install")
	if again.ExitCode != 0 {
		t.Fatalf("reinstalling the user service: exit %d\nstdout: %s\nstderr: %s",
			again.ExitCode, strings.TrimSpace(again.Stdout), strings.TrimSpace(again.Stderr))
	}
	assertInstallSummary(t, again.Stdout, "user service", "login", true, map[string]string{
		"config:": configPath + " (kept)",
	})

	t.Run("the printed commands run as printed", func(t *testing.T) {
		for _, label := range []string{"start:", "stop:", "restart:", "uninstall:"} {
			args := printedCommandArgs(t, summaryValue(t, again.Stdout, label))
			// The summary prints "peers uninstall" for a person at a terminal;
			// the test passes --yes to skip the question.
			if label == "uninstall:" {
				args = append(args, "--yes")
			}
			res := runWithEnv(t, "peer", userEnv, "peers", args...)
			if res.ExitCode != 0 {
				t.Fatalf("%s command from the summary: exit %d\nstdout: %s\nstderr: %s",
					strings.TrimSuffix(label, ":"), res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
			}
		}
	})
}

// TestREQBrokerConfig064_ReinstallReportsDegradedAuditLog checks that an unusable
// audit log leaves the running system service installed, names the fault and the
// repair command, exits 3, prints the summary, and reports the Broker as degraded.
func TestREQBrokerConfig064_ReinstallReportsDegradedAuditLog(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	resetSystemServiceState(t)
	t.Cleanup(func() {
		_ = os.Remove(systemAuditPath)
		resetSystemServiceState(t)
	})

	installSystemService(t)
	syscontainer.WaitFor(t, "the system Broker socket "+systemSocket, func() bool {
		_, err := os.Stat(systemSocket)
		return err == nil
	})

	// A directory at the audit log path cannot be opened for appending even by
	// root, so the Broker's audit step fails.
	if err := os.Remove(systemAuditPath); err != nil {
		t.Fatalf("removing %s: %v", systemAuditPath, err)
	}
	if err := os.Mkdir(systemAuditPath, 0o755); err != nil {
		t.Fatalf("replacing %s with a directory: %v", systemAuditPath, err)
	}

	res := syscontainer.RunAs(t, "root", nil, "service", "install", "--system")
	if res.ExitCode != 3 {
		t.Fatalf("reinstalling with an unusable audit log: exit %d, want 3\nstdout: %s\nstderr: %s",
			res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
	}

	assertInstallSummary(t, res.Stdout, "system service", "boot", true, map[string]string{
		"binary:":    releaseBinary,
		"config:":    systemConfigPath + " (kept)",
		"audit log:": systemAuditPath,
	})
	if _, err := os.Stat(systemUnitPath); err != nil {
		t.Errorf("the unit file %s is missing after the degraded reinstall: %v", systemUnitPath, err)
	}
	if _, err := os.Stat(releaseBinary); err != nil {
		t.Errorf("the binary %s is missing after the degraded reinstall: %v", releaseBinary, err)
	}

	degradedLine := summaryValue(t, res.Stderr, "Broker:")
	if !strings.Contains(degradedLine, "degraded (diagnostics only)") {
		t.Errorf("degraded line = %q, want the diagnostics-only level", degradedLine)
	}
	if !strings.Contains(degradedLine, "audit") {
		t.Errorf("degraded line = %q, want the audit log reason", degradedLine)
	}
	fixLine := summaryValue(t, res.Stderr, "Fix:")
	if !strings.Contains(fixLine, systemAuditPath) {
		t.Errorf("fix line = %q, want the audit log path %s", fixLine, systemAuditPath)
	}
	if !strings.Contains(fixLine, "peers service restart") {
		t.Errorf("fix line = %q, want the service restart command", fixLine)
	}

	active := runCommandAs(t, "root", "systemctl", "is-active", "peers.service")
	if state := strings.TrimSpace(active.Stdout); state != "active" {
		t.Errorf("systemctl is-active peers.service = %q after the degraded reinstall, want %q\nstderr: %s",
			state, "active", strings.TrimSpace(active.Stderr))
	}
	status := syscontainer.RunAs(t, "root", nil, "service", "status")
	if status.ExitCode == 0 {
		t.Errorf("peers service status exited 0 for a diagnostics-only Broker\nstdout: %s\nstderr: %s",
			strings.TrimSpace(status.Stdout), strings.TrimSpace(status.Stderr))
	}
	if combined := status.Stdout + status.Stderr; !strings.Contains(combined, "degraded") {
		t.Errorf("peers service status output = %q, want it to report the degraded run", combined)
	}
}

// assertInstallSummary checks the install summary and every value the caller names;
// requireFirst makes the summary the first line, which a direct service install
// prints but the install script does not.
func assertInstallSummary(t *testing.T, stdout, level, start string, requireFirst bool, wants map[string]string) {
	t.Helper()

	suffix := " installed (" + level + ", starts at " + start + ")"
	lines := strings.Split(strings.TrimRight(stdout, "\n"), "\n")
	matches := 0
	var first string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "peers ") && strings.HasSuffix(trimmed, suffix) {
			matches++
			if first == "" {
				first = trimmed
			}
		}
	}
	if matches > 1 {
		t.Errorf("install output has more than one summary line:\n%s", stdout)
	}
	if requireFirst {
		if len(lines) == 0 || strings.TrimSpace(lines[0]) == "" {
			t.Fatalf("the install printed no summary:\n%s", stdout)
		}
		first = strings.TrimSpace(lines[0])
		if !strings.HasPrefix(first, "peers ") || !strings.HasSuffix(first, suffix) {
			t.Errorf("summary first line = %q, want %q", first, "peers <version>"+suffix)
		}
	} else if first == "" {
		t.Fatalf("the install printed no line ending in %q:\n%s", suffix, stdout)
	}
	if version := strings.TrimSuffix(strings.TrimPrefix(first, "peers "), suffix); strings.TrimSpace(version) == "" || strings.Contains(version, " ") {
		t.Errorf("summary line = %q, want one version word", first)
	}

	for label, want := range wants {
		if got := summaryValue(t, stdout, label); got != want {
			t.Errorf("summary %s = %q, want %q", label, got, want)
		}
	}
}

// printedCommandArgs splits a summary command into the arguments after its binary,
// dropping the sudo prefix because callers run the command as root.
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
