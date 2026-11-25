package service

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// TestUninstallRejectsUnsupportedPlatform checks the platform check.
func TestUninstallRejectsUnsupportedPlatform(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "plan9"
	_, err := Uninstall(opts)
	if err == nil || !strings.Contains(err.Error(), `cannot uninstall the service on platform "plan9"`) {
		t.Fatalf("Uninstall error = %v, want the platform check", err)
	}
}

// TestUninstallWithoutSystemctlOrUnit checks that a host without systemd and
// without a unit file reports that nothing is installed.
func TestUninstallWithoutSystemctlOrUnit(t *testing.T) {
	opts := baseOptions(t)
	opts.LookPath = func(string) (string, error) { return "", errors.New("not found") }
	ran := false
	opts.Run = func(string, ...string) ([]byte, error) {
		ran = true
		return nil, nil
	}

	res, err := Uninstall(opts)
	if err != nil {
		t.Fatalf("Uninstall: %v", err)
	}
	if res.Removed {
		t.Error("Removed = true with no systemctl and no unit file")
	}
	if res.UnitPath != "/home/alice/.config/systemd/user/peers.service" {
		t.Errorf("UnitPath = %q, want the user unit path", res.UnitPath)
	}
	if ran {
		t.Error("Uninstall ran a command without systemctl")
	}
	if len(res.Commands) != 0 {
		t.Errorf("Result.Commands = %q, want none", res.Commands)
	}
}

// TestUninstallLinuxReportsInspectionFailure checks the failure of the unit
// file inspection.
func TestUninstallLinuxReportsInspectionFailure(t *testing.T) {
	opts := baseOptions(t)
	writeFileUnder(t, opts.Root, "/home/alice/.config", "not a directory")
	_, err := Uninstall(opts)
	if err == nil || !strings.Contains(err.Error(), "cannot inspect /home/alice/.config/systemd/user/peers.service") {
		t.Fatalf("Uninstall error = %v, want the inspection failure", err)
	}
}

// TestUninstallLinuxKeepsTheUnitWhenDisableFails checks that a failed stop
// leaves the unit file for a retry.
func TestUninstallLinuxKeepsTheUnitWhenDisableFails(t *testing.T) {
	opts := baseOptions(t)
	rootedUnit := writeInstalledFile(t, opts, "/home/alice/.config/systemd/user/peers.service")
	opts.Run = func(name string, args ...string) ([]byte, error) {
		if slices.Contains(args, "disable") {
			return []byte("Job for peers.service failed"), errors.New("exit status 1")
		}
		return nil, nil
	}

	res, err := Uninstall(opts)
	if err == nil || !strings.Contains(err.Error(), "cannot run systemctl --user disable --now peers.service") {
		t.Fatalf("Uninstall error = %v, want the disable failure", err)
	}
	if !strings.Contains(err.Error(), "Job for peers.service failed") {
		t.Errorf("Uninstall error = %v, want the command output", err)
	}
	if res.Removed {
		t.Error("Removed = true after a failed disable")
	}
	if got := mustReadFile(t, rootedUnit); got != "installed" {
		t.Errorf("unit file = %q, want it kept", got)
	}
}

// TestUninstallLinuxReportsAnUnreadableUnit checks that a unit path that is
// not a readable file fails before it is removed.
func TestUninstallLinuxReportsAnUnreadableUnit(t *testing.T) {
	opts := baseOptions(t)
	path, err := unitPath(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(opts.Root, path), 0o755); err != nil {
		t.Fatal(err)
	}

	res, err := Uninstall(opts)
	if err == nil || !strings.Contains(err.Error(), "cannot read "+path) {
		t.Fatalf("Uninstall error = %v, want the read failure", err)
	}
	if res.Removed {
		t.Error("Removed = true after a failed read")
	}
}

// TestUninstallLinuxKeepsTheUnitWhenRemoveFails checks that a unit file that
// cannot be deleted is reported and left in place.
func TestUninstallLinuxKeepsTheUnitWhenRemoveFails(t *testing.T) {
	skipUnlessPermissionDenied(t)
	opts := baseOptions(t)
	const unitPath = "/home/alice/.config/systemd/user/peers.service"
	rootedUnit := writeInstalledFile(t, opts, unitPath)
	dir := filepath.Dir(rootedUnit)
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	res, err := Uninstall(opts)
	if err == nil || !strings.Contains(err.Error(), "cannot remove "+unitPath) {
		t.Fatalf("Uninstall error = %v, want the removal failure", err)
	}
	if res.Removed {
		t.Error("Removed = true after a failed removal")
	}
	if got := mustReadFile(t, rootedUnit); got != "installed" {
		t.Errorf("unit file = %q, want it kept", got)
	}
}

// TestUninstallDarwinReportsInspectionFailure checks the failure of the plist
// inspection.
func TestUninstallDarwinReportsInspectionFailure(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.EUID = 501
	opts.UID = 501
	opts.Home = "/Users/alice"
	writeFileUnder(t, opts.Root, "/Users/alice/Library/LaunchAgents", "not a directory")

	_, err := Uninstall(opts)
	if err == nil || !strings.Contains(err.Error(), "cannot inspect /Users/alice/Library/LaunchAgents/io.github.wangzw.peers.plist") {
		t.Fatalf("Uninstall error = %v, want the inspection failure", err)
	}
}

// TestUninstallDarwinKeepsThePlistWhenRemoveFails checks that a plist that
// cannot be deleted is reported and left in place.
func TestUninstallDarwinKeepsThePlistWhenRemoveFails(t *testing.T) {
	skipUnlessPermissionDenied(t)
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.EUID = 501
	opts.UID = 501
	opts.Home = "/Users/alice"
	const plistPath = "/Users/alice/Library/LaunchAgents/io.github.wangzw.peers.plist"
	rootedPlist := writeInstalledFile(t, opts, plistPath)
	dir := filepath.Dir(rootedPlist)
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	opts.Run = func(name string, args ...string) ([]byte, error) {
		if len(args) > 0 && args[0] == "bootout" {
			return nil, exitError(t, 3)
		}
		return nil, nil
	}

	res, err := Uninstall(opts)
	if err == nil || !strings.Contains(err.Error(), "cannot remove "+plistPath) {
		t.Fatalf("Uninstall error = %v, want the removal failure", err)
	}
	if res.Removed {
		t.Error("Removed = true after a failed removal")
	}
	if got := mustReadFile(t, rootedPlist); got != "installed" {
		t.Errorf("plist = %q, want it kept", got)
	}
}
