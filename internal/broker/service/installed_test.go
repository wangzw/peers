package service

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// TestInstalledLinuxUserFileExists checks that an existing user unit file reports
// installed without querying the service manager.
func TestInstalledLinuxUserFileExists(t *testing.T) {
	opts := baseOptions(t)
	writeInstalledFile(t, opts, "/home/alice/.config/systemd/user/peers.service")
	opts.Run = func(string, ...string) ([]byte, error) {
		t.Error("Installed queried the service manager although the unit file exists")
		return nil, nil
	}

	installed, err := Installed(opts)
	if err != nil {
		t.Fatalf("Installed: %v", err)
	}
	if !installed {
		t.Error("Installed = false, want true with the unit file present")
	}
}

// TestInstalledLinuxSystemFileExists checks that an existing system unit file
// reports installed.
func TestInstalledLinuxSystemFileExists(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	writeInstalledFile(t, opts, "/etc/systemd/system/peers.service")

	installed, err := Installed(opts)
	if err != nil {
		t.Fatalf("Installed: %v", err)
	}
	if !installed {
		t.Error("Installed = false, want true with the unit file present")
	}
}

// TestInstalledLinuxUserStillEnabled checks that systemd still reporting the user
// unit enabled makes it installed.
func TestInstalledLinuxUserStillEnabled(t *testing.T) {
	opts := baseOptions(t)
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		return nil, nil
	}

	installed, err := Installed(opts)
	if err != nil {
		t.Fatalf("Installed: %v", err)
	}
	if !installed {
		t.Error("Installed = false, want true when systemd still has the unit")
	}
	wantCmds := []string{"systemctl --user is-enabled peers.service"}
	if !slices.Equal(cmds, wantCmds) {
		t.Errorf("commands = %q, want %q", cmds, wantCmds)
	}
}

// TestInstalledLinuxSystemStillActive checks that a system unit still active
// although not enabled counts as installed.
func TestInstalledLinuxSystemStillActive(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		if slices.Contains(args, "is-enabled") {
			return nil, exitError(t, 1)
		}
		return nil, nil
	}

	installed, err := Installed(opts)
	if err != nil {
		t.Fatalf("Installed: %v", err)
	}
	if !installed {
		t.Error("Installed = false, want true when the unit is still active")
	}
	wantCmds := []string{
		"systemctl is-enabled peers.service",
		"systemctl is-active --quiet peers.service",
	}
	if !slices.Equal(cmds, wantCmds) {
		t.Errorf("commands = %q, want %q", cmds, wantCmds)
	}
}

// TestInstalledLinuxNothingLeft checks that neither the file nor systemd
// reporting the unit makes it not installed.
func TestInstalledLinuxNothingLeft(t *testing.T) {
	opts := baseOptions(t)
	opts.Run = func(string, ...string) ([]byte, error) { return nil, exitError(t, 1) }

	installed, err := Installed(opts)
	if err != nil {
		t.Fatalf("Installed: %v", err)
	}
	if installed {
		t.Error("Installed = true, want false when neither the file nor systemd reports the unit")
	}
}

// TestInstalledDarwinFileExists checks that an existing user plist reports
// installed.
func TestInstalledDarwinFileExists(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.EUID = 501
	opts.UID = 501
	opts.Home = "/Users/alice"
	writeInstalledFile(t, opts, "/Users/alice/Library/LaunchAgents/io.github.wangzw.peers.plist")

	installed, err := Installed(opts)
	if err != nil {
		t.Fatalf("Installed: %v", err)
	}
	if !installed {
		t.Error("Installed = false, want true with the plist present")
	}
}

// TestInstalledDarwinStillLoaded checks that a job launchd still has loaded in
// the gui domain counts as installed.
func TestInstalledDarwinStillLoaded(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.EUID = 501
	opts.UID = 501
	opts.Home = "/Users/alice"
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		if len(args) > 0 && args[0] == "print" {
			return nil, nil
		}
		return nil, exitError(t, 113)
	}

	installed, err := Installed(opts)
	if err != nil {
		t.Fatalf("Installed: %v", err)
	}
	if !installed {
		t.Error("Installed = false, want true when launchd still has the job loaded")
	}
	wantCmds := []string{"launchctl print gui/501/io.github.wangzw.peers"}
	if !slices.Equal(cmds, wantCmds) {
		t.Errorf("commands = %q, want %q", cmds, wantCmds)
	}
}

// TestInstalledDarwinNothingLeft checks that neither the plist nor a loaded job
// makes it not installed.
func TestInstalledDarwinNothingLeft(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.EUID = 501
	opts.UID = 501
	opts.Home = "/Users/alice"
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		if len(args) == 2 && args[1] == "gui/501" {
			return nil, nil
		}
		return nil, exitError(t, 113)
	}

	installed, err := Installed(opts)
	if err != nil {
		t.Fatalf("Installed: %v", err)
	}
	if installed {
		t.Error("Installed = true, want false when neither the plist nor launchd reports the job")
	}
	wantCmds := []string{"launchctl print gui/501/io.github.wangzw.peers"}
	if !slices.Equal(cmds, wantCmds) {
		t.Errorf("commands = %q, want %q", cmds, wantCmds)
	}
}

// TestInstalledDarwinWithoutGUIDomain checks that a user installation in a
// missing gui domain reports not installed.
func TestInstalledDarwinWithoutGUIDomain(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.EUID = 501
	opts.UID = 501
	opts.Home = "/Users/alice"
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		return nil, exitError(t, 113)
	}

	installed, err := Installed(opts)
	if err != nil {
		t.Fatalf("Installed: %v", err)
	}
	if installed {
		t.Error("Installed = true, want false without a gui domain")
	}
	wantCmds := []string{"launchctl print gui/501/io.github.wangzw.peers"}
	if !slices.Equal(cmds, wantCmds) {
		t.Errorf("commands = %q, want %q", cmds, wantCmds)
	}
}

// TestInstalledDarwinSystemStillLoaded checks that a loaded system daemon job
// counts as installed.
func TestInstalledDarwinSystemStillLoaded(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.System = true
	opts.EUID = 0
	opts.UID = 0
	opts.Home = "/var/root"
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		return nil, nil
	}

	installed, err := Installed(opts)
	if err != nil {
		t.Fatalf("Installed: %v", err)
	}
	if !installed {
		t.Error("Installed = false, want true when launchd still has the daemon loaded")
	}
	wantCmds := []string{"launchctl print system/io.github.wangzw.peers"}
	if !slices.Equal(cmds, wantCmds) {
		t.Errorf("commands = %q, want %q", cmds, wantCmds)
	}
}

// TestInstalledUnreadableState checks that a unit path that cannot be inspected
// is an error rather than not installed.
func TestInstalledUnreadableState(t *testing.T) {
	opts := baseOptions(t)
	blocker := filepath.Join(opts.Root, "home/alice/.config")
	if err := os.MkdirAll(filepath.Dir(blocker), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blocker, []byte("not a directory"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := Installed(opts)
	if err == nil {
		t.Fatal("Installed succeeded although the unit path could not be inspected")
	}
	if !strings.Contains(err.Error(), "inspect") {
		t.Errorf("error %q does not name the failed inspection", err)
	}
}

// TestInstalledUnsupportedPlatform checks that an unsupported platform returns
// the expected error.
func TestInstalledUnsupportedPlatform(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "windows"

	installed, err := Installed(opts)
	const want = `cannot check for an installed service on platform "windows"`
	if err == nil || err.Error() != want {
		t.Fatalf("Installed error = %v, want %q", err, want)
	}
	if installed {
		t.Error("Installed = true on an unsupported platform")
	}
}

// TestServiceFileExistsReportsInspectionFailure checks that an inspection
// failure is reported as an error with the file absent.
func TestServiceFileExistsReportsInspectionFailure(t *testing.T) {
	opts := baseOptions(t)
	blocker := filepath.Join(opts.Root, "home/alice/.config")
	if err := os.MkdirAll(filepath.Dir(blocker), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blocker, []byte("not a directory"), 0o644); err != nil {
		t.Fatal(err)
	}

	exists, err := serviceFileExists(opts, "/home/alice/.config/systemd/user/peers.service")
	if err == nil {
		t.Fatal("serviceFileExists succeeded although the path could not be inspected")
	}
	if exists {
		t.Error("serviceFileExists = true with a failed inspection")
	}
}

// TestServiceFileExistsMissing checks that a missing file reports not present
// without an error.
func TestServiceFileExistsMissing(t *testing.T) {
	opts := baseOptions(t)

	exists, err := serviceFileExists(opts, "/home/alice/.config/systemd/user/peers.service")
	if err != nil {
		t.Fatalf("serviceFileExists: %v", err)
	}
	if exists {
		t.Error("serviceFileExists = true for a missing file")
	}
}

// TestServiceFileExistsPresent checks that an existing file reports present.
func TestServiceFileExistsPresent(t *testing.T) {
	opts := baseOptions(t)
	writeInstalledFile(t, opts, "/home/alice/.config/systemd/user/peers.service")

	exists, err := serviceFileExists(opts, "/home/alice/.config/systemd/user/peers.service")
	if err != nil {
		t.Fatalf("serviceFileExists: %v", err)
	}
	if !exists {
		t.Error("serviceFileExists = false for a present file")
	}
}
