package service

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func writeInstalledFile(t *testing.T, opts Options, path string) string {
	t.Helper()
	rootedPath := filepath.Join(opts.Root, path)
	if err := os.MkdirAll(filepath.Dir(rootedPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rootedPath, []byte("installed"), 0o644); err != nil {
		t.Fatal(err)
	}
	return rootedPath
}

// TestUninstallLinuxUser checks that a Linux user uninstall disables and removes
// the unit, reloads systemd and keeps the data.
func TestUninstallLinuxUser(t *testing.T) {
	opts := baseOptions(t)
	unitPath := "/home/alice/.config/systemd/user/peers.service"
	rootedUnit := writeInstalledFile(t, opts, unitPath)

	dataDir := filepath.Join(opts.Root, "home/alice/.peers")
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	kept := filepath.Join(dataDir, "config.yaml")
	if err := os.WriteFile(kept, []byte("broker: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var cmds []string
	opts.Run = recordCommands(&cmds)

	res, err := Uninstall(opts)
	if err != nil {
		t.Fatalf("Uninstall: %v", err)
	}
	wantCmds := []string{
		"systemctl --user disable --now peers.service",
		"systemctl --user daemon-reload",
	}
	if !slices.Equal(cmds, wantCmds) {
		t.Errorf("commands = %q, want %q", cmds, wantCmds)
	}
	if !slices.Equal(res.Commands, wantCmds) {
		t.Errorf("Result.Commands = %q, want %q", res.Commands, wantCmds)
	}
	if !res.Removed || res.UnitPath != unitPath {
		t.Errorf("Result = %+v, want Removed with UnitPath %q", res, unitPath)
	}
	if _, err := os.Stat(rootedUnit); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("unit file still exists: %v", err)
	}
	if _, err := os.Stat(kept); err != nil {
		t.Errorf("configuration was removed: %v", err)
	}
}

// TestUninstallLinuxSystemKeepsData checks that a Linux system uninstall removes
// the unit and keeps the configuration, identity and audit log.
func TestUninstallLinuxSystemKeepsData(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	rootedUnit := writeInstalledFile(t, opts, "/etc/systemd/system/peers.service")

	for _, path := range []string{
		"/etc/peers/config.yaml",
		"/var/lib/peers/identity.json",
		"/var/log/peers/audit.log",
	} {
		rootedPath := filepath.Join(opts.Root, path)
		if err := os.MkdirAll(filepath.Dir(rootedPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(rootedPath, []byte("kept"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	var cmds []string
	opts.Run = recordCommands(&cmds)

	res, err := Uninstall(opts)
	if err != nil {
		t.Fatalf("Uninstall: %v", err)
	}
	wantCmds := []string{
		"systemctl disable --now peers.service",
		"systemctl daemon-reload",
	}
	if !slices.Equal(cmds, wantCmds) {
		t.Errorf("commands = %q, want %q", cmds, wantCmds)
	}
	if !res.Removed {
		t.Error("Removed = false, want true")
	}
	if _, err := os.Stat(rootedUnit); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("unit file still exists: %v", err)
	}
	for _, path := range []string{
		"/etc/peers/config.yaml",
		"/var/lib/peers/identity.json",
		"/var/log/peers/audit.log",
	} {
		if _, err := os.Stat(filepath.Join(opts.Root, path)); err != nil {
			t.Errorf("%s was removed: %v", path, err)
		}
	}
}

// TestUninstallLinuxWithoutSystemctl checks that a Linux uninstall with an
// installed unit and no systemctl reports the systemd hint.
func TestUninstallLinuxWithoutSystemctl(t *testing.T) {
	opts := baseOptions(t)
	writeInstalledFile(t, opts, "/home/alice/.config/systemd/user/peers.service")
	opts.LookPath = func(string) (string, error) { return "", errors.New("not found") }

	_, err := Uninstall(opts)
	if err == nil || !strings.Contains(err.Error(), "cannot find systemctl (is systemd installed?)") {
		t.Fatalf("Uninstall error = %v, want the parenthesized systemctl hint", err)
	}
}

// TestUninstallRepeats checks that a second uninstall succeeds with Removed=false
// and runs no removal command.
func TestUninstallRepeats(t *testing.T) {
	opts := baseOptions(t)
	writeInstalledFile(t, opts, "/home/alice/.config/systemd/user/peers.service")
	opts.Run = func(string, ...string) ([]byte, error) { return nil, nil }

	if _, err := Uninstall(opts); err != nil {
		t.Fatalf("first Uninstall: %v", err)
	}

	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		return nil, errors.New("not found")
	}
	res, err := Uninstall(opts)
	if err != nil {
		t.Fatalf("second Uninstall: %v", err)
	}
	if res.Removed {
		t.Error("Removed = true on the second run, want false")
	}
	wantCmds := []string{
		"systemctl --user is-enabled peers.service",
		"systemctl --user is-active --quiet peers.service",
	}
	if !slices.Equal(cmds, wantCmds) {
		t.Errorf("commands = %q, want %q", cmds, wantCmds)
	}
	if len(res.Commands) != 0 {
		t.Errorf("Result.Commands = %q, want none when nothing is installed", res.Commands)
	}
}

// TestUninstallLinuxServiceStillLoaded checks that a Linux uninstall removes a
// unit systemd still has loaded although is-enabled fails.
func TestUninstallLinuxServiceStillLoaded(t *testing.T) {
	// Without a daemon-reload, is-enabled fails while is-active still reports
	// the loaded unit, and disable --now stops it.
	opts := baseOptions(t)
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		switch {
		case slices.Contains(args, "is-enabled"):
			return []byte("Unit file does not exist"), errors.New("exit status 1")
		case slices.Contains(args, "is-active"):
			return nil, nil
		}
		return nil, nil
	}

	res, err := Uninstall(opts)
	if err != nil {
		t.Fatalf("Uninstall: %v", err)
	}
	if !res.Removed {
		t.Error("Removed = false, want true when systemd still has the unit")
	}
	wantCmds := []string{
		"systemctl --user is-enabled peers.service",
		"systemctl --user is-active --quiet peers.service",
		"systemctl --user disable --now peers.service",
		"systemctl --user daemon-reload",
	}
	if !slices.Equal(cmds, wantCmds) {
		t.Errorf("commands = %q, want %q", cmds, wantCmds)
	}
}

// TestUninstallLinuxDaemonReloadFailureRestoresTheUnit checks that a failed
// daemon-reload restores the removed unit and reports the failure.
func TestUninstallLinuxDaemonReloadFailureRestoresTheUnit(t *testing.T) {
	opts := baseOptions(t)
	rootedUnit := writeInstalledFile(t, opts, "/home/alice/.config/systemd/user/peers.service")
	opts.Run = func(name string, args ...string) ([]byte, error) {
		if slices.Contains(args, "daemon-reload") {
			return []byte("reload failed"), errors.New("exit status 1")
		}
		return nil, nil
	}

	res, err := Uninstall(opts)
	if err == nil || !strings.Contains(err.Error(), "reload failed") {
		t.Fatalf("Uninstall error = %v, want the daemon-reload failure", err)
	}
	if res.Removed {
		t.Error("Removed = true after a failed daemon-reload")
	}
	if got := mustReadFile(t, rootedUnit); got != "installed" {
		t.Errorf("unit file = %q, want it restored", got)
	}
}

// TestUninstallLinuxDaemonReloadRestoreFailureReported checks that a failed
// daemon-reload and a failed restore report both failures.
func TestUninstallLinuxDaemonReloadRestoreFailureReported(t *testing.T) {
	opts := baseOptions(t)
	rootedUnit := writeInstalledFile(t, opts, "/home/alice/.config/systemd/user/peers.service")
	opts.Run = func(name string, args ...string) ([]byte, error) {
		if slices.Contains(args, "daemon-reload") {
			if err := os.Mkdir(rootedUnit, 0o755); err != nil {
				t.Fatalf("creating %s: %v", rootedUnit, err)
			}
			if err := os.WriteFile(filepath.Join(rootedUnit, "keep"), []byte("x"), 0o644); err != nil {
				t.Fatalf("writing marker: %v", err)
			}
			return []byte("reload failed"), errors.New("exit status 1")
		}
		return nil, nil
	}

	_, err := Uninstall(opts)
	if err == nil {
		t.Fatal("Uninstall succeeded after a failed daemon-reload and restore")
	}
	for _, want := range []string{"reload failed", "could not restore"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

// TestUninstallRequiresTheMatchingAccount checks that a system uninstall requires
// root and a user uninstall rejects root.
func TestUninstallRequiresTheMatchingAccount(t *testing.T) {
	t.Run("system requires root", func(t *testing.T) {
		opts := baseOptions(t)
		opts.System = true
		opts.EUID = 1000
		_, err := Uninstall(opts)
		const want = "service remove --system must run as root"
		if err == nil || err.Error() != want {
			t.Fatalf("Uninstall error = %v, want %q", err, want)
		}
	})

	t.Run("user rejects root", func(t *testing.T) {
		opts := baseOptions(t)
		opts.EUID = 0
		_, err := Uninstall(opts)
		const want = "service remove without --system must not run as root; the user unit belongs to one account"
		if err == nil || err.Error() != want {
			t.Fatalf("Uninstall error = %v, want %q", err, want)
		}
	})
}

// TestUninstallDarwinUser checks that a darwin user uninstall boots the job out
// and removes the plist.
func TestUninstallDarwinUser(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.EUID = 501
	opts.UID = 501
	opts.Home = "/Users/alice"
	plist := writeInstalledFile(t, opts, "/Users/alice/Library/LaunchAgents/io.github.wangzw.peers.plist")

	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		if len(args) > 0 && args[0] == "bootout" {
			return nil, exitError(t, 3)
		}
		return nil, nil
	}

	res, err := Uninstall(opts)
	if err != nil {
		t.Fatalf("Uninstall: %v", err)
	}
	wantCmds := []string{
		"launchctl print gui/501",
		"launchctl bootout gui/501/io.github.wangzw.peers",
	}
	if !slices.Equal(cmds, wantCmds) {
		t.Errorf("commands = %q, want %q", cmds, wantCmds)
	}
	if res.Domain != "gui/501" {
		t.Errorf("Domain = %q, want gui/501", res.Domain)
	}
	if !res.Removed {
		t.Error("Removed = false, want true")
	}
	if _, err := os.Stat(plist); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("plist still exists: %v", err)
	}
}

// TestUninstallDarwinWithoutGUIDomain checks that a darwin uninstall without a
// gui domain removes the plist without booting out.
func TestUninstallDarwinWithoutGUIDomain(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.EUID = 501
	opts.UID = 501
	opts.Home = "/Users/alice"
	plist := writeInstalledFile(t, opts, "/Users/alice/Library/LaunchAgents/io.github.wangzw.peers.plist")
	opts.Run = func(string, ...string) ([]byte, error) {
		return nil, exitError(t, 113)
	}

	res, err := Uninstall(opts)
	if err != nil {
		t.Fatalf("Uninstall: %v", err)
	}
	if res.Domain != "" {
		t.Errorf("Domain = %q, want empty without a gui domain", res.Domain)
	}
	if !res.Removed {
		t.Error("Removed = false, want the plist removed")
	}
	if _, err := os.Stat(plist); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("plist still exists: %v", err)
	}
}

// TestUninstallDarwinWithoutGUIDomainOrPlist checks that a darwin uninstall with
// no gui domain and no plist reports nothing removed without querying the label.
func TestUninstallDarwinWithoutGUIDomainOrPlist(t *testing.T) {
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

	res, err := Uninstall(opts)
	if err != nil {
		t.Fatalf("Uninstall: %v", err)
	}
	if res.Removed {
		t.Error("Removed = true, want false with no gui domain and no plist")
	}
	wantCmds := []string{"launchctl print gui/501"}
	if !slices.Equal(cmds, wantCmds) {
		t.Errorf("commands = %q, want %q; the label must not be queried without a domain", cmds, wantCmds)
	}
}

// TestUninstallDarwinSystem checks that a darwin system uninstall boots out the
// system job and removes the plist.
func TestUninstallDarwinSystem(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.System = true
	opts.EUID = 0
	opts.UID = 0
	opts.Home = "/var/root"
	plist := writeInstalledFile(t, opts, "/Library/LaunchDaemons/io.github.wangzw.peers.plist")

	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		if len(args) > 0 && args[0] == "bootout" {
			return nil, exitError(t, 3)
		}
		return nil, nil
	}

	res, err := Uninstall(opts)
	if err != nil {
		t.Fatalf("Uninstall: %v", err)
	}
	wantCmds := []string{"launchctl bootout system/io.github.wangzw.peers"}
	if !slices.Equal(cmds, wantCmds) {
		t.Errorf("commands = %q, want %q", cmds, wantCmds)
	}
	if res.Domain != "system" {
		t.Errorf("Domain = %q, want system", res.Domain)
	}
	if _, err := os.Stat(plist); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("plist still exists: %v", err)
	}
}

// TestUninstallDarwinBootoutFailureKeepsThePlistForARetry checks that a failed
// bootout keeps the plist and a later uninstall that sees exit 3 removes it.
func TestUninstallDarwinBootoutFailureKeepsThePlistForARetry(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.EUID = 501
	opts.UID = 501
	opts.Home = "/Users/alice"
	plist := writeInstalledFile(t, opts, "/Users/alice/Library/LaunchAgents/io.github.wangzw.peers.plist")

	bootoutCode := 5
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		if len(args) > 0 && args[0] == "bootout" {
			return []byte("Boot-out failed"), exitError(t, bootoutCode)
		}
		return nil, nil
	}

	if _, err := Uninstall(opts); err == nil || !strings.Contains(err.Error(), "Boot-out failed") {
		t.Fatalf("Uninstall error = %v, want the bootout failure", err)
	}
	if _, err := os.Stat(plist); err != nil {
		t.Fatalf("plist was removed after a failed bootout: %v", err)
	}

	bootoutCode = 3
	cmds = nil
	res, err := Uninstall(opts)
	if err != nil {
		t.Fatalf("second Uninstall: %v", err)
	}
	if !res.Removed {
		t.Error("Removed = false on the retry")
	}
	if _, err := os.Stat(plist); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("plist still exists after the retry: %v", err)
	}
	wantCmds := []string{
		"launchctl print gui/501",
		"launchctl bootout gui/501/io.github.wangzw.peers",
	}
	if !slices.Equal(cmds, wantCmds) {
		t.Errorf("retry commands = %q, want %q", cmds, wantCmds)
	}
}

// TestUninstallDarwinLoadedWithoutFile checks that a darwin uninstall removes the
// service when it is still loaded without a plist.
func TestUninstallDarwinLoadedWithoutFile(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.EUID = 501
	opts.UID = 501
	opts.Home = "/Users/alice"
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		if len(args) > 0 && args[0] == "bootout" {
			return nil, exitError(t, 3)
		}
		return nil, nil
	}

	res, err := Uninstall(opts)
	if err != nil {
		t.Fatalf("Uninstall: %v", err)
	}
	if !res.Removed {
		t.Error("Removed = false, want true when the service is still loaded")
	}
	wantCmds := []string{
		"launchctl print gui/501",
		"launchctl print gui/501/io.github.wangzw.peers",
		"launchctl bootout gui/501/io.github.wangzw.peers",
	}
	if !slices.Equal(cmds, wantCmds) {
		t.Errorf("commands = %q, want %q", cmds, wantCmds)
	}
}

// TestUninstallDarwinNotInstalled checks that a darwin uninstall with no plist
// and no loaded label reports nothing removed.
func TestUninstallDarwinNotInstalled(t *testing.T) {
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

	res, err := Uninstall(opts)
	if err != nil {
		t.Fatalf("Uninstall: %v", err)
	}
	if res.Removed {
		t.Error("Removed = true, want false")
	}
	wantCmds := []string{
		"launchctl print gui/501",
		"launchctl print gui/501/io.github.wangzw.peers",
	}
	if !slices.Equal(cmds, wantCmds) {
		t.Errorf("commands = %q, want %q", cmds, wantCmds)
	}
	if len(res.Commands) != 0 {
		t.Errorf("Result.Commands = %q, want none", res.Commands)
	}
}

// TestUninstallDarwinSystemBootoutFailureKeepsThePlist checks that a failed
// system bootout keeps the plist.
func TestUninstallDarwinSystemBootoutFailureKeepsThePlist(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.System = true
	opts.EUID = 0
	opts.UID = 0
	opts.Home = "/var/root"
	plist := writeInstalledFile(t, opts, "/Library/LaunchDaemons/io.github.wangzw.peers.plist")
	opts.Run = func(name string, args ...string) ([]byte, error) {
		if len(args) > 0 && args[0] == "bootout" {
			return []byte("Boot-out failed"), exitError(t, 5)
		}
		return nil, nil
	}

	if _, err := Uninstall(opts); err == nil {
		t.Fatal("Uninstall succeeded after a failed bootout")
	}
	if _, err := os.Stat(plist); err != nil {
		t.Errorf("plist was removed after a failed bootout: %v", err)
	}
}
