package main

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/broker/service"
)

type uninstallSeams struct {
	options         func(bool) (service.Options, error)
	installed       func(service.Options) (bool, error)
	uninstallSvc    func(service.Options) (service.Result, error)
	serviceBinary   func(service.Options) (string, bool, error)
	removeRuntime   func(service.Options) ([]string, error)
	purgeData       func(service.Options) ([]string, error)
	otherAccounts   func(service.Options) ([]string, error)
	removeMCP       func(string, string) (string, error)
	mcpLookup       func(string) mcpRegistration
	executable      func() (string, error)
	evalSymlinks    func(string) (string, error)
	getenv          func(string) string
	fileOwner       func(string) (uint32, error)
	removeFile      func(string) error
	systemInstalled func(service.Options) bool
	sudo            func(context.Context, string, []string, io.Reader, io.Writer, io.Writer) error
}

func withUninstallSeams(t *testing.T, s uninstallSeams) {
	t.Helper()
	previous := uninstallSeams{
		options:         uninstallOptionsFn,
		installed:       uninstallInstalledFn,
		uninstallSvc:    uninstallServiceFn,
		serviceBinary:   uninstallServiceBinaryFn,
		removeRuntime:   uninstallRemoveRuntimeDirFn,
		purgeData:       uninstallPurgeDataFn,
		otherAccounts:   uninstallOtherAccountsFn,
		removeMCP:       uninstallMCPFn,
		mcpLookup:       uninstallMCPLookupFn,
		executable:      uninstallExecutableFn,
		evalSymlinks:    uninstallEvalSymlinksFn,
		getenv:          uninstallGetenvFn,
		fileOwner:       uninstallFileOwnerFn,
		removeFile:      uninstallRemoveFileFn,
		systemInstalled: uninstallSystemInstalledCheck,
		sudo:            uninstallSudoFn,
	}
	t.Cleanup(func() {
		uninstallOptionsFn = previous.options
		uninstallInstalledFn = previous.installed
		uninstallServiceFn = previous.uninstallSvc
		uninstallServiceBinaryFn = previous.serviceBinary
		uninstallRemoveRuntimeDirFn = previous.removeRuntime
		uninstallPurgeDataFn = previous.purgeData
		uninstallOtherAccountsFn = previous.otherAccounts
		uninstallMCPFn = previous.removeMCP
		uninstallMCPLookupFn = previous.mcpLookup
		uninstallExecutableFn = previous.executable
		uninstallEvalSymlinksFn = previous.evalSymlinks
		uninstallGetenvFn = previous.getenv
		uninstallFileOwnerFn = previous.fileOwner
		uninstallRemoveFileFn = previous.removeFile
		uninstallSystemInstalledCheck = previous.systemInstalled
		uninstallSudoFn = previous.sudo
	})

	if s.options != nil {
		uninstallOptionsFn = s.options
	}
	if s.installed != nil {
		uninstallInstalledFn = s.installed
	}
	if s.uninstallSvc != nil {
		uninstallServiceFn = s.uninstallSvc
	}
	if s.serviceBinary != nil {
		uninstallServiceBinaryFn = s.serviceBinary
	}
	if s.removeRuntime != nil {
		uninstallRemoveRuntimeDirFn = s.removeRuntime
	}
	if s.purgeData != nil {
		uninstallPurgeDataFn = s.purgeData
	}
	if s.otherAccounts != nil {
		uninstallOtherAccountsFn = s.otherAccounts
	}
	if s.removeMCP != nil {
		uninstallMCPFn = s.removeMCP
	}
	if s.mcpLookup != nil {
		uninstallMCPLookupFn = s.mcpLookup
	}
	if s.executable != nil {
		uninstallExecutableFn = s.executable
	}
	if s.evalSymlinks != nil {
		uninstallEvalSymlinksFn = s.evalSymlinks
	}
	if s.getenv != nil {
		uninstallGetenvFn = s.getenv
	}
	if s.fileOwner != nil {
		uninstallFileOwnerFn = s.fileOwner
	}
	if s.removeFile != nil {
		uninstallRemoveFileFn = s.removeFile
	}
	if s.systemInstalled != nil {
		uninstallSystemInstalledCheck = s.systemInstalled
	}
	if s.sudo != nil {
		uninstallSudoFn = s.sudo
	}
}

type uninstallRecorder struct {
	steps     []string
	mcpFor    string
	mcpBinary string
}

func uninstallOptions(root string, system bool, euid int) service.Options {
	return service.Options{
		System: system,
		GOOS:   "linux",
		EUID:   euid,
		UID:    euid,
		Home:   "/home/alice",
		Exe:    "/usr/local/bin/peers",
		Root:   root,
	}
}

// TestUninstallUsageErrors checks that an unknown flag or a positional argument
// is a usage error.
func TestUninstallUsageErrors(t *testing.T) {
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"uninstall", "--keep-data"}, "unknown flag: --keep-data"},
		{[]string{"uninstall", "extra"}, `unexpected argument "extra"`},
	}
	for _, tc := range cases {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			a, _, stderr, _ := newDispatchApp("dev")
			if got := a.run(context.Background(), tc.args); got != exitUsage {
				t.Fatalf("run(%q) = %d, want %d", tc.args, got, exitUsage)
			}
			errOut := stderr.String()
			if !strings.Contains(errOut, "peers uninstall: "+tc.want) {
				t.Errorf("stderr = %q, want the command path and %q", errOut, tc.want)
			}
			if !strings.Contains(errOut, "Run 'peers uninstall --help' for usage.") {
				t.Errorf("stderr = %q, want the help hint", errOut)
			}
			if strings.Contains(errOut, "Usage:") {
				t.Errorf("stderr = %q, want no full usage block", errOut)
			}
		})
	}
}

// TestUninstallHelp checks that help lists the command's flags.
func TestUninstallHelp(t *testing.T) {
	a, stdout, _, _ := newDispatchApp("dev")
	if got := a.run(context.Background(), []string{"uninstall", "-h"}); got != exitOK {
		t.Fatalf("run(uninstall -h) = %d, want %d", got, exitOK)
	}
	out := stdout.String()
	for _, want := range []string{"Usage:", "peers uninstall [flags]", "--purge", "--yes", "--help"} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout = %q, want it to contain %q", out, want)
		}
	}
}

// TestUninstallRootSystemLevel checks that a root uninstall removes the system
// level with the data purged.
func TestUninstallRootSystemLevel(t *testing.T) {
	root := t.TempDir()
	binary := "/usr/local/libexec/peers"
	binPath := filepath.Join(root, binary)
	if err := os.MkdirAll(filepath.Dir(binPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(binPath, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}

	rec := &uninstallRecorder{}
	withUninstallSeams(t, uninstallSeams{
		options: func(system bool) (service.Options, error) {
			return uninstallOptions(root, system, 0), nil
		},
		installed: func(opts service.Options) (bool, error) { return opts.System, nil },
		serviceBinary: func(service.Options) (string, bool, error) {
			return binary, true, nil
		},
		uninstallSvc: func(service.Options) (service.Result, error) {
			rec.steps = append(rec.steps, "service")
			return service.Result{UnitPath: "/etc/systemd/system/peers.service", Removed: true}, nil
		},
		removeMCP: func(account, binary string) (string, error) {
			rec.steps = append(rec.steps, "mcp")
			rec.mcpFor, rec.mcpBinary = account, binary
			return "removed the peer MCP registration for bob", nil
		},
		mcpLookup: func(string) mcpRegistration { return mcpRegistration{} },
		removeRuntime: func(service.Options) ([]string, error) {
			rec.steps = append(rec.steps, "runtime")
			return []string{"/run/peers"}, nil
		},
		purgeData: func(service.Options) ([]string, error) {
			rec.steps = append(rec.steps, "purge")
			return []string{"/etc/peers", "group peers"}, nil
		},
		otherAccounts: func(service.Options) ([]string, error) {
			rec.steps = append(rec.steps, "others")
			return []string{"other"}, nil
		},
		getenv: func(key string) string {
			if key == "SUDO_USER" {
				return "bob"
			}
			return ""
		},
		fileOwner:    func(string) (uint32, error) { return 0, nil },
		removeFile:   func(path string) error { rec.steps = append(rec.steps, "binary"); return os.Remove(path) },
		executable:   func() (string, error) { return binPath, nil },
		evalSymlinks: func(path string) (string, error) { return path, nil },
	})

	a, stdout, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 0 }
	if got := a.run(context.Background(), []string{"uninstall", "--purge", "--yes"}); got != exitOK {
		t.Fatalf("run(uninstall --purge) = %d, want %d\nstdout: %s\nstderr: %s", got, exitOK, stdout, stderr)
	}
	if want := []string{"service", "mcp", "runtime", "purge", "others", "binary"}; !slices.Equal(rec.steps, want) {
		t.Errorf("steps = %q, want %q", rec.steps, want)
	}
	if rec.mcpFor != "bob" || rec.mcpBinary != binary {
		t.Errorf("MCP registration %q of %q, want bob of %s", rec.mcpFor, rec.mcpBinary, binary)
	}
	if strings.Contains(stderr.String(), uninstallDataPrompt) {
		t.Errorf("stderr = %q, want no data question with --purge", stderr)
	}
	for _, want := range []string{
		"stopped and removed the service /etc/systemd/system/peers.service",
		"Other accounts run a user Broker: other",
		"removed the binary " + binary,
		"each account that registered the peer MCP server",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("stdout does not contain %q:\n%s", want, stdout)
		}
	}
	if _, err := os.Stat(binPath); !os.IsNotExist(err) {
		t.Errorf("binary still exists: %v", err)
	}
}

// TestUninstallRootPurgeSkipsDataQuestion checks that --purge purges the data
// without asking.
func TestUninstallRootPurgeSkipsDataQuestion(t *testing.T) {
	root := t.TempDir()
	purged := false
	withUninstallSeams(t, uninstallSeams{
		options: func(system bool) (service.Options, error) {
			return uninstallOptions(root, system, 0), nil
		},
		installed: func(opts service.Options) (bool, error) { return opts.System, nil },
		serviceBinary: func(service.Options) (string, bool, error) {
			return "/usr/local/bin/peers", true, nil
		},
		uninstallSvc: func(service.Options) (service.Result, error) {
			return service.Result{UnitPath: "/etc/systemd/system/peers.service", Removed: true}, nil
		},
		removeMCP: func(string, string) (string, error) { return "removed", nil },
		mcpLookup: func(string) mcpRegistration { return mcpRegistration{} },
		removeRuntime: func(service.Options) ([]string, error) {
			return []string{"/run/peers"}, nil
		},
		purgeData: func(service.Options) ([]string, error) {
			purged = true
			return []string{"/etc/peers"}, nil
		},
		otherAccounts: func(service.Options) ([]string, error) { return nil, nil },
		getenv:        func(string) string { return "" },
		fileOwner:     func(string) (uint32, error) { return 0, nil },
		removeFile:    func(string) error { return nil },
		executable:    func() (string, error) { return "/usr/local/bin/peers", nil },
		evalSymlinks:  func(path string) (string, error) { return path, nil },
	})

	a, _, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 0 }
	if got := a.run(context.Background(), []string{"uninstall", "--purge", "--yes"}); got != exitOK {
		t.Fatalf("run(uninstall --purge) = %d, want %d (stderr %s)", got, exitOK, stderr)
	}
	if strings.Contains(stderr.String(), uninstallDataPrompt) {
		t.Errorf("stderr = %q, want no data question with --purge", stderr)
	}
	if !purged {
		t.Error("PurgeData was not called with --purge")
	}
}

// TestUninstallRootPurgeWithTerminalSkipsDataQuestion checks that --purge does
// not become the data question when a terminal is present.
func TestUninstallRootPurgeWithTerminalSkipsDataQuestion(t *testing.T) {
	root := t.TempDir()
	purged := false
	withUninstallSeams(t, uninstallSeams{
		options: func(system bool) (service.Options, error) {
			return uninstallOptions(root, system, 0), nil
		},
		installed: func(opts service.Options) (bool, error) { return opts.System, nil },
		serviceBinary: func(service.Options) (string, bool, error) {
			return "/usr/local/bin/peers", true, nil
		},
		uninstallSvc: func(service.Options) (service.Result, error) {
			return service.Result{UnitPath: "/etc/systemd/system/peers.service", Removed: true}, nil
		},
		removeMCP: func(string, string) (string, error) { return "removed", nil },
		mcpLookup: func(string) mcpRegistration { return mcpRegistration{} },
		removeRuntime: func(service.Options) ([]string, error) {
			return []string{"/run/peers"}, nil
		},
		purgeData: func(service.Options) ([]string, error) {
			purged = true
			return []string{"/etc/peers"}, nil
		},
		otherAccounts: func(service.Options) ([]string, error) { return nil, nil },
		getenv:        func(string) string { return "" },
		fileOwner:     func(string) (uint32, error) { return 0, nil },
		removeFile:    func(string) error { return nil },
		executable:    func() (string, error) { return "/usr/local/bin/peers", nil },
		evalSymlinks:  func(path string) (string, error) { return path, nil },
	})

	a, _, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 0 }
	withTerminalAnswer(t, a, "n\n")
	if got := a.run(context.Background(), []string{"uninstall", "--purge"}); got != exitOK {
		t.Fatalf("run(uninstall --purge) = %d, want %d (stderr %s)", got, exitOK, stderr)
	}
	if strings.Contains(stderr.String(), uninstallDataPrompt) {
		t.Errorf("stderr = %q, want no data question with --purge", stderr)
	}
	if !purged {
		t.Error("PurgeData was not called with --purge")
	}
}

// TestUninstallRootNotInstalled checks that a root uninstall with nothing
// installed reports it and changes nothing.
func TestUninstallRootNotInstalled(t *testing.T) {
	rec := &uninstallRecorder{}
	withUninstallSeams(t, uninstallSeams{
		options: func(system bool) (service.Options, error) {
			return uninstallOptions(t.TempDir(), system, 0), nil
		},
		installed:    func(service.Options) (bool, error) { return false, nil },
		executable:   func() (string, error) { return "/usr/local/bin/peers", nil },
		evalSymlinks: func(path string) (string, error) { return path, nil },
		mcpLookup:    func(string) mcpRegistration { return mcpRegistration{} },
		uninstallSvc: func(service.Options) (service.Result, error) {
			rec.steps = append(rec.steps, "service")
			return service.Result{}, nil
		},
	})

	a, stdout, _, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 0 }
	if got := a.run(context.Background(), []string{"uninstall"}); got != exitOK {
		t.Fatalf("run(uninstall) = %d, want %d", got, exitOK)
	}
	if !strings.Contains(stdout.String(), "not installed") {
		t.Errorf("stdout = %q, want it to report no installation", stdout)
	}
	if len(rec.steps) != 0 {
		t.Errorf("steps = %q, want none", rec.steps)
	}
}

// TestUninstallRootNoServiceInstallation checks that a --no-service installation
// is found through the MCP registration and removed.
func TestUninstallRootNoServiceInstallation(t *testing.T) {
	root := t.TempDir()
	resolved := "/usr/local/bin/peers"
	if err := os.MkdirAll(filepath.Join(root, filepath.Dir(resolved)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, resolved), []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	rec := &uninstallRecorder{}
	withUninstallSeams(t, uninstallSeams{
		options: func(system bool) (service.Options, error) {
			return uninstallOptions(root, system, 0), nil
		},
		installed: func(service.Options) (bool, error) { return false, nil },
		mcpLookup: func(account string) mcpRegistration {
			if account == "bob" {
				return mcpRegistration{command: resolved}
			}
			return mcpRegistration{}
		},
		serviceBinary: func(service.Options) (string, bool, error) {
			return "", false, nil
		},
		uninstallSvc: func(service.Options) (service.Result, error) {
			rec.steps = append(rec.steps, "service")
			return service.Result{}, nil
		},
		removeMCP: func(account, binary string) (string, error) {
			rec.steps = append(rec.steps, "mcp")
			rec.mcpFor, rec.mcpBinary = account, binary
			return "removed", nil
		},
		removeRuntime: func(service.Options) ([]string, error) { return nil, nil },
		purgeData:     func(service.Options) ([]string, error) { return nil, nil },
		otherAccounts: func(service.Options) ([]string, error) { return nil, nil },
		getenv: func(key string) string {
			if key == "SUDO_USER" {
				return "bob"
			}
			return ""
		},
		executable:   func() (string, error) { return filepath.Join(root, resolved), nil },
		evalSymlinks: func(string) (string, error) { return resolved, nil },
		fileOwner:    func(string) (uint32, error) { return 0, nil },
		removeFile:   func(path string) error { rec.steps = append(rec.steps, "binary"); return os.Remove(path) },
	})

	a, stdout, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 0 }
	if got := a.run(context.Background(), []string{"uninstall", "--yes"}); got != exitOK {
		t.Fatalf("run(uninstall --yes) = %d, want %d\nstdout: %s\nstderr: %s", got, exitOK, stdout, stderr)
	}
	if rec.mcpBinary != resolved || rec.mcpFor != "bob" {
		t.Errorf("MCP registration %q of %q, want bob of %s", rec.mcpFor, rec.mcpBinary, resolved)
	}
	if !slices.Contains(rec.steps, "binary") {
		t.Errorf("steps = %q, want the no-service binary removed", rec.steps)
	}
	if _, err := os.Stat(filepath.Join(root, resolved)); !os.IsNotExist(err) {
		t.Errorf("binary still exists: %v", err)
	}
}

// TestUninstallAccountNoServiceAtDefaultPrefix checks that a user installation
// without a service at the default prefix is removed.
func TestUninstallAccountNoServiceAtDefaultPrefix(t *testing.T) {
	root := t.TempDir()
	exe := "/home/alice/.local/bin/peers"
	full := filepath.Join(root, exe)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	rec := &uninstallRecorder{}
	withUninstallSeams(t, uninstallSeams{
		options: func(system bool) (service.Options, error) {
			return uninstallOptions(root, system, 1000), nil
		},
		installed: func(service.Options) (bool, error) { return false, nil },
		mcpLookup: func(account string) mcpRegistration {
			if account == "" {
				return mcpRegistration{command: exe}
			}
			return mcpRegistration{}
		},
		serviceBinary: func(service.Options) (string, bool, error) {
			return "", false, nil
		},
		uninstallSvc: func(service.Options) (service.Result, error) {
			rec.steps = append(rec.steps, "service")
			return service.Result{}, nil
		},
		removeMCP: func(account, binary string) (string, error) {
			rec.steps = append(rec.steps, "mcp")
			rec.mcpFor, rec.mcpBinary = account, binary
			return "removed", nil
		},
		removeRuntime: func(service.Options) ([]string, error) { return nil, nil },
		purgeData:     func(service.Options) ([]string, error) { return nil, nil },
		otherAccounts: func(service.Options) ([]string, error) { return nil, nil },
		fileOwner:     func(string) (uint32, error) { return 1000, nil },
		removeFile:    func(path string) error { rec.steps = append(rec.steps, "binary"); return os.Remove(path) },
		executable:    func() (string, error) { return full, nil },
		evalSymlinks:  func(path string) (string, error) { return path, nil },
		systemInstalled: func(service.Options) bool {
			return false
		},
	})

	a, stdout, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 1000 }
	if got := a.run(context.Background(), []string{"uninstall", "--yes"}); got != exitOK {
		t.Fatalf("run(uninstall --yes) = %d, want %d\nstdout: %s\nstderr: %s", got, exitOK, stdout, stderr)
	}
	if rec.mcpFor != "" || rec.mcpBinary != exe {
		t.Errorf("MCP registration %q of %q, want the running account of %s", rec.mcpFor, rec.mcpBinary, exe)
	}
	if !slices.Contains(rec.steps, "binary") {
		t.Errorf("steps = %q, want the no-service binary removed", rec.steps)
	}
	if _, err := os.Stat(full); !os.IsNotExist(err) {
		t.Errorf("binary still exists: %v", err)
	}
}

// TestUninstallNoServiceOutsideTheDefaultPrefix checks that a downloaded copy
// outside the installation prefix is left alone.
func TestUninstallNoServiceOutsideTheDefaultPrefix(t *testing.T) {
	root := t.TempDir()
	exe := "/home/alice/downloads/peers"
	rec := &uninstallRecorder{}
	withUninstallSeams(t, uninstallSeams{
		options: func(system bool) (service.Options, error) {
			return uninstallOptions(root, system, 1000), nil
		},
		installed: func(service.Options) (bool, error) { return false, nil },
		mcpLookup: func(string) mcpRegistration {
			return mcpRegistration{command: exe}
		},
		uninstallSvc: func(service.Options) (service.Result, error) {
			rec.steps = append(rec.steps, "service")
			return service.Result{}, nil
		},
		removeMCP: func(string, string) (string, error) {
			rec.steps = append(rec.steps, "mcp")
			return "removed", nil
		},
		removeRuntime: func(service.Options) ([]string, error) {
			rec.steps = append(rec.steps, "runtime")
			return nil, nil
		},
		executable: func() (string, error) { return filepath.Join(root, exe), nil },
		evalSymlinks: func(path string) (string, error) {
			return path, nil
		},
	})

	a, stdout, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 1000 }
	if got := a.run(context.Background(), []string{"uninstall"}); got != exitOK {
		t.Fatalf("run(uninstall) = %d, want %d\nstdout: %s\nstderr: %s", got, exitOK, stdout, stderr)
	}
	if !strings.Contains(stdout.String(), "not installed") {
		t.Errorf("stdout = %q, want it to report no installation", stdout)
	}
	if len(rec.steps) != 0 {
		t.Errorf("steps = %q, want a downloaded copy outside the prefix left alone", rec.steps)
	}
}

// TestUninstallKeepsADownloadedCopyWhenNoUnitFileExists checks that the running
// executable is kept when it is not the installation binary.
func TestUninstallKeepsADownloadedCopyWhenNoUnitFileExists(t *testing.T) {
	root := t.TempDir()
	exe := "/home/alice/downloads/peers"
	full := filepath.Join(root, exe)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	rec := &uninstallRecorder{}
	withUninstallSeams(t, uninstallSeams{
		options: func(system bool) (service.Options, error) {
			return uninstallOptions(root, system, 0), nil
		},
		installed: func(opts service.Options) (bool, error) { return opts.System, nil },
		serviceBinary: func(service.Options) (string, bool, error) {
			return "", false, nil
		},
		mcpLookup: func(string) mcpRegistration {
			return mcpRegistration{command: exe}
		},
		uninstallSvc: func(service.Options) (service.Result, error) {
			rec.steps = append(rec.steps, "service")
			return service.Result{UnitPath: "/etc/systemd/system/peers.service", Removed: true}, nil
		},
		removeMCP: func(string, string) (string, error) {
			rec.steps = append(rec.steps, "mcp")
			return "removed", nil
		},
		removeRuntime: func(service.Options) ([]string, error) {
			rec.steps = append(rec.steps, "runtime")
			return nil, nil
		},
		purgeData:     func(service.Options) ([]string, error) { return nil, nil },
		otherAccounts: func(service.Options) ([]string, error) { return nil, nil },
		getenv: func(key string) string {
			if key == "SUDO_USER" {
				return "bob"
			}
			return ""
		},
		fileOwner:    func(string) (uint32, error) { return 0, nil },
		removeFile:   func(string) error { t.Error("removed a downloaded copy"); return nil },
		executable:   func() (string, error) { return full, nil },
		evalSymlinks: func(path string) (string, error) { return path, nil },
	})

	a, stdout, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 0 }
	if got := a.run(context.Background(), []string{"uninstall", "--yes"}); got != exitOK {
		t.Fatalf("run(uninstall --yes) = %d, want %d\nstdout: %s\nstderr: %s", got, exitOK, stdout, stderr)
	}
	if want := []string{"service", "runtime"}; !slices.Equal(rec.steps, want) {
		t.Errorf("steps = %q, want %q", rec.steps, want)
	}
	if !strings.Contains(stdout.String(), "kept the running executable "+full) {
		t.Errorf("stdout = %q, want it to say the executable was kept", stdout)
	}
	if _, err := os.Stat(full); err != nil {
		t.Errorf("downloaded copy was removed: %v", err)
	}
}

// TestUninstallAccountUserLevelKeepsData checks that a user uninstall without
// --purge keeps the data.
func TestUninstallAccountUserLevelKeepsData(t *testing.T) {
	root := t.TempDir()
	binary := "/home/alice/bin/peers"
	binPath := filepath.Join(root, binary)
	if err := os.MkdirAll(filepath.Dir(binPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(binPath, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	rec := &uninstallRecorder{}
	withUninstallSeams(t, uninstallSeams{
		options: func(system bool) (service.Options, error) {
			return uninstallOptions(root, system, 1000), nil
		},
		installed: func(opts service.Options) (bool, error) { return !opts.System, nil },
		serviceBinary: func(service.Options) (string, bool, error) {
			return binary, true, nil
		},
		uninstallSvc: func(service.Options) (service.Result, error) {
			rec.steps = append(rec.steps, "service")
			return service.Result{UnitPath: "/home/alice/.config/systemd/user/peers.service", Removed: true}, nil
		},
		removeMCP: func(account, binary string) (string, error) {
			rec.steps = append(rec.steps, "mcp")
			if account != "" {
				t.Errorf("account = %q, want the running account", account)
			}
			return "removed the peer MCP registration", nil
		},
		mcpLookup: func(string) mcpRegistration { return mcpRegistration{} },
		removeRuntime: func(service.Options) ([]string, error) {
			rec.steps = append(rec.steps, "runtime")
			return []string{"/tmp/peers-1000"}, nil
		},
		purgeData: func(service.Options) ([]string, error) {
			rec.steps = append(rec.steps, "purge")
			return nil, nil
		},
		otherAccounts: func(service.Options) ([]string, error) {
			rec.steps = append(rec.steps, "others")
			return nil, nil
		},
		fileOwner:       func(string) (uint32, error) { return 1000, nil },
		removeFile:      func(path string) error { rec.steps = append(rec.steps, "binary"); return os.Remove(path) },
		executable:      func() (string, error) { return filepath.Join(root, binary), nil },
		evalSymlinks:    func(path string) (string, error) { return path, nil },
		systemInstalled: func(service.Options) bool { return false },
	})

	a, stdout, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 1000 }
	if got := a.run(context.Background(), []string{"uninstall", "--yes"}); got != exitOK {
		t.Fatalf("run(uninstall --yes) = %d, want %d\nstdout: %s\nstderr: %s", got, exitOK, stdout, stderr)
	}
	if want := []string{"service", "mcp", "runtime", "binary"}; !slices.Equal(rec.steps, want) {
		t.Errorf("steps = %q, want %q", rec.steps, want)
	}
	if !strings.Contains(stdout.String(), "kept configuration, data and logs: /home/alice/.peers") {
		t.Errorf("stdout = %q, want the kept paths", stdout)
	}
	if strings.Contains(stderr.String(), uninstallDataPrompt) {
		t.Errorf("stderr = %q, want no data question with --yes", stderr)
	}
}

// TestUninstallAccountHintsAtSystemLevel checks that a user uninstall reports
// the remaining system installation.
func TestUninstallAccountHintsAtSystemLevel(t *testing.T) {
	root := t.TempDir()
	withUninstallSeams(t, uninstallSeams{
		options: func(system bool) (service.Options, error) {
			return uninstallOptions(root, system, 1000), nil
		},
		installed: func(opts service.Options) (bool, error) { return true, nil },
		serviceBinary: func(service.Options) (string, bool, error) {
			return "/home/alice/bin/peers", true, nil
		},
		uninstallSvc: func(service.Options) (service.Result, error) {
			return service.Result{UnitPath: "/home/alice/.config/systemd/user/peers.service", Removed: true}, nil
		},
		removeMCP:       func(string, string) (string, error) { return "removed", nil },
		mcpLookup:       func(string) mcpRegistration { return mcpRegistration{} },
		removeRuntime:   func(service.Options) ([]string, error) { return nil, nil },
		purgeData:       func(service.Options) ([]string, error) { return nil, nil },
		otherAccounts:   func(service.Options) ([]string, error) { return nil, nil },
		fileOwner:       func(string) (uint32, error) { return 1000, nil },
		removeFile:      func(string) error { return nil },
		executable:      func() (string, error) { return "/usr/local/bin/peers", nil },
		evalSymlinks:    func(path string) (string, error) { return path, nil },
		systemInstalled: func(service.Options) bool { return true },
	})

	a, stdout, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 1000 }
	if got := a.run(context.Background(), []string{"uninstall", "--yes"}); got != exitOK {
		t.Fatalf("run(uninstall --yes) = %d, want %d (stderr %s)", got, exitOK, stderr)
	}
	if !strings.Contains(stdout.String(), "sudo peers uninstall") {
		t.Errorf("stdout = %q, want the sudo hint", stdout)
	}
	if !strings.Contains(stdout.String(), "--yes") {
		t.Errorf("stdout = %q, want the rerunnable --yes command", stdout)
	}
}

// TestUninstallAccountOnlySystemRefused checks that an account with only a
// system installation gets the rerunnable sudo command and exit failure.
func TestUninstallAccountOnlySystemRefused(t *testing.T) {
	cases := []struct {
		name  string
		purge bool
		want  string
	}{
		{"keep data", false, "sudo peers uninstall --yes"},
		{"purge", true, "sudo peers uninstall --purge --yes"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &uninstallRecorder{}
			withUninstallSeams(t, uninstallSeams{
				options: func(system bool) (service.Options, error) {
					return uninstallOptions(t.TempDir(), system, 1000), nil
				},
				installed: func(opts service.Options) (bool, error) { return opts.System, nil },
				mcpLookup: func(string) mcpRegistration { return mcpRegistration{} },
				uninstallSvc: func(service.Options) (service.Result, error) {
					rec.steps = append(rec.steps, "service")
					return service.Result{}, nil
				},
				executable: func() (string, error) { return "/usr/local/bin/peers", nil },
			})

			args := []string{"uninstall"}
			if tc.purge {
				args = append(args, "--purge")
			}
			a, stdout, stderr, _ := newDispatchApp("dev")
			a.deps.euid = func() int { return 1000 }
			if got := a.run(context.Background(), args); got != exitFailure {
				t.Fatalf("run(%q) = %d, want %d (stderr %s)", args, got, exitFailure, stderr)
			}
			if len(rec.steps) != 0 {
				t.Errorf("steps = %q, want nothing changed", rec.steps)
			}
			if !strings.Contains(stderr.String(), "only a system installation is present") {
				t.Errorf("stderr = %q, want it to explain the situation", stderr)
			}
			if !strings.Contains(stderr.String(), tc.want) {
				t.Errorf("stderr = %q, want the rerunnable command %q", stderr, tc.want)
			}
			if stdout.Len() != 0 {
				t.Errorf("stdout = %q, want empty", stdout)
			}
		})
	}
}

// TestUninstallAccountOnlySystemDoesNotRunSudo checks that the refused command
// reports the sudo command without running any step.
func TestUninstallAccountOnlySystemDoesNotRunSudo(t *testing.T) {
	rec := &uninstallRecorder{}
	withUninstallSeams(t, uninstallSeams{
		options: func(system bool) (service.Options, error) {
			return uninstallOptions(t.TempDir(), system, 1000), nil
		},
		installed: func(opts service.Options) (bool, error) { return opts.System, nil },
		mcpLookup: func(string) mcpRegistration { return mcpRegistration{} },
		uninstallSvc: func(service.Options) (service.Result, error) {
			rec.steps = append(rec.steps, "service")
			return service.Result{}, nil
		},
		removeMCP: func(string, string) (string, error) {
			rec.steps = append(rec.steps, "mcp")
			return "removed", nil
		},
		executable: func() (string, error) { return "/usr/local/bin/peers", nil },
	})

	a, _, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 1000 }
	if got := a.run(context.Background(), []string{"uninstall"}); got != exitFailure {
		t.Fatalf("run(uninstall) = %d, want %d", got, exitFailure)
	}
	if len(rec.steps) != 0 {
		t.Errorf("steps = %q, want none", rec.steps)
	}
	if !strings.Contains(stderr.String(), "sudo peers uninstall --yes") {
		t.Errorf("stderr = %q, want the rerunnable sudo command", stderr)
	}
}

func onlySystemSudoSeams(t *testing.T, exe string, sudo func(context.Context, string, []string, io.Reader, io.Writer, io.Writer) error) {
	t.Helper()
	withUninstallSeams(t, uninstallSeams{
		options: func(system bool) (service.Options, error) {
			return uninstallOptions(t.TempDir(), system, 1000), nil
		},
		installed:  func(opts service.Options) (bool, error) { return opts.System, nil },
		mcpLookup:  func(string) mcpRegistration { return mcpRegistration{} },
		executable: func() (string, error) { return exe, nil },
		sudo:       sudo,
	})
}

// TestUninstallAccountOnlySystemYesRunsSudo checks that --yes reruns the command
// through sudo with --yes.
func TestUninstallAccountOnlySystemYesRunsSudo(t *testing.T) {
	var (
		gotExe  string
		gotArgs []string
		calls   int
	)
	onlySystemSudoSeams(t, "/usr/local/bin/peers",
		func(_ context.Context, exe string, args []string, _ io.Reader, _, _ io.Writer) error {
			calls++
			gotExe, gotArgs = exe, args
			return nil
		})

	a, stdout, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 1000 }
	if got := a.run(context.Background(), []string{"uninstall", "--yes"}); got != exitOK {
		t.Fatalf("run(uninstall --yes) = %d, want %d (stderr %q)", got, exitOK, stderr)
	}
	if calls != 1 {
		t.Fatalf("sudo calls = %d, want 1", calls)
	}
	if gotExe != "/usr/local/bin/peers" {
		t.Errorf("sudo executable = %q, want the running binary", gotExe)
	}
	if want := []string{"uninstall", "--yes"}; !slices.Equal(gotArgs, want) {
		t.Errorf("sudo args = %q, want %q", gotArgs, want)
	}
	if strings.Contains(stderr.String(), uninstallSystemPrompt) {
		t.Errorf("stderr = %q, want no question with --yes", stderr)
	}
	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want empty", stdout)
	}
}

// TestUninstallAccountOnlySystemTerminalAnswersYesRunsSudo checks that a
// terminal answer of y reruns through sudo without adding --yes.
func TestUninstallAccountOnlySystemTerminalAnswersYesRunsSudo(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "leaves the data question to the child",
			args: []string{"uninstall"},
			want: []string{"uninstall"},
		},
		{
			name: "forwards --purge without --yes",
			args: []string{"uninstall", "--purge"},
			want: []string{"uninstall", "--purge"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotArgs []string
			onlySystemSudoSeams(t, "/usr/local/bin/peers",
				func(_ context.Context, _ string, args []string, _ io.Reader, _, _ io.Writer) error {
					gotArgs = args
					return nil
				})

			a, stdout, stderr, _ := newDispatchApp("dev")
			a.deps.euid = func() int { return 1000 }
			withTerminalAnswer(t, a, "y\n")
			if got := a.run(context.Background(), tc.args); got != exitOK {
				t.Fatalf("run(%q) = %d, want %d (stderr %q)", tc.args, got, exitOK, stderr)
			}
			if !slices.Equal(gotArgs, tc.want) {
				t.Errorf("sudo args = %q, want %q", gotArgs, tc.want)
			}
			if !strings.Contains(stderr.String(), uninstallSystemPrompt) {
				t.Errorf("stderr = %q, want the system uninstall question", stderr)
			}
			if strings.Contains(stdout.String(), "Run: sudo") {
				t.Errorf("stdout = %q, want the command to run instead of being printed", stdout)
			}
		})
	}
}

// TestUninstallAccountOnlySystemTerminalDeclines checks that a declined question
// prints the rerunnable command and runs nothing.
func TestUninstallAccountOnlySystemTerminalDeclines(t *testing.T) {
	onlySystemSudoSeams(t, "/usr/local/bin/peers",
		func(context.Context, string, []string, io.Reader, io.Writer, io.Writer) error {
			t.Fatal("sudo ran after the question was declined")
			return nil
		})

	a, stdout, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 1000 }
	withTerminalAnswer(t, a, "\n")
	if got := a.run(context.Background(), []string{"uninstall"}); got != exitOK {
		t.Fatalf("run(uninstall) = %d, want %d (stderr %q)", got, exitOK, stderr)
	}
	if !strings.Contains(stdout.String(), "Run: sudo peers uninstall --yes") {
		t.Errorf("stdout = %q, want the rerunnable command", stdout)
	}
	if !strings.Contains(stderr.String(), uninstallSystemPrompt) {
		t.Errorf("stderr = %q, want the system uninstall question", stderr)
	}
}

// TestUninstallStopsAfterAFailedStep checks that a failed step names itself and
// stops the later steps.
func TestUninstallStopsAfterAFailedStep(t *testing.T) {
	rec := &uninstallRecorder{}
	withUninstallSeams(t, uninstallSeams{
		options: func(system bool) (service.Options, error) {
			return uninstallOptions(t.TempDir(), system, 1000), nil
		},
		installed: func(opts service.Options) (bool, error) { return !opts.System, nil },
		serviceBinary: func(service.Options) (string, bool, error) {
			return "/home/alice/bin/peers", true, nil
		},
		uninstallSvc: func(service.Options) (service.Result, error) {
			return service.Result{}, os.ErrPermission
		},
		removeRuntime: func(service.Options) ([]string, error) {
			rec.steps = append(rec.steps, "runtime")
			return nil, nil
		},
		purgeData: func(service.Options) ([]string, error) {
			rec.steps = append(rec.steps, "purge")
			return nil, nil
		},
		mcpLookup: func(string) mcpRegistration { return mcpRegistration{} },
	})

	a, _, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 1000 }
	if got := a.run(context.Background(), []string{"uninstall", "--yes"}); got != exitFailure {
		t.Fatalf("run(uninstall --yes) = %d, want %d", got, exitFailure)
	}
	if !strings.Contains(stderr.String(), "cannot stop and remove the service") {
		t.Errorf("stderr = %q, want the failed step named", stderr)
	}
	if len(rec.steps) != 0 {
		t.Errorf("later steps ran after the failure: %q", rec.steps)
	}
}

// TestUninstallKeepsABinaryOfAnotherAccount checks that a user uninstall keeps a
// binary owned by another account.
func TestUninstallKeepsABinaryOfAnotherAccount(t *testing.T) {
	root := t.TempDir()
	binary := "/usr/local/bin/peers"
	full := filepath.Join(root, binary)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	withUninstallSeams(t, uninstallSeams{
		options: func(system bool) (service.Options, error) {
			return uninstallOptions(root, system, 1000), nil
		},
		installed: func(opts service.Options) (bool, error) { return !opts.System, nil },
		serviceBinary: func(service.Options) (string, bool, error) {
			return binary, true, nil
		},
		uninstallSvc: func(service.Options) (service.Result, error) {
			return service.Result{UnitPath: "/home/alice/.config/systemd/user/peers.service", Removed: true}, nil
		},
		removeMCP:     func(string, string) (string, error) { return "removed", nil },
		mcpLookup:     func(string) mcpRegistration { return mcpRegistration{} },
		removeRuntime: func(service.Options) ([]string, error) { return nil, nil },
		purgeData:     func(service.Options) ([]string, error) { return nil, nil },
		otherAccounts: func(service.Options) ([]string, error) { return nil, nil },
		fileOwner:     func(string) (uint32, error) { return 0, nil },
		removeFile: func(string) error {
			t.Error("removed a binary that belongs to another account")
			return nil
		},
		executable:   func() (string, error) { return full, nil },
		evalSymlinks: func(path string) (string, error) { return path, nil },
		systemInstalled: func(service.Options) bool {
			return false
		},
	})

	a, stdout, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 1000 }
	if got := a.run(context.Background(), []string{"uninstall", "--yes"}); got != exitOK {
		t.Fatalf("run(uninstall --yes) = %d, want %d (stderr %s)", got, exitOK, stderr)
	}
	if !strings.Contains(stdout.String(), "belongs to another account") {
		t.Errorf("stdout = %q, want the reason the binary was kept", stdout)
	}
}

// TestUninstallReportsPathsRemovedBeforeAPurgeFailure checks that a mid-purge
// failure reports the removed paths before the error.
func TestUninstallReportsPathsRemovedBeforeAPurgeFailure(t *testing.T) {
	root := t.TempDir()
	withUninstallSeams(t, uninstallSeams{
		options: func(system bool) (service.Options, error) {
			return uninstallOptions(root, system, 1000), nil
		},
		installed: func(opts service.Options) (bool, error) { return !opts.System, nil },
		serviceBinary: func(service.Options) (string, bool, error) {
			return "/home/alice/bin/peers", true, nil
		},
		uninstallSvc: func(service.Options) (service.Result, error) {
			return service.Result{UnitPath: "/home/alice/.config/systemd/user/peers.service", Removed: true}, nil
		},
		removeMCP:     func(string, string) (string, error) { return "removed", nil },
		mcpLookup:     func(string) mcpRegistration { return mcpRegistration{} },
		removeRuntime: func(service.Options) ([]string, error) { return nil, nil },
		purgeData: func(service.Options) ([]string, error) {
			return []string{
				"/home/alice/.peers",
				"kept group ops: the installation did not record creating it; remove it manually with: groupdel ops",
			}, errors.New("disk on fire")
		},
		otherAccounts:   func(service.Options) ([]string, error) { return nil, nil },
		fileOwner:       func(string) (uint32, error) { return 1000, nil },
		removeFile:      func(string) error { return nil },
		executable:      func() (string, error) { return "/usr/local/bin/peers", nil },
		evalSymlinks:    func(path string) (string, error) { return path, nil },
		systemInstalled: func(service.Options) bool { return false },
	})

	a, stdout, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 1000 }
	if got := a.run(context.Background(), []string{"uninstall", "--purge", "--yes"}); got != exitFailure {
		t.Fatalf("run(uninstall --purge) = %d, want %d", got, exitFailure)
	}
	if !strings.Contains(stdout.String(), "removed /home/alice/.peers") {
		t.Errorf("stdout does not list the removed path before the failure:\n%s", stdout)
	}
	if !strings.Contains(stdout.String(), "kept group ops:") {
		t.Errorf("stdout does not print the kept group as it is:\n%s", stdout)
	}
	if strings.Contains(stdout.String(), "removed kept group") {
		t.Errorf("stdout prefixes a kept-group message with removed:\n%s", stdout)
	}
	if !strings.Contains(stderr.String(), "cannot remove the data") {
		t.Errorf("stderr = %q, want the failed step named", stderr)
	}
}

// TestUninstallRejectsRelativeHomeBeforeAnyStep checks that a relative home
// directory fails before any step runs.
func TestUninstallRejectsRelativeHomeBeforeAnyStep(t *testing.T) {
	rec := &uninstallRecorder{}
	withUninstallSeams(t, uninstallSeams{
		options: func(system bool) (service.Options, error) {
			opts := uninstallOptions(t.TempDir(), system, 1000)
			opts.Home = "alice"
			return opts, nil
		},
		installed: func(service.Options) (bool, error) { return true, nil },
		serviceBinary: func(service.Options) (string, bool, error) {
			return "/home/alice/.local/bin/peers", true, nil
		},
		uninstallSvc: func(service.Options) (service.Result, error) {
			rec.steps = append(rec.steps, "service")
			return service.Result{UnitPath: "/home/alice/.config/systemd/user/peers.service", Removed: true}, nil
		},
		removeMCP: func(string, string) (string, error) {
			rec.steps = append(rec.steps, "mcp")
			return "removed", nil
		},
		mcpLookup: func(string) mcpRegistration { return mcpRegistration{} },
		removeRuntime: func(service.Options) ([]string, error) {
			rec.steps = append(rec.steps, "runtime")
			return nil, nil
		},
		purgeData: func(service.Options) ([]string, error) {
			rec.steps = append(rec.steps, "purge")
			return nil, nil
		},
		otherAccounts: func(service.Options) ([]string, error) {
			rec.steps = append(rec.steps, "others")
			return nil, nil
		},
		systemInstalled: func(service.Options) bool {
			return false
		},
	})

	a, _, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 1000 }
	if got := a.run(context.Background(), []string{"uninstall", "--purge"}); got != exitFailure {
		t.Errorf("run(uninstall --purge) = %d, want %d", got, exitFailure)
	}
	if !strings.Contains(stderr.String(), "home directory") {
		t.Errorf("stderr = %q, want it to name the home directory", stderr)
	}
	if len(rec.steps) != 0 {
		t.Errorf("steps = %q, want nothing changed before rejecting the home directory", rec.steps)
	}
}

// TestYesAnswer checks which answers count as yes.
func TestYesAnswer(t *testing.T) {
	for _, answer := range []string{"y", "Y", "yes", "YES"} {
		if !isYes(answer) {
			t.Errorf("isYes(%q) = false, want true", answer)
		}
	}
	for _, answer := range []string{"", "n", "no", "yess", "1"} {
		if isYes(answer) {
			t.Errorf("isYes(%q) = true, want false", answer)
		}
	}
	line, err := readAnswer(strings.NewReader(" yes \n"))
	if err != nil {
		t.Fatalf("readAnswer: %v", err)
	}
	if !isYes(line) {
		t.Errorf("isYes(readAnswer(%q)) = false, want true", " yes \n")
	}
}

func runBinaryDeletion(t *testing.T, binary, exe string) (removed bool, output string) {
	t.Helper()
	root := t.TempDir()
	full := filepath.Join(root, binary)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	withUninstallSeams(t, uninstallSeams{
		options: func(system bool) (service.Options, error) {
			return uninstallOptions(root, system, 0), nil
		},
		installed: func(opts service.Options) (bool, error) { return opts.System, nil },
		serviceBinary: func(service.Options) (string, bool, error) {
			return binary, true, nil
		},
		uninstallSvc: func(service.Options) (service.Result, error) {
			return service.Result{UnitPath: "/etc/systemd/system/peers.service", Removed: true}, nil
		},
		removeMCP:     func(string, string) (string, error) { return "removed", nil },
		mcpLookup:     func(string) mcpRegistration { return mcpRegistration{} },
		removeRuntime: func(service.Options) ([]string, error) { return nil, nil },
		purgeData:     func(service.Options) ([]string, error) { return nil, nil },
		otherAccounts: func(service.Options) ([]string, error) { return nil, nil },
		getenv:        func(string) string { return "" },
		fileOwner:     func(string) (uint32, error) { return 0, nil },
		removeFile: func(path string) error {
			removed = true
			return os.Remove(path)
		},
		executable:   func() (string, error) { return filepath.Join(root, exe), nil },
		evalSymlinks: func(path string) (string, error) { return path, nil },
	})

	a, stdout, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 0 }
	if got := a.run(context.Background(), []string{"uninstall", "--yes"}); got != exitOK {
		t.Fatalf("run(uninstall) = %d, want %d\nstdout: %s\nstderr: %s", got, exitOK, stdout, stderr)
	}
	return removed, stdout.String()
}

// TestUninstallKeepsABinaryThatIsNotPeers checks that a binary that is not this
// peers binary is kept and reported.
func TestUninstallKeepsABinaryThatIsNotPeers(t *testing.T) {
	cases := []struct {
		name   string
		binary string
		exe    string
		script string
	}{
		{name: "shell", binary: "/bin/sh", exe: "/usr/local/bin/peers"},
		{name: "env", binary: "/usr/bin/env", exe: "/usr/local/bin/peers"},
		{
			name:   "same name different inode",
			binary: "/opt/check/peers",
			exe:    "/opt/other/peers",
			script: "#!/bin/sh\necho ran > \"$0.ran\"\necho peers fake-version\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.script != "" {
				removed, output, marker := runBinaryDeletionWithScript(t, tc.binary, tc.exe, tc.script)
				if removed {
					t.Errorf("removed %s, which is not this peers binary\n%s", tc.binary, output)
				}
				if _, err := os.Stat(marker); !os.IsNotExist(err) {
					t.Errorf("executed %s; a binary of another inode must not be run: %v", tc.binary, err)
				}
				if !strings.Contains(output, "kept the binary "+tc.binary) {
					t.Errorf("output does not say the binary was kept:\n%s", output)
				}
				if !strings.Contains(output, "remove it manually with: rm "+tc.binary) {
					t.Errorf("output does not give the manual removal command:\n%s", output)
				}
				return
			}
			removed, output := runBinaryDeletion(t, tc.binary, tc.exe)
			if removed {
				t.Errorf("removed %s, which is not this peers binary\n%s", tc.binary, output)
			}
			if !strings.Contains(output, "kept the binary "+tc.binary) {
				t.Errorf("output does not say the binary was kept:\n%s", output)
			}
		})
	}
}

func runBinaryDeletionWithScript(t *testing.T, binary, exe, script string) (removed bool, output, marker string) {
	t.Helper()
	root := t.TempDir()
	marker = filepath.Join(root, binary+".ran")
	for path, content := range map[string]string{binary: script, exe: "other"} {
		full := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	withUninstallSeams(t, uninstallSeams{
		options: func(system bool) (service.Options, error) {
			return uninstallOptions(root, system, 0), nil
		},
		installed: func(opts service.Options) (bool, error) { return opts.System, nil },
		serviceBinary: func(service.Options) (string, bool, error) {
			return binary, true, nil
		},
		uninstallSvc: func(service.Options) (service.Result, error) {
			return service.Result{UnitPath: "/etc/systemd/system/peers.service", Removed: true}, nil
		},
		removeMCP:     func(string, string) (string, error) { return "removed", nil },
		mcpLookup:     func(string) mcpRegistration { return mcpRegistration{} },
		removeRuntime: func(service.Options) ([]string, error) { return nil, nil },
		purgeData:     func(service.Options) ([]string, error) { return nil, nil },
		otherAccounts: func(service.Options) ([]string, error) { return nil, nil },
		getenv:        func(string) string { return "" },
		fileOwner:     func(string) (uint32, error) { return 0, nil },
		removeFile: func(path string) error {
			removed = true
			return os.Remove(path)
		},
		executable:   func() (string, error) { return filepath.Join(root, exe), nil },
		evalSymlinks: func(path string) (string, error) { return path, nil },
	})

	a, stdout, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 0 }
	if got := a.run(context.Background(), []string{"uninstall", "--yes"}); got != exitOK {
		t.Fatalf("run(uninstall) = %d, want %d\nstdout: %s\nstderr: %s", got, exitOK, stdout, stderr)
	}
	return removed, stdout.String(), marker
}

// TestUninstallRemovesTheServiceBinary checks that the service binary is
// removed.
func TestUninstallRemovesTheServiceBinary(t *testing.T) {
	root := t.TempDir()
	binary := "/usr/local/bin/peers"
	full := filepath.Join(root, binary)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	removed := false
	withUninstallSeams(t, uninstallSeams{
		options: func(system bool) (service.Options, error) {
			return uninstallOptions(root, system, 0), nil
		},
		installed: func(opts service.Options) (bool, error) { return opts.System, nil },
		serviceBinary: func(service.Options) (string, bool, error) {
			return binary, true, nil
		},
		uninstallSvc: func(service.Options) (service.Result, error) {
			return service.Result{UnitPath: "/etc/systemd/system/peers.service", Removed: true}, nil
		},
		removeMCP:     func(string, string) (string, error) { return "removed", nil },
		mcpLookup:     func(string) mcpRegistration { return mcpRegistration{} },
		removeRuntime: func(service.Options) ([]string, error) { return nil, nil },
		purgeData:     func(service.Options) ([]string, error) { return nil, nil },
		otherAccounts: func(service.Options) ([]string, error) { return nil, nil },
		getenv:        func(string) string { return "" },
		fileOwner:     func(string) (uint32, error) { return 0, nil },
		removeFile: func(path string) error {
			removed = true
			return os.Remove(path)
		},
		executable:   func() (string, error) { return full, nil },
		evalSymlinks: func(path string) (string, error) { return path, nil },
	})

	a, stdout, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 0 }
	if got := a.run(context.Background(), []string{"uninstall", "--yes"}); got != exitOK {
		t.Fatalf("run(uninstall --yes) = %d, want %d\nstdout: %s\nstderr: %s", got, exitOK, stdout, stderr)
	}
	if !removed {
		t.Errorf("the service binary was not removed:\n%s", stdout)
	}
	if _, err := os.Stat(full); !os.IsNotExist(err) {
		t.Errorf("binary still exists: %v", err)
	}
}

func writeUnderRoot(t *testing.T, root, path string) {
	t.Helper()
	writeUnderRootWith(t, root, path, "content\n")
}

func writeUnderRootWith(t *testing.T, root, path, content string) {
	t.Helper()
	full := filepath.Join(root, path)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatalf("creating %s: %v", filepath.Dir(full), err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatalf("writing %s: %v", full, err)
	}
}

func completionMarker(path string) string {
	switch {
	case strings.HasSuffix(path, "_peers"):
		return "#compdef peers\n# zsh completion for peers\n"
	case strings.HasSuffix(path, ".fish"):
		return "# fish completion for peers\n"
	default:
		return "# bash completion V2 for peers\n"
	}
}

// TestUninstallWithoutTerminalNeedsYes checks that a data-removing uninstall
// without a terminal fails and names --yes.
func TestUninstallWithoutTerminalNeedsYes(t *testing.T) {
	cases := []struct {
		name   string
		system bool
		euid   int
		args   []string
	}{
		{"root system installation", true, 0, []string{"uninstall"}},
		{"account user installation", false, 1000, []string{"uninstall"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			rec := &uninstallRecorder{}
			withUninstallSeams(t, uninstallSeams{
				options: func(system bool) (service.Options, error) {
					return uninstallOptions(root, system, tc.euid), nil
				},
				installed: func(opts service.Options) (bool, error) { return opts.System == tc.system, nil },
				serviceBinary: func(service.Options) (string, bool, error) {
					return "/usr/local/bin/peers", true, nil
				},
				uninstallSvc: func(service.Options) (service.Result, error) {
					rec.steps = append(rec.steps, "service")
					return service.Result{}, nil
				},
				removeMCP: func(string, string) (string, error) {
					rec.steps = append(rec.steps, "mcp")
					return "removed", nil
				},
				mcpLookup:     func(string) mcpRegistration { return mcpRegistration{} },
				removeRuntime: func(service.Options) ([]string, error) { return nil, nil },
				purgeData: func(service.Options) ([]string, error) {
					rec.steps = append(rec.steps, "purge")
					return nil, nil
				},
				otherAccounts:   func(service.Options) ([]string, error) { return nil, nil },
				getenv:          func(string) string { return "" },
				systemInstalled: func(service.Options) bool { return false },
			})

			a, stdout, stderr, _ := newDispatchApp("dev")
			a.deps.euid = func() int { return tc.euid }
			if got := a.run(context.Background(), tc.args); got != exitFailure {
				t.Fatalf("run(%q) = %d, want %d (stdout %q, stderr %q)", tc.args, got, exitFailure, stdout, stderr)
			}
			if !strings.Contains(stderr.String(), "--yes") {
				t.Errorf("stderr = %q, want it to name --yes", stderr)
			}
			if len(rec.steps) != 0 {
				t.Errorf("steps = %q, want nothing removed without an answer", rec.steps)
			}
		})
	}
}

// TestUninstallPurgeWithoutTerminalNeedsYes checks that --purge still needs
// --yes when there is no terminal.
func TestUninstallPurgeWithoutTerminalNeedsYes(t *testing.T) {
	cases := []struct {
		name   string
		system bool
		euid   int
	}{
		{"root system installation", true, 0},
		{"account user installation", false, 1000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			rec := &uninstallRecorder{}
			withUninstallSeams(t, uninstallSeams{
				options: func(system bool) (service.Options, error) {
					return uninstallOptions(root, system, tc.euid), nil
				},
				installed: func(opts service.Options) (bool, error) { return opts.System == tc.system, nil },
				serviceBinary: func(service.Options) (string, bool, error) {
					return "/usr/local/bin/peers", true, nil
				},
				uninstallSvc: func(service.Options) (service.Result, error) {
					rec.steps = append(rec.steps, "service")
					return service.Result{}, nil
				},
				removeMCP: func(string, string) (string, error) {
					rec.steps = append(rec.steps, "mcp")
					return "removed", nil
				},
				mcpLookup:     func(string) mcpRegistration { return mcpRegistration{} },
				removeRuntime: func(service.Options) ([]string, error) { return nil, nil },
				purgeData: func(service.Options) ([]string, error) {
					rec.steps = append(rec.steps, "purge")
					return nil, nil
				},
				otherAccounts:   func(service.Options) ([]string, error) { return nil, nil },
				getenv:          func(string) string { return "" },
				systemInstalled: func(service.Options) bool { return false },
			})

			a, stdout, stderr, _ := newDispatchApp("dev")
			a.deps.euid = func() int { return tc.euid }
			if got := a.run(context.Background(), []string{"uninstall", "--purge"}); got != exitFailure {
				t.Fatalf("run(uninstall --purge) = %d, want %d (stdout %q, stderr %q)", got, exitFailure, stdout, stderr)
			}
			if !strings.Contains(stderr.String(), "--yes") {
				t.Errorf("stderr = %q, want it to name --yes", stderr)
			}
			if len(rec.steps) != 0 {
				t.Errorf("steps = %q, want nothing removed without --yes", rec.steps)
			}
		})
	}
}

// TestUninstallAnswersYesToTheDataQuestion checks that the question goes to
// standard error and an answer of y deletes the data.
func TestUninstallAnswersYesToTheDataQuestion(t *testing.T) {
	root := t.TempDir()
	purged := false
	withUninstallSeams(t, uninstallSeams{
		options: func(system bool) (service.Options, error) {
			return uninstallOptions(root, system, 1000), nil
		},
		installed: func(opts service.Options) (bool, error) { return !opts.System, nil },
		serviceBinary: func(service.Options) (string, bool, error) {
			return "/home/alice/bin/peers", true, nil
		},
		uninstallSvc:  func(service.Options) (service.Result, error) { return service.Result{}, nil },
		removeMCP:     func(string, string) (string, error) { return "removed", nil },
		mcpLookup:     func(string) mcpRegistration { return mcpRegistration{} },
		removeRuntime: func(service.Options) ([]string, error) { return nil, nil },
		purgeData: func(service.Options) ([]string, error) {
			purged = true
			return nil, nil
		},
		otherAccounts: func(service.Options) ([]string, error) { return nil, nil },
		getenv:        func(string) string { return "" },
		fileOwner:     func(string) (uint32, error) { return 1000, nil },
		removeFile:    func(string) error { return nil },
		executable:    func() (string, error) { return "/home/alice/bin/peers", nil },
		evalSymlinks:  func(path string) (string, error) { return path, nil },
		systemInstalled: func(service.Options) bool {
			return false
		},
	})

	a, _, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 1000 }
	withTerminalAnswer(t, a, "y\n")
	if got := a.run(context.Background(), []string{"uninstall"}); got != exitOK {
		t.Fatalf("run(uninstall) = %d, want %d (stderr %q)", got, exitOK, stderr)
	}
	if !strings.Contains(stderr.String(), uninstallDataPrompt) {
		t.Errorf("stderr = %q, want the data question", stderr)
	}
	if !purged {
		t.Error("PurgeData was not called after answering y")
	}
}

// TestUninstallRemovesCompletionsAndManPages checks that the installation's
// completion files and man pages are removed and foreign ones kept.
func TestUninstallRemovesCompletionsAndManPages(t *testing.T) {
	t.Run("system installation", func(t *testing.T) {
		root := t.TempDir()
		installed := []string{
			"/usr/local/share/bash-completion/completions/peers",
			"/usr/local/share/zsh/site-functions/_peers",
			"/usr/local/share/fish/vendor_completions.d/peers.fish",
			"/usr/local/share/man/man1/peers.1",
			"/usr/local/share/man/man1/peers-update.1",
		}
		for _, path := range installed {
			if strings.Contains(path, "completion") || strings.HasSuffix(path, "_peers") || strings.HasSuffix(path, ".fish") {
				writeUnderRootWith(t, root, path, completionMarker(path))
				continue
			}
			writeUnderRoot(t, root, path)
		}
		unrelated := []string{
			"/usr/local/share/man/man1/other.1",
			"/usr/local/share/man/man1/peerstat.1",
		}
		for _, path := range unrelated {
			writeUnderRoot(t, root, path)
		}

		withUninstallSeams(t, uninstallSeams{
			options: func(system bool) (service.Options, error) {
				return uninstallOptions(root, system, 0), nil
			},
			installed: func(opts service.Options) (bool, error) { return opts.System, nil },
			serviceBinary: func(service.Options) (string, bool, error) {
				return "/usr/local/bin/peers", true, nil
			},
			uninstallSvc:  func(service.Options) (service.Result, error) { return service.Result{}, nil },
			removeMCP:     func(string, string) (string, error) { return "removed", nil },
			mcpLookup:     func(string) mcpRegistration { return mcpRegistration{} },
			removeRuntime: func(service.Options) ([]string, error) { return nil, nil },
			purgeData:     func(service.Options) ([]string, error) { return nil, nil },
			otherAccounts: func(service.Options) ([]string, error) { return nil, nil },
			getenv:        func(string) string { return "" },
		})

		a, stdout, stderr, _ := newDispatchApp("dev")
		a.deps.euid = func() int { return 0 }
		if got := a.run(context.Background(), []string{"uninstall", "--purge", "--yes"}); got != exitOK {
			t.Fatalf("run(uninstall --purge) = %d, want %d (stderr %q)", got, exitOK, stderr)
		}
		for _, path := range installed {
			if _, err := os.Stat(filepath.Join(root, path)); !os.IsNotExist(err) {
				t.Errorf("%s still exists: %v", path, err)
			}
			if !strings.Contains(stdout.String(), "removed "+filepath.Join(root, path)) {
				t.Errorf("stdout does not report removing %s:\n%s", path, stdout)
			}
		}
		for _, path := range unrelated {
			if _, err := os.Stat(filepath.Join(root, path)); err != nil {
				t.Errorf("removed an unrelated man page %s: %v", path, err)
			}
		}
	})

	t.Run("user installation", func(t *testing.T) {
		root := t.TempDir()
		installed := []string{
			"/home/alice/.local/share/bash-completion/completions/peers",
			"/home/alice/.config/fish/completions/peers.fish",
		}
		for _, path := range installed {
			writeUnderRootWith(t, root, path, completionMarker(path))
		}
		untouched := "/home/alice/.local/share/zsh/site-functions/_peers"
		writeUnderRoot(t, root, untouched)

		withUninstallSeams(t, uninstallSeams{
			options: func(system bool) (service.Options, error) {
				return uninstallOptions(root, system, 1000), nil
			},
			installed: func(opts service.Options) (bool, error) { return !opts.System, nil },
			serviceBinary: func(service.Options) (string, bool, error) {
				return "/home/alice/bin/peers", true, nil
			},
			uninstallSvc:  func(service.Options) (service.Result, error) { return service.Result{}, nil },
			removeMCP:     func(string, string) (string, error) { return "removed", nil },
			mcpLookup:     func(string) mcpRegistration { return mcpRegistration{} },
			removeRuntime: func(service.Options) ([]string, error) { return nil, nil },
			purgeData:     func(service.Options) ([]string, error) { return nil, nil },
			otherAccounts: func(service.Options) ([]string, error) { return nil, nil },
			systemInstalled: func(service.Options) bool {
				return false
			},
		})

		a, _, stderr, _ := newDispatchApp("dev")
		a.deps.euid = func() int { return 1000 }
		if got := a.run(context.Background(), []string{"uninstall", "--yes"}); got != exitOK {
			t.Fatalf("run(uninstall --yes) = %d, want %d (stderr %q)", got, exitOK, stderr)
		}
		for _, path := range installed {
			if _, err := os.Stat(filepath.Join(root, path)); !os.IsNotExist(err) {
				t.Errorf("%s still exists: %v", path, err)
			}
		}
		if _, err := os.Stat(filepath.Join(root, untouched)); err != nil {
			t.Errorf("removed the user zsh completion: %v", err)
		}
	})
}

// TestUninstallKeepsForeignFilesAtInstallationPaths checks that a file without
// cobra's marker and a directory at an installation path are kept and reported.
func TestUninstallKeepsForeignFilesAtInstallationPaths(t *testing.T) {
	root := t.TempDir()
	foreign := "/usr/local/share/bash-completion/completions/peers"
	writeUnderRoot(t, root, foreign)
	directory := "/usr/local/share/man/man1/peers-extra.1"
	if err := os.MkdirAll(filepath.Join(root, directory), 0o755); err != nil {
		t.Fatal(err)
	}

	withUninstallSeams(t, uninstallSeams{
		options: func(system bool) (service.Options, error) {
			return uninstallOptions(root, system, 0), nil
		},
		installed: func(opts service.Options) (bool, error) { return opts.System, nil },
		serviceBinary: func(service.Options) (string, bool, error) {
			return "/usr/local/bin/peers", true, nil
		},
		uninstallSvc:  func(service.Options) (service.Result, error) { return service.Result{}, nil },
		removeMCP:     func(string, string) (string, error) { return "removed", nil },
		mcpLookup:     func(string) mcpRegistration { return mcpRegistration{} },
		removeRuntime: func(service.Options) ([]string, error) { return nil, nil },
		purgeData:     func(service.Options) ([]string, error) { return nil, nil },
		otherAccounts: func(service.Options) ([]string, error) { return nil, nil },
		getenv:        func(string) string { return "" },
	})

	a, stdout, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 0 }
	if got := a.run(context.Background(), []string{"uninstall", "--purge", "--yes"}); got != exitOK {
		t.Fatalf("run(uninstall --purge --yes) = %d, want %d (stderr %q)", got, exitOK, stderr)
	}
	if _, err := os.Stat(filepath.Join(root, foreign)); err != nil {
		t.Errorf("removed a completion file that was not written by peers: %v", err)
	}
	if !strings.Contains(stdout.String(), "kept "+filepath.Join(root, foreign)+": it was not written by peers") {
		t.Errorf("stdout does not report keeping the foreign completion file:\n%s", stdout)
	}
	if _, err := os.Stat(filepath.Join(root, directory)); err != nil {
		t.Errorf("removed a directory named like a man page: %v", err)
	}
	if !strings.Contains(stdout.String(), "kept "+filepath.Join(root, directory)+": it is a directory") {
		t.Errorf("stdout does not report keeping the directory:\n%s", stdout)
	}
}
