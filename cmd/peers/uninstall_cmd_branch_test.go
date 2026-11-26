package main

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/broker/service"
)

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }

func safeUninstallSeams(opts service.Options, installed func(service.Options) (bool, error)) uninstallSeams {
	return uninstallSeams{
		options: func(system bool) (service.Options, error) {
			o := opts
			o.System = system
			return o, nil
		},
		installed: installed,
		serviceBinary: func(service.Options) (string, bool, error) {
			return "", false, nil
		},
		uninstallSvc:  func(service.Options) (service.Result, error) { return service.Result{}, nil },
		removeMCP:     func(string, string) (string, error) { return "removed", nil },
		mcpLookup:     func(string) mcpRegistration { return mcpRegistration{} },
		removeRuntime: func(service.Options) ([]string, error) { return nil, nil },
		purgeData:     func(service.Options) ([]string, error) { return nil, nil },
		otherAccounts: func(service.Options) ([]string, error) { return nil, nil },
		getenv:        func(string) string { return "" },
		fileOwner:     func(string) (uint32, error) { return uint32(opts.EUID), nil },
		removeFile:    func(string) error { return nil },
		executable:    func() (string, error) { return filepath.Join(opts.Root, "/home/alice/bin/peers"), nil },
		evalSymlinks:  func(path string) (string, error) { return path, nil },
		systemInstalled: func(service.Options) bool {
			return false
		},
		sudo: func(context.Context, string, []string, io.Reader, io.Writer, io.Writer) error { return nil },
	}
}

func userLevelSeams(opts service.Options) uninstallSeams {
	return safeUninstallSeams(opts, func(o service.Options) (bool, error) { return !o.System, nil })
}

func systemLevelSeams(opts service.Options) uninstallSeams {
	return safeUninstallSeams(opts, func(o service.Options) (bool, error) { return o.System, nil })
}

func withTerminal(t *testing.T) {
	t.Helper()
	previous := isTerminal
	isTerminal = func(io.Reader) bool { return true }
	t.Cleanup(func() { isTerminal = previous })
}

// TestUninstallOptionsFailure fails the options lookup at each level before
// anything is removed.
func TestUninstallOptionsFailure(t *testing.T) {
	cases := []struct {
		name   string
		system bool
	}{
		{"user options", false},
		{"system options", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := userLevelSeams(uninstallOptions(t.TempDir(), false, 1000))
			s.options = func(system bool) (service.Options, error) {
				if system == tc.system {
					return service.Options{}, errors.New("no home directory")
				}
				return uninstallOptions(t.TempDir(), system, 1000), nil
			}
			withUninstallSeams(t, s)

			a, stdout, stderr, _ := newDispatchApp("dev")
			a.deps.euid = func() int { return 1000 }
			if got := a.run(context.Background(), []string{"uninstall"}); got != exitFailure {
				t.Fatalf("run(uninstall) = %d, want %d", got, exitFailure)
			}
			if want := "cannot determine the options: no home directory"; !strings.Contains(stderr.String(), want) {
				t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
			}
			if stdout.Len() != 0 {
				t.Errorf("stdout = %q, want empty", stdout.String())
			}
		})
	}
}

// TestUninstallInspectionFailure reports an installation-inspection error for
// root, the user level and an account's system check.
func TestUninstallInspectionFailure(t *testing.T) {
	broken := errors.New("broken unit file")
	cases := []struct {
		name      string
		euid      int
		installed func(service.Options) (bool, error)
	}{
		{"root installation", 0, func(o service.Options) (bool, error) {
			return false, broken
		}},
		{"account installation", 1000, func(o service.Options) (bool, error) {
			if !o.System {
				return false, broken
			}
			return false, nil
		}},
		{"account system check", 1000, func(o service.Options) (bool, error) {
			if o.System {
				return false, broken
			}
			return false, nil
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withUninstallSeams(t, safeUninstallSeams(uninstallOptions(t.TempDir(), false, tc.euid), tc.installed))

			a, stdout, stderr, _ := newDispatchApp("dev")
			a.deps.euid = func() int { return tc.euid }
			if got := a.run(context.Background(), []string{"uninstall"}); got != exitFailure {
				t.Fatalf("run(uninstall) = %d, want %d", got, exitFailure)
			}
			if want := "cannot inspect the installation: broken unit file"; !strings.Contains(stderr.String(), want) {
				t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
			}
			if stdout.Len() != 0 {
				t.Errorf("stdout = %q, want empty", stdout.String())
			}
		})
	}
}

// TestUninstallExecutableLookupFailure reports an executable lookup error at the
// root and user levels.
func TestUninstallExecutableLookupFailure(t *testing.T) {
	cases := []struct {
		name      string
		euid      int
		installed func(service.Options) (bool, error)
	}{
		{"root", 0, func(service.Options) (bool, error) { return false, nil }},
		{"account", 1000, func(service.Options) (bool, error) { return false, nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := safeUninstallSeams(uninstallOptions(t.TempDir(), false, tc.euid), tc.installed)
			s.executable = func() (string, error) { return "", errors.New("no executable") }
			withUninstallSeams(t, s)

			a, stdout, stderr, _ := newDispatchApp("dev")
			a.deps.euid = func() int { return tc.euid }
			if got := a.run(context.Background(), []string{"uninstall"}); got != exitFailure {
				t.Fatalf("run(uninstall) = %d, want %d", got, exitFailure)
			}
			if want := "cannot locate the executable: no executable"; !strings.Contains(stderr.String(), want) {
				t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
			}
			if stdout.Len() != 0 {
				t.Errorf("stdout = %q, want empty", stdout.String())
			}
		})
	}
}

// TestUninstallConfirmReadFailure reports an unreadable answer to each
// confirmation question.
func TestUninstallConfirmReadFailure(t *testing.T) {
	cases := []struct {
		name      string
		installed func(service.Options) (bool, error)
		step      string
	}{
		{"data question", func(o service.Options) (bool, error) { return !o.System, nil }, "cannot confirm the data removal: cannot read the answer: read failed"},
		{"system question", func(o service.Options) (bool, error) { return o.System, nil }, "cannot confirm the system uninstall: cannot read the answer: read failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withUninstallSeams(t, safeUninstallSeams(uninstallOptions(t.TempDir(), false, 1000), tc.installed))

			a, stdout, stderr, _ := newDispatchApp("dev")
			a.deps.euid = func() int { return 1000 }
			withTerminal(t)
			a.stdin = errorReader{err: errors.New("read failed")}
			if got := a.run(context.Background(), []string{"uninstall"}); got != exitFailure {
				t.Fatalf("run(uninstall) = %d, want %d", got, exitFailure)
			}
			if want := tc.step; !strings.Contains(stderr.String(), want) {
				t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
			}
			if stdout.Len() != 0 {
				t.Errorf("stdout = %q, want empty", stdout.String())
			}
		})
	}
}

// TestUninstallSudoFailure reports a failed executable lookup or sudo run on the
// system uninstall path.
func TestUninstallSudoFailure(t *testing.T) {
	cases := []struct {
		name string
		step string
		set  func(*uninstallSeams)
	}{
		{
			name: "executable lookup",
			step: "cannot locate the executable: no executable",
			set: func(s *uninstallSeams) {
				s.executable = func() (string, error) { return "", errors.New("no executable") }
			},
		},
		{
			name: "sudo run",
			step: "cannot run sudo: sudo denied",
			set: func(s *uninstallSeams) {
				s.sudo = func(context.Context, string, []string, io.Reader, io.Writer, io.Writer) error {
					return errors.New("sudo denied")
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := systemLevelSeams(uninstallOptions(t.TempDir(), false, 1000))
			tc.set(&s)
			withUninstallSeams(t, s)

			a, _, stderr, _ := newDispatchApp("dev")
			a.deps.euid = func() int { return 1000 }
			if got := a.run(context.Background(), []string{"uninstall", "--yes"}); got != exitFailure {
				t.Fatalf("run(uninstall --yes) = %d, want %d", got, exitFailure)
			}
			if !strings.Contains(stderr.String(), tc.step) {
				t.Errorf("stderr = %q, want it to contain %q", stderr.String(), tc.step)
			}
		})
	}
}

// fakeSudo writes a sudo stand-in that exits with FAKE_SUDO_EXIT.
func fakeSudo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	script := "#!/bin/sh\nprintf '%s\\n' \"$*\"\nexit ${FAKE_SUDO_EXIT:-0}\n"
	if err := os.WriteFile(filepath.Join(dir, "sudo"), []byte(script), 0o755); err != nil {
		t.Fatalf("writing the sudo stand-in: %v", err)
	}
	return dir
}

// TestRunSudoUninstall runs sudo with the executable and arguments, returns its
// exit code and reports a missing sudo.
func TestRunSudoUninstall(t *testing.T) {
	exe := "/usr/local/bin/peers"
	args := []string{"uninstall", "--purge", "--yes"}

	t.Run("runs sudo with the executable and arguments", func(t *testing.T) {
		t.Setenv("PATH", fakeSudo(t))
		var stdout, stderr strings.Builder
		if err := runSudoUninstall(context.Background(), exe, args, strings.NewReader(""), &stdout, &stderr); err != nil {
			t.Fatalf("runSudoUninstall: %v", err)
		}
		if want := exe + " uninstall --purge --yes\n"; stdout.String() != want {
			t.Errorf("stdout = %q, want %q", stdout.String(), want)
		}
		if stderr.Len() != 0 {
			t.Errorf("stderr = %q, want empty", stderr.String())
		}
	})

	t.Run("returns the child exit code", func(t *testing.T) {
		t.Setenv("PATH", fakeSudo(t))
		t.Setenv("FAKE_SUDO_EXIT", "3")
		err := runSudoUninstall(context.Background(), exe, args, strings.NewReader(""), io.Discard, io.Discard)
		var exit *exitError
		if !errors.As(err, &exit) {
			t.Fatalf("runSudoUninstall error = %v, want an exitError", err)
		}
		if exit.code != 3 {
			t.Errorf("exit code = %d, want 3", exit.code)
		}
	})

	t.Run("reports a missing sudo without an exit code", func(t *testing.T) {
		t.Setenv("PATH", t.TempDir())
		err := runSudoUninstall(context.Background(), exe, args, strings.NewReader(""), io.Discard, io.Discard)
		if err == nil {
			t.Fatal("runSudoUninstall succeeded without sudo")
		}
		var exit *exitError
		if errors.As(err, &exit) {
			t.Errorf("error = %v, want the exec failure rather than an exit code", err)
		}
		var execErr *exec.Error
		if !errors.As(err, &execErr) {
			t.Errorf("error = %v, want an exec.Error from the lookup", err)
		}
	})
}

// TestUninstallSystemSkipsTheRegistrationOfUnknownAccounts warns instead of
// removing MCP registrations when no account is known.
func TestUninstallSystemSkipsTheRegistrationOfUnknownAccounts(t *testing.T) {
	s := systemLevelSeams(uninstallOptions(t.TempDir(), true, 0))
	s.getenv = func(key string) string {
		if key == "SUDO_USER" {
			return "root"
		}
		return ""
	}
	s.serviceBinary = func(service.Options) (string, bool, error) {
		return "/usr/local/bin/peers", true, nil
	}
	s.removeMCP = func(string, string) (string, error) {
		t.Error("removed an MCP registration although no account is known")
		return "", nil
	}
	withUninstallSeams(t, s)

	a, stdout, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 0 }
	if got := a.run(context.Background(), []string{"uninstall", "--yes"}); got != exitOK {
		t.Fatalf("run(uninstall --yes) = %d, want %d (stderr %q)", got, exitOK, stderr)
	}
	if want := "skipped the peer MCP registration; run claude mcp remove -s user peer in each account"; !strings.Contains(stdout.String(), want) {
		t.Errorf("stdout = %q, want it to contain %q", stdout.String(), want)
	}
}

// TestUninstallServiceBinaryInspectionFailure reports an unreadable service
// definition.
func TestUninstallServiceBinaryInspectionFailure(t *testing.T) {
	s := userLevelSeams(uninstallOptions(t.TempDir(), false, 1000))
	s.serviceBinary = func(service.Options) (string, bool, error) {
		return "", false, errors.New("unreadable unit")
	}
	withUninstallSeams(t, s)

	a, _, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 1000 }
	if got := a.run(context.Background(), []string{"uninstall", "--yes"}); got != exitFailure {
		t.Fatalf("run(uninstall --yes) = %d, want %d", got, exitFailure)
	}
	if want := "cannot read the service definition: unreadable unit"; !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
	}
}

// TestUninstallLevelExecutableLookupFailure reports a failed executable lookup
// during the per-level removal.
func TestUninstallLevelExecutableLookupFailure(t *testing.T) {
	s := userLevelSeams(uninstallOptions(t.TempDir(), false, 1000))
	s.executable = func() (string, error) { return "", errors.New("no executable") }
	withUninstallSeams(t, s)

	a, _, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 1000 }
	if got := a.run(context.Background(), []string{"uninstall", "--yes"}); got != exitFailure {
		t.Fatalf("run(uninstall --yes) = %d, want %d", got, exitFailure)
	}
	if want := "cannot locate the executable: no executable"; !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
	}
}

// TestUninstallLevelFindsTheInstallationBinary resolves the installed binary,
// removes it and passes it to the MCP removal.
func TestUninstallLevelFindsTheInstallationBinary(t *testing.T) {
	root := t.TempDir()
	exe := "/home/alice/.local/bin/peers"
	full := filepath.Join(root, exe)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	var gotBinary string
	s := userLevelSeams(uninstallOptions(root, false, 1000))
	s.executable = func() (string, error) { return full, nil }
	s.evalSymlinks = func(string) (string, error) { return exe, nil }
	s.mcpLookup = func(string) mcpRegistration { return mcpRegistration{command: full} }
	s.removeMCP = func(_ string, binary string) (string, error) {
		gotBinary = binary
		return "removed", nil
	}
	s.removeFile = func(path string) error { return os.Remove(path) }
	withUninstallSeams(t, s)

	a, stdout, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 1000 }
	if got := a.run(context.Background(), []string{"uninstall", "--yes"}); got != exitOK {
		t.Fatalf("run(uninstall --yes) = %d, want %d (stderr %q)", got, exitOK, stderr)
	}
	if gotBinary != exe {
		t.Errorf("MCP removal binary = %q, want the installation binary %q", gotBinary, exe)
	}
	if !strings.Contains(stdout.String(), "removed the binary "+exe) {
		t.Errorf("stdout = %q, want the binary removal", stdout.String())
	}
	if _, err := os.Stat(full); !os.IsNotExist(err) {
		t.Errorf("binary still exists: %v", err)
	}
}

// TestUninstallMCPRemovalFailure reports a failed MCP registration removal.
func TestUninstallMCPRemovalFailure(t *testing.T) {
	s := userLevelSeams(uninstallOptions(t.TempDir(), false, 1000))
	s.serviceBinary = func(service.Options) (string, bool, error) {
		return "/home/alice/bin/peers", true, nil
	}
	s.removeMCP = func(string, string) (string, error) { return "", errors.New("claude failed") }
	withUninstallSeams(t, s)

	a, _, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 1000 }
	if got := a.run(context.Background(), []string{"uninstall", "--yes"}); got != exitFailure {
		t.Fatalf("run(uninstall --yes) = %d, want %d", got, exitFailure)
	}
	if want := "cannot remove the MCP registration: claude failed"; !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
	}
}

// TestUninstallCompletionRemovalFailure reports an uninspectable completion file
// and names it.
func TestUninstallCompletionRemovalFailure(t *testing.T) {
	root := t.TempDir()
	// The completions directory cannot exist because its parent is a file, so
	// inspecting the completion file fails rather than reporting it absent.
	writeUnderRoot(t, root, "/home/alice/.local/share/bash-completion")
	s := userLevelSeams(uninstallOptions(root, false, 1000))
	withUninstallSeams(t, s)

	a, _, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 1000 }
	if got := a.run(context.Background(), []string{"uninstall", "--yes"}); got != exitFailure {
		t.Fatalf("run(uninstall --yes) = %d, want %d", got, exitFailure)
	}
	errOut := stderr.String()
	if want := "cannot remove the shell completions and man pages: cannot inspect "; !strings.Contains(errOut, want) {
		t.Errorf("stderr = %q, want it to contain %q", errOut, want)
	}
	if want := "completions/peers"; !strings.Contains(errOut, want) {
		t.Errorf("stderr = %q, want it to name the completion file (%q)", errOut, want)
	}
}

// TestUninstallRuntimeDirFailure reports a failed runtime directory removal.
func TestUninstallRuntimeDirFailure(t *testing.T) {
	s := userLevelSeams(uninstallOptions(t.TempDir(), false, 1000))
	s.removeRuntime = func(service.Options) ([]string, error) { return nil, errors.New("busy") }
	withUninstallSeams(t, s)

	a, _, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 1000 }
	if got := a.run(context.Background(), []string{"uninstall", "--yes"}); got != exitFailure {
		t.Fatalf("run(uninstall --yes) = %d, want %d", got, exitFailure)
	}
	if want := "cannot remove the runtime directory: busy"; !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
	}
}

// TestUninstallOtherAccountsFailure reports a failed account listing on a
// system uninstall.
func TestUninstallOtherAccountsFailure(t *testing.T) {
	s := systemLevelSeams(uninstallOptions(t.TempDir(), true, 0))
	s.serviceBinary = func(service.Options) (string, bool, error) {
		return "/usr/local/bin/peers", true, nil
	}
	s.otherAccounts = func(service.Options) ([]string, error) { return nil, errors.New("no account database") }
	withUninstallSeams(t, s)

	a, _, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 0 }
	if got := a.run(context.Background(), []string{"uninstall", "--yes"}); got != exitFailure {
		t.Fatalf("run(uninstall --yes) = %d, want %d", got, exitFailure)
	}
	if want := "cannot list other accounts' Brokers: no account database"; !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
	}
}

// TestUninstallOtherAccountsWithoutKnownBinary warns about other accounts'
// Brokers when the binary is unknown.
func TestUninstallOtherAccountsWithoutKnownBinary(t *testing.T) {
	s := systemLevelSeams(uninstallOptions(t.TempDir(), true, 0))
	s.otherAccounts = func(service.Options) ([]string, error) { return []string{"bob"}, nil }
	withUninstallSeams(t, s)

	a, stdout, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 0 }
	if got := a.run(context.Background(), []string{"uninstall", "--yes"}); got != exitOK {
		t.Fatalf("run(uninstall --yes) = %d, want %d (stderr %q)", got, exitOK, stderr)
	}
	out := stdout.String()
	if want := "Other accounts run a user Broker: bob."; !strings.Contains(out, want) {
		t.Errorf("stdout = %q, want it to contain %q", out, want)
	}
	if want := "Their Brokers may fail to start if the shared binary is removed."; !strings.Contains(out, want) {
		t.Errorf("stdout = %q, want the unknown-binary warning %q", out, want)
	}
	if strings.Contains(out, "removed the binary") {
		t.Errorf("stdout = %q, want no binary removal without a known binary", out)
	}
}

// TestUninstallBinaryRemovalFailure reports a failed binary removal.
func TestUninstallBinaryRemovalFailure(t *testing.T) {
	root := t.TempDir()
	binary := "/usr/local/bin/peers"
	full := filepath.Join(root, binary)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	s := systemLevelSeams(uninstallOptions(root, true, 0))
	s.serviceBinary = func(service.Options) (string, bool, error) {
		return binary, true, nil
	}
	s.executable = func() (string, error) { return full, nil }
	s.removeFile = func(string) error { return os.ErrPermission }
	withUninstallSeams(t, s)

	a, _, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 0 }
	if got := a.run(context.Background(), []string{"uninstall", "--yes"}); got != exitFailure {
		t.Fatalf("run(uninstall --yes) = %d, want %d", got, exitFailure)
	}
	if want := "cannot remove the binary: cannot remove " + binary + ": permission denied"; !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
	}
}

// TestUninstallKeepsACompletionDirectory leaves a completion directory in place
// and reports it.
func TestUninstallKeepsACompletionDirectory(t *testing.T) {
	root := t.TempDir()
	dir := "/home/alice/.local/share/bash-completion/completions/peers"
	if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
		t.Fatal(err)
	}
	withUninstallSeams(t, userLevelSeams(uninstallOptions(root, false, 1000)))

	a, stdout, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 1000 }
	if got := a.run(context.Background(), []string{"uninstall", "--yes"}); got != exitOK {
		t.Fatalf("run(uninstall --yes) = %d, want %d (stderr %q)", got, exitOK, stderr)
	}
	if want := "kept " + filepath.Join(root, dir) + ": it is a directory"; !strings.Contains(stdout.String(), want) {
		t.Errorf("stdout = %q, want it to contain %q", stdout.String(), want)
	}
	if _, err := os.Stat(filepath.Join(root, dir)); err != nil {
		t.Errorf("completion directory was removed: %v", err)
	}
}

// TestUninstallCompletionReadFailure reports an unreadable completion file.
func TestUninstallCompletionReadFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads a mode-000 file, so the read failure cannot be reproduced as root")
	}
	root := t.TempDir()
	path := "/home/alice/.local/share/bash-completion/completions/peers"
	writeUnderRootWith(t, root, path, completionMarker(path))
	full := filepath.Join(root, path)
	if err := os.Chmod(full, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(full, 0o644) })
	withUninstallSeams(t, userLevelSeams(uninstallOptions(root, false, 1000)))

	a, _, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 1000 }
	if got := a.run(context.Background(), []string{"uninstall", "--yes"}); got != exitFailure {
		t.Fatalf("run(uninstall --yes) = %d, want %d", got, exitFailure)
	}
	if want := "cannot read " + full; !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want it to name the unreadable file (%q)", stderr.String(), want)
	}
	if !strings.Contains(stderr.String(), "permission denied") {
		t.Errorf("stderr = %q, want the read failure", stderr.String())
	}
}

// TestUninstallCompletionDanglingSymlink removes the real completion file and
// leaves a dangling symlink unreported.
func TestUninstallCompletionDanglingSymlink(t *testing.T) {
	root := t.TempDir()
	bash := "/home/alice/.local/share/bash-completion/completions/peers"
	fish := "/home/alice/.config/fish/completions/peers.fish"
	bashFull := filepath.Join(root, bash)
	if err := os.MkdirAll(filepath.Dir(bashFull), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "gone"), bashFull); err != nil {
		t.Fatal(err)
	}
	writeUnderRootWith(t, root, fish, completionMarker(fish))
	withUninstallSeams(t, userLevelSeams(uninstallOptions(root, false, 1000)))

	a, stdout, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 1000 }
	if got := a.run(context.Background(), []string{"uninstall", "--yes"}); got != exitOK {
		t.Fatalf("run(uninstall --yes) = %d, want %d (stderr %q)", got, exitOK, stderr)
	}
	out := stdout.String()
	if want := "removed " + filepath.Join(root, fish); !strings.Contains(out, want) {
		t.Errorf("stdout = %q, want the real completion removed (%q)", out, want)
	}
	if _, err := os.Stat(filepath.Join(root, fish)); !os.IsNotExist(err) {
		t.Errorf("the real completion file still exists: %v", err)
	}
	if strings.Contains(out, bashFull) {
		t.Errorf("stdout = %q, want the dangling symlink left unreported", out)
	}
	if _, err := os.Lstat(bashFull); err != nil {
		t.Errorf("the dangling symlink was removed: %v", err)
	}
}

// TestUninstallManPageRemovalFailure reports a failed man page removal.
func TestUninstallManPageRemovalFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permissions, so the removal failure cannot be reproduced as root")
	}
	root := t.TempDir()
	manDir := filepath.Join(root, "/usr/local/share/man/man1")
	if err := os.MkdirAll(manDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(manDir, "peers.1"), []byte("page\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(manDir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(manDir, 0o755) })
	s := systemLevelSeams(uninstallOptions(root, true, 0))
	s.serviceBinary = func(service.Options) (string, bool, error) {
		return "/usr/local/bin/peers", true, nil
	}
	withUninstallSeams(t, s)

	a, _, stderr, _ := newDispatchApp("dev")
	a.deps.euid = func() int { return 0 }
	if got := a.run(context.Background(), []string{"uninstall", "--yes"}); got != exitFailure {
		t.Fatalf("run(uninstall --yes) = %d, want %d", got, exitFailure)
	}
	if want := "cannot remove the shell completions and man pages: cannot remove " + filepath.Join(manDir, "peers.1"); !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
	}
}

// TestRemoveServiceBinaryInspectionFailure reports an uninspectable service
// binary path.
func TestRemoveServiceBinaryInspectionFailure(t *testing.T) {
	root := t.TempDir()
	// /usr/local is a file, so inspecting its child fails with ENOTDIR rather
	// than reporting the binary absent.
	writeUnderRoot(t, root, "/usr/local")
	a, _, _, _ := newDispatchApp("dev")

	err := a.removeServiceBinary(uninstallOptions(root, true, 0), true, "/usr/local/bin/peers")
	if err == nil {
		t.Fatal("removeServiceBinary succeeded for an uninspectable path")
	}
	if want := "cannot inspect /usr/local/bin/peers"; !strings.Contains(err.Error(), want) {
		t.Errorf("error = %q, want it to contain %q", err, want)
	}
}

// TestRemoveServiceBinaryOwnerLookupFailure reports a failed owner lookup.
func TestRemoveServiceBinaryOwnerLookupFailure(t *testing.T) {
	root := t.TempDir()
	binary := "/home/alice/bin/peers"
	full := filepath.Join(root, binary)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	withUninstallSeams(t, uninstallSeams{
		executable: func() (string, error) { return full, nil },
		fileOwner:  func(string) (uint32, error) { return 0, os.ErrPermission },
	})
	a, _, _, _ := newDispatchApp("dev")

	err := a.removeServiceBinary(uninstallOptions(root, false, 1000), false, binary)
	if err == nil {
		t.Fatal("removeServiceBinary succeeded although the owner is unknown")
	}
	if want := "cannot inspect " + binary + ": permission denied"; !strings.Contains(err.Error(), want) {
		t.Errorf("error = %q, want it to contain %q", err, want)
	}
}

// TestRemoveServiceBinaryRemovalFailure reports a failed removal.
func TestRemoveServiceBinaryRemovalFailure(t *testing.T) {
	root := t.TempDir()
	binary := "/home/alice/bin/peers"
	full := filepath.Join(root, binary)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	withUninstallSeams(t, uninstallSeams{
		executable: func() (string, error) { return full, nil },
		fileOwner:  func(string) (uint32, error) { return 1000, nil },
		removeFile: func(string) error { return os.ErrPermission },
	})
	a, _, _, _ := newDispatchApp("dev")

	err := a.removeServiceBinary(uninstallOptions(root, false, 1000), false, binary)
	if err == nil {
		t.Fatal("removeServiceBinary succeeded although the removal failed")
	}
	if want := "cannot remove " + binary + ": permission denied"; !strings.Contains(err.Error(), want) {
		t.Errorf("error = %q, want it to contain %q", err, want)
	}
}

// TestRemoveServiceBinaryRejectsANonAbsolutePath keeps a relative binary path and
// prints the manual removal command.
func TestRemoveServiceBinaryRejectsANonAbsolutePath(t *testing.T) {
	root := t.TempDir()
	writeUnderRoot(t, root, "/peers")
	a, stdout, _, _ := newDispatchApp("dev")

	if err := a.removeServiceBinary(uninstallOptions(root, true, 0), true, "peers"); err != nil {
		t.Fatalf("removeServiceBinary: %v", err)
	}
	want := "kept the binary peers: it is not an absolute path; remove it manually with: rm peers"
	if !strings.Contains(stdout.String(), want) {
		t.Errorf("stdout = %q, want it to contain %q", stdout.String(), want)
	}
	if _, err := os.Stat(filepath.Join(root, "/peers")); err != nil {
		t.Errorf("the file was removed: %v", err)
	}
}

// TestRemoveServiceBinaryRejectsAnUninspectableFile keeps a binary that cannot
// be inspected.
func TestRemoveServiceBinaryRejectsAnUninspectableFile(t *testing.T) {
	root := t.TempDir()
	binary := "/usr/local/bin/peers"
	full := filepath.Join(root, binary)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "gone"), full); err != nil {
		t.Fatal(err)
	}
	a, stdout, _, _ := newDispatchApp("dev")

	if err := a.removeServiceBinary(uninstallOptions(root, true, 0), true, binary); err != nil {
		t.Fatalf("removeServiceBinary: %v", err)
	}
	if want := "kept the binary " + binary + ": it could not be inspected:"; !strings.Contains(stdout.String(), want) {
		t.Errorf("stdout = %q, want it to contain %q", stdout.String(), want)
	}
}

// TestKeptPaths lists the paths an uninstall without a purge keeps at each level
// and platform.
func TestKeptPaths(t *testing.T) {
	cases := []struct {
		name string
		opts service.Options
		want []string
	}{
		{
			name: "system linux",
			opts: service.Options{System: true, GOOS: "linux"},
			want: []string{"/etc/peers", "/var/lib/peers", "/var/log/peers/audit.log"},
		},
		{
			name: "system darwin",
			opts: service.Options{System: true, GOOS: "darwin"},
			want: []string{"/etc/peers", "/var/lib/peers", "/var/log/peers"},
		},
		{
			name: "user linux",
			opts: service.Options{Home: "/home/alice", GOOS: "linux"},
			want: []string{"/home/alice/.peers"},
		},
		{
			name: "user darwin",
			opts: service.Options{Home: "/home/alice", GOOS: "darwin"},
			want: []string{"/home/alice/.peers", "/home/alice/Library/Logs/peers"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := keptPaths(tc.opts); !slices.Equal(got, tc.want) {
				t.Errorf("keptPaths(%+v) = %q, want %q", tc.opts, got, tc.want)
			}
		})
	}
}

// TestRootedPathWithoutAPrefix joins the root prefix only when one is set.
func TestRootedPathWithoutAPrefix(t *testing.T) {
	if got := rootedPath(service.Options{}, "/etc/peers"); got != "/etc/peers" {
		t.Errorf("rootedPath with no root = %q, want /etc/peers", got)
	}
	if got := rootedPath(service.Options{Root: "/root"}, "/etc/peers"); got != "/root/etc/peers" {
		t.Errorf("rootedPath with a root = %q, want /root/etc/peers", got)
	}
}

// TestResolvePathFallsBack returns the input when symlink resolution fails and
// the target when it succeeds.
func TestResolvePathFallsBack(t *testing.T) {
	previous := uninstallEvalSymlinksFn
	t.Cleanup(func() { uninstallEvalSymlinksFn = previous })

	uninstallEvalSymlinksFn = func(string) (string, error) { return "", errors.New("broken link") }
	if got := resolvePath("/x/peers"); got != "/x/peers" {
		t.Errorf("resolvePath with a failing lookup = %q, want the input", got)
	}
	uninstallEvalSymlinksFn = func(string) (string, error) { return "/real/peers", nil }
	if got := resolvePath("/x/peers"); got != "/real/peers" {
		t.Errorf("resolvePath = %q, want the resolved path", got)
	}
}

// TestFileOwner reports the file's owner and os.ErrNotExist for a missing file.
func TestFileOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers")
	if err := os.WriteFile(path, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	uid, err := fileOwner(path)
	if err != nil {
		t.Fatalf("fileOwner: %v", err)
	}
	if int(uid) != os.Getuid() {
		t.Errorf("fileOwner = %d, want the file's owner %d", uid, os.Getuid())
	}
	if _, err := fileOwner(filepath.Join(t.TempDir(), "missing")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("fileOwner of a missing file = %v, want os.ErrNotExist", err)
	}
}

// TestUninstallMCPDefaultsRunClaudeFromPath checks that the production MCP
// wiring reads the registration and removes a matching one by running claude on
// PATH.
func TestUninstallMCPDefaultsRunClaudeFromPath(t *testing.T) {
	dir := t.TempDir()
	script := "#!/bin/sh\n" +
		"case \"$*\" in\n" +
		"  *\"mcp get peer\"*) echo \"Command: /opt/peers/bin/peers\" ;;\n" +
		"  *\"mcp remove\"*) exit 0 ;;\n" +
		"  *) exit 1 ;;\n" +
		"esac\n"
	if err := os.WriteFile(filepath.Join(dir, "claude"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)

	reg := defaultLookupPeerMCP("")
	if reg.command != "/opt/peers/bin/peers" {
		t.Fatalf("defaultLookupPeerMCP command = %q, want the registered command", reg.command)
	}
	outcome, err := defaultRemoveMCPRegistration("", "/opt/peers/bin/peers")
	if err != nil {
		t.Fatalf("defaultRemoveMCPRegistration: %v", err)
	}
	if want := "removed the peer MCP registration"; outcome != want {
		t.Errorf("outcome = %q, want %q", outcome, want)
	}
}

// TestUninstallSystemInstalledCheck reports an installation from the unit file
// under the root and rejects a missing one.
func TestUninstallSystemInstalledCheck(t *testing.T) {
	root := t.TempDir()
	opts := service.Options{
		System: true,
		GOOS:   "linux",
		Root:   root,
		Run: func(string, ...string) ([]byte, error) {
			return nil, errors.New("no service manager")
		},
	}
	writeUnderRoot(t, root, "/etc/systemd/system/peers.service")
	if !uninstallSystemInstalledCheck(opts) {
		t.Error("the unit file present under the root was not reported as installed")
	}
	if err := os.Remove(filepath.Join(root, "/etc/systemd/system/peers.service")); err != nil {
		t.Fatal(err)
	}
	if uninstallSystemInstalledCheck(opts) {
		t.Error("a missing unit and a failing service manager were reported as installed")
	}
}
