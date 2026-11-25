package service

import (
	"encoding/xml"
	"errors"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/procsig"
)

// TestPostUpdateDarwinFailsWithoutTheDomainBeforeWriting checks that a missing
// user launchd domain stops the update before the plist is written: an
// interactive session that never ran must not leave a half-applied definition
// behind and must not start the deferred helper.
func TestPostUpdateDarwinFailsWithoutTheDomainBeforeWriting(t *testing.T) {
	opts, _ := darwinPostUpdateOptions(t)
	opts.Run = func(string, ...string) ([]byte, error) {
		return nil, exitError(t, 113)
	}
	started := false
	opts.StartDetached = func(*exec.Cmd) error {
		started = true
		return nil
	}

	err := PostUpdate(opts)
	if err == nil {
		t.Fatal("PostUpdate succeeded without a launchd domain")
	}
	if !strings.Contains(err.Error(), "launchd has no gui/501 domain") {
		t.Errorf("error = %q, want it to name the missing launchd domain", err)
	}
	plistPath := rooted(opts, filepath.Join("/Users/alice/Library/LaunchAgents", Label+".plist"))
	if _, statErr := os.Stat(plistPath); !errors.Is(statErr, fs.ErrNotExist) {
		t.Errorf("the plist was written before the domain check: %v", statErr)
	}
	if started {
		t.Error("PostUpdate started the helper without a launchd domain")
	}
}

// TestPostUpdateReportsAnUnreadableUnit checks that a unit path that cannot be
// read as the old definition stops the update before the unit database is
// reloaded.
func TestPostUpdateReportsAnUnreadableUnit(t *testing.T) {
	opts := baseOptions(t)
	path := filepath.Join("/home/alice/.config/systemd/user", UnitName)
	if err := os.MkdirAll(rooted(opts, path), 0o755); err != nil {
		t.Fatalf("create a directory at the unit path: %v", err)
	}
	var cmds []string
	opts.Run = recordCommands(&cmds)

	err := PostUpdate(opts)
	if err == nil {
		t.Fatal("PostUpdate succeeded although the old unit could not be read")
	}
	for _, want := range []string{"cannot read " + path, "before rewriting it"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want it to mention %q", err, want)
		}
	}
	if len(cmds) != 0 {
		t.Errorf("commands = %q, want no daemon-reload after an unreadable unit", cmds)
	}
}

// TestSaveUnitReportsAnInspectFailure checks that a unit path under a
// non-directory is reported as an inspection failure.
func TestSaveUnitReportsAnInspectFailure(t *testing.T) {
	opts := baseOptions(t)
	parent := rooted(opts, "/home/alice/.config")
	if err := os.MkdirAll(filepath.Dir(parent), 0o755); err != nil {
		t.Fatalf("create the home directory: %v", err)
	}
	if err := os.WriteFile(parent, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("write a file where a directory is expected: %v", err)
	}
	path := filepath.Join("/home/alice/.config/systemd/user", UnitName)

	_, err := saveUnit(opts, path)
	if err == nil {
		t.Fatal("saveUnit succeeded although the unit path cannot be inspected")
	}
	for _, want := range []string{"cannot inspect " + path, "not a directory"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want it to mention %q", err, want)
		}
	}
}

// TestSaveAndRestoreMainUnit checks the old binary's undo path: the saved
// content and mode come back exactly, a unit this run created is removed
// instead, and Linux reloads the unit database while macOS needs no command.
func TestSaveAndRestoreMainUnit(t *testing.T) {
	t.Run("linux keeps content and mode", func(t *testing.T) {
		opts := baseOptions(t)
		path := filepath.Join("/home/alice/.config/systemd/user", UnitName)
		fsPath := writeRootedFile(t, opts, path, "old unit\n", 0o640)

		saved, err := SaveMainUnit(opts)
		if err != nil {
			t.Fatalf("SaveMainUnit: %v", err)
		}
		if !saved.Exists {
			t.Fatal("SavedUnit.Exists = false, want the old unit")
		}
		if got := string(saved.Content); got != "old unit\n" {
			t.Errorf("SavedUnit.Content = %q, want %q", got, "old unit\n")
		}
		if got := saved.Mode.Perm(); got != 0o640 {
			t.Errorf("SavedUnit.Mode = %#o, want 0640", got)
		}

		if err := os.WriteFile(fsPath, []byte("next unit\n"), 0o600); err != nil {
			t.Fatalf("write the next definition: %v", err)
		}
		var cmds []string
		opts.Run = recordCommands(&cmds)
		if err := RestoreMainUnit(opts, saved); err != nil {
			t.Fatalf("RestoreMainUnit: %v", err)
		}
		if got := mustReadFile(t, fsPath); got != "old unit\n" {
			t.Errorf("restored content = %q, want %q", got, "old unit\n")
		}
		info, err := os.Stat(fsPath)
		if err != nil {
			t.Fatalf("stat the restored unit: %v", err)
		}
		if got := info.Mode().Perm(); got != 0o640 {
			t.Errorf("restored mode = %#o, want 0640", got)
		}
		if want := []string{"systemctl --user daemon-reload"}; !slices.Equal(cmds, want) {
			t.Errorf("commands = %q, want %q", cmds, want)
		}
	})

	t.Run("missing unit is removed", func(t *testing.T) {
		opts := baseOptions(t)
		path := filepath.Join("/home/alice/.config/systemd/user", UnitName)

		saved, err := SaveMainUnit(opts)
		if err != nil {
			t.Fatalf("SaveMainUnit: %v", err)
		}
		if saved.Exists {
			t.Error("SavedUnit.Exists = true, want false for a missing unit")
		}
		fsPath := writeRootedFile(t, opts, path, "written by the update\n", 0o644)
		var cmds []string
		opts.Run = recordCommands(&cmds)
		if err := RestoreMainUnit(opts, saved); err != nil {
			t.Fatalf("RestoreMainUnit: %v", err)
		}
		if _, statErr := os.Stat(fsPath); !errors.Is(statErr, fs.ErrNotExist) {
			t.Errorf("the restored definition still exists: %v", statErr)
		}
		if want := []string{"systemctl --user daemon-reload"}; !slices.Equal(cmds, want) {
			t.Errorf("commands = %q, want %q", cmds, want)
		}
	})

	t.Run("darwin runs no command", func(t *testing.T) {
		opts, _ := darwinControlOptions(t)
		path := filepath.Join("/Users/alice/Library/LaunchAgents", Label+".plist")
		fsPath := writeRootedFile(t, opts, path, "old plist\n", 0o644)

		saved, err := SaveMainUnit(opts)
		if err != nil {
			t.Fatalf("SaveMainUnit: %v", err)
		}
		if err := os.WriteFile(fsPath, []byte("next plist\n"), 0o600); err != nil {
			t.Fatalf("write the next plist: %v", err)
		}
		var cmds []string
		opts.Run = recordCommands(&cmds)
		if err := RestoreMainUnit(opts, saved); err != nil {
			t.Fatalf("RestoreMainUnit: %v", err)
		}
		if got := mustReadFile(t, fsPath); got != "old plist\n" {
			t.Errorf("restored content = %q, want %q", got, "old plist\n")
		}
		if len(cmds) != 0 {
			t.Errorf("commands = %q, want none on macOS", cmds)
		}
	})
}

// TestSaveAndRestoreMainUnitReportTheirErrors checks the errors the exported
// helpers surface: a platform without a service definition and a unit that
// cannot be read both fail without returning a saved unit.
func TestSaveAndRestoreMainUnitReportTheirErrors(t *testing.T) {
	t.Run("unknown platform", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "windows"

		saved, err := SaveMainUnit(opts)
		if err == nil {
			t.Fatalf("SaveMainUnit = %+v, want an error", saved)
		}
		if !strings.Contains(err.Error(), `cannot find the unit path on platform "windows"`) {
			t.Errorf("error = %q, want it to name the unsupported platform", err)
		}
		if saved.Exists || saved.Content != nil {
			t.Errorf("SaveMainUnit = %+v on error, want the zero value", saved)
		}
		if err := RestoreMainUnit(opts, SavedUnit{}); err == nil {
			t.Fatal("RestoreMainUnit succeeded for a platform without a unit path")
		}
	})

	t.Run("unreadable unit", func(t *testing.T) {
		opts := baseOptions(t)
		path := filepath.Join("/home/alice/.config/systemd/user", UnitName)
		if err := os.MkdirAll(rooted(opts, path), 0o755); err != nil {
			t.Fatalf("create a directory at the unit path: %v", err)
		}

		saved, err := SaveMainUnit(opts)
		if err == nil {
			t.Fatalf("SaveMainUnit = %+v, want an error", saved)
		}
		if !strings.Contains(err.Error(), "cannot read "+path) {
			t.Errorf("error = %q, want it to name the unreadable unit", err)
		}
	})
}

// TestRestoreUnitReportsWriteAndRemoveFailures checks both restore failures:
// a definition that cannot be written back and one this run created that cannot
// be removed are each reported as a failed restore, and neither reloads the
// unit database.
func TestRestoreUnitReportsWriteAndRemoveFailures(t *testing.T) {
	cases := []struct {
		name  string
		saved savedUnit
	}{
		{name: "write", saved: savedUnit{exists: true, content: []byte("old unit\n"), mode: 0o644}},
		{name: "remove", saved: savedUnit{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := baseOptions(t)
			path := filepath.Join("/home/alice/.config/systemd/user", UnitName)
			dir := rooted(opts, path)
			if err := os.MkdirAll(filepath.Join(dir, "occupant"), 0o755); err != nil {
				t.Fatalf("create a directory at the unit path: %v", err)
			}
			var cmds []string
			opts.Run = recordCommands(&cmds)

			err := restoreUnit(opts, path, tc.saved)
			if err == nil {
				t.Fatal("restoreUnit succeeded although the path cannot be replaced")
			}
			if !strings.Contains(err.Error(), "the service definition could not be restored") {
				t.Errorf("error = %q, want it to name the failed restore", err)
			}
			if len(cmds) != 0 {
				t.Errorf("commands = %q, want no daemon-reload after a failed restore", cmds)
			}
		})
	}
}

// TestPostUpdateReportsARewriteThatCannotRender checks that a definition that
// cannot be rendered leaves no file behind and returns the render error.
func TestPostUpdateReportsARewriteThatCannotRender(t *testing.T) {
	opts := baseOptions(t)
	opts.Exe = "relative/peers"

	err := PostUpdate(opts)
	if err == nil {
		t.Fatal("PostUpdate succeeded with a relative executable path")
	}
	if !strings.Contains(err.Error(), "must be absolute") {
		t.Errorf("error = %q, want it to name the relative path", err)
	}
	path := rooted(opts, filepath.Join("/home/alice/.config/systemd/user", UnitName))
	if _, statErr := os.Stat(path); !errors.Is(statErr, fs.ErrNotExist) {
		t.Errorf("the failed rewrite left a unit file behind: %v", statErr)
	}
}

// TestStartLaunchdReloadReportsMissingSetup checks the checks that run before
// the helper is started: an executable that cannot be identified, a level with
// no log path, and a helper the system refuses to start are each reported.
func TestStartLaunchdReloadReportsMissingSetup(t *testing.T) {
	const plistPath = "/Library/LaunchDaemons/" + Label + ".plist"

	t.Run("executable is missing", func(t *testing.T) {
		opts, _ := darwinPostUpdateOptions(t)
		if err := os.Remove(rooted(opts, opts.Exe)); err != nil {
			t.Fatalf("remove the executable: %v", err)
		}
		opts.StartDetached = func(*exec.Cmd) error { return nil }

		err := startLaunchdReload(opts, "system", plistPath)
		if err == nil {
			t.Fatal("startLaunchdReload succeeded without the executable")
		}
		if !strings.Contains(err.Error(), "cannot identify the binary") {
			t.Errorf("error = %q, want it to name the missing executable", err)
		}
	})

	t.Run("platform has no log path", func(t *testing.T) {
		opts, _ := darwinPostUpdateOptions(t)
		opts.GOOS = "linux"
		opts.StartDetached = func(*exec.Cmd) error { return nil }

		err := startLaunchdReload(opts, "system", plistPath)
		if err == nil {
			t.Fatal("startLaunchdReload succeeded on a platform without a log path")
		}
		if !strings.Contains(err.Error(), `cannot find a launchd post-update log on "linux"`) {
			t.Errorf("error = %q, want it to name the unsupported platform", err)
		}
	})

	t.Run("helper cannot start", func(t *testing.T) {
		opts, _ := darwinPostUpdateOptions(t)
		opts.System = true
		opts.EUID = 0
		opts.Owner = ownLogDirAs(opts)
		// The identity is read under the test root, but exec runs the real path.
		// A temporary path that holds no file makes Start fail without naming a
		// host binary that this machine might actually run.
		opts.Exe = filepath.Join(t.TempDir(), "peers")
		writeRootedFile(t, opts, opts.Exe, "not executable", 0o755)
		if err := os.MkdirAll(rooted(opts, systemLogDir), 0o755); err != nil {
			t.Fatalf("create the log directory: %v", err)
		}
		opts.StartDetached = nil

		err := startLaunchdReload(opts, "system", plistPath)
		if err == nil {
			t.Fatal("startLaunchdReload succeeded although the helper cannot start")
		}
		if !strings.Contains(err.Error(), "cannot start the post-update helper") {
			t.Errorf("error = %q, want it to name the failed start", err)
		}
		logPath := rooted(opts, filepath.Join(systemLogDir, LaunchdHelperLogName))
		if _, statErr := os.Stat(logPath); statErr != nil {
			t.Errorf("the helper log %s was not created before the start: %v", logPath, statErr)
		}
	})
}

// TestCheckLogDirReportsWhatItRejects checks each reason the post-update log
// directory is refused: an owner lookup that is missing or fails, an entry that
// is not a directory, and a directory with the wrong permissions.
func TestCheckLogDirReportsWhatItRejects(t *testing.T) {
	sentinel := errors.New("owner lookup failed")
	cases := []struct {
		name  string
		owner func(string) (uint32, fs.FileMode, error)
		want  string
	}{
		{
			name: "no owner function",
			want: "cannot inspect the post-update log directory /var/log/peers",
		},
		{
			name:  "owner lookup fails",
			owner: func(string) (uint32, fs.FileMode, error) { return 0, 0, sentinel },
			want:  "owner lookup failed",
		},
		{
			name:  "not a directory",
			owner: func(string) (uint32, fs.FileMode, error) { return 0, 0o600, nil },
			want:  "it is not a directory",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := baseOptions(t)
			opts.Owner = tc.owner

			err := checkLogDir(opts, "/var/log/peers")
			if err == nil {
				t.Fatal("checkLogDir accepted the directory")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to mention %q", err, tc.want)
			}
		})
	}

	t.Run("accepts the account's own directory", func(t *testing.T) {
		opts := baseOptions(t)
		opts.Owner = func(string) (uint32, fs.FileMode, error) {
			return 1000, fs.ModeDir | 0o750, nil
		}
		if err := checkLogDir(opts, "/var/log/peers"); err != nil {
			t.Errorf("checkLogDir: %v", err)
		}
	})
}

// TestLaunchdHelperLogPath checks the path lookup for both levels and each
// error: a platform without launchd and a user installation without a home.
func TestLaunchdHelperLogPath(t *testing.T) {
	cases := []struct {
		name    string
		opts    Options
		want    string
		wantErr string
	}{
		{
			name:    "not darwin",
			opts:    Options{GOOS: "linux"},
			wantErr: `cannot find a launchd post-update log on "linux"`,
		},
		{
			name: "system",
			opts: Options{GOOS: "darwin", System: true},
			want: filepath.Join(systemLogDir, LaunchdHelperLogName),
		},
		{
			name:    "user without home",
			opts:    Options{GOOS: "darwin"},
			wantErr: "needs a home directory",
		},
		{
			name: "user",
			opts: Options{GOOS: "darwin", Home: "/Users/alice"},
			want: filepath.Join("/Users/alice", "Library", "Logs", "peers", LaunchdHelperLogName),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := launchdHelperLogPath(tc.opts)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("launchdHelperLogPath = %q, want an error", got)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("error = %q, want it to mention %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("launchdHelperLogPath: %v", err)
			}
			if got != tc.want {
				t.Errorf("path = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestRelaunchOptions checks the options the detached helper runs with: the
// level it was asked for, darwin, the current account, a command runner that
// returns the command's combined output, and a logger that writes to standard
// error.
func TestRelaunchOptions(t *testing.T) {
	opts := RelaunchOptions(true)
	if !opts.System {
		t.Error("System = false, want the system installation")
	}
	if opts.GOOS != "darwin" {
		t.Errorf("GOOS = %q, want darwin", opts.GOOS)
	}
	if got, want := opts.EUID, os.Geteuid(); got != want {
		t.Errorf("EUID = %d, want %d", got, want)
	}
	if got, want := opts.UID, os.Getuid(); got != want {
		t.Errorf("UID = %d, want %d", got, want)
	}
	if opts.Run == nil {
		t.Fatal("Run is nil")
	}
	if out, err := opts.Run("sh", "-c", "printf marker"); err != nil || string(out) != "marker" {
		t.Errorf("Run = %q, %v, want %q and no error", out, err, "marker")
	}

	if opts.Logf == nil {
		t.Fatal("Logf is nil")
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("create a pipe: %v", err)
	}
	old := os.Stderr
	os.Stderr = w
	t.Cleanup(func() { os.Stderr = old })
	opts.Logf("helper %s", "line")
	os.Stderr = old
	if err := w.Close(); err != nil {
		t.Fatalf("close the pipe writer: %v", err)
	}
	defer func() { _ = r.Close() }()
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read the captured standard error: %v", err)
	}
	if got := string(data); got != "helper line\n" {
		t.Errorf("Logf wrote %q, want %q", got, "helper line\n")
	}

	if user := RelaunchOptions(false); user.System {
		t.Error("RelaunchOptions(false).System = true, want the user installation")
	}
}

// TestPlistRunsBinaryReportsWhatItCannotRead checks each reason the helper
// refuses the rewritten plist: a missing plist, a plist that is not parseable,
// an executable that cannot be inspected, and the matching case that passes.
func TestPlistRunsBinaryReportsWhatItCannotRead(t *testing.T) {
	t.Run("plist is missing", func(t *testing.T) {
		opts := baseOptions(t)
		err := plistRunsBinary(opts, "/missing.plist", ExecutableID{})
		if err == nil {
			t.Fatal("plistRunsBinary accepted a missing plist")
		}
		if !strings.Contains(err.Error(), "cannot read /missing.plist") {
			t.Errorf("error = %q, want it to name the unreadable plist", err)
		}
	})

	t.Run("plist is not parseable", func(t *testing.T) {
		opts := baseOptions(t)
		writeRootedFile(t, opts, "/broken.plist", "<plist><dict>", 0o644)
		err := plistRunsBinary(opts, "/broken.plist", ExecutableID{})
		if err == nil {
			t.Fatal("plistRunsBinary accepted an unparseable plist")
		}
		if !strings.Contains(err.Error(), "cannot read /broken.plist") {
			t.Errorf("error = %q, want it to name the unparseable plist", err)
		}
	})

	t.Run("executable is missing", func(t *testing.T) {
		opts := baseOptions(t)
		content, err := LaunchDaemonPlist("/opt/peers/bin/peers")
		if err != nil {
			t.Fatalf("render the plist: %v", err)
		}
		writeRootedFile(t, opts, "/plist", content, 0o644)
		err = plistRunsBinary(opts, "/plist", ExecutableID{})
		if err == nil {
			t.Fatal("plistRunsBinary accepted a plist whose executable is missing")
		}
		if !strings.Contains(err.Error(), `cannot inspect the executable "/opt/peers/bin/peers"`) {
			t.Errorf("error = %q, want it to name the missing executable", err)
		}
	})

	t.Run("executable matches", func(t *testing.T) {
		opts := baseOptions(t)
		exe := writeRootedFile(t, opts, "/opt/peers/bin/peers", "binary", 0o755)
		content, err := LaunchDaemonPlist("/opt/peers/bin/peers")
		if err != nil {
			t.Fatalf("render the plist: %v", err)
		}
		writeRootedFile(t, opts, "/plist", content, 0o644)
		id := mustExecutableID(t, exe)
		if err := plistRunsBinary(opts, "/plist", id); err != nil {
			t.Errorf("plistRunsBinary: %v", err)
		}
	})
}

// TestLaunchdProgramPathRejectsMalformedPlists checks every shape a plist can
// take that does not start with a ProgramArguments array of strings.
func TestLaunchdProgramPathRejectsMalformedPlists(t *testing.T) {
	cases := []struct {
		name string
		data string
		want string
	}{
		{name: "no ProgramArguments", data: `<dict><key>Label</key><string>x</string></dict>`, want: "no ProgramArguments in the plist"},
		{name: "token error", data: "<", want: "unexpected EOF"},
		{name: "key does not decode", data: "<key>", want: "unexpected EOF"},
		{name: "array ends at once", data: "<key>ProgramArguments</key></dict>", want: "ProgramArguments is not an array"},
		{name: "not an array", data: "<key>ProgramArguments</key><foo></foo>", want: "ProgramArguments is not an array"},
		{name: "no first entry", data: "<key>ProgramArguments</key><array></array>", want: "ProgramArguments has no first entry"},
		{name: "first entry is not a string", data: "<key>ProgramArguments</key><array><key>x</key></array>", want: "ProgramArguments has no first entry"},
		{name: "string does not decode", data: "<key>ProgramArguments</key><array><string>", want: "unexpected EOF"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := launchdProgramPath([]byte(tc.data))
			if err == nil {
				t.Fatalf("launchdProgramPath = %q, want an error", got)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to mention %q", err, tc.want)
			}
		})
	}

	t.Run("reads the first argument", func(t *testing.T) {
		data := "<dict><key>ProgramArguments</key><array><string>/opt/peers</string><string>run</string></array></dict>"
		got, err := launchdProgramPath([]byte(data))
		if err != nil {
			t.Fatalf("launchdProgramPath: %v", err)
		}
		if got != "/opt/peers" {
			t.Errorf("program = %q, want /opt/peers", got)
		}
	})
}

// TestNextStartElement checks the token scan: it skips character data, returns
// the next start element, and fails on a decoder error or an end element.
func TestNextStartElement(t *testing.T) {
	t.Run("skips text", func(t *testing.T) {
		dec := xmlDecoder(t, "  text <foo>")
		got, err := nextStartElement(dec)
		if err != nil {
			t.Fatalf("nextStartElement: %v", err)
		}
		if got.Name.Local != "foo" {
			t.Errorf("element = %q, want foo", got.Name.Local)
		}
	})

	t.Run("decoder error", func(t *testing.T) {
		dec := xmlDecoder(t, "<")
		if _, err := nextStartElement(dec); err == nil {
			t.Fatal("nextStartElement accepted a malformed document")
		}
	})

	t.Run("end element before start", func(t *testing.T) {
		dec := xmlDecoder(t, "<foo></foo>")
		if _, err := dec.Token(); err != nil {
			t.Fatalf("read the opening element: %v", err)
		}
		_, err := nextStartElement(dec)
		if err == nil {
			t.Fatal("nextStartElement accepted an end element")
		}
		if !strings.Contains(err.Error(), "unexpected closing element </foo>") {
			t.Errorf("error = %q, want it to name the closing element", err)
		}
	})
}

// xmlDecoder returns a decoder over data.
func xmlDecoder(t *testing.T, data string) *xml.Decoder {
	t.Helper()
	return xml.NewDecoder(strings.NewReader(data))
}

// TestParsePlistEnvironment checks the EnvironmentVariables parser: the sorted
// string entries it returns, the value types it skips, and every malformed
// shape it rejects.
func TestParsePlistEnvironment(t *testing.T) {
	cases := []struct {
		name    string
		data    string
		want    []plistEnvironmentEntry
		wantErr string
	}{
		{
			name: "sorted strings, other types skipped",
			data: `<dict><key>EnvironmentVariables</key><dict>` +
				`<key>B</key><string>2</string>` +
				`<key>C</key><integer>1</integer>` +
				`<key>A</key><string>1</string>` +
				`</dict></dict>`,
			want: []plistEnvironmentEntry{{Name: "A", Value: "1"}, {Name: "B", Value: "2"}},
		},
		{
			name: "no EnvironmentVariables key",
			data: `<dict><key>Label</key><string>x</string></dict>`,
		},
		{
			name:    "token error",
			data:    "<",
			wantErr: "unexpected EOF",
		},
		{
			name:    "key does not decode",
			data:    "<key>",
			wantErr: "unexpected EOF",
		},
		{
			name:    "dictionary ends instead",
			data:    "<key>EnvironmentVariables</key></dict>",
			wantErr: "EnvironmentVariables is not a dictionary",
		},
		{
			name:    "value is not a dictionary",
			data:    "<key>EnvironmentVariables</key><array></array>",
			wantErr: "EnvironmentVariables is not a dictionary",
		},
		{
			name:    "dictionary does not close",
			data:    "<key>EnvironmentVariables</key><dict>",
			wantErr: "unexpected EOF",
		},
		{
			name:    "entry key does not decode",
			data:    "<key>EnvironmentVariables</key><dict><key>",
			wantErr: "unexpected EOF",
		},
		{
			name:    "entry string does not decode",
			data:    "<key>EnvironmentVariables</key><dict><key>A</key><string>",
			wantErr: "unexpected EOF",
		},
		{
			name:    "skipped entry does not close",
			data:    "<key>EnvironmentVariables</key><dict><array>",
			wantErr: "unexpected EOF",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parsePlistEnvironment([]byte(tc.data))
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("parsePlistEnvironment = %v, want an error", got)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("error = %q, want it to mention %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parsePlistEnvironment: %v", err)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("entries = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestPlistEnvironmentIgnoresAnUnparseableFile checks that a file this package
// cannot parse contributes no entries and no error.
func TestPlistEnvironmentIgnoresAnUnparseableFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.plist")
	if err := os.WriteFile(path, []byte("<"), 0o644); err != nil {
		t.Fatalf("write the unparseable plist: %v", err)
	}

	got, err := plistEnvironment(path)
	if err != nil {
		t.Fatalf("plistEnvironment: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("entries = %v, want none from an unparseable file", got)
	}

	if err := os.WriteFile(path, []byte(`<dict><key>EnvironmentVariables</key><dict><key>A</key><string>1</string></dict></dict>`), 0o644); err != nil {
		t.Fatalf("write the parseable plist: %v", err)
	}
	got, err = plistEnvironment(path)
	if err != nil {
		t.Fatalf("plistEnvironment: %v", err)
	}
	want := []plistEnvironmentEntry{{Name: "A", Value: "1"}}
	if !slices.Equal(got, want) {
		t.Errorf("entries = %v, want %v", got, want)
	}
}

// TestWithPlistEnvironment checks how kept entries are written: unparseable
// content that still has the document end receives them, while content without
// that end or without entries is returned as it is.
func TestWithPlistEnvironment(t *testing.T) {
	t.Run("unparseable content still receives kept entries", func(t *testing.T) {
		const content = "<plist><dict><key>&bogus;</key></dict>\n</plist>"
		got := withPlistEnvironment(content, []plistEnvironmentEntry{{Name: "KEPT", Value: "v"}})
		for _, want := range []string{"&bogus;", "<key>KEPT</key>\n\t\t<string>v</string>"} {
			if !strings.Contains(got, want) {
				t.Errorf("content lost %q:\n%s", want, got)
			}
		}
	})

	t.Run("no document end", func(t *testing.T) {
		const content = "not a plist"
		got := withPlistEnvironment(content, []plistEnvironmentEntry{{Name: "KEPT", Value: "v"}})
		if got != content {
			t.Errorf("content = %q, want it unchanged as %q", got, content)
		}
	})

	t.Run("no entries", func(t *testing.T) {
		const content = "<plist><dict></dict>\n</plist>"
		if got := withPlistEnvironment(content, nil); got != content {
			t.Errorf("content = %q, want it unchanged as %q", got, content)
		}
	})
}

// TestRemovePlistEnvironmentLeavesUnreadableContentAlone checks that a document
// whose EnvironmentVariables cannot be located is returned unchanged.
func TestRemovePlistEnvironmentLeavesUnreadableContentAlone(t *testing.T) {
	cases := []struct {
		name    string
		content string
	}{
		{name: "token error", content: "&bogus;"},
		{name: "key does not decode", content: "<key>"},
		{name: "dictionary does not start", content: "<key>EnvironmentVariables</key></plist>"},
		{name: "dictionary does not close", content: "<key>EnvironmentVariables</key><dict>"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := removePlistEnvironment(tc.content); got != tc.content {
				t.Errorf("content = %q, want it unchanged as %q", got, tc.content)
			}
		})
	}

	t.Run("removes the dictionary", func(t *testing.T) {
		const content = "<dict>\n\t<key>EnvironmentVariables</key>\n\t<dict>\n\t\t<key>A</key>\n\t\t<string>1</string>\n\t</dict>\n\t<key>Label</key>\n\t<string>x</string>\n</dict>\n</plist>"
		got := removePlistEnvironment(content)
		if strings.Contains(got, "EnvironmentVariables") || strings.Contains(got, "<string>1</string>") {
			t.Errorf("the dictionary survived:\n%s", got)
		}
		for _, want := range []string{"<key>Label</key>", "<string>x</string>"} {
			if !strings.Contains(got, want) {
				t.Errorf("content lost %q:\n%s", want, got)
			}
		}
	})
}

// TestLaunchdReloadReportsABootoutFailure checks the first command of the
// deferred reload: a bootout that fails for any reason other than "not loaded"
// stops the reload before the bootstrap.
func TestLaunchdReloadReportsABootoutFailure(t *testing.T) {
	opts, fake, plistPath, id := launchdReloadFixture(t)
	fake.loaded = true
	fake.pid = 111
	opts.WaitProcessExit = func(int, time.Duration) error { return nil }
	baseRun := opts.Run
	opts.Run = func(name string, args ...string) ([]byte, error) {
		if len(args) > 0 && args[0] == "bootout" {
			return []byte("bootout refused"), exitError(t, 1)
		}
		return baseRun(name, args...)
	}

	err := LaunchdReload(opts, "system", plistPath, 4242, id, filepath.Join(t.TempDir(), "audit.log"))
	if err == nil {
		t.Fatal("LaunchdReload succeeded although bootout failed")
	}
	for _, want := range []string{"cannot run launchctl bootout system/" + Label, "bootout refused"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want it to mention %q", err, want)
		}
	}
	for _, cmd := range fake.cmds {
		if strings.Contains(cmd, "bootstrap") {
			t.Errorf("the reload ran %q after a failed bootout", cmd)
		}
	}
}

// TestLaunchdReloadReportsAnUnloadTimeout checks that a job launchd never
// finishes unloading stops the reload before the bootstrap, with the manual
// bootout and the retry named.
func TestLaunchdReloadReportsAnUnloadTimeout(t *testing.T) {
	opts, fake, plistPath, id := launchdReloadFixture(t)
	fake.loaded = true
	fake.pid = 111
	opts.WaitProcessExit = func(int, time.Duration) error { return nil }
	opts.StopTimeout = time.Nanosecond
	baseRun := opts.Run
	opts.Run = func(name string, args ...string) ([]byte, error) {
		if len(args) > 0 && args[0] == "print" {
			return []byte("state = running"), nil
		}
		return baseRun(name, args...)
	}

	err := LaunchdReload(opts, "system", plistPath, 4242, id, filepath.Join(t.TempDir(), "audit.log"))
	if err == nil {
		t.Fatal("LaunchdReload succeeded although the job never unloaded")
	}
	for _, want := range []string{"cannot unload the old service in system", "launchctl bootout system/" + Label} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want it to mention %q", err, want)
		}
	}
	for _, cmd := range fake.cmds {
		if strings.Contains(cmd, "bootstrap") {
			t.Errorf("the reload ran %q after an unload timeout", cmd)
		}
	}
}

// TestWaitProcessExitPollsWithoutAnInjectedWait checks the production path used
// when Options carries no wait function: a live child is reported as still
// running after the bound, and a killed one returns once it is gone.
func TestWaitProcessExitPollsWithoutAnInjectedWait(t *testing.T) {
	live := exec.Command("sleep", "10")
	if err := live.Start(); err != nil {
		t.Fatalf("start the live child: %v", err)
	}
	if err := waitProcessExit(baseOptions(t), live.Process.Pid, 50*time.Millisecond); err == nil ||
		!strings.Contains(err.Error(), "is still running") {
		t.Errorf("waitProcessExit(live child) = %v, want the still-running error", err)
	}
	if err := procsig.SendSignal(live.Process.Pid, syscall.SIGKILL); err != nil {
		t.Fatalf("kill the live child: %v", err)
	}
	_ = live.Wait()

	cmd := exec.Command("sleep", "10")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start the child: %v", err)
	}
	pid := cmd.Process.Pid
	if err := procsig.SendSignal(pid, syscall.SIGKILL); err != nil {
		t.Fatalf("kill the child: %v", err)
	}
	_ = cmd.Wait()

	if err := waitProcessExit(baseOptions(t), pid, time.Second); err != nil {
		t.Errorf("waitProcessExit: %v", err)
	}
}
