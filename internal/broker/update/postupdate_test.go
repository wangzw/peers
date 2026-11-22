package update

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/service"
)

// postUpdateScript returns a stand-in release binary. It answers the version
// preflight, records the arguments of the hidden post-update command in
// marker, writes notes to stderr, and exits with code.
func postUpdateScript(version, marker string, code int, notes string) []byte {
	stderr := ""
	if notes != "" {
		stderr = fmt.Sprintf("echo '%s' >&2\n", notes)
	}
	return []byte(fmt.Sprintf(`#!/bin/sh
if [ "$1" = "--version" ]; then
  echo "peers %s"
  exit 0
fi
printf '%%s\n' "$*" > '%s'
%s
exit %d
`, version, marker, stderr, code))
}

// postUpdateHangScript returns a stand-in release binary whose post-update
// command never finishes on its own. exec keeps sleep the direct child, so the
// timeout kills the process the test started.
func postUpdateHangScript(version string) []byte {
	return []byte(fmt.Sprintf(`#!/bin/sh
if [ "$1" = "--version" ]; then
  echo "peers %s"
  exit 0
fi
exec sleep 30
`, version))
}

// serveScript stages script as the release binary of version and returns the
// options that install it over target.
func serveScript(t *testing.T, target, version string, script []byte) ApplyOptions {
	t.Helper()
	archive := tarGz(t, tarEntry{name: binaryName, body: script, mode: 0o755})
	srv := newApplyServer(t, version, writeBytes(archive), checksumsFor(archive, testArchiveName(version)))
	return testApplyOptions(srv, target, version)
}

// postUpdateArgs reads the arguments the installed binary received.
func postUpdateArgs(t *testing.T, marker string) string {
	t.Helper()
	data, err := os.ReadFile(marker)
	if err != nil {
		t.Fatalf("the post-update step did not run: %v", err)
	}
	return strings.TrimSpace(string(data))
}

// assertNoRollbackFiles checks that Install removed its rollback copy.
func assertNoRollbackFiles(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading the target directory: %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".peers-rollback-") {
			t.Errorf("rollback file %q was left behind", entry.Name())
		}
	}
}

func TestInstallRunsPostUpdate(t *testing.T) {
	unitGuardFixture(t)
	const version = "0.2.0"
	original := []byte("original binary")
	target, _ := newTarget(t, original, 0o750)
	marker := filepath.Join(filepath.Dir(target), "post-update-args")
	script := postUpdateScript(version, marker, 0, "")

	if err := Install(context.Background(), serveScript(t, target, version, script), "user"); err != nil {
		t.Fatalf("Install() error = %v", err)
	}

	if got, want := postUpdateArgs(t, marker), fmt.Sprintf("internal post-update --level user --pid %d", os.Getpid()); got != want {
		t.Errorf("post-update arguments = %q, want %q", got, want)
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("reading the installed binary: %v", err)
	}
	if !bytes.Equal(got, script) {
		t.Errorf("target content = %q, want the installed binary", got)
	}
	assertNoRollbackFiles(t, filepath.Dir(target))
}

// injectUnitGuard replaces the two pre-step unit guard calls for one test and
// restores them when it ends. It returns a pointer that reports whether
// SaveMainUnit ran.
func injectUnitGuard(t *testing.T, options func(bool) (service.Options, error), save func(service.Options) (service.SavedUnit, error)) *bool {
	t.Helper()
	previousOptions := postUpdateOptions
	previousSave := saveMainUnit
	t.Cleanup(func() {
		postUpdateOptions = previousOptions
		saveMainUnit = previousSave
	})
	saveCalled := new(bool)
	postUpdateOptions = options
	saveMainUnit = func(opts service.Options) (service.SavedUnit, error) {
		*saveCalled = true
		return save(opts)
	}
	return saveCalled
}

// assertUnitGuardFailure checks the shared contract of a pre-step unit guard
// failure: the error names the guard, carries the cause, says the previous
// binary was restored, and the step never started.
func assertUnitGuardFailure(t *testing.T, err, cause error, marker, target string, original []byte, before os.FileInfo) {
	t.Helper()
	if err == nil {
		t.Fatal("Install() = nil, want an error")
	}
	const prefix = "saving the service definition before the post-update step"
	if !strings.HasPrefix(err.Error(), prefix) {
		t.Errorf("error %q does not start with %q", err, prefix)
	}
	if !errors.Is(err, cause) {
		t.Errorf("error %q does not carry the injected cause", err)
	}
	if !strings.Contains(err.Error(), "the previous binary was restored") {
		t.Errorf("error %q does not say the binary was restored", err)
	}
	if _, statErr := os.Stat(marker); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("the post-update step ran after the unit guard failed: %v", statErr)
	}
	assertUnchanged(t, target, original, before)
	assertNoRollbackFiles(t, filepath.Dir(target))
}

// TestInstallRestoresWhenPostUpdateOptionsFails checks the first failure branch
// of the pre-step unit guard: options for the level cannot be resolved, so the
// previous binary comes back and the unit is not read.
func TestInstallRestoresWhenPostUpdateOptionsFails(t *testing.T) {
	unitGuardFixture(t)
	const version = "0.2.0"
	original := []byte("original binary")
	target, before := newTarget(t, original, 0o750)
	marker := filepath.Join(filepath.Dir(target), "post-update-ran")
	script := postUpdateScript(version, marker, 0, "")

	cause := errors.New("cannot locate the home directory")
	saveCalled := injectUnitGuard(t,
		func(bool) (service.Options, error) { return service.Options{}, cause },
		func(service.Options) (service.SavedUnit, error) { return service.SavedUnit{}, nil })

	err := Install(context.Background(), serveScript(t, target, version, script), "user")
	assertUnitGuardFailure(t, err, cause, marker, target, original, before)
	if *saveCalled {
		t.Error("SaveMainUnit ran after PostUpdateOptions failed")
	}
}

// TestInstallRestoresWhenSaveMainUnitFails checks the second failure branch: the
// main unit cannot be read before the step, so the previous binary comes back
// and the step never runs.
func TestInstallRestoresWhenSaveMainUnitFails(t *testing.T) {
	unitGuardFixture(t)
	const version = "0.2.0"
	original := []byte("original binary")
	target, before := newTarget(t, original, 0o750)
	marker := filepath.Join(filepath.Dir(target), "post-update-ran")
	script := postUpdateScript(version, marker, 0, "")

	cause := errors.New("cannot read the unit file")
	saveCalled := injectUnitGuard(t,
		func(bool) (service.Options, error) { return service.Options{}, nil },
		func(service.Options) (service.SavedUnit, error) { return service.SavedUnit{}, cause })

	err := Install(context.Background(), serveScript(t, target, version, script), "user")
	assertUnitGuardFailure(t, err, cause, marker, target, original, before)
	if !*saveCalled {
		t.Error("SaveMainUnit did not run")
	}
}

func TestInstallPostUpdateNotNeeded(t *testing.T) {
	unitGuardFixture(t)
	const version = "0.2.0"
	original := []byte("original binary")
	target, _ := newTarget(t, original, 0o750)
	marker := filepath.Join(filepath.Dir(target), "post-update-args")
	script := postUpdateScript(version, marker, postUpdateExitNotNeeded, "")

	if err := Install(context.Background(), serveScript(t, target, version, script), "user"); err != nil {
		t.Fatalf("Install() error = %v, want exit code 2 to continue", err)
	}

	// The step ran; only its exit code says there was nothing to do.
	if got, want := postUpdateArgs(t, marker), fmt.Sprintf("internal post-update --level user --pid %d", os.Getpid()); got != want {
		t.Errorf("post-update arguments = %q, want %q", got, want)
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("reading the installed binary: %v", err)
	}
	if !bytes.Equal(got, script) {
		t.Errorf("target content = %q, want the installed binary", got)
	}
	assertNoRollbackFiles(t, filepath.Dir(target))
}

func TestInstallPostUpdateFailureRollsBack(t *testing.T) {
	unitGuardFixture(t)
	const version = "0.2.0"
	original := []byte("original binary")
	target, before := newTarget(t, original, 0o750)
	marker := filepath.Join(filepath.Dir(target), "post-update-args")
	script := postUpdateScript(version, marker, 3, "cannot rewrite the service unit")

	err := Install(context.Background(), serveScript(t, target, version, script), "user")
	if err == nil {
		t.Fatal("Install() = nil, want an error")
	}
	if !strings.Contains(err.Error(), "post-update") {
		t.Errorf("error %q does not name the post-update step", err)
	}
	if !strings.Contains(err.Error(), "cannot rewrite the service unit") {
		t.Errorf("error %q does not carry the step's reason", err)
	}
	if !strings.Contains(err.Error(), "previous binary was restored") {
		t.Errorf("error %q does not say the binary was restored", err)
	}
	if got := postUpdateArgs(t, marker); got == "" {
		t.Error("the post-update step did not run")
	}
	assertUnchanged(t, target, original, before)
	assertNoRollbackFiles(t, filepath.Dir(target))
}

// TestInstallRunsPostUpdateFromTheRootDirectory checks the step's working
// directory: a system service has no meaningful working directory, so the
// binary must not depend on one that an account may have removed.
func TestInstallRunsPostUpdateFromTheRootDirectory(t *testing.T) {
	unitGuardFixture(t)
	const version = "0.2.0"
	original := []byte("original binary")
	target, _ := newTarget(t, original, 0o750)
	marker := filepath.Join(filepath.Dir(target), "post-update-pwd")
	script := []byte(fmt.Sprintf(`#!/bin/sh
if [ "$1" = "--version" ]; then
  echo "peers %s"
  exit 0
fi
pwd > '%s'
exit 0
`, version, marker))

	if err := Install(context.Background(), serveScript(t, target, version, script), "user"); err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	data, err := os.ReadFile(marker)
	if err != nil {
		t.Fatalf("read the working directory: %v", err)
	}
	if got := strings.TrimSpace(string(data)); got != "/" {
		t.Errorf("the post-update working directory = %q, want /", got)
	}
}

// TestRunPostUpdateUsesItsOwnBoundWithoutADeadline checks the production bound
// when the caller's context has no deadline: the step must still be stopped,
// because a hung binary cannot keep the previous version from coming back. The
// short injected bound keeps the test quick.
func TestRunPostUpdateUsesItsOwnBoundWithoutADeadline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers")
	if err := os.WriteFile(path, postUpdateHangScript("0.2.0"), 0o755); err != nil {
		t.Fatalf("write the hanging binary: %v", err)
	}
	start := time.Now()
	err := RunPostUpdate(context.Background(), PostUpdateOptions{
		Binary:  path,
		Level:   "system",
		Timeout: 150 * time.Millisecond,
	})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("RunPostUpdate() = nil, want a timeout error")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Errorf("error %q does not report a timeout", err)
	}
	if elapsed > 5*time.Second {
		t.Errorf("RunPostUpdate took %s, want it to stop near the injected bound", elapsed)
	}
}

// TestPostUpdateBoundIsTheDocumentedThirtySeconds checks the default bound the
// design names. A change to the constant without a design change fails here.
func TestPostUpdateBoundIsTheDocumentedThirtySeconds(t *testing.T) {
	if got := postUpdateBound(PostUpdateOptions{}); got != 30*time.Second {
		t.Errorf("the default post-update bound = %s, want 30s", got)
	}
	if got := postUpdateBound(PostUpdateOptions{Timeout: time.Minute}); got != time.Minute {
		t.Errorf("an injected post-update bound = %s, want 1m", got)
	}
}

func TestInstallPostUpdateTimeoutRollsBack(t *testing.T) {
	unitGuardFixture(t)
	const version = "0.2.0"
	original := []byte("original binary")
	target, before := newTarget(t, original, 0o750)
	script := postUpdateHangScript(version)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	err := Install(ctx, serveScript(t, target, version, script), "system")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Install() = nil, want a timeout error")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Errorf("error %q does not report a timeout", err)
	}
	if !strings.Contains(err.Error(), "previous binary was restored") {
		t.Errorf("error %q does not say the binary was restored", err)
	}
	if elapsed > 10*time.Second {
		t.Errorf("Install() took %s, want it to stop near the deadline", elapsed)
	}
	assertUnchanged(t, target, original, before)
	assertNoRollbackFiles(t, filepath.Dir(target))
}

// postUpdateRewriteScript returns a stand-in release binary whose post-update
// step writes content to unitPath and records that it ran in marker. hang makes
// the step run until its bound or context stops it; otherwise it exits with
// code.
func postUpdateRewriteScript(version, unitPath, content, marker string, code int, hang bool) []byte {
	tail := fmt.Sprintf("exit %d\n", code)
	if hang {
		tail = "exec sleep 30\n"
	}
	return []byte(fmt.Sprintf(`#!/bin/sh
if [ "$1" = "--version" ]; then
  echo "peers %s"
  exit 0
fi
printf '%%s' '%s' > '%s'
printf 'ran\n' > '%s'
%s
`, version, content, unitPath, marker, tail))
}

// unitGuardFixture points the unit restore at a temporary user installation: the
// account home and the user configuration directory live under one temporary
// directory, and a stand-in systemctl records the commands a restore runs. It
// returns the unit path that level's rewrite targets and a function that reads
// the recorded commands.
func unitGuardFixture(t *testing.T) (unitPath string, commands func() []string) {
	t.Helper()
	root := t.TempDir()
	home := filepath.Join(root, "home")
	xdg := filepath.Join(root, "config")
	bin := filepath.Join(root, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatalf("create the stand-in binary directory: %v", err)
	}
	logPath := filepath.Join(root, "systemctl.log")
	const systemctl = "#!/bin/sh\nprintf 'systemctl %s\\n' \"$*\" >> \"$PEERS_SYSTEMCTL_LOG\"\n"
	if err := os.WriteFile(filepath.Join(bin, "systemctl"), []byte(systemctl), 0o755); err != nil {
		t.Fatalf("write the stand-in systemctl: %v", err)
	}
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", xdg)
	t.Setenv("PEERS_SYSTEMCTL_LOG", logPath)
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))

	switch runtime.GOOS {
	case "linux":
		unitPath = filepath.Join(xdg, "systemd", "user", service.UnitName)
	case "darwin":
		unitPath = filepath.Join(home, "Library", "LaunchAgents", service.Label+".plist")
	default:
		t.Fatalf("the unit guard fixture supports linux and darwin, not %s", runtime.GOOS)
	}
	return unitPath, func() []string {
		data, err := os.ReadFile(logPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			t.Fatalf("read the recorded systemctl commands: %v", err)
		}
		return strings.Split(strings.TrimSpace(string(data)), "\n")
	}
}

// writeTestUnit writes a main unit file, creating its directory.
func writeTestUnit(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create the unit directory: %v", err)
	}
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write the unit file %s: %v", path, err)
	}
}

// assertNoUnitTempFiles checks that the atomic rewrite left no temporary file
// next to the unit.
func assertNoUnitTempFiles(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read the unit directory %s: %v", dir, err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".peers-install-") {
			t.Errorf("temporary unit file %q was left behind", entry.Name())
		}
	}
}

// TestInstallPostUpdateTimeoutRestoresTheUnit checks the backstop the old
// binary provides: the step is killed by its bound after it rewrote the unit,
// so its own restore never runs, and the old binary must put the original bytes
// back before it returns the failure.
func TestInstallPostUpdateTimeoutRestoresTheUnit(t *testing.T) {
	const version = "0.2.0"
	original := []byte("original binary")
	target, before := newTarget(t, original, 0o750)
	unit, commands := unitGuardFixture(t)
	oldUnit := []byte("old unit\n")
	writeTestUnit(t, unit, oldUnit)
	marker := filepath.Join(filepath.Dir(target), "post-update-ran")
	script := postUpdateRewriteScript(version, unit, "new unit\n", marker, 0, true)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	err := Install(ctx, serveScript(t, target, version, script), "user")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Install() = nil, want a timeout error")
	}
	for _, want := range []string{"timed out", "previous binary was restored"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	if elapsed > 10*time.Second {
		t.Errorf("Install() took %s, want it to stop near the deadline", elapsed)
	}
	if _, statErr := os.Stat(marker); statErr != nil {
		t.Fatalf("the post-update step did not run: %v", statErr)
	}
	if got, readErr := os.ReadFile(unit); readErr != nil {
		t.Fatalf("read the unit file after the timeout: %v", readErr)
	} else if !bytes.Equal(got, oldUnit) {
		t.Errorf("unit content after the timeout = %q, want the original %q", got, oldUnit)
	}
	assertUnchanged(t, target, original, before)
	assertNoRollbackFiles(t, filepath.Dir(target))
	assertNoUnitTempFiles(t, filepath.Dir(unit))
	if runtime.GOOS == "linux" {
		if got := commands(); !slices.Contains(got, "systemctl --user daemon-reload") {
			t.Errorf("systemctl commands = %q, want a daemon-reload after the restore", got)
		}
	}
}

// TestInstallPostUpdateFailureRemovesANewUnit checks the file that did not
// exist before the update: a failed step leaves the new definition behind, and
// the restore removes it instead of recreating an empty one.
func TestInstallPostUpdateFailureRemovesANewUnit(t *testing.T) {
	const version = "0.2.0"
	original := []byte("original binary")
	target, before := newTarget(t, original, 0o750)
	unit, _ := unitGuardFixture(t)
	if err := os.MkdirAll(filepath.Dir(unit), 0o755); err != nil {
		t.Fatalf("create the unit directory: %v", err)
	}
	marker := filepath.Join(filepath.Dir(target), "post-update-ran")
	script := postUpdateRewriteScript(version, unit, "new unit\n", marker, 3, false)

	err := Install(context.Background(), serveScript(t, target, version, script), "user")
	if err == nil {
		t.Fatal("Install() = nil, want an error")
	}
	if !strings.Contains(err.Error(), "previous binary was restored") {
		t.Errorf("error %q does not say the binary was restored", err)
	}
	if _, statErr := os.Stat(unit); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("a unit the failed step created was left behind: %v", statErr)
	}
	assertUnchanged(t, target, original, before)
	assertNoRollbackFiles(t, filepath.Dir(target))
	assertNoUnitTempFiles(t, filepath.Dir(unit))
}

// TestInstallPostUpdateCancelRestoresTheUnit checks the second way the step
// never restores itself: its context is canceled while it runs. The old binary
// must restore the original bytes anyway.
func TestInstallPostUpdateCancelRestoresTheUnit(t *testing.T) {
	const version = "0.2.0"
	original := []byte("original binary")
	target, before := newTarget(t, original, 0o750)
	unit, _ := unitGuardFixture(t)
	oldUnit := []byte("old unit\n")
	writeTestUnit(t, unit, oldUnit)
	marker := filepath.Join(filepath.Dir(target), "post-update-ran")
	script := postUpdateRewriteScript(version, unit, "new unit\n", marker, 0, true)
	o := serveScript(t, target, version, script)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Install(ctx, o, "user")
	}()

	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, statErr := os.Stat(marker); statErr == nil {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatal("the post-update step did not run")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()

	var err error
	select {
	case err = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Install() did not return after the context was canceled")
	}
	if err == nil {
		t.Fatal("Install() = nil, want a cancellation error")
	}
	for _, want := range []string{"canceled", "previous binary was restored"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	if got, readErr := os.ReadFile(unit); readErr != nil {
		t.Fatalf("read the unit file after the cancellation: %v", readErr)
	} else if !bytes.Equal(got, oldUnit) {
		t.Errorf("unit content after the cancellation = %q, want the original %q", got, oldUnit)
	}
	assertUnchanged(t, target, original, before)
	assertNoRollbackFiles(t, filepath.Dir(target))
	assertNoUnitTempFiles(t, filepath.Dir(unit))
}
