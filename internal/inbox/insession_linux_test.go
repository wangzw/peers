//go:build linux

package inbox

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestPlatformParentPID(t *testing.T) {
	got, err := platformParentPID(os.Getpid())
	if err != nil {
		t.Fatalf("platformParentPID(%d): %v", os.Getpid(), err)
	}
	if want := os.Getppid(); got != want {
		t.Fatalf("platformParentPID(%d) = %d, want %d", os.Getpid(), got, want)
	}
}

func TestParseProcStatParent(t *testing.T) {
	// The command name contains spaces and parentheses, which must not shift the
	// parent field.
	data := []byte("1234 (a (weird) name) S 42 1234 1234 0 -1 4194304 0 0\n")
	got, err := parseProcStatParent(data)
	if err != nil {
		t.Fatalf("parseProcStatParent: %v", err)
	}
	if got != 42 {
		t.Fatalf("parseProcStatParent = %d, want 42", got)
	}
}

func TestParseProcStatParentRejectsMalformed(t *testing.T) {
	for _, data := range []string{"", "1234 no parenthesised name", "1234 (name) S"} {
		if _, err := parseProcStatParent([]byte(data)); err == nil {
			t.Fatalf("parseProcStatParent(%q) accepted malformed data", data)
		}
	}
}

// TestParseProcStatParentNonNumericParent checks that a non-numeric parent
// field is reported with the offending field quoted.
func TestParseProcStatParentNonNumericParent(t *testing.T) {
	_, err := parseProcStatParent([]byte("1234 (name) S nope 1234\n"))
	if err == nil || !strings.Contains(err.Error(), `parent field "nope" is not a number`) {
		t.Fatalf("parseProcStatParent error = %v, want the non-numeric parent field error", err)
	}
	// Positive control: the same line with a numeric parent parses.
	got, err := parseProcStatParent([]byte("1234 (name) S 42 1234\n"))
	if err != nil || got != 42 {
		t.Fatalf("parseProcStatParent = (%d, %v), want (42, nil)", got, err)
	}
}

// aMissingPID returns a process id with no /proc entry, so a read of its
// process information fails the way an exited process does. A pid is only
// chosen after its absence was observed, so the test does not depend on the
// host's pid range or on which processes happen to run.
func aMissingPID(t *testing.T) int {
	t.Helper()
	for pid := 1 << 22; pid < (1<<22)+1000; pid++ {
		if _, err := os.Stat(procPath(pid, "")); errors.Is(err, fs.ErrNotExist) {
			return pid
		}
	}
	t.Skip("no unused process id in the probed range")
	return 0
}

// TestPlatformParentPIDMissingProcess covers the read failure of
// platformParentPID: a process that no longer exists is reported as such rather
// than as an unknown parent.
func TestPlatformParentPIDMissingProcess(t *testing.T) {
	// Positive control: the reader works for the current process.
	if got, err := platformParentPID(os.Getpid()); err != nil || got != os.Getppid() {
		t.Fatalf("platformParentPID(%d) = (%d, %v), want (%d, nil)", os.Getpid(), got, err, os.Getppid())
	}

	pid := aMissingPID(t)
	_, err := platformParentPID(pid)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("platformParentPID(%d) error = %v, want a not-exist error", pid, err)
	}
	if !strings.Contains(err.Error(), procPath(pid, "stat")) {
		t.Fatalf("platformParentPID error %q does not name %q", err, procPath(pid, "stat"))
	}
}

// platformEnvironRole selects the helper role of a re-executed test binary. The
// helper reports readiness and then waits, so the parent can read its
// /proc/<pid>/environ while it is alive.
const platformEnvironRole = "PEERS_TEST_PLATFORM_ENVIRON_ROLE"

// TestPlatformEnvironReadsProcessEnvironment checks that the /proc/<pid>/environ
// read returns exactly the environment a child copy of this test binary was
// started with.
func TestPlatformEnvironReadsProcessEnvironment(t *testing.T) {
	if os.Getenv(platformEnvironRole) == "child" {
		fmt.Println("ready")
		_, _ = io.Copy(io.Discard, os.Stdin)
		return
	}

	const marker = "PEERS_TEST_ENVIRON_MARKER=present"
	pid, finish := aStartEnvironChild(t, []string{marker})
	entries, err := platformEnviron(pid)
	finish()
	if err != nil {
		t.Fatalf("platformEnviron(%d): %v", pid, err)
	}
	// The child's environment is exactly the entries this test passed: the
	// marker and the helper role. A trailing NUL left on an entry would show up
	// here as a missing value.
	want := map[string]bool{marker: true, platformEnvironRole + "=child": true}
	if len(entries) != len(want) {
		t.Fatalf("platformEnviron(%d) = %q, want exactly %d entries", pid, entries, len(want))
	}
	for _, entry := range entries {
		if !want[entry] {
			t.Fatalf("platformEnviron(%d) = %q, which contains unexpected entry %q", pid, entries, entry)
		}
	}
}

// TestPlatformEnvironMissingProcess covers the open failure: the environment of
// a process that no longer exists is reported as not existing.
func TestPlatformEnvironMissingProcess(t *testing.T) {
	pid := aMissingPID(t)
	_, err := platformEnviron(pid)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("platformEnviron(%d) error = %v, want a not-exist error", pid, err)
	}
	if !strings.Contains(err.Error(), procPath(pid, "environ")) {
		t.Fatalf("platformEnviron error %q does not name %q", err, procPath(pid, "environ"))
	}
}

// aStartEnvironChild starts a helper copy of this test binary with env plus the
// helper role as its environment. It returns the child's pid and a function that
// releases and reaps the child; the child is already past exec when this
// returns, so its /proc/<pid>/environ is the environment passed here.
func aStartEnvironChild(t *testing.T, env []string) (int, func()) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	cmd := exec.Command(exe, "-test.run=^TestPlatformEnvironReadsProcessEnvironment$")
	cmd.Env = append(env, platformEnvironRole+"=child")
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("child stdin: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("child stdout: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting the child process: %v", err)
	}

	// The child prints its readiness only after exec, so the environment the
	// parent reads next is final.
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || strings.TrimSpace(line) != "ready" {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("child readiness line = %q, error %v; want %q", line, err, "ready")
	}

	finished := false
	finish := func() {
		if finished {
			return
		}
		finished = true
		_ = stdin.Close()
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("child process %d: %v", cmd.Process.Pid, err)
			}
		case <-time.After(aWaitTimeout):
			_ = cmd.Process.Kill()
			if err := <-done; err == nil {
				t.Errorf("child process %d reported success after being killed", cmd.Process.Pid)
			}
			t.Errorf("child process %d did not exit after its stdin closed", cmd.Process.Pid)
		}
	}
	t.Cleanup(finish)
	return cmd.Process.Pid, finish
}

// TestProcPath pins the path the platform readers open, with a pid far outside
// the live range so the test does not depend on any running process.
func TestProcPath(t *testing.T) {
	if got, want := procPath(12345, "stat"), "/proc/12345/stat"; got != want {
		t.Fatalf("procPath = %q, want %q", got, want)
	}
	if got, want := procPath(1<<22, "environ"), "/proc/"+strconv.Itoa(1<<22)+"/environ"; got != want {
		t.Fatalf("procPath = %q, want %q", got, want)
	}
}
