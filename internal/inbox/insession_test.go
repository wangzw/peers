package inbox

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// insessionTestRole selects the role of a re-executed test binary.
const insessionTestRole = "PEERS_TEST_INSESSION_ROLE"

// TestInSessionAncestorProcess proves that a process whose own environment does
// not name the inbox socket is still recognised when one of its parents
// carries it. A session command that clears the variable and starts a Broker
// leaves exactly this shape behind, and the Broker must still refuse to start.
//
// The parent must carry the variable in the environment it had at exec time:
// /proc and kern.procargs2 expose that copy, and a t.Setenv value may not reach
// it. The test therefore runs a re-executed copy of itself as the parent, and
// that copy starts the child with the variable removed.
func TestInSessionAncestorProcess(t *testing.T) {
	switch os.Getenv(insessionTestRole) {
	case "parent":
		insessionParentHelper(t)
		return
	case "child":
		fmt.Printf("InSession()=%t\n", InSession())
		return
	}

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	cmd := exec.Command(exe, "-test.run=^TestInSessionAncestorProcess$")
	cmd.Env = append(withoutEnv(os.Environ(), sessionEnvVar),
		insessionTestRole+"=parent",
		sessionEnvVar+"=/tmp/session/inbox.sock",
		"HOME="+t.TempDir(),
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helper process: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "InSession()=true") {
		t.Fatalf("child of a session reported %q, want InSession()=true", strings.TrimSpace(string(out)))
	}
}

// insessionParentHelper runs inside a process whose environment carries the
// inbox socket path. It starts a child with the variable removed, so the child
// can only find the variable by reading its parent's process information.
func insessionParentHelper(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	cmd := exec.Command(exe, "-test.run=^TestInSessionAncestorProcess$")
	cmd.Env = append(withoutEnv(os.Environ(), sessionEnvVar), insessionTestRole+"=child")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("child process: %v", err)
	}
}

// withoutEnv returns env without any entry for key.
func withoutEnv(env []string, key string) []string {
	prefix := key + "="
	out := make([]string, 0, len(env))
	for _, entry := range env {
		if strings.HasPrefix(entry, prefix) {
			continue
		}
		out = append(out, entry)
	}
	return out
}

// prepareInSession clears the current process's own session variable, points
// HOME and the default configuration directory at an empty directory, and
// returns that directory. Tests that want a registry record write it under the
// returned directory.
func prepareInSession(t *testing.T) string {
	t.Helper()
	t.Setenv(sessionEnvVar, "")
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", "")
	return home
}

// stubReaders replaces the process-table readers for one test.
func stubReaders(t *testing.T, parent func(int) (int, error), env func(int) ([]string, error)) {
	t.Helper()
	oldParent, oldEnv := readParentPID, readEnviron
	readParentPID, readEnviron = parent, env
	t.Cleanup(func() {
		readParentPID, readEnviron = oldParent, oldEnv
	})
}

// fakeTree is a process tree for stubTree. A pid missing from a map is read as
// a failure, which models an ancestor that has exited.
type fakeTree struct {
	parents   map[int]int
	parentErr map[int]bool
	envs      map[int][]string
	envErr    map[int]bool
}

func stubTree(t *testing.T, tree fakeTree) {
	t.Helper()
	stubReaders(t,
		func(pid int) (int, error) {
			if tree.parentErr[pid] {
				return 0, errors.New("cannot read the parent")
			}
			next, ok := tree.parents[pid]
			if !ok {
				return 0, errors.New("cannot read the parent")
			}
			return next, nil
		},
		func(pid int) ([]string, error) {
			if tree.envErr[pid] {
				return nil, errors.New("cannot read the environment")
			}
			return tree.envs[pid], nil
		},
	)
}

func missingParent(int) (int, error)     { return 0, errors.New("no parent") }
func emptyEnviron(int) ([]string, error) { return nil, nil }

func writeSessionRecord(t *testing.T, dir string, pid int, body string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("create session registry: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, strconv.Itoa(pid)+".json"), []byte(body), 0o600); err != nil {
		t.Fatalf("write session record: %v", err)
	}
}

func TestInSessionOwnEnvironment(t *testing.T) {
	prepareInSession(t)
	stubReaders(t, missingParent, emptyEnviron)

	if InSession() {
		t.Fatal("InSession() = true with a clean environment and no ancestors")
	}
	t.Setenv(sessionEnvVar, "/tmp/session/inbox.sock")
	if !InSession() {
		t.Fatal("InSession() = false with CLAUDE_CODE_MESSAGING_SOCKET set")
	}
}

func TestInSessionAncestorEnvironment(t *testing.T) {
	prepareInSession(t)
	start := os.Getppid()
	stubTree(t, fakeTree{
		parents: map[int]int{start: start + 1, start + 1: 1},
		envs:    map[int][]string{start + 1: {sessionEnvVar + "=/tmp/session/inbox.sock"}},
	})

	if !InSession() {
		t.Fatal("InSession() = false when a grandparent carries the session variable")
	}
}

func TestInSessionEmptyAncestorVariable(t *testing.T) {
	prepareInSession(t)
	start := os.Getppid()
	stubTree(t, fakeTree{
		parents: map[int]int{start: start + 1, start + 1: 1},
		envs:    map[int][]string{start + 1: {sessionEnvVar + "="}},
	})

	if InSession() {
		t.Fatal("InSession() = true with an empty session variable in an ancestor")
	}
}

func TestInSessionAncestorRecord(t *testing.T) {
	home := prepareInSession(t)
	start := os.Getppid()
	writeSessionRecord(t, filepath.Join(home, ".claude", "sessions"), start, `{"name":"worker"}`)
	stubTree(t, fakeTree{parents: map[int]int{start: 1}})

	if !InSession() {
		t.Fatal("InSession() = false when the parent has a session registry record")
	}
}

func TestInSessionRecordMustBeRegularFile(t *testing.T) {
	home := prepareInSession(t)
	start := os.Getppid()
	if err := os.MkdirAll(filepath.Join(home, ".claude", "sessions", strconv.Itoa(start)+".json"), 0o700); err != nil {
		t.Fatalf("create a directory in place of the record: %v", err)
	}
	stubTree(t, fakeTree{parents: map[int]int{start: 1}})

	if InSession() {
		t.Fatal("InSession() = true for a record path that is not a regular file")
	}
}

func TestInSessionHonorsClaudeConfigDir(t *testing.T) {
	prepareInSession(t)
	dir := t.TempDir()
	t.Setenv("CLAUDE_CONFIG_DIR", dir)
	start := os.Getppid()
	writeSessionRecord(t, filepath.Join(dir, "sessions"), start, `{"name":"worker"}`)
	stubTree(t, fakeTree{parents: map[int]int{start: 1}})

	if !InSession() {
		t.Fatal("InSession() = false when the record is under CLAUDE_CONFIG_DIR")
	}
}

func TestInSessionSkipsUnreadableAncestor(t *testing.T) {
	prepareInSession(t)
	start := os.Getppid()
	stubTree(t, fakeTree{
		parents: map[int]int{start: start + 1, start + 1: start + 2, start + 2: 1},
		envErr:  map[int]bool{start + 1: true},
		envs:    map[int][]string{start + 2: {sessionEnvVar + "=/tmp/session/inbox.sock"}},
	})

	if !InSession() {
		t.Fatal("InSession() = false although an ancestor above an unreadable one carries the variable")
	}
}

func TestInSessionStopsOnCycleAtLimit(t *testing.T) {
	prepareInSession(t)
	start := os.Getppid()
	reads := 0
	stubReaders(t,
		func(pid int) (int, error) {
			reads++
			if pid == start {
				return start + 1, nil
			}
			return start, nil
		},
		emptyEnviron,
	)

	if InSession() {
		t.Fatal("InSession() = true for a clean cycle")
	}
	if reads != maxAncestorDepth {
		t.Fatalf("parent reads = %d, want %d", reads, maxAncestorDepth)
	}
}

func TestInSessionStopsOnSelfParent(t *testing.T) {
	prepareInSession(t)
	reads := 0
	stubReaders(t,
		func(pid int) (int, error) {
			reads++
			return pid, nil
		},
		emptyEnviron,
	)

	if InSession() {
		t.Fatal("InSession() = true for a self-parented process")
	}
	if reads != 1 {
		t.Fatalf("parent reads = %d, want 1", reads)
	}
}

func TestInSessionCleanProcessTree(t *testing.T) {
	prepareInSession(t)
	start := os.Getppid()
	stubTree(t, fakeTree{parents: map[int]int{start: start + 1, start + 1: 1}})

	if InSession() {
		t.Fatal("InSession() = true for a clean process tree")
	}
}

func TestEnvironmentHasSession(t *testing.T) {
	cases := []struct {
		name string
		env  []string
		want bool
	}{
		{"set", []string{"PATH=/bin", sessionEnvVar + "=/tmp/session/inbox.sock"}, true},
		{"empty", []string{sessionEnvVar + "="}, false},
		{"absent", []string{"PATH=/bin"}, false},
		{"longer name", []string{sessionEnvVar + "_EXTRA=/tmp/session/inbox.sock"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := environmentHasSession(tc.env); got != tc.want {
				t.Fatalf("environmentHasSession(%q) = %v, want %v", tc.env, got, tc.want)
			}
		})
	}
}

func TestParseNulList(t *testing.T) {
	cases := []struct {
		name string
		data string
		want []string
	}{
		{"two", "a\x00b\x00", []string{"a", "b"}},
		{"no terminator", "a", []string{"a"}},
		{"padding", "\x00a\x00\x00b\x00", []string{"a", "b"}},
		{"empty", "", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseNulList([]byte(tc.data))
			if len(got) != len(tc.want) {
				t.Fatalf("parseNulList(%q) = %q, want %q", tc.data, got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("parseNulList(%q) = %q, want %q", tc.data, got, tc.want)
				}
			}
		})
	}
}

func TestParseProcargs2(t *testing.T) {
	raw := []byte{2, 0, 0, 0} // argument count
	raw = append(raw, []byte("peers\x00run\x00")...)
	raw = append(raw, 0, 0) // alignment padding between the two areas
	raw = append(raw, []byte("HOME=/tmp\x00"+sessionEnvVar+"=/s\x00")...)

	got := parseProcargs2(raw)
	want := []string{"peers", "run", "HOME=/tmp", sessionEnvVar + "=/s"}
	if len(got) != len(want) {
		t.Fatalf("parseProcargs2 = %q, want %q", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("parseProcargs2 = %q, want %q", got, want)
		}
	}
	if parseProcargs2([]byte{1, 2}) != nil {
		t.Fatal("parseProcargs2 accepted a buffer shorter than its argument count")
	}
}
