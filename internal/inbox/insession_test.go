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

const insessionTestRole = "PEERS_TEST_INSESSION_ROLE"

// TestInSessionAncestorProcess checks that a process is in a session when a parent carries the variable even though its own environment does not.
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
	// The parent must carry the variable in the environment it had at exec time: /proc and kern.procargs2 expose that copy, and a t.Setenv value may not reach it, so a re-executed copy of the test is the parent and it starts the child with the variable removed.
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

func prepareInSession(t *testing.T) string {
	t.Helper()
	t.Setenv(sessionEnvVar, "")
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", "")
	return home
}

func stubReaders(t *testing.T, parent func(int) (int, error), env func(int) ([]string, error)) {
	t.Helper()
	oldParent, oldEnv := readParentPID, readEnviron
	readParentPID, readEnviron = parent, env
	t.Cleanup(func() {
		readParentPID, readEnviron = oldParent, oldEnv
	})
}

// fakeTree seeds the stubbed readers: a pid missing from parents reads as a failed parent read, one missing from envs reads as an empty environment, and a read failure must be registered in parentErr or envErr.
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

// TestInSessionOwnEnvironment checks that the process's own environment decides when no ancestor is readable.
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

// TestInSessionAncestorEnvironment checks that an ancestor's environment carrying the variable marks the process as in a session.
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

// TestInSessionEmptyAncestorVariable checks that an empty variable in an ancestor does not mark the process as in a session.
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

// TestInSessionAncestorRecord checks that an ancestor's session registry record marks the process as in a session.
func TestInSessionAncestorRecord(t *testing.T) {
	home := prepareInSession(t)
	start := os.Getppid()
	writeSessionRecord(t, filepath.Join(home, ".claude", "sessions"), start, `{"name":"worker"}`)
	stubTree(t, fakeTree{parents: map[int]int{start: 1}})

	if !InSession() {
		t.Fatal("InSession() = false when the parent has a session registry record")
	}
}

// TestInSessionRecordMustBeRegularFile checks that a record path that is not a regular file does not mark the process as in a session.
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

// TestInSessionHonorsClaudeConfigDir checks that the registry record is looked up under CLAUDE_CONFIG_DIR when it is set.
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

// TestInSessionSkipsUnreadableAncestor checks that an ancestor whose environment cannot be read is skipped and the walk continues.
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

// TestInSessionStopsOnCycleAtLimit checks that a cyclic parent chain stops at maxAncestorDepth.
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

// TestInSessionStopsOnSelfParent checks that a process whose parent is itself stops the walk.
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

// TestInSessionCleanProcessTree checks that a clean process tree reports not in a session.
func TestInSessionCleanProcessTree(t *testing.T) {
	prepareInSession(t)
	start := os.Getppid()
	stubTree(t, fakeTree{parents: map[int]int{start: start + 1, start + 1: 1}})

	if InSession() {
		t.Fatal("InSession() = true for a clean process tree")
	}
}

// TestEnvironmentHasSession checks that only a non-empty exact variable name counts.
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

// TestParseNulList checks that a NUL-separated block splits into its non-empty entries.
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

// TestParseProcargs2 checks that the procargs2 buffer yields its argument and environment strings.
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
