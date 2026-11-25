//go:build linux

package accountdb

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/user"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// writeScript writes body as an executable shell script into dir and returns
// its path.
func writeScript(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	script := "#!/bin/sh\n" + body
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	return path
}

// installFakeGetent makes the fake script at path the only getent the package
// probes, and restores the real list when the test ends.
func installFakeGetent(t *testing.T, path string) {
	t.Helper()
	old := getentPaths
	getentPaths = []string{path}
	t.Cleanup(func() { getentPaths = old })
}

// fakeGetentWithOutput installs a fake getent that records its arguments in
// marker and writes output. It returns the marker path.
func fakeGetentWithOutput(t *testing.T, output string) (path, marker string) {
	t.Helper()
	dir := t.TempDir()
	out := filepath.Join(dir, "out")
	if err := os.WriteFile(out, []byte(output), 0o644); err != nil {
		t.Fatalf("writing fake output: %v", err)
	}
	marker = filepath.Join(dir, "args")
	path = writeScript(t, dir, "getent", fmt.Sprintf(
		"printf '%%s\\n' \"$@\" > %q\ncat %q\n", marker, out))
	installFakeGetent(t, path)
	return path, marker
}

// assertArgs checks the arguments the fake getent received.
func assertArgs(t *testing.T, marker string, want ...string) {
	t.Helper()
	data, err := os.ReadFile(marker)
	if err != nil {
		t.Fatalf("reading recorded arguments: %v", err)
	}
	got := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Errorf("getent arguments = %q, want %q", got, want)
	}
}

func currentUID(t *testing.T) uint32 {
	t.Helper()
	return uint32(os.Getuid())
}

func TestLookupUIDPasswdEntry(t *testing.T) {
	_, marker := fakeGetentWithOutput(t, "alice:x:4242:4343:Alice:/home/alice:/bin/bash\n")

	got, err := LookupUID(4242)
	if err != nil {
		t.Fatalf("LookupUID(4242): %v", err)
	}
	want := User{Name: "alice", UID: 4242, GID: 4343, Home: "/home/alice"}
	if got != want {
		t.Errorf("LookupUID(4242) = %+v, want %+v", got, want)
	}
	assertArgs(t, marker, "passwd", "--", "4242")
}

func TestLookupNamePasswdEntry(t *testing.T) {
	_, marker := fakeGetentWithOutput(t, "alice:x:4242:4343:Alice:/home/alice:/bin/bash\n")

	got, err := LookupName("alice")
	if err != nil {
		t.Fatalf("LookupName(alice): %v", err)
	}
	want := User{Name: "alice", UID: 4242, GID: 4343, Home: "/home/alice"}
	if got != want {
		t.Errorf("LookupName(alice) = %+v, want %+v", got, want)
	}
	assertArgs(t, marker, "passwd", "--", "alice")
}

func TestLookupGroupEntry(t *testing.T) {
	_, marker := fakeGetentWithOutput(t, "staff:x:777:\n")

	gid, err := LookupGroup("staff")
	if err != nil {
		t.Fatalf("LookupGroup(staff): %v", err)
	}
	if gid != 777 {
		t.Errorf("LookupGroup(staff) = %d, want 777", gid)
	}
	assertArgs(t, marker, "group", "--", "staff")
}

func TestGroupIDsIncludesPrimaryAndDeduplicates(t *testing.T) {
	_, marker := fakeGetentWithOutput(t, "alice               7 8 7\n")

	ids, err := GroupIDs(User{Name: "alice", GID: 5})
	if err != nil {
		t.Fatalf("GroupIDs: %v", err)
	}
	want := []uint32{5, 7, 8}
	if fmt.Sprint(ids) != fmt.Sprint(want) {
		t.Errorf("GroupIDs = %v, want %v", ids, want)
	}
	assertArgs(t, marker, "initgroups", "--", "alice")
}

func TestGetentExitTwoIsNotFound(t *testing.T) {
	path := writeScript(t, t.TempDir(), "getent", "exit 2\n")
	installFakeGetent(t, path)

	calls := []struct {
		name string
		call func() error
	}{
		{"uid", func() error { _, err := LookupUID(4242); return err }},
		{"name", func() error { _, err := LookupName("alice"); return err }},
		{"group", func() error { _, err := LookupGroup("staff"); return err }},
		{"initgroups", func() error { _, err := GroupIDs(User{Name: "alice"}); return err }},
	}
	for _, c := range calls {
		if err := c.call(); !errors.Is(err, ErrNotFound) {
			t.Errorf("%s lookup error = %v, want ErrNotFound", c.name, err)
		}
	}
}

func TestGetentUsesAbsolutePath(t *testing.T) {
	// A getent on PATH must never run, even though the test process has PATH
	// pointing at one.
	pathDir := t.TempDir()
	pathMarker := filepath.Join(pathDir, "ran")
	writeScript(t, pathDir, "getent", fmt.Sprintf("touch %q\n", pathMarker))
	t.Setenv("PATH", pathDir)

	path, _ := fakeGetentWithOutput(t, "alice:x:4242:4343::/home/alice:/bin/sh\n")
	if !filepath.IsAbs(path) {
		t.Fatalf("the fake getent path %q is not absolute", path)
	}
	if _, err := LookupUID(4242); err != nil {
		t.Fatalf("LookupUID(4242): %v", err)
	}
	if _, err := os.Stat(pathMarker); !os.IsNotExist(err) {
		t.Errorf("the getent found through PATH ran (stat %q: %v)", pathMarker, err)
	}
}

func TestGetentRunsWithEmptyEnvironment(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "environment")
	path := writeScript(t, dir, "getent", fmt.Sprintf(
		"tr '\\0' '\\n' < /proc/$$/environ > %q\nprintf 'alice:x:4242:4343::/home/alice:/bin/sh\\n'\n",
		marker))
	installFakeGetent(t, path)

	if _, err := LookupUID(4242); err != nil {
		t.Fatalf("LookupUID(4242): %v", err)
	}
	data, err := os.ReadFile(marker)
	if err != nil {
		t.Fatalf("reading the child environment: %v", err)
	}
	if got := strings.TrimSuffix(string(data), "\n"); got != "LANG=C" {
		t.Errorf("getent environment = %q, want only LANG=C", got)
	}
}

func TestGetentSeparatesKeyWithDashDash(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "args")
	path := writeScript(t, dir, "getent", fmt.Sprintf(
		"printf '%%s\\n' \"$@\" > %q\nprintf '%%s:x:4242:4343::/home/alice:/bin/sh\\n' \"$3\"\n", marker))
	installFakeGetent(t, path)

	got, err := LookupName("-alice")
	if err != nil {
		t.Fatalf("LookupName(-alice): %v", err)
	}
	if got.Name != "-alice" {
		t.Errorf("LookupName(-alice) name = %q, want -alice", got.Name)
	}
	assertArgs(t, marker, "passwd", "--", "-alice")
}

func TestInvalidNamesNeverReachGetent(t *testing.T) {
	path, marker := fakeGetentWithOutput(t, "alice:x:4242:4343::/home/alice:/bin/sh\n")

	for _, name := range []string{
		"", "bad:name", "bad\nname", "bad\x00name", "bad name", "bad\tname",
		"bad\rname", " leading", "trailing ",
	} {
		if _, err := LookupName(name); err == nil {
			t.Errorf("LookupName(%q) succeeded, want an error", name)
		}
		if _, err := LookupGroup(name); err == nil {
			t.Errorf("LookupGroup(%q) succeeded, want an error", name)
		}
		if _, err := GroupIDs(User{Name: name}); err == nil {
			t.Errorf("GroupIDs(%q) succeeded, want an error", name)
		}
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Errorf("a rejected name reached getent %s (stat %q: %v)", path, marker, err)
	}
}

func TestGetentRejectsMultipleLines(t *testing.T) {
	// Both lines are well formed on their own and the first one carries the
	// requested uid, so only the one-line rule can reject the answer.
	fakeGetentWithOutput(t, "evil:x:"+strconv.FormatUint(uint64(currentUID(t)), 10)+":0::/evil:/bin/sh\n"+
		"evil2:x:"+strconv.FormatUint(uint64(currentUID(t)), 10)+":0::/evil2:/bin/sh\n")

	want, err := osUserLookupUID(currentUID(t))
	if err != nil {
		t.Fatalf("osUserLookupUID(%d): %v", currentUID(t), err)
	}
	got, err := LookupUID(currentUID(t))
	if err != nil {
		t.Fatalf("LookupUID(%d): %v", currentUID(t), err)
	}
	if got != want {
		t.Errorf("LookupUID(%d) = %+v, want the os/user fallback %+v", currentUID(t), got, want)
	}
}

func TestGetentRejectsInconsistentOrMalformedEntries(t *testing.T) {
	uid := currentUID(t)
	want, err := osUserLookupUID(uid)
	if err != nil {
		t.Fatalf("osUserLookupUID(%d): %v", uid, err)
	}

	for _, c := range []struct {
		name   string
		output string
	}{
		{"mismatched uid", "evil:x:1:1::/evil:/bin/sh\n"},
		{"relative home", fmt.Sprintf("evil:x:%d:1::evil:/bin/sh\n", uid)},
		{"six fields", fmt.Sprintf("evil:x:%d:1::/evil\n", uid)},
		{"non-decimal uid", "evil:x:one:1::/evil:/bin/sh\n"},
		{"signed uid", "evil:x:+1:1::/evil:/bin/sh\n"},
	} {
		t.Run(c.name, func(t *testing.T) {
			fakeGetentWithOutput(t, c.output)
			got, err := LookupUID(uid)
			if err != nil {
				t.Fatalf("LookupUID(%d): %v", uid, err)
			}
			if got != want {
				t.Errorf("LookupUID(%d) = %+v, want the os/user fallback %+v", uid, got, want)
			}
		})
	}
}

func TestGetentTimeoutIsReportedAndFallsBack(t *testing.T) {
	oldWait := getentWait
	getentWait = 50 * time.Millisecond
	t.Cleanup(func() { getentWait = oldWait })

	path := writeScript(t, t.TempDir(), "getent", "exec sleep 30\n")
	installFakeGetent(t, path)

	if _, err := getentQuery("passwd", "--", "4242"); !errors.Is(err, errGetentTimeout) {
		t.Fatalf("getentQuery after the timeout = %v, want errGetentTimeout", err)
	}

	var logs bytes.Buffer
	oldLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(oldLogger) })

	want, err := osUserLookupUID(currentUID(t))
	if err != nil {
		t.Fatalf("osUserLookupUID(%d): %v", currentUID(t), err)
	}
	got, err := LookupUID(currentUID(t))
	if err != nil {
		t.Fatalf("LookupUID(%d) after a getent timeout: %v", currentUID(t), err)
	}
	if got != want {
		t.Errorf("LookupUID(%d) = %+v, want the os/user fallback %+v", currentUID(t), got, want)
	}
	if !strings.Contains(logs.String(), "timed out") {
		t.Errorf("timeout warning does not say the lookup timed out:\n%s", logs.String())
	}
}

func TestMissingGetentFallsBack(t *testing.T) {
	old := getentPaths
	getentPaths = []string{"/nonexistent/accountdb-getent-a", "/nonexistent/accountdb-getent-b"}
	t.Cleanup(func() { getentPaths = old })

	want, err := osUserLookupUID(currentUID(t))
	if err != nil {
		t.Fatalf("osUserLookupUID(%d): %v", currentUID(t), err)
	}
	got, err := LookupUID(currentUID(t))
	if err != nil {
		t.Fatalf("LookupUID(%d) without getent: %v", currentUID(t), err)
	}
	if got != want {
		t.Errorf("LookupUID(%d) = %+v, want the os/user fallback %+v", currentUID(t), got, want)
	}

	const absent = uint32(0x7ffffffe)
	if _, err := LookupUID(absent); !errors.Is(err, errUnavailable) {
		t.Errorf("LookupUID(%d) without getent = %v, want errUnavailable", absent, err)
	}
}

func TestLookupNameRejectsANumericKeyResolvingToAnotherName(t *testing.T) {
	// getent treats a numeric key as a uid, so asking for the name "0" returns
	// root's entry. The name comparison is what rejects it.
	fakeGetentWithOutput(t, "root:x:0:0:root:/root:/bin/sh\n")

	if u, err := LookupName("0"); err == nil {
		t.Fatalf("LookupName(\"0\") = %+v, want an error", u)
	}
}

func TestLookupUIDRejectsAReturnedUIDThatDiffers(t *testing.T) {
	fakeGetentWithOutput(t, "root:x:0:0:root:/root:/bin/sh\n")

	const absent = uint32(0x7ffffffe)
	u, err := LookupUID(absent)
	if err == nil {
		t.Fatalf("LookupUID(%d) = %+v, want an error", absent, u)
	}
	// The mismatched answer is not trusted; the local fallback has no entry,
	// so the failure is a failed check rather than a missing account.
	if !errors.Is(err, errUnavailable) || errors.Is(err, ErrNotFound) {
		t.Fatalf("LookupUID(%d) = %+v, %v, want errUnavailable", absent, u, err)
	}
}

func TestLookupGroupRejectsMalformedEntries(t *testing.T) {
	name := currentGroupName(t)
	want, err := osUserLookupGroup(name)
	if err != nil {
		t.Fatalf("osUserLookupGroup(%q): %v", name, err)
	}

	for _, c := range []struct {
		name   string
		output string
	}{
		{"three fields", name + ":x:777\n"},
		{"non-decimal gid", name + ":x:not-a-number:\n"},
		{"gid above 32 bits", name + ":x:4294967296:\n"},
	} {
		t.Run(c.name, func(t *testing.T) {
			fakeGetentWithOutput(t, c.output)
			got, err := LookupGroup(name)
			if err != nil {
				t.Fatalf("LookupGroup(%q): %v", name, err)
			}
			if got != want {
				t.Errorf("LookupGroup(%q) = %d, want the os/user fallback %d", name, got, want)
			}
		})
	}
}

func TestGroupIDsTransientFailureIsUnavailable(t *testing.T) {
	name := "accountdb-no-such-user-" + strconv.FormatUint(uint64(time.Now().UnixNano()), 10)
	for _, c := range []struct {
		name    string
		install func(t *testing.T)
	}{
		{"exit status 1", func(t *testing.T) {
			installFakeGetent(t, writeScript(t, t.TempDir(), "getent", "exit 1\n"))
		}},
		{"missing getent", func(t *testing.T) {
			old := getentPaths
			getentPaths = []string{"/nonexistent/accountdb-getent-a"}
			t.Cleanup(func() { getentPaths = old })
		}},
		{"timeout", func(t *testing.T) {
			old := getentWait
			getentWait = 20 * time.Millisecond
			t.Cleanup(func() { getentWait = old })
			installFakeGetent(t, writeScript(t, t.TempDir(), "getent", "exec sleep 30\n"))
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			c.install(t)
			_, err := GroupIDs(User{Name: name, UID: 4242, GID: 5})
			if err == nil {
				t.Fatal("GroupIDs succeeded, want a failure")
			}
			if errors.Is(err, ErrNotFound) {
				t.Errorf("GroupIDs error = %v, want a temporary failure, not ErrNotFound", err)
			}
			if !errors.Is(err, errUnavailable) {
				t.Errorf("GroupIDs error = %v, want it to wrap errUnavailable", err)
			}
		})
	}
}

func TestLocalGroupFallbackChecksUID(t *testing.T) {
	u, err := osUserLookupUID(currentUID(t))
	if err != nil {
		t.Fatalf("osUserLookupUID(%d): %v", currentUID(t), err)
	}
	u.UID++

	ids, err := osUserGroupIDs(u)
	if err == nil {
		t.Fatalf("osUserGroupIDs with a mismatched uid = %v, want an error", ids)
	}
	if errors.Is(err, ErrNotFound) {
		t.Errorf("osUserGroupIDs error = %v, want a uid mismatch, not ErrNotFound", err)
	}
}

func TestLookupGroupRejectsANumericKeyResolvingToAnotherName(t *testing.T) {
	// getent treats a numeric key as a gid, so asking for the group name "0"
	// returns root's group.
	fakeGetentWithOutput(t, "root:x:0:\n")

	if gid, err := LookupGroup("0"); err == nil {
		t.Fatalf("LookupGroup(\"0\") = %d, want an error", gid)
	}
}

func TestGetentOutputLimit(t *testing.T) {
	path := writeScript(t, t.TempDir(), "getent", fmt.Sprintf(
		"head -c %d /dev/zero | tr '\\0' a\n", getentOutputLimit+1))
	installFakeGetent(t, path)

	if _, err := getentQuery("passwd", "--", "4242"); !errors.Is(err, errGetentOutput) {
		t.Fatalf("getentQuery with %d bytes of output = %v, want errGetentOutput",
			getentOutputLimit+1, err)
	}
}

// getentLingeringPipeMargin is the headroom the lingering-pipe test allows
// getentQuery to return above the pipe wait. The wait is the product's
// getentPipeWait: two seconds in production, shortened in this test so the
// deadline follows the product timer instead of fixing a limit of its own.
// What decides pass from fail is the fake getent's child, which holds the
// output pipe for ten seconds; the margin therefore only has to keep the whole
// bound below ten seconds while still leaving room for the child to be reaped
// and for scheduling under -race. Five seconds does both: even the production
// two-second pipe wait plus this margin is seven seconds, and the shortened
// test value lands at 5.1 seconds.
const getentLingeringPipeMargin = 5 * time.Second

// TestGetentPipeWaitDefaultsToTheProductBound proves the test seam starts at
// the production WaitDelay, so shortening it in one test cannot silently change
// what release builds enforce.
func TestGetentPipeWaitDefaultsToTheProductBound(t *testing.T) {
	if want := 2 * time.Second; getentWaitDelay != want {
		t.Fatalf("getentWaitDelay = %v, want the %v product bound", getentWaitDelay, want)
	}
	if getentPipeWait != getentWaitDelay {
		t.Fatalf("getentPipeWait = %v, want the product default %v", getentPipeWait, getentWaitDelay)
	}
}

func TestGetentWaitDelayBoundsALingeringPipe(t *testing.T) {
	// The child inherits stdout and outlives the shell, so the output pipe
	// stays open after getent has exited. WaitDelay must close it. The child
	// records its pid so the test can kill what the shell leaves behind.
	//
	// The seam is shortened here because the wait itself is the product timer;
	// the deadline below tracks whatever value the seam holds. A test that
	// spent the full two seconds would only add dead time on the failing and
	// passing paths alike.
	oldWait := getentPipeWait
	getentPipeWait = 100 * time.Millisecond
	t.Cleanup(func() { getentPipeWait = oldWait })
	bound := getentPipeWait + getentLingeringPipeMargin

	dir := t.TempDir()
	pidPath := filepath.Join(dir, "child.pid")
	path := writeScript(t, dir, "getent", fmt.Sprintf("sleep 10 &\necho $! > %q\n", pidPath))
	installFakeGetent(t, path)
	t.Cleanup(func() {
		data, err := os.ReadFile(pidPath)
		if err != nil {
			return
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err != nil {
			return
		}
		if child, err := os.FindProcess(pid); err == nil {
			_ = child.Kill()
		}
	})

	done := make(chan error, 1)
	go func() {
		_, err := getentQuery("passwd", "--", "4242")
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Error("getentQuery trusted an answer whose pipe was still held")
		}
	case <-time.After(bound):
		t.Fatal("getentQuery did not return while a child held the output pipe")
	}
}

func TestLookupUIDTransientFailureIsUnavailable(t *testing.T) {
	installFakeGetent(t, writeScript(t, t.TempDir(), "getent", "exit 1\n"))

	const absent = uint32(0x7ffffffe)
	_, err := LookupUID(absent)
	if err == nil {
		t.Fatal("LookupUID succeeded while getent failed")
	}
	if !errors.Is(err, errUnavailable) || errors.Is(err, ErrNotFound) {
		t.Errorf("LookupUID(%d) = %v, want errUnavailable, not ErrNotFound", absent, err)
	}
}

func TestLookupNameTransientFailureIsUnavailable(t *testing.T) {
	installFakeGetent(t, writeScript(t, t.TempDir(), "getent", "exit 1\n"))

	name := "accountdb-no-such-user-" + strconv.FormatUint(uint64(time.Now().UnixNano()), 10)
	_, err := LookupName(name)
	if err == nil {
		t.Fatal("LookupName succeeded while getent failed")
	}
	if !errors.Is(err, errUnavailable) || errors.Is(err, ErrNotFound) {
		t.Errorf("LookupName(%q) = %v, want errUnavailable, not ErrNotFound", name, err)
	}
}

func TestLookupGroupTransientFailureIsUnavailable(t *testing.T) {
	installFakeGetent(t, writeScript(t, t.TempDir(), "getent", "exit 1\n"))

	name := "accountdb-no-such-group-" + strconv.FormatUint(uint64(time.Now().UnixNano()), 10)
	_, err := LookupGroup(name)
	if err == nil {
		t.Fatal("LookupGroup succeeded while getent failed")
	}
	if !errors.Is(err, errUnavailable) || errors.Is(err, ErrNotFound) {
		t.Errorf("LookupGroup(%q) = %v, want errUnavailable, not ErrNotFound", name, err)
	}
}

func currentGroupName(t *testing.T) string {
	t.Helper()
	g, err := user.LookupGroupId(strconv.FormatUint(uint64(os.Getgid()), 10))
	if err != nil {
		t.Fatalf("user.LookupGroupId(%d): %v", os.Getgid(), err)
	}
	return g.Name
}

// TestParseDecimal32RejectsEmptyID checks that parseDecimal32 rejects an empty
// id.
func TestParseDecimal32RejectsEmptyID(t *testing.T) {
	if _, err := parseDecimal32(""); err == nil || err.Error() != "empty id" {
		t.Errorf("parseDecimal32(\"\") error = %v, want \"empty id\"", err)
	}
}

// TestOSUserToUserRejectsInvalidIDs checks that osUserToUser reports an
// invalid uid or gid and returns the zero user.
func TestOSUserToUserRejectsInvalidIDs(t *testing.T) {
	for _, c := range []struct {
		name string
		in   user.User
		want string
	}{
		{"uid", user.User{Username: "bob", Uid: "not-a-number", Gid: "1"}, "invalid uid"},
		{"gid", user.User{Username: "bob", Uid: "1", Gid: "not-a-number"}, "invalid gid"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := osUserToUser(&c.in)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("osUserToUser(%+v) error = %v, want it to mention %q", c.in, err, c.want)
			}
			if got != (User{}) {
				t.Errorf("osUserToUser(%+v) = %+v, want the zero user alongside the error", c.in, got)
			}
		})
	}
}

// TestOSUserGroupIDsForCurrentAccount checks that osUserGroupIDs returns the
// current account's groups sorted, without duplicates, including the primary
// gid.
func TestOSUserGroupIDsForCurrentAccount(t *testing.T) {
	u, err := osUserLookupUID(currentUID(t))
	if err != nil {
		t.Fatalf("osUserLookupUID(%d): %v", currentUID(t), err)
	}
	ids, err := osUserGroupIDs(u)
	if err != nil {
		t.Fatalf("osUserGroupIDs(%+v): %v", u, err)
	}
	if len(ids) == 0 {
		t.Fatal("osUserGroupIDs returned no groups")
	}
	found := false
	for i, id := range ids {
		if id == u.GID {
			found = true
		}
		if i > 0 && ids[i-1] >= id {
			t.Errorf("osUserGroupIDs(%+v) = %v, want strictly increasing ids", u, ids)
			break
		}
	}
	if !found {
		t.Errorf("osUserGroupIDs(%+v) = %v, want it to contain the primary gid %d", u, ids, u.GID)
	}
}

// TestLookupNameMalformedGetentFallsBackToLocalAccount checks that a malformed
// getent passwd answer falls back to the local account.
func TestLookupNameMalformedGetentFallsBackToLocalAccount(t *testing.T) {
	want, err := osUserLookupUID(currentUID(t))
	if err != nil {
		t.Fatalf("osUserLookupUID(%d): %v", currentUID(t), err)
	}
	fakeGetentWithOutput(t, "not-a-passwd-entry\n")

	got, err := LookupName(want.Name)
	if err != nil {
		t.Fatalf("LookupName(%q): %v", want.Name, err)
	}
	if got != want {
		t.Errorf("LookupName(%q) = %+v, want the local account %+v", want.Name, got, want)
	}
}

// TestGroupIDsMalformedInitgroupsFallsBackToLocalAccount checks that a
// malformed getent initgroups answer falls back to the local account's groups.
func TestGroupIDsMalformedInitgroupsFallsBackToLocalAccount(t *testing.T) {
	want, err := osUserLookupUID(currentUID(t))
	if err != nil {
		t.Fatalf("osUserLookupUID(%d): %v", currentUID(t), err)
	}
	wantIDs, err := osUserGroupIDs(want)
	if err != nil {
		t.Fatalf("osUserGroupIDs(%+v): %v", want, err)
	}
	fakeGetentWithOutput(t, "someone-else 1 2\n")

	ids, err := GroupIDs(want)
	if err != nil {
		t.Fatalf("GroupIDs(%+v): %v", want, err)
	}
	if !slices.Equal(ids, wantIDs) {
		t.Errorf("GroupIDs(%+v) = %v, want the local groups %v", want, ids, wantIDs)
	}
	for _, id := range []uint32{1, 2} {
		if slices.Contains(wantIDs, id) {
			continue
		}
		if slices.Contains(ids, id) {
			t.Errorf("GroupIDs(%+v) = %v, want the fake getent's group %d excluded", want, ids, id)
		}
	}
}

// TestParsePasswdRejectsMalformedEntries checks that parsePasswd rejects an
// invalid account name and a non-numeric gid.
func TestParsePasswdRejectsMalformedEntries(t *testing.T) {
	for _, c := range []struct {
		name    string
		in      string
		wantErr string
	}{
		{"invalid name", "bad name:x:1:1::/home/bob:/bin/sh\n", "cannot use account name"},
		{"invalid gid", "bob:x:1:not-a-number::/home/bob:/bin/sh\n", "passwd entry gid"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := parsePasswd([]byte(c.in))
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("parsePasswd(%q) error = %v, want it to mention %q", c.in, err, c.wantErr)
			}
		})
	}
}

// TestParseGroupRejectsMalformedEntries checks that parseGroup rejects empty
// output and an invalid account name.
func TestParseGroupRejectsMalformedEntries(t *testing.T) {
	for _, c := range []struct {
		name    string
		in      string
		wantErr string
	}{
		{"empty output", "", "empty output"},
		{"invalid name", "bad name:x:1:\n", "cannot use account name"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, _, err := parseGroup([]byte(c.in))
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("parseGroup(%q) error = %v, want it to mention %q", c.in, err, c.wantErr)
			}
		})
	}
}

// TestParseInitgroupsRejectsMalformedEntries checks that parseInitgroups
// rejects empty output, another account's name and a non-numeric id.
func TestParseInitgroupsRejectsMalformedEntries(t *testing.T) {
	for _, c := range []struct {
		name    string
		in      string
		wantErr string
	}{
		{"empty output", "", "empty output"},
		{"other account", "other 1 2\n", "does not start with"},
		{"invalid id", "bob 1 not-a-number\n", "initgroups entry"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := parseInitgroups([]byte(c.in), "bob")
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("parseInitgroups(%q) error = %v, want it to mention %q", c.in, err, c.wantErr)
			}
		})
	}
}

// TestPlatformLookupsCurrentAccount checks that the platform lookups resolve
// the current account and its primary group.
func TestPlatformLookupsCurrentAccount(t *testing.T) {
	uid := currentUID(t)
	u, err := LookupUID(uid)
	if err != nil {
		t.Fatalf("LookupUID(%d): %v", uid, err)
	}
	if u.UID != uid || u.Name == "" || u.Home == "" {
		t.Errorf("LookupUID(%d) = %+v, want the current account", uid, u)
	}

	byName, err := LookupName(u.Name)
	if err != nil {
		t.Fatalf("LookupName(%q): %v", u.Name, err)
	}
	if byName != u {
		t.Errorf("LookupName(%q) = %+v, want %+v", u.Name, byName, u)
	}

	group, err := user.LookupGroupId(strconv.FormatUint(uint64(os.Getgid()), 10))
	if err != nil {
		t.Fatalf("user.LookupGroupId(%d): %v", os.Getgid(), err)
	}
	gid, err := LookupGroup(group.Name)
	if err != nil {
		t.Fatalf("LookupGroup(%q): %v", group.Name, err)
	}
	if gid != uint32(os.Getgid()) {
		t.Errorf("LookupGroup(%q) = %d, want %d", group.Name, gid, os.Getgid())
	}

	ids, err := GroupIDs(u)
	if err != nil {
		t.Fatalf("GroupIDs(%q): %v", u.Name, err)
	}
	found := false
	for _, id := range ids {
		if id == uint32(os.Getgid()) {
			found = true
		}
	}
	if !found {
		t.Errorf("GroupIDs(%q) = %v, want it to contain the primary gid %d", u.Name, ids, os.Getgid())
	}
}
