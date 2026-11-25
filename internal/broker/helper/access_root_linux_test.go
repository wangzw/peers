//go:build linux

package helper

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

const accessCheckIdentityBound = processTimeout

const killProbeRole = "kill-probe"

func init() {
	testRoleHooks[killProbeRole] = runKillProbeRole
}

// TestAccessCheckChildKeepsRootRealIdentity checks that the access-check child
// keeps real and saved uid 0 with the account as its effective identity,
// skipping without root.
func TestAccessCheckChildKeepsRootRealIdentity(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("run under sudo: the child must start as root")
	}
	acct := accessTestCallerAccount(t)
	exe := accessTestExecutable(t)

	proc, err := accessCheckStart(exe)(context.Background(), acct)
	if err != nil {
		t.Fatalf("starting the access-check child: %v", err)
	}
	t.Cleanup(func() {
		if proc.Kill != nil {
			_ = proc.Kill()
		}
		if proc.Wait != nil {
			_ = proc.Wait()
		}
	})

	pid := waitForAccessCheckChild(t, exe)
	ids := waitForAccountIdentity(t, pid, acct)

	t.Logf("account %s (uid %d gid %d groups %v): child uid %d/%d/%d gid %d/%d/%d groups %v",
		acct.Name, acct.UID, acct.GID, acct.Groups,
		ids.uid[0], ids.uid[1], ids.uid[2], ids.gid[0], ids.gid[1], ids.gid[2], ids.groups)

	if ids.uid[0] != 0 {
		t.Errorf("child real uid = %d, want 0: the caller could inspect or signal the helper", ids.uid[0])
	}
	if ids.uid[1] != acct.UID {
		t.Errorf("child effective uid = %d, want %d", ids.uid[1], acct.UID)
	}
	if ids.uid[2] != 0 {
		t.Errorf("child saved uid = %d, want 0: a later setuid would return to root", ids.uid[2])
	}
	if ids.gid[1] != acct.GID {
		t.Errorf("child effective gid = %d, want %d", ids.gid[1], acct.GID)
	}
	if !slices.Equal(ids.groups, acct.Groups) {
		t.Errorf("child groups = %v, want %v", ids.groups, acct.Groups)
	}

	runKillProbe(t, exe, pid, acct)

	// Closing stdin ends the child; the non-zero exit is expected because no
	// request was sent.
	if err := proc.Stdin.Close(); err != nil {
		t.Errorf("closing the child's stdin: %v", err)
	}
	_ = proc.Wait()
}

func accessTestCallerAccount(t *testing.T) Account {
	t.Helper()

	var candidates []uint32
	if raw := os.Getenv("SUDO_UID"); raw != "" {
		if uid, err := strconv.ParseUint(raw, 10, 32); err == nil && uid != 0 {
			candidates = append(candidates, uint32(uid))
		}
	}
	if u, err := user.Lookup("nobody"); err == nil {
		if uid, err := strconv.ParseUint(u.Uid, 10, 32); err == nil && uid != 0 {
			candidates = append(candidates, uint32(uid))
		}
	}
	for _, uid := range candidates {
		if acct, err := LookupAccount(uid); err == nil {
			return acct
		}
	}
	t.Skip("no non-root account is available for the access-check child")
	return Account{}
}

func accessTestExecutable(t *testing.T) string {
	t.Helper()

	src, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	dir, err := os.MkdirTemp("", "peers-access-check-")
	if err != nil {
		t.Fatalf("creating a directory for the test binary: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("chmod %s: %v", dir, err)
	}

	dst := dir + "/" + "helper.test"
	if err := copyFile(src, dst, 0o755); err != nil {
		t.Fatalf("copying the test binary: %v", err)
	}
	return dst
}

func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return os.Chmod(dst, mode)
}

func waitForAccessCheckChild(t *testing.T, exe string) int {
	t.Helper()
	want := []string{exe, "internal", "access-check"}

	deadline := time.Now().Add(accessCheckIdentityBound)
	for {
		if pid, ok := findChildWithArgs(os.Getpid(), want); ok {
			return pid
		}
		if time.Now().After(deadline) {
			t.Fatalf("no access-check child with args %q appeared", want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func findChildWithArgs(parent int, want []string) (int, bool) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0, false
	}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		status, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
		if err != nil {
			continue
		}
		if procPPID(status) != parent {
			continue
		}
		cmdline, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
		if err != nil {
			continue
		}
		args := strings.Split(string(cmdline), "\x00")
		for len(args) > 0 && args[len(args)-1] == "" {
			args = args[:len(args)-1]
		}
		if slices.Equal(args, want) {
			return pid, true
		}
	}
	return 0, false
}

func procPPID(status []byte) int {
	for _, line := range strings.Split(string(status), "\n") {
		if !strings.HasPrefix(line, "PPid:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return -1
		}
		ppid, err := strconv.Atoi(fields[1])
		if err != nil {
			return -1
		}
		return ppid
	}
	return -1
}

type procIdentity struct {
	uid    [3]uint32 // real, effective, saved
	gid    [3]uint32 // real, effective, saved
	groups []uint32
}

func waitForAccountIdentity(t *testing.T, pid int, acct Account) procIdentity {
	t.Helper()
	deadline := time.Now().Add(accessCheckIdentityBound)
	for {
		ids, err := readProcIdentity(pid)
		if err == nil && ids.uid[1] == acct.UID {
			return ids
		}
		if time.Now().After(deadline) {
			if err != nil {
				t.Fatalf("reading /proc/%d/status: %v", pid, err)
			}
			t.Fatalf("child effective uid = %d after %v, want %d", ids.uid[1], accessCheckIdentityBound, acct.UID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func readProcIdentity(pid int) (procIdentity, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return procIdentity{}, err
	}
	var ids procIdentity
	for _, line := range strings.Split(string(data), "\n") {
		switch {
		case strings.HasPrefix(line, "Uid:"):
			if err := parseThreeIDs(strings.TrimPrefix(line, "Uid:"), &ids.uid); err != nil {
				return procIdentity{}, err
			}
		case strings.HasPrefix(line, "Gid:"):
			if err := parseThreeIDs(strings.TrimPrefix(line, "Gid:"), &ids.gid); err != nil {
				return procIdentity{}, err
			}
		case strings.HasPrefix(line, "Groups:"):
			groups, err := parseGroups(strings.TrimPrefix(line, "Groups:"))
			if err != nil {
				return procIdentity{}, err
			}
			ids.groups = groups
		}
	}
	return ids, nil
}

func parseThreeIDs(value string, dst *[3]uint32) error {
	fields := strings.Fields(value)
	if len(fields) < 3 {
		return fmt.Errorf("expected three ids, got %q", value)
	}
	for i := 0; i < 3; i++ {
		id, err := strconv.ParseUint(fields[i], 10, 32)
		if err != nil {
			return err
		}
		dst[i] = uint32(id)
	}
	return nil
}

func parseGroups(value string) ([]uint32, error) {
	fields := strings.Fields(value)
	groups := make([]uint32, 0, len(fields))
	for _, field := range fields {
		id, err := strconv.ParseUint(field, 10, 32)
		if err != nil {
			return nil, err
		}
		groups = append(groups, uint32(id))
	}
	return groups, nil
}

func runKillProbe(t *testing.T, exe string, pid int, acct Account) {
	t.Helper()

	cmd := exec.Command(exe)
	cmd.Env = replaceEnv(os.Environ(),
		"PEERS_TEST_HELPER_ROLE="+killProbeRole,
		"PEERS_TEST_KILL_PID="+strconv.Itoa(pid),
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{
		Uid:    acct.UID,
		Gid:    acct.GID,
		Groups: acct.Groups,
	}}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("kill probe as %s for pid %d: %v\n%s", acct.Name, pid, err, strings.TrimSpace(string(out)))
	}
	t.Logf("kill probe as %s (uid %d) for pid %d answered EPERM", acct.Name, acct.UID, pid)
}

func replaceEnv(env []string, kv ...string) []string {
	names := make(map[string]struct{}, len(kv))
	for _, entry := range kv {
		name, _, _ := strings.Cut(entry, "=")
		names[name] = struct{}{}
	}
	out := make([]string, 0, len(env)+len(kv))
	for _, entry := range env {
		name, _, _ := strings.Cut(entry, "=")
		if _, ok := names[name]; !ok {
			out = append(out, entry)
		}
	}
	return append(out, kv...)
}

func runKillProbeRole() int {
	pid, err := strconv.Atoi(os.Getenv("PEERS_TEST_KILL_PID"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "kill probe: bad pid: %v\n", err)
		return 2
	}
	//procsig:allow probes that an unprivileged process cannot signal this pid, so EPERM and success must differ
	err = syscall.Kill(pid, 0)
	switch {
	case errors.Is(err, syscall.EPERM):
		return 0
	case err != nil:
		fmt.Fprintf(os.Stderr, "kill(%d, 0) = %v, want EPERM\n", pid, err)
		return 1
	default:
		fmt.Fprintf(os.Stderr, "kill(%d, 0) succeeded, want EPERM\n", pid)
		return 1
	}
}
