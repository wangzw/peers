//go:build linux

package helper

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"os/user"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
)

// TestRootHelperReportsTargetIdentity checks that the helper reports the target
// account's uid, gid and groups, and skips without root.
func TestRootHelperReportsTargetIdentity(t *testing.T) {
	requireRoot(t)
	acct := accountByName(t, "nobody")
	exe := rootTestExecutable(t)

	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	proc, err := startHelperProcess(exe, acct, log, nil)
	if err != nil {
		t.Fatalf("startHelperProcess as %s: %v", acct.Name, err)
	}
	client := NewClient(proc.Stdout, proc.Stdin)
	ctx := testContext(t)
	ready, err := client.WaitReady(ctx)
	if err != nil {
		_ = proc.Kill()
		_ = proc.Wait()
		t.Fatalf("WaitReady: %v", err)
	}
	defer func() {
		_ = client.Close()
		_ = proc.Wait()
	}()

	if got := ready.GetUid(); got != acct.UID {
		t.Errorf("helper getuid = %d, want %d (%s)", got, acct.UID, acct.Name)
	}
	if got := ready.GetGid(); got != acct.GID {
		t.Errorf("helper getgid = %d, want %d (%s primary group)", got, acct.GID, acct.Name)
	}
	gotGroups := slices.Clone(ready.GetGroups())
	slices.Sort(gotGroups)
	if !slices.Equal(gotGroups, acct.Groups) {
		t.Errorf("helper groups = %v, want %v", gotGroups, acct.Groups)
	}
}

// TestRootAccessCheckUsesTargetCredentials checks group and other permission
// decisions made as the target account, and skips without root.
func TestRootAccessCheckUsesTargetCredentials(t *testing.T) {
	requireRoot(t)
	nobody := accountByName(t, "nobody")
	outsider, ok := accountOutsideGroup(t, nobody.GID)
	if !ok {
		t.Skip("no account outside nobody's primary group is available")
	}
	exe := rootTestExecutable(t)
	dir := rootTestDir(t)

	memberFile := writeOwnedFile(t, dir, "member.yaml", 0o640, 0, int(nobody.GID))
	otherWriteFile := writeOwnedFile(t, dir, "other-write.yaml", 0o757, 0, 0)
	otherNoWriteFile := writeOwnedFile(t, dir, "other-none.yaml", 0o750, 0, 0)

	checker := &AccessChecker{
		Exe:    exe,
		Lookup: LookupAccount,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	checks := []struct {
		name string
		uid  uint32
		path string
		need Access
		want bool
	}{
		{"group member reads 0640", nobody.UID, memberFile, AccessRead, true},
		{"non-member reads 0640", outsider.UID, memberFile, AccessRead, false},
		{"other writes 0757", nobody.UID, otherWriteFile, AccessWrite, true},
		{"other cannot write 0750", nobody.UID, otherNoWriteFile, AccessWrite, false},
	}
	for _, tc := range checks {
		t.Run(tc.name, func(t *testing.T) {
			allowed, err := checker.Check(context.Background(), tc.uid, tc.path, tc.need)
			if err != nil {
				t.Fatalf("Check: %v", err)
			}
			if allowed != tc.want {
				t.Errorf("allowed = %v, want %v", allowed, tc.want)
			}
		})
	}
}

func requireRoot(t *testing.T) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("requires root: run with sudo to exercise credential derivation")
	}
}

func accountByName(t *testing.T, name string) Account {
	t.Helper()
	u, err := user.Lookup(name)
	if err != nil {
		t.Skipf("account %q is not available: %v", name, err)
	}
	uid, err := strconv.ParseUint(u.Uid, 10, 32)
	if err != nil {
		t.Fatalf("account %q uid %q: %v", name, u.Uid, err)
	}
	acct, err := LookupAccount(uint32(uid))
	if err != nil {
		t.Fatalf("LookupAccount(%d): %v", uid, err)
	}
	return acct
}

func accountOutsideGroup(t *testing.T, gid uint32) (Account, bool) {
	t.Helper()
	for _, name := range []string{"daemon", "bin", "mail", "games", "man", "lp", "www-data"} {
		u, err := user.Lookup(name)
		if err != nil {
			continue
		}
		uid, err := strconv.ParseUint(u.Uid, 10, 32)
		if err != nil {
			continue
		}
		acct, err := LookupAccount(uint32(uid))
		if err != nil {
			continue
		}
		if !slices.Contains(acct.Groups, gid) {
			return acct, true
		}
	}
	return Account{}, false
}

func rootTestDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "peers-root-test-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("chmod %s: %v", dir, err)
	}
	return dir
}

func writeOwnedFile(t *testing.T, dir, name string, mode os.FileMode, uid, gid int) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	if err := os.Chown(path, uid, gid); err != nil {
		t.Fatalf("chown %s: %v", path, err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("chmod %s: %v", path, err)
	}
	return path
}

func rootTestExecutable(t *testing.T) string {
	t.Helper()
	src, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	dir := rootTestDir(t)
	dst := filepath.Join(dir, "helper.test")
	in, err := os.Open(src)
	if err != nil {
		t.Fatalf("opening %s: %v", src, err)
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o755)
	if err != nil {
		t.Fatalf("creating %s: %v", dst, err)
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		t.Fatalf("copying the test binary: %v", err)
	}
	if err := out.Close(); err != nil {
		t.Fatalf("closing %s: %v", dst, err)
	}
	if err := os.Chmod(dst, 0o755); err != nil {
		t.Fatalf("chmod %s: %v", dst, err)
	}
	return dst
}
