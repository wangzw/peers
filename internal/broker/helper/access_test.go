package helper

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/wangzw/peers/internal/broker/helperpb"
)

func accessTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// skipUnlessAccessChecksAreMeaningful skips permission-sensitive cases under
// root, where access(2) allows everything.
func skipUnlessAccessChecksAreMeaningful(t *testing.T) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("access checks are meaningful only for a non-root account; covered by test/e2e/system")
	}
}

// writeAccessTestFile creates a file with an exact mode, unaffected by umask.
func writeAccessTestFile(t *testing.T, dir, name string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("chmod %s: %v", path, err)
	}
	return path
}

// runAccessCheck feeds one request to RunAccessCheck and decodes its response.
func runAccessCheck(t *testing.T, req *helperpb.AccessCheckRequest) (*helperpb.AccessCheckResponse, error) {
	t.Helper()
	var in bytes.Buffer
	if err := newMsgWriter(&in).write(req); err != nil {
		t.Fatalf("encoding request: %v", err)
	}
	var out bytes.Buffer
	if err := RunAccessCheck(&in, &out); err != nil {
		return nil, err
	}
	var resp helperpb.AccessCheckResponse
	if err := newMsgReader(&out).read(&resp); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	return &resp, nil
}

func TestRunAccessCheckReadWrite(t *testing.T) {
	skipUnlessAccessChecksAreMeaningful(t)

	dir := t.TempDir()
	cases := []struct {
		name  string
		mode  os.FileMode
		read  bool
		write bool
	}{
		{"read-only", 0o400, true, false},
		{"read-write", 0o600, true, true},
		{"owner-rwx-other-rwx", 0o757, true, true},
		{"owner-rwx-group-rx", 0o750, true, true},
		{"none", 0o000, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeAccessTestFile(t, dir, tc.name, tc.mode)
			checks := []struct {
				name   string
				access helperpb.Access
				want   bool
			}{
				{"read", helperpb.Access_ACCESS_READ, tc.read},
				{"write", helperpb.Access_ACCESS_WRITE, tc.write},
			}
			for _, check := range checks {
				resp, err := runAccessCheck(t, &helperpb.AccessCheckRequest{
					ProtocolVersion: helperpb.ProtocolVersion,
					Path:            path,
					Access:          check.access,
				})
				if err != nil {
					t.Fatalf("%s: %v", check.name, err)
				}
				if got := resp.GetAllowed(); got != check.want {
					t.Errorf("%s: allowed = %v, want %v", check.name, got, check.want)
				}
				if got := resp.GetError(); got != "" {
					t.Errorf("%s: error = %q, want empty", check.name, got)
				}
			}
		})
	}
}

func TestRunAccessCheckMissingFileDenied(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.yaml")
	resp, err := runAccessCheck(t, &helperpb.AccessCheckRequest{
		ProtocolVersion: helperpb.ProtocolVersion,
		Path:            path,
		Access:          helperpb.Access_ACCESS_READ,
	})
	if err != nil {
		t.Fatalf("RunAccessCheck: %v", err)
	}
	if resp.GetAllowed() {
		t.Error("allowed = true for a missing file, want false")
	}
	if got := resp.GetError(); got != "" {
		t.Errorf("error = %q, want empty: a missing file is a denial, not a failed check", got)
	}
}

// TestRunAccessCheckPathTooLongIsError checks that a path the kernel refuses
// for its length is reported as a failed check. Treating every non-nil error
// as a denial would hide a broken check behind "not allowed".
func TestRunAccessCheckPathTooLongIsError(t *testing.T) {
	resp, err := runAccessCheck(t, &helperpb.AccessCheckRequest{
		ProtocolVersion: helperpb.ProtocolVersion,
		Path:            strings.Repeat("a", 4097),
		Access:          helperpb.Access_ACCESS_READ,
	})
	if err != nil {
		t.Fatalf("RunAccessCheck: %v", err)
	}
	if resp.GetAllowed() {
		t.Error("allowed = true for a path the kernel cannot resolve, want false")
	}
	if got := resp.GetError(); got == "" {
		t.Error("error is empty, want the kernel's reason: a failed check is not a denial")
	}
}

func TestRunAccessCheckReportsUID(t *testing.T) {
	resp, err := runAccessCheck(t, &helperpb.AccessCheckRequest{
		ProtocolVersion: helperpb.ProtocolVersion,
		Path:            filepath.Join(t.TempDir(), "config.yaml"),
		Access:          helperpb.Access_ACCESS_READ,
	})
	if err != nil {
		t.Fatalf("RunAccessCheck: %v", err)
	}
	if got, want := resp.GetUid(), uint32(os.Getuid()); got != want {
		t.Errorf("uid = %d, want %d", got, want)
	}
	if got := resp.GetProtocolVersion(); got != helperpb.ProtocolVersion {
		t.Errorf("protocol version = %d, want %d", got, helperpb.ProtocolVersion)
	}
}

func TestRunAccessCheckRejectsBadRequest(t *testing.T) {
	cases := []struct {
		name string
		req  *helperpb.AccessCheckRequest
	}{
		{
			name: "wrong protocol version",
			req: &helperpb.AccessCheckRequest{
				ProtocolVersion: helperpb.ProtocolVersion + 1,
				Path:            "/config.yaml",
				Access:          helperpb.Access_ACCESS_READ,
			},
		},
		{
			name: "unspecified access",
			req: &helperpb.AccessCheckRequest{
				ProtocolVersion: helperpb.ProtocolVersion,
				Path:            "/config.yaml",
				Access:          helperpb.Access_ACCESS_UNSPECIFIED,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := runAccessCheck(t, tc.req)
			if err != nil {
				t.Fatalf("RunAccessCheck: %v", err)
			}
			if resp.GetAllowed() {
				t.Error("allowed = true, want false for a bad request")
			}
			if got := resp.GetError(); got == "" {
				t.Error("error is empty, want the reason for the rejection")
			}
			if got := resp.GetUid(); got != uint32(os.Getuid()) {
				t.Errorf("uid = %d, want %d", got, uint32(os.Getuid()))
			}
		})
	}
}

func TestRunAccessCheckEmptyInputIsError(t *testing.T) {
	var out bytes.Buffer
	err := RunAccessCheck(bytes.NewReader(nil), &out)
	if err == nil {
		t.Fatal("RunAccessCheck on empty stdin returned nil, want an error")
	}
	if out.Len() != 0 {
		t.Errorf("wrote %d bytes after failing to read a request, want 0", out.Len())
	}
}

// countingStart wraps a Start function and counts its calls.
func countingStart(inner func(context.Context, Account) (*Process, error)) (func(context.Context, Account) (*Process, error), *atomic.Int32) {
	var count atomic.Int32
	return func(ctx context.Context, acct Account) (*Process, error) {
		count.Add(1)
		return inner(ctx, acct)
	}, &count
}

// pipeStart returns a Start function backed by in-memory pipes. The child side
// reads one request and, when respond reports ok, writes the given response.
// Kill closes the pipes and counts the calls; Wait always succeeds.
func pipeStart(t *testing.T, respond func(*helperpb.AccessCheckRequest) (*helperpb.AccessCheckResponse, bool)) (func(context.Context, Account) (*Process, error), *atomic.Int32) {
	t.Helper()
	var kills atomic.Int32
	start := func(context.Context, Account) (*Process, error) {
		reqR, reqW := io.Pipe()
		respR, respW := io.Pipe()
		go func() {
			var req helperpb.AccessCheckRequest
			if err := newMsgReader(reqR).read(&req); err != nil {
				return
			}
			resp, ok := respond(&req)
			if !ok {
				return
			}
			_ = newMsgWriter(respW).write(resp)
		}()
		kill := func() error {
			kills.Add(1)
			_ = reqW.Close()
			_ = respW.Close()
			return nil
		}
		return &Process{
			Stdin:  reqW,
			Stdout: respR,
			Wait:   func() error { return nil },
			Kill:   kill,
		}, nil
	}
	return start, &kills
}

func TestAccessCheckCommandExecsAsRootWithAccountIdentity(t *testing.T) {
	acct := Account{
		Name:   "alice",
		UID:    1001,
		GID:    1002,
		Groups: []uint32{27, 109, 1002},
		Home:   "/home/alice",
	}
	cmd := accessCheckCommand("/usr/bin/peers", acct)

	if cmd.SysProcAttr == nil {
		t.Fatal("SysProcAttr is nil")
	}
	if cmd.SysProcAttr.Credential != nil {
		t.Errorf("Credential = %+v, want nil: the check must exec as root and drop only its effective identity",
			cmd.SysProcAttr.Credential)
	}
	if cmd.Stderr != nil {
		t.Error("Stderr is set, want nil so the child cannot write into the Broker's log")
	}

	wantEnv := []string{
		"HOME=/home/alice",
		"USER=alice",
		"LOGNAME=alice",
		"PATH=/usr/bin:/bin:/usr/sbin:/sbin",
		accessCheckUIDEnv + "=1001",
		accessCheckGIDEnv + "=1002",
		accessCheckGroupsEnv + "=27,109,1002",
	}
	if !slices.Equal(cmd.Env, wantEnv) {
		t.Errorf("Env = %q, want %q", cmd.Env, wantEnv)
	}
}

func TestCheckSpawnsEveryCall(t *testing.T) {
	skipUnlessAccessChecksAreMeaningful(t)

	path := writeAccessTestFile(t, t.TempDir(), "config.yaml", 0o400)
	start, calls := countingStart(testExecStart(t, "access-check"))
	checker := &AccessChecker{Start: start, Logger: accessTestLogger()}
	uid := uint32(os.Getuid())

	want := []bool{true, false, false}
	for i, wantAllowed := range want {
		if i == 1 {
			if err := os.Chmod(path, 0o000); err != nil {
				t.Fatalf("chmod: %v", err)
			}
		}
		allowed, err := checker.Check(context.Background(), uid, path, AccessRead)
		if err != nil {
			t.Fatalf("check %d: %v", i+1, err)
		}
		if allowed != wantAllowed {
			t.Errorf("check %d: allowed = %v, want %v", i+1, allowed, wantAllowed)
		}
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("started %d check processes, want 3: results must not be cached", got)
	}
}

func TestCheckUnknownAccountDenied(t *testing.T) {
	var started atomic.Int32
	var logBuf bytes.Buffer
	checker := &AccessChecker{
		Lookup: func(uint32) (Account, error) {
			return Account{}, fmt.Errorf("account database: %w", ErrNoAccount)
		},
		Start: func(context.Context, Account) (*Process, error) {
			started.Add(1)
			return nil, errors.New("must not start a check process")
		},
		Logger: slog.New(slog.NewTextHandler(&logBuf, nil)),
	}

	allowed, err := checker.Check(context.Background(), 424242, "/config.yaml", AccessRead)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if allowed {
		t.Error("allowed = true for a nonexistent account, want false")
	}
	if started.Load() != 0 {
		t.Error("started a check process for a nonexistent account")
	}
	if !strings.Contains(logBuf.String(), "access_check_unknown_account") {
		t.Errorf("log %q does not record msg=access_check_unknown_account", logBuf.String())
	}
}

func TestCheckUIDMismatchIsError(t *testing.T) {
	uid := uint32(os.Getuid())
	start, _ := pipeStart(t, func(*helperpb.AccessCheckRequest) (*helperpb.AccessCheckResponse, bool) {
		return &helperpb.AccessCheckResponse{
			ProtocolVersion: helperpb.ProtocolVersion,
			Uid:             uid + 1,
			Allowed:         true,
		}, true
	})
	checker := &AccessChecker{Start: start, Timeout: time.Second, Logger: accessTestLogger()}

	allowed, err := checker.Check(context.Background(), uid, "/config.yaml", AccessRead)
	if err == nil {
		t.Fatalf("Check = (%v, nil), want an error: the result came from another account", allowed)
	}
}

func TestCheckTimeout(t *testing.T) {
	uid := uint32(os.Getuid())
	start, kills := pipeStart(t, func(*helperpb.AccessCheckRequest) (*helperpb.AccessCheckResponse, bool) {
		return nil, false
	})
	const timeout = 100 * time.Millisecond
	checker := &AccessChecker{Start: start, Timeout: timeout, Logger: accessTestLogger()}

	// The check must give up at its own timeout. Waiting that bound plus the
	// package fallback catches a check that ignores the timeout without making
	// the assertion depend on scheduling.
	type result struct {
		allowed bool
		err     error
	}
	done := make(chan result, 1)
	go func() {
		allowed, err := checker.Check(context.Background(), uid, "/config.yaml", AccessRead)
		done <- result{allowed, err}
	}()
	var res result
	select {
	case res = <-done:
	case <-time.After(timeout + processTimeout):
		t.Fatalf("Check did not give up near the %v timeout", timeout)
	}
	if res.err == nil {
		t.Fatalf("Check = (%v, nil), want a timeout error", res.allowed)
	}
	if kills.Load() == 0 {
		t.Error("the check process was not killed on timeout")
	}
}

func TestCheckRealProcess(t *testing.T) {
	skipUnlessAccessChecksAreMeaningful(t)

	path := writeAccessTestFile(t, t.TempDir(), "config.yaml", 0o400)
	checker := &AccessChecker{Start: testExecStart(t, "access-check"), Logger: accessTestLogger()}
	uid := uint32(os.Getuid())

	allowed, err := checker.Check(context.Background(), uid, path, AccessRead)
	if err != nil {
		t.Fatalf("read check: %v", err)
	}
	if !allowed {
		t.Error("read check on a 0400 file = false, want true")
	}

	allowed, err = checker.Check(context.Background(), uid, path, AccessWrite)
	if err != nil {
		t.Fatalf("write check: %v", err)
	}
	if allowed {
		t.Error("write check on a 0400 file = true, want false")
	}
}

func TestCheckWithoutExecutableIsError(t *testing.T) {
	checker := &AccessChecker{Logger: accessTestLogger()}
	allowed, err := checker.Check(context.Background(), uint32(os.Getuid()), "/config.yaml", AccessRead)
	if err == nil {
		t.Fatalf("Check = (%v, nil), want an error when neither Exe nor Start is set", allowed)
	}
}

// TestCheckHelperReportedErrorIsError checks that an error the check process
// reports is a failed check, not a denial. A caller that ignored the reported
// error would turn a broken check into permission_denied.
func TestCheckHelperReportedErrorIsError(t *testing.T) {
	uid := uint32(os.Geteuid())
	start, _ := pipeStart(t, func(*helperpb.AccessCheckRequest) (*helperpb.AccessCheckResponse, bool) {
		return &helperpb.AccessCheckResponse{
			ProtocolVersion: helperpb.ProtocolVersion,
			Uid:             uid,
			Error:           "boom",
		}, true
	})
	checker := &AccessChecker{
		Lookup:  func(uint32) (Account, error) { return Account{UID: uid, GID: uid}, nil },
		Start:   start,
		Timeout: time.Second,
		Logger:  accessTestLogger(),
	}

	allowed, err := checker.Check(context.Background(), uid, "/config.yaml", AccessRead)
	if err == nil {
		t.Fatalf("Check = (%v, nil), want the check process's error", allowed)
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("Check error = %v, want it to carry %q", err, "boom")
	}
	if allowed {
		t.Error("allowed = true when the check process reported an error")
	}
}

// TestCheckErrorIsNotAllowed checks that a response carrying an error is never
// treated as an allow, even when its uid matches and its Allowed field is true.
func TestCheckErrorIsNotAllowed(t *testing.T) {
	uid := uint32(os.Geteuid())
	start, _ := pipeStart(t, func(*helperpb.AccessCheckRequest) (*helperpb.AccessCheckResponse, bool) {
		return &helperpb.AccessCheckResponse{
			ProtocolVersion: helperpb.ProtocolVersion,
			Uid:             uid,
			Allowed:         true,
			Error:           "boom",
		}, true
	})
	checker := &AccessChecker{
		Lookup:  func(uint32) (Account, error) { return Account{UID: uid, GID: uid}, nil },
		Start:   start,
		Timeout: time.Second,
		Logger:  accessTestLogger(),
	}

	allowed, err := checker.Check(context.Background(), uid, "/config.yaml", AccessRead)
	if err == nil {
		t.Fatalf("Check = (%v, nil), want the reported error", allowed)
	}
	if allowed {
		t.Error("allowed = true when the response carried an error")
	}
}

// TestCheckLookupFailureIsError checks that an account lookup failure is
// reported as a failed check and never spawns a check process.
func TestCheckLookupFailureIsError(t *testing.T) {
	var started atomic.Int32
	checker := &AccessChecker{
		Lookup: func(uint32) (Account, error) { return Account{}, errors.New("group db") },
		Start: func(context.Context, Account) (*Process, error) {
			started.Add(1)
			return nil, errors.New("must not start a check process")
		},
		Logger: accessTestLogger(),
	}

	allowed, err := checker.Check(context.Background(), 424242, "/config.yaml", AccessRead)
	if err == nil {
		t.Fatalf("Check = (%v, nil), want the account lookup error", allowed)
	}
	if !strings.Contains(err.Error(), "group db") {
		t.Errorf("Check error = %v, want it to carry %q", err, "group db")
	}
	if got := started.Load(); got != 0 {
		t.Errorf("started %d check processes, want 0 after a failed lookup", got)
	}
}

// inProcessStart runs RunAccessCheck in the test process over pipes. The parent
// side is exercised as it is with a child, but the check shares package state
// with the test, so a test can replace accessCheckSyscall.
func inProcessStart(t *testing.T) func(context.Context, Account) (*Process, error) {
	t.Helper()
	return func(context.Context, Account) (*Process, error) {
		reqR, reqW := io.Pipe()
		respR, respW := io.Pipe()
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = RunAccessCheck(reqR, respW)
			_ = respW.Close()
		}()
		return &Process{
			Stdin:  reqW,
			Stdout: respR,
			Wait:   func() error { <-done; return nil },
			Kill:   func() error { _ = reqW.Close(); _ = respW.Close(); return nil },
		}, nil
	}
}

// TestCheckFaccessat2RefusedIsError checks that a kernel or sandbox that
// refuses faccessat2 fails the check instead of falling back to the permission
// bits, which would ignore an access control list entry. ENOSYS and EPERM both
// mean the syscall did not judge the path; the Broker reports either as
// internal rather than as a denial.
func TestCheckFaccessat2RefusedIsError(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"ENOSYS", unix.ENOSYS},
		{"EPERM", unix.EPERM},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			restore := accessCheckSyscall
			accessCheckSyscall = func(string, uint32) error { return tc.err }
			t.Cleanup(func() { accessCheckSyscall = restore })

			uid := uint32(os.Geteuid())
			checker := &AccessChecker{
				Lookup:  func(uint32) (Account, error) { return Account{UID: uid, GID: uid}, nil },
				Start:   inProcessStart(t),
				Timeout: time.Second,
				Logger:  accessTestLogger(),
			}

			allowed, err := checker.Check(context.Background(), uid, "/config.yaml", AccessRead)
			if err == nil {
				t.Fatalf("Check = (%v, nil), want an error when faccessat2 is refused", allowed)
			}
			if !strings.Contains(err.Error(), tc.err.Error()) {
				t.Errorf("Check error = %v, want it to carry %q", err, tc.err.Error())
			}
			if allowed {
				t.Error("allowed = true when faccessat2 is refused")
			}
		})
	}
}

// TestBecomeAccountSkipsTheCurrentIdentity covers the root access check on
// macOS: the account database lists more groups than Darwin's NGROUPS limit, so
// setgroups would fail even though the process is already root. A check for the
// current effective identity must not touch the groups.
func TestBecomeAccountSkipsTheCurrentIdentity(t *testing.T) {
	groups := make([]uint32, 64)
	for i := range groups {
		groups[i] = uint32(i)
	}
	if err := becomeAccount(uint32(os.Geteuid()), uint32(os.Getegid()), groups); err != nil {
		t.Fatalf("becomeAccount for the current identity: %v", err)
	}
}

// TestBecomeAccountRejectsADifferentGID covers the other half of the skip
// condition: an equal uid alone does not skip the switch. A non-root process
// cannot change its groups, so the attempted switch reports an error.
func TestBecomeAccountRejectsADifferentGID(t *testing.T) {
	skipUnlessAccessChecksAreMeaningful(t)
	if err := becomeAccount(uint32(os.Geteuid()), uint32(os.Getegid())+1, nil); err == nil {
		t.Fatal("becomeAccount with the current uid and a different gid: got nil, want an error")
	}
}
