package helper

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/helperpb"
	"github.com/wangzw/peers/internal/broker/lookupgate"
)

// clearAccessIdentityEnv removes the access-check identity variables for the
// duration of the test; t.Setenv cannot unset a variable.
func clearAccessIdentityEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{accessCheckUIDEnv, accessCheckGIDEnv, accessCheckGroupsEnv} {
		value, ok := os.LookupEnv(name)
		if err := os.Unsetenv(name); err != nil {
			t.Fatalf("unsetting %s: %v", name, err)
		}
		if ok {
			t.Cleanup(func() { _ = os.Setenv(name, value) })
		}
	}
}

// TestParseIDList checks that the group list from the environment parses as decimal ids and reports a bad field with its text.
func TestParseIDList(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		want    []uint32
		errText string
	}{
		{"empty", "", nil, ""},
		{"one id", "27", []uint32{27}, ""},
		{"several ids", "27,109,1002", []uint32{27, 109, 1002}, ""},
		{"not a number", "27,x", nil, `"x"`},
		{"empty field", "27,,109", nil, `""`},
		{"negative", "-1", nil, `"-1"`},
		{"overflow", "4294967296", nil, `"4294967296"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseIDList(tc.value)
			if tc.errText != "" {
				if err == nil {
					t.Fatalf("parseIDList(%q) = %v, nil; want an error", tc.value, got)
				}
				if !strings.Contains(err.Error(), tc.errText) {
					t.Errorf("parseIDList(%q) error = %v, want it to name %s", tc.value, err, tc.errText)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseIDList(%q): %v", tc.value, err)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("parseIDList(%q) = %v, want %v", tc.value, got, tc.want)
			}
		})
	}
}

// TestEnterAccountFromEnv checks that the identity from the environment is skipped when unset or current and reported before any switch when unparsable.
func TestEnterAccountFromEnv(t *testing.T) {
	t.Run("no identity", func(t *testing.T) {
		clearAccessIdentityEnv(t)
		if err := enterAccountFromEnv(); err != nil {
			t.Fatalf("enterAccountFromEnv with no identity variables: %v", err)
		}
	})
	t.Run("bad uid", func(t *testing.T) {
		clearAccessIdentityEnv(t)
		t.Setenv(accessCheckUIDEnv, "not-a-number")
		err := enterAccountFromEnv()
		if err == nil || !strings.Contains(err.Error(), "account uid") {
			t.Fatalf("enterAccountFromEnv error = %v, want the unparsable uid", err)
		}
	})
	t.Run("current identity", func(t *testing.T) {
		clearAccessIdentityEnv(t)
		t.Setenv(accessCheckUIDEnv, fmt.Sprint(os.Geteuid()))
		t.Setenv(accessCheckGIDEnv, fmt.Sprint(os.Getegid()))
		t.Setenv(accessCheckGroupsEnv, "1")
		if err := enterAccountFromEnv(); err != nil {
			t.Fatalf("enterAccountFromEnv for the current identity: %v", err)
		}
	})
	t.Run("bad groups", func(t *testing.T) {
		clearAccessIdentityEnv(t)
		t.Setenv(accessCheckUIDEnv, fmt.Sprint(os.Geteuid()))
		t.Setenv(accessCheckGIDEnv, fmt.Sprint(os.Getegid()))
		t.Setenv(accessCheckGroupsEnv, "1,nope")
		err := enterAccountFromEnv()
		if err == nil || !strings.Contains(err.Error(), "account groups") {
			t.Fatalf("enterAccountFromEnv error = %v, want the unparsable group list", err)
		}
	})
}

// TestRunAccessCheckReportsIdentitySwitchFailure checks that a failed identity switch is reported in the response and never as allowed.
func TestRunAccessCheckReportsIdentitySwitchFailure(t *testing.T) {
	clearAccessIdentityEnv(t)
	t.Setenv(accessCheckUIDEnv, fmt.Sprint(os.Geteuid()))
	t.Setenv(accessCheckGIDEnv, fmt.Sprint(os.Getegid()))
	t.Setenv(accessCheckGroupsEnv, "bogus")

	var out bytes.Buffer
	err := RunAccessCheck(bytes.NewReader(nil), &out)
	if err == nil {
		t.Fatal("RunAccessCheck returned nil after the identity switch failed")
	}
	if !strings.Contains(err.Error(), "account groups") {
		t.Errorf("RunAccessCheck error = %v, want the group list failure", err)
	}

	var resp helperpb.AccessCheckResponse
	if decErr := newMsgReader(&out).read(&resp); decErr != nil {
		t.Fatalf("decoding the reported failure: %v", decErr)
	}
	if resp.GetAllowed() {
		t.Error("allowed = true after the identity switch failed")
	}
	if got := resp.GetError(); !strings.Contains(got, "account groups") {
		t.Errorf("response error = %q, want the group list failure", got)
	}
	if got, want := resp.GetUid(), uint32(os.Geteuid()); got != want {
		t.Errorf("response uid = %d, want the process's effective uid %d", got, want)
	}
	if got, want := resp.GetProtocolVersion(), helperpb.ProtocolVersion; got != want {
		t.Errorf("response protocol version = %d, want %d", got, want)
	}
}

// writeErrorWriter fails every write.
type writeErrorWriter struct{}

func (writeErrorWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

// TestRunAccessCheckWriteFailures checks that a response which cannot be written is returned as an error for both a result and a switch report.
func TestRunAccessCheckWriteFailures(t *testing.T) {
	t.Run("check result", func(t *testing.T) {
		clearAccessIdentityEnv(t)
		var in bytes.Buffer
		if err := newMsgWriter(&in).write(&helperpb.AccessCheckRequest{
			ProtocolVersion: helperpb.ProtocolVersion,
			Path:            filepath.Join(t.TempDir(), "config.yaml"),
			Access:          helperpb.Access_ACCESS_READ,
		}); err != nil {
			t.Fatalf("encoding the request: %v", err)
		}
		err := RunAccessCheck(&in, writeErrorWriter{})
		if err == nil || !strings.Contains(err.Error(), "writing response") {
			t.Fatalf("RunAccessCheck error = %v, want the failed response write", err)
		}
	})
	t.Run("identity switch report", func(t *testing.T) {
		clearAccessIdentityEnv(t)
		t.Setenv(accessCheckUIDEnv, fmt.Sprint(os.Geteuid()))
		t.Setenv(accessCheckGIDEnv, fmt.Sprint(os.Getegid()))
		t.Setenv(accessCheckGroupsEnv, "bogus")

		err := RunAccessCheck(bytes.NewReader(nil), writeErrorWriter{})
		if err == nil || !strings.Contains(err.Error(), "reporting it") {
			t.Fatalf("RunAccessCheck error = %v, want the failed report write", err)
		}
	})
}

// TestCheckDefaultLoggerDeniesUnknownAccount checks that a checker without a logger denies an unknown account and records its uid through slog.Default.
func TestCheckDefaultLoggerDeniesUnknownAccount(t *testing.T) {
	old := slog.Default()
	var logged bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })

	checker := &AccessChecker{
		Lookup:  func(uint32) (Account, error) { return Account{}, ErrNoAccount },
		Timeout: processTimeout,
	}
	allowed, err := checker.Check(context.Background(), 9001, "/config.yaml", AccessRead)
	if err != nil {
		t.Fatalf("Check with a default logger: %v", err)
	}
	if allowed {
		t.Error("allowed = true for an unknown account")
	}
	for _, want := range []string{"access_check_unknown_account", "uid=9001"} {
		if !strings.Contains(logged.String(), want) {
			t.Errorf("default logger output %q does not contain %q", logged.String(), want)
		}
	}
}

// TestCheckAccountSlotTableFullIsError checks that a new account is refused with errAccessSlotTableFull and starts no process when every slot entry is held.
func TestCheckAccountSlotTableFullIsError(t *testing.T) {
	uid := uint32(500000)
	var started atomic.Int32
	checker := &AccessChecker{
		Lookup: func(uint32) (Account, error) { return Account{UID: uid, GID: uid}, nil },
		Start: func(context.Context, Account) (*Process, error) {
			started.Add(1)
			return nil, errors.New("must not start")
		},
		Timeout:       processTimeout,
		Logger:        accessTestLogger(),
		MaxConcurrent: 8,
		MaxPerAccount: 2,
	}

	slots := checker.slotSet()
	ctx := context.Background()
	held := make([]*accessAccountSlot, 0, maxAccessAccounts)
	for i := 0; i < maxAccessAccounts; i++ {
		entry, err := slots.acquireAccount(ctx, uint32(i+1))
		if err != nil {
			t.Fatalf("account %d: %v", i+1, err)
		}
		held = append(held, entry)
	}
	t.Cleanup(func() {
		for _, entry := range held {
			slots.releaseAccount(entry)
		}
	})

	allowed, err := checker.Check(ctx, uid, "/config.yaml", AccessRead)
	if err == nil || allowed {
		t.Fatalf("Check = (%v, %v), want a refused check when the slot table is full", allowed, err)
	}
	if !errors.Is(err, errAccessSlotTableFull) {
		t.Errorf("Check error = %v, want it to wrap %v", err, errAccessSlotTableFull)
	}
	if got := started.Load(); got != 0 {
		t.Errorf("started %d check processes after the slot table refused the check, want 0", got)
	}
}

// waitAccessSlotHeld waits until a check holds uid's account slot, which means
// it has resolved the account and is waiting for a global slot.
func waitAccessSlotHeld(t *testing.T, slots *accessSlots, uid uint32) {
	t.Helper()
	deadline := time.Now().Add(processTimeout)
	for {
		slots.mu.Lock()
		entry := slots.accounts[uid]
		held := entry != nil && len(entry.slots) > 0
		slots.mu.Unlock()
		if held {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("uid %d did not take its account slot within %s", uid, processTimeout)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestCheckGlobalSlotWaitTimesOut checks that a check holding its account slot fails with ErrNoCheckSlot when its context ends during the global slot wait.
func TestCheckGlobalSlotWaitTimesOut(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	releaseAll := func() { once.Do(func() { close(release) }) }
	t.Cleanup(releaseAll)

	started := make(chan uint32, 4)
	checker := &AccessChecker{
		Lookup:        func(uid uint32) (Account, error) { return Account{UID: uid, GID: uid}, nil },
		Start:         accessGateStart(started, release),
		Timeout:       processTimeout,
		Logger:        accessTestLogger(),
		MaxConcurrent: 1,
		MaxPerAccount: 2,
	}

	first := checkAsync(checker, 1000)
	waitStartedAccount(t, started)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	blocked := make(chan checkResult, 1)
	go func() {
		allowed, err := checker.Check(ctx, 1001, "/config.yaml", AccessRead)
		blocked <- checkResult{allowed: allowed, err: err}
	}()

	// Cancel only once the check holds its account slot, so the failure comes
	// from the global slot wait rather than from the lookup gate.
	waitAccessSlotHeld(t, checker.slotSet(), 1001)
	cancel()

	var res checkResult
	select {
	case res = <-blocked:
	case <-time.After(processTimeout):
		t.Fatal("Check did not return after its context was cancelled")
	}
	if res.err == nil || res.allowed {
		t.Fatalf("Check = (%v, %v), want a failed check when no global slot freed up", res.allowed, res.err)
	}
	if !errors.Is(res.err, ErrNoCheckSlot) {
		t.Errorf("Check error = %v, want it to wrap ErrNoCheckSlot", res.err)
	}
	if !strings.Contains(res.err.Error(), "no check slot became free") {
		t.Errorf("Check error = %v, want it to name the missing check slot", res.err)
	}
	select {
	case uid := <-started:
		t.Errorf("uid %d started a check while the only global slot was taken", uid)
	default:
	}

	releaseAll()
	if res := <-first; res.err != nil {
		t.Errorf("first check: %v", res.err)
	}
}

// TestCheckStartProcessFailure checks that a check whose process cannot start fails with the start error and is never allowed.
func TestCheckStartProcessFailure(t *testing.T) {
	uid := uint32(os.Geteuid())
	checker := &AccessChecker{
		Exe:     filepath.Join(t.TempDir(), "no-such-peers"),
		Lookup:  func(uint32) (Account, error) { return Account{UID: uid, GID: uid}, nil },
		Timeout: processTimeout,
		Logger:  accessTestLogger(),
	}

	allowed, err := checker.Check(context.Background(), uid, "/config.yaml", AccessRead)
	if err == nil || allowed {
		t.Fatalf("Check = (%v, %v), want a failed check when the process cannot start", allowed, err)
	}
	if !strings.Contains(err.Error(), "starting the process") {
		t.Errorf("Check error = %v, want it to name the start step", err)
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Check error = %v, want it to carry the missing executable", err)
	}
}

// TestCheckRequestWriteFailure checks that a check whose request cannot be written kills and reaps the process and reports the write failure.
func TestCheckRequestWriteFailure(t *testing.T) {
	uid := uint32(os.Geteuid())
	var kills, waits atomic.Int32
	reqR, reqW := io.Pipe()
	t.Cleanup(func() { _ = reqR.Close() })
	if err := reqW.Close(); err != nil {
		t.Fatalf("closing the request pipe: %v", err)
	}
	start := func(context.Context, Account) (*Process, error) {
		return &Process{
			Stdin:  reqW,
			Stdout: bytes.NewReader(nil),
			Wait:   func() error { waits.Add(1); return nil },
			Kill:   func() error { kills.Add(1); return nil },
		}, nil
	}
	checker := &AccessChecker{
		Lookup:  func(uint32) (Account, error) { return Account{UID: uid, GID: uid}, nil },
		Start:   start,
		Timeout: processTimeout,
		Logger:  accessTestLogger(),
	}

	allowed, err := checker.Check(context.Background(), uid, "/config.yaml", AccessRead)
	if err == nil || allowed {
		t.Fatalf("Check = (%v, %v), want a failed check when the request cannot be written", allowed, err)
	}
	if !strings.Contains(err.Error(), "sending the request") {
		t.Errorf("Check error = %v, want it to name the request write", err)
	}
	if got := kills.Load(); got != 1 {
		t.Errorf("killed the check process %d times, want 1", got)
	}
	if got := waits.Load(); got != 1 {
		t.Errorf("reaped the check process %d times, want 1", got)
	}
}

// TestCheckResultReadFailure checks that a check process closing stdout without a response fails the check and is killed and reaped.
func TestCheckResultReadFailure(t *testing.T) {
	uid := uint32(os.Geteuid())
	var kills, waits atomic.Int32
	start := func(context.Context, Account) (*Process, error) {
		reqR, reqW := io.Pipe()
		respR, respW := io.Pipe()
		go func() {
			var req helperpb.AccessCheckRequest
			if err := newMsgReader(reqR).read(&req); err != nil {
				return
			}
			_ = respW.Close()
		}()
		return &Process{
			Stdin:  reqW,
			Stdout: respR,
			Wait:   func() error { waits.Add(1); return nil },
			Kill: func() error {
				kills.Add(1)
				_ = reqW.Close()
				_ = respW.Close()
				return nil
			},
		}, nil
	}
	checker := &AccessChecker{
		Lookup:  func(uint32) (Account, error) { return Account{UID: uid, GID: uid}, nil },
		Start:   start,
		Timeout: processTimeout,
		Logger:  accessTestLogger(),
	}

	allowed, err := checker.Check(context.Background(), uid, "/config.yaml", AccessRead)
	if err == nil || allowed {
		t.Fatalf("Check = (%v, %v), want a failed check when the result cannot be read", allowed, err)
	}
	if !strings.Contains(err.Error(), "reading the result") {
		t.Errorf("Check error = %v, want it to name the result read", err)
	}
	if got := kills.Load(); got != 1 {
		t.Errorf("killed the check process %d times, want 1", got)
	}
	if got := waits.Load(); got != 1 {
		t.Errorf("reaped the check process %d times, want 1", got)
	}
}

// TestCheckProtocolVersionMismatch checks that a response for another protocol version is rejected instead of read as an allow.
func TestCheckProtocolVersionMismatch(t *testing.T) {
	uid := uint32(os.Geteuid())
	start, _ := pipeStart(t, func(*helperpb.AccessCheckRequest) (*helperpb.AccessCheckResponse, bool) {
		return &helperpb.AccessCheckResponse{
			ProtocolVersion: helperpb.ProtocolVersion + 1,
			Uid:             uid,
			Allowed:         true,
		}, true
	})
	checker := &AccessChecker{
		Lookup:  func(uint32) (Account, error) { return Account{UID: uid, GID: uid}, nil },
		Start:   start,
		Timeout: processTimeout,
		Logger:  accessTestLogger(),
	}

	allowed, err := checker.Check(context.Background(), uid, "/config.yaml", AccessRead)
	if err == nil || allowed {
		t.Fatalf("Check = (%v, %v), want a failed check for another protocol version", allowed, err)
	}
	if !strings.Contains(err.Error(), "reported protocol version") {
		t.Errorf("Check error = %v, want it to name the protocol version", err)
	}
}

// TestCheckWaitFailure checks that a check process which answers correctly but exits unsuccessfully fails the check with its exit error.
func TestCheckWaitFailure(t *testing.T) {
	uid := uint32(os.Geteuid())
	waitErr := errors.New("exit status 1")
	start := func(context.Context, Account) (*Process, error) {
		reqR, reqW := io.Pipe()
		respR, respW := io.Pipe()
		go func() {
			var req helperpb.AccessCheckRequest
			if err := newMsgReader(reqR).read(&req); err != nil {
				return
			}
			_ = newMsgWriter(respW).write(&helperpb.AccessCheckResponse{
				ProtocolVersion: helperpb.ProtocolVersion,
				Uid:             uid,
				Allowed:         true,
			})
			_ = respW.Close()
		}()
		return &Process{
			Stdin:  reqW,
			Stdout: respR,
			Wait:   func() error { return waitErr },
			Kill:   func() error { _ = reqW.Close(); _ = respW.Close(); return nil },
		}, nil
	}
	checker := &AccessChecker{
		Lookup:  func(uint32) (Account, error) { return Account{UID: uid, GID: uid}, nil },
		Start:   start,
		Timeout: processTimeout,
		Logger:  accessTestLogger(),
	}

	allowed, err := checker.Check(context.Background(), uid, "/config.yaml", AccessRead)
	if err == nil || allowed {
		t.Fatalf("Check = (%v, %v), want a failed check when the process exits unsuccessfully", allowed, err)
	}
	if !errors.Is(err, waitErr) {
		t.Errorf("Check error = %v, want it to carry the exit status", err)
	}
	if !strings.Contains(err.Error(), "the check process failed") {
		t.Errorf("Check error = %v, want it to name the exit", err)
	}
}

// TestCheckLookupGateWaitsUntilCancelled checks that a check which cannot enter the lookup gate fails with DeadlineExceeded before resolving the account.
func TestCheckLookupGateWaitsUntilCancelled(t *testing.T) {
	releases := make([]func(), 0, lookupgate.Concurrency)
	for i := 0; i < lookupgate.Concurrency; i++ {
		release, err := lookupgate.Acquire(context.Background())
		if err != nil {
			t.Fatalf("taking lookup slot %d: %v", i+1, err)
		}
		releases = append(releases, release)
	}
	t.Cleanup(func() {
		for _, release := range releases {
			release()
		}
	})

	var lookups atomic.Int32
	checker := &AccessChecker{
		Lookup:  func(uint32) (Account, error) { lookups.Add(1); return Account{}, nil },
		Timeout: processTimeout,
		Logger:  accessTestLogger(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	allowed, err := checker.Check(ctx, 9002, "/config.yaml", AccessRead)
	if err == nil || allowed {
		t.Fatalf("Check = (%v, %v), want a failed check when the lookup gate stays full", allowed, err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Check error = %v, want it to carry the lookup deadline", err)
	}
	if got := lookups.Load(); got != 0 {
		t.Errorf("ran %d account lookups after the gate wait expired, want 0", got)
	}
}

// TestHumanDuration checks the wording humanDuration gives the check budget.
func TestHumanDuration(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{0, "1 second"},
		{500 * time.Millisecond, "1 second"},
		{time.Second, "1 second"},
		{1500 * time.Millisecond, "2 seconds"},
		{2 * time.Second, "2 seconds"},
		{90 * time.Second, "90 seconds"},
	}
	for _, tc := range cases {
		if got := humanDuration(tc.d); got != tc.want {
			t.Errorf("humanDuration(%v) = %q, want %q", tc.d, got, tc.want)
		}
	}
}

// TestCheckAccessCheckStartRunsTheCheckProcess checks that the default start path runs the access-check subcommand and returns its answer.
func TestCheckAccessCheckStartRunsTheCheckProcess(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	uid, gid := uint32(os.Geteuid()), uint32(os.Getegid())
	path := writeAccessTestFile(t, t.TempDir(), "config.yaml", 0o600)
	checker := &AccessChecker{
		Exe: exe,
		Lookup: func(uint32) (Account, error) {
			return Account{Name: "test", UID: uid, GID: gid, Groups: []uint32{gid}, Home: t.TempDir()}, nil
		},
		Timeout: processTimeout,
		Logger:  accessTestLogger(),
	}

	allowed, err := checker.Check(context.Background(), uid, path, AccessRead)
	if err != nil {
		t.Fatalf("Check through the default start path: %v", err)
	}
	if !allowed {
		t.Error("allowed = false for a file the account can read, want true")
	}
}

// TestAccessCheckStartStdinPipeFailure checks that a start whose first pipe cannot be created returns that error and no process.
func TestAccessCheckStartStdinPipeFailure(t *testing.T) {
	boom := errors.New("stdin pipe failure")
	stubHelperPipe(t, 1, boom)

	proc, err := accessCheckStart("unused")(context.Background(), Account{UID: uint32(os.Geteuid())})
	if proc != nil {
		t.Fatal("accessCheckStart returned a process after the stdin pipe failed")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("accessCheckStart error = %v, want the stdin pipe error", err)
	}
}

// TestAccessCheckStartStdoutPipeFailure checks that a start whose second pipe cannot be created closes both ends of the stdin pipe.
func TestAccessCheckStartStdoutPipeFailure(t *testing.T) {
	boom := errors.New("stdout pipe failure")
	rec := stubHelperPipe(t, 2, boom)

	proc, err := accessCheckStart("unused")(context.Background(), Account{UID: uint32(os.Geteuid())})
	if proc != nil {
		t.Fatal("accessCheckStart returned a process after the stdout pipe failed")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("accessCheckStart error = %v, want the stdout pipe error", err)
	}
	if len(rec.pipes) != 1 {
		t.Fatalf("created %d stdin pipes, want 1", len(rec.pipes))
	}
	assertPipeClosed(t, "stdin writer", rec.pipes[0][1])
	assertPipeClosed(t, "stdin read end", rec.pipes[0][0])
}
