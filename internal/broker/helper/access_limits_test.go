package helper

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/helperpb"
)

// accessGateStart returns a Start function whose fake check process blocks
// until release is closed. Each call reports its uid on started before it
// blocks, so a test can count how many checks are running at once.
func accessGateStart(started chan<- uint32, release <-chan struct{}) func(context.Context, Account) (*Process, error) {
	return func(ctx context.Context, acct Account) (*Process, error) {
		started <- acct.UID
		reqR, reqW := io.Pipe()
		respR, respW := io.Pipe()
		go func() {
			var req helperpb.AccessCheckRequest
			if err := newMsgReader(reqR).read(&req); err != nil {
				return
			}
			select {
			case <-release:
			case <-ctx.Done():
				return
			}
			_ = newMsgWriter(respW).write(&helperpb.AccessCheckResponse{
				ProtocolVersion: helperpb.ProtocolVersion,
				Uid:             acct.UID,
				Allowed:         true,
			})
			_ = respW.Close()
		}()
		return &Process{
			Stdin:  reqW,
			Stdout: respR,
			Wait:   func() error { return nil },
			Kill:   func() error { _ = reqW.Close(); _ = respW.Close(); return nil },
		}, nil
	}
}

// TestCheckLimitsConcurrentChecksPerAccount reproduces the unbounded process
// growth: without a per-account slot one account could start one check process
// per call. The zero value of the checker must take the documented default of
// two checks per account.
func TestCheckLimitsConcurrentChecksPerAccount(t *testing.T) {
	uid := uint32(1000)
	const checks = 5
	const maxPerAccount = 2

	started := make(chan uint32, checks)
	release := make(chan struct{})
	checker := &AccessChecker{
		Lookup:  func(uint32) (Account, error) { return Account{UID: uid}, nil },
		Start:   accessGateStart(started, release),
		Timeout: 5 * time.Second,
		Logger:  accessTestLogger(),
	}

	var wg sync.WaitGroup
	results := make(chan error, checks)
	for i := 0; i < checks; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			allowed, err := checker.Check(context.Background(), uid, "/config.yaml", AccessRead)
			switch {
			case err != nil:
				results <- err
			case !allowed:
				results <- errors.New("check was denied")
			}
		}()
	}

	waitStarted := func(count int) {
		for i := 0; i < count; i++ {
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				close(release)
				wg.Wait()
				t.Fatalf("only %d of %d checks started within 5s, want %d", i, checks, count)
			}
		}
	}

	waitStarted(maxPerAccount)
	select {
	case <-started:
		close(release)
		wg.Wait()
		t.Fatal("a third check for the same account started while two were still running: the per-account limit is not enforced")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	wg.Wait()
	close(results)
	for err := range results {
		t.Errorf("check: %v", err)
	}
}

// checkResult is the outcome of one Check run in a test goroutine.
type checkResult struct {
	allowed bool
	err     error
}

// checkAsync runs one Check in the background.
func checkAsync(checker *AccessChecker, uid uint32) <-chan checkResult {
	out := make(chan checkResult, 1)
	go func() {
		allowed, err := checker.Check(context.Background(), uid, "/config.yaml", AccessRead)
		out <- checkResult{allowed: allowed, err: err}
	}()
	return out
}

// waitStartedAccount receives one started uid, failing the test if none starts
// within timeout.
func waitStartedAccount(t *testing.T, started <-chan uint32, timeout time.Duration) uint32 {
	t.Helper()
	select {
	case uid := <-started:
		return uid
	case <-time.After(timeout):
		t.Fatalf("no check started within %s", timeout)
		return 0
	}
}

// TestCheckPerAccountSlotsLeaveGlobalSlotsForOthers checks the order in which a
// check takes its slots: the account's slot first and the global slot second.
// One account sends four checks, twice its own share, and only after they have
// taken their slots does a second account send two. The first account may hold
// two global slots, one per running check, and never more; a checker that took
// the global slot first would hold four global slots for the same account while
// two of its checks wait for an account slot, and the second account could not
// start.
func TestCheckPerAccountSlotsLeaveGlobalSlotsForOthers(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	releaseAll := func() { once.Do(func() { close(release) }) }
	t.Cleanup(releaseAll)

	started := make(chan uint32, 16)
	looked := make(chan struct{}, 8)
	checker := &AccessChecker{
		Lookup: func(uid uint32) (Account, error) {
			looked <- struct{}{}
			return Account{UID: uid}, nil
		},
		Start:         accessGateStart(started, release),
		Timeout:       5 * time.Second,
		Logger:        accessTestLogger(),
		MaxConcurrent: 4,
		MaxPerAccount: 2,
	}

	var results []<-chan checkResult
	for i := 0; i < 4; i++ {
		results = append(results, checkAsync(checker, 1000))
	}
	// Wait until every check of the first account has passed the account lookup
	// and is about to take its slots.
	for i := 0; i < 4; i++ {
		select {
		case <-looked:
		case <-time.After(5 * time.Second):
			releaseAll()
			t.Fatalf("only %d checks reached the slot acquisition", i)
		}
	}
	seen := map[uint32]int{}
	for i := 0; i < 2; i++ {
		seen[waitStartedAccount(t, started, 5*time.Second)]++
	}
	if seen[1000] != 2 {
		t.Fatalf("uid 1000 started %d checks, want its per-account share of 2", seen[1000])
	}
	// The account's other two checks may hold a global slot only if the checker
	// takes the global slot first. Give them time to reach that point, then
	// inspect the global semaphore directly: two slots may be held, not four.
	time.Sleep(100 * time.Millisecond)
	if got := len(checker.slotSet().global); got != 2 {
		releaseAll()
		t.Fatalf("the account holds %d global slots while two of its checks wait for an account slot, want 2", got)
	}

	// The second account gets the two free global slots.
	for i := 0; i < 2; i++ {
		results = append(results, checkAsync(checker, 1001))
	}
	for i := 0; i < 2; i++ {
		uid := waitStartedAccount(t, started, 2*time.Second)
		if uid != 1001 {
			releaseAll()
			t.Fatalf("started uid %d, want the second account 1001", uid)
		}
	}

	releaseAll()
	for _, r := range results {
		res := <-r
		if res.err != nil {
			t.Errorf("check: %v", res.err)
		} else if !res.allowed {
			t.Error("check was denied")
		}
	}
}

// TestCheckGlobalSlotWaitsForASlotToFree checks that a check which finds the
// global slots taken waits and then starts once one is released, within its
// timeout budget.
func TestCheckGlobalSlotWaitsForASlotToFree(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	releaseAll := func() { once.Do(func() { close(release) }) }
	t.Cleanup(releaseAll)

	started := make(chan uint32, 16)
	checker := &AccessChecker{
		Lookup:        func(uid uint32) (Account, error) { return Account{UID: uid}, nil },
		Start:         accessGateStart(started, release),
		Timeout:       5 * time.Second,
		Logger:        accessTestLogger(),
		MaxConcurrent: 2,
		MaxPerAccount: 2,
	}

	first := checkAsync(checker, 1000)
	second := checkAsync(checker, 1001)
	waitStartedAccount(t, started, 5*time.Second)
	waitStartedAccount(t, started, 5*time.Second)

	third := checkAsync(checker, 1002)
	select {
	case uid := <-started:
		t.Fatalf("uid %d started while both global slots were taken", uid)
	case <-time.After(100 * time.Millisecond):
	}

	releaseAll()
	if uid := waitStartedAccount(t, started, 2*time.Second); uid != 1002 {
		t.Fatalf("started uid %d, want the waiting account 1002", uid)
	}
	for _, r := range []<-chan checkResult{first, second, third} {
		if res := <-r; res.err != nil {
			t.Errorf("check: %v", res.err)
		}
	}
}

// TestCheckNoSlotWithinBudgetFails checks that a check which cannot take a slot
// before its timeout fails and is never allowed.
func TestCheckNoSlotWithinBudgetFails(t *testing.T) {
	uid := uint32(1000)
	release := make(chan struct{})
	var once sync.Once
	releaseAll := func() { once.Do(func() { close(release) }) }
	t.Cleanup(releaseAll)

	started := make(chan uint32, 4)
	var logBuf bytes.Buffer
	checker := &AccessChecker{
		Lookup:        func(uint32) (Account, error) { return Account{UID: uid, GID: uid}, nil },
		Start:         accessGateStart(started, release),
		Timeout:       5 * time.Second,
		Logger:        slog.New(slog.NewTextHandler(&logBuf, nil)),
		MaxConcurrent: 8,
		MaxPerAccount: 1,
	}

	first := checkAsync(checker, uid)
	waitStartedAccount(t, started, 5*time.Second)

	// A shorter caller deadline bounds this check so the busy path is reached
	// while the first check still holds the account's only slot.
	busyCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	allowed, err := checker.Check(busyCtx, uid, "/config.yaml", AccessRead)
	if err == nil {
		t.Fatalf("Check = (%v, nil), want an error when the account's slot stays taken", allowed)
	}
	if allowed {
		t.Error("allowed = true after the slot wait expired")
	}
	if !strings.Contains(err.Error(), "no check slot became free") {
		t.Errorf("Check error = %v, want it to name the missing check slot", err)
	}
	select {
	case <-started:
		t.Error("a check process started after the slot wait expired")
	default:
	}
	if got := strings.Count(logBuf.String(), "access_check_busy"); got != 1 {
		t.Errorf("logged access_check_busy %d times, want 1", got)
	}

	releaseAll()
	if res := <-first; res.err != nil {
		t.Errorf("first check: %v", res.err)
	}
}

// TestCheckBusyLogOncePerMinute checks that one account's access_check_busy
// record is written at most once a minute.
func TestCheckBusyLogOncePerMinute(t *testing.T) {
	uid := uint32(1000)
	release := make(chan struct{})
	var once sync.Once
	releaseAll := func() { once.Do(func() { close(release) }) }
	t.Cleanup(releaseAll)

	started := make(chan uint32, 4)
	var logBuf bytes.Buffer
	now := time.Unix(1000, 0)
	checker := &AccessChecker{
		Lookup:        func(uint32) (Account, error) { return Account{UID: uid, GID: uid}, nil },
		Start:         accessGateStart(started, release),
		Timeout:       5 * time.Second,
		Logger:        slog.New(slog.NewTextHandler(&logBuf, nil)),
		MaxConcurrent: 8,
		MaxPerAccount: 1,
		now:           func() time.Time { return now },
	}

	first := checkAsync(checker, uid)
	waitStartedAccount(t, started, 5*time.Second)

	// A shorter caller deadline reaches the busy path quickly while the first
	// check keeps the account's only slot.
	busy := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		_, err := checker.Check(ctx, uid, "/config.yaml", AccessRead)
		return err
	}
	for i := 0; i < 3; i++ {
		if err := busy(); err == nil {
			t.Fatal("Check succeeded while the account's only slot was taken")
		}
	}
	if got := strings.Count(logBuf.String(), "access_check_busy"); got != 1 {
		t.Errorf("logged access_check_busy %d times within one minute, want 1", got)
	}

	now = now.Add(61 * time.Second)
	if err := busy(); err == nil {
		t.Fatal("Check succeeded while the account's only slot was taken")
	}
	if got := strings.Count(logBuf.String(), "access_check_busy"); got != 2 {
		t.Errorf("logged access_check_busy %d times after the minute passed, want 2", got)
	}

	releaseAll()
	<-first
}

// TestCheckTimeoutReleasesItsSlot checks that a check abandoned at its timeout
// returns its slot once the process has been stopped, so the next check runs.
func TestCheckTimeoutReleasesItsSlot(t *testing.T) {
	uid := uint32(1000)
	start, kills := pipeStart(t, func(*helperpb.AccessCheckRequest) (*helperpb.AccessCheckResponse, bool) {
		return nil, false
	})
	checker := &AccessChecker{
		Lookup:        func(uint32) (Account, error) { return Account{UID: uid, GID: uid}, nil },
		Start:         start,
		Timeout:       50 * time.Millisecond,
		Logger:        accessTestLogger(),
		MaxConcurrent: 8,
		MaxPerAccount: 1,
	}

	allowed, err := checker.Check(context.Background(), uid, "/config.yaml", AccessRead)
	if err == nil || allowed {
		t.Fatalf("Check = (%v, %v), want a timeout error", allowed, err)
	}
	if kills.Load() == 0 {
		t.Error("the timed-out check process was not killed")
	}

	// The abandoned check must have returned its slot: the next check runs.
	checker.Start, _ = pipeStart(t, func(*helperpb.AccessCheckRequest) (*helperpb.AccessCheckResponse, bool) {
		return &helperpb.AccessCheckResponse{
			ProtocolVersion: helperpb.ProtocolVersion,
			Uid:             uid,
			Allowed:         true,
		}, true
	})
	allowed, err = checker.Check(context.Background(), uid, "/config.yaml", AccessRead)
	if err != nil {
		t.Fatalf("check after a timed-out one: %v", err)
	}
	if !allowed {
		t.Error("check after a timed-out one was denied, want allowed: its slot was not returned")
	}
}
