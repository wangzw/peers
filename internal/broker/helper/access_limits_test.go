package helper

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/helperpb"
)

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

// TestCheckLimitsConcurrentChecksPerAccount checks that the zero-value checker
// allows at most two checks per account at once.
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
			case <-time.After(processTimeout):
				close(release)
				wg.Wait()
				t.Fatalf("only %d of %d checks started within %v, want %d", i, checks, processTimeout, count)
			}
		}
	}

	waitStarted(maxPerAccount)
	// The window is the assertion: a third check that started would send to
	// started.
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

type checkResult struct {
	allowed bool
	err     error
}

func checkAsync(checker *AccessChecker, uid uint32) <-chan checkResult {
	out := make(chan checkResult, 1)
	go func() {
		allowed, err := checker.Check(context.Background(), uid, "/config.yaml", AccessRead)
		out <- checkResult{allowed: allowed, err: err}
	}()
	return out
}

func waitStartedAccount(t *testing.T, started <-chan uint32) uint32 {
	t.Helper()
	select {
	case uid := <-started:
		return uid
	case <-time.After(processTimeout):
		t.Fatalf("no check started within %s", processTimeout)
		return 0
	}
}

// TestCheckPerAccountSlotsLeaveGlobalSlotsForOthers checks that a check takes
// its account slot before the global slot, so one account cannot hold every
// global slot.
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
		case <-time.After(processTimeout):
			releaseAll()
			t.Fatalf("only %d checks reached the slot acquisition", i)
		}
	}
	seen := map[uint32]int{}
	for i := 0; i < 2; i++ {
		seen[waitStartedAccount(t, started)]++
	}
	if seen[1000] != 2 {
		t.Fatalf("uid 1000 started %d checks, want its per-account share of 2", seen[1000])
	}
	// Waiting for the second account's checks to start is the assertion: their
	// two free global slots are still there only if the first account's waiters
	// never took them.
	for i := 0; i < 2; i++ {
		results = append(results, checkAsync(checker, 1001))
	}
	for i := 0; i < 2; i++ {
		uid := waitStartedAccount(t, started)
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

// TestCheckGlobalSlotWaitsForASlotToFree checks that a check waits for a
// global slot and starts once one is released.
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
	waitStartedAccount(t, started)
	waitStartedAccount(t, started)

	third := checkAsync(checker, 1002)
	// The window is the assertion: with both global slots taken, a third start
	// would send to started.
	select {
	case uid := <-started:
		t.Fatalf("uid %d started while both global slots were taken", uid)
	case <-time.After(100 * time.Millisecond):
	}

	releaseAll()
	if uid := waitStartedAccount(t, started); uid != 1002 {
		t.Fatalf("started uid %d, want the waiting account 1002", uid)
	}
	for _, r := range []<-chan checkResult{first, second, third} {
		if res := <-r; res.err != nil {
			t.Errorf("check: %v", res.err)
		}
	}
}

// TestCheckNoSlotWithinBudgetFails checks that a check which cannot take a slot
// before its deadline fails and logs access_check_busy once.
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
	waitStartedAccount(t, started)

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
	if !errors.Is(err, ErrNoCheckSlot) {
		t.Errorf("Check error = %v, want it to wrap ErrNoCheckSlot", err)
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

// TestCheckBusyLogOncePerMinute checks that access_check_busy is written at
// most once per minute.
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
	waitStartedAccount(t, started)

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
// returns its slot.
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

// TestCheckUnknownAccountsShareSlots checks that uids with no account entry
// share the unknown-account bucket while a real account keeps its own.
func TestCheckUnknownAccountsShareSlots(t *testing.T) {
	checker := &AccessChecker{MaxConcurrent: 8, MaxPerAccount: 2}
	slots := checker.slotSet()
	ctx := context.Background()

	first, err := slots.acquireAccount(ctx, unknownCheckAccount)
	if err != nil {
		t.Fatalf("first unknown slot: %v", err)
	}
	second, err := slots.acquireAccount(ctx, unknownCheckAccount)
	if err != nil {
		t.Fatalf("second unknown slot: %v", err)
	}

	// A third unknown check must fail rather than wait out the full budget.
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	_, err = slots.acquireAccount(short, unknownCheckAccount)
	cancel()
	if err == nil {
		t.Fatal("a third unknown check took a slot while two were in use")
	}

	known, err := slots.acquireAccount(ctx, 1000)
	if err != nil {
		t.Fatalf("known account slot while the unknown bucket is full: %v", err)
	}
	slots.releaseAccount(known)
	slots.releaseAccount(first)
	slots.releaseAccount(second)

	slots.mu.Lock()
	size := len(slots.accounts)
	slots.mu.Unlock()
	if size != 2 {
		t.Fatalf("the account slot table holds %d entries, want the shared bucket and the known account", size)
	}
}

// TestCheckRejectsLookupFailureWithoutSlot checks that a lookup failure fails
// closed and creates no slot entry.
func TestCheckRejectsLookupFailureWithoutSlot(t *testing.T) {
	boom := errors.New("account database unavailable")
	checker := &AccessChecker{
		Lookup:  func(uint32) (Account, error) { return Account{}, boom },
		Timeout: 5 * time.Second,
		Logger:  accessTestLogger(),
	}

	allowed, err := checker.Check(context.Background(), 7000, "/config.yaml", AccessRead)
	if err == nil || allowed {
		t.Fatalf("Check = (%v, %v), want a failed, denied check", allowed, err)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("Check error = %v, want the lookup failure", err)
	}

	slots := checker.slotSet()
	slots.mu.Lock()
	size := len(slots.accounts)
	slots.mu.Unlock()
	if size != 0 {
		t.Fatalf("the account slot table holds %d entries after a failed lookup, want 0", size)
	}
}

// TestCheckUnknownAccountUsesSharedBucketOnly checks that many unknown uids
// share one slot entry.
func TestCheckUnknownAccountUsesSharedBucketOnly(t *testing.T) {
	checker := &AccessChecker{
		Lookup: func(uid uint32) (Account, error) {
			return Account{}, fmt.Errorf("%w: uid %d", ErrNoAccount, uid)
		},
		Timeout:       5 * time.Second,
		Logger:        accessTestLogger(),
		MaxConcurrent: 8,
		MaxPerAccount: 2,
	}

	for i := 0; i < 5; i++ {
		allowed, err := checker.Check(context.Background(), uint32(300000+i), "/config.yaml", AccessRead)
		if err != nil || allowed {
			t.Fatalf("unknown-account check %d = (%v, %v), want denied without an error", i, allowed, err)
		}
	}

	slots := checker.slotSet()
	slots.mu.Lock()
	defer slots.mu.Unlock()
	if len(slots.accounts) != 1 {
		t.Fatalf("the account slot table holds %d entries for five unknown uids, want 1", len(slots.accounts))
	}
	if _, ok := slots.accounts[unknownCheckAccount]; !ok {
		t.Fatal("the unknown uids did not share unknownCheckAccount's entry")
	}
}

// TestCheckSlotTableDropsIdleAccounts checks that an account idle past
// accessAccountIdleTTL is dropped from the slot table.
func TestCheckSlotTableDropsIdleAccounts(t *testing.T) {
	now := time.Unix(1000, 0)
	started := make(chan uint32, 32)
	release := make(chan struct{})
	close(release)
	checker := &AccessChecker{
		Lookup:        func(uid uint32) (Account, error) { return Account{UID: uid, GID: uid}, nil },
		Start:         accessGateStart(started, release),
		Timeout:       5 * time.Second,
		Logger:        accessTestLogger(),
		MaxConcurrent: 8,
		MaxPerAccount: 2,
		now:           func() time.Time { return now },
	}

	for _, uid := range []uint32{2000, 2001, 2002} {
		if allowed, err := checker.Check(context.Background(), uid, "/config.yaml", AccessRead); err != nil || !allowed {
			t.Fatalf("check for uid %d = (%v, %v), want allowed", uid, allowed, err)
		}
	}
	slots := checker.slotSet()
	slots.mu.Lock()
	before := len(slots.accounts)
	slots.mu.Unlock()
	if before != 3 {
		t.Fatalf("the account slot table holds %d entries, want 3", before)
	}

	now = now.Add(accessAccountIdleTTL + time.Second)
	if allowed, err := checker.Check(context.Background(), 2003, "/config.yaml", AccessRead); err != nil || !allowed {
		t.Fatalf("check for uid 2003 = (%v, %v), want allowed", allowed, err)
	}

	slots.mu.Lock()
	after := len(slots.accounts)
	_, kept := slots.accounts[2000]
	slots.mu.Unlock()
	if kept {
		t.Fatal("an account entry idle past the TTL survived the sweep")
	}
	if after != 1 {
		t.Fatalf("the account slot table holds %d entries after the sweep, want only the new account", after)
	}
}

// TestCheckLookupConcurrencyCapped checks that account lookups run at most four
// at once.
func TestCheckLookupConcurrencyCapped(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	releaseAll := func() { once.Do(func() { close(release) }) }
	t.Cleanup(releaseAll)

	var running, peak atomic.Int32
	started := make(chan uint32, 64)
	checker := &AccessChecker{
		Lookup: func(uid uint32) (Account, error) {
			n := running.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			started <- uid
			<-release
			running.Add(-1)
			return Account{}, fmt.Errorf("%w: uid %d", ErrNoAccount, uid)
		},
		Timeout:       5 * time.Second,
		Logger:        accessTestLogger(),
		MaxConcurrent: 100,
		MaxPerAccount: 100,
	}

	const checks = 20
	results := make([]<-chan checkResult, 0, checks)
	for i := 0; i < checks; i++ {
		results = append(results, checkAsync(checker, uint32(400000+i)))
	}
	for i := 0; i < 4; i++ {
		waitStartedAccount(t, started)
	}
	// The window is the assertion: a fifth lookup that ran at once would send to
	// started.
	select {
	case <-started:
		releaseAll()
		for _, r := range results {
			<-r
		}
		t.Fatal("a fifth account database lookup ran at once")
	case <-time.After(100 * time.Millisecond):
	}
	if got := peak.Load(); got > 4 {
		releaseAll()
		for _, r := range results {
			<-r
		}
		t.Fatalf("peak concurrent lookups = %d, want at most 4", got)
	}

	releaseAll()
	for _, r := range results {
		if res := <-r; res.err != nil {
			t.Errorf("check: %v", res.err)
		}
	}
}

// TestAccessSlotTableIsBounded checks that the slot table refuses a new account
// at 4096 entries and makes room after a slot is returned.
func TestAccessSlotTableIsBounded(t *testing.T) {
	const wantMax = 4096
	now := time.Unix(1000, 0)
	slots := newAccessSlots(8, 2, func() time.Time { return now })
	ctx := context.Background()

	held := make(map[uint32]*accessAccountSlot, wantMax)
	for i := 0; i < wantMax; i++ {
		entry, err := slots.acquireAccount(ctx, uint32(i+1))
		if err != nil {
			t.Fatalf("account %d: %v", i+1, err)
		}
		held[uint32(i+1)] = entry
	}
	if got := len(slots.accounts); got != wantMax {
		t.Fatalf("table holds %d entries, want %d", got, wantMax)
	}

	const extra = uint32(1 << 20)
	if _, err := slots.acquireAccount(ctx, extra); !errors.Is(err, errAccessSlotTableFull) {
		t.Fatalf("acquiring a %dth account = %v, want %v", wantMax+1, err, errAccessSlotTableFull)
	}
	if _, ok := slots.accounts[extra]; ok {
		t.Fatal("a refused account got a table entry")
	}

	slots.releaseAccount(held[1])
	if _, err := slots.acquireAccount(ctx, extra); err != nil {
		t.Fatalf("acquiring an account after a slot was returned: %v", err)
	}
	if got := len(slots.accounts); got != wantMax {
		t.Fatalf("table holds %d entries after replacing an idle one, want %d", got, wantMax)
	}
}

// TestCheckUnknownAccountLogOncePerMinute checks that
// access_check_unknown_account is written at most once a minute.
func TestCheckUnknownAccountLogOncePerMinute(t *testing.T) {
	uid := uint32(7000)
	var logBuf bytes.Buffer
	now := time.Unix(1000, 0)
	checker := &AccessChecker{
		Lookup:  func(uint32) (Account, error) { return Account{}, ErrNoAccount },
		Timeout: 5 * time.Second,
		Logger:  slog.New(slog.NewTextHandler(&logBuf, nil)),
		now:     func() time.Time { return now },
	}

	check := func(uid uint32) {
		t.Helper()
		allowed, err := checker.Check(context.Background(), uid, "/config.yaml", AccessRead)
		if err != nil {
			t.Fatalf("Check for uid %d: %v", uid, err)
		}
		if allowed {
			t.Fatalf("unknown account %d was allowed", uid)
		}
	}
	for i := 0; i < 3; i++ {
		check(uid)
	}
	if got := strings.Count(logBuf.String(), "access_check_unknown_account"); got != 1 {
		t.Fatalf("logged access_check_unknown_account %d times within one minute, want 1", got)
	}

	now = now.Add(61 * time.Second)
	check(uid)
	if got := strings.Count(logBuf.String(), "access_check_unknown_account"); got != 2 {
		t.Fatalf("logged access_check_unknown_account %d times after the minute passed, want 2", got)
	}
}

// TestCheckUnknownAccountLogsOnceAcrossUIDs checks that rotating through many
// unknown uids writes one access_check_unknown_account record.
func TestCheckUnknownAccountLogsOnceAcrossUIDs(t *testing.T) {
	var logBuf bytes.Buffer
	now := time.Unix(1000, 0)
	checker := &AccessChecker{
		Lookup:  func(uint32) (Account, error) { return Account{}, ErrNoAccount },
		Timeout: 5 * time.Second,
		Logger:  slog.New(slog.NewTextHandler(&logBuf, nil)),
		now:     func() time.Time { return now },
	}

	const uids = 1000
	for i := 0; i < uids; i++ {
		uid := uint32(200000 + i)
		allowed, err := checker.Check(context.Background(), uid, "/config.yaml", AccessRead)
		if err != nil {
			t.Fatalf("Check for unknown uid %d: %v", uid, err)
		}
		if allowed {
			t.Fatalf("unknown account %d was allowed", uid)
		}
	}
	if got := strings.Count(logBuf.String(), "access_check_unknown_account"); got != 1 {
		t.Fatalf("logged access_check_unknown_account %d times for %d unknown uids, want 1", got, uids)
	}
}

// TestReleaseAccountDoesNotTouchAReplacementEntry checks that a slot is
// returned to the entry it was taken from, not to a replacement built under the
// same key.
func TestReleaseAccountDoesNotTouchAReplacementEntry(t *testing.T) {
	now := time.Unix(1000, 0)
	slots := newAccessSlots(8, 1, func() time.Time { return now })
	ctx := context.Background()

	old, err := slots.acquireAccount(ctx, 7)
	if err != nil {
		t.Fatalf("acquiring the first entry: %v", err)
	}

	slots.mu.Lock()
	delete(slots.accounts, 7)
	fresh := &accessAccountSlot{slots: make(chan struct{}, 1), idleAt: now}
	fresh.slots <- struct{}{}
	slots.accounts[7] = fresh
	slots.mu.Unlock()

	slots.releaseAccount(old)

	if got := len(fresh.slots); got != 1 {
		t.Fatalf("the replacement entry holds %d slots after the old entry was released, want 1", got)
	}
}
