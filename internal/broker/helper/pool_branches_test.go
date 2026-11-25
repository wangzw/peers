package helper

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/helperpb"
	"github.com/wangzw/peers/internal/inbox"
)

// poolNewState builds the per-account state NewPool would create for uid.
func poolNewState(uid uint32) *helperState {
	return &helperState{uid: uid, watches: make(map[uint64]*poolWatch), backoff: time.Second}
}

func poolFakeWatchCount(f *fakeOps, dir string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.watches[dir])
}

func poolFakeStoppedCount(f *fakeOps, dir string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stopped[dir]
}

// TestPoolDefaultOptionsNormalize checks that NewPool fills slog.Default and raises a RespawnMax below RespawnMin.
func TestPoolDefaultOptionsNormalize(t *testing.T) {
	p := NewPool(PoolOptions{RespawnMin: 2 * time.Second, RespawnMax: time.Second})
	t.Cleanup(func() { _ = p.Close() })

	if got := p.opts.RespawnMax; got != 2*time.Second {
		t.Fatalf("RespawnMax = %v, want it raised to RespawnMin 2s", got)
	}
	if p.opts.Logger != slog.Default() {
		t.Fatal("Logger is not slog.Default when none is configured")
	}
	if p.opts.Lookup == nil || p.opts.Start == nil || p.opts.After == nil || p.connect == nil {
		t.Fatal("NewPool left a default dependency unset")
	}
}

// TestPoolDefaultStartRunsHelperProcess checks that the default Start runs the test binary's account-helper subcommand and serves a Lookup.
func TestPoolDefaultStartRunsHelperProcess(t *testing.T) {
	uid := uint32(os.Getuid())
	pool := NewPool(PoolOptions{
		Exe:          os.Args[0],
		Lookup:       LookupAccount,
		ReadyTimeout: processTimeout,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	t.Cleanup(func() { _ = pool.Close() })

	b, release, err := pool.Acquire(context.Background(), uid)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()

	_, err = b.Lookup(context.Background(), "/no/such/dir", "/no/such/dir/inbox.sock")
	if !errors.Is(err, inbox.ErrNoRecord) {
		t.Fatalf("Lookup error = %v, want inbox.ErrNoRecord from the started helper", err)
	}
}

// TestPoolDefaultStartRejectsMissingExecutable checks that a Start of an executable that does not exist reports ErrUnavailable keeping the exec failure.
func TestPoolDefaultStartRejectsMissingExecutable(t *testing.T) {
	exe := filepath.Join(t.TempDir(), "missing-peers")
	uid := uint32(os.Getuid())
	pool := NewPool(PoolOptions{
		Exe:          exe,
		Lookup:       LookupAccount,
		ReadyTimeout: processTimeout,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	t.Cleanup(func() { _ = pool.Close() })

	b, release, err := pool.Acquire(context.Background(), uid)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()

	_, err = b.Lookup(context.Background(), "/cfg", "/x.sock")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Lookup error = %v, want it to wrap ErrUnavailable", err)
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Lookup error = %v, want it to keep the exec failure", err)
	}
}

// TestPoolDefaultStartNeedsExecutable checks that the default Start refuses with a message naming the missing executable when none is configured.
func TestPoolDefaultStartNeedsExecutable(t *testing.T) {
	uid := uint32(os.Getuid())
	pool := NewPool(PoolOptions{
		Lookup:       LookupAccount,
		ReadyTimeout: processTimeout,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	t.Cleanup(func() { _ = pool.Close() })

	b, release, err := pool.Acquire(context.Background(), uid)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()

	_, err = b.Lookup(context.Background(), "/cfg", "/x.sock")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Lookup error = %v, want it to wrap ErrUnavailable", err)
	}
	if !strings.Contains(err.Error(), "no executable is configured") {
		t.Fatalf("Lookup error = %v, want it to name the missing executable", err)
	}
}

// TestStartHelperProcessArgsKillsChild checks that a real child is gone after Kill and reaped by Wait.
func TestStartHelperProcessArgsKillsChild(t *testing.T) {
	sleep, err := exec.LookPath("sleep")
	if err != nil {
		t.Skipf("sleep is not available: %v", err)
	}
	acct, err := LookupAccount(uint32(os.Getuid()))
	if err != nil {
		t.Fatalf("LookupAccount: %v", err)
	}
	proc, err := startHelperProcessArgs(sleep, []string{"60"}, acct,
		slog.New(slog.NewTextHandler(io.Discard, nil)), func() bool { return false })
	if err != nil {
		t.Fatalf("startHelperProcessArgs: %v", err)
	}
	if proc == nil {
		t.Fatal("startHelperProcessArgs returned no process")
	}
	t.Cleanup(func() {
		_ = proc.Kill()
		_ = proc.Wait()
	})
	_ = proc.Stdin.Close()
	if err := proc.Kill(); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	if err := proc.Wait(); err == nil {
		t.Fatal("Wait = nil, want an error for a killed child")
	}
}

// TestPoolStartFailureWhileClosedIsPoolClosed checks that a Start failing after Close began reports errPoolClosed.
func TestPoolStartFailureWhileClosedIsPoolClosed(t *testing.T) {
	boom := errors.New("boom")
	var p *Pool
	h := newPoolHarness(t, func(o *PoolOptions) {
		o.Start = func(context.Context, Account) (*Process, error) {
			p.closed.Store(true)
			return nil, boom
		}
	})
	p = h.pool
	b, release, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()

	_, err = b.Lookup(context.Background(), "/cfg", "/x.sock")
	if !errors.Is(err, errPoolClosed) {
		t.Fatalf("Lookup error = %v, want errPoolClosed", err)
	}
}

// TestPoolStartDeadlineKeepsBothErrors checks that a Start ended by the caller's deadline keeps the deadline, the start error and ErrUnavailable.
func TestPoolStartDeadlineKeepsBothErrors(t *testing.T) {
	boom := errors.New("boom")
	started := make(chan struct{})
	var starts atomic.Int32
	h := newPoolHarness(t, func(o *PoolOptions) {
		o.Start = func(ctx context.Context, _ Account) (*Process, error) {
			starts.Add(1)
			close(started)
			<-ctx.Done()
			return nil, boom
		}
	})
	b, release, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()

	ctx := newTestDeadlineContext()
	errCh := make(chan error, 1)
	go func() {
		_, err := b.Lookup(ctx, "/cfg", "/x.sock")
		errCh <- err
	}()
	select {
	case <-started:
	case <-time.After(processTimeout):
		t.Fatal("Lookup never reached Start")
	}
	ctx.end()
	err = waitCallError(t, "Lookup", errCh)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Lookup error = %v, want it to wrap ErrUnavailable", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Lookup error = %v, want it to keep the deadline", err)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("Lookup error = %v, want it to keep the start error", err)
	}
	if got := starts.Load(); got != 1 {
		t.Fatalf("Start calls = %d, want 1", got)
	}
}

// TestPoolStartCancelIsNotUnavailable checks that a caller cancel during Start returns the start error without ErrUnavailable.
func TestPoolStartCancelIsNotUnavailable(t *testing.T) {
	boom := errors.New("boom")
	var starts atomic.Int32
	h := newPoolHarness(t, func(o *PoolOptions) {
		o.Start = func(ctx context.Context, _ Account) (*Process, error) {
			starts.Add(1)
			<-ctx.Done()
			return nil, boom
		}
	})
	b, release, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := b.Lookup(ctx, "/cfg", "/x.sock")
		errCh <- err
	}()
	poolWaitFor(t, "the spawn to start", func() bool { return starts.Load() == 1 })
	cancel()

	err = <-errCh
	if errors.Is(err, ErrUnavailable) {
		t.Fatalf("Lookup error = %v, want a caller cancellation not to wrap ErrUnavailable", err)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("Lookup error = %v, want the start error", err)
	}
}

// TestEnsureLiveRefusesAfterClose checks that ensureLive refuses with errPoolClosed after Close without starting a child.
func TestEnsureLiveRefusesAfterClose(t *testing.T) {
	h := newPoolHarness(t)
	h.pool.closed.Store(true)
	if _, err := h.pool.ensureLive(context.Background(), poolNewState(1001)); !errors.Is(err, errPoolClosed) {
		t.Fatalf("ensureLive error = %v, want errPoolClosed", err)
	}
	if got := h.startCount(); got != 0 {
		t.Fatalf("Start calls = %d, want 0", got)
	}
}

// TestEnsureLiveRefusesWhenChildrenClosed checks that ensureLive refuses with errPoolClosed once the child group is closed.
func TestEnsureLiveRefusesWhenChildrenClosed(t *testing.T) {
	h := newPoolHarness(t)
	h.pool.children.closeAndWait()
	if _, err := h.pool.ensureLive(context.Background(), poolNewState(1001)); !errors.Is(err, errPoolClosed) {
		t.Fatalf("ensureLive error = %v, want errPoolClosed", err)
	}
	if got := h.startCount(); got != 0 {
		t.Fatalf("Start calls = %d, want 0", got)
	}
}

// TestEnsureLiveClosedAfterHandshake checks that Close during the Ready handshake closes and reaps the fresh child and reports errPoolClosed.
func TestEnsureLiveClosedAfterHandshake(t *testing.T) {
	var p *Pool
	h := newPoolHarness(t)
	h.setWaitReady(func(context.Context) (*helperpb.Ready, error) {
		p.closed.Store(true)
		return &helperpb.Ready{ProtocolVersion: helperpb.ProtocolVersion, Uid: 1001, Gid: 1001}, nil
	})
	p = h.pool
	b, release, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()

	_, err = b.Lookup(context.Background(), "/cfg", "/x.sock")
	if !errors.Is(err, errPoolClosed) {
		t.Fatalf("Lookup error = %v, want errPoolClosed", err)
	}
	if got := h.conn(0).closedCount(); got != 1 {
		t.Fatalf("connection closed = %d, want 1", got)
	}
	if wait, _ := h.counters(); wait != 1 {
		t.Fatalf("Wait calls = %d, want 1", wait)
	}
}

// TestEnsureLiveAlreadyCancelled checks that a call with a cancelled context starts no child and returns context.Canceled.
func TestEnsureLiveAlreadyCancelled(t *testing.T) {
	h := newPoolHarness(t)
	b, release, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = b.Lookup(ctx, "/cfg", "/x.sock")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Lookup error = %v, want context.Canceled", err)
	}
	if errors.Is(err, ErrUnavailable) {
		t.Fatalf("Lookup error = %v, want a caller cancellation not to wrap ErrUnavailable", err)
	}
	if got := h.startCount(); got != 0 {
		t.Fatalf("Start calls = %d, want 0", got)
	}
}

// TestBackendInvokeAfterClose checks that a backend lookup after Close reports errPoolClosed.
func TestBackendInvokeAfterClose(t *testing.T) {
	h := newPoolHarness(t)
	b, release, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()
	if err := h.pool.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := b.Lookup(context.Background(), "/cfg", "/x.sock"); !errors.Is(err, errPoolClosed) {
		t.Fatalf("Lookup after Close = %v, want errPoolClosed", err)
	}
}

// TestBackendWatchAfterClose checks that a watch after Close reports errPoolClosed.
func TestBackendWatchAfterClose(t *testing.T) {
	h := newPoolHarness(t)
	b, release, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()
	if err := h.pool.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := b.Watch(context.Background(), "/cfg", func(int) {}); !errors.Is(err, errPoolClosed) {
		t.Fatalf("Watch after Close = %v, want errPoolClosed", err)
	}
}

// TestPoolCloseClearsWatchStops checks that Close clears a watch's stop so it does not reach the closed connection.
func TestPoolCloseClearsWatchStops(t *testing.T) {
	h := newPoolHarness(t)
	b, release, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()
	stop, err := b.Watch(context.Background(), "/cfg", func(int) {})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	conn := h.conn(0)
	if got := poolFakeWatchCount(conn.ops, "/cfg"); got != 1 {
		t.Fatalf("registered watches = %d, want 1", got)
	}

	if err := h.pool.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	stop()

	if got := poolFakeStoppedCount(conn.ops, "/cfg"); got != 0 {
		t.Fatalf("watch stop calls = %d, want 0: Close must clear the entry's stop", got)
	}
	if got := conn.closedCount(); got != 1 {
		t.Fatalf("connection closed = %d, want 1", got)
	}
}

// TestPoolWatchFireAfterDetach checks that a watch fires before detach and stays silent after.
func TestPoolWatchFireAfterDetach(t *testing.T) {
	var calls atomic.Int32
	e := &poolWatch{configDir: "/cfg", onChange: func(int) { calls.Add(1) }}
	e.fire(1)
	if got := calls.Load(); got != 1 {
		t.Fatalf("callback calls = %d, want 1", got)
	}
	if stop := e.detach(); stop != nil {
		t.Fatal("detach returned a stop for an entry that had none")
	}
	e.fire(2)
	if got := calls.Load(); got != 1 {
		t.Fatalf("callback calls after detach = %d, want 1: a detached watch must not fire", got)
	}
}

// TestChildGroupAddAfterClose checks that the child group refuses a new child once closeAndWait has begun.
func TestChildGroupAddAfterClose(t *testing.T) {
	var g childGroup
	g.closeAndWait()
	if g.add() {
		g.done()
		t.Fatal("add succeeded after closeAndWait, want it refused so no child outlives the wait")
	}
}

// TestPoolStartRespawnStaysClosed checks that startRespawn does not claim an account in a closed pool.
func TestPoolStartRespawnStaysClosed(t *testing.T) {
	h := newPoolHarness(t)
	h.pool.closed.Store(true)
	st := poolNewState(1001)
	h.pool.startRespawn(st)
	if st.respawn != nil {
		t.Fatal("startRespawn claimed the account in a closed pool")
	}
}

// TestPoolStartRespawnKeepsExistingLoop checks that startRespawn leaves an already running loop's token in place.
func TestPoolStartRespawnKeepsExistingLoop(t *testing.T) {
	h := newPoolHarness(t)
	st := poolNewState(1001)
	tok := &respawnRun{}
	st.respawn = tok
	h.pool.startRespawn(st)
	if st.respawn != tok {
		t.Fatal("startRespawn replaced the token of a loop that was already running")
	}
}

// TestRespawnLoopStopsOnClose checks that a respawn loop ends and clears its token when the pool closes while it waits.
func TestRespawnLoopStopsOnClose(t *testing.T) {
	h := newPoolHarness(t)
	p := h.pool
	st := poolNewState(1001)
	st.watches[1] = &poolWatch{id: 1, configDir: "/cfg", onChange: func(int) {}}
	tok := &respawnRun{}
	st.respawn = tok
	if !p.children.add() {
		t.Fatal("the test could not count its own respawn loop")
	}
	go p.respawnLoop(st, tok)

	_ = h.nextOffer(t)
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	st.mu.Lock()
	remaining := st.respawn
	st.mu.Unlock()
	if remaining != nil {
		t.Fatal("the closed pool left the respawn token set")
	}
	if got := h.startCount(); got != 0 {
		t.Fatalf("Start calls = %d, want 0", got)
	}
}

// TestRespawnLoopGivesUpWhenPoolCloses checks that a respawn loop ends and leaves its token clear when the pool closed during its spawn attempt.
func TestRespawnLoopGivesUpWhenPoolCloses(t *testing.T) {
	h := newPoolHarness(t)
	p := h.pool
	st := poolNewState(1001)
	st.watches[1] = &poolWatch{id: 1, configDir: "/cfg", onChange: func(int) {}}
	tok := &respawnRun{}
	st.respawn = tok
	if !p.children.add() {
		t.Fatal("the test could not count its own respawn loop")
	}
	go p.respawnLoop(st, tok)

	offer := h.nextOffer(t)
	p.closed.Store(true)
	offer <- time.Now()

	poolWaitFor(t, "the respawn loop to give up", func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.respawn == nil
	})
	if got := h.startCount(); got != 0 {
		t.Fatalf("Start calls = %d, want 0", got)
	}
	p.children.closeAndWait()
}

// TestRespawnBackoffCapsAtRespawnMax checks that the second respawn delay is capped at RespawnMax.
func TestRespawnBackoffCapsAtRespawnMax(t *testing.T) {
	h := newPoolHarness(t, func(o *PoolOptions) {
		o.RespawnMin = time.Second
		o.RespawnMax = 1500 * time.Millisecond
	})
	b, release, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()
	stop, err := b.Watch(context.Background(), "/cfg", func(int) {})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer stop()

	h.setStartErr(errors.New("boom"))
	h.conn(0).crash()

	first := h.nextOffer(t)
	if got := h.lastDelay(); got != time.Second {
		t.Fatalf("first respawn delay = %v, want 1s", got)
	}
	first <- time.Now()

	second := h.nextOffer(t)
	if got := h.lastDelay(); got != 1500*time.Millisecond {
		t.Fatalf("second respawn delay = %v, want the RespawnMax cap 1.5s", got)
	}
	_ = second
}

// TestDiscardLiveSkipsAStaleHelper checks that discarding a helper that is no longer current leaves the current one and closes nothing.
func TestDiscardLiveSkipsAStaleHelper(t *testing.T) {
	h := newPoolHarness(t)
	curConn := &poolFakeConn{done: make(chan struct{}), ops: &fakeOps{}}
	staleConn := &poolFakeConn{done: make(chan struct{}), ops: &fakeOps{}}
	current := &liveHelper{conn: curConn, proc: &Process{}}
	stale := &liveHelper{conn: staleConn, proc: &Process{}}
	st := poolNewState(1001)
	st.live = current

	h.pool.discardLive(st, stale)

	if st.live != current {
		t.Fatal("discardLive detached a helper that was no longer current")
	}
	if got := staleConn.closedCount(); got != 0 {
		t.Fatalf("stale connection closed = %d, want 0", got)
	}
}

// TestDiscardLiveStartsRespawnForWatches checks that discarding a helper a watch still needs starts the respawn loop and closes and kills that helper.
func TestDiscardLiveStartsRespawnForWatches(t *testing.T) {
	h := newPoolHarness(t)
	h.setFirstRereadBlocking(true)
	b, release, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()
	stop, err := b.Watch(context.Background(), "/cfg", func(int) {})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := b.Reread(ctx, inbox.Session{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Reread error = %v, want context.DeadlineExceeded", err)
	}

	offer := h.nextOffer(t)
	if got := h.lastDelay(); got != time.Second {
		t.Fatalf("respawn delay = %v, want 1s", got)
	}
	_ = offer
	if got := h.conn(0).closedCount(); got != 1 {
		t.Fatalf("discarded connection closed = %d, want 1", got)
	}
	if _, kill := h.counters(); kill != 1 {
		t.Fatalf("Kill calls = %d, want 1", kill)
	}
}

// TestRestoreWatchesKeepsAttachedEntry checks that restore fires an attached entry with pid 0 without subscribing again.
func TestRestoreWatchesKeepsAttachedEntry(t *testing.T) {
	h := newPoolHarness(t)
	conn := &poolFakeConn{done: make(chan struct{}), ops: &fakeOps{}}
	live := &liveHelper{conn: conn, proc: &Process{}}
	st := poolNewState(1001)

	var fired atomic.Int32
	var gotPID atomic.Int32
	entry := &poolWatch{id: 1, configDir: "/cfg", onChange: func(pid int) {
		gotPID.Store(int32(pid))
		fired.Add(1)
	}, conn: conn, stop: func() {}}
	st.watches[entry.id] = entry

	h.pool.restoreWatches(st, live)

	if got := fired.Load(); got != 1 {
		t.Fatalf("callback calls = %d, want the pid=0 reread", got)
	}
	if got := gotPID.Load(); got != 0 {
		t.Fatalf("callback pid = %d, want 0", got)
	}
	if got := poolFakeWatchCount(conn.ops, "/cfg"); got != 0 {
		t.Fatalf("Watch calls = %d, want 0 for an attached entry", got)
	}
}

// TestRestoreWatchesSkipsStoppedEntry checks that restore skips and never fires a stopped entry while it subscribes and fires an active one.
func TestRestoreWatchesSkipsStoppedEntry(t *testing.T) {
	h := newPoolHarness(t)
	conn := &poolFakeConn{done: make(chan struct{}), ops: &fakeOps{}}
	live := &liveHelper{conn: conn, proc: &Process{}}
	st := poolNewState(1001)

	var stoppedCalls atomic.Int32
	stopped := &poolWatch{id: 1, configDir: "/stopped", onChange: func(int) { stoppedCalls.Add(1) }}
	stopped.detach()
	st.watches[stopped.id] = stopped

	var activeCalls atomic.Int32
	active := &poolWatch{id: 2, configDir: "/active", onChange: func(int) { activeCalls.Add(1) }}
	st.watches[active.id] = active

	h.pool.restoreWatches(st, live)

	if got := stoppedCalls.Load(); got != 0 {
		t.Fatalf("stopped callback calls = %d, want 0", got)
	}
	if got := activeCalls.Load(); got != 1 {
		t.Fatalf("active callback calls = %d, want the pid=0 reread", got)
	}
	if got := poolFakeWatchCount(conn.ops, "/stopped"); got != 0 {
		t.Fatalf("Watch calls for the stopped entry = %d, want 0", got)
	}
	if got := poolFakeWatchCount(conn.ops, "/active"); got != 1 {
		t.Fatalf("Watch calls for the active entry = %d, want 1", got)
	}
}

// TestRestoreWatchesSkipsFailedWatch checks that a failed subscription leaves the entry detached and unfired while an attached one still fires.
func TestRestoreWatchesSkipsFailedWatch(t *testing.T) {
	h := newPoolHarness(t)
	conn := &poolFakeConn{done: make(chan struct{}), ops: &fakeOps{}}
	conn.ops.watchErr = errors.New("cannot watch")
	live := &liveHelper{conn: conn, proc: &Process{}}
	st := poolNewState(1001)

	var failedCalls atomic.Int32
	failed := &poolWatch{id: 1, configDir: "/cfg", onChange: func(int) { failedCalls.Add(1) }}
	st.watches[failed.id] = failed

	var attachedCalls atomic.Int32
	attached := &poolWatch{id: 2, configDir: "/attached", onChange: func(int) { attachedCalls.Add(1) }, conn: conn}
	st.watches[attached.id] = attached

	h.pool.restoreWatches(st, live)

	if got := failedCalls.Load(); got != 0 {
		t.Fatalf("failed watch callback calls = %d, want 0 when the watch failed", got)
	}
	if got := attachedCalls.Load(); got != 1 {
		t.Fatalf("attached watch callback calls = %d, want the pid=0 reread", got)
	}
}

// poolStopOnExitHandler runs stop when the account_helper_exited record is
// handled, which lets a test make the second stoppingNow check true without a
// real shutdown.
type poolStopOnExitHandler struct {
	once sync.Once
	stop func()
}

func (h *poolStopOnExitHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *poolStopOnExitHandler) Handle(_ context.Context, r slog.Record) error {
	if r.Message == "account_helper_exited" {
		h.once.Do(h.stop)
	}
	return nil
}

func (h *poolStopOnExitHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *poolStopOnExitHandler) WithGroup(string) slog.Handler { return h }

// TestWatchLiveSkipsRespawnWhenStopBeginsDuringLog checks that a stop beginning during the exit record prevents a respawn.
func TestWatchLiveSkipsRespawnWhenStopBeginsDuringLog(t *testing.T) {
	stop := make(chan struct{})
	handler := &poolStopOnExitHandler{stop: func() { close(stop) }}
	h := newPoolHarness(t, func(o *PoolOptions) {
		o.Logger = slog.New(handler)
		o.Stopping = stop
	})
	b, release, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()
	poolMustLookup(t, b, "/sock")
	stopWatch, err := b.Watch(context.Background(), "/cfg", func(int) {})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer stopWatch()

	h.conn(0).crash()

	// watchLive is counted in the child group, and a respawn loop would be too.
	// The drain returning proves no loop was left waiting on its timer.
	done := make(chan struct{})
	go func() {
		h.pool.children.closeAndWait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(processTimeout):
		t.Fatal("a respawn loop started after the stop began during the exit record")
	}
	if got := h.startCount(); got != 1 {
		t.Fatalf("Start calls = %d, want 1: a stop must not respawn the helper", got)
	}
}

// TestRespawnLoopStopsWithNoWatches checks that a loop with no watch clears its token and requests no timer.
func TestRespawnLoopStopsWithNoWatches(t *testing.T) {
	h := newPoolHarness(t)
	p := h.pool
	st := poolNewState(1001)
	tok := &respawnRun{}
	st.respawn = tok
	if !p.children.add() {
		t.Fatal("the test could not count its own respawn loop")
	}

	p.respawnLoop(st, tok)

	st.mu.Lock()
	remaining := st.respawn
	st.mu.Unlock()
	if remaining != nil {
		t.Fatal("a loop with no watches left its respawn token set")
	}
	h.mu.Lock()
	delays := len(h.delays)
	h.mu.Unlock()
	if delays != 0 {
		t.Fatalf("timer requests = %d, want 0 from a loop with no watches", delays)
	}
}

// TestRespawnLoopStopsWhenWatchesEndWhileWaiting checks that a loop which loses its last watch during the timer wait clears its token and starts nothing.
func TestRespawnLoopStopsWhenWatchesEndWhileWaiting(t *testing.T) {
	h := newPoolHarness(t)
	b, release, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()
	stop, err := b.Watch(context.Background(), "/cfg", func(int) {})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer stop()

	h.conn(0).crash()
	offer := h.nextOffer(t)

	st := b.state
	st.mu.Lock()
	running := st.respawn != nil
	st.mu.Unlock()
	if !running {
		t.Fatal("the crashed helper's watch did not start a respawn loop")
	}

	stop()
	offer <- time.Now()

	poolWaitFor(t, "the respawn loop to drop its token", func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.respawn == nil
	})
	if got := h.startCount(); got != 1 {
		t.Fatalf("Start calls = %d, want 1 after the last watch ended", got)
	}
}

// helperPipeRecorder records the pipes a stubbed helperPipe created.
type helperPipeRecorder struct {
	pipes [][2]*os.File
}

// stubHelperPipe replaces helperPipe so that its nth call (n = call) fails with
// err, recording the pipes the earlier calls created.
func stubHelperPipe(t *testing.T, call int, err error) *helperPipeRecorder {
	t.Helper()
	orig := helperPipe
	rec := &helperPipeRecorder{}
	calls := 0
	helperPipe = func() (*os.File, *os.File, error) {
		calls++
		if calls == call {
			return nil, nil, err
		}
		r, w, pipeErr := os.Pipe()
		if pipeErr != nil {
			return nil, nil, pipeErr
		}
		rec.pipes = append(rec.pipes, [2]*os.File{r, w})
		return r, w, nil
	}
	t.Cleanup(func() { helperPipe = orig })
	return rec
}

// TestStartHelperProcessArgsStdinPipeFailure checks that a helper whose first pipe cannot be created returns that error and no process.
func TestStartHelperProcessArgsStdinPipeFailure(t *testing.T) {
	boom := errors.New("stdin pipe failure")
	stubHelperPipe(t, 1, boom)

	proc, err := startHelperProcessArgs("unused", nil, Account{UID: uint32(os.Geteuid())},
		slog.New(slog.NewTextHandler(io.Discard, nil)), func() bool { return false })
	if proc != nil {
		t.Fatal("startHelperProcessArgs returned a process after the stdin pipe failed")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("startHelperProcessArgs error = %v, want the stdin pipe error", err)
	}
}

// TestStartHelperProcessArgsStdoutPipeFailure checks that a helper whose second pipe cannot be created closes both ends of the stdin pipe.
func TestStartHelperProcessArgsStdoutPipeFailure(t *testing.T) {
	boom := errors.New("stdout pipe failure")
	rec := stubHelperPipe(t, 2, boom)

	proc, err := startHelperProcessArgs("unused", nil, Account{UID: uint32(os.Geteuid())},
		slog.New(slog.NewTextHandler(io.Discard, nil)), func() bool { return false })
	if proc != nil {
		t.Fatal("startHelperProcessArgs returned a process after the stdout pipe failed")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("startHelperProcessArgs error = %v, want the stdout pipe error", err)
	}
	if len(rec.pipes) != 1 {
		t.Fatalf("created %d stdin pipes, want 1", len(rec.pipes))
	}
	assertPipeClosed(t, "stdin writer", rec.pipes[0][1])
	assertPipeClosed(t, "stdin read end", rec.pipes[0][0])
}

// assertPipeClosed fails t unless f was already closed by the code under test.
func assertPipeClosed(t *testing.T, name string, f *os.File) {
	t.Helper()
	if err := f.Close(); !errors.Is(err, os.ErrClosed) {
		t.Errorf("%s was not closed: Close = %v", name, err)
	}
}

// TestRestoreWatchesSkipsEntryStoppedDuringRestore checks that restore stops a fresh subscription for an entry detached during the call.
func TestRestoreWatchesSkipsEntryStoppedDuringRestore(t *testing.T) {
	h := newPoolHarness(t)
	conn := &poolFakeConn{done: make(chan struct{}), ops: &fakeOps{}}
	live := &liveHelper{conn: conn, proc: &Process{}}
	st := poolNewState(1001)

	var fired atomic.Int32
	entry := &poolWatch{id: 1, configDir: "/cfg", onChange: func(int) { fired.Add(1) }}
	st.watches[entry.id] = entry
	conn.ops.onWatch = func() { entry.detach() }

	h.pool.restoreWatches(st, live)

	if got := fired.Load(); got != 0 {
		t.Fatalf("callback calls = %d, want 0 for an entry stopped during restore", got)
	}
	if got := poolFakeStoppedCount(conn.ops, "/cfg"); got != 1 {
		t.Fatalf("fresh subscription stop calls = %d, want 1", got)
	}
}
