package helper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/helperpb"
	"github.com/wangzw/peers/internal/inbox"
)

// The regular pool tests use only fakes, so these limits never come from a
// real process or a real wait; they describe the pool's configured behaviour.
const (
	poolTestReadyTimeout = 5 * time.Second
	poolTestRespawnMax   = 30 * time.Second
	poolTestBackoffOne   = time.Second
	poolTestBackoffTwo   = 2 * time.Second
	poolTestBackoffThree = 4 * time.Second
)

// poolNopWriteCloser stands in for the child's stdin in tests that never use it.
type poolNopWriteCloser struct{ io.Writer }

func (poolNopWriteCloser) Close() error { return nil }

// poolFakeConn is the helperConn test double. It never talks to a real child.
type poolFakeConn struct {
	ready      *helperpb.Ready
	readyBlock bool
	ops        *fakeOps
	done       chan struct{}
	// reread overrides the fake ops for tests that need to hold one call.
	reread func(ctx context.Context, s inbox.Session) (inbox.Session, error)
	// waitReadyFunc replaces the Ready handshake for tests that need to act
	// while the spawn is waiting for it.
	waitReadyFunc func(ctx context.Context) (*helperpb.Ready, error)

	mu     sync.Mutex
	closed int
}

func (c *poolFakeConn) WaitReady(ctx context.Context) (*helperpb.Ready, error) {
	if c.waitReadyFunc != nil {
		return c.waitReadyFunc(ctx)
	}
	if c.readyBlock {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return c.ready, nil
}

func (c *poolFakeConn) Lookup(ctx context.Context, configDir, socketPath string) (inbox.Session, error) {
	return c.ops.Lookup(configDir, socketPath)
}

func (c *poolFakeConn) Reread(ctx context.Context, s inbox.Session) (inbox.Session, error) {
	if c.reread != nil {
		return c.reread(ctx, s)
	}
	return c.ops.Reread(s)
}

func (c *poolFakeConn) Watch(ctx context.Context, configDir string, onChange func(pid int)) (func(), error) {
	return c.ops.Watch(configDir, onChange)
}

func (c *poolFakeConn) Deliver(ctx context.Context, s inbox.Session, sig, text, msgID string, timeout time.Duration) error {
	return c.ops.Deliver(ctx, s, sig, text, msgID, timeout)
}

func (c *poolFakeConn) Done() <-chan struct{} { return c.done }

func (c *poolFakeConn) Close() error {
	c.mu.Lock()
	c.closed++
	c.mu.Unlock()
	c.closeDone()
	return nil
}

func (c *poolFakeConn) crash() { c.closeDone() }

func (c *poolFakeConn) closeDone() {
	select {
	case <-c.done:
	default:
		close(c.done)
	}
}

func (c *poolFakeConn) closedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// poolHarness builds a Pool whose Start and connect are replaced by fakes.
type poolHarness struct {
	t *testing.T

	mu        sync.Mutex
	pool      *Pool
	starts    []Account
	conns     []*poolFakeConn
	byProc    map[*Process]*poolFakeConn
	startErr  error
	readyFn   func(Account) *helperpb.Ready
	block     bool
	waitBlock bool
	// blockFirstReread makes the first spawned helper hold its Reread until the
	// call context ends; later helpers answer immediately.
	blockFirstReread bool
	waitReadyFunc    func(ctx context.Context) (*helperpb.Ready, error)
	waitN            int
	killN            int
	delays           []time.Duration
	offers           chan chan time.Time
}

func newPoolHarness(t *testing.T, mutate ...func(*PoolOptions)) *poolHarness {
	t.Helper()
	h := &poolHarness{
		t:      t,
		byProc: make(map[*Process]*poolFakeConn),
		offers: make(chan chan time.Time, 8),
		readyFn: func(acct Account) *helperpb.Ready {
			return &helperpb.Ready{
				ProtocolVersion: helperpb.ProtocolVersion,
				Uid:             acct.UID,
				Gid:             acct.GID,
				Groups:          acct.Groups,
			}
		},
	}
	opts := PoolOptions{
		Lookup: func(uid uint32) (Account, error) {
			return Account{UID: uid, GID: uid, Name: fmt.Sprintf("u%d", uid)}, nil
		},
		Start:        h.start,
		ReadyTimeout: poolTestReadyTimeout,
		RespawnMin:   time.Second,
		RespawnMax:   poolTestRespawnMax,
		After:        h.after,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	for _, m := range mutate {
		m(&opts)
	}
	h.pool = NewPool(opts)
	h.pool.connect = func(proc *Process) helperConn {
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.byProc[proc]
	}
	t.Cleanup(func() { _ = h.pool.Close() })
	return h
}

func (h *poolHarness) start(ctx context.Context, acct Account) (*Process, error) {
	h.mu.Lock()
	isFirst := len(h.conns) == 0
	h.starts = append(h.starts, acct)
	err := h.startErr
	readyFn := h.readyFn
	block := h.block
	waitBlock := h.waitBlock
	blockFirstReread := h.blockFirstReread
	waitReadyFunc := h.waitReadyFunc
	h.mu.Unlock()
	if err != nil {
		return nil, err
	}

	conn := &poolFakeConn{
		ready:         readyFn(acct),
		readyBlock:    block,
		ops:           &fakeOps{},
		done:          make(chan struct{}),
		waitReadyFunc: waitReadyFunc,
	}
	if blockFirstReread {
		conn.reread = func(ctx context.Context, s inbox.Session) (inbox.Session, error) {
			if isFirst {
				<-ctx.Done()
				return inbox.Session{}, ctx.Err()
			}
			return s, nil
		}
	}
	// When waitBlock is set, Wait does not return until Kill is called, so a
	// test can tell whether Close waited for the child to be reaped.
	waitReturn := make(chan struct{})
	var killOnce sync.Once
	proc := &Process{
		Stdin:  poolNopWriteCloser{io.Discard},
		Stdout: strings.NewReader(""),
		Wait: func() error {
			if waitBlock {
				<-waitReturn
			}
			h.mu.Lock()
			h.waitN++
			h.mu.Unlock()
			return nil
		},
		Kill: func() error {
			h.mu.Lock()
			h.killN++
			h.mu.Unlock()
			if waitBlock {
				killOnce.Do(func() { close(waitReturn) })
			}
			return nil
		},
	}
	h.mu.Lock()
	h.byProc[proc] = conn
	h.conns = append(h.conns, conn)
	h.mu.Unlock()
	return proc, nil
}

func (h *poolHarness) after(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	h.mu.Lock()
	h.delays = append(h.delays, d)
	h.mu.Unlock()
	h.offers <- ch
	return ch
}

func (h *poolHarness) startCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.starts)
}

func (h *poolHarness) startsFor(uid uint32) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, a := range h.starts {
		if a.UID == uid {
			n++
		}
	}
	return n
}

func (h *poolHarness) conn(i int) *poolFakeConn {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.conns[i]
}

func (h *poolHarness) connCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.conns)
}

func (h *poolHarness) setStartErr(err error) {
	h.mu.Lock()
	h.startErr = err
	h.mu.Unlock()
}

func (h *poolHarness) setReady(fn func(Account) *helperpb.Ready) {
	h.mu.Lock()
	h.readyFn = fn
	h.mu.Unlock()
}

func (h *poolHarness) setBlocking(block bool) {
	h.mu.Lock()
	h.block = block
	h.mu.Unlock()
}

func (h *poolHarness) setWaitBlocking(block bool) {
	h.mu.Lock()
	h.waitBlock = block
	h.mu.Unlock()
}

func (h *poolHarness) setFirstRereadBlocking(block bool) {
	h.mu.Lock()
	h.blockFirstReread = block
	h.mu.Unlock()
}

func (h *poolHarness) setWaitReady(fn func(ctx context.Context) (*helperpb.Ready, error)) {
	h.mu.Lock()
	h.waitReadyFunc = fn
	h.mu.Unlock()
}

func (h *poolHarness) counters() (wait, kill int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.waitN, h.killN
}

func (h *poolHarness) lastDelay() time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.delays) == 0 {
		return 0
	}
	return h.delays[len(h.delays)-1]
}

func (h *poolHarness) watchCount(conn *poolFakeConn, dir string) int {
	conn.ops.mu.Lock()
	defer conn.ops.mu.Unlock()
	return len(conn.ops.watches[dir])
}

func poolWaitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(processTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// nextOffer returns the timer channel the respawn loop is waiting on.
func (h *poolHarness) nextOffer(t *testing.T) chan time.Time {
	t.Helper()
	select {
	case ch := <-h.offers:
		return ch
	case <-time.After(processTimeout):
		t.Fatal("timed out waiting for a respawn timer")
		return nil
	}
}

func poolMustLookup(t *testing.T, b *Backend, socket string) {
	t.Helper()
	if _, err := b.Lookup(context.Background(), "/cfg", socket); err != nil && !errors.Is(err, inbox.ErrNoRecord) {
		t.Fatalf("Lookup: %v", err)
	}
}

func TestPoolSpawnsOnFirstUseNotOnAcquire(t *testing.T) {
	h := newPoolHarness(t)
	b, release, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()

	if got := h.startCount(); got != 0 {
		t.Fatalf("Start calls after Acquire = %d, want 0", got)
	}
	poolMustLookup(t, b, "/one.sock")
	if got := h.startCount(); got != 1 {
		t.Fatalf("Start calls after first Lookup = %d, want 1", got)
	}
	poolMustLookup(t, b, "/one.sock")
	if got := h.startCount(); got != 1 {
		t.Fatalf("Start calls after second Lookup = %d, want 1", got)
	}
}

func TestPoolOneHelperPerAccount(t *testing.T) {
	h := newPoolHarness(t)
	b1, r1, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire 1001: %v", err)
	}
	defer r1()
	b2, r2, err := h.pool.Acquire(context.Background(), 1002)
	if err != nil {
		t.Fatalf("Acquire 1002: %v", err)
	}
	defer r2()

	poolMustLookup(t, b2, "/two.sock")

	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := b1.Lookup(context.Background(), "/cfg", "/one.sock")
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil && !errors.Is(err, inbox.ErrNoRecord) {
			t.Errorf("concurrent Lookup: %v", err)
		}
	}

	if got := h.startCount(); got != 2 {
		t.Fatalf("Start calls = %d, want 2 (one per account)", got)
	}
	if got := h.startsFor(1001); got != 1 {
		t.Fatalf("Start calls for uid 1001 = %d, want 1", got)
	}
	if got := h.startsFor(1002); got != 1 {
		t.Fatalf("Start calls for uid 1002 = %d, want 1", got)
	}
}

func TestPoolRejectsWrongIdentity(t *testing.T) {
	h := newPoolHarness(t)
	h.setReady(func(acct Account) *helperpb.Ready {
		return &helperpb.Ready{
			ProtocolVersion: helperpb.ProtocolVersion,
			Uid:             acct.UID + 1,
			Gid:             acct.GID,
		}
	})
	b, release, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()

	if _, err := b.Lookup(context.Background(), "/cfg", "/x.sock"); err == nil {
		t.Fatal("Lookup succeeded despite a wrong identity in Ready")
	}
	if _, kill := h.counters(); kill != 1 {
		t.Fatalf("Kill calls = %d, want 1", kill)
	}
}

func TestPoolRejectsVersionMismatch(t *testing.T) {
	h := newPoolHarness(t)
	h.setReady(func(acct Account) *helperpb.Ready {
		return &helperpb.Ready{
			ProtocolVersion: helperpb.ProtocolVersion + 1,
			Uid:             acct.UID,
			Gid:             acct.GID,
		}
	})
	b, release, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()

	_, err = b.Lookup(context.Background(), "/cfg", "/x.sock")
	if err == nil {
		t.Fatal("Lookup succeeded despite a protocol version mismatch")
	}
	if !strings.Contains(err.Error(), "protocol version") {
		t.Fatalf("Lookup error = %v, want it to mention the protocol version", err)
	}
	if _, kill := h.counters(); kill != 1 {
		t.Fatalf("Kill calls = %d, want 1", kill)
	}
}

func TestPoolReadyTimeout(t *testing.T) {
	const readyTimeout = 100 * time.Millisecond
	h := newPoolHarness(t, func(o *PoolOptions) {
		o.ReadyTimeout = readyTimeout
	})
	h.setBlocking(true)
	b, release, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()

	// The pool's own ready timeout is the assertion. The wait is that timeout
	// plus the package fallback, so a pool that ignores it fails instead of
	// hanging the test.
	done := make(chan error, 1)
	go func() {
		_, err := b.Lookup(context.Background(), "/cfg", "/x.sock")
		done <- err
	}()
	select {
	case err = <-done:
	case <-time.After(readyTimeout + processTimeout):
		t.Fatal("Lookup did not give up at the pool's ready timeout")
	}
	if err == nil {
		t.Fatal("Lookup succeeded although the helper never became ready")
	}
	if _, kill := h.counters(); kill != 1 {
		t.Fatalf("Kill calls = %d, want 1", kill)
	}
}

// TestPoolSpawnFailureIsUnavailable checks that a helper that cannot be started
// is reported with ErrUnavailable. Registration uses that sentinel to tell a
// helper failure from a session record that exists but cannot be read.
func TestPoolSpawnFailureIsUnavailable(t *testing.T) {
	h := newPoolHarness(t)
	h.setStartErr(errors.New("fork/exec /usr/local/bin/peers: invalid argument"))
	b, release, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()

	_, err = b.Lookup(context.Background(), "/cfg", "/x.sock")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Lookup error = %v, want it to wrap ErrUnavailable", err)
	}
}

// TestPoolAccountLookupFailureIsUnavailable checks that a failure to read the
// account database is reported with ErrUnavailable. The helper cannot be
// started without the account, so registration must report a helper failure
// rather than a missing session record.
func TestPoolAccountLookupFailureIsUnavailable(t *testing.T) {
	lookupErr := errors.New("user: lookup uid 1001: temporary failure")
	h := newPoolHarness(t, func(o *PoolOptions) {
		o.Lookup = func(uint32) (Account, error) { return Account{}, lookupErr }
	})
	b, release, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()

	_, err = b.Lookup(context.Background(), "/cfg", "/x.sock")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Lookup error = %v, want it to wrap ErrUnavailable", err)
	}
	if !errors.Is(err, lookupErr) {
		t.Fatalf("Lookup error = %v, want it to keep the account lookup error", err)
	}
	if got := h.startCount(); got != 0 {
		t.Fatalf("Start calls = %d, want 0", got)
	}
}

// TestPoolReadyTimeoutIsUnavailable checks that a helper that never sends its
// Ready frame is reported with ErrUnavailable. The context is the same one the
// registration path uses: a short call budget that expires before ReadyTimeout,
// so the caller's own deadline ends the wait. Without the sentinel the
// registration reports a missing record instead of a helper that did not start.
func TestPoolReadyTimeoutIsUnavailable(t *testing.T) {
	h := newPoolHarness(t)
	h.setBlocking(true)
	b, release, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()

	// The caller's own budget is the assertion: it expires before ReadyTimeout,
	// so the DeadlineExceeded below can only come from the caller's context.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = b.Lookup(ctx, "/cfg", "/x.sock")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Lookup error = %v, want it to wrap ErrUnavailable", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Lookup error = %v, want it to keep the context deadline", err)
	}
}

// TestPoolReadyCancelIsNotUnavailable checks that a caller's own cancellation
// while the helper is still starting up is not reported as a helper failure.
// The helper is not what ended the call, so registration must not turn the
// cancellation into account_helper_failed.
func TestPoolReadyCancelIsNotUnavailable(t *testing.T) {
	h := newPoolHarness(t)
	h.setBlocking(true)
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
	// Cancel only after the child exists and is being waited for, so the
	// cancellation ends the Ready handshake rather than the spawn.
	poolWaitFor(t, "the helper to start", func() bool { return h.startCount() == 1 })
	cancel()

	err = <-errCh
	if errors.Is(err, ErrUnavailable) {
		t.Fatalf("Lookup error = %v, want a caller cancellation not to wrap ErrUnavailable", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Lookup error = %v, want context.Canceled", err)
	}
}

// TestPoolWatchTimeoutIsUnavailable checks that the session-directory watch
// path reports the same sentinel when the helper never becomes ready. The
// watch is what fails first during registration, and it must not be mistaken
// for a record that does not exist either.
func TestPoolWatchTimeoutIsUnavailable(t *testing.T) {
	h := newPoolHarness(t)
	h.setBlocking(true)
	b, release, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()

	// The caller's own deadline is the assertion for the watch path.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := b.Watch(ctx, "/cfg", func(int) {}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Watch error = %v, want it to wrap ErrUnavailable", err)
	} else if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Watch error = %v, want it to keep the context deadline", err)
	}
}

// TestPoolDiscardsHelperAfterCallTimeout checks that one call timing out marks
// the helper bad. A helper that ignores a request while its pipe is not full
// would otherwise make every later call wait for the full timeout.
func TestPoolDiscardsHelperAfterCallTimeout(t *testing.T) {
	h := newPoolHarness(t)
	h.setFirstRereadBlocking(true)
	b, release, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()

	// The caller's own deadline is the assertion for the discarding path too.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := b.Reread(ctx, inbox.Session{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Reread error = %v, want context.DeadlineExceeded", err)
	}
	if got := h.startCount(); got != 1 {
		t.Fatalf("Start calls after the timeout = %d, want 1", got)
	}
	if _, kill := h.counters(); kill != 1 {
		t.Fatalf("Kill calls after the timeout = %d, want 1: the stopped helper must be discarded", kill)
	}
	if got := h.conn(0).closedCount(); got != 1 {
		t.Fatalf("first connection closed = %d, want 1", got)
	}

	// The next call must start a new helper rather than wait on the old one.
	// The package fallback bounds the rebuild: a missing rebuild fails at the
	// call instead of blocking until the test timeout.
	rebuildCtx, cancelRebuild := context.WithTimeout(context.Background(), processTimeout)
	defer cancelRebuild()
	if _, err := b.Reread(rebuildCtx, inbox.Session{}); err != nil {
		t.Fatalf("Reread after the timeout: %v", err)
	}
	if got := h.startCount(); got != 2 {
		t.Fatalf("Start calls after the rebuild = %d, want 2", got)
	}
}

func TestPoolSpawnFailureDeliverIsNotWritten(t *testing.T) {
	h := newPoolHarness(t)
	h.setStartErr(errors.New("boom"))
	b, release, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()

	err = b.Deliver(context.Background(), inbox.Session{}, "sig", "text", "id", time.Second)
	if !errors.Is(err, inbox.ErrNotWritten) {
		t.Fatalf("Deliver error = %v, want it to wrap inbox.ErrNotWritten", err)
	}
}

func TestPoolClosesConnWhenUnused(t *testing.T) {
	h := newPoolHarness(t)
	b, release, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	poolMustLookup(t, b, "/x.sock")
	release()

	conn := h.conn(0)
	poolWaitFor(t, "the idle connection to close", func() bool { return conn.closedCount() == 1 })
	poolWaitFor(t, "the idle child to be reaped", func() bool {
		wait, _ := h.counters()
		return wait == 1
	})

	b2, release2, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire after idle close: %v", err)
	}
	defer release2()
	poolMustLookup(t, b2, "/x.sock")
	if got := h.startCount(); got != 2 {
		t.Fatalf("Start calls after a fresh use = %d, want 2", got)
	}
}

func TestPoolKeepsConnWhileWatching(t *testing.T) {
	h := newPoolHarness(t)
	ctx := context.Background()
	b, release, err := h.pool.Acquire(ctx, 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	stop, err := b.Watch(ctx, "/cfg", func(int) {})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	release()

	conn := h.conn(0)
	// release reclaims synchronously, so no goroutine can close the connection
	// between here and the check while the watch is still registered.
	if got := conn.closedCount(); got != 0 {
		t.Fatalf("connection closed = %d while a watch is active, want 0", got)
	}

	stop()
	poolWaitFor(t, "the connection to close after the watch stopped", func() bool {
		return conn.closedCount() == 1
	})
}

func TestPoolRespawnRestoresWatches(t *testing.T) {
	h := newPoolHarness(t)
	ctx := context.Background()
	b, release, err := h.pool.Acquire(ctx, 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()

	var mu sync.Mutex
	var pids []int
	stop, err := b.Watch(ctx, "/cfg", func(pid int) {
		mu.Lock()
		pids = append(pids, pid)
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer stop()

	h.conn(0).crash()

	offer := h.nextOffer(t)
	if got := h.lastDelay(); got != time.Second {
		t.Fatalf("first respawn delay = %v, want 1s", got)
	}
	offer <- time.Now()

	poolWaitFor(t, "the helper to respawn", func() bool { return h.connCount() == 2 })
	second := h.conn(1)
	poolWaitFor(t, "the watch to be restored", func() bool { return h.watchCount(second, "/cfg") == 1 })
	poolWaitFor(t, "the pid=0 reread callback", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pids) == 1 && pids[0] == 0
	})

	second.ops.fire("/cfg", 7)
	poolWaitFor(t, "an event from the restored watch", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pids) == 2 && pids[1] == 7
	})
}

func TestPoolRespawnBackoffDoubles(t *testing.T) {
	h := newPoolHarness(t)
	ctx := context.Background()
	b, release, err := h.pool.Acquire(ctx, 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()

	stop, err := b.Watch(ctx, "/cfg", func(int) {})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer stop()

	h.conn(0).crash()
	h.setStartErr(errors.New("boom"))

	wantDelays := []time.Duration{poolTestBackoffOne, poolTestBackoffTwo, poolTestBackoffThree}
	for i, want := range wantDelays {
		offer := h.nextOffer(t)
		if got := h.lastDelay(); got != want {
			t.Fatalf("respawn delay = %v, want %v", got, want)
		}
		if i == len(wantDelays)-1 {
			// Clear the failure before waking the last attempt so it succeeds.
			h.setStartErr(nil)
		}
		offer <- time.Now()
	}

	poolWaitFor(t, "the helper to respawn after the backoff attempts", func() bool {
		return h.connCount() == 2
	})

	h.conn(1).crash()
	offer := h.nextOffer(t)
	if got := h.lastDelay(); got != time.Second {
		t.Fatalf("respawn delay after a successful respawn = %v, want 1s", got)
	}
	offer <- time.Now()
}

func TestPoolCloseWaitsForSpawnInProgress(t *testing.T) {
	h := newPoolHarness(t)
	h.setBlocking(true)     // WaitReady blocks until the spawn context ends
	h.setWaitBlocking(true) // Wait returns only after Kill

	b, release, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()

	lookupDone := make(chan error, 1)
	go func() {
		_, err := b.Lookup(context.Background(), "/cfg", "/x.sock")
		lookupDone <- err
	}()
	poolWaitFor(t, "the spawn to start", func() bool { return h.startCount() == 1 })

	closed := make(chan struct{})
	go func() {
		_ = h.pool.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(processTimeout):
		t.Fatal("Close did not return after cancelling the in-progress spawn")
	}
	select {
	case err := <-lookupDone:
		t.Logf("Lookup returned after Close: %v", err)
	case <-time.After(processTimeout):
		t.Fatal("Lookup did not return after Close")
	}

	wait, kill := h.counters()
	if kill != 1 {
		t.Fatalf("Kill calls = %d, want 1: Close returned before the child was killed", kill)
	}
	if wait != 1 {
		t.Fatalf("Wait calls = %d, want 1: Close returned before the child was reaped", wait)
	}
}

func TestPoolCloseWaitsForReclaimInProgress(t *testing.T) {
	h := newPoolHarness(t)
	h.setWaitBlocking(true)

	b, release, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	poolMustLookup(t, b, "/x.sock")
	release()

	conn := h.conn(0)
	poolWaitFor(t, "the reclaim to close the connection", func() bool {
		return conn.closedCount() == 1
	})

	began := time.Now()
	if err := h.pool.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if elapsed := time.Since(began); elapsed < reapTimeout/2 {
		t.Fatalf("Close returned after %v, want it to wait for the reap timeout", elapsed)
	}

	wait, kill := h.counters()
	if kill != 1 {
		t.Fatalf("Kill calls = %d, want 1: Close returned before the child was killed", kill)
	}
	if wait != 1 {
		t.Fatalf("Wait calls = %d, want 1: Close returned before the child was reaped", wait)
	}
}

func TestPoolCloseClosesEveryConn(t *testing.T) {
	h := newPoolHarness(t)
	ctx := context.Background()

	b1, r1, err := h.pool.Acquire(ctx, 1001)
	if err != nil {
		t.Fatalf("Acquire 1001: %v", err)
	}
	defer r1()
	b2, r2, err := h.pool.Acquire(ctx, 1002)
	if err != nil {
		t.Fatalf("Acquire 1002: %v", err)
	}
	defer r2()
	poolMustLookup(t, b1, "/one.sock")
	poolMustLookup(t, b2, "/two.sock")

	if err := h.pool.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for i := 0; i < 2; i++ {
		if got := h.conn(i).closedCount(); got != 1 {
			t.Fatalf("connection %d closed = %d, want 1", i, got)
		}
	}
	if wait, _ := h.counters(); wait != 2 {
		t.Fatalf("Wait calls = %d, want 2", wait)
	}
	if _, _, err := h.pool.Acquire(ctx, 1001); err == nil {
		t.Fatal("Acquire succeeded after Close")
	}
}

// poolSyncBuffer is a log sink the background goroutines can write while the
// test reads it.
type poolSyncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *poolSyncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *poolSyncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// poolAccountHelperRecord waits for a record with the given message and decodes
// the JSON lines the pool's logger wrote. The pool's logger is the Broker's
// NewLogger in production; a plain JSON handler keeps the message under "msg".
func poolAccountHelperRecord(t *testing.T, logs *poolSyncBuffer, message string) map[string]any {
	t.Helper()
	poolWaitFor(t, "the "+message+" record", func() bool {
		return strings.Contains(logs.String(), `"msg":"`+message+`"`)
	})
	for _, line := range strings.Split(strings.TrimSuffix(logs.String(), "\n"), "\n") {
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line is not JSON: %v: %q", err, line)
		}
		if rec["msg"] == message {
			return rec
		}
	}
	t.Fatalf("no %s record in %q", message, logs.String())
	return nil
}

// poolLogRecords decodes every JSON line the pool's logger wrote.
func poolLogRecords(t *testing.T, logs *poolSyncBuffer) []map[string]any {
	t.Helper()
	raw := strings.TrimSuffix(logs.String(), "\n")
	if raw == "" {
		return nil
	}
	lines := strings.Split(raw, "\n")
	records := make([]map[string]any, len(lines))
	for i, line := range lines {
		if err := json.Unmarshal([]byte(line), &records[i]); err != nil {
			t.Fatalf("log line %d is not JSON: %v: %q", i, err, line)
		}
	}
	return records
}

// poolAssertNoHelperWarning fails when any record for an account helper was
// written at WARN or ERROR.
func poolAssertNoHelperWarning(t *testing.T, logs *poolSyncBuffer) {
	t.Helper()
	for _, rec := range poolLogRecords(t, logs) {
		msg, _ := rec["msg"].(string)
		if !strings.HasPrefix(msg, "account_helper_") {
			continue
		}
		if level := rec["level"]; level == "ERROR" || level == "WARN" {
			t.Fatalf("%s was logged at %v during a stop (log: %s)", msg, level, logs.String())
		}
	}
}

// TestPoolStoppingHelperExitIsNotAnError reproduces a service stop: the
// Broker's signal context ends and the helper goes away at the same time. That
// exit belongs to the stop, not to a failure, so no account_helper_ record may
// be logged at ERROR or WARN, and the helper must not be respawned.
func TestPoolStoppingHelperExitIsNotAnError(t *testing.T) {
	var logs poolSyncBuffer
	stop := make(chan struct{})
	h := newPoolHarness(t, func(o *PoolOptions) {
		o.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
		o.Stopping = stop
	})
	ctx := context.Background()
	b, release, err := h.pool.Acquire(ctx, 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()
	poolMustLookup(t, b, "/sock")

	// A watch keeps the pool interested in the helper, so a respawn is possible
	// and the test exercises the same state a service stop reaches.
	stopWatch, err := b.Watch(ctx, "/cfg", func(int) {})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer stopWatch()

	close(stop)
	h.conn(0).crash()
	// watchLive is counted in the child group, so waiting for the group to
	// drain means every line it writes has been written before it is read.
	h.pool.children.closeAndWait()

	poolAssertNoHelperWarning(t, &logs)
	if !strings.Contains(logs.String(), `"msg":"account_helper_exited"`) {
		t.Fatalf("helper exit was not logged: %s", logs.String())
	}
	if got := h.startCount(); got != 1 {
		t.Fatalf("Start calls = %d, want 1: a stop must not respawn the helper", got)
	}
}

// TestPoolHelperExitAfterCloseIsInfo covers a helper exit observed after Close
// started, with no Stopping channel configured, which is the self-update path.
// Close closes closeCh, and stoppingNow must treat that as a stop.
func TestPoolHelperExitAfterCloseIsInfo(t *testing.T) {
	var logs poolSyncBuffer
	p := NewPool(PoolOptions{Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Close has returned, so a later Add only balances watchLive's done.
	p.children.wg.Add(1)
	conn := &poolFakeConn{done: make(chan struct{}), ops: &fakeOps{}}
	live := &liveHelper{
		conn: conn,
		proc: &Process{Wait: func() error { return errors.New("exit status 1") }},
	}
	st := &helperState{uid: 1001, watches: make(map[uint64]*poolWatch)}
	st.live = live
	conn.crash()

	p.watchLive(st, live)

	poolAssertNoHelperWarning(t, &logs)
	if !strings.Contains(logs.String(), `"msg":"account_helper_exited"`) {
		t.Fatalf("helper exit was not logged: %s", logs.String())
	}
}

// TestPoolUnexpectedHelperExitIsAnError keeps an exit outside a stop at the
// error level.
func TestPoolUnexpectedHelperExitIsAnError(t *testing.T) {
	var logs poolSyncBuffer
	h := newPoolHarness(t, func(o *PoolOptions) {
		o.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	})
	b, release, err := h.pool.Acquire(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()
	poolMustLookup(t, b, "/sock")

	h.conn(0).crash()

	rec := poolAccountHelperRecord(t, &logs, "account_helper_exited")
	if got, want := rec["level"], "ERROR"; got != want {
		t.Fatalf("unexpected helper exit level = %v, want %v (log: %s)", got, want, logs.String())
	}
}

// TestDetachLiveIgnoresARemovedHelper covers a late detach for a helper the
// account already lost: reclamation or Close removes it first, and the goroutine
// watching its connection reaches detachLive afterwards, by which time a new
// helper may already be current. The late detach must report false and leave the
// new helper and its watches alone.
func TestDetachLiveIgnoresARemovedHelper(t *testing.T) {
	st := &helperState{uid: 1001, watches: make(map[uint64]*poolWatch)}
	stale := &liveHelper{conn: &poolFakeConn{done: make(chan struct{})}}
	current := &liveHelper{conn: &poolFakeConn{done: make(chan struct{})}}
	st.live = current
	entry := &poolWatch{id: 1, configDir: "/cfg", onChange: func(int) {}, conn: current.conn, stop: func() {}}
	st.watches[entry.id] = entry

	watches, ok := detachLive(st, stale)
	if ok {
		t.Fatal("detachLive reported a helper that was already removed as current")
	}
	if watches != 0 {
		t.Fatalf("detachLive watches = %d, want 0", watches)
	}
	if st.live != current {
		t.Fatal("detachLive cleared the current helper while detaching an old one")
	}
	entry.mu.Lock()
	conn, stop := entry.conn, entry.stop
	entry.mu.Unlock()
	if conn != current.conn || stop == nil {
		t.Fatal("detachLive detached a watch of the current helper while detaching an old one")
	}
}
