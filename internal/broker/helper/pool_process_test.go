package helper

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/inbox"
	"github.com/wangzw/peers/internal/testkit/fakesession"
)

type helperRun struct {
	pid  int
	proc *Process
	wait chan error
}

type helperRecorder struct {
	mu   sync.Mutex
	runs []*helperRun
}

func (r *helperRecorder) start(t *testing.T) func(context.Context, Account) (*Process, error) {
	t.Helper()
	return func(context.Context, Account) (*Process, error) {
		child := startTestRole(t, "account-helper")
		run := &helperRun{
			pid:  child.cmd.Process.Pid,
			wait: make(chan error, 1),
		}
		run.proc = &Process{
			Stdin:  child.stdin,
			Stdout: child.stdout,
			Wait: func() error {
				err := child.wait()
				run.wait <- err
				return err
			},
			Kill: func() error { return child.cmd.Process.Kill() },
		}
		r.mu.Lock()
		r.runs = append(r.runs, run)
		r.mu.Unlock()
		return run.proc, nil
	}
}

func (r *helperRecorder) snapshot() []*helperRun {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*helperRun(nil), r.runs...)
}

func helperTestPool(t *testing.T, start func(context.Context, Account) (*Process, error)) *Pool {
	t.Helper()
	pool := NewPool(PoolOptions{
		Start:        start,
		Lookup:       LookupAccount,
		ReadyTimeout: processTimeout,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	t.Cleanup(func() { _ = pool.Close() })
	return pool
}

func lookupMissing(t *testing.T, b *Backend) {
	t.Helper()
	_, err := b.Lookup(context.Background(), "/no/such/dir", "/no/such/dir/inbox.sock")
	if !errors.Is(err, inbox.ErrNoRecord) {
		t.Fatalf("Lookup error = %v, want inbox.ErrNoRecord", err)
	}
}

// TestPoolHelperExitsWhenUnused checks that an unreferenced helper exits and a later use starts a new child.
func TestPoolHelperExitsWhenUnused(t *testing.T) {
	rec := &helperRecorder{}
	pool := helperTestPool(t, rec.start(t))
	uid := uint32(os.Getuid())

	b, release, err := pool.Acquire(context.Background(), uid)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	lookupMissing(t, b)
	release()

	runs := rec.snapshot()
	if len(runs) != 1 {
		t.Fatalf("helper children = %d, want 1", len(runs))
	}
	select {
	case err := <-runs[0].wait:
		if err != nil {
			t.Fatalf("unused helper exited with %v, want a clean exit", err)
		}
	case <-time.After(processTimeout):
		t.Fatalf("unused helper did not exit within %v", processTimeout)
	}

	b2, release2, err := pool.Acquire(context.Background(), uid)
	if err != nil {
		t.Fatalf("Acquire after idle exit: %v", err)
	}
	defer release2()
	lookupMissing(t, b2)

	runs = rec.snapshot()
	if len(runs) != 2 {
		t.Fatalf("helper children after a fresh use = %d, want 2", len(runs))
	}
	if runs[0].pid == runs[1].pid {
		t.Fatalf("the pool reused pid %d, want a new child after reclaim", runs[0].pid)
	}
}

// TestPoolDeliverInFlightOnCrashIsUncertain checks that a delivery in flight when the real helper is killed is uncertain.
func TestPoolDeliverInFlightOnCrashIsUncertain(t *testing.T) {
	fake := fakesession.New(t, fakesession.Options{
		ConfigDir: t.TempDir(),
		Mode:      fakesession.HoldOpen,
	})

	rec := &helperRecorder{}
	pool := helperTestPool(t, rec.start(t))
	ctx := context.Background()
	uid := uint32(os.Getuid())

	b, release, err := pool.Acquire(ctx, uid)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()
	sess, err := b.Lookup(ctx, fake.ConfigDir(), fake.SocketPath())
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- b.Deliver(ctx, sess, "sig", "text", "m1", 10*time.Second)
	}()

	if _, ok := fake.WaitFrame(processTimeout); !ok {
		t.Fatal("the helper never wrote the message frame")
	}

	runs := rec.snapshot()
	if len(runs) != 1 {
		t.Fatalf("helper children = %d, want 1", len(runs))
	}
	if err := runs[0].proc.Kill(); err != nil {
		t.Fatalf("killing the helper: %v", err)
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, inbox.ErrUncertain) {
			t.Fatalf("Deliver error = %v, want inbox.ErrUncertain", err)
		}
	case <-time.After(processTimeout):
		t.Fatal("Deliver did not return after the helper crashed")
	}
}

// TestPoolCloseTerminatesHelpers checks that Close reaps every live helper child and refuses later Acquires.
func TestPoolCloseTerminatesHelpers(t *testing.T) {
	rec := &helperRecorder{}
	uid := uint32(os.Getuid())
	pool := NewPool(PoolOptions{
		Start: rec.start(t),
		// Both uids resolve to this account so the child's Ready identity matches; the test is about process lifetime.
		Lookup: func(uint32) (Account, error) {
			return LookupAccount(uid)
		},
		ReadyTimeout: processTimeout,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	ctx := context.Background()

	b1, r1, err := pool.Acquire(ctx, uid)
	if err != nil {
		t.Fatalf("Acquire %d: %v", uid, err)
	}
	defer r1()
	b2, r2, err := pool.Acquire(ctx, uid+1)
	if err != nil {
		t.Fatalf("Acquire %d: %v", uid+1, err)
	}
	defer r2()
	lookupMissing(t, b1)
	lookupMissing(t, b2)

	runs := rec.snapshot()
	if len(runs) != 2 {
		t.Fatalf("live helper children = %d, want 2", len(runs))
	}

	if err := pool.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for i, run := range runs {
		select {
		case err := <-run.wait:
			if err != nil {
				t.Fatalf("helper %d exited with %v, want a clean exit", i, err)
			}
		case <-time.After(processTimeout):
			t.Fatalf("helper %d was not reaped by Close within %v", i, processTimeout)
		}
	}
	if _, _, err := pool.Acquire(ctx, uid); !errors.Is(err, errPoolClosed) {
		t.Fatalf("Acquire after Close = %v, want errPoolClosed", err)
	}
}
