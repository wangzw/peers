package broker

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/helper"
	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/inbox"
	"github.com/wangzw/peers/internal/testkit/fakesession"
)

// blockingAfterLookupBackend blocks after the first Lookup reads a record, so a
// registry event can land before the agent enters the table.
type blockingAfterLookupBackend struct {
	SessionBackend
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingAfterLookupBackend) Lookup(ctx context.Context, configDir, socketPath string) (inbox.Session, error) {
	s, err := b.SessionBackend.Lookup(ctx, configDir, socketPath)
	b.once.Do(func() {
		close(b.started)
		<-b.release
	})
	return s, err
}

type lookupBlockingSessions struct {
	SessionRouter
	backend *blockingAfterLookupBackend
}

func (s *lookupBlockingSessions) Acquire(ctx context.Context, uid uint32) (SessionBackend, func(), error) {
	base, release, err := s.SessionRouter.Acquire(ctx, uid)
	if err != nil {
		return nil, nil, err
	}
	s.backend.SessionBackend = base
	return s.backend, release, nil
}

type attemptGate struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *attemptGate) hook(*regConn) {
	g.once.Do(func() {
		close(g.started)
		<-g.release
	})
}

func setAttemptReadHook(t *testing.T, fn func(c *regConn)) {
	t.Helper()
	attemptReadHook.Store(&fn)
	t.Cleanup(func() { attemptReadHook.Store(nil) })
}

func setAttemptBusyHook(t *testing.T, fn func(c *regConn)) {
	t.Helper()
	attemptBusyHook.Store(&fn)
	t.Cleanup(func() { attemptBusyHook.Store(nil) })
}

// withholdRecordBackend makes the first Lookup report a missing record though the
// backing backend holds one, so the only registry event is the one the test
// pushes itself.
type withholdRecordBackend struct {
	SessionBackend
	once sync.Once
}

func (b *withholdRecordBackend) Lookup(ctx context.Context, configDir, socketPath string) (inbox.Session, error) {
	withheld := false
	b.once.Do(func() { withheld = true })
	if withheld {
		return inbox.Session{}, inbox.ErrNoRecord
	}
	return b.SessionBackend.Lookup(ctx, configDir, socketPath)
}

type withholdRecordSessions struct {
	SessionRouter
	backend *withholdRecordBackend
}

func (s *withholdRecordSessions) Acquire(ctx context.Context, uid uint32) (SessionBackend, func(), error) {
	base, release, err := s.SessionRouter.Acquire(ctx, uid)
	if err != nil {
		return nil, nil, err
	}
	s.backend.SessionBackend = base
	return s.backend, release, nil
}

type failingLookupBackend struct {
	SessionBackend
	err error
}

func (b *failingLookupBackend) Lookup(context.Context, string, string) (inbox.Session, error) {
	return inbox.Session{}, b.err
}

type failingLookupSessions struct {
	SessionRouter
	err error
}

func (s *failingLookupSessions) Acquire(ctx context.Context, uid uint32) (SessionBackend, func(), error) {
	base, release, err := s.SessionRouter.Acquire(ctx, uid)
	if err != nil {
		return nil, nil, err
	}
	return &failingLookupBackend{SessionBackend: base, err: s.err}, release, nil
}

// TestRegisterReportsAccountHelperFailure checks that a Lookup failure wrapping
// helper.ErrUnavailable is reported as account_helper_failed instead of
// session_record_not_found.
func TestRegisterReportsAccountHelperFailure(t *testing.T) {
	tb := newTestBroker(t, func(d *Deps) {
		d.Sessions = &failingLookupSessions{
			SessionRouter: d.Sessions,
			err:           fmt.Errorf("%w: fork/exec: invalid argument", helper.ErrUnavailable),
		}
	})
	dir := shortTempDir(t)
	socketPath := filepath.Join(dir, "s.sock")
	rs := startRegistrationAt(t, tb.client, dir, socketPath)

	waitForReg(t, "the connection to wait", func() bool {
		return tb.lockedWaitingCount() == 1
	})
	tb.clock.Advance(10 * time.Second)

	if reason := rs.problem(eventTimeout); reason != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_ACCOUNT_HELPER_FAILED {
		t.Fatalf("reason = %v, want account_helper_failed", reason)
	}
}

// TestRegisterReportsAccountHelperTimeout checks that a helper that never
// reports Ready makes registration report account_helper_failed.
func TestRegisterReportsAccountHelperTimeout(t *testing.T) {
	tb := newTestBroker(t, func(d *Deps) {
		d.Sessions = silentHelperSessions{pool: neverReadyPool(t)}
	})
	dir := shortTempDir(t)
	socketPath := filepath.Join(dir, "s.sock")
	rs := startRegistrationAt(t, tb.client, dir, socketPath)

	waitForReg(t, "the connection to wait", func() bool {
		return tb.lockedWaitingCount() == 1
	})
	tb.clock.Advance(10 * time.Second)

	if reason := rs.problem(eventTimeout); reason != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_ACCOUNT_HELPER_FAILED {
		t.Fatalf("reason = %v, want account_helper_failed", reason)
	}
}

type silentHelperSessions struct{ pool *helper.Pool }

func (s silentHelperSessions) Acquire(ctx context.Context, uid uint32) (SessionBackend, func(), error) {
	backend, release, err := s.pool.Acquire(ctx, uid)
	if err != nil {
		return nil, nil, err
	}
	return backend, release, nil
}

type brokerTestWriteCloser struct{ io.Writer }

func (brokerTestWriteCloser) Close() error { return nil }

// neverReadyPool starts a helper process that stays silent and holds its stdout
// open, so the Broker waits for the call deadline instead of seeing a lost pipe.
func neverReadyPool(t *testing.T) *helper.Pool {
	t.Helper()
	pool := helper.NewPool(helper.PoolOptions{
		Exe: "/bin/true",
		Start: func(context.Context, helper.Account) (*helper.Process, error) {
			pr, pw, err := os.Pipe()
			if err != nil {
				return nil, err
			}
			t.Cleanup(func() {
				_ = pw.Close()
				_ = pr.Close()
			})
			return &helper.Process{
				Stdin:  brokerTestWriteCloser{io.Discard},
				Stdout: pr,
				Wait:   func() error { return nil },
				Kill:   func() error { return nil },
			}, nil
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	t.Cleanup(func() { _ = pool.Close() })
	return pool
}

// TestRegisterWarnsOnLookupFailure checks that a Lookup failure that is neither a
// missing nor a partial record leaves a Warn record.
func TestRegisterWarnsOnLookupFailure(t *testing.T) {
	tb := newTestBroker(t, func(d *Deps) {
		d.Sessions = &failingLookupSessions{
			SessionRouter: d.Sessions,
			err:           fmt.Errorf("inbox: reading session registry: %w", fs.ErrPermission),
		}
	})
	dir := shortTempDir(t)
	socketPath := filepath.Join(dir, "s.sock")
	_ = startRegistrationAt(t, tb.client, dir, socketPath)

	waitForReg(t, "a Warn record for the failed lookup", func() bool {
		for _, rec := range tb.logLines(t) {
			if rec["level"] == "WARN" && rec["event"] == EvRegistrationWaiting {
				return true
			}
		}
		return false
	})
}

// TestRegisterKeepsUnreadableRecordAsMissing checks that an error reading a
// record that exists stays session_record_not_found, since only the helper's own
// failure sentinel means it could not start or talk to the Broker.
func TestRegisterKeepsUnreadableRecordAsMissing(t *testing.T) {
	tb := newTestBroker(t, func(d *Deps) {
		d.Sessions = &failingLookupSessions{
			SessionRouter: d.Sessions,
			err:           fmt.Errorf("inbox: reading session registry: %w", fs.ErrPermission),
		}
	})
	dir := shortTempDir(t)
	socketPath := filepath.Join(dir, "s.sock")
	rs := startRegistrationAt(t, tb.client, dir, socketPath)

	waitForReg(t, "the connection to wait", func() bool {
		return tb.lockedWaitingCount() == 1
	})
	tb.clock.Advance(10 * time.Second)

	if reason := rs.problem(eventTimeout); reason != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SESSION_RECORD_NOT_FOUND {
		t.Fatalf("reason = %v, want session_record_not_found", reason)
	}
}

// TestRegisterKeepsRetryRequestedAtAttemptEnd checks that a registry event
// arriving between the running attempt's read of retry and its unlock still
// causes another pass.
func TestRegisterKeepsRetryRequestedAtAttemptEnd(t *testing.T) {
	dir := shortTempDir(t)
	socketPath := filepath.Join(dir, "s.sock")
	fake := fakesession.New(t, fakesession.Options{ConfigDir: dir, SocketPath: socketPath, Name: "late"})
	withheld := &withholdRecordBackend{}
	tb := newTestBroker(t, func(d *Deps) {
		d.Sessions = &withholdRecordSessions{SessionRouter: d.Sessions, backend: withheld}
	})

	gate := &attemptGate{started: make(chan struct{}), release: make(chan struct{})}
	setAttemptReadHook(t, gate.hook)
	busy := make(chan struct{}, 1)
	setAttemptBusyHook(t, func(*regConn) {
		select {
		case busy <- struct{}{}:
		default:
		}
	})

	rs := startRegistrationAt(t, tb.client, dir, socketPath)

	select {
	case <-gate.started:
	case <-time.After(eventTimeout):
		t.Fatal("the first attempt did not reach the pause")
	}

	uid := uint32(tb.b.deps.Geteuid())
	tb.b.events.push(uid, watchKeyFor(uid, dir).dir, fake.PID())
	select {
	case <-busy:
	case <-time.After(eventTimeout):
		t.Fatal("the registry event did not reach the running attempt")
	}

	close(gate.release)

	complete := rs.complete(eventTimeout)
	if complete.GetAgentAddress() != "late@hostA" {
		t.Fatalf("agent_address = %q, want late@hostA", complete.GetAgentAddress())
	}
}

// TestRegisterKeepsChangeMadeDuringLookup checks that a record change handled
// while the first Lookup is in flight is applied to the attached agent.
func TestRegisterKeepsChangeMadeDuringLookup(t *testing.T) {
	blocked := &blockingAfterLookupBackend{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	tb := newTestBroker(t, func(d *Deps) {
		d.Sessions = &lookupBlockingSessions{SessionRouter: d.Sessions, backend: blocked}
	})
	fake := fakesession.New(t, fakesession.Options{Name: "changing", Status: "idle"})
	rs := startRegistration(t, tb, fake)

	select {
	case <-blocked.started:
	case <-time.After(eventTimeout):
		t.Fatal("the first lookup did not complete within the deadline")
	}

	fake.SetStatus("busy")
	waitForReg(t, "the registry event to be handled", func() bool {
		return tb.b.events.handledCount() > 0
	})
	close(blocked.release)

	rs.complete(eventTimeout)
	waitForReg(t, "the agent to pick up the status change", func() bool {
		snap, ok := tb.b.lookupAgent(mustAgentID(t, fake.SocketPath()))
		return ok && snap.Session.Status == "busy"
	})
}
