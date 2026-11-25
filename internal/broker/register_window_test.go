package broker

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/inbox"
	"github.com/wangzw/peers/internal/testkit/fakesession"
)

// watchGate counts Watch calls and blocks the second, the agent's own watch,
// until release is closed.
type watchGate struct {
	mu      sync.Mutex
	calls   int
	second  chan struct{}
	release chan struct{}
	once    sync.Once
}

func newWatchGate() *watchGate {
	return &watchGate{second: make(chan struct{}), release: make(chan struct{})}
}

type gatedWatchBackend struct {
	SessionBackend
	gate *watchGate
}

func (b gatedWatchBackend) Watch(ctx context.Context, configDir string, onChange func(pid int)) (func(), error) {
	b.gate.mu.Lock()
	b.gate.calls++
	n := b.gate.calls
	b.gate.mu.Unlock()
	if n == 2 {
		b.gate.once.Do(func() { close(b.gate.second) })
		<-b.gate.release
	}
	return b.SessionBackend.Watch(ctx, configDir, onChange)
}

// resolvedDirSessions models the default configuration directory: an empty
// configDir resolves to dir while the connection's watch key stays empty, so the
// agent's watch is a second Watch call rather than a transferred reference.
type resolvedDirSessions struct {
	SessionRouter
	gate *watchGate
	dir  string
}

func (s *resolvedDirSessions) Acquire(ctx context.Context, uid uint32) (SessionBackend, func(), error) {
	base, release, err := s.SessionRouter.Acquire(ctx, uid)
	if err != nil {
		return nil, nil, err
	}
	inner := resolvedDirBackend{SessionBackend: base, dir: s.dir}
	return gatedWatchBackend{SessionBackend: inner, gate: s.gate}, release, nil
}

type resolvedDirBackend struct {
	SessionBackend
	dir string
}

func (b resolvedDirBackend) resolve(configDir string) string {
	if configDir == "" {
		return b.dir
	}
	return configDir
}

func (b resolvedDirBackend) Lookup(ctx context.Context, configDir, socketPath string) (inbox.Session, error) {
	return b.SessionBackend.Lookup(ctx, b.resolve(configDir), socketPath)
}

func (b resolvedDirBackend) Reread(ctx context.Context, s inbox.Session) (inbox.Session, error) {
	s.ConfigDir = b.resolve(s.ConfigDir)
	return b.SessionBackend.Reread(ctx, s)
}

func (b resolvedDirBackend) Watch(ctx context.Context, configDir string, onChange func(int)) (func(), error) {
	return b.SessionBackend.Watch(ctx, b.resolve(configDir), onChange)
}

type gatedWatchSessions struct {
	SessionRouter
	gate *watchGate
}

func (s *gatedWatchSessions) Acquire(ctx context.Context, uid uint32) (SessionBackend, func(), error) {
	base, release, err := s.SessionRouter.Acquire(ctx, uid)
	if err != nil {
		return nil, nil, err
	}
	return gatedWatchBackend{SessionBackend: base, gate: s.gate}, release, nil
}

func (g *watchGate) callCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.calls
}

func (g *watchGate) waitSecond(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case <-g.second:
	case <-time.After(d):
		t.Fatal("the agent's watch was not created within the deadline")
	}
}

func (tb *testBroker) lockedWaitAttempted() bool {
	tb.b.mu.Lock()
	defer tb.b.mu.Unlock()
	for c := range tb.b.waiting {
		if !c.attempted {
			return false
		}
	}
	return len(tb.b.waiting) > 0
}

// lockedWaitingReported reports whether a waiting connection has sent its
// not_registered event; the flag is set under Broker.mu, so the fake clock makes
// the check independent of the sender goroutine.
func (tb *testBroker) lockedWaitingReported() bool {
	tb.b.mu.Lock()
	defer tb.b.mu.Unlock()
	for c := range tb.b.waiting {
		if c.reported {
			return true
		}
	}
	return false
}

// TestSameKeyRegistrationKeepsItsWatch checks that a connection already watching
// the agent's configuration directory passes its reference to the agent instead
// of releasing it and subscribing again.
func TestSameKeyRegistrationKeepsItsWatch(t *testing.T) {
	gate := newWatchGate()
	close(gate.release)
	tb := newTestBroker(t, func(d *Deps) {
		d.Sessions = &gatedWatchSessions{SessionRouter: d.Sessions, gate: gate}
	})
	fake := fakesession.New(t, fakesession.Options{Name: "one-watch"})
	rs := startRegistration(t, tb, fake)
	rs.complete(eventTimeout)

	if calls := gate.callCount(); calls != 1 {
		t.Fatalf("Watch calls = %d, want 1: the connection's watch should pass to the agent", calls)
	}
}

// TestStatusDuringFirstAgentWatchDoesNotPanic checks that Status can render the
// agent table while the agent's first watch is being created, before its ref is
// assigned.
func TestStatusDuringFirstAgentWatchDoesNotPanic(t *testing.T) {
	gate := newWatchGate()
	fake := fakesession.New(t, fakesession.Options{Name: "windowed"})
	tb := newTestBroker(t, func(d *Deps) {
		d.Sessions = &resolvedDirSessions{SessionRouter: d.Sessions, gate: gate, dir: fake.ConfigDir()}
	})
	rs := startRegistrationAt(t, tb.client, "", fake.SocketPath())

	gate.waitSecond(t, eventTimeout)

	resp, err := tb.client.Status(context.Background(), &localpb.StatusRequest{})
	if err != nil {
		t.Fatalf("Status while the agent's watch is being created: %v", err)
	}
	if len(resp.GetAgents()) != 0 {
		t.Fatalf("Status reported %d agents before the registration attached its ref", len(resp.GetAgents()))
	}

	close(gate.release)
	complete := rs.complete(eventTimeout)
	if complete.GetAgentAddress() != "windowed@hostA" {
		t.Fatalf("agent_address = %q, want windowed@hostA", complete.GetAgentAddress())
	}
}

// TestCancelDuringFirstAgentWatchReleasesEverything checks that a stream ending
// while its agent's first watch is being created leaves neither an agent nor a
// watch behind.
func TestCancelDuringFirstAgentWatchReleasesEverything(t *testing.T) {
	gate := newWatchGate()
	dir := shortTempDir(t)
	socketPath := filepath.Join(dir, "s.sock")
	tb := newTestBroker(t, func(d *Deps) {
		d.Sessions = &resolvedDirSessions{SessionRouter: d.Sessions, gate: gate, dir: dir}
	})
	rs := startRegistrationAt(t, tb.client, "", socketPath)

	waitForReg(t, "the connection to wait", func() bool {
		return tb.lockedWaitingCount() == 1
	})
	waitForReg(t, "the first watch", func() bool {
		return tb.lockedWatchCount() == 1
	})

	fakesession.New(t, fakesession.Options{ConfigDir: dir, SocketPath: socketPath, Name: "abandoned"})
	gate.waitSecond(t, eventTimeout)

	rs.cancel()
	waitForReg(t, "the stream to release its backend", func() bool {
		return tb.sessions.ReleaseCount() == 1
	})
	close(gate.release)

	waitForReg(t, "the aborted registration to release its agent and watch", func() bool {
		return tb.lockedAgentCount() == 0 && tb.lockedWatchCount() == 0
	})
}

// TestStatusChangeDuringAgentWatchCreationIsNotLost checks that a record change
// that arrives while the agent's watch is being created is applied once the
// agent attaches.
func TestStatusChangeDuringAgentWatchCreationIsNotLost(t *testing.T) {
	gate := newWatchGate()
	fake := fakesession.New(t, fakesession.Options{Name: "windowed"})
	tb := newTestBroker(t, func(d *Deps) {
		d.Sessions = &resolvedDirSessions{SessionRouter: d.Sessions, gate: gate, dir: fake.ConfigDir()}
	})

	rs := startRegistrationAt(t, tb.client, "", fake.SocketPath())
	gate.waitSecond(t, eventTimeout)

	handled := tb.b.events.handledCount()
	fake.SetStatus("busy")
	waitForReg(t, "the status change to reach the event loop", func() bool {
		return tb.b.events.handledCount() > handled
	})

	close(gate.release)
	complete := rs.complete(eventTimeout)
	id := AgentID(complete.GetAgentId())

	waitForReg(t, "the agent to carry the new status", func() bool {
		snapshot, ok := tb.b.lookupAgent(id)
		return ok && snapshot.Session.Status == "busy"
	})
}

// TestRegisterWaitHasLowerBound checks that a report appears only after the full
// registrationWait has elapsed.
func TestRegisterWaitHasLowerBound(t *testing.T) {
	dir := shortTempDir(t)
	socketPath := filepath.Join(dir, "s.sock")
	tb := newTestBroker(t)
	rs := startRegistrationAt(t, tb.client, dir, socketPath)

	waitForReg(t, "the connection to wait", func() bool {
		return tb.lockedWaitingCount() == 1
	})
	waitForReg(t, "the first attempt to store its reason", func() bool {
		return tb.lockedWaitAttempted()
	})

	// The literals pin the documented ten seconds; deriving them from
	// registrationWait would move the probe with a changed constant.
	tb.clock.Advance(10*time.Second - time.Nanosecond)
	if tb.lockedWaitingReported() {
		t.Fatal("a not_registered was sent before the wait elapsed")
	}

	tb.clock.Advance(time.Nanosecond)
	if reason := rs.problem(eventTimeout); reason != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SESSION_RECORD_NOT_FOUND {
		t.Fatalf("reason = %v, want session_record_not_found", reason)
	}
}
