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

// watchGate counts the Watch calls of the backends its router hands out and
// blocks the second call until release is closed. The second call is the agent's
// own watch, created while the first registration attaches the agent.
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

// resolvedDirSessions hands out backends that model the default configuration
// directory: an empty configDir names dir, but the connection's watch key stays
// empty while the registry lookup reports the resolved directory. The two keys
// differ even though they name the same directory, so the agent's watch is a
// second Watch call rather than a reference transferred from the connection.
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

// resolvedDirBackend resolves the empty configuration directory to dir, as the
// inbox package does for the account's default directory.
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

// gatedWatchSessions hands out backends whose Watch calls pass through gate.
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

// lockedWaitAttempted reports whether every waiting connection has run its
// first attempt, so its reason is stored and the wait timer would report it.
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

// lockedWaitingReported reports whether any waiting connection has sent its
// not_registered event. The flag is set under Broker.mu by the wait timer, so
// the fake clock's synchronous advance makes this check independent of the
// sender goroutine.
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

// TestSameKeyRegistrationKeepsItsWatch checks that a connection already
// watching the agent's configuration directory passes that reference to the
// agent instead of releasing it and subscribing again. A second Watch call
// would release Broker.mu and open the window in which a registry event is not
// applied.
func TestSameKeyRegistrationKeepsItsWatch(t *testing.T) {
	gate := newWatchGate()
	// Let a second Watch through rather than block it: this test only counts.
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

// TestStatusDuringFirstAgentWatchDoesNotPanic covers the window between the
// agent entering the table and its disambiguation ref being assigned: while the
// agent's first watch is being created, Status must see a table it can render.
// The registration names no configuration directory, so the agent's watch is a
// second Watch call and creating it releases Broker.mu.
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

// TestCancelDuringFirstAgentWatchReleasesEverything covers the same window from
// the other side: a registration stream that ends while its agent's first watch
// is being created must leave neither an agent nor a watch behind.
func TestCancelDuringFirstAgentWatchReleasesEverything(t *testing.T) {
	gate := newWatchGate()
	dir := shortTempDir(t)
	socketPath := filepath.Join(dir, "s.sock")
	tb := newTestBroker(t, func(d *Deps) {
		d.Sessions = &resolvedDirSessions{SessionRouter: d.Sessions, gate: gate, dir: dir}
	})
	rs := startRegistrationAt(t, tb.client, "", socketPath)

	// Let the first attempt fail for lack of a record. The completion then runs
	// on the registry event goroutine, so the stream's own goroutine is free to
	// process the cancellation while the agent's watch is being created.
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

// TestStatusChangeDuringAgentWatchCreationIsNotLost covers the window between
// releasing a waiting connection's watch and the agent's own watch being ready.
// Creating the agent's watch releases Broker.mu, so a record change in that
// window reaches handleRegistryEvent while the agent is not in the table and the
// connection has already stopped watching. The change must still be applied once
// the agent attaches.
func TestStatusChangeDuringAgentWatchCreationIsNotLost(t *testing.T) {
	gate := newWatchGate()
	fake := fakesession.New(t, fakesession.Options{Name: "windowed"})
	tb := newTestBroker(t, func(d *Deps) {
		d.Sessions = &resolvedDirSessions{SessionRouter: d.Sessions, gate: gate, dir: fake.ConfigDir()}
	})

	// Naming no configuration directory leaves the connection's watch key empty
	// while the record reports the resolved directory. The two keys differ, so
	// the agent's watch is created with a second Watch call.
	rs := startRegistrationAt(t, tb.client, "", fake.SocketPath())
	gate.waitSecond(t, eventTimeout)

	// The second Watch is blocked, so Broker.mu is free while the agent is not
	// yet in the table. The change is handled by the event loop and, without a
	// reread after the attachment, is dropped.
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

// TestRegisterWaitHasLowerBound checks both sides of registrationWait: a
// report may not appear before the full wait has elapsed, and appears as soon
// as it has.
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

	// The literals pin the documented ten seconds: deriving the probe from
	// registrationWait would move it along with a changed constant.
	tb.clock.Advance(10*time.Second - time.Nanosecond)
	if tb.lockedWaitingReported() {
		t.Fatal("a not_registered was sent before the wait elapsed")
	}

	tb.clock.Advance(time.Nanosecond)
	if reason := rs.problem(eventTimeout); reason != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SESSION_RECORD_NOT_FOUND {
		t.Fatalf("reason = %v, want session_record_not_found", reason)
	}
}
