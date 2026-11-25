package broker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/inbox"
)

type signallingRereadBackend struct {
	userBackend
	refreshed chan struct{}
}

func (b *signallingRereadBackend) Reread(_ context.Context, s inbox.Session) (inbox.Session, error) {
	select {
	case b.refreshed <- struct{}{}:
	default:
	}
	return s, nil
}

type stoppedHelperBackend struct {
	userBackend
	started chan struct{}
	once    sync.Once
}

func (b *stoppedHelperBackend) Reread(ctx context.Context, _ inbox.Session) (inbox.Session, error) {
	b.once.Do(func() { close(b.started) })
	<-ctx.Done()
	return inbox.Session{}, ctx.Err()
}

type countingBlockingBackend struct {
	userBackend
	calls   atomic.Int64
	started chan struct{}
	release chan struct{}
}

func newCountingBlockingBackend() *countingBlockingBackend {
	return &countingBlockingBackend{started: make(chan struct{}), release: make(chan struct{})}
}

func (b *countingBlockingBackend) Reread(_ context.Context, s inbox.Session) (inbox.Session, error) {
	if b.calls.Add(1) == 1 {
		close(b.started)
		<-b.release
	}
	return s, nil
}

func (b *countingBlockingBackend) count() int64 { return b.calls.Load() }

func addTrackedAgent(b *Broker, id AgentID, uid uint32, socketPath, configDir string, pid int, backend SessionBackend) {
	a := &agent{
		id:         id,
		socketPath: socketPath,
		caller:     Caller{UID: uid},
		backend:    backend,
		session:    inbox.Session{PID: pid, Name: string(id), SocketPath: socketPath, ConfigDir: configDir},
		name:       string(id),
		status:     "idle",
		public:     make(map[string]string),
		conns:      make(map[*regConn]struct{}),
		watchDir:   watchKeyFor(uid, configDir),
		watching:   true,
	}
	b.agents[id] = a
}

// TestOneAccountEventsDoNotDelayAnother checks that an account whose helper
// stopped does not hold up another account's registry events.
func TestOneAccountEventsDoNotDelayAnother(t *testing.T) {
	const (
		blockedUID = 9001
		otherUID   = 9002
		agentCount = 5
		eventCount = 5
	)
	blocked := &stoppedHelperBackend{started: make(chan struct{})}
	refreshed := make(chan struct{}, 1)
	other := &signallingRereadBackend{refreshed: refreshed}

	tb := newTestBroker(t)
	tb.b.mu.Lock()
	for i := 0; i < agentCount; i++ {
		addTrackedAgent(tb.b, AgentID(fmt.Sprintf("blocked-%d", i)), blockedUID,
			fmt.Sprintf("/tmp/blocked-%d.sock", i), "/dir-blocked", 100+i, blocked)
	}
	addTrackedAgent(tb.b, "other-agent", otherUID, "/tmp/other.sock", "/dir-other", 200, other)
	tb.b.mu.Unlock()

	for i := 0; i < eventCount; i++ {
		tb.b.events.push(blockedUID, "/dir-blocked", 0)
	}
	select {
	case <-blocked.started:
	case <-time.After(eventTimeout):
		t.Fatal("the blocked account's first reread did not start")
	}

	tb.b.events.push(otherUID, "/dir-other", 0)
	select {
	case <-refreshed:
	case <-time.After(eventTimeout):
		t.Fatal("the other account's reread waited behind the blocked account's events")
	}
}

// TestRepeatedEventsForOneDirectoryCoalesce checks that events queued during one
// reread are merged into at most one more reread.
func TestRepeatedEventsForOneDirectoryCoalesce(t *testing.T) {
	const uid = 9003
	backend := newCountingBlockingBackend()
	tb := newTestBroker(t)
	tb.b.mu.Lock()
	addTrackedAgent(tb.b, "repeated-agent", uid, "/tmp/repeated.sock", "/dir-repeated", 7, backend)
	tb.b.mu.Unlock()

	tb.b.events.push(uid, "/dir-repeated", 7)
	select {
	case <-backend.started:
	case <-time.After(eventTimeout):
		t.Fatal("the first reread did not start")
	}

	for i := 0; i < 100; i++ {
		tb.b.events.push(uid, "/dir-repeated", 7)
	}
	close(backend.release)

	waitForReg(t, "the queued reread to run", func() bool {
		return tb.b.events.handledCount() >= 2
	})
	waitForReg(t, "the account's event lane to exit", func() bool {
		return !tb.b.events.laneActive(uid)
	})
	if got := backend.count(); got > 2 {
		t.Fatalf("Reread ran %d times for one directory with a pending event, want at most 2", got)
	}
}
