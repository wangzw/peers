package broker

import (
	"os"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/inbox"
	"github.com/wangzw/peers/internal/testkit/fakesession"
)

// TestAttachExistingAgentRequiresSameAccount checks that a connection whose
// socket path names another account's agent reports socket_not_owned instead of
// attaching.
func TestAttachExistingAgentRequiresSameAccount(t *testing.T) {
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{Name: "owned-elsewhere"})

	id, socketPath, err := AgentIDFor(fake.SocketPath())
	if err != nil {
		t.Fatalf("AgentIDFor(%q): %v", fake.SocketPath(), err)
	}
	otherUID := uint32(os.Geteuid()) + 1

	tb.b.mu.Lock()
	tb.b.agents[id] = &agent{
		id:         id,
		socketPath: socketPath,
		caller:     Caller{UID: otherUID},
		session: inbox.Session{
			PID:        fake.PID(),
			Name:       "owned-elsewhere",
			SocketPath: socketPath,
			ConfigDir:  fake.ConfigDir(),
		},
		name:  "owned-elsewhere",
		conns: make(map[*regConn]struct{}),
	}
	tb.b.mu.Unlock()

	rs := startRegistration(t, tb, fake)
	deadline := time.Now().Add(eventTimeout)
	for tb.lockedWaitingCount() == 0 {
		select {
		case resp := <-rs.events:
			t.Fatalf("registration event = %v, want not_registered socket_not_owned", resp)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("the registration neither completed nor started waiting")
		}
		time.Sleep(5 * time.Millisecond)
	}
	tb.clock.Advance(registrationWait)

	if reason := rs.problem(eventTimeout); reason != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SOCKET_NOT_OWNED {
		t.Fatalf("reason = %v, want socket_not_owned", reason)
	}
	if conns := tb.lockedConnCount(id); conns != 0 {
		t.Errorf("the other account's agent has %d connection(s), want 0", conns)
	}
}
