package broker

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/testkit/fakesession"
)

// localAliasNextChanged returns the next agent_changed event of rs, failing the
// test on any other event or on timeout.
func localAliasNextChanged(t *testing.T, rs *regStream, d time.Duration) *localpb.AgentChangedEvent {
	t.Helper()
	resp := rs.next(d)
	changed := resp.GetAgentChanged()
	if changed == nil {
		t.Fatalf("registration event = %v, want agent_changed", resp)
	}
	return changed
}

func setLocalAlias(t *testing.T, tb *testBroker, alias string) {
	t.Helper()
	tb.cfg.setValue("broker.alias", config.Value{Kind: config.KindString, String: alias})
}

// TestLocalAliasChangedPushesNewAddress covers REQ-agent-messaging-004: the
// address contains the local alias, so changing the alias must reach every
// registration stream of every registered agent. A topic roster entry contains
// no alias, so the topic table must not be called.
func TestLocalAliasChangedPushesNewAddress(t *testing.T) {
	tb := newTestBroker(t)
	sessionA := fakesession.New(t, fakesession.Options{Name: "a"})
	sessionB := fakesession.New(t, fakesession.Options{Name: "b"})

	a1 := startRegistration(t, tb, sessionA)
	a1.complete(eventTimeout)
	a2 := startRegistrationAt(t, tb.newClient(t), sessionA.ConfigDir(), sessionA.SocketPath())
	a2.complete(eventTimeout)
	b1 := startRegistration(t, tb, sessionB)
	b1.complete(eventTimeout)

	topicsBefore := len(tb.topics.AgentChangedCalls())

	setLocalAlias(t, tb, "hostZ")
	tb.b.localAliasChanged()

	agentA := string(mustAgentID(t, sessionA.SocketPath()))
	for _, rs := range []*regStream{a1, a2} {
		changed := localAliasNextChanged(t, rs, eventTimeout)
		if changed.GetAgentId() != agentA {
			t.Fatalf("agent_id = %q, want %q", changed.GetAgentId(), agentA)
		}
		if changed.GetAgentAddress() != "a@hostZ" {
			t.Fatalf("agent_address = %q, want %q", changed.GetAgentAddress(), "a@hostZ")
		}
	}
	changed := localAliasNextChanged(t, b1, eventTimeout)
	if changed.GetAgentAddress() != "b@hostZ" {
		t.Fatalf("agent_address = %q, want %q", changed.GetAgentAddress(), "b@hostZ")
	}

	// One event per stream, not one per agent broadcast several times. Change
	// the alias again and read the next event of each stream: a duplicate of
	// the hostZ change would arrive before this barrier event.
	setLocalAlias(t, tb, "hostY")
	tb.b.localAliasChanged()
	for _, rs := range []*regStream{a1, a2} {
		if got := localAliasNextChanged(t, rs, eventTimeout).GetAgentAddress(); got != "a@hostY" {
			t.Fatalf("agent_address after the barrier change = %q, want %q", got, "a@hostY")
		}
	}
	if got := localAliasNextChanged(t, b1, eventTimeout).GetAgentAddress(); got != "b@hostY" {
		t.Fatalf("agent_address after the barrier change = %q, want %q", got, "b@hostY")
	}

	if got := len(tb.topics.AgentChangedCalls()); got != topicsBefore {
		t.Fatalf("AgentChanged calls went from %d to %d, want no new calls", topicsBefore, got)
	}
}

// TestLocalAliasChangedSkipsWaiting checks that a connection which has not
// completed registration receives no agent_changed event.
func TestLocalAliasChangedSkipsWaiting(t *testing.T) {
	tb := newTestBroker(t)
	dir := shortTempDir(t)
	socketPath := filepath.Join(dir, "s.sock")
	rs := startRegistrationAt(t, tb.client, dir, socketPath)

	setLocalAlias(t, tb, "hostZ")
	tb.b.localAliasChanged()

	// Let the waiting registration complete. If the alias change had reached
	// this connection, its agent_changed would arrive before the completion.
	fakesession.New(t, fakesession.Options{ConfigDir: dir, SocketPath: socketPath, Name: "late"})

	complete := rs.complete(eventTimeout)
	if got := complete.GetAgentAddress(); got != "late@hostZ" {
		t.Fatalf("agent_address = %q, want late@hostZ", got)
	}
	if got := tb.lockedAgentCount(); got != 1 {
		t.Fatalf("registered agents = %d, want 1", got)
	}
}

// TestLocalAliasChangedUpdatesLastAddress checks that the alias push records the
// address it announced. A stale record makes a later recompute of the agent
// table see a change that was already pushed and announce it a second time.
func TestLocalAliasChangedUpdatesLastAddress(t *testing.T) {
	tb := newTestBroker(t)
	first := fakesession.New(t, fakesession.Options{Name: "a"})
	rs := startRegistration(t, tb, first)
	rs.complete(eventTimeout)

	setLocalAlias(t, tb, "hostZ")
	tb.b.localAliasChanged()
	if changed := localAliasNextChanged(t, rs, eventTimeout); changed.GetAgentAddress() != "a@hostZ" {
		t.Fatalf("agent_address = %q, want a@hostZ", changed.GetAgentAddress())
	}

	// Registering a second agent recomputes every ref and re-examines every
	// address. The first agent's address has not changed since the push above,
	// so the registration must not push again. A second alias change is certain
	// to push, and the stream delivers in order, so receiving that event first
	// proves the registration stayed quiet.
	second := fakesession.New(t, fakesession.Options{Name: "b"})
	startRegistration(t, tb, second).complete(eventTimeout)

	setLocalAlias(t, tb, "hostY")
	tb.b.localAliasChanged()
	if changed := localAliasNextChanged(t, rs, eventTimeout); changed.GetAgentAddress() != "a@hostY" {
		t.Fatalf("agent_address = %q, want a@hostY", changed.GetAgentAddress())
	}
}

// TestLocalAliasChangedKeepsPrevName checks that the pushed event carries the
// agent's current previous name and status alongside the new address.
func TestLocalAliasChangedKeepsPrevName(t *testing.T) {
	tb := newTestBroker(t)
	session := fakesession.New(t, fakesession.Options{Name: "a"})
	rs := startRegistration(t, tb, session)
	rs.complete(eventTimeout)

	id := mustAgentID(t, session.SocketPath())
	setAgentState(t, tb.b, id, "a", "busy", "old")

	setLocalAlias(t, tb, "hostZ")
	tb.b.localAliasChanged()

	changed := localAliasNextChanged(t, rs, eventTimeout)
	if changed.GetAgentAddress() != "a@hostZ" {
		t.Fatalf("agent_address = %q, want %q", changed.GetAgentAddress(), "a@hostZ")
	}
	if changed.GetPreviousName() != "old" {
		t.Fatalf("previous_name = %q, want %q", changed.GetPreviousName(), "old")
	}
	if changed.GetStatus() != localpb.AgentStatus_AGENT_STATUS_BUSY {
		t.Fatalf("status = %v, want BUSY", changed.GetStatus())
	}
}
