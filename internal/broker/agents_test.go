package broker

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/peer"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakesession"
)

// setAgentState replaces an agent's name, status and previous name without
// pushing events. Later work items use it to build a table state directly.
func setAgentState(t *testing.T, b *Broker, id AgentID, name, status, prevName string) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	a, ok := b.agents[id]
	if !ok {
		t.Fatalf("setAgentState: agent %s is not in the table", id)
	}
	a.name = name
	a.status = status
	a.prevName = prevName
}

// setRefreshOne replaces the per-agent reread hook. Later work items use it to
// observe refreshAgents without the session-tracking implementation.
func setRefreshOne(b *Broker, fn func(a *agent)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refreshOne = fn
}

func testAgentID(prefix string) AgentID {
	if len(prefix) > 64 {
		prefix = prefix[:64]
	}
	return AgentID(prefix + strings.Repeat("0", 64-len(prefix)))
}

func queueLen(q *eventQueue) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

func addTestAgent(b *Broker, id AgentID) *regConn {
	c := &regConn{out: newEventQueue()}
	b.agents[id] = &agent{
		id:       id,
		name:     "worker",
		status:   "idle",
		public:   make(map[string]string),
		conns:    map[*regConn]struct{}{c: {}},
		watching: false,
	}
	c.agent = b.agents[id]
	return c
}

func TestRecomputeRefsPushesChanges(t *testing.T) {
	tb := newTestBroker(t)
	b := tb.b

	first := testAgentID("aaaaaa0")
	second := testAgentID("aaaaaa1")
	third := testAgentID("bbbbbb0")

	b.mu.Lock()
	firstConn := addTestAgent(b, first)
	secondConn := addTestAgent(b, second)
	thirdConn := addTestAgent(b, third)
	b.recomputeRefsLocked()
	lengths := []int{len(b.agents[first].ref), len(b.agents[second].ref), len(b.agents[third].ref)}
	b.mu.Unlock()

	if lengths[0] != 7 || lengths[1] != 7 || lengths[2] != 6 {
		t.Fatalf("ref lengths after first recompute = %v, want [7 7 6]", lengths)
	}

	// The first recompute introduced all three agents; it must not have pushed
	// agent_changed to the streams.
	if n := queueLen(firstConn.out); n != 0 {
		t.Fatalf("first recompute queued %d events for the first agent, want 0", n)
	}
	tb.topics.ResetCalls()

	b.mu.Lock()
	delete(b.agents, second)
	b.recomputeRefsLocked()
	firstLen := len(b.agents[first].ref)
	thirdLen := len(b.agents[third].ref)
	firstQueued := queueLen(firstConn.out)
	secondQueued := queueLen(secondConn.out)
	thirdQueued := queueLen(thirdConn.out)
	b.mu.Unlock()

	if firstLen != 6 || thirdLen != 6 {
		t.Fatalf("ref lengths after removing the second agent = %d, %d, want 6, 6", firstLen, thirdLen)
	}
	if firstQueued != 1 || secondQueued != 0 || thirdQueued != 0 {
		t.Fatalf("agent_changed queued to [first second third] = [%d %d %d], want [1 0 0]",
			firstQueued, secondQueued, thirdQueued)
	}
	calls := tb.topics.AgentChangedCalls()
	if len(calls) != 1 || calls[0].Agent != first {
		t.Fatalf("AgentChanged calls = %v, want exactly one for %s", calls, first)
	}
}

// nextAgentChanged returns the next registration event, which must be an
// agent_changed. It is local to this file so the tests here do not depend on
// the session-tracking helpers added later.
func nextAgentChanged(t *testing.T, rs *regStream, d time.Duration) *localpb.AgentChangedEvent {
	t.Helper()
	event := rs.next(d).GetAgentChanged()
	if event == nil {
		t.Fatal("registration event is not an agent_changed event")
	}
	return event
}

// TestSameNameRegistrationPushesSiblingAddress covers the display address of a
// sibling: registering a second agent with the same name adds a disambiguation
// ref to the first agent's address, which the first registration stream must
// hear about even though the first agent's ref does not change.
func TestSameNameRegistrationPushesSiblingAddress(t *testing.T) {
	tb := newTestBroker(t)
	first := fakesession.New(t, fakesession.Options{Name: "w"})
	firstStream := startRegistration(t, tb, first)
	firstStream.complete(eventTimeout)

	second := fakesession.New(t, fakesession.Options{Name: "w"})
	secondStream := startRegistration(t, tb, second)
	secondStream.complete(eventTimeout)

	event := nextAgentChanged(t, firstStream, eventTimeout)
	snap, ok := tb.b.lookupAgent(mustAgentID(t, first.SocketPath()))
	if !ok {
		t.Fatal("the first agent left the table")
	}
	if !strings.Contains(snap.Address, " [") {
		t.Fatalf("first address = %q, want a disambiguation ref", snap.Address)
	}
	if event.GetAgentAddress() != snap.Address {
		t.Fatalf("agent_changed address = %q, want %q", event.GetAgentAddress(), snap.Address)
	}
}

// TestSameNameUnregistrationPushesSiblingAddress covers the other direction:
// once the second agent is gone, the first agent's address drops its ref, and
// the first stream must hear about that too.
func TestSameNameUnregistrationPushesSiblingAddress(t *testing.T) {
	tb := newTestBroker(t)
	first := fakesession.New(t, fakesession.Options{Name: "w"})
	firstStream := startRegistration(t, tb, first)
	firstStream.complete(eventTimeout)

	second := fakesession.New(t, fakesession.Options{Name: "w"})
	secondStream := startRegistration(t, tb, second)
	secondStream.complete(eventTimeout)

	// Consume the change the second registration caused.
	nextAgentChanged(t, firstStream, eventTimeout)

	secondStream.cancel()
	event := nextAgentChanged(t, firstStream, eventTimeout)
	if event.GetAgentAddress() != "w@hostA" {
		t.Fatalf("agent_changed address after the sibling left = %q, want w@hostA", event.GetAgentAddress())
	}
}

func TestLookupAndAllAgents(t *testing.T) {
	tb := newTestBroker(t)
	one := fakesession.New(t, fakesession.Options{Name: "one"})
	two := fakesession.New(t, fakesession.Options{Name: "two"})

	streamOne := startRegistration(t, tb, one)
	streamOne.complete(eventTimeout)
	streamTwo := startRegistration(t, tb, two)
	streamTwo.complete(eventTimeout)

	idOne := mustAgentID(t, one.SocketPath())
	idTwo := mustAgentID(t, two.SocketPath())

	snapshot, ok := tb.b.lookupAgent(idOne)
	if !ok {
		t.Fatalf("lookupAgent(%s) = not found", idOne)
	}
	if snapshot.Name != "one" || snapshot.Address != "one@hostA" {
		t.Fatalf("snapshot = %+v, want name one and address one@hostA", snapshot)
	}
	if snapshot.Session.SocketPath != one.SocketPath() {
		t.Fatalf("snapshot session socket = %q, want %q", snapshot.Session.SocketPath, one.SocketPath())
	}
	if snapshot.Inbox == nil {
		t.Fatal("snapshot Inbox is nil")
	}

	all := tb.b.allAgents()
	if len(all) != 2 {
		t.Fatalf("allAgents returned %d agents, want 2", len(all))
	}
	if all[0].ID > all[1].ID {
		t.Fatalf("allAgents is not sorted: %s then %s", all[0].ID, all[1].ID)
	}

	streamOne.cancel()
	waitForReg(t, "first agent to leave the table", func() bool {
		_, ok := tb.b.lookupAgent(idOne)
		return !ok
	})
	if _, ok := tb.b.lookupAgent(idTwo); !ok {
		t.Fatal("the second agent disappeared with the first")
	}
}

func TestOnUnregisterCalledOutsideLock(t *testing.T) {
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{Name: "leaver"})
	stream := startRegistration(t, tb, fake)
	stream.complete(eventTimeout)
	id := mustAgentID(t, fake.SocketPath())

	called := make(chan AgentID, 1)
	tb.b.onUnregister(func(got AgentID) {
		// allAgents takes b.mu; the hook must run outside it.
		_ = tb.b.allAgents()
		called <- got
	})

	stream.cancel()
	select {
	case got := <-called:
		if got != id {
			t.Fatalf("onUnregister got %s, want %s", got, id)
		}
	case <-time.After(eventTimeout):
		t.Fatal("onUnregister was not called in time")
	}
}

func regCallerContext(uid uint32) context.Context {
	return peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: &callerAuthInfo{caller: Caller{UID: uid}},
	})
}

func TestAgentForChecksAccount(t *testing.T) {
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{Name: "mine"})
	stream := startRegistration(t, tb, fake)
	stream.complete(eventTimeout)
	id := mustAgentID(t, fake.SocketPath())

	self := regCallerContext(uint32(os.Geteuid()))
	a, err := tb.b.agentFor(self, string(id))
	if err != nil {
		t.Fatalf("agentFor for the owning account: %v", err)
	}
	if a.id != id {
		t.Fatalf("agentFor returned %s, want %s", a.id, id)
	}

	other := regCallerContext(uint32(os.Geteuid()) + 1)
	if _, err := tb.b.agentFor(other, string(id)); errorCode(err) != localpb.ErrorCode_ERROR_CODE_NOT_REGISTERED {
		t.Fatalf("agentFor for another account = %v, want not_registered", err)
	}
	if _, err := tb.b.agentFor(self, string(testAgentID("dddddd0"))); errorCode(err) != localpb.ErrorCode_ERROR_CODE_NOT_REGISTERED {
		t.Fatalf("agentFor for an unknown id = %v, want not_registered", err)
	}
}

func TestRefreshAgentsCallsHookInOrder(t *testing.T) {
	tb := newTestBroker(t)
	var ids []AgentID
	for _, name := range []string{"a", "b", "c"} {
		fake := fakesession.New(t, fakesession.Options{Name: name})
		stream := startRegistration(t, tb, fake)
		stream.complete(eventTimeout)
		ids = append(ids, mustAgentID(t, fake.SocketPath()))
	}

	var mu sync.Mutex
	var order []AgentID
	setRefreshOne(tb.b, func(a *agent) {
		// Taking Broker.mu here must not deadlock: the hook runs outside it.
		_ = tb.b.allAgents()
		mu.Lock()
		order = append(order, a.id)
		mu.Unlock()
	})

	unknown := testAgentID("eeeeee0")
	tb.b.refreshAgents([]AgentID{ids[2], unknown, ids[0]})

	mu.Lock()
	got := append([]AgentID(nil), order...)
	mu.Unlock()
	if len(got) != 2 || got[0] != ids[2] || got[1] != ids[0] {
		t.Fatalf("refresh order = %v, want [%s %s]", got, ids[2], ids[0])
	}
}

func TestStatusValueMapping(t *testing.T) {
	cases := []struct {
		status string
		topic  string
		proto  localpb.AgentStatus
	}{
		{"idle", "idle", localpb.AgentStatus_AGENT_STATUS_IDLE},
		{"busy", "busy", localpb.AgentStatus_AGENT_STATUS_BUSY},
		{"waiting", "waiting", localpb.AgentStatus_AGENT_STATUS_WAITING},
		{"shell", "shell", localpb.AgentStatus_AGENT_STATUS_SHELL},
		{"weird", "", localpb.AgentStatus_AGENT_STATUS_UNSPECIFIED},
		{"", "", localpb.AgentStatus_AGENT_STATUS_UNSPECIFIED},
	}
	for _, tc := range cases {
		if got := topicStatus(tc.status); got != tc.topic {
			t.Errorf("topicStatus(%q) = %q, want %q", tc.status, got, tc.topic)
		}
		if got := statusToProto(tc.status); got != tc.proto {
			t.Errorf("statusToProto(%q) = %v, want %v", tc.status, got, tc.proto)
		}
	}
}
