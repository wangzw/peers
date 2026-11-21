package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/inbox"
	"github.com/wangzw/peers/internal/testkit/fakesession"
)

// rewriteRecordSocket rewrites the messagingSocketPath field of a session
// record in place, as a session that rebound its inbox would.
func rewriteRecordSocket(t *testing.T, recordPath, socketPath string) {
	t.Helper()
	data, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatalf("read %s: %v", recordPath, err)
	}
	var record map[string]any
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatalf("decode %s: %v", recordPath, err)
	}
	record["messagingSocketPath"] = socketPath
	data, err = json.Marshal(record)
	if err != nil {
		t.Fatalf("encode %s: %v", recordPath, err)
	}
	if err := os.WriteFile(recordPath, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", recordPath, err)
	}
}

// blockingRereadBackend blocks the first Reread until the test releases it, so
// the test can hold the event loop while the record on disk is still partial.
type blockingRereadBackend struct {
	SessionBackend
	started chan struct{}
	release chan struct{}
	done    chan struct{}
	once    sync.Once
}

func (b *blockingRereadBackend) Reread(ctx context.Context, s inbox.Session) (inbox.Session, error) {
	first := false
	b.once.Do(func() {
		first = true
		close(b.started)
		<-b.release
	})
	next, err := b.SessionBackend.Reread(ctx, s)
	if first {
		close(b.done)
	}
	return next, err
}

// rereadBlockingSessions hands out one blockingRereadBackend wrapping the real
// backend.
type rereadBlockingSessions struct {
	SessionRouter
	backend *blockingRereadBackend
}

func (s *rereadBlockingSessions) Acquire(ctx context.Context, uid uint32) (SessionBackend, func(), error) {
	base, release, err := s.SessionRouter.Acquire(ctx, uid)
	if err != nil {
		return nil, nil, err
	}
	s.backend.SessionBackend = base
	return s.backend, release, nil
}

// agentChanged returns the next registration event, which must be an
// agent_changed.
func agentChanged(t *testing.T, rs *regStream, d time.Duration) *localpb.AgentChangedEvent {
	t.Helper()
	event := rs.next(d).GetAgentChanged()
	if event == nil {
		t.Fatalf("registration event is not an agent_changed event")
	}
	return event
}

// REQ-agent-messaging-005: after a rename the registration stream carries the
// new name, and the previous name is kept for the retention window.
func TestRenameUpdatesAddressAndPrevName(t *testing.T) {
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{Name: "api-worker"})
	rs := startRegistration(t, tb, fake)
	rs.complete(3 * time.Second)
	id := mustAgentID(t, fake.SocketPath())
	drainNotify(tb.topics.notify)

	fake.Rename("api-v2")

	event := agentChanged(t, rs, 3*time.Second)
	if event.GetAgentAddress() != "api-v2@hostA" {
		t.Fatalf("agent_address = %q, want api-v2@hostA", event.GetAgentAddress())
	}
	if event.GetPreviousName() != "api-worker" {
		t.Fatalf("previous_name = %q, want api-worker", event.GetPreviousName())
	}
	if event.GetAgentId() != string(id) {
		t.Fatalf("agent_id = %q, want %q", event.GetAgentId(), id)
	}

	call, ok := tb.topics.LastAgentChanged(id)
	if !ok {
		t.Fatal("the topic table did not see the rename")
	}
	if call.Name != "api-v2" || call.PrevName != "api-worker" || call.Status != "" {
		t.Fatalf("AgentChanged = %+v, want name api-v2, prev api-worker, empty status", call)
	}
}

func TestPrevNameExpiresAfterTenMinutes(t *testing.T) {
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{Name: "api-worker"})
	rs := startRegistration(t, tb, fake)
	rs.complete(3 * time.Second)
	id := mustAgentID(t, fake.SocketPath())
	drainNotify(tb.topics.notify)

	fake.Rename("api-v2")
	agentChanged(t, rs, 3*time.Second)

	tb.clock.Advance(10*time.Minute - time.Nanosecond)
	if snap, ok := tb.b.lookupAgent(id); !ok || snap.PrevName != "api-worker" {
		t.Fatalf("previous name before the retention window ended = %q (present %v), want api-worker",
			snap.PrevName, ok)
	}

	tb.clock.Advance(time.Nanosecond)
	event := agentChanged(t, rs, 3*time.Second)
	if event.GetPreviousName() != "" {
		t.Fatalf("previous_name after expiry = %q, want empty", event.GetPreviousName())
	}
	if event.GetAgentAddress() != "api-v2@hostA" {
		t.Fatalf("agent_address after expiry = %q, want api-v2@hostA", event.GetAgentAddress())
	}

	call, ok := tb.topics.LastAgentChanged(id)
	if !ok || call.PrevName != "" {
		t.Fatalf("AgentChanged after expiry = %+v, want an empty previous name", call)
	}
}

func TestSecondRenameRestartsPrevNameTimer(t *testing.T) {
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{Name: "a"})
	rs := startRegistration(t, tb, fake)
	rs.complete(3 * time.Second)
	id := mustAgentID(t, fake.SocketPath())

	fake.Rename("b")
	agentChanged(t, rs, 3*time.Second)
	tb.clock.Advance(5 * time.Minute)

	fake.Rename("c")
	event := agentChanged(t, rs, 3*time.Second)
	if event.GetPreviousName() != "b" {
		t.Fatalf("previous_name = %q, want b", event.GetPreviousName())
	}

	// The timer of the first rename would fire here; the second rename must
	// have stopped it.
	tb.clock.Advance(5 * time.Minute)
	if snap, ok := tb.b.lookupAgent(id); !ok || snap.PrevName != "b" {
		t.Fatalf("previous name ten minutes after the first rename = %q, want b", snap.PrevName)
	}
	rs.expectNoEvent(200 * time.Millisecond)

	tb.clock.Advance(5 * time.Minute)
	if event := agentChanged(t, rs, 3*time.Second); event.GetPreviousName() != "" {
		t.Fatalf("previous_name after the second timer expired = %q, want empty", event.GetPreviousName())
	}
}

// REQ-agent-messaging-033: clearing the conversation leaves the agent ID, the
// disambiguation ref, the address and the topic membership unchanged.
func TestIDAndRefStableAcrossRenameAndClear(t *testing.T) {
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{Name: "worker"})
	rs := startRegistration(t, tb, fake)
	rs.complete(3 * time.Second)
	id := mustAgentID(t, fake.SocketPath())

	if err := tb.topics.Join(id, "t1"); err != nil {
		t.Fatalf("Join: %v", err)
	}
	tb.b.mu.Lock()
	ref := tb.b.agents[id].ref
	tb.b.mu.Unlock()

	fake.Rename("worker2")
	agentChanged(t, rs, 3*time.Second)
	drainNotify(tb.topics.notify)

	fake.SetSessionID("after-clear")
	fake.SetStatus("busy")

	select {
	case <-tb.topics.notify:
	case <-time.After(eventTimeout):
		t.Fatal("the status change did not reach the topic table")
	}
	call, ok := tb.topics.LastAgentChanged(id)
	if !ok || call.Status != "busy" {
		t.Fatalf("AgentChanged after /clear and a status change = %+v, want status busy", call)
	}

	// Clearing the conversation changes neither the address nor the roster, so
	// the registration stream saw only the rename.
	rs.expectNoEvent(300 * time.Millisecond)

	snap, ok := tb.b.lookupAgent(id)
	if !ok {
		t.Fatal("the agent left the table")
	}
	if snap.ID != id {
		t.Fatalf("agent id = %s, want %s", snap.ID, id)
	}
	if snap.Ref != ref {
		t.Fatalf("ref = %q, want %q", snap.Ref, ref)
	}
	if snap.Name != "worker2" || snap.Address != "worker2@hostA" {
		t.Fatalf("snapshot = name %q address %q, want worker2 and worker2@hostA", snap.Name, snap.Address)
	}
	if topics := tb.topics.TopicsOf(id); len(topics) != 1 || topics[0] != "t1" {
		t.Fatalf("topics = %v, want [t1]", topics)
	}
}

func TestStatusChangeReachesTopic(t *testing.T) {
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{Name: "status"})
	rs := startRegistration(t, tb, fake)
	rs.complete(3 * time.Second)
	id := mustAgentID(t, fake.SocketPath())
	drainNotify(tb.topics.notify)

	fake.SetStatus("busy")
	select {
	case <-tb.topics.notify:
	case <-time.After(eventTimeout):
		t.Fatal("the status change did not reach the topic table")
	}
	call, ok := tb.topics.LastAgentChanged(id)
	if !ok || call.Status != "busy" {
		t.Fatalf("AgentChanged = %+v, want status busy", call)
	}
	if snap, _ := tb.b.lookupAgent(id); snap.Session.Status != "busy" {
		t.Fatalf("session status = %q, want busy", snap.Session.Status)
	}
	// A status change alone does not move the address, so no agent_changed.
	rs.expectNoEvent(300 * time.Millisecond)

	fake.SetStatus("weird")
	select {
	case <-tb.topics.notify:
	case <-time.After(eventTimeout):
		t.Fatal("the second status change did not reach the topic table")
	}
	if call, _ := tb.topics.LastAgentChanged(id); call.Status != "" {
		t.Fatalf("AgentChanged status = %q, want empty for an unknown status", call.Status)
	}
}

func TestRecordRemovedUnregisters(t *testing.T) {
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{Name: "gone"})
	rs := startRegistration(t, tb, fake)
	rs.complete(3 * time.Second)
	id := mustAgentID(t, fake.SocketPath())
	if err := tb.topics.Join(id, "t1"); err != nil {
		t.Fatalf("Join: %v", err)
	}

	fake.RemoveRecord()

	if reason := rs.problem(3 * time.Second); reason != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SESSION_RECORD_NOT_FOUND {
		t.Fatalf("reason = %v, want session_record_not_found", reason)
	}
	if calls := tb.topics.LeaveAllCalls(); len(calls) != 1 || calls[0].Agent != id {
		t.Fatalf("LeaveAll calls = %v, want one for %s", calls, id)
	}
	if n := tb.lockedAgentCount(); n != 0 {
		t.Fatalf("agents = %d, want 0", n)
	}

	fake.RestoreRecord()

	complete := rs.complete(3 * time.Second)
	if complete.GetAgentId() != string(id) {
		t.Fatalf("agent_id after recovery = %q, want %q", complete.GetAgentId(), id)
	}
}

func TestRenameToUnusableRevokes(t *testing.T) {
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{Name: "ok"})
	rs := startRegistration(t, tb, fake)
	rs.complete(3 * time.Second)

	fake.Rename("a@b")

	problem := rs.problemEvent(3 * time.Second)
	if problem.GetReason() != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_NAME_UNUSABLE {
		t.Fatalf("reason = %v, want name_unusable", problem.GetReason())
	}
	if got, want := problem.GetDetail(), `the session name contains "@"`; got != want {
		t.Fatalf("detail = %q, want %q", got, want)
	}
	if n := tb.lockedAgentCount(); n != 0 {
		t.Fatalf("agents = %d, want 0", n)
	}

	fake.Rename("ab")

	complete := rs.complete(3 * time.Second)
	if complete.GetAgentAddress() != "ab@hostA" {
		t.Fatalf("agent_address after recovery = %q, want ab@hostA", complete.GetAgentAddress())
	}
}

func TestSocketPathChangeUnregisters(t *testing.T) {
	t.Run("another path", func(t *testing.T) {
		tb := newTestBroker(t)
		fake := fakesession.New(t, fakesession.Options{Name: "mover"})
		rs := startRegistration(t, tb, fake)
		rs.complete(3 * time.Second)
		id := mustAgentID(t, fake.SocketPath())
		if err := tb.topics.Join(id, "t1"); err != nil {
			t.Fatalf("Join: %v", err)
		}

		newPath := filepath.Join(shortTempDir(t), "m.sock")
		rewriteRecordSocket(t, fake.RecordPath(), newPath)

		if reason := rs.problem(3 * time.Second); reason != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SESSION_RECORD_NOT_FOUND {
			t.Fatalf("reason = %v, want session_record_not_found", reason)
		}
		if calls := tb.topics.LeaveAllCalls(); len(calls) != 1 {
			t.Fatalf("LeaveAll calls = %d, want 1", len(calls))
		}
		if n := tb.lockedAgentCount(); n != 0 {
			t.Fatalf("agents = %d, want 0", n)
		}
		found := false
		for _, line := range tb.logLines(t) {
			if line["event"] == EvAgentSocketChanged {
				found = true
			}
		}
		if !found {
			t.Fatal("the log has no agent_socket_changed event")
		}

		// A session that re-registers at the new path gets the new identity.
		rebound := fakesession.New(t, fakesession.Options{
			ConfigDir:  fake.ConfigDir(),
			SocketPath: newPath,
			Name:       "mover",
		})
		fresh := startRegistration(t, tb, rebound)
		complete := fresh.complete(3 * time.Second)
		if want := mustAgentID(t, newPath); complete.GetAgentId() != string(want) {
			t.Fatalf("agent_id at the new path = %q, want %q", complete.GetAgentId(), want)
		}
	})

	t.Run("empty path", func(t *testing.T) {
		tb := newTestBroker(t)
		fake := fakesession.New(t, fakesession.Options{Name: "emptied"})
		rs := startRegistration(t, tb, fake)
		rs.complete(3 * time.Second)

		rewriteRecordSocket(t, fake.RecordPath(), "")

		if reason := rs.problem(3 * time.Second); reason != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SESSION_RECORD_NOT_FOUND {
			t.Fatalf("reason = %v, want session_record_not_found", reason)
		}
		if n := tb.lockedAgentCount(); n != 0 {
			t.Fatalf("agents = %d, want 0", n)
		}
	})
}

func TestPartialRecordKeepsAgent(t *testing.T) {
	blocked := &blockingRereadBackend{
		started: make(chan struct{}),
		release: make(chan struct{}),
		done:    make(chan struct{}),
	}
	tb := newTestBroker(t, func(d *Deps) {
		d.Sessions = &rereadBlockingSessions{SessionRouter: d.Sessions, backend: blocked}
	})
	fake := fakesession.New(t, fakesession.Options{Name: "partial"})
	rs := startRegistration(t, tb, fake)
	rs.complete(3 * time.Second)
	id := mustAgentID(t, fake.SocketPath())
	drainNotify(tb.topics.notify)

	fake.WritePartialRecord(`{"name":`)

	// Hold the reread of the partial record until the test releases it, so the
	// event is handled while the record is still half-written.
	select {
	case <-blocked.started:
	case <-time.After(eventTimeout):
		t.Fatal("the partial record was not reread")
	}
	close(blocked.release)
	select {
	case <-blocked.done:
	case <-time.After(eventTimeout):
		t.Fatal("the partial record reread did not finish")
	}

	if snap, ok := tb.b.lookupAgent(id); !ok || snap.Name != "partial" {
		t.Fatalf("agent after a partial record = %+v (present %v), want name partial", snap.Name, ok)
	}

	fake.SetStatus("busy")
	select {
	case <-tb.topics.notify:
	case <-time.After(eventTimeout):
		t.Fatal("the recovered record did not reach the topic table")
	}
	if call, _ := tb.topics.LastAgentChanged(id); call.Status != "busy" {
		t.Fatalf("AgentChanged status = %q, want busy", call.Status)
	}
	if n := tb.lockedAgentCount(); n != 1 {
		t.Fatalf("agents = %d, want 1", n)
	}
	rs.expectNoEvent(300 * time.Millisecond)
}

func TestRefreshAgentsWithoutEvents(t *testing.T) {
	tb := newTestBroker(t, func(d *Deps) { d.Sessions = &fakeSessions{noWatch: true} })
	fake := fakesession.New(t, fakesession.Options{Name: "x1"})
	rs := startRegistration(t, tb, fake)
	rs.complete(3 * time.Second)
	id := mustAgentID(t, fake.SocketPath())

	fake.Rename("x2")
	tb.b.refreshAgents([]AgentID{id})

	snap, ok := tb.b.lookupAgent(id)
	if !ok {
		t.Fatal("the agent left the table")
	}
	if snap.Name != "x2" || snap.Address != "x2@hostA" {
		t.Fatalf("snapshot = name %q address %q, want x2 and x2@hostA", snap.Name, snap.Address)
	}
}

func TestRereadAgent(t *testing.T) {
	tb := newTestBroker(t, func(d *Deps) { d.Sessions = &fakeSessions{noWatch: true} })
	fake := fakesession.New(t, fakesession.Options{Name: "x1"})
	rs := startRegistration(t, tb, fake)
	rs.complete(3 * time.Second)
	id := mustAgentID(t, fake.SocketPath())

	fake.Rename("x3")
	snap, err := tb.b.rereadAgent(id)
	if err != nil {
		t.Fatalf("rereadAgent: %v", err)
	}
	if snap.Name != "x3" || snap.Address != "x3@hostA" {
		t.Fatalf("snapshot = name %q address %q, want x3 and x3@hostA", snap.Name, snap.Address)
	}

	fake.RemoveRecord()
	if _, err := tb.b.rereadAgent(id); err != errAgentGone {
		t.Fatalf("rereadAgent of a removed record = %v, want errAgentGone", err)
	}
	if calls := tb.topics.LeaveAllCalls(); len(calls) != 1 {
		t.Fatalf("LeaveAll calls = %d, want 1", len(calls))
	}

	if _, err := tb.b.rereadAgent(testAgentID("ffff000")); err != errAgentGone {
		t.Fatalf("rereadAgent of an unknown id = %v, want errAgentGone", err)
	}
}

func TestRenameChangesSiblingRefs(t *testing.T) {
	tb := newTestBroker(t)
	first := fakesession.New(t, fakesession.Options{Name: "w"})
	second := fakesession.New(t, fakesession.Options{Name: "w"})

	firstStream := startRegistration(t, tb, first)
	firstStream.complete(3 * time.Second)
	secondStream := startRegistration(t, tb, second)
	secondStream.complete(3 * time.Second)

	firstSnap, ok := tb.b.lookupAgent(mustAgentID(t, first.SocketPath()))
	if !ok {
		t.Fatal("the first agent is not in the table")
	}
	if firstSnap.Address == "w@hostA" {
		t.Fatalf("first address = %q, want a ref because both sessions are named w", firstSnap.Address)
	}
	secondSnap, ok := tb.b.lookupAgent(mustAgentID(t, second.SocketPath()))
	if !ok {
		t.Fatal("the second agent is not in the table")
	}
	if secondSnap.Address == "w@hostA" {
		t.Fatalf("second address = %q, want a ref because both sessions are named w", secondSnap.Address)
	}

	// The second registration added a ref to the first agent's address, so the
	// first stream already carries the matching agent_changed.
	if event := agentChanged(t, firstStream, 3*time.Second); event.GetAgentAddress() != firstSnap.Address {
		t.Fatalf("agent_changed at the second registration = %q, want %q",
			event.GetAgentAddress(), firstSnap.Address)
	}

	first.Rename("v")

	event := agentChanged(t, firstStream, 3*time.Second)
	if event.GetAgentAddress() != "v@hostA" || event.GetPreviousName() != "w" {
		t.Fatalf("renamed event = address %q previous %q, want v@hostA and w",
			event.GetAgentAddress(), event.GetPreviousName())
	}

	sibling := agentChanged(t, secondStream, 3*time.Second)
	if sibling.GetAgentAddress() != "w@hostA" {
		t.Fatalf("sibling address = %q, want w@hostA after the other session was renamed",
			sibling.GetAgentAddress())
	}
	if sibling.GetPreviousName() != "" {
		t.Fatalf("sibling previous_name = %q, want empty", sibling.GetPreviousName())
	}
}

func TestConcurrentRenamesAndDisconnect(t *testing.T) {
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{Name: "racing"})
	rs := startRegistration(t, tb, fake)
	rs.complete(3 * time.Second)
	id := mustAgentID(t, fake.SocketPath())

	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			fake.Rename(fmt.Sprintf("racing-%d", i))
			if i == 0 {
				close(started)
				// Stay in the loop until the connection is cancelled, so the
				// remaining renames overlap the disconnect.
				<-release
			}
		}
	}()

	<-started
	agentChanged(t, rs, 3*time.Second)
	rs.cancel()
	close(release)
	<-done

	waitForReg(t, "the agent to unregister under concurrent renames", func() bool {
		return tb.lockedAgentCount() == 0
	})
	if calls := tb.topics.LeaveAllCalls(); len(calls) != 1 || calls[0].Agent != id {
		t.Fatalf("LeaveAll calls = %v, want one for %s", calls, id)
	}
}
