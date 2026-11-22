package broker

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/inbox"
	"github.com/wangzw/peers/internal/testkit/fakesession"
)

// The Broker warns about the session registry's frame protocol version, which
// inbox.Session carries. Keep the dependency visible at compile time so the
// tests do not silently stop covering the field if inbox drops it.
var _ int = inbox.Session{}.PeerProtocol

// sessionProtocolEvent is the user-visible event name the tests pin.
const sessionProtocolEvent = "session_protocol_unrecognized"

// sessionProtocolLogs returns every session_protocol_unrecognized record.
func sessionProtocolLogs(t *testing.T, tb *testBroker) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, rec := range tb.logLines(t) {
		if rec["event"] == sessionProtocolEvent {
			out = append(out, rec)
		}
	}
	return out
}

// peerProtocolOf reads the peer_protocol field of one log record.
func peerProtocolOf(t *testing.T, rec map[string]any) int {
	t.Helper()
	v, ok := rec["peer_protocol"].(float64)
	if !ok {
		t.Fatalf("peer_protocol = %v (%T), want a number", rec["peer_protocol"], rec["peer_protocol"])
	}
	return int(v)
}

// writeSessionPeerProtocol rewrites the peerProtocol field of a session record
// in place, as a session speaking another frame protocol would.
func writeSessionPeerProtocol(t *testing.T, recordPath string, protocol any) {
	t.Helper()
	data, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatalf("read %s: %v", recordPath, err)
	}
	var record map[string]any
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatalf("decode %s: %v", recordPath, err)
	}
	record["peerProtocol"] = protocol
	data, err = json.Marshal(record)
	if err != nil {
		t.Fatalf("encode %s: %v", recordPath, err)
	}
	if err := os.WriteFile(recordPath, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", recordPath, err)
	}
}

// TestSessionProtocolWarnOnce pins that an unknown registry protocol version
// is logged once per agent and value, not on every reread.
func TestSessionProtocolWarnOnce(t *testing.T) {
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{
		Name:  "proto",
		Extra: map[string]any{"peerProtocol": 2},
	})
	rs := startRegistration(t, tb, fake)
	rs.complete(eventTimeout)

	logs := sessionProtocolLogs(t, tb)
	if len(logs) != 1 {
		t.Fatalf("%s records = %d, want 1: %v", sessionProtocolEvent, len(logs), tb.logLines(t))
	}
	if got := peerProtocolOf(t, logs[0]); got != 2 {
		t.Fatalf("peer_protocol = %d, want 2", got)
	}
	id := mustAgentID(t, fake.SocketPath())
	if got, want := logs[0]["agent"], shortID(id); got != want {
		t.Fatalf("agent = %v, want the short Agent ID %q", got, want)
	}

	// A reread that still sees version 2 must not warn again.
	drainNotify(tb.topics.notify)
	fake.Rename("proto-renamed")
	select {
	case <-tb.topics.notify:
	case <-time.After(eventTimeout):
		t.Fatal("the rename did not reach the topic table")
	}

	snap, ok := tb.b.lookupAgent(id)
	if !ok {
		t.Fatal("the agent left the table")
	}
	if snap.Session.PeerProtocol != 2 {
		t.Fatalf("reread peerProtocol = %d, want 2", snap.Session.PeerProtocol)
	}
	if logs := sessionProtocolLogs(t, tb); len(logs) != 1 {
		t.Fatalf("%s records after a reread = %d, want 1: %v", sessionProtocolEvent, len(logs), tb.logLines(t))
	}
}

// TestSessionProtocolWarnOnChange pins that a new unknown value is logged
// again, not only the first one seen at registration.
func TestSessionProtocolWarnOnChange(t *testing.T) {
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{
		Name:  "proto",
		Extra: map[string]any{"peerProtocol": 2},
	})
	rs := startRegistration(t, tb, fake)
	rs.complete(eventTimeout)

	if logs := sessionProtocolLogs(t, tb); len(logs) != 1 {
		t.Fatalf("%s records = %d, want 1: %v", sessionProtocolEvent, len(logs), tb.logLines(t))
	}

	writeSessionPeerProtocol(t, fake.RecordPath(), 3)
	waitForReg(t, "a second session_protocol_unrecognized record", func() bool {
		return len(sessionProtocolLogs(t, tb)) >= 2
	})

	logs := sessionProtocolLogs(t, tb)
	if len(logs) != 2 {
		t.Fatalf("%s records = %d, want 2: %v", sessionProtocolEvent, len(logs), tb.logLines(t))
	}
	if got := peerProtocolOf(t, logs[1]); got != 3 {
		t.Fatalf("second peer_protocol = %d, want 3", got)
	}
}

// TestSessionProtocolNoWarn pins that the supported version and a missing
// version are not reported as unrecognized.
func TestSessionProtocolNoWarn(t *testing.T) {
	for _, tc := range []struct {
		name  string
		extra map[string]any
	}{
		{name: "supported", extra: map[string]any{"peerProtocol": 1}},
		{name: "missing", extra: map[string]any{"peerProtocol": nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tb := newTestBroker(t)
			fake := fakesession.New(t, fakesession.Options{Name: "quiet", Extra: tc.extra})
			rs := startRegistration(t, tb, fake)
			rs.complete(eventTimeout)

			if logs := sessionProtocolLogs(t, tb); len(logs) != 0 {
				t.Fatalf("%s records = %d, want none: %v", sessionProtocolEvent, len(logs), tb.logLines(t))
			}
		})
	}
}

// TestSessionProtocolDoesNotBlockRegistration pins that the warning is not a
// registration error.
func TestSessionProtocolDoesNotBlockRegistration(t *testing.T) {
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{
		Name:  "warned",
		Extra: map[string]any{"peerProtocol": 2},
	})
	rs := startRegistration(t, tb, fake)
	complete := rs.complete(eventTimeout)

	id := mustAgentID(t, fake.SocketPath())
	if complete.GetAgentId() != string(id) {
		t.Fatalf("agent_id = %q, want %q", complete.GetAgentId(), id)
	}
	if _, ok := tb.b.lookupAgent(id); !ok {
		t.Fatal("the agent was not registered")
	}
	if logs := sessionProtocolLogs(t, tb); len(logs) != 1 {
		t.Fatalf("%s records = %d, want 1: %v", sessionProtocolEvent, len(logs), tb.logLines(t))
	}
}
