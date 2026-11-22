package broker

import (
	"testing"

	"github.com/wangzw/peers/internal/testkit/fakesession"
)

// TestAgentIDStableAcrossRestartReq037 checks REQ-broker-config-037: when the
// Broker process exits and a new one starts, a session that registers again on
// the same inbox socket path gets the same Agent ID (design-mcpserver.md,
// "重新登记"). The ID must come from the socket path, so an ID taken from a
// random value or an in-process counter fails this test.
func TestAgentIDStableAcrossRestartReq037(t *testing.T) {
	first := newTestBroker(t)
	session := fakesession.New(t, fakesession.Options{Name: "stable"})

	firstReg := startRegistration(t, first, session)
	id1 := firstReg.complete(eventTimeout).GetAgentId()
	if id1 == "" {
		t.Fatal("the first Broker returned an empty agent id")
	}

	first.b.Stop()

	second := newTestBroker(t)
	secondReg := startRegistration(t, second, session)
	id2 := secondReg.complete(eventTimeout).GetAgentId()

	if id2 != id1 {
		t.Fatalf("agent id after the Broker restart = %q, want %q", id2, id1)
	}
	if want := string(mustAgentID(t, session.SocketPath())); id1 != want {
		t.Fatalf("agent id = %q, want the ID derived from the socket path, %q", id1, want)
	}

	other := fakesession.New(t, fakesession.Options{Name: "other"})
	otherReg := startRegistration(t, second, other)
	otherID := otherReg.complete(eventTimeout).GetAgentId()
	if otherID == id1 {
		t.Fatalf("a different session got the same agent id %q", otherID)
	}
}
