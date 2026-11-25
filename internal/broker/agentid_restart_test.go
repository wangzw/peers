package broker

import (
	"testing"

	"github.com/wangzw/peers/internal/testkit/fakesession"
)

// TestAgentIDStableAcrossRestartReq037 checks that a session registering on the
// same inbox socket path gets the same agent ID after the Broker restarts, and
// that the ID is derived from the socket path.
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
