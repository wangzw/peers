//go:build e2e

package e2e

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/syscontainer"
)

// e2eSystemSocketPath is the system Broker's local socket.
const e2eSystemSocketPath = "/run/peers/broker.sock"

// TestREQBrokerConfig018_TwoAccountsShareSystemBroker registers one session as
// each account with the same system Broker, then checks that they see each
// other, can exchange messages, and that both appear in the root status.
func TestREQBrokerConfig018_TwoAccountsShareSystemBroker(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Plain)
		return
	}

	// The configuration file must exist for the root status read to be granted;
	// the system Broker serves every account from this one file.
	broker := syscontainer.StartSystemBroker(t, "broker:\n  alias: sys\n")
	if _, err := os.Stat(e2eSystemSocketPath); err != nil {
		t.Fatalf("the system Broker socket is not at %s: %v", e2eSystemSocketPath, err)
	}
	found := false
	for _, proc := range syscontainer.Pids("peers", "service", "run", "--system") {
		if proc.PID == broker.PID {
			found = true
		}
	}
	if !found {
		t.Errorf("process %d does not appear in Pids(peers, broker, --system)", broker.PID)
	}

	a := syscontainer.NewSession(t, "peer", "a1")
	b := syscontainer.NewSession(t, "other", "b1")
	third := syscontainer.NewSession(t, "root", "observer")
	e2eSysWaitRegistered(t, a)
	e2eSysWaitRegistered(t, b)
	e2eSysWaitRegistered(t, third)

	const topic = "e2e/shared"
	e2eSysJoinTopic(t, a, topic)
	e2eSysJoinTopic(t, b, topic)

	aSelf := e2eSysSelfAddress(t, a)
	bSelf := e2eSysSelfAddress(t, b)
	e2eSysWaitVisible(t, a, topic, bSelf)
	e2eSysWaitVisible(t, b, topic, aSelf)

	if text, isError := a.Call(t, "send_message", map[string]any{"to": bSelf, "message": "ping-018"}); isError {
		t.Fatalf("send_message from peer to other reported an error: %s", text)
	}
	frame, ok := b.Fake.WaitFrame(3 * time.Second)
	if !ok {
		t.Fatalf("other's session did not receive the message within 3s")
	}
	if frame.FromName != aSelf {
		t.Errorf("the message is signed %q, want peer's address %q", frame.FromName, aSelf)
	}
	if !strings.Contains(frame.Content, "ping-018") {
		t.Errorf("the delivered content %q does not contain %q", frame.Content, "ping-018")
	}

	if text, isError := b.Call(t, "send_message", map[string]any{"to": frame.FromName, "message": "pong-018"}); isError {
		t.Fatalf("send_message from other to peer reported an error: %s", text)
	}
	reply, ok := a.Fake.WaitFrame(3 * time.Second)
	if !ok {
		t.Fatalf("peer's session did not receive the reply within 3s")
	}
	if reply.FromName != bSelf {
		t.Errorf("the reply is signed %q, want other's address %q", reply.FromName, bSelf)
	}
	if !strings.Contains(reply.Content, "pong-018") {
		t.Errorf("the delivered content %q does not contain %q", reply.Content, "pong-018")
	}

	// Each message names one recipient. The third session shares the system
	// Broker and must receive neither of them, so delivery is addressed, not
	// broadcast to every account.
	thirdSelf := e2eSysSelfAddress(t, third)
	if frames := third.Fake.Frames(); len(frames) != 0 {
		t.Errorf("the third session received %d frames before the check, want none", len(frames))
	}
	harness.Consistently(t, time.Second, "the third session receives neither message", func() (bool, string) {
		if frames := third.Fake.Frames(); len(frames) != 0 {
			return false, fmt.Sprintf("the third session received %d frames", len(frames))
		}
		return true, ""
	})

	status := syscontainer.RunAs(t, "root", nil, "service", "status")
	if status.ExitCode != 0 {
		t.Fatalf("peers service status as root: exit %d\n%s\n%s", status.ExitCode, status.Stdout, status.Stderr)
	}
	if !strings.Contains(status.Stdout, "Agents (3):") {
		t.Fatalf("status does not list all three agents:\n%s", status.Stdout)
	}
	peerLine := e2eSysStatusLine(t, status.Stdout, aSelf)
	if !strings.Contains(peerLine, "peer") {
		t.Errorf("the status line for %q does not name the peer account: %q", aSelf, peerLine)
	}
	otherLine := e2eSysStatusLine(t, status.Stdout, bSelf)
	if !strings.Contains(otherLine, "other") {
		t.Errorf("the status line for %q does not name the other account: %q", bSelf, otherLine)
	}
	thirdLine := e2eSysStatusLine(t, status.Stdout, thirdSelf)
	if !strings.Contains(thirdLine, "root") {
		t.Errorf("the status line for %q does not name the root account: %q", thirdSelf, thirdLine)
	}
}

// e2eSysWaitRegistered waits until list_agents succeeds, which means the MCP
// server has registered the session with the Broker.
func e2eSysWaitRegistered(t *testing.T, session *syscontainer.Session) {
	t.Helper()
	harness.EventuallyPoll(t, harness.ToolCallWaitBound, "session "+session.Account+" to register", func(deadline time.Time) (bool, string) {
		_, isError, err := session.PollCall(t, deadline, "list_agents", map[string]any{})
		if err != nil {
			return false, err.Error()
		}
		return !isError, ""
	})
}

// e2eSysJoinTopic joins topic once and fails the test on a tool error.
func e2eSysJoinTopic(t *testing.T, session *syscontainer.Session, topic string) {
	t.Helper()
	if text, isError := session.Call(t, "join_topic", map[string]any{"name": topic}); isError {
		t.Fatalf("join_topic %q as %s reported an error: %s", topic, session.Account, text)
	}
}

// e2eSysSelfAddress waits until the session reports its own address.
func e2eSysSelfAddress(t *testing.T, session *syscontainer.Session) string {
	t.Helper()
	var address string
	harness.EventuallyPoll(t, harness.ToolCallWaitBound, "session "+session.Account+" to report its address", func(deadline time.Time) (bool, string) {
		out, state := e2eSysListAgentsPoll(t, deadline, session, nil)
		if state != "" {
			return false, state
		}
		if out.Self == "" {
			return false, "self_address is empty"
		}
		address = out.Self
		return true, ""
	})
	return address
}

// e2eSysWaitVisible waits until observer lists address among the agents that
// share topic.
func e2eSysWaitVisible(t *testing.T, observer *syscontainer.Session, topic, address string) {
	t.Helper()
	harness.EventuallyPoll(t, harness.ToolCallWaitBound, "agent "+address+" to be visible to "+observer.Account, func(deadline time.Time) (bool, string) {
		out, state := e2eSysListAgentsPoll(t, deadline, observer, map[string]any{"topic": topic})
		if state != "" {
			return false, state
		}
		for _, agent := range out.Agents {
			if agent.Address == address {
				return true, ""
			}
		}
		return false, address + " is not listed"
	})
}

// e2eSysListAgentsPoll calls list_agents from a wait and decodes the JSON text
// result: the call is bounded by the wait's deadline and a transport error is
// returned as state.
func e2eSysListAgentsPoll(t *testing.T, deadline time.Time, session *syscontainer.Session, args map[string]any) (mcpserver.ListAgentsOut, string) {
	t.Helper()
	return sysPollDecode[mcpserver.ListAgentsOut](t, deadline, session, "list_agents", args)
}

// e2eSysStatusLine returns the status table line that names address.
func e2eSysStatusLine(t *testing.T, out, address string) string {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, address) {
			return line
		}
	}
	t.Fatalf("status output does not name %q:\n%s", address, out)
	return ""
}
