//go:build e2e

package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/node"
)

// lifecycleRestoreBound waits for an MCP server process to restore its topics
// after the local Broker restarts. The process retries a lost Broker with a
// backoff that starts at 250ms and caps at five seconds (mcpserver's
// loopConnBackoff); a capped retry and the restore that follows need more than
// five seconds, so the bound doubles the cap.
const lifecycleRestoreBound = 10 * time.Second

// TestLocalSessionChanges covers what happens to a registered agent when its
// session changes: a rename, a status change, a conversation clear, the MCP
// server process stopping and starting, the session ending, and a Broker
// restart. The Broker and the MCP server processes are real processes.
func TestLocalSessionChanges(t *testing.T) {
	host := harness.NewLocalHost(t, harness.LocalOptions{Name: "lifecycle"})
	listen := host.FreeAddr(t)
	brokerConfig := harness.ConfigYAML("broker:\n  listen: " + listen)
	listen = lifecycleStartBroker(t, host, brokerConfig, listen)
	brokerConfig = harness.ConfigYAML("broker:\n  listen: " + listen)
	workspace := lifecycleWorkspace(t)

	t.Run("REQ-agent-messaging-005 rename", func(t *testing.T) {
		api := lifecycleSession(t, host, "api-worker", workspace)
		defer api.End(t)
		observer := lifecycleSession(t, host, "observer", workspace)
		defer observer.End(t)
		alias := lifecycleAlias(t, lifecycleWaitRegistered(t, observer, "observer").Self)
		lifecycleWaitAgent(t, observer, "api-worker")

		api.Fake().Rename("api-v2")
		lifecycleWaitAgent(t, observer, "api-v2")
		lifecycleWaitNoAgent(t, observer, "api-worker")

		before := len(api.Fake().Frames())
		r := observer.Call(t, "send_message", map[string]any{
			"to":      "api-worker@" + alias,
			"message": "to the old name",
		})
		if !r.IsError {
			t.Fatalf("a send to the old address succeeded: %s", r.Text)
		}
		harness.Consistently(t, 500*time.Millisecond, "the old address delivers nothing", func() (bool, string) {
			if n := len(api.Fake().Frames()); n != before {
				return false, fmt.Sprintf("the session has %d frames, want %d", n, before)
			}
			return true, ""
		})
	})

	t.Run("REQ-agent-messaging-031 old name error", func(t *testing.T) {
		api := lifecycleSession(t, host, "api-worker", workspace)
		defer api.End(t)
		observer := lifecycleSession(t, host, "observer", workspace)
		defer observer.End(t)
		alias := lifecycleAlias(t, lifecycleWaitRegistered(t, observer, "observer").Self)
		lifecycleWaitAgent(t, observer, "api-worker")

		api.Fake().Rename("api-v2")
		lifecycleWaitAgent(t, observer, "api-v2")

		r := observer.Call(t, "send_message", map[string]any{
			"to":      "api-worker@" + alias,
			"message": "to the old name",
		})
		r.MustCode(t, "not_found")
		if !strings.Contains(r.Text, "api-v2") {
			t.Fatalf("the error does not name the current name api-v2: %s", r.Text)
		}
		if frames := api.Fake().Frames(); len(frames) != 0 {
			t.Fatalf("the renamed session received %d frames", len(frames))
		}
	})

	t.Run("REQ-agent-messaging-032 previous name", func(t *testing.T) {
		api := lifecycleSession(t, host, "api-worker", workspace)
		defer api.End(t)
		observer := lifecycleSession(t, host, "observer", workspace)
		defer observer.End(t)
		lifecycleWaitRegistered(t, observer, "observer")
		lifecycleWaitAgent(t, observer, "api-worker")

		api.Fake().Rename("api-v2")
		harness.EventuallyPoll(t, 3*time.Second, "the listing names api-worker as the previous name", func(deadline time.Time) (bool, string) {
			out, state := lifecycleListAgentsPoll(t, deadline, observer, nil)
			if state != "" {
				return false, state
			}
			entry, ok := lifecycleFindAgent(out, "api-v2")
			if !ok {
				return false, "api-v2 is not listed yet"
			}
			if entry.PreviousName != "api-worker" {
				return false, fmt.Sprintf("previous_name is %q, want api-worker", entry.PreviousName)
			}
			return true, ""
		})
	})

	t.Run("REQ-agent-messaging-010 status", func(t *testing.T) {
		worker := lifecycleSession(t, host, "worker", workspace)
		defer worker.End(t)
		observer := lifecycleSession(t, host, "observer", workspace)
		defer observer.End(t)
		lifecycleWaitRegistered(t, observer, "observer")
		lifecycleWaitAgent(t, observer, "worker")

		worker.Fake().SetBusy(true)
		lifecycleWaitStatus(t, observer, "worker", "busy")
		if got := lifecycleOracleStatus(t, host, "worker"); got != localpb.AgentStatus_AGENT_STATUS_BUSY {
			t.Fatalf("Broker status is %v, want BUSY", got)
		}

		worker.Fake().SetBusy(false)
		lifecycleWaitStatus(t, observer, "worker", "idle")
		if got := lifecycleOracleStatus(t, host, "worker"); got != localpb.AgentStatus_AGENT_STATUS_IDLE {
			t.Fatalf("Broker status is %v, want IDLE", got)
		}

		// An unknown status reaches the tool list as an empty field and the
		// local interface as UNSPECIFIED.
		worker.Fake().SetStatus("frobbing")
		lifecycleWaitStatus(t, observer, "worker", "")
		if got := lifecycleOracleStatus(t, host, "worker"); got != localpb.AgentStatus_AGENT_STATUS_UNSPECIFIED {
			t.Fatalf("Broker status is %v, want UNSPECIFIED", got)
		}
	})

	t.Run("REQ-agent-messaging-033 clear keeps identity", func(t *testing.T) {
		clearer := lifecycleSession(t, host, "clearer", workspace)
		defer clearer.End(t)
		observer := lifecycleSession(t, host, "observer", workspace)
		defer observer.End(t)
		lifecycleWaitRegistered(t, observer, "observer")
		lifecycleWaitAgent(t, observer, "clearer")
		// Shared topics are the intersection of both sides' memberships, so the
		// observer must hold t1 as well. Confirm the membership from the
		// observer's own listing before asserting the intersection.
		lifecycleJoin(t, observer, "t1")
		lifecycleWaitTopic(t, observer, "t1")
		lifecycleJoin(t, clearer, "t1")

		want := []string{"github.com/org/repo", "t1"}
		var address string
		harness.EventuallyPoll(t, 3*time.Second, "clearer shares the manual topic with observer", func(deadline time.Time) (bool, string) {
			out, state := lifecycleListAgentsPoll(t, deadline, observer, nil)
			if state != "" {
				return false, state
			}
			entry, ok := lifecycleFindAgent(out, "clearer")
			if !ok {
				return false, "clearer is not listed"
			}
			address = entry.Address
			return lifecycleSameSet(entry.SharedTopics, want), fmt.Sprintf("shared topics are %v, want %v", entry.SharedTopics, want)
		})

		// Clearing the conversation changes the session identifier, which must
		// not change the address or the memberships. A status change made after
		// the identifier change is a barrier: seeing it proves the Broker has
		// reread the record with the new identifier, so the window below
		// observes a record it has already processed.
		clearer.Fake().SetSessionID("after-clear")
		clearer.Fake().SetBusy(true)
		lifecycleWaitStatus(t, observer, "clearer", "busy")
		harness.ConsistentlyPoll(t, 1*time.Second, harness.ToolCallPollBound, "clearing the conversation keeps the address and memberships", nil, func(deadline time.Time) harness.Observation {
			out, state := lifecycleListAgentsPoll(t, deadline, observer, nil)
			if state != "" {
				return harness.Unobservable(state)
			}
			entry, ok := lifecycleFindAgent(out, "clearer")
			if !ok {
				return harness.Observed(false, "clearer is no longer listed")
			}
			if entry.Address != address {
				return harness.Observed(false, fmt.Sprintf("address is %q, want %q", entry.Address, address))
			}
			if !lifecycleSameSet(entry.SharedTopics, want) {
				return harness.Observed(false, fmt.Sprintf("shared topics are %v, want %v", entry.SharedTopics, want))
			}
			return harness.Observed(true, "")
		})

		r := observer.Call(t, "send_message", map[string]any{"to": address, "message": "after the clear"})
		r.MustOK(t)
		frame := clearer.WaitFrame(t, 3*time.Second)
		if frame.SessionID != "" {
			t.Fatalf("the delivered frame carries session_id %q, want none", frame.SessionID)
		}
	})

	t.Run("REQ-agent-messaging-034 disable and enable", func(t *testing.T) {
		worker := lifecycleSession(t, host, "worker", workspace)
		defer worker.End(t)
		observer := lifecycleSession(t, host, "observer", workspace)
		defer observer.End(t)
		lifecycleWaitRegistered(t, observer, "observer")
		lifecycleWaitAgent(t, observer, "worker")
		// Shared topics are the intersection of both sides' memberships, so the
		// observer must hold manual as well. Confirm the membership from the
		// observer's own listing before asserting the intersection.
		lifecycleJoin(t, observer, "manual")
		lifecycleWaitTopic(t, observer, "manual")
		lifecycleJoin(t, worker, "manual")
		harness.EventuallyPoll(t, 3*time.Second, "worker shares the manual topic with observer", func(deadline time.Time) (bool, string) {
			out, state := lifecycleListAgentsPoll(t, deadline, observer, nil)
			if state != "" {
				return false, state
			}
			entry, ok := lifecycleFindAgent(out, "worker")
			if !ok {
				return false, "worker is not listed"
			}
			return lifecycleSameSet(entry.SharedTopics, []string{"github.com/org/repo", "manual"}),
				fmt.Sprintf("shared topics are %v", entry.SharedTopics)
		})

		worker.StopMCP(t, syscall.SIGINT)
		lifecycleWaitNoAgent(t, observer, "worker")

		worker.RestartMCP(t)
		harness.EventuallyPoll(t, 3*time.Second, "the agent is registered again with the same name", func(deadline time.Time) (bool, string) {
			out, state := lifecycleListAgentsPoll(t, deadline, observer, nil)
			if state != "" {
				return false, state
			}
			entry, ok := lifecycleFindAgent(out, "worker")
			if !ok {
				return false, "worker is not listed"
			}
			if !lifecycleSameSet(entry.SharedTopics, []string{"github.com/org/repo"}) {
				return false, fmt.Sprintf("shared topics are %v, want only the default topic", entry.SharedTopics)
			}
			return true, ""
		})
	})

	t.Run("REQ-agent-messaging-012 local end", func(t *testing.T) {
		doomed := lifecycleSession(t, host, "doomed", workspace)
		observer := lifecycleSession(t, host, "observer", workspace)
		defer observer.End(t)
		lifecycleWaitRegistered(t, observer, "observer")
		lifecycleWaitAgent(t, observer, "doomed")

		doomed.End(t)
		harness.EventuallyPoll(t, 3*time.Second, "an ended session leaves the local list", func(deadline time.Time) (bool, string) {
			out, state := lifecycleListAgentsPoll(t, deadline, observer, nil)
			if state != "" {
				return false, state
			}
			if _, ok := lifecycleFindAgent(out, "doomed"); ok {
				return false, "doomed is still listed"
			}
			return true, ""
		})
	})

	t.Run("REQ-topics-022 membership ends with session and returns after broker restart", func(t *testing.T) {
		first := lifecycleSession(t, host, "x", workspace)
		lifecycleWaitRegistered(t, first, "x")
		observer := lifecycleSession(t, host, "observer", workspace)
		defer observer.End(t)
		lifecycleWaitRegistered(t, observer, "observer")
		lifecycleWaitAgent(t, observer, "x")
		lifecycleJoin(t, first, "t1")
		socketPath := first.Fake().SocketPath()
		first.End(t)

		// Topic membership is keyed by the AgentID derived from the inbox
		// socket path. Start the replacement on the same path so a membership
		// the Broker failed to clear would show up in its listing, and wait
		// until the ended session is out of the agent table first so the new
		// connection cannot attach to the old agent instead.
		harness.EventuallyPoll(t, 3*time.Second, "the ended session leaves the local list", func(deadline time.Time) (bool, string) {
			out, state := lifecycleListAgentsPoll(t, deadline, observer, nil)
			if state != "" {
				return false, state
			}
			if _, ok := lifecycleFindAgent(out, "x"); ok {
				return false, "x is still listed"
			}
			return true, ""
		})
		second := host.StartSession(t, harness.SessionSpec{
			Name:       "x",
			StartMCP:   true,
			Workdir:    workspace,
			SocketPath: socketPath,
		})
		defer second.End(t)
		harness.EventuallyPoll(t, 3*time.Second, "the new session has only the default topic", func(deadline time.Time) (bool, string) {
			out, state := lifecycleListTopicsPoll(t, deadline, second)
			if state != "" {
				return false, state
			}
			return lifecycleSameSet(out.Joined, []string{"github.com/org/repo"}), fmt.Sprintf("joined topics are %v", out.Joined)
		})

		y := lifecycleSession(t, host, "y", workspace)
		defer y.End(t)
		lifecycleWaitAgent(t, observer, "y")
		// Shared topics are the intersection of both sides' memberships, so the
		// observer must hold t1 as well. Confirm the membership from the
		// observer's own listing before asserting the intersection.
		lifecycleJoin(t, observer, "t1")
		lifecycleWaitTopic(t, observer, "t1")
		lifecycleJoin(t, y, "t1")
		harness.EventuallyPoll(t, 3*time.Second, "y shares t1 with observer", func(deadline time.Time) (bool, string) {
			out, state := lifecycleListAgentsPoll(t, deadline, observer, nil)
			if state != "" {
				return false, state
			}
			entry, ok := lifecycleFindAgent(out, "y")
			if !ok {
				return false, "y is not listed"
			}
			return lifecycleSameSet(entry.SharedTopics, []string{"github.com/org/repo", "t1"}),
				fmt.Sprintf("shared topics are %v", entry.SharedTopics)
		})

		lifecycleRestartBroker(t, host, brokerConfig, listen)

		harness.EventuallyPoll(t, lifecycleRestoreBound, "the MCP server process restores its topics after the Broker restart", func(deadline time.Time) (bool, string) {
			out, state := lifecycleListTopicsPoll(t, deadline, y)
			if state != "" {
				return false, state
			}
			return lifecycleSameSet(out.Joined, []string{"github.com/org/repo", "t1"}), fmt.Sprintf("joined topics are %v", out.Joined)
		})
		harness.EventuallyPoll(t, lifecycleRestoreBound, "observer sees the restored membership", func(deadline time.Time) (bool, string) {
			out, state := lifecycleListAgentsPoll(t, deadline, observer, nil)
			if state != "" {
				return false, state
			}
			entry, ok := lifecycleFindAgent(out, "y")
			if !ok {
				return false, "y is not listed"
			}
			return lifecycleSameSet(entry.SharedTopics, []string{"github.com/org/repo", "t1"}),
				fmt.Sprintf("shared topics are %v", entry.SharedTopics)
		})
	})
}

// lifecycleSession starts one simulated session with a real MCP server process
// working in workdir.
func lifecycleSession(t *testing.T, host *harness.Node, name, workdir string) *harness.Session {
	t.Helper()
	return host.StartSession(t, harness.SessionSpec{Name: name, StartMCP: true, Workdir: workdir})
}

// lifecycleStartBroker starts the node's Broker and fails when it does not come
// up. It returns the address the Broker actually uses in place of requested.
func lifecycleStartBroker(t *testing.T, host *harness.Node, config, requested string) string {
	t.Helper()
	start := host.StartBroker(t, harness.BrokerSpec{ConfigYAML: config})
	if !start.Running {
		t.Fatalf("the Broker did not start: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
	}
	return start.Address(requested)
}

// lifecycleRestartBroker stops the Broker and starts it again, returning the
// address the Broker actually uses in place of requested. The MCP server
// processes never start a Broker, so the restart is the only process that can
// answer.
func lifecycleRestartBroker(t *testing.T, host *harness.Node, config, requested string) string {
	t.Helper()
	host.StopBroker(t, syscall.SIGTERM)
	return lifecycleStartBroker(t, host, config, requested)
}

// lifecycleGitEnv is the environment of the git commands that create test
// workspaces. It never reads the host's git configuration.
func lifecycleGitEnv(t *testing.T) []string {
	t.Helper()
	return []string{
		"HOME=" + t.TempDir(),
		"PATH=" + os.Getenv("PATH"),
		"LANG=C.UTF-8",
		"GIT_CONFIG_NOSYSTEM=1",
	}
}

// lifecycleWorkspace creates the shared git workspace whose origin names the
// default topic github.com/org/repo.
func lifecycleWorkspace(t *testing.T) string {
	t.Helper()
	ws, err := node.GitWorkspace(filepath.Join(t.TempDir(), "repo"), "https://github.com/org/repo", lifecycleGitEnv(t))
	if err != nil {
		t.Fatalf("creating the git workspace: %v", err)
	}
	return ws
}

// lifecycleWaitRegistered waits until the session is registered and lists
// itself, and returns that listing.
func lifecycleWaitRegistered(t *testing.T, s *harness.Session, name string) mcpserver.ListAgentsOut {
	t.Helper()
	var out mcpserver.ListAgentsOut
	harness.EventuallyPoll(t, 3*time.Second, "the session is registered and lists itself", func(deadline time.Time) (bool, string) {
		var state string
		out, state = lifecycleListAgentsPoll(t, deadline, s, nil)
		if state != "" {
			return false, state
		}
		if got := lifecycleAddressName(out.Self); got != name {
			return false, fmt.Sprintf("self_address is %q, want the name part %s", out.Self, name)
		}
		return true, ""
	})
	return out
}

// lifecycleListAgentsPoll calls list_agents from a wait and returns the
// decoded listing: the call is bounded by the wait's deadline and a transport
// error is returned as state.
func lifecycleListAgentsPoll(t *testing.T, deadline time.Time, s *harness.Session, args map[string]any) (mcpserver.ListAgentsOut, string) {
	t.Helper()
	return pollDecode[mcpserver.ListAgentsOut](t, deadline, s, "list_agents", args)
}

// lifecycleListTopicsPoll calls list_topics from a wait and returns the decoded
// listing. The call is bounded by the wait's deadline and a transport error is
// returned as state.
func lifecycleListTopicsPoll(t *testing.T, deadline time.Time, s *harness.Session) (mcpserver.ListTopicsOut, string) {
	t.Helper()
	return pollDecode[mcpserver.ListTopicsOut](t, deadline, s, "list_topics", nil)
}

// lifecycleJoin joins name, retrying while the session is still registering.
// join_topic's handler may spend topicRPCTimeout before the update hint runs,
// so the poll uses harness.TopicCallPollBound.
func lifecycleJoin(t *testing.T, s *harness.Session, name string) {
	t.Helper()
	harness.EventuallyPoll(t, harness.TopicCallWaitBound, "join_topic "+name+" succeeds", func(deadline time.Time) (bool, string) {
		r, err := s.PollCallWithin(t, deadline, harness.TopicCallPollBound, "join_topic", map[string]any{"name": name})
		if err != nil {
			return false, err.Error()
		}
		if r.IsError {
			return false, r.Text
		}
		return true, ""
	})
}

// lifecycleWaitTopic waits until the session's own listing reports the topic.
func lifecycleWaitTopic(t *testing.T, s *harness.Session, topic string) {
	t.Helper()
	harness.EventuallyPoll(t, 3*time.Second, "list_topics contains "+topic, func(deadline time.Time) (bool, string) {
		out, state := lifecycleListTopicsPoll(t, deadline, s)
		if state != "" {
			return false, state
		}
		for _, joined := range out.Joined {
			if joined == topic {
				return true, ""
			}
		}
		return false, fmt.Sprintf("joined topics are %v", out.Joined)
	})
}

// lifecycleWaitAgent waits until observer lists an agent whose name part is
// name.
func lifecycleWaitAgent(t *testing.T, observer *harness.Session, name string) {
	t.Helper()
	harness.EventuallyPoll(t, 3*time.Second, "the agent "+name+" is listed", func(deadline time.Time) (bool, string) {
		out, state := lifecycleListAgentsPoll(t, deadline, observer, nil)
		if state != "" {
			return false, state
		}
		if _, ok := lifecycleFindAgent(out, name); !ok {
			return false, name + " is not listed"
		}
		return true, ""
	})
}

// lifecycleWaitNoAgent waits until observer no longer lists a name.
func lifecycleWaitNoAgent(t *testing.T, observer *harness.Session, name string) {
	t.Helper()
	harness.EventuallyPoll(t, 3*time.Second, "the agent "+name+" is gone from the list", func(deadline time.Time) (bool, string) {
		out, state := lifecycleListAgentsPoll(t, deadline, observer, nil)
		if state != "" {
			return false, state
		}
		if _, ok := lifecycleFindAgent(out, name); ok {
			return false, name + " is still listed"
		}
		return true, ""
	})
}

// lifecycleWaitStatus waits until observer lists name with the given status
// word; an empty want means the field is omitted.
func lifecycleWaitStatus(t *testing.T, observer *harness.Session, name, want string) {
	t.Helper()
	harness.EventuallyPoll(t, 3*time.Second, "the agent "+name+" has status "+want, func(deadline time.Time) (bool, string) {
		out, state := lifecycleListAgentsPoll(t, deadline, observer, nil)
		if state != "" {
			return false, state
		}
		entry, ok := lifecycleFindAgent(out, name)
		if !ok {
			return false, name + " is not listed"
		}
		return entry.Status == want, fmt.Sprintf("status is %q, want %q", entry.Status, want)
	})
}

// lifecycleOracleStatus returns the status the Broker reports for name.
func lifecycleOracleStatus(t *testing.T, host *harness.Node, name string) localpb.AgentStatus {
	t.Helper()
	resp, detail := host.Oracle().Status(t)
	if detail != nil {
		t.Fatalf("Status returned %s: %s", detail.GetCode(), detail.GetMessage())
	}
	for _, agent := range resp.GetAgents() {
		if lifecycleAddressName(agent.GetAddress()) == name {
			return agent.GetStatus()
		}
	}
	t.Fatalf("the Broker status does not contain %s: %v", name, resp.GetAgents())
	return localpb.AgentStatus_AGENT_STATUS_UNSPECIFIED
}

// lifecycleAlias returns the Broker alias from an agent address.
func lifecycleAlias(t *testing.T, addr string) string {
	t.Helper()
	at := strings.LastIndexByte(addr, '@')
	if at < 0 || at == len(addr)-1 {
		t.Fatalf("address %q carries no alias", addr)
	}
	return addr[at+1:]
}

// lifecycleSplitAddress splits "name [ref]@alias" into name and ref.
func lifecycleSplitAddress(addr string) (name, ref string) {
	at := strings.LastIndexByte(addr, '@')
	if at < 0 {
		return addr, ""
	}
	head := addr[:at]
	if i := strings.LastIndex(head, " ["); i >= 0 && strings.HasSuffix(head, "]") {
		return head[:i], head[i+2 : len(head)-1]
	}
	return head, ""
}

// lifecycleAddressName returns the name part of an address, without a ref.
func lifecycleAddressName(addr string) string {
	name, _ := lifecycleSplitAddress(addr)
	return name
}

// lifecycleFindAgent returns the entry whose name part is name.
func lifecycleFindAgent(out mcpserver.ListAgentsOut, name string) (mcpserver.AgentOut, bool) {
	for _, agent := range out.Agents {
		if lifecycleAddressName(agent.Address) == name {
			return agent, true
		}
	}
	return mcpserver.AgentOut{}, false
}

// lifecycleSameSet reports whether got and want contain the same strings.
func lifecycleSameSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	seen := make(map[string]bool, len(got))
	for _, value := range got {
		seen[value] = true
	}
	for _, value := range want {
		if !seen[value] {
			return false
		}
	}
	return true
}
