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

	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/node"
)

// TestLocalTopics covers joining, leaving and visibility on one host and every
// branch that decides which topics a new agent joins automatically.
func TestLocalTopics(t *testing.T) {
	host := harness.NewLocalHost(t, harness.LocalOptions{Name: "topics"})
	listen := host.FreeAddr(t)
	listen = topicsStartBroker(t, host, topicsConfig(listen, true), listen)
	defaultConfig := topicsConfig(listen, true)

	t.Run("REQ-topics-001 join", func(t *testing.T) {
		s := topicsSession(t, host, "joiner", topicsPlain(t))
		defer s.End(t)
		topicsJoin(t, s, "release")
		out, state := topicsList(t, s)
		if state != "" {
			t.Fatalf("list_topics failed: %s", state)
		}
		if !topicsContains(out.Joined, "release") {
			t.Fatalf("joined topics after join_topic are %v, want release", out.Joined)
		}
	})

	t.Run("REQ-topics-002 join new name without approval", func(t *testing.T) {
		ws := topicsPlain(t)
		p := topicsSession(t, host, "p", ws)
		defer p.End(t)
		q := topicsSession(t, host, "q", ws)
		defer q.End(t)

		name := fmt.Sprintf("fresh-%d", time.Now().UnixNano())
		topicsJoin(t, p, name)
		topicsJoin(t, q, name)

		topicsWaitVisible(t, p, "q")
		topicsWaitVisible(t, q, "p")
	})

	t.Run("REQ-topics-003 leave", func(t *testing.T) {
		ws := topicsPlain(t)
		p := topicsSession(t, host, "p", ws)
		defer p.End(t)
		q := topicsSession(t, host, "q", ws)
		defer q.End(t)
		topicsJoin(t, p, "shared")
		topicsJoin(t, q, "shared")
		topicsWaitVisible(t, p, "q")

		topicsLeave(t, p, "shared")
		out, state := topicsList(t, p)
		if state != "" {
			t.Fatalf("list_topics failed: %s", state)
		}
		if topicsContains(out.Joined, "shared") {
			t.Fatalf("joined topics still contain shared: %v", out.Joined)
		}
		topicsWaitInvisible(t, q, "p")
	})

	t.Run("REQ-topics-004 many topics", func(t *testing.T) {
		s := topicsSession(t, host, "multi", topicsPlain(t))
		defer s.End(t)
		topicsJoin(t, s, "t1")
		topicsJoin(t, s, "t2")
		out, state := topicsList(t, s)
		if state != "" {
			t.Fatalf("list_topics failed: %s", state)
		}
		if !topicsSameSet(out.Joined, []string{"t1", "t2"}) {
			t.Fatalf("joined topics are %v, want t1 and t2", out.Joined)
		}
	})

	t.Run("REQ-topics-006 no topic means invisible", func(t *testing.T) {
		lonely := topicsSession(t, host, "lonely", topicsPlain(t))
		defer lonely.End(t)
		harness.EventuallyPoll(t, harness.EventTimeout, "the session with no topics is registered", func(deadline time.Time) (bool, string) {
			out, state := topicsListPoll(t, deadline, lonely)
			if state != "" {
				return false, state
			}
			return len(out.Joined) == 0, fmt.Sprintf("joined topics are %v", out.Joined)
		})

		watcher := topicsSession(t, host, "watcher", topicsPlain(t))
		defer watcher.End(t)
		topicsJoin(t, watcher, "watch")
		harness.ConsistentlyPoll(t, 1*time.Second, harness.ListAgentsPollBound, "an agent in no topic appears in no list", nil, func(deadline time.Time) harness.Observation {
			out, state := topicsListAgentsPoll(t, deadline, watcher, nil)
			if state != "" {
				return harness.Unobservable(state)
			}
			if _, ok := topicsFindAgent(out, "lonely"); ok {
				return harness.Observed(false, "lonely is listed by watcher")
			}
			return harness.Observed(true, "")
		})
	})

	t.Run("REQ-topics-015 project topics", func(t *testing.T) {
		ws := topicsProject(t, "topics: [t1, t2]\n", "https://github.com/org/repo")
		s := topicsSession(t, host, "project", ws)
		defer s.End(t)
		topicsWaitJoinedSet(t, s, "t1", "t2")
	})

	t.Run("REQ-topics-016 origin topic", func(t *testing.T) {
		ws := topicsOrigin(t, "https://github.com/org/repo")
		s := topicsSession(t, host, "origin", ws)
		defer s.End(t)
		topicsWaitJoinedSet(t, s, "github.com/org/repo")
	})

	t.Run("REQ-topics-017 spellings", func(t *testing.T) {
		origins := []string{
			"git@github.com:org/repo.git",
			"ssh://git@GitHub.com:22/org/repo.git",
			"https://github.com/org/repo",
		}
		sessions := make([]*harness.Session, 0, len(origins))
		for i, origin := range origins {
			s := topicsSession(t, host, fmt.Sprintf("spelling%d", i), topicsOrigin(t, origin))
			defer s.End(t)
			sessions = append(sessions, s)
			topicsWaitJoinedSet(t, s, "github.com/org/repo")
		}
		for i, s := range sessions {
			for j := range sessions {
				if i == j {
					continue
				}
				topicsWaitVisible(t, s, fmt.Sprintf("spelling%d", j))
			}
		}
	})

	t.Run("REQ-topics-018 non-git", func(t *testing.T) {
		loner := topicsSession(t, host, "loner", topicsPlain(t))
		defer loner.End(t)
		harness.EventuallyPoll(t, harness.EventTimeout, "a session outside a git repository joins no topic", func(deadline time.Time) (bool, string) {
			out, state := topicsListPoll(t, deadline, loner)
			if state != "" {
				return false, state
			}
			return len(out.Joined) == 0, fmt.Sprintf("joined topics are %v", out.Joined)
		})

		watcher := topicsSession(t, host, "watcher", topicsOrigin(t, "https://github.com/org/repo"))
		defer watcher.End(t)
		topicsWaitJoinedSet(t, watcher, "github.com/org/repo")
		harness.ConsistentlyPoll(t, 1*time.Second, harness.ListAgentsPollBound, "an agent with no topic appears in no list", nil, func(deadline time.Time) harness.Observation {
			out, state := topicsListAgentsPoll(t, deadline, watcher, nil)
			if state != "" {
				return harness.Unobservable(state)
			}
			if _, ok := topicsFindAgent(out, "loner"); ok {
				return harness.Observed(false, "loner is listed by watcher")
			}
			return harness.Observed(true, "")
		})
	})

	t.Run("REQ-topics-020 broker auto_join off", func(t *testing.T) {
		listen = topicsRestartBroker(t, host, topicsConfig(listen, false), listen)
		defaultConfig = topicsConfig(listen, true)

		s := topicsSession(t, host, "no-join", topicsOrigin(t, "https://github.com/org/repo"))
		defer s.End(t)
		topicsWaitRegistered(t, s)
		harness.ConsistentlyPoll(t, 1*time.Second, harness.TopicCallPollBound, "Broker configuration turns automatic joining off", nil, func(deadline time.Time) harness.Observation {
			out, state := topicsListPoll(t, deadline, s)
			if state != "" {
				return harness.Unobservable(state)
			}
			if len(out.Joined) != 0 {
				return harness.Observed(false, fmt.Sprintf("joined topics are %v", out.Joined))
			}
			return harness.Observed(true, "")
		})
	})

	t.Run("REQ-topics-021 project auto_join off", func(t *testing.T) {
		topicsRestartBroker(t, host, defaultConfig, listen)

		off := topicsSession(t, host, "off", topicsProject(t, "auto_join: false\n", "https://github.com/org/repo"))
		defer off.End(t)
		topicsWaitRegistered(t, off)
		harness.ConsistentlyPoll(t, 1*time.Second, harness.TopicCallPollBound, "project configuration turns automatic joining off", nil, func(deadline time.Time) harness.Observation {
			out, state := topicsListPoll(t, deadline, off)
			if state != "" {
				return harness.Unobservable(state)
			}
			if len(out.Joined) != 0 {
				return harness.Observed(false, fmt.Sprintf("joined topics are %v", out.Joined))
			}
			return harness.Observed(true, "")
		})

		on := topicsSession(t, host, "on", topicsOrigin(t, "https://github.com/org/repo"))
		defer on.End(t)
		topicsWaitJoinedSet(t, on, "github.com/org/repo")
	})

	t.Run("workdir subdirectory", func(t *testing.T) {
		ws := topicsOrigin(t, "https://github.com/org/repo")
		sub := filepath.Join(ws, "sub", "dir")
		if err := os.MkdirAll(sub, 0o755); err != nil {
			t.Fatalf("creating the subdirectory: %v", err)
		}
		atRoot := topicsSession(t, host, "at-root", ws)
		defer atRoot.End(t)
		topicsWaitJoinedSet(t, atRoot, "github.com/org/repo")
		inSub := topicsSession(t, host, "in-sub", sub)
		defer inSub.End(t)
		topicsWaitJoinedSet(t, inSub, "github.com/org/repo")
	})

	t.Run("REQ-topics-026 join default without name", func(t *testing.T) {
		origin := topicsSession(t, host, "nameless-origin", topicsOrigin(t, "https://github.com/org/repo"))
		defer origin.End(t)
		topicsWaitJoinedSet(t, origin, "github.com/org/repo")
		topicsLeave(t, origin, "github.com/org/repo")
		topicsWaitJoinedSet(t, origin)
		topicsJoinDefault(t, origin)
		topicsWaitJoinedSet(t, origin, "github.com/org/repo")

		project := topicsSession(t, host, "nameless-project",
			topicsProject(t, "topics: [t1, t2]\n", "https://github.com/org/repo"))
		defer project.End(t)
		topicsWaitJoinedSet(t, project, "t1", "t2")
		topicsLeave(t, project, "t1")
		topicsLeave(t, project, "t2")
		topicsWaitJoinedSet(t, project)
		topicsJoinDefault(t, project)
		topicsWaitJoinedSet(t, project, "t1", "t2")

		none := topicsSession(t, host, "nameless-none", topicsPlain(t))
		defer none.End(t)
		topicsWaitRegistered(t, none)
		r := none.Call(t, "join_topic", nil)
		if !r.IsError {
			t.Fatalf("join_topic without a name succeeded in a workspace with no default topic: %s", string(r.Structured))
		}
		if !strings.Contains(r.Text, "default topic") || !strings.Contains(r.Text, "invalid_argument") {
			t.Fatalf("join_topic error = %q, want a default-topic invalid_argument error", r.Text)
		}
	})

	t.Run("REQ-topics-026 join default with broker auto_join off", func(t *testing.T) {
		listen = topicsRestartBroker(t, host, topicsConfig(listen, false), listen)
		// The defers run in reverse: this restore is registered before the
		// session's End below, so the session ends first and the restore never
		// reaches a session that is still running.
		defer func() {
			listen = topicsRestartBroker(t, host, defaultConfig, listen)
		}()

		s := topicsSession(t, host, "nameless-no-autojoin", topicsOrigin(t, "https://github.com/org/repo"))
		defer s.End(t)
		topicsWaitRegistered(t, s)
		harness.ConsistentlyPoll(t, 1*time.Second, harness.TopicCallPollBound, "automatic joining stays off before the nameless call", nil, func(deadline time.Time) harness.Observation {
			out, state := topicsListPoll(t, deadline, s)
			if state != "" {
				return harness.Unobservable(state)
			}
			if len(out.Joined) != 0 {
				return harness.Observed(false, fmt.Sprintf("joined topics are %v", out.Joined))
			}
			return harness.Observed(true, "")
		})
		topicsJoinDefault(t, s)
		topicsWaitJoinedSet(t, s, "github.com/org/repo")
	})

	t.Run("REQ-topics-027 leave default without name", func(t *testing.T) {
		ws := topicsOrigin(t, "https://github.com/org/repo")
		s := topicsSession(t, host, "nameless-leave", ws)
		defer s.End(t)
		topicsWaitJoinedSet(t, s, "github.com/org/repo")

		topicsJoin(t, s, "sentinel")
		topicsWaitJoinedSet(t, s, "github.com/org/repo", "sentinel")

		topicsLeaveDefault(t, s)
		topicsWaitJoinedSet(t, s, "sentinel")

		listen = topicsRestartBroker(t, host, defaultConfig, listen)
		// Waiting for exactly this set after the restart proves the restore
		// ran; an empty set could otherwise be read before it started.
		topicsWaitJoinedSetWithin(t, s, harness.TopicCallWaitBound, "sentinel")

		// The restore joins topics one at a time, so it can reach the sentinel
		// before it decides about the default topic; holding the exact set for
		// a second keeps a default topic joined late from passing unseen.
		harness.ConsistentlyPoll(t, 1*time.Second, harness.TopicCallPollBound, "the default topic stays out once the sentinel is restored", nil, func(deadline time.Time) harness.Observation {
			out, state := topicsListPoll(t, deadline, s)
			if state != "" {
				return harness.Unobservable(state)
			}
			if !topicsSameSet(out.Joined, []string{"sentinel"}) {
				return harness.Observed(false, fmt.Sprintf("joined topics are %v", out.Joined))
			}
			return harness.Observed(true, "")
		})

		fresh := topicsSession(t, host, "nameless-leave-fresh", ws)
		defer fresh.End(t)
		topicsWaitJoinedSet(t, fresh, "github.com/org/repo")
	})
}

func topicsConfig(listen string, autoJoin bool) string {
	parts := []string{"broker:\n  listen: " + listen}
	if !autoJoin {
		parts = append(parts, "topics:\n  auto_join: false")
	}
	return harness.ConfigYAML(parts...)
}

func topicsSession(t *testing.T, host *harness.Node, name, workdir string) *harness.Session {
	t.Helper()
	return host.StartSession(t, harness.SessionSpec{Name: name, StartMCP: true, Workdir: workdir})
}

func topicsStartBroker(t *testing.T, host *harness.Node, config, requested string) string {
	t.Helper()
	start := host.StartBroker(t, harness.BrokerSpec{ConfigYAML: config})
	if !start.Running {
		t.Fatalf("the Broker did not start: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
	}
	return start.Address(requested)
}

func topicsRestartBroker(t *testing.T, host *harness.Node, config, requested string) string {
	t.Helper()
	host.StopBroker(t, syscall.SIGTERM)
	return topicsStartBroker(t, host, config, requested)
}

func topicsGitEnv(t *testing.T) []string {
	t.Helper()
	return []string{
		"HOME=" + t.TempDir(),
		"PATH=" + os.Getenv("PATH"),
		"LANG=C.UTF-8",
		"GIT_CONFIG_NOSYSTEM=1",
	}
}

func topicsPlain(t *testing.T) string {
	t.Helper()
	ws, err := node.PlainWorkspace(filepath.Join(t.TempDir(), "plain"))
	if err != nil {
		t.Fatalf("creating the plain workspace: %v", err)
	}
	return ws
}

func topicsOrigin(t *testing.T, origin string) string {
	t.Helper()
	ws, err := node.GitWorkspace(filepath.Join(t.TempDir(), "repo"), origin, topicsGitEnv(t))
	if err != nil {
		t.Fatalf("creating the git workspace: %v", err)
	}
	return ws
}

func topicsProject(t *testing.T, projectYAML, origin string) string {
	t.Helper()
	ws, err := node.ProjectWorkspace(filepath.Join(t.TempDir(), "repo"), projectYAML, origin, topicsGitEnv(t))
	if err != nil {
		t.Fatalf("creating the project workspace: %v", err)
	}
	return ws
}

func topicsWaitRegistered(t *testing.T, s *harness.Session) {
	t.Helper()
	harness.EventuallyPoll(t, harness.EventTimeout, "the session is registered", func(deadline time.Time) (bool, string) {
		_, state := topicsListPoll(t, deadline, s)
		if state != "" {
			return false, state
		}
		return true, ""
	})
}

func topicsList(t *testing.T, s *harness.Session) (mcpserver.ListTopicsOut, string) {
	t.Helper()
	r := s.Call(t, "list_topics", nil)
	if r.IsError {
		return mcpserver.ListTopicsOut{}, r.Text
	}
	out, err := harness.Decode[mcpserver.ListTopicsOut](string(r.Structured))
	if err != nil {
		return mcpserver.ListTopicsOut{}, "decoding list_topics: " + err.Error()
	}
	return out, ""
}

func topicsListPoll(t *testing.T, deadline time.Time, s *harness.Session) (mcpserver.ListTopicsOut, string) {
	t.Helper()
	return pollDecode[mcpserver.ListTopicsOut](t, deadline, s, "list_topics", nil)
}

func topicsListAgents(t *testing.T, s *harness.Session, args map[string]any) (mcpserver.ListAgentsOut, string) {
	t.Helper()
	r := s.Call(t, "list_agents", args)
	if r.IsError {
		return mcpserver.ListAgentsOut{}, r.Text
	}
	out, err := harness.Decode[mcpserver.ListAgentsOut](string(r.Structured))
	if err != nil {
		return mcpserver.ListAgentsOut{}, "decoding list_agents: " + err.Error()
	}
	return out, ""
}

func topicsListAgentsPoll(t *testing.T, deadline time.Time, s *harness.Session, args map[string]any) (mcpserver.ListAgentsOut, string) {
	t.Helper()
	return pollDecode[mcpserver.ListAgentsOut](t, deadline, s, "list_agents", args)
}

// brokerTopicTool reports whether a tool is a topic tool that reaches the
// Broker, whose handler may spend topicRPCTimeout before the update hint runs.
// list_topics makes one ListTopics RPC bounded by topicRPCTimeout, so it
// belongs here too: polling it with the generic bound would cut the call before
// the product's own timeout and hide a Broker that stopped answering.
func brokerTopicTool(tool string) bool {
	switch tool {
	case "join_topic", "leave_topic", "list_topics":
		return true
	}
	return false
}

// pollableTool reports whether a wait may retry a tool after a transport error:
// only a tool with no side effect or a guaranteed idempotent effect qualifies.
func pollableTool(tool string) bool {
	switch tool {
	case "list_agents", "list_brokers", "list_topics", "get_config", "join_topic", "leave_topic":
		return true
	}
	return false
}

func topicsJoin(t *testing.T, s *harness.Session, name string) mcpserver.TopicsOut {
	t.Helper()
	var out mcpserver.TopicsOut
	harness.EventuallyPoll(t, harness.TopicCallWaitBound, "join_topic "+name+" succeeds", func(deadline time.Time) (bool, string) {
		r, err := s.PollCallWithin(t, deadline, harness.TopicCallPollBound, "join_topic", map[string]any{"name": name})
		if err != nil {
			return false, err.Error()
		}
		if r.IsError {
			return false, r.Text
		}
		var decErr error
		out, decErr = harness.Decode[mcpserver.TopicsOut](string(r.Structured))
		if decErr != nil {
			return false, "decoding join_topic: " + decErr.Error()
		}
		return true, ""
	})
	return out
}

func topicsLeave(t *testing.T, s *harness.Session, name string) {
	t.Helper()
	harness.EventuallyPoll(t, harness.TopicCallWaitBound, "leave_topic "+name+" succeeds", func(deadline time.Time) (bool, string) {
		r, err := s.PollCallWithin(t, deadline, harness.TopicCallPollBound, "leave_topic", map[string]any{"name": name})
		if err != nil {
			return false, err.Error()
		}
		if r.IsError {
			return false, r.Text
		}
		return true, ""
	})
}

func topicsJoinDefault(t *testing.T, s *harness.Session) mcpserver.TopicsOut {
	t.Helper()
	var out mcpserver.TopicsOut
	harness.EventuallyPoll(t, harness.TopicCallWaitBound, "join_topic without a name succeeds", func(deadline time.Time) (bool, string) {
		r, err := s.PollCallWithin(t, deadline, harness.TopicCallPollBound, "join_topic", nil)
		if err != nil {
			return false, err.Error()
		}
		if r.IsError {
			return false, r.Text
		}
		var decErr error
		out, decErr = harness.Decode[mcpserver.TopicsOut](string(r.Structured))
		if decErr != nil {
			return false, "decoding join_topic: " + decErr.Error()
		}
		return true, ""
	})
	return out
}

func topicsLeaveDefault(t *testing.T, s *harness.Session) {
	t.Helper()
	harness.EventuallyPoll(t, harness.TopicCallWaitBound, "leave_topic without a name succeeds", func(deadline time.Time) (bool, string) {
		r, err := s.PollCallWithin(t, deadline, harness.TopicCallPollBound, "leave_topic", nil)
		if err != nil {
			return false, err.Error()
		}
		if r.IsError {
			return false, r.Text
		}
		return true, ""
	})
}

func topicsWaitJoinedSet(t *testing.T, s *harness.Session, want ...string) {
	t.Helper()
	topicsWaitJoinedSetWithin(t, s, harness.EventTimeout, want...)
}

// topicsWaitJoinedSetWithin waits until the joined set is exactly want. bound
// must be at least harness.TopicCallWaitBound, the list_topics wait bound;
// EventuallyPoll lifts only to ToolCallWaitBound, so a wait that covers a
// reconnect and the follow-on restore passes lifecycleRestoreWait instead.
func topicsWaitJoinedSetWithin(t *testing.T, s *harness.Session, bound time.Duration, want ...string) {
	t.Helper()
	harness.EventuallyPoll(t, bound, "the joined topics are exactly "+strings.Join(want, ", "), func(deadline time.Time) (bool, string) {
		out, state := topicsListPoll(t, deadline, s)
		if state != "" {
			return false, state
		}
		return topicsSameSet(out.Joined, want), fmt.Sprintf("joined topics are %v, want %v", out.Joined, want)
	})
}

func topicsWaitVisible(t *testing.T, observer *harness.Session, name string) {
	t.Helper()
	harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "the agent "+name+" is visible", func(deadline time.Time) (bool, string) {
		out, state := topicsListAgentsPoll(t, deadline, observer, nil)
		if state != "" {
			return false, state
		}
		if _, ok := topicsFindAgent(out, name); !ok {
			return false, name + " is not listed"
		}
		return true, ""
	})
}

func topicsWaitInvisible(t *testing.T, observer *harness.Session, name string) {
	t.Helper()
	harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "the agent "+name+" is no longer visible", func(deadline time.Time) (bool, string) {
		out, state := topicsListAgentsPoll(t, deadline, observer, nil)
		if state != "" {
			return false, state
		}
		if _, ok := topicsFindAgent(out, name); ok {
			return false, name + " is still listed"
		}
		return true, ""
	})
}

func topicsContains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func topicsSplitAddress(addr string) (name, ref string) {
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

func topicsFindAgent(out mcpserver.ListAgentsOut, name string) (mcpserver.AgentOut, bool) {
	for _, agent := range out.Agents {
		if got, _ := topicsSplitAddress(agent.Address); got == name {
			return agent, true
		}
	}
	return mcpserver.AgentOut{}, false
}

func topicsSameSet(got, want []string) bool {
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
