//go:build e2e

package e2e

import (
	"encoding/json"
	"slices"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/node"
)

const (
	// versionLocalNoBrokerBudget is the fallback for the first failed attempt
	// after the Broker stops: one connection attempt and the probe that follows
	// it. The wait that uses it polls a tool through EventuallyPoll; the
	// project-wide fallback is longer than ToolCallWaitBound, so the wait keeps
	// it unchanged.
	versionLocalNoBrokerBudget = harness.EventTimeout
	// versionLocalRecoveryBudget bounds the reconnect after the user starts a
	// compatible Broker. mcpserver.ConnBackoffCap is the longest wait between
	// attempts; the margin covers the dial, Hello and registration that follow.
	versionLocalRecoveryBudget = mcpserver.ConnBackoffCap + 5*time.Second
)

// versionLocalBrokerProcs returns the node's Broker processes. The
// environment is the filter because every process the node starts inherits it,
// including a Broker a test starts.
func versionLocalBrokerProcs(n *harness.Node) ([]node.Proc, error) {
	home := n.AccountHome()
	return node.ListProcs(func(p node.Proc) bool {
		if p.Env["PEERS_TEST_HOME"] != home {
			return false
		}
		return len(p.Args) >= 3 && p.Args[1] == "service" && p.Args[2] == "run"
	})
}

// versionLocalMustBrokerProcs is versionLocalBrokerProcs for a call outside an
// Eventually condition.
func versionLocalMustBrokerProcs(t *testing.T, n *harness.Node) []node.Proc {
	t.Helper()
	procs, err := versionLocalBrokerProcs(n)
	if err != nil {
		t.Fatalf("listing the Broker processes: %v", err)
	}
	return procs
}

// versionLocalMCPPIDs returns the node's MCP server pids, sorted. A pid set
// that stays the same across recovery proves the session did not reconnect the
// MCP service.
func versionLocalMCPPIDs(t *testing.T, n *harness.Node) []int {
	t.Helper()
	home := n.AccountHome()
	procs, err := node.ListProcs(func(p node.Proc) bool {
		if p.Env["PEERS_TEST_HOME"] != home {
			return false
		}
		return len(p.Args) >= 2 && p.Args[1] == "mcp"
	})
	if err != nil {
		t.Fatalf("listing the node's MCP server processes: %v", err)
	}
	out := make([]int, 0, len(procs))
	for _, p := range procs {
		out = append(out, p.PID)
	}
	sort.Ints(out)
	return out
}

// versionLocalLogRecord returns the last JSON log record whose event field is
// event, or nil when the log has none.
func versionLocalLogRecord(log []byte, event string) map[string]any {
	var found map[string]any
	for _, line := range strings.Split(string(log), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			continue
		}
		if record["event"] == event {
			found = record
		}
	}
	return found
}

// versionLocalAddressName returns the name part of an agent address.
func versionLocalAddressName(addr string) string {
	if at := strings.LastIndexByte(addr, '@'); at >= 0 {
		return addr[:at]
	}
	return addr
}

// versionLocalSetup starts a node whose Broker speaks the default local
// protocol version, starts session a at that version, and starts session b at
// version 2. It waits until b reports the mismatch so the caller does not race
// the background Broker selection.
func versionLocalSetup(t *testing.T) (*harness.Node, harness.BrokerStart, *harness.Session, *harness.Session) {
	t.Helper()

	n := harness.NewLocalHost(t, harness.LocalOptions{Name: "v"})
	n.WriteConfig(t, "broker:\n  listen: "+n.FreeAddr(t)+"\n")
	st := n.StartBroker(t, harness.BrokerSpec{})
	if !st.Running {
		t.Fatalf("the Broker did not start: exit code %d: %s", st.ExitCode, strings.TrimSpace(st.StderrLast))
	}
	if _, detail := n.Oracle().Status(t); detail != nil {
		t.Fatalf("Status returned %s: %s", detail.GetCode(), detail.GetMessage())
	}

	a := n.StartSession(t, harness.SessionSpec{Name: "a", StartMCP: true})
	harness.EventuallyPoll(t, harness.EventTimeout, "a is registered", func(deadline time.Time) (bool, string) {
		r, err := a.PollCall(t, deadline, "list_agents", nil)
		if err != nil {
			return false, err.Error()
		}
		if r.IsError {
			return false, r.Text
		}
		return true, ""
	})

	b := n.StartSession(t, harness.SessionSpec{
		Name:     "b",
		StartMCP: true,
		Env:      map[string]string{"PEERS_TEST_LOCAL_PROTOCOL_VERSION": "2"},
	})

	harness.EventuallyPoll(t, harness.EventTimeout, "b reports the local protocol version mismatch", func(deadline time.Time) (bool, string) {
		r, err := b.PollCall(t, deadline, "list_agents", nil)
		if err != nil {
			return false, err.Error()
		}
		if !r.IsError {
			return false, "list_agents succeeded"
		}
		if r.Code() != "version_mismatch" {
			return false, r.Text
		}
		return true, ""
	})
	return n, st, a, b
}

// TestLocalVersionMismatchReq016 checks the REQ-broker-config-016 acceptance:
// a session whose MCP server process speaks another local protocol version
// fails every tool call with version_mismatch, the Broker keeps running and
// keeps its registered agents, and no second Broker appears.
func TestLocalVersionMismatchReq016(t *testing.T) {
	n, st, a, b := versionLocalSetup(t)

	for i := 0; i < 3; i++ {
		r := b.Call(t, "list_agents", nil)
		if !r.IsError {
			t.Fatalf("list_agents %d succeeded against a Broker at another version: %s", i+1, r.Text)
		}
		if got := r.Code(); got != "version_mismatch" {
			t.Fatalf("list_agents %d error code = %q, want version_mismatch; result: %s", i+1, got, r.Text)
		}
		if !strings.Contains(r.Text, "restart the Broker") {
			t.Fatalf("list_agents %d error = %q, want a \"restart the Broker\" hint", i+1, r.Text)
		}
	}

	procs := versionLocalMustBrokerProcs(t, n)
	if len(procs) != 1 {
		t.Fatalf("the node runs %d Broker processes, want 1: %v", len(procs), procs)
	}
	if procs[0].PID != st.PID {
		t.Fatalf("the Broker process is %d, want the one that was started: %d", procs[0].PID, st.PID)
	}

	resp, detail := n.Oracle().Status(t)
	if detail != nil {
		t.Fatalf("Status returned %s: %s", detail.GetCode(), detail.GetMessage())
	}
	if got := len(resp.GetAgents()); got != 1 {
		t.Fatalf("the Broker reports %d agents, want only a: %v", got, resp.GetAgents())
	}
	if name := versionLocalAddressName(resp.GetAgents()[0].GetAddress()); name != "a" {
		t.Fatalf("the registered agent is %q, want a", name)
	}

	a.Call(t, "list_agents", nil).MustOK(t)

	record := versionLocalLogRecord(n.BrokerLog(t), "hello_version_mismatch")
	if record == nil {
		t.Fatal("the Broker log has no hello_version_mismatch record")
	}
	if got := record["client_version"]; got != float64(2) {
		t.Errorf("client_version = %v, want 2", got)
	}
	if got := record["broker_version"]; got != float64(1) {
		t.Errorf("broker_version = %v, want 1", got)
	}
}

// TestLocalVersionMismatchRecoveryReq016 checks the other half of
// REQ-broker-config-016: after the incompatible Broker stops, b's MCP server
// process reports that no Broker is available and starts nothing. When the
// user starts a Broker at a version this MCP server speaks, the session
// recovers without reconnecting the MCP server process.
func TestLocalVersionMismatchRecoveryReq016(t *testing.T) {
	n, st, a, b := versionLocalSetup(t)

	a.End(t)
	if code := n.StopBroker(t, syscall.SIGTERM); code != 0 {
		t.Fatalf("the Broker exited %d on SIGTERM, want 0", code)
	}

	mcpBefore := versionLocalMCPPIDs(t, n)

	harness.EventuallyPoll(t, versionLocalNoBrokerBudget, "b reports that no Broker is available", func(deadline time.Time) (bool, string) {
		r, err := b.PollCall(t, deadline, "list_agents", nil)
		if err != nil {
			return false, err.Error()
		}
		if !r.IsError {
			return false, "list_agents succeeded without a Broker"
		}
		for _, want := range []string{"no Broker is available", "peers service install", "peers service run"} {
			if !strings.Contains(r.Text, want) {
				return false, r.Text
			}
		}
		procs, err := versionLocalBrokerProcs(n)
		if err != nil {
			return false, err.Error()
		}
		if len(versionLocalWithout(procs, st.PID)) != 0 {
			return false, "an MCP server process started a Broker"
		}
		return true, ""
	})

	fresh := n.StartBroker(t, harness.BrokerSpec{Env: map[string]string{"PEERS_TEST_LOCAL_PROTOCOL_VERSION": "2"}})
	if !fresh.Running {
		t.Fatalf("the replacement Broker did not start: exit code %d: %s", fresh.ExitCode, strings.TrimSpace(fresh.StderrLast))
	}

	harness.EventuallyPoll(t, versionLocalRecoveryBudget, "b recovers after a compatible Broker starts", func(deadline time.Time) (bool, string) {
		r, err := b.PollCall(t, deadline, "list_agents", nil)
		if err != nil {
			return false, err.Error()
		}
		if r.IsError {
			return false, r.Text
		}
		return true, ""
	})
	if procs := versionLocalMustBrokerProcs(t, n); len(procs) != 1 {
		t.Fatalf("the node runs %d Broker processes after recovery, want 1: %v", len(procs), procs)
	}
	if got := versionLocalMCPPIDs(t, n); !slices.Equal(got, mcpBefore) {
		t.Fatalf("the MCP server processes changed from %v to %v; the session reconnected instead of recovering", mcpBefore, got)
	}
}

// versionLocalWithout returns procs without the process whose PID is pid.
func versionLocalWithout(procs []node.Proc, pid int) []node.Proc {
	out := procs[:0]
	for _, p := range procs {
		if p.PID != pid {
			out = append(out, p)
		}
	}
	return out
}
