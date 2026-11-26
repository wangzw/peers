//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/node"
)

// TestOtherAccountRejected checks that a user Broker serves no other account,
// neither through the filesystem nor through the account check.
func TestOtherAccountRejected(t *testing.T) {
	c := harness.Up(t, harness.TopoSingle)
	host := c.Node("hostA")

	peerUID := accountsUID(t, host, "id", "-u")
	socket := fmt.Sprintf("/tmp/peers-%d/broker.sock", peerUID)

	start := host.StartBroker(t, harness.BrokerSpec{})
	if !start.Running {
		t.Fatalf("the peer Broker did not start: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
	}

	owner := host.StartSession(t, harness.SessionSpec{Name: "owner", StartMCP: true})
	defer owner.End(t)
	harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "the owner session is registered", func(deadline time.Time) (bool, string) {
		out, state := accountsListAgentsPoll(t, deadline, owner)
		if state != "" {
			return false, state
		}
		return accountsAddressName(out.Self) == "owner", out.Self
	})

	t.Run("REQ-broker-config-019 filesystem layer", func(t *testing.T) {
		status := accountsRunAsOther(t, c, "hostA", "fs-status",
			map[string]string{"PEERS_TEST_SOCKET": socket}, "peers", "service", "status")
		if status.ExitCode != 1 {
			t.Fatalf("peers service status as other exited %d, want 1\nstdout: %s\nstderr: %s",
				status.ExitCode, status.Stdout, status.Stderr)
		}
		if !strings.Contains(status.Stderr, "cannot reach the Broker") {
			t.Fatalf("peers service status as other did not report the Broker unreachable: %s", status.Stderr)
		}
		if !strings.Contains(status.Stderr, "may not connect to it") {
			t.Fatalf("peers service status as other did not report the filesystem denial: %s", status.Stderr)
		}

		audit := accountsRunAsOther(t, c, "hostA", "fs-audit",
			map[string]string{"PEERS_TEST_SOCKET": socket}, "peers", "service", "audit")
		if audit.ExitCode != 1 {
			t.Fatalf("peers service audit as other exited %d, want 1\nstdout: %s\nstderr: %s",
				audit.ExitCode, audit.Stdout, audit.Stderr)
		}
		if !strings.Contains(audit.Stderr, "cannot reach the Broker") {
			t.Fatalf("peers service audit as other did not report the Broker unreachable: %s", audit.Stderr)
		}

		own := accountsRunAsOther(t, c, "hostA", "fs-own", nil, "peers", "service", "status")
		if own.ExitCode != 1 {
			t.Fatalf("peers service status as other without a socket entered %d, want 1\nstdout: %s\nstderr: %s",
				own.ExitCode, own.Stdout, own.Stderr)
		}
		if !strings.Contains(own.Stderr, "no Broker is available") {
			t.Fatalf("peers service status as other did not search only its own runtime directory: %s", own.Stderr)
		}
	})

	t.Run("REQ-broker-config-019 credential layer", func(t *testing.T) {
		before := accountsRunAsOther(t, c, "hostA", "cred-before",
			map[string]string{"PEERS_TEST_SOCKET": socket}, "peers", "service", "status")
		if !strings.Contains(before.Stderr, "may not connect to it") {
			t.Fatalf("the filesystem layer did not deny the connection first: %s", before.Stderr)
		}

		runtimeDir := fmt.Sprintf("/tmp/peers-%d", peerUID)
		c.Root(t, "hostA", "chmod", "0711", runtimeDir)
		c.Root(t, "hostA", "chmod", "0666", socket)
		t.Cleanup(func() {
			_, _ = c.RootOK(t, "hostA", "chmod", "0700", runtimeDir)
			_, _ = c.RootOK(t, "hostA", "chmod", "0600", socket)
		})

		after := accountsRunAsOther(t, c, "hostA", "cred-after",
			map[string]string{"PEERS_TEST_SOCKET": socket}, "peers", "service", "status")
		if after.ExitCode != 1 {
			t.Fatalf("peers service status as other exited %d after the filesystem was opened, want 1\nstdout: %s\nstderr: %s",
				after.ExitCode, after.Stdout, after.Stderr)
		}
		if !strings.Contains(after.Stderr, "cannot reach the Broker") {
			t.Fatalf("peers service status as other did not report the Broker unreachable: %s", after.Stderr)
		}
		if strings.Contains(after.Stderr, "may not connect to it") {
			t.Fatalf("peers service status still failed at the filesystem layer: %s", after.Stderr)
		}
		if !strings.Contains(after.Stderr, "not this account (2000)") {
			t.Fatalf("peers service status did not reject the other account's credentials: %s", after.Stderr)
		}

		intruder := accountsOneshotRejected(t, c, "hostA", socket, "intruder")
		if !strings.Contains(intruder.Text, "not this account (2000)") {
			t.Fatalf("the other account's list_agents did not report the credential rejection: %s", intruder.Text)
		}

		if log := host.BrokerLog(t); !accountsLogRecord(log, "local_connection_rejected", 2000) {
			t.Fatalf("the Broker log has no local_connection_rejected record carrying uid 2000:\n%s", log)
		}

		// The window is one second, twenty polls, which leaves room for a
		// registration that was still in flight after the command stopped the
		// MCP server process.
		harness.ConsistentlyPoll(t, 1*time.Second, harness.ListAgentsPollBound, "the intruder stays off the peer Broker", nil, func(deadline time.Time) harness.Observation {
			status, detail := host.Oracle().Status(t)
			if detail != nil {
				return harness.Observed(false, detail.GetMessage())
			}
			for _, agent := range status.Agents {
				if strings.Contains(agent.GetAddress(), "intruder") {
					return harness.Observed(false, "the Broker lists "+agent.GetAddress())
				}
			}
			out, state := accountsListAgentsPoll(t, deadline, owner)
			if state != "" {
				return harness.Unobservable(state)
			}
			for _, agent := range out.Agents {
				if strings.Contains(agent.Address, "intruder") {
					return harness.Observed(false, "the owner's list contains "+agent.Address)
				}
			}
			return harness.Observed(true, "")
		})
	})

	t.Run("REQ-broker-config-019 account paths", func(t *testing.T) {
		if got := strings.TrimSpace(c.Root(t, "hostA", "stat", "-c", "%u", "/home/peer")); got != strconv.Itoa(peerUID) {
			t.Fatalf("/home/peer is owned by uid %s, want the peer uid %d", got, peerUID)
		}
		if got := strings.TrimSpace(c.Root(t, "hostA", "stat", "-c", "%u", "/home/other")); got != "2000" {
			t.Fatalf("/home/other is owned by uid %s, want 2000", got)
		}
		if got := strings.TrimSpace(c.Root(t, "hostA", "find", "/home/other", "-maxdepth", "2", "-name", "mcp-*.log")); got == "" {
			t.Fatalf("/home/other holds no MCP log; the other account's session did not use its own home")
		}
		if got := strings.TrimSpace(c.Root(t, "hostA", "find", "/home/peer", "-user", "2000")); got != "" {
			t.Fatalf("files of uid 2000 are under /home/peer: %s", got)
		}
		runtimeDir := fmt.Sprintf("/tmp/peers-%d", peerUID)
		if got := strings.TrimSpace(c.Root(t, "hostA", "stat", "-c", "%u %a", runtimeDir)); got != fmt.Sprintf("%d 700", peerUID) {
			t.Fatalf("%s is %q, want a 0700 directory of uid %d", runtimeDir, got, peerUID)
		}
		if got := strings.TrimSpace(c.Root(t, "hostA", "find", runtimeDir, "-user", "2000")); got != "" {
			t.Fatalf("files of uid 2000 are under the peer runtime directory: %s", got)
		}
		if out, err := c.RootOK(t, "hostA", "stat", "-c", "%u", "/tmp/peers-2000"); err == nil {
			if got := strings.TrimSpace(out); got != "2000" {
				t.Fatalf("/tmp/peers-2000 is owned by uid %s, want 2000", got)
			}
		}
	})
}

type accountsCommand struct {
	ExitCode       int
	Stdout, Stderr string
}

// accountsRunAsOther runs argv as uid 2000 inside nodeName. The root exec
// returns one combined stream, so the streams are written to files under the
// control directory.
func accountsRunAsOther(t *testing.T, c *harness.Cluster, nodeName, tag string, env map[string]string, argv ...string) accountsCommand {
	t.Helper()
	outDir := filepath.Join(c.CTLDir(t, nodeName), "other")
	if err := os.MkdirAll(outDir, 0o777); err != nil {
		t.Fatalf("creating %s: %v", outDir, err)
	}
	if err := os.Chmod(outDir, 0o777); err != nil {
		t.Fatalf("making %s writable by the other account: %v", outDir, err)
	}

	base := "/ctl/other/" + tag
	script := `"$@" >"$E2E_OUT" 2>"$E2E_ERR"; printf '%s' $? >"$E2E_CODE"`
	rootArgv := []string{
		"setpriv", "--reuid=2000", "--regid=2000", "--init-groups", "--", "env",
		"HOME=/home/other",
		"PATH=/usr/local/bin:/usr/local/sbin:/usr/bin:/bin:/usr/sbin:/sbin",
		"E2E_OUT=" + base + ".out",
		"E2E_ERR=" + base + ".err",
		"E2E_CODE=" + base + ".code",
	}
	for name, value := range env {
		rootArgv = append(rootArgv, name+"="+value)
	}
	rootArgv = append(rootArgv, "sh", "-c", script, "sh")
	rootArgv = append(rootArgv, argv...)
	c.Root(t, nodeName, rootArgv...)

	read := func(suffix string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(outDir, tag+suffix))
		if err != nil {
			t.Fatalf("reading the other account's %s output: %v", suffix, err)
		}
		return string(data)
	}
	code := strings.TrimSpace(read(".code"))
	exitCode, err := strconv.Atoi(code)
	if err != nil {
		t.Fatalf("the other account's exit code %q is not a number", code)
	}
	return accountsCommand{ExitCode: exitCode, Stdout: read(".out"), Stderr: read(".err")}
}

func accountsOneshotRejected(t *testing.T, c *harness.Cluster, nodeName, socket, name string) node.ToolResult {
	t.Helper()
	var result node.ToolResult
	harness.Eventually(t, harness.EventTimeout, "oneshot-session reports the credential rejection", func() (bool, string) {
		// --env, not the process environment, carries the variables to the MCP
		// server process: oneshot-session builds that environment itself.
		out := accountsRunAsOther(t, c, nodeName, "oneshot", nil,
			"e2e-node", "oneshot-session", "--name", name, "--bin", "peers",
			"--env", "PEERS_TEST_SOCKET="+socket)
		if out.ExitCode != 0 {
			return false, fmt.Sprintf("oneshot-session exited %d: %s", out.ExitCode, out.Stderr)
		}
		if err := json.Unmarshal([]byte(out.Stdout), &result); err != nil {
			return false, fmt.Sprintf("decoding the ToolResult %q: %v", out.Stdout, err)
		}
		if !result.IsError {
			return false, "list_agents succeeded for the other account: " + result.Text
		}
		if strings.Contains(result.Text, "still being set up") {
			return false, result.Text
		}
		return true, ""
	})
	return result
}

func accountsLogRecord(data []byte, event string, uid int) bool {
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var record struct {
			Event string `json:"event"`
			UID   *int   `json:"uid"`
		}
		if json.Unmarshal([]byte(line), &record) != nil {
			continue
		}
		if record.Event == event && record.UID != nil && *record.UID == uid {
			return true
		}
	}
	return false
}

func accountsUID(t *testing.T, host *harness.Node, argv ...string) int {
	t.Helper()
	res := host.Exec(t, argv, nil)
	if res.ExitCode != 0 {
		t.Fatalf("%v exited %d: %s", argv, res.ExitCode, res.Stderr)
	}
	uid, err := strconv.Atoi(strings.TrimSpace(res.Stdout))
	if err != nil {
		t.Fatalf("%v printed %q, not an account id", argv, res.Stdout)
	}
	return uid
}

func accountsListAgentsPoll(t *testing.T, deadline time.Time, s *harness.Session) (mcpserver.ListAgentsOut, string) {
	t.Helper()
	return pollDecode[mcpserver.ListAgentsOut](t, deadline, s, "list_agents", nil)
}

func accountsAddressName(address string) string {
	name, _, _ := strings.Cut(address, "@")
	return name
}
