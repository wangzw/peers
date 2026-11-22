//go:build e2e

package e2e

import (
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/test/e2e/harness"
)

// TestDegradedRun covers --allow-degraded on a container: the foreground
// failures that stay fatal, the two degraded levels, the retries that recover
// without a restart, and the service-independent listen backoff.
func TestDegradedRun(t *testing.T) {
	c := harness.Up(t, harness.TopoFlat(2))
	host := c.Node("hostA")

	identity := filepath.Join(host.AccountHome(), ".peers", "identity.key")
	audit := filepath.Join(host.AccountHome(), ".peers", "audit.log")

	drConfig := func(listen string) string {
		return harness.ConfigYAML("broker:\n  listen: \"" + listen + "\"")
	}
	drNoSocket := func(t *testing.T) {
		t.Helper()
		res := host.Exec(t, []string{"sh", "-c", `test ! -S "$PEERS_TEST_RUNTIME_ROOT/$(id -u)/broker.sock"`}, nil)
		if res.ExitCode != 0 {
			t.Fatalf("a local socket was left behind: %s", strings.TrimSpace(res.Stderr))
		}
	}
	drStop := func(t *testing.T) {
		t.Helper()
		if code := host.StopBroker(t, syscall.SIGTERM); code != 0 {
			t.Fatalf("the degraded Broker exited %d on SIGTERM, want 0", code)
		}
	}
	// drExec bounds every one-off command, so a Broker that unexpectedly stays
	// in the foreground fails the test instead of hanging it.
	drExec := func(t *testing.T, argv []string, env map[string]string) harness.ExecResult {
		t.Helper()
		return host.ExecTimeout(t, argv, env, 30*time.Second)
	}
	drLastLine := func(stderr string) string {
		lines := strings.Split(strings.TrimRight(stderr, "\n"), "\n")
		return strings.TrimSpace(lines[len(lines)-1])
	}
	drWaitStatus := func(t *testing.T, what string, want func(*localpb.StatusResponse) bool, bound time.Duration) *localpb.StatusResponse {
		t.Helper()
		var got *localpb.StatusResponse
		harness.EventuallyPoll(t, bound, what, func(time.Time) (bool, string) {
			resp, detail := host.Oracle().Status(t)
			if detail != nil {
				return false, detail.GetMessage()
			}
			got = resp
			if !want(resp) {
				return false, fmt.Sprintf("degraded = %+v", resp.GetDegraded())
			}
			return true, ""
		})
		return got
	}

	t.Run("REQ-broker-config-056 foreground failures exit", func(t *testing.T) {
		// An unparsable configuration is a fatal configuration failure, and the
		// last standard-error line names the reason.
		host.WriteConfig(t, "broker: [unclosed\n")
		res := drExec(t, []string{"/usr/local/bin/peers", "service", "run", "--user"}, nil)
		if res.ExitCode == 0 {
			t.Fatalf("a Broker with an unparsable configuration exited 0: %s", res.Stderr)
		}
		if last := drLastLine(res.Stderr); !strings.Contains(last, "parse error") {
			t.Fatalf("the last standard-error line is %q, want the parse failure", last)
		}
		drNoSocket(t)

		// --allow-degraded does not cover a configuration that cannot be
		// parsed: the diagnostics interface must not run on untrusted values.
		res = drExec(t, []string{"/usr/local/bin/peers", "service", "run", "--user", "--allow-degraded"}, nil)
		if res.ExitCode == 0 {
			t.Fatalf("a degraded Broker with an unparsable configuration exited 0: %s", res.Stderr)
		}
		if last := drLastLine(res.Stderr); !strings.Contains(last, "parse error") {
			t.Fatalf("the last standard-error line is %q, want the parse failure", last)
		}
		drNoSocket(t)

		// A taken listen address is the listen exit code and leaves no socket.
		host.WriteConfig(t, drConfig(":17443"))
		held := c.HoldPort(t, "hostA", ":17443")
		res = drExec(t, []string{"/usr/local/bin/peers", "service", "run", "--user"}, nil)
		if res.ExitCode != 4 {
			t.Fatalf("the Broker exited %d with 17443 held, want 4: %s", res.ExitCode, res.Stderr)
		}
		if last := drLastLine(res.Stderr); !strings.Contains(last, "unavailable") {
			t.Fatalf("the last standard-error line is %q, want the unavailable listen address", last)
		}
		drNoSocket(t)
		c.ReleasePort(t, "hostA", held)

		// An unreadable identity fails startup too.
		start := host.StartBroker(t, harness.BrokerSpec{})
		if !start.Running {
			t.Fatalf("the Broker used to create an identity did not start: exit %d: %s", start.ExitCode, start.StderrLast)
		}
		drStop(t)
		drChmod(t, host, identity, "000")
		t.Cleanup(func() { drChmod(t, host, identity, "600") })
		res = drExec(t, []string{"/usr/local/bin/peers", "service", "run", "--user"}, nil)
		if res.ExitCode == 0 {
			t.Fatalf("a Broker with an unreadable identity exited 0: %s", res.Stderr)
		}
		if last := drLastLine(res.Stderr); !strings.Contains(last, "identity") {
			t.Fatalf("the last standard-error line is %q, want the identity failure", last)
		}
		drNoSocket(t)
	})

	t.Run("REQ-broker-config-060 before-lock failures exit", func(t *testing.T) {
		// Inside a session's process tree.
		res := host.Exec(t, []string{"/usr/local/bin/peers", "service", "run", "--user", "--allow-degraded"},
			map[string]string{"CLAUDE_CODE_MESSAGING_SOCKET": "/tmp/inbox.sock"})
		if res.ExitCode != 6 {
			t.Fatalf("a degraded Broker inside a session exited %d, want 6: %s", res.ExitCode, res.Stderr)
		}

		// The system form without root.
		res = host.Exec(t, []string{"/usr/local/bin/peers", "service", "run", "--system", "--allow-degraded"}, nil)
		if res.ExitCode != 7 {
			t.Fatalf("a non-root system Broker exited %d, want 7: %s", res.ExitCode, res.Stderr)
		}

		// A second instance loses the account lock.
		host.WriteConfig(t, drConfig(":17443"))
		first := host.StartBroker(t, harness.BrokerSpec{})
		if !first.Running {
			t.Fatalf("the first Broker did not start: exit %d: %s", first.ExitCode, first.StderrLast)
		}
		res = host.Exec(t, []string{"/usr/local/bin/peers", "service", "run", "--user", "--allow-degraded"}, nil)
		if res.ExitCode != 3 {
			t.Fatalf("a second degraded Broker exited %d, want 3: %s", res.ExitCode, res.Stderr)
		}
		host.Oracle().Hello(t)
		drStop(t)
	})

	t.Run("REQ-broker-config-057 diagnostics entry and recovery", func(t *testing.T) {
		drChmod(t, host, identity, "000")
		host.WriteConfig(t, drConfig(":17443"))
		start := host.StartBroker(t, harness.BrokerSpec{Args: []string{"--allow-degraded"}})
		if !start.Running {
			t.Fatalf("the degraded Broker did not answer: exit %d: %s", start.ExitCode, start.StderrLast)
		}

		resp := drWaitStatus(t, "the Broker reports diagnostics only", func(s *localpb.StatusResponse) bool {
			return s.GetDegraded() != nil &&
				s.GetDegraded().GetLevel() == localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY
		}, 10*time.Second)
		if !strings.Contains(resp.GetDegraded().GetReason(), "identity") {
			t.Fatalf("reason %q does not name the identity", resp.GetDegraded().GetReason())
		}

		// Every call except Hello and Status is refused.
		_, detail := host.Oracle().ListBrokers(t)
		if detail == nil || detail.GetCode() != localpb.ErrorCode_ERROR_CODE_BROKER_DEGRADED {
			t.Fatalf("ListBrokers in diagnostics = %+v, want broker_degraded", detail)
		}

		// Fix the file and touch the configuration; the Broker retries in
		// place and keeps its process.
		drChmod(t, host, identity, "600")
		host.WriteConfig(t, drConfig(":17443"))
		drWaitStatus(t, "the Broker leaves degraded mode", func(s *localpb.StatusResponse) bool {
			return s.GetDegraded() == nil
		}, 30*time.Second)
		if res := host.Exec(t, []string{"kill", "-0", strconv.Itoa(start.PID)}, nil); res.ExitCode != 0 {
			t.Fatalf("the Broker process %d did not survive recovery", start.PID)
		}
		drStop(t)
	})

	t.Run("REQ-broker-config-059 diagnostics from the audit log", func(t *testing.T) {
		if res := host.Exec(t, []string{"rm", "-f", audit}, nil); res.ExitCode != 0 {
			t.Fatalf("removing the audit log: %s", res.Stderr)
		}
		if res := host.Exec(t, []string{"mkdir", audit}, nil); res.ExitCode != 0 {
			t.Fatalf("replacing the audit log with a directory: %s", res.Stderr)
		}
		t.Cleanup(func() { _ = host.Exec(t, []string{"rmdir", audit}, nil) })
		host.WriteConfig(t, drConfig(":17443"))

		start := host.StartBroker(t, harness.BrokerSpec{Args: []string{"--allow-degraded"}})
		if !start.Running {
			t.Fatalf("the degraded Broker did not answer: exit %d: %s", start.ExitCode, start.StderrLast)
		}
		resp := drWaitStatus(t, "the audit log failure is reported", func(s *localpb.StatusResponse) bool {
			return s.GetDegraded() != nil &&
				s.GetDegraded().GetLevel() == localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY
		}, 10*time.Second)
		if !strings.Contains(resp.GetDegraded().GetReason(), "audit") {
			t.Fatalf("reason %q does not name the audit log", resp.GetDegraded().GetReason())
		}
		if !strings.Contains(resp.GetDegraded().GetFix(), audit) {
			t.Fatalf("fix %q does not name the audit log path", resp.GetDegraded().GetFix())
		}

		// The session-facing calls fail with the reason and the
		// broker_degraded code.
		session := host.StartSession(t, harness.SessionSpec{Name: "diag", StartMCP: true})
		defer session.End(t)
		for _, call := range []struct {
			tool string
			args map[string]any
		}{
			{"list_agents", nil},
			{"send_message", map[string]any{"to": "nobody@hostA.peer", "message": "while degraded"}},
			{"get_config", nil},
		} {
			harness.EventuallyPoll(t, 10*time.Second, "the session reports "+call.tool+" as degraded", func(deadline time.Time) (bool, string) {
				r, err := session.PollCall(t, deadline, call.tool, call.args)
				if err != nil {
					return false, err.Error()
				}
				if !r.IsError || r.Code() != "broker_degraded" {
					return false, "result = " + r.Text
				}
				if !strings.Contains(r.Text, "audit") {
					return false, call.tool + " text does not carry the reason: " + r.Text
				}
				return true, ""
			})
		}

		// The status command prints the diagnostics-only line with the same
		// reason the Status call reported, names the audit log in its fix, and
		// exits non-zero without repeating the reason on standard error.
		status := drExec(t, []string{"/usr/local/bin/peers", "service", "status"}, nil)
		if status.ExitCode == 0 {
			t.Fatalf("peers service status exited 0 while diagnostics only: %s", status.Stdout)
		}
		if status.Stderr != "" {
			t.Fatalf("peers service status stderr = %q, want empty", status.Stderr)
		}
		wantLine := "Broker: degraded (diagnostics only): " + resp.GetDegraded().GetReason()
		if !strings.Contains(status.Stdout, wantLine) {
			t.Fatalf("status stdout = %q, want the line %q", status.Stdout, wantLine)
		}
		for _, want := range []string{audit, "Fix:"} {
			if !strings.Contains(status.Stdout, want) {
				t.Fatalf("status stdout = %q, want it to contain %q", status.Stdout, want)
			}
		}

		if res := host.Exec(t, []string{"rmdir", audit}, nil); res.ExitCode != 0 {
			t.Fatalf("fixing the audit log path: %s", res.Stderr)
		}
		host.WriteConfig(t, drConfig(":17443"))
		drWaitStatus(t, "the Broker leaves degraded mode", func(s *localpb.StatusResponse) bool {
			return s.GetDegraded() == nil
		}, 30*time.Second)

		// The same session reconnects after the degraded service closed its
		// connections; its next call succeeds.
		harness.EventuallyPoll(t, 30*time.Second, "the same session lists agents after recovery", func(deadline time.Time) (bool, string) {
			r, err := session.PollCall(t, deadline, "list_agents", nil)
			if err != nil {
				return false, err.Error()
			}
			if r.IsError {
				return false, r.Text
			}
			return true, ""
		})
		drStop(t)
	})

	t.Run("REQ-broker-config-058 local only keeps local features", func(t *testing.T) {
		hostB := c.Node("hostB")
		aConfig := srConfig("broker:\n  listen: \":17443\"")
		bConfig := srConfig()

		// Pair first while hostA can listen. hostA dials hostB, so the link
		// survives hostA's restart local only and the topic roster can be
		// rebuilt.
		startA := host.StartBroker(t, harness.BrokerSpec{ConfigYAML: aConfig})
		if !startA.Running {
			t.Fatalf("hostA's Broker did not start: exit %d: %s", startA.ExitCode, startA.StderrLast)
		}
		srStartBroker(t, hostB, bConfig)
		opA, opB := srOperator(t, host), srOperator(t, hostB)
		defer opA.End(t)
		defer opB.End(t)
		alpha := srSession(t, host, "alpha")
		defer alpha.End(t)
		beta := srSession(t, hostB, "beta")
		defer beta.End(t)
		key := srCreateKey(t, opB)
		// This topology connects the nodes directly, so hostB's advertised
		// address is its address on the shared network.
		bAddr := net.JoinHostPort(c.Addr("hostB", "net"), "17443")
		srConnect(t, opA, bAddr, key.Key).MustOK(t)
		srWaitConnected(t, host, "hostB.peer", srRelinkBound)
		srWaitConnected(t, hostB, "hostA.peer", srRelinkBound)
		srWaitSessionVisible(t, alpha, "beta")

		// Restart hostA with its listen address held: local only, no listener.
		if code := host.StopBroker(t, syscall.SIGTERM); code != 0 {
			t.Fatalf("hostA's healthy Broker exited %d", code)
		}
		held := c.HoldPort(t, "hostA", ":17443")
		defer c.ReleasePort(t, "hostA", held)
		start := host.StartBroker(t, harness.BrokerSpec{ConfigYAML: aConfig, Args: []string{"--allow-degraded"}})
		if !start.Running {
			t.Fatalf("the local-only Broker did not answer: exit %d: %s", start.ExitCode, start.StderrLast)
		}
		resp := drWaitStatus(t, "the Broker reports local only", func(s *localpb.StatusResponse) bool {
			return s.GetDegraded() != nil &&
				s.GetDegraded().GetLevel() == localpb.DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY
		}, 10*time.Second)
		for _, want := range []string{":17443", "broker.listen"} {
			if !strings.Contains(resp.GetDegraded().GetReason()+resp.GetDegraded().GetFix(), want) {
				t.Fatalf("degraded text %+v does not contain %q", resp.GetDegraded(), want)
			}
		}

		// The status command prints the local-only line and the address that
		// could not be bound.
		status := drExec(t, []string{"/usr/local/bin/peers", "service", "status"}, nil)
		if status.ExitCode != 0 {
			t.Fatalf("peers service status in local-only degradation: exit %d\nstdout: %s\nstderr: %s",
				status.ExitCode, status.Stdout, status.Stderr)
		}
		for _, want := range []string{"Broker: degraded (local only):", ":17443"} {
			if !strings.Contains(status.Stdout, want) {
				t.Fatalf("status stdout = %q, want it to contain %q", status.Stdout, want)
			}
		}

		// The Broker itself listens nowhere.
		ss := host.Exec(t, []string{"ss", "-ltnp"}, nil)
		if ss.ExitCode != 0 {
			t.Fatalf("ss -ltnp exited %d: %s", ss.ExitCode, strings.TrimSpace(ss.Stderr))
		}
		for _, line := range bpListenLines(ss.Stdout) {
			if strings.Contains(line, ":17443") && strings.Contains(line, "peers") {
				t.Fatalf("the local-only Broker still listens: %q", line)
			}
		}

		// Local sessions still register, join topics and send to each other.
		alphaOut := slWaitRegistered(t, alpha, "alpha")
		local := srSession(t, host, "local")
		defer local.End(t)
		slWaitVisible(t, alpha, "local")
		slJoin(t, alpha, "degraded-topic")
		harness.EventuallyPoll(t, harness.TopicCallWaitBound, "alpha lists the joined topic", func(deadline time.Time) (bool, string) {
			out, state := pollDecode[mcpserver.ListTopicsOut](t, deadline, alpha, "list_topics", nil)
			if state != "" {
				return false, state
			}
			for _, name := range out.Joined {
				if name == "degraded-topic" {
					return true, ""
				}
			}
			return false, fmt.Sprintf("joined topics are %v", out.Joined)
		})

		r := alpha.Call(t, "create_pairing_key", nil)
		if !r.IsError {
			t.Fatalf("create_pairing_key in local-only mode returned %q, want an error", r.Text)
		}
		for _, want := range []string{":17443", "broker.listen"} {
			if !strings.Contains(r.Text, want) {
				t.Fatalf("create_pairing_key text %q does not contain %q", r.Text, want)
			}
		}

		localAddress := "local@" + slAlias(t, alphaOut.Self)
		send := alpha.Call(t, "send_message", map[string]any{"to": localAddress, "message": "local only works"})
		send.MustOK(t)
		frame := local.WaitFrame(t, harness.EventTimeout)
		if body := slEnvelopeBody(t, frame.Content); body != "local only works" {
			t.Fatalf("the delivered message is %q", body)
		}

		// hostA is the dialer, so a cut makes the link unreachable while the
		// missing listener is not the cause. The error must not carry the
		// local-only reason; the acceptor-side reason is unit-tested, since a
		// restarted local-only Broker has an empty topic roster.
		srWaitConnected(t, host, "hostB.peer", srRelinkBound)
		c.Cut(t, "hostA", "hostB")
		t.Cleanup(func() { c.Restore(t) })
		harness.Eventually(t, srRelinkBound, "hostA notices the cut", func() (bool, string) {
			if srPeerConnected(t, host, "hostB.peer") {
				return false, "hostA still reports the link connected"
			}
			return true, ""
		})
		unreachable := alpha.Call(t, "send_message", map[string]any{"to": "beta@hostB.peer", "message": "after the cut"})
		unreachable.MustCode(t, "peer_unreachable")
		if reason := resp.GetDegraded().GetReason(); strings.Contains(unreachable.Text, reason) {
			t.Fatalf("a dialer-side peer_unreachable carried the acceptor's degraded reason: %q", unreachable.Text)
		}
		c.Restore(t)
		srWaitConnected(t, host, "hostB.peer", srRelinkBound)

		// Changing broker.listen to a free port recovers without a restart.
		host.WriteConfig(t, drConfig(":17445"))
		drWaitStatus(t, "the Broker leaves degraded mode after a listen change", func(s *localpb.StatusResponse) bool {
			return s.GetDegraded() == nil
		}, 30*time.Second)
		drStop(t)
	})

	t.Run("REQ-broker-config-061 recovery listens on the new port", func(t *testing.T) {
		host.WriteConfig(t, drConfig(":17443"))
		held := c.HoldPort(t, "hostA", ":17443")
		start := host.StartBroker(t, harness.BrokerSpec{Args: []string{"--allow-degraded"}})
		if !start.Running {
			t.Fatalf("the local-only Broker did not answer: exit %d: %s", start.ExitCode, start.StderrLast)
		}
		drWaitStatus(t, "the Broker reports local only", func(s *localpb.StatusResponse) bool {
			return s.GetDegraded() != nil &&
				s.GetDegraded().GetLevel() == localpb.DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY
		}, 10*time.Second)

		// A new free port recovers without a restart, and the new address
		// accepts a connection from another host.
		host.WriteConfig(t, drConfig(":17447"))
		drWaitStatus(t, "the Broker leaves degraded mode after a listen change", func(s *localpb.StatusResponse) bool {
			return s.GetDegraded() == nil
		}, 30*time.Second)
		addr := net.JoinHostPort(c.Addr("hostA", "net"), "17447")
		harness.Eventually(t, 10*time.Second, "hostB reaches 17447 after recovery", func() (bool, string) {
			if !c.TCPReachable(t, "hostB", addr, 2*time.Second) {
				return false, "hostB cannot reach " + addr
			}
			return true, ""
		})
		if res := host.Exec(t, []string{"kill", "-0", strconv.Itoa(start.PID)}, nil); res.ExitCode != 0 {
			t.Fatalf("the Broker process %d did not survive recovery", start.PID)
		}
		c.ReleasePort(t, "hostA", held)
		drStop(t)
	})

	t.Run("REQ-broker-config-062 listen retry reaches the address", func(t *testing.T) {
		host.WriteConfig(t, drConfig(":17443"))
		held := c.HoldPort(t, "hostA", ":17443")
		start := host.StartBroker(t, harness.BrokerSpec{Args: []string{"--allow-degraded"}})
		if !start.Running {
			t.Fatalf("the local-only Broker did not answer: exit %d: %s", start.ExitCode, start.StderrLast)
		}
		drWaitStatus(t, "the Broker reports local only", func(s *localpb.StatusResponse) bool {
			return s.GetDegraded() != nil &&
				s.GetDegraded().GetLevel() == localpb.DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY
		}, 10*time.Second)

		c.ReleasePort(t, "hostA", held)
		addr := net.JoinHostPort(c.Addr("hostA", "net"), "17443")
		harness.Eventually(t, 2*time.Minute, "hostB reaches 17443 after the port is released", func() (bool, string) {
			if !c.TCPReachable(t, "hostB", addr, 2*time.Second) {
				return false, "hostB cannot reach " + addr
			}
			resp, detail := host.Oracle().Status(t)
			if detail != nil {
				return false, detail.GetMessage()
			}
			if resp.GetDegraded() != nil {
				return false, "the Broker still reports degraded"
			}
			return true, ""
		})
		if res := host.Exec(t, []string{"kill", "-0", strconv.Itoa(start.PID)}, nil); res.ExitCode != 0 {
			t.Fatalf("the Broker process %d did not survive the retry", start.PID)
		}
		drStop(t)
	})
}

func drChmod(t *testing.T, host *harness.Node, path, mode string) {
	t.Helper()
	res := host.Exec(t, []string{"chmod", mode, path}, nil)
	if res.ExitCode != 0 {
		t.Fatalf("chmod %s %s: %s", mode, path, res.Stderr)
	}
}
