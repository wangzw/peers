//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/test/e2e/ctl"
	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/node"
)

// Broker process timing. brokerReadyBound is the harness readiness bound plus
// three seconds. brokerRecoverBound is the MCP server's longest connection
// backoff (5s) plus the same margin.
const (
	brokerReadyBound   = 13 * time.Second
	brokerRecoverBound = 5*time.Second + brokerReadyBound

	// brokerDefaultListen is broker.listen's default value.
	brokerDefaultListen = ":17443"

	// brokerAbsenceWindow is how long a failed Broker start is watched for a
	// process it may have left behind. The failed process has already exited,
	// so half a second is long enough for any other process to show up in
	// /proc, and long compared with the lock and socket setup.
	brokerAbsenceWindow = 500 * time.Millisecond
)

// TestBrokerProcessLocal covers the user Broker process on the test
// host: a session with no Broker, the account lock that keeps a single
// instance, the --listen override, and the process exit codes and standard
// error line.
func TestBrokerProcessLocal(t *testing.T) {
	t.Run("REQ-broker-config-038 no Broker until started", func(t *testing.T) {
		host := harness.NewLocalHost(t, harness.LocalOptions{Name: "nobroker"})
		listen := host.FreeAddr(t)
		host.WriteConfig(t, "broker:\n  listen: "+listen+"\n")

		session := host.StartSession(t, harness.SessionSpec{Name: "solo", StartMCP: true})

		// The MCP server process must not start a Broker of its own.
		harness.Consistently(t, brokerAbsenceWindow, "no Broker process appears", func() (bool, string) {
			if n := len(bpLocalProcs(t, host, "broker")); n != 0 {
				return false, fmt.Sprintf("%d Broker processes", n)
			}
			return true, ""
		})

		harness.EventuallyPoll(t, brokerReadyBound, "list_agents names both ways to start a Broker", func(deadline time.Time) (bool, string) {
			r, err := session.PollCall(t, deadline, "list_agents", nil)
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
			return true, ""
		})

		mcpProcs := bpLocalProcs(t, host, "mcp")
		if len(mcpProcs) != 1 {
			t.Fatalf("the node has %d MCP server processes, want 1: %+v", len(mcpProcs), mcpProcs)
		}
		mcpPID := mcpProcs[0].PID

		// Starting the Broker in a terminal is enough: the session registers
		// without reconnecting the MCP server process.
		start := host.StartBroker(t, harness.BrokerSpec{})
		if !start.Running {
			t.Fatalf("the explicit Broker did not start: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
		}
		harness.EventuallyPoll(t, brokerRecoverBound, "list_agents succeeds after the Broker starts", func(deadline time.Time) (bool, string) {
			r, err := session.PollCall(t, deadline, "list_agents", nil)
			if err != nil {
				return false, err.Error()
			}
			if r.IsError {
				return false, r.Text
			}
			return true, ""
		})
		after := bpLocalProcs(t, host, "mcp")
		if len(after) != 1 || after[0].PID != mcpPID {
			t.Fatalf("the MCP server process changed from %d to %+v; the session reconnected instead of recovering", mcpPID, after)
		}
	})

	t.Run("REQ-broker-config-013 single instance", func(t *testing.T) {
		host := harness.NewLocalHost(t, harness.LocalOptions{Name: "single"})
		listen := host.FreeAddr(t)
		host.WriteConfig(t, "broker:\n  listen: "+listen+"\n")

		// Five terminal starts race for the account lock. One keeps running and
		// the other four exit with the already-running code.
		bin := harness.BuildBinary(t, runtime.GOOS, runtime.GOARCH, "peers_testenv")
		out := t.TempDir()
		host.Exec(t, []string{"sh", "-c", bpConcurrentStartScript, "sh", bin, listen, out}, nil)

		codes, err := filepath.Glob(filepath.Join(out, "*.code"))
		if err != nil {
			t.Fatalf("listing the concurrent start results: %v", err)
		}
		if len(codes) != 4 {
			t.Fatalf("%d of the five starts exited, want 4 with the winner still running: %v", len(codes), codes)
		}
		for _, path := range codes {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("reading %s: %v", path, err)
			}
			if got := strings.TrimSpace(string(data)); got != strconv.Itoa(3) {
				t.Fatalf("%s = %q, want exit code 3", filepath.Base(path), got)
			}
			stderr, err := os.ReadFile(strings.TrimSuffix(path, ".code") + ".err")
			if err != nil {
				t.Fatalf("reading the standard error of %s: %v", path, err)
			}
			if !strings.Contains(string(stderr), "already running") {
				t.Fatalf("%s: standard error = %q, want it to say already running", path, stderr)
			}
		}

		procs := bpLocalProcs(t, host, "broker")
		if len(procs) != 1 {
			t.Fatalf("the node has %d Broker processes after five concurrent starts, want exactly 1: %+v", len(procs), procs)
		}
		winner := procs[0]

		sessions := make([]*harness.Session, 5)
		for i := range sessions {
			sessions[i] = host.StartSession(t, harness.SessionSpec{Name: fmt.Sprintf("worker-%d", i), StartMCP: true})
		}
		harness.EventuallyPoll(t, brokerReadyBound, "the five sessions register on the one Broker", func(deadline time.Time) (bool, string) {
			for i, session := range sessions {
				r, err := session.PollCall(t, deadline, "list_agents", nil)
				if err != nil {
					return false, fmt.Sprintf("session worker-%d: %s", i, err.Error())
				}
				if r.IsError {
					return false, fmt.Sprintf("session worker-%d: %s", i, r.Text)
				}
			}
			return true, ""
		})

		resp, detail := host.Oracle().Status(t)
		if detail != nil {
			t.Fatalf("Status returned %s: %s", detail.GetCode(), detail.GetMessage())
		}
		if got := len(resp.GetAgents()); got != 5 {
			t.Fatalf("the Broker lists %d agents, want 5", got)
		}

		// A second start with another listen address still fails on the account
		// lock and leaves the first Broker untouched.
		other := host.FreeAddr(t)
		second := host.Exec(t, []string{bin, "service", "run", "--user", "--listen", other}, nil)
		if second.ExitCode != 3 {
			t.Fatalf("a second Broker exited %d, want 3: %s", second.ExitCode, strings.TrimSpace(second.Stderr))
		}
		if last := bpLastLine(second.Stderr); !strings.Contains(last, "already running") {
			t.Fatalf("the second Broker's last standard error line is %q, want it to contain %q", last, "already running")
		}
		harness.Consistently(t, brokerAbsenceWindow, "the failed second start leaves exactly one Broker", func() (bool, string) {
			if n := len(bpLocalProcs(t, host, "broker")); n != 1 {
				return false, fmt.Sprintf("%d Broker processes", n)
			}
			return true, ""
		})
		if hello := host.Oracle().Hello(t); hello == nil {
			t.Fatal("the running Broker no longer answers Hello after the second start failed")
		}

		kill := host.Exec(t, []string{"kill", "-TERM", strconv.Itoa(winner.PID)}, nil)
		if kill.ExitCode != 0 {
			t.Fatalf("stopping the Broker %d: exit code %d: %s", winner.PID, kill.ExitCode, strings.TrimSpace(kill.Stderr))
		}
		harness.Eventually(t, 3*time.Second, "the Broker exits after SIGTERM", func() (bool, string) {
			if n := len(bpLocalProcs(t, host, "broker")); n != 0 {
				return false, fmt.Sprintf("%d Broker processes", n)
			}
			return true, ""
		})
	})

	t.Run("REQ-broker-config-025 listen flag", func(t *testing.T) {
		host := harness.NewLocalHost(t, harness.LocalOptions{Name: "listen"})
		configured := host.FreeAddr(t)
		override := host.FreeAddr(t)
		host.WriteConfig(t, "broker:\n  listen: "+configured+"\n")
		before := host.ReadConfig(t)

		start := host.StartBroker(t, harness.BrokerSpec{Args: []string{"--listen", override}})
		if !start.Running {
			t.Fatalf("the Broker did not start: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
		}

		// The one retry rewrites the FreeAddr addresses it finds in the
		// arguments and in the configuration file. Check the listener and the
		// file the product leaves behind against those actual addresses; the
		// assertion itself is unchanged: --listen must not touch the file and
		// only the override may be listening.
		override = start.Address(override)
		configured = start.Address(configured)
		expectedConfig := string(before)
		for old, actual := range start.Addrs {
			expectedConfig = strings.ReplaceAll(expectedConfig, old, actual)
		}

		addrs, err := node.ListeningAddrs(start.PID)
		if err != nil {
			t.Fatalf("reading the Broker's listening addresses: %v", err)
		}
		if !bpContainsString(addrs, override) {
			t.Fatalf("the Broker listens on %v, want %s", addrs, override)
		}
		if bpContainsString(addrs, configured) {
			t.Fatalf("the Broker listens on the configured address %s although --listen named %s: %v", configured, override, addrs)
		}

		if after := host.ReadConfig(t); string(after) != expectedConfig {
			t.Fatalf("--listen changed the configuration file:\nbefore %q\nafter  %q", expectedConfig, after)
		}

		if code := host.StopBroker(t, syscall.SIGTERM); code != 0 {
			t.Fatalf("the Broker exited %d on SIGTERM, want 0", code)
		}
		if after := host.ReadConfig(t); string(after) != expectedConfig {
			t.Fatalf("shutdown changed the configuration file:\nbefore %q\nafter  %q", expectedConfig, after)
		}
	})
}

// bpConcurrentStartScript starts five Brokers on one address, waits until four
// have exited, and leaves the winner running. Each exit code goes to $out/i.code
// and each standard error to $out/i.err; the process that wins never writes
// either file while this script runs.
const bpConcurrentStartScript = `
bin=$1; listen=$2; out=$3
i=1
while [ $i -le 5 ]; do
  ( "$bin" service run --listen "$listen" >/dev/null 2>"$out/$i.err"; echo $? >"$out/$i.code" ) </dev/null >/dev/null 2>&1 &
  i=$((i+1))
done
n=0
while [ $n -lt 100 ]; do
  c=0
  for f in "$out"/*.code; do [ -e "$f" ] && c=$((c+1)); done
  [ $c -ge 4 ] && exit 0
  n=$((n+1))
  sleep 0.05
done
exit 1
`

// TestBrokerProcessInContainer covers the default listen address, the Broker's
// unavailability while that address is taken, recovery without reconnecting
// the MCP server process, and the structured runtime log. The subtests share
// one node and run in order.
func TestBrokerProcessInContainer(t *testing.T) {
	c := harness.Up(t, harness.TopoSingle)
	host := c.Node("hostA")

	var (
		session     *harness.Session
		heldPort    string
		mcpPID      int
		brokerStart harness.BrokerStart
	)

	t.Run("REQ-broker-config-024 default address taken", func(t *testing.T) {
		heldPort = c.HoldPort(t, "hostA", brokerDefaultListen)

		start := host.StartBroker(t, harness.BrokerSpec{})
		if start.Running {
			t.Fatalf("the Broker started although %s was held; pid %d", brokerDefaultListen, start.PID)
		}
		if start.ExitCode != 4 {
			t.Fatalf("the Broker exited %d, want 4: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
		}
		if !strings.Contains(start.StderrLast, brokerDefaultListen) {
			t.Fatalf("the Broker's last standard error line is %q, want it to name %s", start.StderrLast, brokerDefaultListen)
		}
		if !strings.Contains(start.StderrLast, "is unavailable") {
			t.Fatalf("the Broker's last standard error line is %q, want it to say the address is unavailable", start.StderrLast)
		}

		harness.Consistently(t, brokerAbsenceWindow, "no Broker process lingers after the failed start", func() (bool, string) {
			if n := len(bpContainerProcs(t, c, "hostA", "broker")); n != 0 {
				return false, fmt.Sprintf("%d Broker processes", n)
			}
			return true, ""
		})

		// Run ss as the node account: inside the container root lacks
		// CAP_SYS_PTRACE, so ss -p would omit the owner of a listener held by
		// the peer account. The same account reads its own process.
		ss := host.Exec(t, []string{"ss", "-ltnp"}, nil)
		if ss.ExitCode != 0 {
			t.Fatalf("ss -ltnp exited %d: %s", ss.ExitCode, strings.TrimSpace(ss.Stderr))
		}
		listeners := bpListenLines(ss.Stdout)
		if len(listeners) != 1 {
			t.Fatalf("the container has %d listening TCP sockets, want only the node agent's: %v", len(listeners), listeners)
		}
		if !strings.Contains(listeners[0], brokerDefaultListen) {
			t.Fatalf("the only listener is %q, want it on %s", listeners[0], brokerDefaultListen)
		}
		if !strings.Contains(listeners[0], "e2e-node") {
			t.Fatalf("the listener on %s belongs to %q, want the node agent", brokerDefaultListen, listeners[0])
		}

		// The MCP handshake does not depend on the Broker, so the session
		// starts and lists its tools even while no Broker can listen.
		session = host.StartSession(t, harness.SessionSpec{Name: "s", StartMCP: true})
		if tools := session.Tools(t); len(tools) != 13 {
			t.Fatalf("the MCP server exposes %d tools, want 13: %v", len(tools), tools)
		}

		harness.EventuallyPoll(t, brokerReadyBound, "list_agents names the ways to start a Broker", func(deadline time.Time) (bool, string) {
			r, err := session.PollCall(t, deadline, "list_agents", nil)
			if err != nil {
				return false, err.Error()
			}
			if !r.IsError {
				return false, "list_agents succeeded while every address was taken"
			}
			for _, want := range []string{"no Broker is available", "peers service install", "peers service run"} {
				if !strings.Contains(r.Text, want) {
					return false, r.Text
				}
			}
			return true, ""
		})
	})

	t.Run("REQ-agent-messaging-038 recover without reconnect", func(t *testing.T) {
		if session == nil || heldPort == "" {
			t.Fatal("the previous subtest did not start the session and hold the port")
		}

		procs := bpContainerProcs(t, c, "hostA", "mcp")
		if len(procs) != 1 {
			t.Fatalf("the node has %d MCP server processes, want 1: %+v", len(procs), procs)
		}
		mcpPID = procs[0].PID

		c.ReleasePort(t, "hostA", heldPort)

		// The user starts the Broker in a terminal; the session must recover
		// without a new MCP server process.
		brokerStart = host.StartBroker(t, harness.BrokerSpec{})
		if !brokerStart.Running {
			t.Fatalf("the explicit Broker did not start: exit code %d: %s", brokerStart.ExitCode, strings.TrimSpace(brokerStart.StderrLast))
		}

		harness.EventuallyPoll(t, brokerRecoverBound, "list_agents succeeds after the Broker starts", func(deadline time.Time) (bool, string) {
			r, err := session.PollCall(t, deadline, "list_agents", nil)
			if err != nil {
				return false, err.Error()
			}
			if r.IsError {
				return false, r.Text
			}
			return true, ""
		})

		after := bpContainerProcs(t, c, "hostA", "mcp")
		if len(after) != 1 {
			t.Fatalf("the node has %d MCP server processes after recovery, want 1: %+v", len(after), after)
		}
		if after[0].PID != mcpPID {
			t.Fatalf("the MCP server process changed from %d to %d; the session reconnected instead of recovering", mcpPID, after[0].PID)
		}
	})

	t.Run("REQ-broker-config-011 structured log", func(t *testing.T) {
		if session == nil {
			t.Fatal("the first subtest did not start the session")
		}
		session.End(t)

		if !brokerStart.Running {
			t.Fatal("the previous subtest did not start the Broker")
		}
		if code := host.StopBroker(t, syscall.SIGTERM); code != 0 {
			t.Fatalf("the Broker exited %d on SIGTERM, want 0", code)
		}

		start := host.StartBroker(t, harness.BrokerSpec{})
		if !start.Running {
			t.Fatalf("the explicit Broker did not start: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
		}

		workspace := host.Workspace(t, "plain", "", "")
		sender := host.StartSession(t, harness.SessionSpec{Name: "sender", StartMCP: true, Workdir: workspace})
		target := host.StartSession(t, harness.SessionSpec{Name: "target", StartMCP: true, Workdir: workspace})

		const topic = "broker-log-probe"
		bpJoinTopic(t, sender, topic)
		bpJoinTopic(t, target, topic)
		targetAddress := bpSelfAddress(t, target)
		bpWaitVisible(t, sender, targetAddress)

		body := fmt.Sprintf("broker-log-body-%d", time.Now().UnixNano())
		sent := sender.Call(t, "send_message", map[string]any{"to": targetAddress, "message": body})
		sent.MustOK(t)
		if frame := target.WaitFrame(t, 3*time.Second); !strings.Contains(frame.Content, body) {
			t.Fatalf("the delivered frame does not contain the sent body: %q", frame.Content)
		}

		failed := sender.Call(t, "send_message", map[string]any{
			"to":      "ghost@" + bpAlias(targetAddress),
			"message": "to a name that does not exist",
		})
		if !failed.IsError {
			t.Fatalf("a send to a nonexistent name succeeded: %s", failed.Text)
		}
		failed.MustCode(t, "not_found")

		if code := host.StopBroker(t, syscall.SIGTERM); code != 0 {
			t.Fatalf("the Broker exited %d on SIGTERM, want 0", code)
		}
		bpCheckStructuredLog(t, host.BrokerLog(t), body)
	})
}

// bpProc is one process of a node, as read from the local process table or
// from a container node agent.
type bpProc struct {
	PID  int
	PPID int
	SID  int
	UID  int
	Args []string
}

// bpLocalProcs returns the processes of the local node of the given kind:
// "broker", "mcp" or "all". A process belongs to the node when its environment
// carries the node's PEERS_TEST_HOME, which every process the node starts
// inherits, including Brokers started from a terminal.
func bpLocalProcs(t *testing.T, host *harness.Node, kind string) []bpProc {
	t.Helper()

	home := host.AccountHome()
	procs, err := node.ListProcs(func(p node.Proc) bool {
		if p.Env["PEERS_TEST_HOME"] != home {
			return false
		}
		return bpHasSubcommand(p.Args, kind)
	})
	if err != nil {
		t.Fatalf("listing the processes of the local node: %v", err)
	}

	out := make([]bpProc, 0, len(procs))
	for _, p := range procs {
		out = append(out, bpProc{PID: p.PID, PPID: p.PPID, SID: p.SID, UID: p.UID, Args: p.Args})
	}
	return out
}

// bpContainerProcs returns the processes of the container node of the given
// kind. It asks the node agent over the control protocol; the agent lists the
// processes of its own account.
func bpContainerProcs(t *testing.T, c *harness.Cluster, name, kind string) []bpProc {
	t.Helper()

	client := ctl.Dial(filepath.Join(c.CTLDir(t, name), "ctl.sock"))
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	infos, err := client.Procs(ctx, ctl.ProcsRequest{Kind: kind})
	if err != nil {
		t.Fatalf("listing the processes of container node %s: %v", name, err)
	}

	out := make([]bpProc, 0, len(infos))
	for _, p := range infos {
		out = append(out, bpProc{PID: p.PID, PPID: p.PPID, SID: p.SID, UID: p.UID, Args: p.Args})
	}
	return out
}

// bpHasSubcommand reports whether args name the peers subcommand kind.
// A Broker runs as "service run"; "all" matches every process.
func bpHasSubcommand(args []string, kind string) bool {
	if kind == "all" {
		return true
	}
	if kind == "broker" {
		return len(args) >= 3 && args[1] == "service" && args[2] == "run"
	}
	return len(args) >= 2 && args[1] == kind
}

// bpContainsString reports whether values contains want.
func bpContainsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// bpLastLine returns the last non-empty line of text.
func bpLastLine(text string) string {
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if line := strings.TrimSpace(lines[i]); line != "" {
			return line
		}
	}
	return ""
}

// bpListenLines returns the data rows of ss -ltnp, one per listening socket,
// without Docker's embedded DNS listener. Every Docker container runs that
// resolver on 127.0.0.11 for the container's whole lifetime, so it is not one
// of the listeners the Broker or the node agent created.
func bpListenLines(ss string) []string {
	var listeners []string
	for _, line := range strings.Split(ss, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "LISTEN") || strings.Contains(trimmed, "127.0.0.11:") {
			continue
		}
		listeners = append(listeners, trimmed)
	}
	return listeners
}

// bpAlias returns the alias part of a name@alias address.
func bpAlias(address string) string {
	at := strings.LastIndexByte(address, '@')
	if at < 0 || at == len(address)-1 {
		return ""
	}
	return address[at+1:]
}

// bpListAgentsPoll calls list_agents from a wait and returns the decoded answer.
// The call is bounded by the wait's deadline and a transport error is returned
// as state.
func bpListAgentsPoll(t *testing.T, deadline time.Time, session *harness.Session) (mcpserver.ListAgentsOut, string) {
	t.Helper()
	return pollDecode[mcpserver.ListAgentsOut](t, deadline, session, "list_agents", nil)
}

// bpJoinTopic joins topic, retrying while the session is still registering.
func bpJoinTopic(t *testing.T, session *harness.Session, topic string) {
	t.Helper()
	harness.EventuallyPoll(t, harness.TopicCallWaitBound, "join_topic "+topic, func(deadline time.Time) (bool, string) {
		r, err := session.PollCallWithin(t, deadline, harness.TopicCallPollBound, "join_topic", map[string]any{"name": topic})
		if err != nil {
			return false, err.Error()
		}
		if r.IsError {
			return false, r.Text
		}
		return true, ""
	})
}

// bpSelfAddress waits until the session reports its own address and returns it.
func bpSelfAddress(t *testing.T, session *harness.Session) string {
	t.Helper()
	var address string
	harness.EventuallyPoll(t, brokerReadyBound, "the session reports its address", func(deadline time.Time) (bool, string) {
		out, state := bpListAgentsPoll(t, deadline, session)
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

// bpWaitVisible waits until observer lists an agent at address.
func bpWaitVisible(t *testing.T, observer *harness.Session, address string) {
	t.Helper()
	harness.EventuallyPoll(t, brokerReadyBound, "the agent "+address+" is visible", func(deadline time.Time) (bool, string) {
		out, state := bpListAgentsPoll(t, deadline, observer)
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

// bpCheckStructuredLog asserts that every line of data is one JSON record with
// time, level and event, and that the sent body never appears. The only
// permitted non-JSON line is the final failure line cmd writes as
// "peers service run: ..."; a normal SIGTERM shutdown writes none.
func bpCheckStructuredLog(t *testing.T, data []byte, forbidden string) {
	t.Helper()

	text := string(data)
	if strings.Contains(text, forbidden) {
		t.Fatalf("the Broker log contains the message body %q", forbidden)
	}

	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	events := map[string]bool{}
	records := 0
	for i, line := range lines {
		if line == "" {
			continue
		}
		if rest, ok := strings.CutPrefix(line, "peers service run: "); ok {
			if i != len(lines)-1 {
				t.Fatalf("a failure line appears before the end of the log: %q", line)
			}
			if rest == "" {
				t.Fatalf("the final failure line carries no reason")
			}
			continue
		}

		var record map[string]json.RawMessage
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("log line %d is not a JSON object: %v: %q", i+1, err, line)
		}
		records++

		for _, key := range []string{"time", "level", "event"} {
			raw, ok := record[key]
			if !ok {
				t.Fatalf("log line %d has no %q key: %q", i+1, key, line)
			}
			var value string
			if err := json.Unmarshal(raw, &value); err != nil || value == "" {
				t.Fatalf("log line %d has an empty or non-string %q: %q", i+1, key, line)
			}
			if key == "level" {
				switch value {
				case "DEBUG", "INFO", "WARN", "ERROR":
				default:
					t.Fatalf("log line %d has level %q, want DEBUG, INFO, WARN or ERROR", i+1, value)
				}
			}
			if key == "event" {
				events[value] = true
			}
		}
		if _, ok := record["msg"]; ok {
			t.Fatalf("log line %d carries a msg key, want event: %q", i+1, line)
		}
	}

	if records == 0 {
		t.Fatal("the Broker wrote no structured log records")
	}
	for _, event := range []string{"broker_started", "broker_stopping"} {
		if !events[event] {
			t.Fatalf("the Broker log has no %q event: %v", event, events)
		}
	}
}

// TestLocalOracleReconnectsAfterBrokerRestart checks that the one Oracle client
// a local node keeps survives a Broker restart: the cached connection reports
// the Broker down, then reconnects with gRPC's backoff once it is back, rather
// than the harness dialing a new client.
func TestLocalOracleReconnectsAfterBrokerRestart(t *testing.T) {
	host := harness.NewLocalHost(t, harness.LocalOptions{Name: "oracle-restart"})
	host.WriteConfig(t, "broker:\n  listen: "+host.FreeAddr(t)+"\n")
	if start := host.StartBroker(t, harness.BrokerSpec{}); !start.Running {
		t.Fatalf("the Broker did not start: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
	}
	oracle := host.Oracle()
	if _, err := oracle.GetConfigRaw(""); err != nil {
		t.Fatalf("GetConfig before the stop: %v", err)
	}

	if code := host.StopBroker(t, syscall.SIGTERM); code != 0 {
		t.Fatalf("the Broker exited %d on SIGTERM, want 0", code)
	}
	if _, err := oracle.GetConfigRaw(""); err == nil {
		t.Fatal("GetConfig succeeded while the Broker was down")
	}

	if start := host.StartBroker(t, harness.BrokerSpec{}); !start.Running {
		t.Fatalf("the Broker did not restart: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
	}
	harness.Eventually(t, brokerRecoverBound, "the same Oracle client reconnects", func() (bool, string) {
		if _, err := oracle.GetConfigRaw(""); err != nil {
			return false, err.Error()
		}
		return true, ""
	})
	if host.Oracle() != oracle {
		t.Fatal("the node dialed a new Oracle client instead of reconnecting the cached one")
	}
}
