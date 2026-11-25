package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	ccsock "github.com/PeterSR/claude-code-socket-transport"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	agentReadyTimeout = eventTimeout
	agentSendTimeout  = eventTimeout
)

// TestMain turns the test binary into the stand-in MCP server the simulated
// Agent starts as its child, and otherwise runs the tests.
func TestMain(m *testing.M) {
	if os.Getenv("RSPROBE_STUB_MCP") == "1" {
		os.Exit(runStubMCP())
	}
	os.Exit(m.Run())
}

type stubEchoIn struct {
	Text string `json:"text"`
}

func runStubMCP() int {
	server := mcp.NewServer(&mcp.Implementation{Name: "rsprobe-stub", Version: "0"}, nil)
	mcp.AddTool[stubEchoIn, any](
		server,
		&mcp.Tool{Name: "echo", Description: "Returns the requested text and the messaging environment."},
		func(_ context.Context, _ *mcp.CallToolRequest, in stubEchoIn) (*mcp.CallToolResult, any, error) {
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: stubEchoText(in.Text)}}}, nil, nil
		},
	)
	if err := server.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		return 1
	}
	return 0
}

func stubEchoText(text string) string {
	lines := []string{
		text,
		"CLAUDE_CODE_MESSAGING_SOCKET=" + os.Getenv("CLAUDE_CODE_MESSAGING_SOCKET"),
		"CLAUDE_CONFIG_DIR=" + os.Getenv("CLAUDE_CONFIG_DIR"),
	}
	if child := os.Getenv("CLAUDE_CODE_CHILD_SESSION"); child != "" {
		lines = append(lines, "CLAUDE_CODE_CHILD_SESSION="+child)
	}
	return strings.Join(lines, "\n")
}

type testAgent struct {
	dir    string
	ctl    string
	socket string
	recv   string

	cancel   context.CancelFunc
	done     chan error
	finished bool
	runErr   error
}

// maxSocketPathBytes is the largest socket path the tests bind, kept below
// macOS's 104-byte sun_path limit.
const maxSocketPathBytes = 100

func testSocketDir(t *testing.T, base, socketName string) string {
	t.Helper()
	if len(filepath.Join(base, socketName)) <= maxSocketPathBytes {
		return base
	}
	dir, err := os.MkdirTemp("/tmp", "rs-")
	if err != nil {
		t.Fatalf("creating a short socket directory: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func startTestAgent(t *testing.T) *testAgent {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("locating the test binary: %v", err)
	}

	dir := t.TempDir()
	pid := strconv.Itoa(os.Getpid())
	sockDir := testSocketDir(t, dir, pid+".inbox.sock")
	agent := &testAgent{
		dir:    dir,
		ctl:    filepath.Join(sockDir, "ctl.sock"),
		socket: filepath.Join(sockDir, pid+".inbox.sock"),
		recv:   filepath.Join(dir, "recv.jsonl"),
		done:   make(chan error, 1),
	}
	recordPath := filepath.Join(dir, "sessions", pid+".json")

	// The MCP child is this test binary, which TestMain turns into the stub.
	t.Setenv("RSPROBE_STUB_MCP", "1")

	ctx, cancel := context.WithCancel(context.Background())
	agent.cancel = cancel
	go func() {
		agent.done <- runAgentRun(ctx, []string{
			"--config-dir", dir,
			"--name", "agent-test",
			"--cwd", dir,
			"--ctl", agent.ctl,
			"--recv-log", agent.recv,
			"--peers", executable,
		})
	}()

	t.Cleanup(func() {
		agent.cancel()
		if !agent.finished {
			select {
			case agent.runErr = <-agent.done:
				agent.finished = true
			case <-time.After(eventTimeout):
				t.Error("agent run did not stop after its context was cancelled")
			}
		}
		if agent.runErr != nil {
			t.Errorf("agent run returned %v", agent.runErr)
		}
		if _, err := os.Stat(recordPath); !os.IsNotExist(err) {
			t.Errorf("session record %s still exists after shutdown: %v", recordPath, err)
		}
		if _, err := os.Stat(agent.socket); !os.IsNotExist(err) {
			t.Errorf("inbox socket %s still exists after shutdown: %v", agent.socket, err)
		}
	})

	agent.waitReady(t)
	return agent
}

// TestTestSocketDirKeepsAShortBase checks that a base path that fits is
// returned unchanged.
func TestTestSocketDirKeepsAShortBase(t *testing.T) {
	base, err := os.MkdirTemp("/tmp", "rs-test-")
	if err != nil {
		t.Fatalf("creating a short base directory: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })

	if got := testSocketDir(t, base, "1234.inbox.sock"); got != base {
		t.Errorf("testSocketDir = %q, want the short base %q", got, base)
	}
}

// TestTestSocketDirFallsBackWhenThePathIsLong checks that an over-long base
// path falls back to a directory under /tmp.
func TestTestSocketDirFallsBackWhenThePathIsLong(t *testing.T) {
	base := t.TempDir()
	for len(filepath.Join(base, "1234.inbox.sock")) <= maxSocketPathBytes {
		base = filepath.Join(base, strings.Repeat("d", 20))
		if err := os.Mkdir(base, 0o755); err != nil {
			t.Fatalf("creating the long base: %v", err)
		}
	}

	dir := testSocketDir(t, base, "1234.inbox.sock")
	if dir == base {
		t.Errorf("testSocketDir = %q, want a fallback outside the long base", dir)
	}
	if socket := filepath.Join(dir, "1234.inbox.sock"); len(socket) > maxSocketPathBytes {
		t.Errorf("socket path %q is %d bytes, want at most %d", socket, len(socket), maxSocketPathBytes)
	}
}

func (a *testAgent) recordPath() string {
	return filepath.Join(a.dir, "sessions", strconv.Itoa(os.Getpid())+".json")
}

// waitReady polls for the control socket the Agent creates once its MCP
// process has connected.
func (a *testAgent) waitReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(agentReadyTimeout)
	for {
		select {
		case a.runErr = <-a.done:
			a.finished = true
			t.Fatalf("agent run exited before becoming ready: %v", a.runErr)
		default:
		}
		if _, err := os.Stat(a.ctl); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("control socket %s did not appear", a.ctl)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (a *testAgent) readRecord(t *testing.T) agentRecord {
	t.Helper()
	record, err := readAgentRecord(a.recordPath())
	if err != nil {
		t.Fatalf("reading the session record: %v", err)
	}
	return record
}

// TestAgentRunWritesRecordAndForwardsCalls checks the session record and that a
// tool call reaches the MCP process without inheriting the parent session.
func TestAgentRunWritesRecordAndForwardsCalls(t *testing.T) {
	// The MCP process must not inherit a parent session's identity.
	t.Setenv("CLAUDE_CODE_CHILD_SESSION", "1")

	agent := startTestAgent(t)

	record := agent.readRecord(t)
	if record.PID != os.Getpid() {
		t.Errorf("record pid = %d, want %d", record.PID, os.Getpid())
	}
	if record.Name != "agent-test" {
		t.Errorf("record name = %q, want %q", record.Name, "agent-test")
	}
	if record.Status != "idle" {
		t.Errorf("record status = %q, want %q", record.Status, "idle")
	}
	if record.MessagingSocketPath != agent.socket {
		t.Errorf("record socket = %q, want %q", record.MessagingSocketPath, agent.socket)
	}

	var out bytes.Buffer
	if err := runAgentCall([]string{"--ctl", agent.ctl, "echo", `{"text":"hi"}`}, &out); err != nil {
		t.Fatalf("agent call echo: %v", err)
	}
	var resp controlResponse
	if err := json.Unmarshal(out.Bytes(), &resp); err != nil {
		t.Fatalf("decoding the answer %q: %v", out.String(), err)
	}
	if !resp.OK {
		t.Fatalf("echo failed: %+v", resp)
	}
	for _, want := range []string{
		"hi",
		"CLAUDE_CODE_MESSAGING_SOCKET=" + agent.socket,
		"CLAUDE_CONFIG_DIR=" + agent.dir,
	} {
		if !strings.Contains(resp.Text, want) {
			t.Errorf("answer %q does not contain %q", resp.Text, want)
		}
	}
	if strings.Contains(resp.Text, "CLAUDE_CODE_CHILD_SESSION") {
		t.Errorf("the MCP process inherited CLAUDE_CODE_CHILD_SESSION: %q", resp.Text)
	}
}

// TestAgentRecvLog checks that a frame is logged with its sender name and body,
// and the peer pid on Linux.
func TestAgentRecvLog(t *testing.T) {
	agent := startTestAgent(t)

	client := &ccsock.Client{NoAuth: true, Timeout: agentSendTimeout}
	_, err := client.SendToSocket(context.Background(), agent.socket, ccsock.Message{
		Text:     "hello-body",
		Priority: ccsock.PriorityNext,
		FromName: "sender",
	})
	// The Agent closes the connection after reading the frame, and the library
	// half-closes only afterwards, so on macOS shutdown reports ENOTCONN even
	// though the frame was delivered; the receive log, not the send result, is
	// what this test checks.
	if err != nil && (runtime.GOOS != "darwin" || !errors.Is(err, syscall.ENOTCONN)) {
		t.Fatalf("sending a frame: %v", err)
	}

	data, err := os.ReadFile(agent.recv)
	if err != nil {
		t.Fatalf("reading the receive log: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatalf("receive log = %q, want one line", string(data))
	}
	var entry recvEntry
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatalf("decoding the receive log line %q: %v", lines[0], err)
	}
	if _, err := time.Parse(time.RFC3339Nano, entry.T); err != nil {
		t.Errorf("t = %q is not RFC3339Nano: %v", entry.T, err)
	}
	// Peer credentials are only available on Linux; elsewhere the receive log
	// records 0 and the frame is still accepted.
	if runtime.GOOS == "linux" && entry.PeerPID != os.Getpid() {
		t.Errorf("peer_pid = %d, want %d", entry.PeerPID, os.Getpid())
	}
	if entry.FromName != "sender" {
		t.Errorf("from_name = %q, want %q", entry.FromName, "sender")
	}
	if !strings.Contains(entry.Content, "hello-body") {
		t.Errorf("content = %q, want it to contain the body", entry.Content)
	}
	if !strings.Contains(entry.Content, `from-name="sender"`) {
		t.Errorf("content = %q, want the attribution envelope", entry.Content)
	}
}

// TestRecordedPeerPIDReportsUnsupported checks that an unsupported credential
// reader logs peer_pid 0 and prints the reason once.
func TestRecordedPeerPIDReportsUnsupported(t *testing.T) {
	var warn bytes.Buffer
	pid := recordedPeerPID(nil, func(net.Conn) (int, error) {
		return 0, errors.ErrUnsupported
	}, &warn)
	if pid != 0 {
		t.Errorf("pid = %d, want 0", pid)
	}
	if !strings.Contains(warn.String(), peerCredentialUnsupported) {
		t.Errorf("warning = %q, want %q", warn.String(), peerCredentialUnsupported)
	}
}

// TestRecordedPeerPIDReturnsTheReadPID checks that a successful read returns
// the pid without warning.
func TestRecordedPeerPIDReturnsTheReadPID(t *testing.T) {
	var warn bytes.Buffer
	pid := recordedPeerPID(nil, func(net.Conn) (int, error) {
		return 42, nil
	}, &warn)
	if pid != 42 {
		t.Errorf("pid = %d, want 42", pid)
	}
	if warn.Len() != 0 {
		t.Errorf("warning = %q, want none", warn.String())
	}
}

// TestRecordedPeerPIDSwallowsOtherErrors checks that a read error other than
// unsupported returns 0 without warning.
func TestRecordedPeerPIDSwallowsOtherErrors(t *testing.T) {
	var warn bytes.Buffer
	pid := recordedPeerPID(nil, func(net.Conn) (int, error) {
		return 0, errors.New("reading credentials failed")
	}, &warn)
	if pid != 0 {
		t.Errorf("pid = %d, want 0", pid)
	}
	if warn.Len() != 0 {
		t.Errorf("warning = %q, want none", warn.String())
	}
}

// TestPeerPIDReportsThePeerProcess checks that a unix socket reports this
// process as its peer pid.
func TestPeerPIDReportsThePeerProcess(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skipf("peer credentials are not implemented on %s", runtime.GOOS)
	}
	// Darwin sizes sun_path at 104 bytes, and t.TempDir keeps the test name in
	// the path; a short directory under /tmp keeps the socket path below it.
	dir, err := os.MkdirTemp("/tmp", "rspeer-")
	if err != nil {
		t.Fatalf("creating the socket directory: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "peer.sock")
	listener, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listening on %s: %v", sock, err)
	}
	defer func() { _ = listener.Close() }()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- conn
	}()

	client, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dialing %s: %v", sock, err)
	}
	defer func() { _ = client.Close() }()
	server, ok := <-accepted
	if !ok {
		t.Fatal("accepting the connection")
	}
	defer func() { _ = server.Close() }()

	pid, err := peerPID(server)
	if err != nil {
		t.Fatalf("peerPID: %v", err)
	}
	if want := os.Getpid(); pid != want {
		t.Fatalf("peerPID = %d, want this process %d", pid, want)
	}
}

// TestAgentSetRewritesInPlace checks that name and status updates rewrite the
// session record in place.
func TestAgentSetRewritesInPlace(t *testing.T) {
	agent := startTestAgent(t)

	before, err := os.Stat(agent.recordPath())
	if err != nil {
		t.Fatalf("stating the session record: %v", err)
	}

	var out bytes.Buffer
	if err := runAgentSet([]string{"--ctl", agent.ctl, "--name", "x2"}, &out); err != nil {
		t.Fatalf("agent set --name: %v", err)
	}
	var resp controlResponse
	if err := json.Unmarshal(out.Bytes(), &resp); err != nil {
		t.Fatalf("decoding the answer %q: %v", out.String(), err)
	}
	if !resp.OK {
		t.Fatalf("set failed: %+v", resp)
	}

	after, err := os.Stat(agent.recordPath())
	if err != nil {
		t.Fatalf("stating the rewritten record: %v", err)
	}
	if !os.SameFile(before, after) {
		t.Error("the session record was replaced instead of rewritten in place")
	}
	if got := agent.readRecord(t).Name; got != "x2" {
		t.Errorf("record name = %q, want %q", got, "x2")
	}

	if err := runAgentSet([]string{"--ctl", agent.ctl, "--status", "busy"}, &out); err != nil {
		t.Fatalf("agent set --status: %v", err)
	}
	if got := agent.readRecord(t).Status; got != "busy" {
		t.Errorf("record status = %q, want %q", got, "busy")
	}
}
