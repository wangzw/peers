package mcpserver

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakelocal"
	"github.com/wangzw/peers/internal/testkit/socktest"
)

// runHandle drives one Run in the background together with an MCP client
// connected over an in-memory transport.
type runHandle struct {
	cs     *mcp.ClientSession
	done   chan struct{}
	cancel context.CancelFunc

	mu  sync.Mutex
	err error

	stopOnce sync.Once
}

// startRunClient starts Run with opts.Transport replaced by one end of an
// in-memory pair, connects a client to the other end, and registers a cleanup
// that stops both.
func startRunClient(t *testing.T, opts Options) *runHandle {
	t.Helper()
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	opts.Transport = serverTransport

	ctx, cancel := context.WithCancel(context.Background())
	h := &runHandle{done: make(chan struct{}), cancel: cancel}
	go func() {
		err := Run(ctx, opts)
		h.mu.Lock()
		h.err = err
		h.mu.Unlock()
		close(h.done)
	}()

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "1"}, nil)
	connectCtx, connectCancel := context.WithTimeout(context.Background(), eventTimeout)
	defer connectCancel()
	cs, err := client.Connect(connectCtx, clientTransport, nil)
	if err != nil {
		cancel()
		t.Fatalf("connecting the MCP client: %v", err)
	}
	h.cs = cs
	t.Cleanup(func() { h.stop(t) })
	return h
}

// stop closes the client, cancels ctx and waits for Run to return.
func (h *runHandle) stop(t *testing.T) {
	t.Helper()
	h.stopOnce.Do(func() {
		_ = h.cs.Close()
		h.cancel()
		select {
		case <-h.done:
		case <-time.After(eventTimeout):
			t.Error("Run did not return after the MCP client closed")
		}
	})
}

// wait waits for Run to return and returns its error.
func (h *runHandle) wait(t *testing.T, d time.Duration) error {
	t.Helper()
	select {
	case <-h.done:
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.err
	case <-time.After(d):
		t.Fatalf("Run did not return within %v", d)
		return nil
	}
}

// finished reports whether Run has returned without consuming its result.
func (h *runHandle) finished() bool {
	select {
	case <-h.done:
		return true
	default:
		return false
	}
}

// errValue returns the error Run returned; it must already have returned.
func (h *runHandle) errValue() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.err
}

// runTestOptions builds options whose Broker-facing behaviour is under test
// control: environment lookups come from env, the working directory is an empty
// temporary directory, the sockets do not exist, and the git origin is
// unavailable.
func runTestOptions(t *testing.T, clock *loopClock, env map[string]string) Options {
	t.Helper()
	dir := t.TempDir()
	return Options{
		Logger:       loopTestLogger(),
		getenv:       func(name string) string { return env[name] },
		getwd:        func() (string, error) { return dir, nil },
		userSocket:   socktest.Path(t, "user.sock"),
		systemSocket: socktest.Path(t, "system.sock"),
		readOrigin:   func(context.Context, string) (string, error) { return "", errors.New("no origin") },
		after:        clock.after,
	}
}

// runNextRegister waits for a Register stream, advancing clock while the
// connection loop is in a backoff wait.
func runNextRegister(t *testing.T, srv *fakelocal.Server, clock *loopClock) *fakelocal.RegisterConn {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		conn, err := srv.NextRegister(ctx)
		cancel()
		if err == nil {
			return conn
		}
		if time.Now().After(deadline) {
			t.Fatalf("no Register stream arrived: %v", err)
		}
		if clock != nil {
			clock.advance()
		}
	}
}

// runWaitJoinTopic waits until the fake Broker has received a JoinTopic for
// name and returns that request.
func runWaitJoinTopic(t *testing.T, srv *fakelocal.Server, name string) *localpb.JoinTopicRequest {
	t.Helper()
	return runWaitJoinTopicAfter(t, srv, name, 0)
}

// runWaitJoinTopicAfter waits until the fake Broker has received a JoinTopic
// for name after the first skip requests and returns that request.
func runWaitJoinTopicAfter(t *testing.T, srv *fakelocal.Server, name string, skip int) *localpb.JoinTopicRequest {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for time.Now().Before(deadline) {
		reqs := topicsJoinRequests(srv)
		for _, req := range reqs[min(skip, len(reqs)):] {
			if req.GetName() == name {
				return req
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("no JoinTopic for %q arrived after %d earlier ones", name, skip)
	return nil
}

// runJoinTopicNames returns the names of the JoinTopic requests received after
// the first skip calls.
func runJoinTopicNames(srv *fakelocal.Server, skip int) []string {
	reqs := topicsJoinRequests(srv)
	if skip > len(reqs) {
		skip = len(reqs)
	}
	var out []string
	for _, req := range reqs[skip:] {
		out = append(out, req.GetName())
	}
	return out
}

// runWaitRegistered calls list_agents until the call succeeds, which means the
// registration_complete event has been applied.
func runWaitRegistered(t *testing.T, cs *mcp.ClientSession) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for time.Now().Before(deadline) {
		if res := callTool(t, cs, "list_agents", nil); !res.IsError {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("list_agents never succeeded after registration")
}

// REQ-agent-messaging-038
func TestRunHandshakeWithoutBroker(t *testing.T) {
	clock := newLoopClock()
	missing := socktest.Path(t, "missing.sock")
	opts := runTestOptions(t, clock, nil)
	opts.userSocket = missing

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	opts.Transport = serverTransport
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, opts) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			_ = err
		case <-time.After(eventTimeout):
			t.Error("Run did not return at cleanup")
		}
	})

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "1"}, nil)
	connectCtx, connectCancel := context.WithTimeout(context.Background(), eventTimeout)
	defer connectCancel()
	cs, err := client.Connect(connectCtx, clientTransport, nil)
	if err != nil {
		t.Fatalf("MCP handshake did not complete within %s: %v", eventTimeout, err)
	}
	t.Cleanup(func() { _ = cs.Close() })

	if got := len(listTools(t, cs)); got != 13 {
		t.Fatalf("ListTools returned %d tools, want 13", got)
	}
}

// runWaitUnavailableError calls list_agents until the connection loop has left
// PhaseStarting and reports why the Broker is unusable. The loop runs in a
// background goroutine, so the first tool call may legitimately arrive before
// the first Broker selection has finished; PhaseStarting answers "still being
// set up" and asks the caller to retry.
func runWaitUnavailableError(t *testing.T, cs *mcp.ClientSession) *mcp.CallToolResult {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for {
		res := callTool(t, cs, "list_agents", nil)
		if res.IsError && !strings.Contains(textContent(t, res), textStarting) {
			return res
		}
		if time.Now().After(deadline) {
			t.Fatalf("list_agents kept reporting that the Broker is still being set up: %s", textContent(t, res))
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// REQ-agent-messaging-038
func TestRunToolErrorsWhenBrokerIsUnavailable(t *testing.T) {
	clock := newLoopClock()
	sock := fakelocal.SocketPath(t)
	opts := runTestOptions(t, clock, map[string]string{
		"CLAUDE_CODE_MESSAGING_SOCKET": "/s",
	})
	opts.userSocket = sock
	h := startRunClient(t, opts)

	res := runWaitUnavailableError(t, h.cs)
	if !res.IsError {
		t.Fatal("list_agents succeeded without a Broker")
	}
	text := textContent(t, res)
	if !strings.HasPrefix(text, "list_agents failed:") {
		t.Errorf("list_agents error = %q, want a \"list_agents failed:\" prefix", text)
	}
	if !strings.Contains(text, "no Broker is available for this account") {
		t.Errorf("list_agents error = %q, want the no-Broker text", text)
	}

	res = callTool(t, h.cs, "send_message", map[string]any{"to": "a@b", "message": "hi"})
	if !res.IsError {
		t.Fatal("send_message succeeded without a Broker")
	}
	if text := textContent(t, res); !strings.HasPrefix(text, "Not sent:") {
		t.Errorf("send_message error = %q, want a \"Not sent:\" prefix", text)
	}

	// The Broker appears; the loop recovers by itself, without the MCP client
	// reconnecting.
	srv := fakelocal.Start(t, sock, fakelocal.Handlers{
		ListAgents: func(context.Context, *localpb.ListAgentsRequest) (*localpb.ListAgentsResponse, error) {
			return &localpb.ListAgentsResponse{SelfAddress: "me@b"}, nil
		},
	})
	conn := runNextRegister(t, srv, clock)
	if err := conn.Complete("agent-1", "me@b", false); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	runWaitRegistered(t, h.cs)

	res = callTool(t, h.cs, "list_agents", nil)
	if res.IsError {
		t.Fatalf("list_agents failed after the Broker came up: %s", textContent(t, res))
	}
	if h.finished() {
		t.Fatalf("Run returned while the Broker was unavailable: %v", h.errValue())
	}
}

// REQ-topics-015, REQ-topics-019
func TestRunJoinsDefaultTopicFromProjectConfig(t *testing.T) {
	clock := newLoopClock()
	sock := fakelocal.SocketPath(t)
	dir := t.TempDir()
	writeProjectFile(t, dir, "topics: [team]\n")

	opts := runTestOptions(t, clock, map[string]string{
		"CLAUDE_CODE_MESSAGING_SOCKET": "/s",
	})
	opts.userSocket = sock
	opts.getwd = func() (string, error) { return dir, nil }

	srv := fakelocal.Start(t, sock, fakelocal.Handlers{
		JoinTopic: func(context.Context, *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			return &localpb.JoinTopicResponse{}, nil
		},
	})
	startRunClient(t, opts)

	conn := runNextRegister(t, srv, clock)
	if err := conn.Complete("agent-1", "me@b", true); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	req := runWaitJoinTopic(t, srv, "team")
	if req.GetAgentId() != "agent-1" {
		t.Errorf("JoinTopic agent_id = %q, want agent-1", req.GetAgentId())
	}
	if req.GetIsPublic() {
		t.Error("the default topic was joined as public")
	}
}

// REQ-topics-027: a nameless leave acts on the topics this process joined
// automatically at its first registration, not on a list computed again from a
// project configuration that changed since.
func TestRunNamelessLeaveUsesTheTopicsJoinedAtRegistration(t *testing.T) {
	clock := newLoopClock()
	sock := fakelocal.SocketPath(t)
	dir := t.TempDir()
	writeProjectFile(t, dir, "topics: [team]\n")

	opts := runTestOptions(t, clock, map[string]string{
		"CLAUDE_CODE_MESSAGING_SOCKET": "/s",
	})
	opts.userSocket = sock
	opts.getwd = func() (string, error) { return dir, nil }

	srv := fakelocal.Start(t, sock, fakelocal.Handlers{
		ListAgents: func(context.Context, *localpb.ListAgentsRequest) (*localpb.ListAgentsResponse, error) {
			return &localpb.ListAgentsResponse{SelfAddress: "me@b"}, nil
		},
		JoinTopic: func(context.Context, *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			return &localpb.JoinTopicResponse{}, nil
		},
		ListTopics: func(context.Context, *localpb.ListTopicsRequest) (*localpb.ListTopicsResponse, error) {
			return &localpb.ListTopicsResponse{JoinedTopics: []string{"team"}}, nil
		},
		LeaveTopic: func(context.Context, *localpb.LeaveTopicRequest) (*localpb.LeaveTopicResponse, error) {
			return &localpb.LeaveTopicResponse{}, nil
		},
	})
	h := startRunClient(t, opts)

	conn := runNextRegister(t, srv, clock)
	if err := conn.Complete("agent-1", "me@b", true); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	runWaitJoinTopic(t, srv, "team")
	runWaitRegistered(t, h.cs)

	// The project configuration changes after the first registration. The
	// nameless call must still leave the topic that was joined then.
	writeProjectFile(t, dir, "topics: [other]\n")
	res := callTool(t, h.cs, "leave_topic", nil)
	if res.IsError {
		t.Fatalf("leave_topic without a name failed: %s", textContent(t, res))
	}

	var left []string
	for _, call := range srv.Calls() {
		if call.Method == "LeaveTopic" {
			left = append(left, call.Request.(*localpb.LeaveTopicRequest).GetName())
		}
	}
	if len(left) != 1 || left[0] != "team" {
		t.Fatalf("nameless leave_topic left %v, want [team]", left)
	}
}

// REQ-topics-020
func TestRunDoesNotJoinWhenAutoJoinIsOff(t *testing.T) {
	clock := newLoopClock()
	sock := fakelocal.SocketPath(t)
	dir := t.TempDir()
	writeProjectFile(t, dir, "topics: [team]\n")

	opts := runTestOptions(t, clock, map[string]string{
		"CLAUDE_CODE_MESSAGING_SOCKET": "/s",
	})
	opts.userSocket = sock
	opts.getwd = func() (string, error) { return dir, nil }

	handlers := fakelocal.Handlers{
		ListAgents: func(context.Context, *localpb.ListAgentsRequest) (*localpb.ListAgentsResponse, error) {
			return &localpb.ListAgentsResponse{SelfAddress: "me@b"}, nil
		},
		JoinTopic: func(context.Context, *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			return &localpb.JoinTopicResponse{}, nil
		},
	}
	srv := fakelocal.Start(t, sock, handlers)
	h := startRunClient(t, opts)

	conn := runNextRegister(t, srv, clock)
	if err := conn.Complete("agent-1", "me@b", false); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	runWaitRegistered(t, h.cs)

	// The completion joins nothing. Join a marker by hand, restart the Broker
	// and register again: the restore replays the memory in insertion order, so
	// a default topic remembered despite auto_join being off would be joined
	// before the marker. Waiting for the marker is what orders this assertion.
	res := callTool(t, h.cs, "join_topic", map[string]any{"name": "marker"})
	if res.IsError {
		t.Fatalf("join_topic marker failed: %s", textContent(t, res))
	}
	runWaitJoinTopic(t, srv, "marker")

	srv.Stop()
	srv = fakelocal.Start(t, sock, handlers)
	conn2 := runNextRegister(t, srv, clock)
	if err := conn2.Complete("agent-1", "me@b", true); err != nil {
		t.Fatalf("Complete after restart: %v", err)
	}
	runWaitJoinTopic(t, srv, "marker")
	for _, name := range runJoinTopicNames(srv, 0) {
		if name == "team" {
			t.Fatal("the default topic was joined even though auto_join was off")
		}
	}
}

// REQ-topics-022
func TestRunRestoresTopicsAfterBrokerRestart(t *testing.T) {
	clock := newLoopClock()
	sock := fakelocal.SocketPath(t)
	dir := t.TempDir()
	writeProjectFile(t, dir, "topics: [team]\n")

	opts := runTestOptions(t, clock, map[string]string{
		"CLAUDE_CODE_MESSAGING_SOCKET": "/s",
	})
	opts.userSocket = sock
	opts.getwd = func() (string, error) { return dir, nil }

	handlers := fakelocal.Handlers{
		JoinTopic: func(context.Context, *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			return &localpb.JoinTopicResponse{}, nil
		},
	}
	srv := fakelocal.Start(t, sock, handlers)
	h := startRunClient(t, opts)

	conn := runNextRegister(t, srv, clock)
	if err := conn.Complete("agent-1", "me@b", true); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	runWaitJoinTopic(t, srv, "team")

	res := callTool(t, h.cs, "join_topic", map[string]any{
		"name": "x", "public": true, "description": "d",
	})
	if res.IsError {
		t.Fatalf("join_topic x failed: %s", textContent(t, res))
	}

	// The restarted fake Broker starts with an empty call log, so the restored
	// joins are the first thing it records.
	srv.Stop()
	srv = fakelocal.Start(t, sock, handlers)

	conn2 := runNextRegister(t, srv, clock)
	if err := conn2.Complete("agent-1", "me@b", true); err != nil {
		t.Fatalf("Complete after restart: %v", err)
	}
	runWaitJoinTopic(t, srv, "team")
	req := runWaitJoinTopic(t, srv, "x")
	if !req.GetIsPublic() || req.GetDescription() != "d" {
		t.Errorf("restored x = {%q public=%v}, want public with description d", req.GetDescription(), req.GetIsPublic())
	}
}

// REQ-agent-messaging-034
func TestRunForgetsManualTopicsAcrossProcesses(t *testing.T) {
	clock := newLoopClock()
	sock := fakelocal.SocketPath(t)
	dir := t.TempDir()
	writeProjectFile(t, dir, "topics: [team]\n")

	opts := runTestOptions(t, clock, map[string]string{
		"CLAUDE_CODE_MESSAGING_SOCKET": "/s",
	})
	opts.userSocket = sock
	opts.getwd = func() (string, error) { return dir, nil }

	handlers := fakelocal.Handlers{
		JoinTopic: func(context.Context, *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			return &localpb.JoinTopicResponse{}, nil
		},
	}
	srv := fakelocal.Start(t, sock, handlers)

	first := startRunClient(t, opts)
	conn := runNextRegister(t, srv, clock)
	if err := conn.Complete("agent-1", "me@b", true); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	runWaitJoinTopic(t, srv, "team")
	res := callTool(t, first.cs, "join_topic", map[string]any{"name": "x"})
	if res.IsError {
		t.Fatalf("join_topic x failed: %s", textContent(t, res))
	}
	runWaitJoinTopic(t, srv, "x")

	// Turning the session off: the first Run ends and the second starts over.
	first.cancel()
	if err := first.wait(t, eventTimeout); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}
	second := startRunClient(t, opts)
	conn2 := runNextRegister(t, srv, clock)
	// The fake Broker keeps the first process's JoinTopic calls, so waiting
	// for any "team" request returns the first process's one. Wait for the
	// second process's own restore instead: it runs only after the new agent
	// ID is in the status store, so this wait also orders the join_topic below.
	skip := len(topicsJoinRequests(srv))
	if err := conn2.Complete("agent-2", "me2@b", true); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	restored := runWaitJoinTopicAfter(t, srv, "team", skip)
	if got := restored.GetAgentId(); got != "agent-2" {
		t.Fatalf("the restored team join was sent for %q, want agent-2", got)
	}

	// A new process must not replay a topic the old one joined by hand. Join a
	// marker and restart the Broker: the restore replays the memory in
	// insertion order, so a wrongly carried-over "x" would come before the
	// marker, and waiting for the marker orders the assertion.
	res2 := callTool(t, second.cs, "join_topic", map[string]any{"name": "marker"})
	if res2.IsError {
		t.Fatalf("join_topic marker failed: %s", textContent(t, res2))
	}
	runWaitJoinTopic(t, srv, "marker")

	srv.Stop()
	srv = fakelocal.Start(t, sock, handlers)
	conn3 := runNextRegister(t, srv, clock)
	if err := conn3.Complete("agent-2", "me2@b", true); err != nil {
		t.Fatalf("Complete after restart: %v", err)
	}
	runWaitJoinTopic(t, srv, "marker")
	for _, name := range runJoinTopicNames(srv, 0) {
		if name == "x" {
			t.Fatalf("the second process restored the manually joined topic x")
		}
	}
}

// REQ-agent-messaging-034
func TestRunCancelClosesRegisterStream(t *testing.T) {
	clock := newLoopClock()
	sock := fakelocal.SocketPath(t)
	opts := runTestOptions(t, clock, map[string]string{
		"CLAUDE_CODE_MESSAGING_SOCKET": "/s",
	})
	opts.userSocket = sock
	srv := fakelocal.Start(t, sock, fakelocal.Handlers{})
	h := startRunClient(t, opts)

	conn := runNextRegister(t, srv, clock)
	h.cancel()
	if err := h.wait(t, eventTimeout); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}
	select {
	case <-conn.Done():
	case <-time.After(eventTimeout):
		t.Fatalf("the Register stream was still open after Run returned (%s)", eventTimeout)
	}
}

func TestRunInstallsNoSignalHandler(t *testing.T) {
	clock := newLoopClock()
	sock := socktest.Path(t, "missing.sock")
	opts := runTestOptions(t, clock, map[string]string{
		"CLAUDE_CODE_MESSAGING_SOCKET": "/s",
	})
	opts.userSocket = sock
	startRunClient(t, opts)

	if signal.Ignored(syscall.SIGTERM) {
		t.Error("SIGTERM is ignored while Run serves; cmd owns the signal handling")
	}

	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("globbing the package sources: %v", err)
	}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			ident, ok := sel.X.(*ast.Ident)
			if !ok || ident.Name != "signal" {
				return true
			}
			if sel.Sel.Name == "Notify" || sel.Sel.Name == "NotifyContext" {
				t.Errorf("%s calls signal.%s; signal handling belongs to cmd", name, sel.Sel.Name)
			}
			return true
		})
	}
}

// REQ-broker-config-016
func TestRunSendsInjectedProtocolVersion(t *testing.T) {
	clock := newLoopClock()
	sock := fakelocal.SocketPath(t)
	opts := runTestOptions(t, clock, map[string]string{
		"CLAUDE_CODE_MESSAGING_SOCKET": "/s",
	})
	opts.userSocket = sock
	opts.LocalProtocolVersion = 7

	srv := fakelocal.Start(t, sock, fakelocal.Handlers{
		Hello: func(context.Context, *localpb.HelloRequest) (*localpb.HelloResponse, error) {
			return &localpb.HelloResponse{ProtocolVersion: 1}, nil
		},
	})
	h := startRunClient(t, opts)

	deadline := time.Now().Add(eventTimeout)
	var text string
	for time.Now().Before(deadline) {
		res := callTool(t, h.cs, "list_agents", nil)
		text = textContent(t, res)
		if strings.Contains(text, "(code: version_mismatch)") {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !strings.Contains(text, "this MCP server speaks version 7") {
		t.Fatalf("list_agents error = %q, want both protocol versions", text)
	}
	if !strings.Contains(text, "(code: version_mismatch)") {
		t.Fatalf("list_agents error = %q, want code version_mismatch", text)
	}

	var hello *localpb.HelloRequest
	for _, call := range srv.Calls() {
		if call.Method == "Hello" {
			hello = call.Request.(*localpb.HelloRequest)
		}
	}
	if hello == nil {
		t.Fatal("the Broker never received Hello")
	}
	if hello.GetProtocolVersion() != 7 {
		t.Fatalf("Hello protocol_version = %d, want 7", hello.GetProtocolVersion())
	}
}

// REQ-broker-config-038
func TestRunReportsNoBroker(t *testing.T) {
	clock := newLoopClock()
	opts := runTestOptions(t, clock, map[string]string{"CLAUDE_CODE_MESSAGING_SOCKET": "/s"})
	h := startRunClient(t, opts)

	deadline := time.Now().Add(eventTimeout)
	var text string
	for time.Now().Before(deadline) {
		res := callTool(t, h.cs, "list_agents", nil)
		text = textContent(t, res)
		if strings.Contains(text, "no Broker is available") {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	for _, want := range []string{"no Broker is available", "ask the administrator", "peers service install", "peers service run", "code 6", "(code: internal)"} {
		if !strings.Contains(text, want) {
			t.Fatalf("list_agents error = %q, want it to contain %q", text, want)
		}
	}
	if h.finished() {
		t.Fatalf("Run returned after reporting no Broker: %v", h.errValue())
	}
}
