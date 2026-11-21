package harness

import (
	"context"
	"fmt"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakesession"
	"github.com/wangzw/peers/test/e2e/node"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// testWaitBound is the project-wide bound for one test wait, three seconds. It
// is a variable so a test can shorten it before starting a session.
var testWaitBound = 3 * time.Second

// sessionStartBound bounds starting a session: launching the MCP server process
// and completing the initialize handshake. It is the thirty-second startup
// allowance the roadmap records for Claude Code, the same bound node.StartSession
// applies to the handshake, not the three-second bound for a test wait. The
// launch is paid by the operating system under whatever load the runner carries:
// on a loaded runner one local start has taken longer than three seconds while
// the rest of the package ran about twenty percent slower.
const sessionStartBound = 30 * time.Second

// oracleBound bounds each OracleClient call. The local interface is a Unix
// socket served in-process by the Broker, so it answers at once; no product
// timer longer than the project-wide three-second bound governs it.
const oracleBound = 3 * time.Second

// sessionCallBound bounds one MCP tool call. It is larger than the default
// send budget of 20 seconds so a send that is waiting for its own deadline
// still returns its error result.
const sessionCallBound = 60 * time.Second

// ToolCallPollBound bounds one MCP tool call made from inside a test wait. The
// bound must cover more than the tool's own work: the update-hint middleware
// runs inside the same tools/call after the handler and queries the Broker with
// updateStatusTimeout, three seconds (internal/mcpserver/updatehint.go:21). A
// call bounded at three seconds would therefore be cut off while the hint was
// still being answered, and because a timed-out query is retried by the next
// call, a loaded Broker would fail every poll of the wait. The seven seconds
// are a three-second handler budget plus the three-second hint and a second of
// margin. The three-second handler budget is what every handler except the
// topic tools uses; a topic handler bounds one Broker RPC at topicRPCTimeout,
// ten seconds (internal/mcpserver/topics.go:16), so a topic tool call polled
// from a wait uses TopicCallPollBound instead. The bound stays far below
// sessionCallBound, so a stalled call cannot turn a bounded wait into a
// sixty-second hang.
const ToolCallPollBound = 7 * time.Second

// TopicCallPollBound bounds one topic tool call made from inside a test wait.
// join_topic and leave_topic bound their Broker RPC at topicRPCTimeout, ten
// seconds (internal/mcpserver/topics.go:16), and the update hint adds its
// three-second query inside the same tools/call. Ten plus three plus a second
// of margin is fourteen seconds.
const TopicCallPollBound = 14 * time.Second

// ToolCallWaitBound is the shortest total bound an Eventually that makes a
// poll-bounded call should use: two poll bounds, so one stalled call leaves a
// second poll to observe the state the first could not. A shorter wait may
// still make the call (PollCall clamps it to the time the wait has left), but
// one stall then consumes the whole wait instead of being retried.
const ToolCallWaitBound = 2 * ToolCallPollBound

// TopicCallWaitBound is ToolCallWaitBound for a wait that polls a topic tool:
// two TopicCallPollBounds, so one slow join or leave leaves a second poll.
const TopicCallWaitBound = 2 * TopicCallPollBound

// LocalOptions configures a node that runs its processes directly on the
// machine running the tests.
type LocalOptions struct {
	// Name names the node in logs and artifact file names.
	Name string
}

// SessionSpec describes one simulated Claude Code session. It is the node
// library's specification, exposed under the harness name so the container
// backend accepts the same type.
type SessionSpec = node.SessionSpec

// BaseEnv is the complete environment of every child process a node starts.
type BaseEnv = node.BaseEnv

// BrokerSpec configures the Broker started by Node.StartBroker.
type BrokerSpec struct {
	// Args follow "service run" on the command line, for example
	// []string{"--listen", "127.0.0.1:41234"}; nil starts it without arguments.
	Args []string

	// ConfigYAML replaces the account's configuration file before the Broker
	// starts when it is non-empty. An empty value leaves the file alone.
	ConfigYAML string

	// Env adds to or overrides the node's base environment, for example
	// {"PEERS_TEST_CLOCK_FILE": path}. A value of "" removes the key.
	Env map[string]string

	// Bound bounds the readiness wait. Zero means 13s, the ten-second readiness
	// allowance plus three seconds.
	Bound time.Duration
}

// BrokerStart is the node library's start result: fields Running, PID,
// ExitCode and StderrLast.
type BrokerStart = node.StartResult

// ExecResult is the outcome of Node.Exec.
type ExecResult struct {
	ExitCode       int
	Stdout, Stderr string
}

// OracleClient reads Broker state over the local socket, without an Agent
// identity. Product errors (a gRPC status carrying an ErrorDetail) are
// returned in the second value; infrastructure errors fail the test.
type OracleClient struct {
	oracle *node.Oracle
	// remote carries the five RPCs to a node agent when the Broker runs in a
	// container; oracle is nil then.
	remote oracleRemote
}

// Hello calls Local.Hello on the Broker.
func (o *OracleClient) Hello(t testing.TB) *localpb.HelloResponse {
	t.Helper()
	if o.remote != nil {
		return o.remote.hello(t)
	}
	ctx, cancel := context.WithTimeout(context.Background(), oracleBound)
	defer cancel()
	resp, err := o.oracle.Hello(ctx)
	if err != nil {
		t.Fatalf("harness: calling Local.Hello: %v", err)
		return nil
	}
	return resp
}

// ListBrokers returns the Brokers known to this Broker.
func (o *OracleClient) ListBrokers(t testing.TB) (*localpb.ListBrokersResponse, *localpb.ErrorDetail) {
	t.Helper()
	if o.remote != nil {
		return o.remote.listBrokers(t)
	}
	ctx, cancel := context.WithTimeout(context.Background(), oracleBound)
	defer cancel()
	resp, err := o.oracle.ListBrokers(ctx)
	if detail := node.ErrorDetailOf(err); detail != nil {
		return nil, detail
	}
	if err != nil {
		t.Fatalf("harness: calling Local.ListBrokers: %v", err)
		return nil, nil
	}
	return resp, nil
}

// ListTopics returns the topics of the agent named by agentID and the public
// topics it has not joined. It reads the local interface directly, so only the
// local backend supports it; a container node cannot carry the agent id
// through its control request.
func (o *OracleClient) ListTopics(t testing.TB, agentID string) (*localpb.ListTopicsResponse, *localpb.ErrorDetail) {
	t.Helper()
	if o.remote != nil {
		t.Fatalf("harness: ListTopics is not available through a container node")
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), oracleBound)
	defer cancel()
	resp, err := o.oracle.ListTopics(ctx, agentID)
	if detail := node.ErrorDetailOf(err); detail != nil {
		return nil, detail
	}
	if err != nil {
		t.Fatalf("harness: calling Local.ListTopics: %v", err)
		return nil, nil
	}
	return resp, nil
}

// Status returns the Broker's status.
func (o *OracleClient) Status(t testing.TB) (*localpb.StatusResponse, *localpb.ErrorDetail) {
	t.Helper()
	if o.remote != nil {
		return o.remote.status(t)
	}
	ctx, cancel := context.WithTimeout(context.Background(), oracleBound)
	defer cancel()
	resp, err := o.oracle.Status(ctx)
	if detail := node.ErrorDetailOf(err); detail != nil {
		return nil, detail
	}
	if err != nil {
		t.Fatalf("harness: calling Local.Status: %v", err)
		return nil, nil
	}
	return resp, nil
}

// GetConfig returns the configuration entry for key, or every entry when key
// is empty.
func (o *OracleClient) GetConfig(t testing.TB, key string) (*localpb.GetConfigResponse, *localpb.ErrorDetail) {
	t.Helper()
	if o.remote != nil {
		return o.remote.getConfig(t, key)
	}
	ctx, cancel := context.WithTimeout(context.Background(), oracleBound)
	defer cancel()
	resp, err := o.oracle.GetConfig(ctx, key)
	if detail := node.ErrorDetailOf(err); detail != nil {
		return nil, detail
	}
	if err != nil {
		t.Fatalf("harness: calling Local.GetConfig: %v", err)
		return nil, nil
	}
	return resp, nil
}

// GetConfigRaw is GetConfig but returns a transport error instead of failing
// the test. A caller uses it to observe a Broker that is down and then wait for
// the same client to reconnect, which a call that failed the test could not do.
func (o *OracleClient) GetConfigRaw(key string) (*localpb.GetConfigResponse, error) {
	if o.remote != nil {
		// No raw variant crosses the node agent; this helper exists for a test
		// that runs the Broker on the test host.
		return nil, fmt.Errorf("GetConfigRaw is not available for a container node")
	}
	ctx, cancel := context.WithTimeout(context.Background(), oracleBound)
	defer cancel()
	return o.oracle.GetConfig(ctx, key)
}

// Audit reads the audit stream to its end and returns the events in the order
// the Broker sent them. A zero since does not set AuditRequest.since; a zero
// limit does not bound the answer.
func (o *OracleClient) Audit(t testing.TB, since time.Time, limit uint32) ([]*localpb.AuditEvent, *localpb.ErrorDetail) {
	t.Helper()
	if o.remote != nil {
		return o.remote.audit(t, since, limit)
	}
	ctx, cancel := context.WithTimeout(context.Background(), oracleBound)
	defer cancel()
	req := &localpb.AuditRequest{Limit: limit}
	if !since.IsZero() {
		req.Since = timestamppb.New(since)
	}
	events, err := o.oracle.Audit(ctx, req)
	if detail := node.ErrorDetailOf(err); detail != nil {
		return nil, detail
	}
	if err != nil {
		t.Fatalf("harness: calling Local.Audit: %v", err)
		return nil, nil
	}
	return events, nil
}

// close releases the underlying connection.
func (o *OracleClient) close() {
	if o.remote != nil {
		o.remote.close()
		return
	}
	if o.oracle != nil {
		_ = o.oracle.Close()
	}
}

// backend is what a Node delegates to. The local backend runs processes
// directly on the test host; the container backend, added with the container
// node work item, talks to a node agent in a container.
type backend interface {
	startBroker(t testing.TB, spec BrokerSpec) BrokerStart
	stopBroker(t testing.TB, sig syscall.Signal) int
	brokerLog(t testing.TB) []byte
	startSession(t testing.TB, spec SessionSpec) *Session
	tryStartSession(spec SessionSpec) (*Session, error)
	exec(t testing.TB, argv []string, env map[string]string) ExecResult
	execWithin(t testing.TB, argv []string, env map[string]string, timeout time.Duration) ExecResult
	oracle() *OracleClient
	freeAddr(t testing.TB) string
	accountHome() string
	writeConfig(t testing.TB, yaml string)
	readConfig(t testing.TB) []byte
	readFile(t testing.TB, path string) []byte
	workspace(t testing.TB, kind, origin, projectYAML string) string
}

// Node is one host in a scenario. Every method reports infrastructure errors
// with t.Fatalf.
type Node struct {
	impl backend
}

// StartBroker starts a Broker on the node and waits until it is ready.
func (n *Node) StartBroker(t testing.TB, spec BrokerSpec) BrokerStart {
	t.Helper()
	return n.impl.startBroker(t, spec)
}

// StopBroker sends sig to the current Broker, waits for it to exit and returns
// its exit code.
func (n *Node) StopBroker(t testing.TB, sig syscall.Signal) int {
	t.Helper()
	return n.impl.stopBroker(t, sig)
}

// BrokerLog returns the log of the most recently started Broker.
func (n *Node) BrokerLog(t testing.TB) []byte {
	t.Helper()
	return n.impl.brokerLog(t)
}

// StartSession starts one simulated Claude Code session on the node.
func (n *Node) StartSession(t testing.TB, spec SessionSpec) *Session {
	t.Helper()
	return n.impl.startSession(t, spec)
}

// TryStartSession starts one simulated Claude Code session and returns the
// error instead of failing the test. It is the entry point for a caller in
// another goroutine: testing.T must not be failed from a goroutine the test
// still has to join, so that caller reports the error on the test goroutine.
func (n *Node) TryStartSession(spec SessionSpec) (*Session, error) {
	return n.impl.tryStartSession(spec)
}

// Exec runs argv once as the node's account and returns its output and exit
// code.
func (n *Node) Exec(t testing.TB, argv []string, env map[string]string) ExecResult {
	t.Helper()
	return n.impl.exec(t, argv, env)
}

// ExecTimeout is Exec with an upper bound on the command. A command that does
// not finish fails the test instead of hanging it.
func (n *Node) ExecTimeout(t testing.TB, argv []string, env map[string]string, timeout time.Duration) ExecResult {
	t.Helper()
	return n.impl.execWithin(t, argv, env, timeout)
}

// Oracle returns a client for the local socket of the node's Broker.
func (n *Node) Oracle() *OracleClient {
	return n.impl.oracle()
}

// FreeAddr reserves an unused loopback address and returns it as
// 127.0.0.1:<port>.
func (n *Node) FreeAddr(t testing.TB) string {
	t.Helper()
	return n.impl.freeAddr(t)
}

// AccountHome returns the node account's home directory.
func (n *Node) AccountHome() string {
	return n.impl.accountHome()
}

// WriteConfig replaces the account's configuration file with yaml.
func (n *Node) WriteConfig(t testing.TB, yaml string) {
	t.Helper()
	n.impl.writeConfig(t, yaml)
}

// ReadConfig returns the raw bytes of the account's configuration file, or nil
// when the file does not exist.
func (n *Node) ReadConfig(t testing.TB) []byte {
	t.Helper()
	return n.impl.readConfig(t)
}

// ReadFile returns the contents of path as the node's account reads it.
func (n *Node) ReadFile(t testing.TB, path string) []byte {
	t.Helper()
	return n.impl.readFile(t, path)
}

// Workspace creates a workspace on the node and returns its path. kind is
// "plain", "git" or "project"; origin is the git remote that git and project
// workspaces use; projectYAML is the project configuration file content a
// project workspace writes.
func (n *Node) Workspace(t testing.TB, kind, origin, projectYAML string) string {
	t.Helper()
	return n.impl.workspace(t, kind, origin, projectYAML)
}

// sessionBackend is what a Session delegates to. callErrWithin is the single
// place that names the session and the tool in a failed call: Call reports its
// error unchanged, so the message carries that prefix once.
type sessionBackend interface {
	callErrWithin(bound time.Duration, tool string, args map[string]any) (Result, error)
	mcpLogTail(lines int) (string, bool)
	tools(t testing.TB) []node.ToolInfo
	stopMCP(t testing.TB, sig syscall.Signal) int
	restartMCP(t testing.TB)
	startExtraMCP(t testing.TB)
	inboxConnections(t testing.TB) int
	fake() FakeSession
	takeFakeErrors(t testing.TB) []error
	end() error
}

// Session is one simulated Claude Code session.
type Session struct {
	name string
	impl sessionBackend

	mu    sync.Mutex
	ended bool

	// pollLogOnce makes the first failed poll call of this session also log the
	// MCP server's standard error; later failures add only their own line.
	pollLogOnce sync.Once
}

// Call invokes a tool on the session's current MCP server process. A call that
// fails at the transport or protocol level fails the test with the backend's
// error as it is: callErrWithin already names the session and the tool.
func (s *Session) Call(t testing.TB, tool string, args map[string]any) Result {
	t.Helper()
	result, err := s.impl.callErrWithin(sessionCallBound, tool, args)
	if err != nil {
		t.Fatalf("%v", err)
		return Result{}
	}
	return result
}

// CallErr invokes a tool and returns a transport or protocol error instead of
// failing the test. A tool that reports an error inside its result still
// returns a nil error, as Call does. Callers in another goroutine use this and
// report the error on the test goroutine.
func (s *Session) CallErr(tool string, args map[string]any) (Result, error) {
	return s.impl.callErrWithin(sessionCallBound, tool, args)
}

// CallErrWithin is CallErr bounded by bound instead of sessionCallBound. A
// bound of zero or less means no bound beyond the call's own. A call made from
// inside a wait uses PollCall instead, which also clamps the bound to the time
// the wait has left.
func (s *Session) CallErrWithin(bound time.Duration, tool string, args map[string]any) (Result, error) {
	return s.impl.callErrWithin(bound, tool, args)
}

// PollCall invokes a tool from inside a wait. The call is bounded by
// ToolCallPollBound and, when the wait has less time left, by the time left to
// deadline, so a stalled call never outlives the wait that made it. A transport
// or protocol failure is returned as an error instead of failing the test, so
// the wait can poll again.
//
// Every failure is logged with the session, the tool and how long the call
// took. A wait that recovers on its next poll would otherwise pass silently;
// the log keeps the stall visible. The first failure of the session also logs
// the tail of the MCP server's standard error, where a Broker round trip that
// stalled leaves what evidence it produces; later failures add only the line.
func (s *Session) PollCall(t testing.TB, deadline time.Time, tool string, args map[string]any) (Result, error) {
	t.Helper()
	return s.PollCallWithin(t, deadline, ToolCallPollBound, tool, args)
}

// PollCallWithin is PollCall with a bound chosen by the caller instead of
// ToolCallPollBound. A topic tool uses TopicCallPollBound: its handler may spend
// topicRPCTimeout before the update hint runs. The bound is still clamped to the
// time left to deadline.
func (s *Session) PollCallWithin(t testing.TB, deadline time.Time, pollBound time.Duration, tool string, args map[string]any) (Result, error) {
	t.Helper()
	bound := pollBound
	if left := time.Until(deadline); left < bound {
		if left <= 0 {
			return Result{}, fmt.Errorf("harness: session %s: calling %s: no time left in the wait", s.name, tool)
		}
		bound = left
	}
	start := time.Now()
	result, err := s.impl.callErrWithin(bound, tool, args)
	if err == nil {
		return result, nil
	}
	t.Logf("harness: session %s: %s failed after %s: %v", s.name, tool, time.Since(start), err)
	s.pollLogOnce.Do(func() {
		tail, ok := s.impl.mcpLogTail(20)
		switch {
		case !ok:
			t.Logf("harness: session %s: no MCP server log is available", s.name)
		case tail == "":
			t.Logf("harness: session %s: the MCP server log is empty", s.name)
		default:
			t.Logf("harness: session %s: MCP server log tail:\n%s", s.name, tail)
		}
	})
	return result, err
}

// Tools lists the tools of the session's current MCP server process, with the
// annotations tools/list returned for each.
func (s *Session) Tools(t testing.TB) []node.ToolInfo {
	t.Helper()
	return s.impl.tools(t)
}

// StopMCP sends sig to the current MCP server process, escalates to SIGTERM
// and SIGKILL, and returns the process exit code.
func (s *Session) StopMCP(t testing.TB, sig syscall.Signal) int {
	t.Helper()
	return s.impl.stopMCP(t, sig)
}

// RestartMCP starts a new MCP server process after the previous one stopped.
func (s *Session) RestartMCP(t testing.TB) {
	t.Helper()
	s.impl.restartMCP(t)
}

// StartExtraMCP starts another MCP server process while the current one keeps
// running.
func (s *Session) StartExtraMCP(t testing.TB) {
	t.Helper()
	s.impl.startExtraMCP(t)
}

// InboxConnections counts the connections writing to the session's inbox
// socket.
func (s *Session) InboxConnections(t testing.TB) int {
	t.Helper()
	return s.impl.inboxConnections(t)
}

// Fake returns the simulated session: the real fakesession.Fake on a local
// node, a control-protocol proxy on a container node.
func (s *Session) Fake() FakeSession {
	return s.impl.fake()
}

// TakeFakeErrors returns the background failures the simulated session
// recorded since the previous call and forgets them. A failure here means the
// fake's accept loop or connection handling broke, which the test must not
// pass over, so the caller reports what it gets.
func (s *Session) TakeFakeErrors(t testing.TB) []error {
	t.Helper()
	return s.impl.takeFakeErrors(t)
}

// WaitFrame waits up to bound for the next frame delivered to the session's
// inbox and fails the test when none arrives.
func (s *Session) WaitFrame(t testing.TB, bound time.Duration) fakesession.Frame {
	t.Helper()
	frame, ok := s.Fake().WaitFrame(bound)
	if !ok {
		t.Fatalf("harness: session %s: no inbox frame within %s", s.name, bound)
	}
	return frame
}

// End ends the session: the MCP server process receives SIGINT with the same
// signal escalation as StopMCP, then the record is removed and the inbox
// socket closed. It is idempotent.
func (s *Session) End(t testing.TB) {
	t.Helper()
	if err := s.endIfNotEnded(); err != nil {
		t.Fatalf("harness: session %s: ending: %v", s.name, err)
	}
}

// endIfNotEnded runs the backend end exactly once and reports whether it was
// already ended before the call.
func (s *Session) endIfNotEnded() error {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return nil
	}
	s.ended = true
	s.mu.Unlock()
	return s.impl.end()
}
