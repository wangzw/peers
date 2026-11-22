package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakesession"
)

const (
	// agentCallTimeout bounds one MCP tool call when the request sets none.
	agentCallTimeout = 30 * time.Second
	// agentRPCBound bounds one call of the local interface probe.
	agentRPCBound = 10 * time.Second
	// agentFrameBound bounds a session frame wait when the request sets none.
	// It waits for an event the test cannot force, so it uses the package
	// fallback rather than a few seconds.
	agentFrameBound = eventTimeout
	// agentExecBound bounds one one-off command when the request sets none.
	agentExecBound = 30 * time.Second
	// agentBrokerStopBound is how long Close waits for the Broker to honour
	// SIGTERM before killing it.
	agentBrokerStopBound = 2 * time.Second
	// agentDialBound bounds the TCP probes when the request sets none. This is
	// not a wait for an event: a probe reports the address unreachable when its
	// dial times out, so the bound is the probe's own conclusion and stays
	// short. A caller that probes a blackhole passes its own timeout.
	agentDialBound = 3 * time.Second
	// agentConnAliveRead is the read deadline of the connection liveness probe.
	agentConnAliveRead = 200 * time.Millisecond
)

// AgentSessionSpec describes a session to start through the node agent. It is
// the operational form of a control-protocol session request.
type AgentSessionSpec struct {
	Name            string
	Status          string
	ConfigDir       string
	ExportConfigDir bool
	Workdir         string
	Mode            string
	BusyDelay       time.Duration
	NoBind          bool
	SocketPath      string
	NoSocketEnv     bool
	Env             map[string]string
	StartMCP        bool
}

// AgentSessionInfo is what the node agent reports after starting a session.
type AgentSessionInfo struct {
	ID         string
	SocketPath string
	ConfigDir  string
	RecordPath string
	FakePID    int
	MCPPID     int
}

// AgentCallResult is one tool call as reported by the node agent.
type AgentCallResult struct {
	IsError    bool
	Text       string
	Structured []byte
	Elapsed    time.Duration
}

// AgentFrame is one frame recorded by a simulated session. The fields mirror
// fakesession.Frame so the host-side proxy can reconstruct it.
type AgentFrame struct {
	Raw       string
	Auth      bool
	AuthRaw   string
	MsgID     string
	Type      string
	FromName  string
	From      string
	SessionID string
	Priority  string
	Content   string
	Fields    map[string]any
}

// AgentFrames is the answer of a frame wait: what was recorded and whether the
// requested frame had arrived.
type AgentFrames struct {
	Frames []AgentFrame
	OK     bool
}

// AgentMCPResult reports the outcome of an MCP server process operation.
type AgentMCPResult struct {
	ExitCode int
	PID      int
}

// AgentBrokerSpec describes a Broker process to start through the node agent.
type AgentBrokerSpec struct {
	Args       []string
	ConfigYAML string
	Env        map[string]string
	Bound      time.Duration
}

// AgentLocalRequest is one call of the local interface probe.
type AgentLocalRequest struct {
	RPC   string
	Key   string
	Since string
	Limit uint32
}

// AgentLocalResult carries a local interface answer. Response and ErrorDetail
// hold protojson; Events holds one protojson object per audit event.
type AgentLocalResult struct {
	Response    []byte
	ErrorDetail []byte
	Events      [][]byte
}

// AgentWorkspace describes a workspace to create.
type AgentWorkspace struct {
	Kind        string
	Origin      string
	ProjectYAML string
}

// AgentExecResult is the outcome of one one-off command.
type AgentExecResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
}

// agentSession is one session started through the node agent plus the extra
// MCP server processes a subagent definition declares.
type agentSession struct {
	session *Session

	mu     sync.Mutex
	extras []*MCPProc
}

// Agent runs the node library as a resident process. Every operation runs in
// Guard, so a Fatalf from fakesession or from the agent itself becomes an
// ordinary error for the requester instead of ending the process.
type Agent struct {
	tb     *TB
	env    BaseEnv
	binary string
	logDir string

	mu          sync.Mutex
	brokerSock  string
	sessions    map[string]*agentSession
	sessionSeq  int
	broker      *BrokerProc
	brokerLog   string
	brokerLogs  []string
	brokerSeq   int
	ports       map[string]net.Listener
	portSeq     int
	conns       map[string]net.Conn
	connSeq     int
	flood       *floodRun
	relay       *Relay
	shutdown    chan struct{}
	shutdownOne sync.Once
	closeOne    sync.Once
	closeErr    error
}

// NewAgent returns a node agent that starts sessions and Brokers from binary,
// writes their logs under logDir, and reaches the Broker on the local socket
// implied by env.
func NewAgent(tb *TB, env BaseEnv, binary, logDir string) *Agent {
	uid := strconv.Itoa(os.Geteuid())
	socket := filepath.Join("/tmp", "peers-"+uid, "broker.sock")
	if root := env.Extra["PEERS_TEST_RUNTIME_ROOT"]; root != "" {
		socket = filepath.Join(root, uid, "broker.sock")
	}
	return &Agent{
		tb:         tb,
		env:        env,
		binary:     binary,
		logDir:     logDir,
		brokerSock: socket,
		sessions:   make(map[string]*agentSession),
		ports:      make(map[string]net.Listener),
		conns:      make(map[string]net.Conn),
		shutdown:   make(chan struct{}),
	}
}

// Done is closed when Shutdown is called.
func (a *Agent) Done() <-chan struct{} { return a.shutdown }

// Shutdown asks the process that owns the agent to stop. It is idempotent.
func (a *Agent) Shutdown() {
	a.shutdownOne.Do(func() { close(a.shutdown) })
}

// Close ends every session, stops the Broker, closes the relay, the held
// ports and the open probe connections, and releases the temporary
// directories. It is idempotent.
func (a *Agent) Close() error {
	a.closeOne.Do(func() {
		a.mu.Lock()
		sessions := make([]*agentSession, 0, len(a.sessions))
		for _, s := range a.sessions {
			sessions = append(sessions, s)
		}
		a.sessions = nil
		broker := a.broker
		a.broker = nil
		relay := a.relay
		a.relay = nil
		flood := a.flood
		a.flood = nil
		ports := a.ports
		a.ports = nil
		conns := a.conns
		a.conns = nil
		a.mu.Unlock()

		for _, s := range sessions {
			if err := s.session.End(); err != nil && a.closeErr == nil {
				a.closeErr = err
			}
		}
		if broker != nil {
			_ = broker.Signal(syscall.SIGTERM)
			if _, err := broker.Wait(agentBrokerStopBound); err != nil {
				_ = broker.Signal(syscall.SIGKILL)
				_, _ = broker.Wait(agentBrokerStopBound)
			}
		}
		if relay != nil {
			_ = relay.Close()
		}
		if flood != nil {
			flood.stop()
		}
		for _, listener := range ports {
			_ = listener.Close()
		}
		for _, conn := range conns {
			_ = conn.Close()
		}
		a.tb.Close()
	})
	return a.closeErr
}

// StartSession starts one simulated session.
func (a *Agent) StartSession(spec AgentSessionSpec) (AgentSessionInfo, error) {
	var info AgentSessionInfo
	err := Guard(func() { info = a.startSession(spec) })
	if err != nil {
		return AgentSessionInfo{}, err
	}
	return info, nil
}

// startSession implements StartSession inside Guard.
func (a *Agent) startSession(spec AgentSessionSpec) AgentSessionInfo {
	mode, err := ParseMode(spec.Mode)
	if err != nil {
		a.tb.Fatalf("%v", err)
	}
	session, err := StartSession(context.Background(), a.tb, a.env, MCPCommand{Path: a.binary, Args: []string{"mcp"}}, SessionSpec{
		Name:            spec.Name,
		Status:          spec.Status,
		ConfigDir:       spec.ConfigDir,
		ExportConfigDir: spec.ExportConfigDir,
		Workdir:         spec.Workdir,
		Mode:            mode,
		BusyDelay:       spec.BusyDelay,
		NoBind:          spec.NoBind,
		SocketPath:      spec.SocketPath,
		NoSocketEnv:     spec.NoSocketEnv,
		Env:             spec.Env,
		StartMCP:        spec.StartMCP,
	})
	if err != nil {
		a.tb.Fatalf("%v", err)
	}

	a.mu.Lock()
	a.sessionSeq++
	id := "s" + strconv.Itoa(a.sessionSeq)
	a.sessions[id] = &agentSession{session: session}
	a.mu.Unlock()

	fake := session.Fake()
	return AgentSessionInfo{
		ID:         id,
		SocketPath: fake.SocketPath(),
		ConfigDir:  fake.ConfigDir(),
		RecordPath: fake.RecordPath(),
		FakePID:    fake.PID(),
		MCPPID:     session.MCPPID(),
	}
}

// Call invokes a tool on a session's main MCP server process, or on one of the
// extra processes when extra is greater than zero.
func (a *Agent) Call(sessionID, tool string, args map[string]any, timeout time.Duration, extra int) (AgentCallResult, error) {
	var result AgentCallResult
	err := Guard(func() { result = a.call(sessionID, tool, args, timeout, extra) })
	if err != nil {
		return AgentCallResult{}, err
	}
	return result, nil
}

// call implements Call inside Guard.
func (a *Agent) call(sessionID, tool string, args map[string]any, timeout time.Duration, extra int) AgentCallResult {
	s := a.session(sessionID)
	if timeout <= 0 {
		timeout = agentCallTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var (
		result ToolResult
		err    error
	)
	if extra == 0 {
		result, err = s.session.Call(ctx, tool, args)
	} else {
		proc := s.extra(extra)
		result, err = proc.Call(ctx, tool, args)
	}
	if err != nil {
		a.tb.Fatalf("%v", err)
	}
	return AgentCallResult{
		IsError:    result.IsError,
		Text:       result.Text,
		Structured: result.Structured,
		Elapsed:    result.Elapsed,
	}
}

// Tools lists the tools of a session's main MCP server process, with their
// annotations.
func (a *Agent) Tools(sessionID string) ([]ToolInfo, error) {
	var tools []ToolInfo
	err := Guard(func() { tools = a.tools(sessionID) })
	if err != nil {
		return nil, err
	}
	return tools, nil
}

// tools implements Tools inside Guard.
func (a *Agent) tools(sessionID string) []ToolInfo {
	s := a.session(sessionID)
	ctx, cancel := context.WithTimeout(context.Background(), agentCallTimeout)
	defer cancel()
	tools, err := s.session.Tools(ctx)
	if err != nil {
		a.tb.Fatalf("%v", err)
	}
	return tools
}

// Frames returns the frames a session recorded. When waitN is zero it returns
// at once; otherwise it waits up to timeout for the waitN-th frame.
func (a *Agent) Frames(sessionID string, waitN int, timeout time.Duration) (AgentFrames, error) {
	var result AgentFrames
	err := Guard(func() { result = a.frames(sessionID, waitN, timeout) })
	if err != nil {
		return AgentFrames{}, err
	}
	return result, nil
}

// frames implements Frames inside Guard.
func (a *Agent) frames(sessionID string, waitN int, timeout time.Duration) AgentFrames {
	s := a.session(sessionID)
	ok := true
	if waitN > 0 {
		if timeout <= 0 {
			timeout = agentFrameBound
		}
		_, ok = s.session.Fake().WaitFrameN(waitN, timeout)
	}
	recorded := s.session.Fake().Frames()
	frames := make([]AgentFrame, 0, len(recorded))
	for _, frame := range recorded {
		frames = append(frames, AgentFrame{
			Raw:       string(frame.Raw),
			Auth:      frame.Auth,
			AuthRaw:   string(frame.AuthRaw),
			MsgID:     frame.MsgID,
			Type:      frame.Type,
			FromName:  frame.FromName,
			From:      frame.From,
			SessionID: frame.SessionID,
			Priority:  frame.Priority,
			Content:   frame.Content,
			Fields:    frame.Fields,
		})
	}
	return AgentFrames{Frames: frames, OK: ok}
}

// Fake applies one operation to a session's simulated session.
func (a *Agent) Fake(sessionID, op string, arg any) error {
	return Guard(func() { a.fake(sessionID, op, arg) })
}

// fake implements Fake inside Guard.
func (a *Agent) fake(sessionID, op string, arg any) {
	fake := a.session(sessionID).session.Fake()
	switch op {
	case "rename":
		fake.Rename(stringArg(arg))
	case "set_status":
		fake.SetStatus(stringArg(arg))
	case "set_busy":
		fake.SetBusy(boolArg(arg))
	case "set_session_id":
		fake.SetSessionID(stringArg(arg))
	case "remove_record":
		fake.RemoveRecord()
	case "restore_record":
		fake.RestoreRecord()
	case "remove_socket":
		fake.RemoveSocket()
	case "set_mode":
		mode, err := ParseMode(stringArg(arg))
		if err != nil {
			a.tb.Fatalf("%v", err)
		}
		fake.SetMode(mode)
	case "set_busy_delay":
		fake.SetBusyDelay(time.Duration(intArg(arg)) * time.Millisecond)
	case "release":
		fake.Release()
	case "inject_background_error":
		fake.InjectBackgroundError(errors.New(stringArg(arg)))
	default:
		a.tb.Fatalf("node: unsupported session operation %q", op)
	}
}

// SessionErrors returns the background failures a session's simulated session
// recorded since the previous call, in the order they were recorded.
func (a *Agent) SessionErrors(sessionID string) ([]string, error) {
	var errs []string
	err := Guard(func() {
		for _, err := range a.session(sessionID).session.Fake().TakeErrors() {
			errs = append(errs, err.Error())
		}
	})
	if err != nil {
		return nil, err
	}
	return errs, nil
}

// MCP applies one operation to the MCP server processes of a session.
func (a *Agent) MCP(sessionID, op string, sig syscall.Signal) (AgentMCPResult, error) {
	var result AgentMCPResult
	err := Guard(func() { result = a.mcp(sessionID, op, sig) })
	if err != nil {
		return AgentMCPResult{}, err
	}
	return result, nil
}

// mcp implements MCP inside Guard.
func (a *Agent) mcp(sessionID, op string, sig syscall.Signal) AgentMCPResult {
	s := a.session(sessionID)
	switch op {
	case "stop":
		pid := s.session.MCPPID()
		code, err := s.session.StopMCP(sig)
		if err != nil {
			a.tb.Fatalf("%v", err)
		}
		return AgentMCPResult{ExitCode: code, PID: pid}
	case "restart":
		ctx, cancel := context.WithTimeout(context.Background(), agentCallTimeout)
		defer cancel()
		if err := s.session.RestartMCP(ctx); err != nil {
			a.tb.Fatalf("%v", err)
		}
		return AgentMCPResult{PID: s.session.MCPPID()}
	case "extra":
		ctx, cancel := context.WithTimeout(context.Background(), agentCallTimeout)
		defer cancel()
		proc, err := s.session.StartExtraMCP(ctx)
		if err != nil {
			a.tb.Fatalf("%v", err)
		}
		s.mu.Lock()
		s.extras = append(s.extras, proc)
		s.mu.Unlock()
		return AgentMCPResult{PID: proc.pid}
	case "stop_extra":
		s.mu.Lock()
		if len(s.extras) == 0 {
			s.mu.Unlock()
			a.tb.Fatalf("node: session %q has no extra MCP server process", sessionID)
		}
		proc := s.extras[len(s.extras)-1]
		s.extras = s.extras[:len(s.extras)-1]
		s.mu.Unlock()
		code, err := proc.Stop(sig)
		if err != nil {
			a.tb.Fatalf("%v", err)
		}
		return AgentMCPResult{ExitCode: code, PID: proc.pid}
	default:
		a.tb.Fatalf("node: unknown MCP operation %q", op)
	}
	return AgentMCPResult{}
}

// EndSession ends a session and forgets it.
func (a *Agent) EndSession(sessionID string) error {
	return Guard(func() {
		s := a.session(sessionID)
		if err := s.session.End(); err != nil {
			a.tb.Fatalf("%v", err)
		}
		a.mu.Lock()
		delete(a.sessions, sessionID)
		a.mu.Unlock()
	})
}

// WaitNoConnections waits until a session's simulated session tracks no inbox
// connection, up to timeout. It wakes when the fake accepts or drops a
// connection, so it returns as soon as the last handler leaves its
// connection. It exists for the end-to-end test harness only; the node agent
// exposes it on the test control socket, not in product behaviour.
func (a *Agent) WaitNoConnections(sessionID string, timeout time.Duration) (bool, error) {
	var ok bool
	err := Guard(func() {
		ok = a.session(sessionID).session.Fake().WaitNoConnections(timeout)
	})
	if err != nil {
		return false, err
	}
	return ok, nil
}

// InboxConnections reports how many peers are connected to a session's inbox
// socket.
func (a *Agent) InboxConnections(sessionID string) (int, error) {
	var n int
	err := Guard(func() {
		s := a.session(sessionID)
		count, err := s.session.InboxConnections()
		if err != nil {
			a.tb.Fatalf("%v", err)
		}
		n = count
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// StartBroker starts a Broker process and waits for it to become ready or to
// exit.
func (a *Agent) StartBroker(spec AgentBrokerSpec) (StartResult, error) {
	var result StartResult
	err := Guard(func() { result = a.startBroker(spec) })
	if err != nil {
		return StartResult{}, err
	}
	return result, nil
}

// startBroker implements StartBroker inside Guard.
func (a *Agent) startBroker(spec AgentBrokerSpec) StartResult {
	if spec.ConfigYAML != "" {
		a.writeAccountConfig(spec.ConfigYAML)
	}

	a.mu.Lock()
	a.brokerSeq++
	n := a.brokerSeq
	a.mu.Unlock()
	logPath := filepath.Join(a.logDir, fmt.Sprintf("broker-%d.log", n))

	proc, result, err := StartBroker(context.Background(), BrokerSpec{
		Path:    a.binary,
		Args:    spec.Args,
		Env:     a.env.Build(spec.Env),
		LogPath: logPath,
		Socket:  a.brokerSock,
	}, spec.Bound)
	if err != nil {
		a.tb.Fatalf("%v", err)
	}

	a.mu.Lock()
	a.broker = proc
	a.brokerLog = logPath
	a.brokerLogs = append(a.brokerLogs, logPath)
	a.mu.Unlock()
	return result
}

// SignalBroker sends sig to the current Broker through its process handle. The
// handle holds its own lifecycle lock across the reaped check and the send, so
// a Broker this agent already waited for is never signalled through a pid that
// may name another process. The agent runs in the same container as the
// Broker, so the handle's pid is reachable; a test process on the host cannot
// signal a pid that does not name a process above its own pid namespace.
func (a *Agent) SignalBroker(sig syscall.Signal) error {
	return Guard(func() {
		a.mu.Lock()
		broker := a.broker
		a.mu.Unlock()
		if broker == nil {
			a.tb.Fatalf("node: no Broker has been started")
		}
		if err := broker.SendSignal(sig); err != nil {
			a.tb.Fatalf("%v", err)
		}
	})
}

// WaitBroker waits up to bound for the current Broker to exit and returns its
// exit code.
func (a *Agent) WaitBroker(bound time.Duration) (int, error) {
	var code int
	err := Guard(func() {
		a.mu.Lock()
		broker := a.broker
		a.mu.Unlock()
		if broker == nil {
			a.tb.Fatalf("node: no Broker has been started")
		}
		exitCode, err := broker.Wait(bound)
		if err != nil {
			a.tb.Fatalf("%v", err)
		}
		code = exitCode
	})
	if err != nil {
		return 0, err
	}
	return code, nil
}

// BrokerLog returns the current Broker log, or every Broker log when all is
// set.
func (a *Agent) BrokerLog(all bool) ([]byte, error) {
	var data []byte
	err := Guard(func() { data = a.brokerLogData(all) })
	if err != nil {
		return nil, err
	}
	return data, nil
}

// brokerLogData implements BrokerLog inside Guard.
func (a *Agent) brokerLogData(all bool) []byte {
	a.mu.Lock()
	paths := append([]string(nil), a.brokerLogs...)
	current := a.brokerLog
	a.mu.Unlock()
	if !all {
		paths = nil
		if current != "" {
			paths = []string{current}
		}
	}
	var out []byte
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			a.tb.Fatalf("node: reading Broker log %s: %v", path, err)
		}
		out = append(out, data...)
	}
	return out
}

// Local calls one RPC of the local interface and returns its protojson answer.
func (a *Agent) Local(req AgentLocalRequest) (AgentLocalResult, error) {
	var result AgentLocalResult
	err := Guard(func() { result = a.local(req) })
	if err != nil {
		return AgentLocalResult{}, err
	}
	return result, nil
}

// local implements Local inside Guard.
func (a *Agent) local(req AgentLocalRequest) AgentLocalResult {
	oracle, err := DialOracle(a.brokerSock)
	if err != nil {
		a.tb.Fatalf("%v", err)
	}
	defer func() { _ = oracle.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), agentRPCBound)
	defer cancel()

	result := AgentLocalResult{}
	switch req.RPC {
	case "hello":
		response, err := oracle.Hello(ctx)
		a.encodeAnswer(&result, response, err)
	case "list_brokers":
		response, err := oracle.ListBrokers(ctx)
		a.encodeAnswer(&result, response, err)
	case "status":
		response, err := oracle.Status(ctx)
		a.encodeAnswer(&result, response, err)
	case "get_config":
		response, err := oracle.GetConfig(ctx, req.Key)
		a.encodeAnswer(&result, response, err)
	case "audit":
		auditReq := &localpb.AuditRequest{Limit: req.Limit}
		if req.Since != "" {
			since, err := time.Parse(time.RFC3339, req.Since)
			if err != nil {
				a.tb.Fatalf("node: parsing audit since %q: %v", req.Since, err)
			}
			auditReq.Since = timestamppb.New(since)
		}
		events, err := oracle.Audit(ctx, auditReq)
		if err != nil {
			a.tb.Fatalf("%v", err)
		}
		result.Events = make([][]byte, 0, len(events))
		for _, event := range events {
			data, err := protojson.Marshal(event)
			if err != nil {
				a.tb.Fatalf("node: encoding audit event: %v", err)
			}
			result.Events = append(result.Events, data)
		}
	default:
		a.tb.Fatalf("node: unknown local RPC %q", req.RPC)
	}
	return result
}

// encodeAnswer stores a protojson response, or the ErrorDetail of a product
// error, in result.
func (a *Agent) encodeAnswer(result *AgentLocalResult, message proto.Message, err error) {
	if err != nil {
		detail := ErrorDetailOf(err)
		if detail == nil {
			a.tb.Fatalf("%v", err)
		}
		data, encodeErr := protojson.Marshal(detail)
		if encodeErr != nil {
			a.tb.Fatalf("node: encoding error detail: %v", encodeErr)
		}
		result.ErrorDetail = data
		return
	}
	data, encodeErr := protojson.Marshal(message)
	if encodeErr != nil {
		a.tb.Fatalf("node: encoding local response: %v", encodeErr)
	}
	result.Response = data
}

// Procs lists this account's processes of the requested kind: broker, mcp or
// all.
func (a *Agent) Procs(kind string) ([]Proc, error) {
	var procs []Proc
	err := Guard(func() { procs = a.procs(kind) })
	if err != nil {
		return nil, err
	}
	return procs, nil
}

// procs implements Procs inside Guard.
func (a *Agent) procs(kind string) []Proc {
	if kind != "broker" && kind != "mcp" && kind != "all" {
		a.tb.Fatalf("node: unknown process kind %q", kind)
	}
	uid := os.Geteuid()
	procs, err := ListProcs(func(proc Proc) bool {
		if proc.UID != uid {
			return false
		}
		switch kind {
		case "broker":
			return hasServiceRun(proc.Args)
		case "mcp":
			return hasSubcommand(proc.Args, "mcp")
		default:
			return true
		}
	})
	if err != nil {
		a.tb.Fatalf("%v", err)
	}
	return procs
}

// hasSubcommand reports whether args carry subcommand as the first argument
// after the program name.
func hasSubcommand(args []string, subcommand string) bool {
	return len(args) >= 2 && args[1] == subcommand
}

// hasServiceRun reports whether args run the Broker through "service run".
func hasServiceRun(args []string) bool {
	return len(args) >= 3 && args[1] == "service" && args[2] == "run"
}

// Workspace creates a workspace of the requested kind under a temporary
// directory and returns its absolute path.
func (a *Agent) Workspace(req AgentWorkspace) (string, error) {
	var path string
	err := Guard(func() { path = a.workspace(req) })
	if err != nil {
		return "", err
	}
	return path, nil
}

// workspace implements Workspace inside Guard.
func (a *Agent) workspace(req AgentWorkspace) string {
	dir := a.tb.TempDir()
	env := a.env.Build(map[string]string{"GIT_CONFIG_NOSYSTEM": "1"})

	var (
		path string
		err  error
	)
	switch req.Kind {
	case "git":
		path, err = GitWorkspace(dir, req.Origin, env)
	case "project":
		path, err = ProjectWorkspace(dir, req.ProjectYAML, req.Origin, env)
	case "plain":
		path, err = PlainWorkspace(dir)
	default:
		a.tb.Fatalf("node: unknown workspace kind %q", req.Kind)
	}
	if err != nil {
		a.tb.Fatalf("%v", err)
	}
	return path
}

// ReadFile returns the contents of path. A missing file is reported as
// missing rather than as an error.
func (a *Agent) ReadFile(path string) ([]byte, bool, error) {
	var (
		data    []byte
		missing bool
	)
	err := Guard(func() { data, missing = a.readFile(path) })
	if err != nil {
		return nil, false, err
	}
	return data, missing, nil
}

// readFile implements ReadFile inside Guard.
func (a *Agent) readFile(path string) ([]byte, bool) {
	a.checkPath(path)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, true
	}
	if err != nil {
		a.tb.Fatalf("node: reading %s: %v", path, err)
	}
	return data, false
}

// WriteFile writes data to path, creating its parent directories first and
// replacing the file atomically.
func (a *Agent) WriteFile(path string, data []byte, mode os.FileMode) error {
	return Guard(func() { a.writeFile(path, data, mode) })
}

// writeFile implements WriteFile inside Guard.
func (a *Agent) writeFile(path string, data []byte, mode os.FileMode) {
	a.checkPath(path)
	if mode == 0 {
		mode = 0o600
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		a.tb.Fatalf("node: creating directory %s: %v", dir, err)
	}
	temp, err := os.CreateTemp(dir, ".ap-write-*")
	if err != nil {
		a.tb.Fatalf("node: creating temporary file in %s: %v", dir, err)
	}
	tempPath := temp.Name()
	cleanup := func() { _ = os.Remove(tempPath) }
	if err := temp.Chmod(mode); err != nil {
		_ = temp.Close()
		cleanup()
		a.tb.Fatalf("node: setting mode of %s: %v", tempPath, err)
	}
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		cleanup()
		a.tb.Fatalf("node: writing %s: %v", tempPath, err)
	}
	if err := temp.Close(); err != nil {
		cleanup()
		a.tb.Fatalf("node: closing %s: %v", tempPath, err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		cleanup()
		a.tb.Fatalf("node: renaming %s to %s: %v", tempPath, path, err)
	}
}

// ListFiles lists the names in a directory.
func (a *Agent) ListFiles(path string) ([]string, error) {
	var names []string
	err := Guard(func() { names = a.listFiles(path) })
	if err != nil {
		return nil, err
	}
	return names, nil
}

// listFiles implements ListFiles inside Guard.
func (a *Agent) listFiles(path string) []string {
	a.checkPath(path)
	entries, err := os.ReadDir(path)
	if err != nil {
		a.tb.Fatalf("node: listing %s: %v", path, err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return names
}

// checkPath fails unless path is under the account home or /tmp.
func (a *Agent) checkPath(path string) {
	abs, err := filepath.Abs(path)
	if err != nil {
		a.tb.Fatalf("node: resolving %s: %v", path, err)
	}
	for _, root := range a.pathRoots() {
		if pathWithin(abs, root) {
			return
		}
	}
	a.tb.Fatalf("node: path %s is outside the account home and /tmp", path)
}

// pathRoots returns the directories the agent is allowed to touch.
func (a *Agent) pathRoots() []string {
	roots := []string{"/tmp"}
	if a.env.Home != "" {
		roots = append(roots, a.env.Home)
	}
	if home := a.env.Extra["PEERS_TEST_HOME"]; home != "" {
		roots = append(roots, home)
	}
	abs := make([]string, 0, len(roots))
	for _, root := range roots {
		if resolved, err := filepath.Abs(root); err == nil {
			abs = append(abs, resolved)
		}
	}
	return abs
}

// pathWithin reports whether path is root or lies below it.
func pathWithin(path, root string) bool {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

// HoldPort listens on addr and returns an identifier and the bound address.
func (a *Agent) HoldPort(addr string) (string, string, error) {
	var (
		id    string
		bound string
	)
	err := Guard(func() { id, bound = a.holdPort(addr) })
	if err != nil {
		return "", "", err
	}
	return id, bound, nil
}

// holdPort implements HoldPort inside Guard.
func (a *Agent) holdPort(addr string) (string, string) {
	listener, err := HoldPort(addr)
	if err != nil {
		a.tb.Fatalf("%v", err)
	}
	a.mu.Lock()
	a.portSeq++
	id := "p" + strconv.Itoa(a.portSeq)
	a.ports[id] = listener
	a.mu.Unlock()
	return id, listener.Addr().String()
}

// ReleasePort closes a held port.
func (a *Agent) ReleasePort(id string) error {
	return Guard(func() {
		a.mu.Lock()
		listener, ok := a.ports[id]
		delete(a.ports, id)
		a.mu.Unlock()
		if !ok {
			a.tb.Fatalf("node: unknown port id %q", id)
		}
		if err := listener.Close(); err != nil {
			a.tb.Fatalf("node: releasing port %s: %v", id, err)
		}
	})
}

// ProbeTCP reports whether a TCP connection to addr succeeds within timeout.
func (a *Agent) ProbeTCP(addr string, timeout time.Duration) bool {
	if timeout <= 0 {
		timeout = agentDialBound
	}
	return TCPReachable(addr, timeout)
}

// OpenConn opens a TCP connection to addr and returns its identifier.
func (a *Agent) OpenConn(addr string, timeout time.Duration) (string, error) {
	var id string
	err := Guard(func() { id = a.openConn(addr, timeout) })
	if err != nil {
		return "", err
	}
	return id, nil
}

// openConn implements OpenConn inside Guard.
func (a *Agent) openConn(addr string, timeout time.Duration) string {
	if timeout <= 0 {
		timeout = agentDialBound
	}
	conn, err := OpenConn(addr, timeout)
	if err != nil {
		a.tb.Fatalf("%v", err)
	}
	a.mu.Lock()
	a.connSeq++
	id := "c" + strconv.Itoa(a.connSeq)
	a.conns[id] = conn
	a.mu.Unlock()
	return id
}

// ConnAlive reports whether an open probe connection still carries data.
func (a *Agent) ConnAlive(id string) (bool, error) {
	var alive bool
	err := Guard(func() { alive = a.connAlive(id) })
	if err != nil {
		return false, err
	}
	return alive, nil
}

// connAlive implements ConnAlive inside Guard.
func (a *Agent) connAlive(id string) bool {
	a.mu.Lock()
	conn, ok := a.conns[id]
	a.mu.Unlock()
	if !ok {
		a.tb.Fatalf("node: unknown connection id %q", id)
	}
	if _, err := conn.Write([]byte{0}); err != nil {
		return false
	}
	if err := conn.SetReadDeadline(time.Now().Add(agentConnAliveRead)); err != nil {
		return false
	}
	buffer := make([]byte, 1)
	if _, err := conn.Read(buffer); err != nil {
		return false
	}
	return true
}

// ProbeHello asks a Broker what it does with an unpaired client and returns
// the reject reason.
func (a *Agent) ProbeHello(addr string, timeout time.Duration) (string, error) {
	var reason string
	err := Guard(func() { reason = a.probeHello(addr, timeout) })
	if err != nil {
		return "", err
	}
	return reason, nil
}

// probeHello implements ProbeHello inside Guard.
func (a *Agent) probeHello(addr string, timeout time.Duration) string {
	if timeout <= 0 {
		timeout = agentDialBound
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	reason, err := ProbeHello(ctx, addr)
	if err != nil {
		a.tb.Fatalf("%v", err)
	}
	return reason.String()
}

// StartRelay starts the man-in-the-middle relay.
func (a *Agent) StartRelay(listen, upstream string) error {
	return Guard(func() {
		a.mu.Lock()
		existing := a.relay
		a.mu.Unlock()
		if existing != nil {
			a.tb.Fatalf("node: a relay is already running")
		}
		relay, err := StartRelay(listen, upstream)
		if err != nil {
			a.tb.Fatalf("%v", err)
		}
		a.mu.Lock()
		a.relay = relay
		a.mu.Unlock()
	})
}

// RelayStats reports the stream and frame counts of the relay.
func (a *Agent) RelayStats() (streams, framesUp, framesDown int, err error) {
	err = Guard(func() {
		a.mu.Lock()
		relay := a.relay
		a.mu.Unlock()
		if relay == nil {
			a.tb.Fatalf("node: no relay has been started")
		}
		streams, framesUp, framesDown = relay.Stats()
	})
	return streams, framesUp, framesDown, err
}

// Exec runs a one-off command as the node account.
func (a *Agent) Exec(argv []string, env map[string]string, timeout time.Duration) (AgentExecResult, error) {
	var result AgentExecResult
	err := Guard(func() { result = a.exec(argv, env, timeout) })
	if err != nil {
		return AgentExecResult{}, err
	}
	return result, nil
}

// exec implements Exec inside Guard.
func (a *Agent) exec(argv []string, env map[string]string, timeout time.Duration) AgentExecResult {
	if len(argv) == 0 {
		a.tb.Fatalf("node: exec needs a command")
	}
	if timeout <= 0 {
		timeout = agentExecBound
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir = a.env.Home
	cmd.Env = a.env.Build(env)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	result := AgentExecResult{}
	err := cmd.Run()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		result.ExitCode = 0
	case errors.As(err, &exitErr):
		result.ExitCode = exitErr.ExitCode()
	case ctx.Err() != nil:
		a.tb.Fatalf("node: %s did not finish within %s", argv[0], timeout)
	default:
		a.tb.Fatalf("node: running %s: %v", argv[0], err)
	}
	result.Stdout = stdout.String()
	result.Stderr = stderr.String()
	return result
}

// session returns a started session or fails.
func (a *Agent) session(id string) *agentSession {
	a.mu.Lock()
	s, ok := a.sessions[id]
	a.mu.Unlock()
	if !ok {
		a.tb.Fatalf("node: unknown session %q", id)
	}
	return s
}

// extra returns the extra MCP server process numbered n, counting from one.
func (s *agentSession) extra(n int) *MCPProc {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n < 1 || n > len(s.extras) {
		panic(FatalError{Msg: fmt.Sprintf("node: no extra MCP server process %d", n)})
	}
	return s.extras[n-1]
}

// accountHome returns the account home the Broker configuration lives in.
func (a *Agent) accountHome() string {
	if home := a.env.Extra["PEERS_TEST_HOME"]; home != "" {
		return home
	}
	return a.env.Home
}

// writeAccountConfig replaces the account Broker configuration with yaml.
func (a *Agent) writeAccountConfig(yaml string) {
	dir := filepath.Join(a.accountHome(), ".peers")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		a.tb.Fatalf("node: creating account directory %s: %v", dir, err)
	}
	path := filepath.Join(dir, "config.yaml")
	temp, err := os.CreateTemp(dir, ".config-*")
	if err != nil {
		a.tb.Fatalf("node: creating temporary configuration: %v", err)
	}
	tempPath := temp.Name()
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		_ = os.Remove(tempPath)
		a.tb.Fatalf("node: setting configuration mode: %v", err)
	}
	if _, err := temp.WriteString(yaml); err != nil {
		_ = temp.Close()
		_ = os.Remove(tempPath)
		a.tb.Fatalf("node: writing configuration: %v", err)
	}
	if err := temp.Close(); err != nil {
		_ = os.Remove(tempPath)
		a.tb.Fatalf("node: closing configuration: %v", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		_ = os.Remove(tempPath)
		a.tb.Fatalf("node: replacing configuration %s: %v", path, err)
	}
}

// ParseMode turns a control-protocol mode name into a fakesession mode.
func ParseMode(name string) (fakesession.Mode, error) {
	switch name {
	case "", "close-after-read":
		return fakesession.CloseAfterRead, nil
	case "hold-open":
		return fakesession.HoldOpen, nil
	case "close-immediately":
		return fakesession.CloseImmediately, nil
	default:
		return fakesession.CloseAfterRead, fmt.Errorf("node: unknown session mode %q", name)
	}
}

// stringArg returns arg as a string or fails.
func stringArg(arg any) string {
	value, ok := arg.(string)
	if !ok {
		panic(FatalError{Msg: fmt.Sprintf("node: expected a string argument, got %T", arg)})
	}
	return value
}

// boolArg returns arg as a bool or fails.
func boolArg(arg any) bool {
	value, ok := arg.(bool)
	if !ok {
		panic(FatalError{Msg: fmt.Sprintf("node: expected a bool argument, got %T", arg)})
	}
	return value
}

// intArg returns arg as an integer or fails.
func intArg(arg any) int {
	switch value := arg.(type) {
	case float64:
		return int(value)
	case int:
		return value
	default:
		panic(FatalError{Msg: fmt.Sprintf("node: expected a number argument, got %T", arg)})
	}
}
