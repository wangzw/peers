package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakesession"
	"github.com/wangzw/peers/test/e2e/ctl"
	"github.com/wangzw/peers/test/e2e/node"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// brokerStopBound bounds the wait for a Broker that was just signalled. The
// Broker exits as soon as its context is cancelled on SIGTERM and has no drain
// timer, so the project-wide three-second bound applies.
const brokerStopBound = 3 * time.Second

// oracleRemote is the container node's half of OracleClient: the five local
// interface RPCs travel over the control protocol to the node agent.
type oracleRemote interface {
	hello(t testing.TB) *localpb.HelloResponse
	listBrokers(t testing.TB) (*localpb.ListBrokersResponse, *localpb.ErrorDetail)
	status(t testing.TB) (*localpb.StatusResponse, *localpb.ErrorDetail)
	getConfig(t testing.TB, key string) (*localpb.GetConfigResponse, *localpb.ErrorDetail)
	audit(t testing.TB, since time.Time, limit uint32) ([]*localpb.AuditEvent, *localpb.ErrorDetail)
	close()
}

// remoteNode drives the node agent of one container over the control socket.
type remoteNode struct {
	t       *testing.T
	name    string
	socket  string
	client  *ctl.Client
	homeDir string
}

func (r *remoteNode) startBroker(tb testing.TB, spec BrokerSpec) BrokerStart {
	tb.Helper()
	res, err := r.client.StartBroker(context.Background(), ctl.BrokerStartRequest{
		Args:       spec.Args,
		ConfigYAML: spec.ConfigYAML,
		Env:        spec.Env,
		BoundMS:    spec.Bound.Milliseconds(),
	})
	if err != nil {
		tb.Fatalf("harness: node %s: starting Broker: %v", r.name, err)
		return BrokerStart{}
	}
	return BrokerStart{
		Running:    res.Running,
		PID:        res.PID,
		ExitCode:   res.ExitCode,
		StderrLast: res.StderrLast,
	}
}

func (r *remoteNode) stopBroker(tb testing.TB, sig syscall.Signal) int {
	tb.Helper()
	ctx := context.Background()
	if err := r.client.SignalBroker(ctx, ctl.BrokerSignalRequest{Signal: strconv.Itoa(int(sig))}); err != nil {
		tb.Fatalf("harness: node %s: signalling the Broker: %v", r.name, err)
		return -1
	}
	res, err := r.client.WaitBroker(ctx, ctl.BrokerWaitRequest{BoundMS: brokerStopBound.Milliseconds()})
	if err != nil {
		tb.Fatalf("harness: node %s: waiting for the Broker: %v", r.name, err)
		return -1
	}
	return res.ExitCode
}

func (r *remoteNode) brokerLog(tb testing.TB) []byte {
	tb.Helper()
	res, err := r.client.BrokerLog(context.Background(), ctl.BrokerLogRequest{})
	if err != nil {
		tb.Fatalf("harness: node %s: reading the Broker log: %v", r.name, err)
		return nil
	}
	return res.Data
}

func (r *remoteNode) startSession(tb testing.TB, spec SessionSpec) *Session {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), sessionStartBound)
	defer cancel()
	info, err := r.client.StartSession(ctx, ctl.SessionStartRequest{
		Name:            spec.Name,
		Status:          spec.Status,
		ConfigDir:       spec.ConfigDir,
		ExportConfigDir: spec.ExportConfigDir,
		Workdir:         spec.Workdir,
		Mode:            modeName(spec.Mode),
		BusyDelayMS:     spec.BusyDelay.Milliseconds(),
		NoBind:          spec.NoBind,
		SocketPath:      spec.SocketPath,
		NoSocketEnv:     spec.NoSocketEnv,
		Env:             spec.Env,
		StartMCP:        spec.StartMCP,
	})
	if err != nil {
		tb.Fatalf("harness: node %s: starting session %s: %v", r.name, spec.Name, err)
		return nil
	}

	return r.newSession(spec, info, tb)
}

// tryStartSession starts a session without failing the test, for a caller in
// another goroutine that reports the error itself.
func (r *remoteNode) tryStartSession(spec SessionSpec) (*Session, error) {
	ctx, cancel := context.WithTimeout(context.Background(), sessionStartBound)
	defer cancel()
	info, err := r.client.StartSession(ctx, ctl.SessionStartRequest{
		Name:            spec.Name,
		Status:          spec.Status,
		ConfigDir:       spec.ConfigDir,
		ExportConfigDir: spec.ExportConfigDir,
		Workdir:         spec.Workdir,
		Mode:            modeName(spec.Mode),
		BusyDelayMS:     spec.BusyDelay.Milliseconds(),
		NoBind:          spec.NoBind,
		SocketPath:      spec.SocketPath,
		NoSocketEnv:     spec.NoSocketEnv,
		Env:             spec.Env,
		StartMCP:        spec.StartMCP,
	})
	if err != nil {
		return nil, fmt.Errorf("harness: node %s: starting session %s: %w", r.name, spec.Name, err)
	}
	return r.newSession(spec, info, r.t), nil
}

// newSession builds the harness session for a start answer.
func (r *remoteNode) newSession(spec SessionSpec, info ctl.SessionInfo, tb testing.TB) *Session {
	s := &Session{
		name: spec.Name,
		impl: &remoteSession{
			name:   spec.Name,
			id:     info.ID,
			client: r.client,
			tb:     tb,
			proxy: &remoteFake{
				tb:     tb,
				name:   spec.Name,
				client: r.client,
				info:   info,
			},
		},
	}
	// A session that the test did not end itself is still ended before the
	// project goes down, so a background failure recorded in the container is
	// reported rather than lost with it.
	r.t.Cleanup(func() {
		if err := s.endIfNotEnded(); err != nil {
			r.t.Errorf("harness: session %s: %v", s.name, err)
		}
	})
	return s
}

func (r *remoteNode) exec(tb testing.TB, argv []string, env map[string]string) ExecResult {
	tb.Helper()
	return r.execWithin(tb, argv, env, 0)
}

func (r *remoteNode) execWithin(tb testing.TB, argv []string, env map[string]string, timeout time.Duration) ExecResult {
	tb.Helper()
	if len(argv) == 0 {
		tb.Fatalf("harness: node %s: Exec needs a command", r.name)
		return ExecResult{}
	}
	req := ctl.ExecRequest{Argv: argv, Env: env}
	if timeout > 0 {
		req.TimeoutMS = timeout.Milliseconds()
	}
	res, err := r.client.Exec(context.Background(), req)
	if err != nil {
		tb.Fatalf("harness: node %s: running %v: %v", r.name, argv, err)
		return ExecResult{}
	}
	return ExecResult{ExitCode: res.ExitCode, Stdout: res.Stdout, Stderr: res.Stderr}
}

func (r *remoteNode) oracle() *OracleClient {
	return &OracleClient{remote: &remoteOracle{name: r.name, client: r.client}}
}

func (r *remoteNode) freeAddr(tb testing.TB) string {
	tb.Helper()
	tb.Fatalf("harness: node %s: FreeAddr is only available on a local node; a container node listens on its own address", r.name)
	return ""
}

func (r *remoteNode) accountHome() string { return r.homeDir }

func (r *remoteNode) writeConfig(tb testing.TB, yaml string) {
	tb.Helper()
	r.writeFile(tb, r.configPath(), []byte(yaml), 0o600)
}

func (r *remoteNode) readConfig(tb testing.TB) []byte {
	tb.Helper()
	data, missing, err := r.readFileMaybe(tb, r.configPath())
	if err != nil {
		tb.Fatalf("harness: node %s: reading the configuration: %v", r.name, err)
		return nil
	}
	if missing {
		return nil
	}
	return data
}

func (r *remoteNode) readFile(tb testing.TB, path string) []byte {
	tb.Helper()
	data, missing, err := r.readFileMaybe(tb, path)
	if err != nil {
		tb.Fatalf("harness: node %s: reading %s: %v", r.name, path, err)
		return nil
	}
	if missing {
		tb.Fatalf("harness: node %s: reading %s: no such file", r.name, path)
		return nil
	}
	return data
}

// workspace creates a workspace through the node agent's workspace endpoint.
func (r *remoteNode) workspace(tb testing.TB, kind, origin, projectYAML string) string {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testWaitBound)
	defer cancel()
	res, err := r.client.Workspace(ctx, ctl.WorkspaceRequest{Kind: kind, Origin: origin, ProjectYAML: projectYAML})
	if err != nil {
		tb.Fatalf("harness: node %s: creating a %s workspace: %v", r.name, kind, err)
		return ""
	}
	return res.Path
}

// configPath is the account's configuration file inside the container.
func (r *remoteNode) configPath() string {
	return r.homeDir + "/.peers/config.yaml"
}

// readFileMaybe reads through the node agent, reporting a missing file
// separately.
func (r *remoteNode) readFileMaybe(tb testing.TB, path string) ([]byte, bool, error) {
	tb.Helper()
	res, err := r.client.ReadFile(context.Background(), ctl.FileReadRequest{Path: path})
	if err != nil {
		return nil, false, err
	}
	return res.Data, res.Missing, nil
}

// writeFile writes through the node agent with the given mode.
func (r *remoteNode) writeFile(tb testing.TB, path string, data []byte, mode uint32) {
	tb.Helper()
	if err := r.client.WriteFile(context.Background(), ctl.FileWriteRequest{Path: path, Data: data, Mode: mode}); err != nil {
		tb.Fatalf("harness: node %s: writing %s: %v", r.name, path, err)
	}
}

// modeName is the control protocol's name for a fakesession mode.
func modeName(mode fakesession.Mode) string {
	switch mode {
	case fakesession.HoldOpen:
		return "hold-open"
	case fakesession.CloseImmediately:
		return "close-immediately"
	default:
		return "close-after-read"
	}
}

// remoteSession drives one simulated session that lives in a container.
type remoteSession struct {
	name   string
	id     string
	client *ctl.Client
	tb     testing.TB
	proxy  *remoteFake
}

func (s *remoteSession) callErrWithin(bound time.Duration, tool string, args map[string]any) (Result, error) {
	ctx := context.Background()
	var timeoutMS int64
	if bound > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, bound)
		defer cancel()
		timeoutMS = bound.Milliseconds()
	}
	res, err := s.client.Call(ctx, ctl.SessionCallRequest{
		ID:        s.id,
		Tool:      tool,
		Args:      args,
		TimeoutMS: timeoutMS,
	})
	if err != nil {
		return Result{}, fmt.Errorf("harness: session %s: calling %s: %w", s.name, tool, err)
	}
	result := Result{ToolResult: node.ToolResult{
		IsError: res.IsError,
		Text:    res.Text,
		Elapsed: time.Duration(res.ElapsedMS) * time.Millisecond,
	}}
	if len(res.Structured) > 0 {
		result.Structured = json.RawMessage(res.Structured)
	}
	return result, nil
}

// mcpLogTail reports no log: the MCP server process runs inside a container and
// the harness does not carry the path of its standard error back here.
func (s *remoteSession) mcpLogTail(int) (string, bool) { return "", false }

// logBrokerStack asks the node agent to send SIGQUIT to the Broker inside the
// container, then reads the Broker log the runtime writes its stack to. The
// signal is delivered inside the container by the agent, which calls
// procsig.SendSignal there; a test process on the host cannot signal a pid in
// the container's namespace.
func (s *remoteSession) logBrokerStack(tb testing.TB) string {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.client.SignalBroker(ctx, ctl.BrokerSignalRequest{Signal: strconv.Itoa(int(syscall.SIGQUIT))}); err != nil {
		return fmt.Sprintf("signalling the Broker in the container: %v", err)
	}
	// The dump is written before the process exits; wait for the exit so the
	// log is complete.
	_, _ = s.client.WaitBroker(ctx, ctl.BrokerWaitRequest{BoundMS: 3000})
	res, err := s.client.BrokerLog(ctx, ctl.BrokerLogRequest{})
	if err != nil {
		return fmt.Sprintf("reading the Broker log in the container: %v", err)
	}
	return quitDump(res.Data)
}

func (s *remoteSession) tools(tb testing.TB) []node.ToolInfo {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testWaitBound)
	defer cancel()
	res, err := s.client.Tools(ctx, ctl.SessionIDRequest{ID: s.id})
	if err != nil {
		tb.Fatalf("harness: session %s: listing tools: %v", s.name, err)
		return nil
	}
	return res.Tools
}

func (s *remoteSession) stopMCP(tb testing.TB, sig syscall.Signal) int {
	tb.Helper()
	res, err := s.client.MCP(context.Background(), ctl.SessionMCPRequest{ID: s.id, Op: "stop", Signal: strconv.Itoa(int(sig))})
	if err != nil {
		tb.Fatalf("harness: session %s: stopping the MCP server process: %v", s.name, err)
		return -1
	}
	return res.ExitCode
}

func (s *remoteSession) restartMCP(tb testing.TB) {
	tb.Helper()
	if _, err := s.client.MCP(context.Background(), ctl.SessionMCPRequest{ID: s.id, Op: "restart"}); err != nil {
		tb.Fatalf("harness: session %s: restarting the MCP server process: %v", s.name, err)
	}
}

func (s *remoteSession) startExtraMCP(tb testing.TB) {
	tb.Helper()
	if _, err := s.client.MCP(context.Background(), ctl.SessionMCPRequest{ID: s.id, Op: "extra"}); err != nil {
		tb.Fatalf("harness: session %s: starting an extra MCP server process: %v", s.name, err)
	}
}

func (s *remoteSession) inboxConnections(tb testing.TB) int {
	tb.Helper()
	res, err := s.client.InboxConnections(context.Background(), ctl.SessionIDRequest{ID: s.id})
	if err != nil {
		tb.Fatalf("harness: session %s: counting inbox connections: %v", s.name, err)
		return 0
	}
	return res.N
}

// fake returns the proxy that forwards fake operations to the container's node
// agent. The session and its frames live in the container process, so the
// host works through the control protocol rather than a local fakesession.
func (s *remoteSession) fake() FakeSession {
	return s.proxy
}

// fakeErrors reads the background failures the fake in the container
// recorded. It returns a control failure as an error rather than ending the
// test, because the teardown cleanup reports it.
func (s *remoteSession) fakeErrors() ([]error, error) {
	ctx, cancel := context.WithTimeout(context.Background(), testWaitBound)
	defer cancel()
	res, err := s.client.Errors(ctx, ctl.SessionIDRequest{ID: s.id})
	if err != nil {
		return nil, fmt.Errorf("harness: session %s: reading simulated session failures: %w", s.name, err)
	}
	return stringErrors(res.Errors), nil
}

// takeFakeErrors is the test-facing form: a control failure is an
// infrastructure failure and fails the test.
func (s *remoteSession) takeFakeErrors(tb testing.TB) []error {
	tb.Helper()
	errs, err := s.fakeErrors()
	if err != nil {
		tb.Fatalf("%v", err)
		return nil
	}
	return errs
}

func (s *remoteSession) end() error {
	// A background failure never reaches the test on its own, so the end of
	// the session is where it is reported.
	errs, readErr := s.fakeErrors()
	endErr := s.client.End(context.Background(), ctl.SessionIDRequest{ID: s.id})
	if readErr != nil {
		if endErr == nil {
			return readErr
		}
		return fmt.Errorf("%v; %w", endErr, readErr)
	}
	if len(errs) > 0 {
		background := fmt.Errorf("harness: session %s: simulated session failures: %s", s.name, joinErrors(errs))
		if endErr == nil {
			return background
		}
		return fmt.Errorf("%v; %w", endErr, background)
	}
	return endErr
}

// remoteOracle calls the five local interface RPCs over /v1/local. Product
// errors come back as an ErrorDetail; infrastructure errors fail the test,
// matching the local oracle.
type remoteOracle struct {
	name   string
	client *ctl.Client
}

func (o *remoteOracle) hello(t testing.TB) *localpb.HelloResponse {
	t.Helper()
	message := &localpb.HelloResponse{}
	if detail := o.decode(t, o.local(t, ctl.LocalRequest{RPC: "hello"}), message); detail != nil {
		t.Fatalf("harness: node %s: Local.Hello returned an error: %s", o.name, detail.GetMessage())
		return nil
	}
	return message
}

func (o *remoteOracle) listBrokers(t testing.TB) (*localpb.ListBrokersResponse, *localpb.ErrorDetail) {
	t.Helper()
	message := &localpb.ListBrokersResponse{}
	if detail := o.decode(t, o.local(t, ctl.LocalRequest{RPC: "list_brokers"}), message); detail != nil {
		return nil, detail
	}
	return message, nil
}

func (o *remoteOracle) status(t testing.TB) (*localpb.StatusResponse, *localpb.ErrorDetail) {
	t.Helper()
	message := &localpb.StatusResponse{}
	if detail := o.decode(t, o.local(t, ctl.LocalRequest{RPC: "status"}), message); detail != nil {
		return nil, detail
	}
	return message, nil
}

func (o *remoteOracle) getConfig(t testing.TB, key string) (*localpb.GetConfigResponse, *localpb.ErrorDetail) {
	t.Helper()
	message := &localpb.GetConfigResponse{}
	if detail := o.decode(t, o.local(t, ctl.LocalRequest{RPC: "get_config", Key: key}), message); detail != nil {
		return nil, detail
	}
	return message, nil
}

func (o *remoteOracle) audit(t testing.TB, since time.Time, limit uint32) ([]*localpb.AuditEvent, *localpb.ErrorDetail) {
	t.Helper()
	req := ctl.LocalRequest{RPC: "audit", Limit: limit}
	if !since.IsZero() {
		req.Since = since.UTC().Format(time.RFC3339Nano)
	}
	res := o.local(t, req)
	if detail := o.errorDetail(t, res); detail != nil {
		return nil, detail
	}
	events := make([]*localpb.AuditEvent, 0, len(res.Events))
	for _, raw := range res.Events {
		event := &localpb.AuditEvent{}
		if err := protojson.Unmarshal(raw, event); err != nil {
			t.Fatalf("harness: node %s: decoding an audit event: %v", o.name, err)
		}
		events = append(events, event)
	}
	return events, nil
}

func (o *remoteOracle) close() {
	// The control client belongs to the cluster and outlives this oracle.
}

// local performs one control request.
func (o *remoteOracle) local(t testing.TB, req ctl.LocalRequest) ctl.LocalResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), oracleBound)
	defer cancel()
	res, err := o.client.Local(ctx, req)
	if err != nil {
		t.Fatalf("harness: node %s: calling Local.%s: %v", o.name, req.RPC, err)
		return ctl.LocalResult{}
	}
	return res
}

// errorDetail decodes the product error in an answer, or nil when there is
// none.
func (o *remoteOracle) errorDetail(t testing.TB, res ctl.LocalResult) *localpb.ErrorDetail {
	t.Helper()
	if len(res.ErrorDetail) == 0 {
		return nil
	}
	detail := &localpb.ErrorDetail{}
	if err := protojson.Unmarshal(res.ErrorDetail, detail); err != nil {
		t.Fatalf("harness: node %s: decoding an error detail: %v", o.name, err)
		return nil
	}
	return detail
}

// decode fills message from the answer and returns the product error, if any.
func (o *remoteOracle) decode(t testing.TB, res ctl.LocalResult, message proto.Message) *localpb.ErrorDetail {
	t.Helper()
	if detail := o.errorDetail(t, res); detail != nil {
		return detail
	}
	if len(res.Response) == 0 {
		t.Fatalf("harness: node %s: the local interface returned no answer", o.name)
		return nil
	}
	if err := protojson.Unmarshal(res.Response, message); err != nil {
		t.Fatalf("harness: node %s: decoding %T: %v", o.name, message, err)
		return nil
	}
	return nil
}
