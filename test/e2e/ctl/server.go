package ctl

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/wangzw/peers/test/e2e/node"
)

// maxRequestBody caps the size of one control request. Writing a file through
// the control protocol is the only large body, and tests write small files.
const maxRequestBody = 32 << 20

// handler serves the control protocol on the node agent.
type handler struct {
	agent *node.Agent
}

// NewHandler returns the control protocol handler for a node agent.
func NewHandler(a *node.Agent) http.Handler {
	return &handler{agent: a}
}

// ServeHTTP accepts one POST request and answers with an Envelope.
func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "ctl: only POST is allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody))
	if err != nil {
		writeEnvelope(w, Envelope{OK: false, Error: fmt.Sprintf("ctl: reading request: %v", err)})
		return
	}

	result, err := h.handle(r.URL.Path, body)
	if err != nil {
		writeEnvelope(w, Envelope{OK: false, Error: err.Error()})
		return
	}
	data, err := json.Marshal(result)
	if err != nil {
		writeEnvelope(w, Envelope{OK: false, Error: fmt.Sprintf("ctl: encoding answer: %v", err)})
		return
	}
	writeEnvelope(w, Envelope{OK: true, Result: data})
}

// handle decodes and carries out one request.
func (h *handler) handle(path string, body []byte) (any, error) {
	switch path {
	case "/v1/session/start":
		req, err := decode[SessionStartRequest](body)
		if err != nil {
			return nil, err
		}
		info, err := h.agent.StartSession(node.AgentSessionSpec{
			Name:            req.Name,
			Status:          req.Status,
			ConfigDir:       req.ConfigDir,
			ExportConfigDir: req.ExportConfigDir,
			Workdir:         req.Workdir,
			Mode:            req.Mode,
			BusyDelay:       time.Duration(req.BusyDelayMS) * time.Millisecond,
			NoBind:          req.NoBind,
			SocketPath:      req.SocketPath,
			NoSocketEnv:     req.NoSocketEnv,
			Env:             req.Env,
			StartMCP:        req.StartMCP,
		})
		if err != nil {
			return nil, err
		}
		return sessionInfo(info), nil

	case "/v1/session/call":
		req, err := decode[SessionCallRequest](body)
		if err != nil {
			return nil, err
		}
		result, err := h.agent.Call(req.ID, req.Tool, req.Args, time.Duration(req.TimeoutMS)*time.Millisecond, req.Extra)
		if err != nil {
			return nil, err
		}
		return sessionCallResult(result), nil

	case "/v1/session/tools":
		req, err := decode[SessionIDRequest](body)
		if err != nil {
			return nil, err
		}
		tools, err := h.agent.Tools(req.ID)
		if err != nil {
			return nil, err
		}
		return SessionToolsResult{Tools: tools}, nil

	case "/v1/session/frames":
		req, err := decode[SessionFramesRequest](body)
		if err != nil {
			return nil, err
		}
		frames, err := h.agent.Frames(req.ID, req.WaitN, time.Duration(req.TimeoutMS)*time.Millisecond)
		if err != nil {
			return nil, err
		}
		return sessionFramesResult(frames), nil

	case "/v1/session/fake":
		req, err := decode[SessionFakeRequest](body)
		if err != nil {
			return nil, err
		}
		if err := h.agent.Fake(req.ID, req.Op, req.Arg); err != nil {
			return nil, err
		}
		return struct{}{}, nil

	case "/v1/session/errors":
		req, err := decode[SessionIDRequest](body)
		if err != nil {
			return nil, err
		}
		errs, err := h.agent.SessionErrors(req.ID)
		if err != nil {
			return nil, err
		}
		if errs == nil {
			errs = []string{}
		}
		return SessionErrorsResult{Errors: errs}, nil

	case "/v1/session/mcp":
		req, err := decode[SessionMCPRequest](body)
		if err != nil {
			return nil, err
		}
		signal, err := parseSignal(req.Signal)
		if err != nil {
			return nil, err
		}
		result, err := h.agent.MCP(req.ID, req.Op, signal)
		if err != nil {
			return nil, err
		}
		return SessionMCPResult{ExitCode: result.ExitCode, PID: result.PID}, nil

	case "/v1/session/end":
		req, err := decode[SessionIDRequest](body)
		if err != nil {
			return nil, err
		}
		if err := h.agent.EndSession(req.ID); err != nil {
			return nil, err
		}
		return struct{}{}, nil

	// The wait_no_conns endpoint serves the end-to-end test harness only: it
	// lets a test observe a simulated session dropping its inbox connection
	// without polling.
	case "/v1/session/wait_no_conns":
		req, err := decode[SessionWaitNoConnectionsRequest](body)
		if err != nil {
			return nil, err
		}
		ok, err := h.agent.WaitNoConnections(req.ID, time.Duration(req.TimeoutMS)*time.Millisecond)
		if err != nil {
			return nil, err
		}
		return SessionWaitNoConnectionsResult{OK: ok}, nil

	case "/v1/session/inbox_conns":
		req, err := decode[SessionIDRequest](body)
		if err != nil {
			return nil, err
		}
		n, err := h.agent.InboxConnections(req.ID)
		if err != nil {
			return nil, err
		}
		return InboxConnectionsResult{N: n}, nil

	case "/v1/broker/start":
		req, err := decode[BrokerStartRequest](body)
		if err != nil {
			return nil, err
		}
		result, err := h.agent.StartBroker(node.AgentBrokerSpec{
			Args:       req.Args,
			ConfigYAML: req.ConfigYAML,
			Env:        req.Env,
			Bound:      time.Duration(req.BoundMS) * time.Millisecond,
		})
		if err != nil {
			return nil, err
		}
		return BrokerStartResult{
			Running:    result.Running,
			PID:        result.PID,
			ExitCode:   result.ExitCode,
			StderrLast: result.StderrLast,
		}, nil

	case "/v1/broker/signal":
		req, err := decode[BrokerSignalRequest](body)
		if err != nil {
			return nil, err
		}
		signal, err := parseSignal(req.Signal)
		if err != nil {
			return nil, err
		}
		if err := h.agent.SignalBroker(signal); err != nil {
			return nil, err
		}
		return struct{}{}, nil

	case "/v1/broker/wait":
		req, err := decode[BrokerWaitRequest](body)
		if err != nil {
			return nil, err
		}
		code, err := h.agent.WaitBroker(time.Duration(req.BoundMS) * time.Millisecond)
		if err != nil {
			return nil, err
		}
		return BrokerWaitResult{ExitCode: code}, nil

	case "/v1/broker/log":
		req, err := decode[BrokerLogRequest](body)
		if err != nil {
			return nil, err
		}
		data, err := h.agent.BrokerLog(req.All)
		if err != nil {
			return nil, err
		}
		return BrokerLogResult{Data: data}, nil

	case "/v1/local":
		req, err := decode[LocalRequest](body)
		if err != nil {
			return nil, err
		}
		result, err := h.agent.Local(node.AgentLocalRequest{
			RPC:   req.RPC,
			Key:   req.Key,
			Since: req.Since,
			Limit: req.Limit,
		})
		if err != nil {
			return nil, err
		}
		return localResult(result), nil

	case "/v1/procs":
		req, err := decode[ProcsRequest](body)
		if err != nil {
			return nil, err
		}
		procs, err := h.agent.Procs(req.Kind)
		if err != nil {
			return nil, err
		}
		out := make([]ProcInfo, 0, len(procs))
		for _, proc := range procs {
			out = append(out, ProcInfo{PID: proc.PID, PPID: proc.PPID, SID: proc.SID, UID: proc.UID, Args: proc.Args, Env: proc.Env})
		}
		return out, nil

	case "/v1/workspace":
		req, err := decode[WorkspaceRequest](body)
		if err != nil {
			return nil, err
		}
		path, err := h.agent.Workspace(node.AgentWorkspace{Kind: req.Kind, Origin: req.Origin, ProjectYAML: req.ProjectYAML})
		if err != nil {
			return nil, err
		}
		return WorkspaceResult{Path: path}, nil

	case "/v1/file/read":
		req, err := decode[FileReadRequest](body)
		if err != nil {
			return nil, err
		}
		data, missing, err := h.agent.ReadFile(req.Path)
		if err != nil {
			return nil, err
		}
		return FileReadResult{Data: data, Missing: missing}, nil

	case "/v1/file/write":
		req, err := decode[FileWriteRequest](body)
		if err != nil {
			return nil, err
		}
		if err := h.agent.WriteFile(req.Path, req.Data, os.FileMode(req.Mode)); err != nil {
			return nil, err
		}
		return struct{}{}, nil

	case "/v1/file/list":
		req, err := decode[FileListRequest](body)
		if err != nil {
			return nil, err
		}
		names, err := h.agent.ListFiles(req.Path)
		if err != nil {
			return nil, err
		}
		return FileListResult{Names: names}, nil

	case "/v1/port/hold":
		req, err := decode[PortHoldRequest](body)
		if err != nil {
			return nil, err
		}
		id, addr, err := h.agent.HoldPort(req.Addr)
		if err != nil {
			return nil, err
		}
		return PortHoldResult{ID: id, Addr: addr}, nil

	case "/v1/port/release":
		req, err := decode[PortReleaseRequest](body)
		if err != nil {
			return nil, err
		}
		if err := h.agent.ReleasePort(req.ID); err != nil {
			return nil, err
		}
		return struct{}{}, nil

	case "/v1/probe/tcp":
		req, err := decode[ProbeTCPRequest](body)
		if err != nil {
			return nil, err
		}
		return ProbeTCPResult{OK: h.agent.ProbeTCP(req.Addr, time.Duration(req.TimeoutMS)*time.Millisecond)}, nil

	case "/v1/probe/conn_open":
		req, err := decode[ConnOpenRequest](body)
		if err != nil {
			return nil, err
		}
		id, err := h.agent.OpenConn(req.Addr, 0)
		if err != nil {
			return nil, err
		}
		return ConnOpenResult{ID: id}, nil

	case "/v1/probe/conn_alive":
		req, err := decode[ConnAliveRequest](body)
		if err != nil {
			return nil, err
		}
		alive, err := h.agent.ConnAlive(req.ID)
		if err != nil {
			return nil, err
		}
		return ConnAliveResult{Alive: alive}, nil

	case "/v1/flood/start":
		req, err := decode[FloodStartRequest](body)
		if err != nil {
			return nil, err
		}
		if err := h.agent.FloodStart(node.FloodSpec{
			Addr:      req.Addr,
			Sources:   req.Sources,
			PerSource: req.PerSource,
		}); err != nil {
			return nil, err
		}
		return struct{}{}, nil

	case "/v1/flood/stats":
		if _, err := decode[struct{}](body); err != nil {
			return nil, err
		}
		stats, err := h.agent.FloodStats()
		if err != nil {
			return nil, err
		}
		return FloodStatsResult{
			Dialed:  stats.Dialed,
			Open:    stats.Open,
			MaxOpen: stats.MaxOpen,
			Closed:  stats.Closed,
		}, nil

	case "/v1/flood/stop":
		if _, err := decode[struct{}](body); err != nil {
			return nil, err
		}
		if err := h.agent.FloodStop(); err != nil {
			return nil, err
		}
		return struct{}{}, nil

	case "/v1/probe/hello":
		req, err := decode[ProbeHelloRequest](body)
		if err != nil {
			return nil, err
		}
		reason, err := h.agent.ProbeHello(req.Addr, time.Duration(req.TimeoutMS)*time.Millisecond)
		if err != nil {
			return nil, err
		}
		return ProbeHelloResult{Reason: reason}, nil

	case "/v1/relay/start":
		req, err := decode[RelayStartRequest](body)
		if err != nil {
			return nil, err
		}
		if err := h.agent.StartRelay(req.Listen, req.Upstream); err != nil {
			return nil, err
		}
		return struct{}{}, nil

	case "/v1/relay/stats":
		if _, err := decode[struct{}](body); err != nil {
			return nil, err
		}
		streams, up, down, err := h.agent.RelayStats()
		if err != nil {
			return nil, err
		}
		return RelayStatsResult{Streams: streams, FramesUp: up, FramesDown: down}, nil

	case "/v1/exec":
		req, err := decode[ExecRequest](body)
		if err != nil {
			return nil, err
		}
		result, err := h.agent.Exec(req.Argv, req.Env, time.Duration(req.TimeoutMS)*time.Millisecond)
		if err != nil {
			return nil, err
		}
		return ExecResult{ExitCode: result.ExitCode, Stdout: result.Stdout, Stderr: result.Stderr}, nil

	case "/v1/shutdown":
		if _, err := decode[struct{}](body); err != nil {
			return nil, err
		}
		h.agent.Shutdown()
		return struct{}{}, nil

	default:
		return nil, fmt.Errorf("ctl: unknown endpoint %q", path)
	}
}

// decode unmarshals a request body into T. An empty body is a zero request.
func decode[T any](body []byte) (T, error) {
	var req T
	if len(body) == 0 {
		return req, nil
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return req, fmt.Errorf("ctl: invalid request: %w", err)
	}
	return req, nil
}

// writeEnvelope writes one JSON envelope.
func writeEnvelope(w http.ResponseWriter, envelope Envelope) {
	w.Header().Set("Content-Type", "application/json")
	data, err := json.Marshal(envelope)
	if err != nil {
		// Every field of Envelope is marshalable; this cannot fail.
		http.Error(w, "ctl: encoding envelope", http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(data)
}

// parseSignal turns a signal name or number into a signal.
func parseSignal(name string) (syscall.Signal, error) {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "", "SIGINT", "INT":
		return syscall.SIGINT, nil
	case "SIGTERM", "TERM":
		return syscall.SIGTERM, nil
	case "SIGKILL", "KILL":
		return syscall.SIGKILL, nil
	case "SIGHUP", "HUP":
		return syscall.SIGHUP, nil
	case "SIGUSR1", "USR1":
		return syscall.SIGUSR1, nil
	case "SIGUSR2", "USR2":
		return syscall.SIGUSR2, nil
	}
	if number, err := strconv.Atoi(strings.TrimSpace(name)); err == nil && number > 0 {
		return syscall.Signal(number), nil
	}
	return 0, fmt.Errorf("ctl: unknown signal %q", name)
}

// sessionInfo converts a started session.
func sessionInfo(info node.AgentSessionInfo) SessionInfo {
	return SessionInfo{
		ID:         info.ID,
		SocketPath: info.SocketPath,
		ConfigDir:  info.ConfigDir,
		RecordPath: info.RecordPath,
		FakePID:    info.FakePID,
		MCPPID:     info.MCPPID,
	}
}

// sessionCallResult converts a tool call.
func sessionCallResult(result node.AgentCallResult) SessionCallResult {
	out := SessionCallResult{
		IsError:   result.IsError,
		Text:      result.Text,
		ElapsedMS: result.Elapsed.Milliseconds(),
	}
	if result.Structured != nil {
		out.Structured = json.RawMessage(result.Structured)
	}
	return out
}

// sessionFramesResult converts a frame wait.
func sessionFramesResult(result node.AgentFrames) SessionFramesResult {
	frames := make([]SessionFrame, 0, len(result.Frames))
	for _, frame := range result.Frames {
		frames = append(frames, SessionFrame{
			Raw:       frame.Raw,
			Auth:      frame.Auth,
			AuthRaw:   frame.AuthRaw,
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
	return SessionFramesResult{Frames: frames, OK: result.OK}
}

// localResult converts a local interface answer.
func localResult(result node.AgentLocalResult) LocalResult {
	out := LocalResult{}
	if result.Response != nil {
		out.Response = json.RawMessage(result.Response)
	}
	if result.ErrorDetail != nil {
		out.ErrorDetail = json.RawMessage(result.ErrorDetail)
	}
	if result.Events != nil {
		out.Events = make([]json.RawMessage, 0, len(result.Events))
		for _, event := range result.Events {
			out.Events = append(out.Events, json.RawMessage(event))
		}
	}
	return out
}
