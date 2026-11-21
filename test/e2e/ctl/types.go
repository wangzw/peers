// Package ctl is the control protocol the host tests use to drive a resident
// node agent inside a container. It is plain HTTP/1.1 over a Unix socket, with
// one POST endpoint per operation and JSON requests and answers.
package ctl

import (
	"encoding/json"

	"github.com/wangzw/peers/test/e2e/node"
)

// Envelope is the answer of every control request. A product failure, such as
// a tool error or a Broker that did not start, is part of Result; Error is
// reserved for a request the agent could not carry out.
type Envelope struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// SessionStartRequest starts one simulated session.
type SessionStartRequest struct {
	Name            string            `json:"name,omitempty"`
	Status          string            `json:"status,omitempty"`
	ConfigDir       string            `json:"config_dir,omitempty"`
	ExportConfigDir bool              `json:"export_config_dir,omitempty"`
	Workdir         string            `json:"workdir,omitempty"`
	Mode            string            `json:"mode,omitempty"`
	BusyDelayMS     int64             `json:"busy_delay_ms,omitempty"`
	NoBind          bool              `json:"no_bind,omitempty"`
	SocketPath      string            `json:"socket_path,omitempty"`
	NoSocketEnv     bool              `json:"no_socket_env,omitempty"`
	Env             map[string]string `json:"env,omitempty"`
	StartMCP        bool              `json:"start_mcp,omitempty"`
}

// SessionInfo describes a started session.
type SessionInfo struct {
	ID         string `json:"id"`
	SocketPath string `json:"socket_path"`
	ConfigDir  string `json:"config_dir"`
	RecordPath string `json:"record_path"`
	FakePID    int    `json:"fake_pid"`
	MCPPID     int    `json:"mcp_pid"`
}

// SessionIDRequest names a session.
type SessionIDRequest struct {
	ID string `json:"id"`
}

// SessionCallRequest invokes a tool on a session's MCP server process.
type SessionCallRequest struct {
	ID        string         `json:"id"`
	Tool      string         `json:"tool"`
	Args      map[string]any `json:"args,omitempty"`
	TimeoutMS int64          `json:"timeout_ms,omitempty"`
	Extra     int            `json:"extra,omitempty"`
}

// SessionCallResult is one tool call.
type SessionCallResult struct {
	IsError    bool            `json:"is_error"`
	Text       string          `json:"text"`
	Structured json.RawMessage `json:"structured,omitempty"`
	ElapsedMS  int64           `json:"elapsed_ms"`
}

// SessionToolsResult lists the tools of a session's MCP server process, with
// the annotations tools/list returned for each.
type SessionToolsResult struct {
	Tools []node.ToolInfo `json:"tools"`
}

// SessionFramesRequest asks for the frames a session recorded. A zero WaitN
// returns at once; otherwise the request waits for the WaitN-th frame.
type SessionFramesRequest struct {
	ID        string `json:"id"`
	WaitN     int    `json:"wait_n,omitempty"`
	TimeoutMS int64  `json:"timeout_ms,omitempty"`
}

// SessionFrame is one frame recorded by a simulated session.
type SessionFrame struct {
	Raw       string         `json:"raw,omitempty"`
	Auth      bool           `json:"auth,omitempty"`
	AuthRaw   string         `json:"auth_raw,omitempty"`
	MsgID     string         `json:"msg_id,omitempty"`
	Type      string         `json:"type,omitempty"`
	FromName  string         `json:"from_name,omitempty"`
	From      string         `json:"from,omitempty"`
	SessionID string         `json:"session_id,omitempty"`
	Priority  string         `json:"priority,omitempty"`
	Content   string         `json:"content,omitempty"`
	Fields    map[string]any `json:"fields,omitempty"`
}

// SessionFramesResult is the answer of a frame wait.
type SessionFramesResult struct {
	Frames []SessionFrame `json:"frames"`
	OK     bool           `json:"ok"`
}

// SessionFakeRequest applies one operation to a simulated session.
type SessionFakeRequest struct {
	ID  string `json:"id"`
	Op  string `json:"op"`
	Arg any    `json:"arg,omitempty"`
}

// SessionErrorsResult carries the background failures a simulated session
// recorded since the previous request.
type SessionErrorsResult struct {
	Errors []string `json:"errors"`
}

// SessionMCPRequest applies one operation to a session's MCP server
// processes.
type SessionMCPRequest struct {
	ID     string `json:"id"`
	Op     string `json:"op"`
	Signal string `json:"signal,omitempty"`
}

// SessionMCPResult reports the exit code and PID of an MCP server process
// operation.
type SessionMCPResult struct {
	ExitCode int `json:"exit_code"`
	PID      int `json:"pid"`
}

// InboxConnectionsResult reports the number of connected inbox peers.
type InboxConnectionsResult struct {
	N int `json:"n"`
}

// SessionWaitNoConnectionsRequest waits until a simulated session tracks no
// inbox connection. It serves the end-to-end test harness only; no product
// code sends or answers it.
type SessionWaitNoConnectionsRequest struct {
	ID        string `json:"id"`
	TimeoutMS int64  `json:"timeout_ms,omitempty"`
}

// SessionWaitNoConnectionsResult reports whether the wait found no
// connections before its timeout.
type SessionWaitNoConnectionsResult struct {
	OK bool `json:"ok"`
}

// BrokerStartRequest starts a Broker process.
type BrokerStartRequest struct {
	Args       []string          `json:"args,omitempty"`
	ConfigYAML string            `json:"config_yaml,omitempty"`
	Env        map[string]string `json:"env,omitempty"`
	BoundMS    int64             `json:"bound_ms,omitempty"`
}

// BrokerStartResult reports how a Broker start ended.
type BrokerStartResult struct {
	Running    bool   `json:"running"`
	PID        int    `json:"pid,omitempty"`
	ExitCode   int    `json:"exit_code,omitempty"`
	StderrLast string `json:"stderr_last,omitempty"`
}

// BrokerSignalRequest sends a signal to the current Broker.
type BrokerSignalRequest struct {
	Signal string `json:"signal"`
}

// BrokerWaitRequest waits for the current Broker to exit.
type BrokerWaitRequest struct {
	BoundMS int64 `json:"bound_ms,omitempty"`
}

// BrokerWaitResult reports the Broker exit code.
type BrokerWaitResult struct {
	ExitCode int `json:"exit_code"`
}

// BrokerLogRequest asks for the current Broker log, or every log so far.
type BrokerLogRequest struct {
	All bool `json:"all,omitempty"`
}

// BrokerLogResult carries the log bytes, base64 encoded by encoding/json.
type BrokerLogResult struct {
	Data []byte `json:"data"`
}

// LocalRequest calls one RPC of the local interface. Key is used by
// get_config, Since and Limit by audit.
type LocalRequest struct {
	RPC   string `json:"rpc"`
	Key   string `json:"key,omitempty"`
	Since string `json:"since,omitempty"`
	Limit uint32 `json:"limit,omitempty"`
}

// LocalResult carries a protojson response, a protojson ErrorDetail, or the
// audit events in order.
type LocalResult struct {
	Response    json.RawMessage   `json:"response,omitempty"`
	ErrorDetail json.RawMessage   `json:"error_detail,omitempty"`
	Events      []json.RawMessage `json:"events,omitempty"`
}

// ProcsRequest selects the processes to list.
type ProcsRequest struct {
	Kind string `json:"kind"`
}

// ProcInfo is one process as the agent sees it.
type ProcInfo struct {
	PID  int               `json:"pid"`
	PPID int               `json:"ppid"`
	SID  int               `json:"sid"`
	UID  int               `json:"uid"`
	Args []string          `json:"args,omitempty"`
	Env  map[string]string `json:"env,omitempty"`
}

// WorkspaceRequest creates a workspace.
type WorkspaceRequest struct {
	Kind        string `json:"kind"`
	Origin      string `json:"origin,omitempty"`
	ProjectYAML string `json:"project_yaml,omitempty"`
}

// WorkspaceResult carries the workspace path.
type WorkspaceResult struct {
	Path string `json:"path"`
}

// FileReadRequest reads a file.
type FileReadRequest struct {
	Path string `json:"path"`
}

// FileReadResult carries the file bytes. A missing file has a null Data and
// Missing set.
type FileReadResult struct {
	Data    []byte `json:"data"`
	Missing bool   `json:"missing,omitempty"`
}

// FileWriteRequest writes a file.
type FileWriteRequest struct {
	Path string `json:"path"`
	Data []byte `json:"data"`
	Mode uint32 `json:"mode,omitempty"`
}

// FileListRequest lists a directory.
type FileListRequest struct {
	Path string `json:"path"`
}

// FileListResult carries the directory entry names.
type FileListResult struct {
	Names []string `json:"names"`
}

// PortHoldRequest holds a TCP port.
type PortHoldRequest struct {
	Addr string `json:"addr"`
}

// PortHoldResult names the held port and the address it is bound to.
type PortHoldResult struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

// PortReleaseRequest releases a held port.
type PortReleaseRequest struct {
	ID string `json:"id"`
}

// ProbeTCPRequest asks whether a TCP address is reachable.
type ProbeTCPRequest struct {
	Addr      string `json:"addr"`
	TimeoutMS int64  `json:"timeout_ms,omitempty"`
}

// ProbeTCPResult reports reachability.
type ProbeTCPResult struct {
	OK bool `json:"ok"`
}

// ConnOpenRequest opens a TCP connection and keeps it.
type ConnOpenRequest struct {
	Addr string `json:"addr"`
}

// ConnOpenResult names the open connection.
type ConnOpenResult struct {
	ID string `json:"id"`
}

// ConnAliveRequest checks an open connection.
type ConnAliveRequest struct {
	ID string `json:"id"`
}

// ConnAliveResult reports whether the connection still carries data.
type ConnAliveResult struct {
	Alive bool `json:"alive"`
}

// FloodStartRequest starts a flood of bare TCP connections from a node. Each
// of Sources distinct loopback source addresses holds PerSource connections.
type FloodStartRequest struct {
	Addr      string `json:"addr"`
	Sources   int    `json:"sources"`
	PerSource int    `json:"per_source"`
}

// FloodStatsResult reports one reading of a running flood.
type FloodStatsResult struct {
	Dialed  int64 `json:"dialed"`
	Open    int64 `json:"open"`
	MaxOpen int64 `json:"max_open"`
	Closed  int64 `json:"closed"`
}

// ProbeHelloRequest sends a Hello to a Broker without a pairing key.
type ProbeHelloRequest struct {
	Addr      string `json:"addr"`
	TimeoutMS int64  `json:"timeout_ms,omitempty"`
}

// ProbeHelloResult carries the reject reason name.
type ProbeHelloResult struct {
	Reason string `json:"reason"`
}

// RelayStartRequest starts the man-in-the-middle relay.
type RelayStartRequest struct {
	Listen   string `json:"listen"`
	Upstream string `json:"upstream"`
}

// RelayStatsResult reports what the relay forwarded.
type RelayStatsResult struct {
	Streams    int `json:"streams"`
	FramesUp   int `json:"frames_up"`
	FramesDown int `json:"frames_down"`
}

// ExecRequest runs a one-off command as the node account.
type ExecRequest struct {
	Argv      []string          `json:"argv"`
	Env       map[string]string `json:"env,omitempty"`
	TimeoutMS int64             `json:"timeout_ms,omitempty"`
}

// ExecResult is the outcome of a one-off command.
type ExecResult struct {
	ExitCode int    `json:"exit_code"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
}
