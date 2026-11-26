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
	// OK reports whether the agent carried out the request.
	OK bool `json:"ok"`
	// Result holds the endpoint's answer, encoded as JSON.
	Result json.RawMessage `json:"result,omitempty"`
	// Error is the message of a request the agent could not carry out.
	Error string `json:"error,omitempty"`
}

// SessionStartRequest starts one simulated session.
type SessionStartRequest struct {
	// Name is the session name written to the record.
	Name string `json:"name,omitempty"`
	// Status is the initial session status; empty leaves it unreported.
	Status string `json:"status,omitempty"`
	// ConfigDir is the Claude configuration directory; empty means $HOME/.claude.
	ConfigDir string `json:"config_dir,omitempty"`
	// ExportConfigDir exports CLAUDE_CONFIG_DIR to the MCP server process.
	ExportConfigDir bool `json:"export_config_dir,omitempty"`
	// Workdir is the MCP server process's working directory; empty means $HOME.
	Workdir string `json:"workdir,omitempty"`
	// Mode selects how accepted inbox connections are handled.
	Mode string `json:"mode,omitempty"`
	// BusyDelayMS delays reading each accepted connection by that many milliseconds.
	BusyDelayMS int64 `json:"busy_delay_ms,omitempty"`
	// NoBind writes only the registry record and binds no inbox socket.
	NoBind bool `json:"no_bind,omitempty"`
	// SocketPath overrides the inbox socket path.
	SocketPath string `json:"socket_path,omitempty"`
	// NoSocketEnv suppresses CLAUDE_CODE_MESSAGING_SOCKET in the MCP environment.
	NoSocketEnv bool `json:"no_socket_env,omitempty"`
	// Env adds environment variables to the MCP server process.
	Env map[string]string `json:"env,omitempty"`
	// StartMCP starts an MCP server process with the session.
	StartMCP bool `json:"start_mcp,omitempty"`
}

// SessionInfo describes a started session.
type SessionInfo struct {
	// ID is the session identifier.
	ID string `json:"id"`
	// SocketPath is the inbox socket path.
	SocketPath string `json:"socket_path"`
	// ConfigDir is the Claude configuration directory in use.
	ConfigDir string `json:"config_dir"`
	// RecordPath is the path of the session registry record.
	RecordPath string `json:"record_path"`
	// FakePID is the pid of the simulated session.
	FakePID int `json:"fake_pid"`
	// MCPPID is the pid of the session's MCP server process; zero when none started.
	MCPPID int `json:"mcp_pid"`
}

// SessionIDRequest names a session.
type SessionIDRequest struct {
	// ID is the session identifier.
	ID string `json:"id"`
}

// SessionCallRequest invokes a tool on a session's MCP server process.
type SessionCallRequest struct {
	// ID is the session identifier.
	ID string `json:"id"`
	// Tool is the tool name to call.
	Tool string `json:"tool"`
	// Args holds the tool arguments.
	Args map[string]any `json:"args,omitempty"`
	// TimeoutMS bounds the tool call; zero uses the agent's default.
	TimeoutMS int64 `json:"timeout_ms,omitempty"`
	// Extra selects an additional MCP server process; zero is the session's main one.
	Extra int `json:"extra,omitempty"`
}

// SessionCallResult is one tool call.
type SessionCallResult struct {
	// IsError reports whether the tool returned an error result.
	IsError bool `json:"is_error"`
	// Text is the tool result text.
	Text string `json:"text"`
	// Structured is the structured tool result, when the tool returned one.
	Structured json.RawMessage `json:"structured,omitempty"`
	// ElapsedMS is the tool call's elapsed time in milliseconds.
	ElapsedMS int64 `json:"elapsed_ms"`
}

// SessionToolsResult lists the tools of a session's MCP server process, with
// the annotations tools/list returned for each.
type SessionToolsResult struct {
	// Tools is the tool listing.
	Tools []node.ToolInfo `json:"tools"`
}

// SessionFramesRequest asks for the frames a session recorded. A zero WaitN
// returns at once; otherwise the request waits for the WaitN-th frame.
type SessionFramesRequest struct {
	// ID is the session identifier.
	ID string `json:"id"`
	// WaitN is the frame count to wait for; zero returns at once.
	WaitN int `json:"wait_n,omitempty"`
	// TimeoutMS bounds the wait; zero uses the agent's default.
	TimeoutMS int64 `json:"timeout_ms,omitempty"`
}

// SessionFrame is one frame recorded by a simulated session.
type SessionFrame struct {
	// Raw is the frame line without its trailing newline.
	Raw string `json:"raw,omitempty"`
	// Auth reports whether an authentication line preceded the frame.
	Auth bool `json:"auth,omitempty"`
	// AuthRaw is the authentication line without its trailing newline.
	AuthRaw string `json:"auth_raw,omitempty"`
	// MsgID is the frame's msg_id.
	MsgID string `json:"msg_id,omitempty"`
	// Type is the frame's type.
	Type string `json:"type,omitempty"`
	// FromName is the sender name decoded from the attribution envelope.
	FromName string `json:"from_name,omitempty"`
	// From is the frame's top-level from field.
	From string `json:"from,omitempty"`
	// SessionID is the frame's session_id field.
	SessionID string `json:"session_id,omitempty"`
	// Priority is the frame's priority field.
	Priority string `json:"priority,omitempty"`
	// Content is message.content verbatim, including any attribution envelope.
	Content string `json:"content,omitempty"`
	// Fields is the whole decoded frame; it is non-nil for every recorded frame.
	Fields map[string]any `json:"fields,omitempty"`
}

// SessionFramesResult is the answer of a frame wait.
type SessionFramesResult struct {
	// Frames are the recorded frames.
	Frames []SessionFrame `json:"frames"`
	// OK reports whether the requested frame had arrived.
	OK bool `json:"ok"`
}

// SessionFakeRequest applies one operation to a simulated session.
type SessionFakeRequest struct {
	// ID is the session identifier.
	ID string `json:"id"`
	// Op is the operation name.
	Op string `json:"op"`
	// Arg is the operation's argument.
	Arg any `json:"arg,omitempty"`
}

// SessionErrorsResult carries the background failures a simulated session
// recorded since the previous request.
type SessionErrorsResult struct {
	// Errors holds the recorded failure messages.
	Errors []string `json:"errors"`
}

// SessionMCPRequest applies one operation to a session's MCP server
// processes.
type SessionMCPRequest struct {
	// ID is the session identifier.
	ID string `json:"id"`
	// Op is the operation name, such as "stop" or "restart".
	Op string `json:"op"`
	// Signal is the signal name for a stop; empty means SIGINT.
	Signal string `json:"signal,omitempty"`
}

// SessionMCPResult reports the exit code and PID of an MCP server process
// operation.
type SessionMCPResult struct {
	// ExitCode is the stopped process's exit code for stop and stop_extra, and 0 for the other operations.
	ExitCode int `json:"exit_code"`
	// PID is the process's pid.
	PID int `json:"pid"`
}

// InboxConnectionsResult reports the number of connected inbox peers.
type InboxConnectionsResult struct {
	// N is the number of connected peers.
	N int `json:"n"`
}

// SessionWaitNoConnectionsRequest waits until a simulated session tracks no
// inbox connection. It serves the end-to-end test harness only; no product
// code sends or answers it.
type SessionWaitNoConnectionsRequest struct {
	// ID is the session identifier.
	ID string `json:"id"`
	// TimeoutMS bounds the wait; zero checks once and does not wait.
	TimeoutMS int64 `json:"timeout_ms,omitempty"`
}

// SessionWaitNoConnectionsResult reports whether the wait found no
// connections before its timeout.
type SessionWaitNoConnectionsResult struct {
	// OK reports whether no connection was tracked.
	OK bool `json:"ok"`
}

// BrokerStartRequest starts a Broker process.
type BrokerStartRequest struct {
	// Args follow "service run --user" on the command line.
	Args []string `json:"args,omitempty"`
	// ConfigYAML is the Broker configuration written before start.
	ConfigYAML string `json:"config_yaml,omitempty"`
	// Env adds environment variables to the Broker process.
	Env map[string]string `json:"env,omitempty"`
	// BoundMS bounds the wait for the Broker to become ready; zero uses the default wait bound of 13 seconds.
	BoundMS int64 `json:"bound_ms,omitempty"`
}

// BrokerStartResult reports how a Broker start ended.
type BrokerStartResult struct {
	// Running reports whether the Broker answered on its local socket before the bound expired.
	Running bool `json:"running"`
	// PID is the Broker's pid when it is running.
	PID int `json:"pid,omitempty"`
	// ExitCode is the Broker's exit code when it exited.
	ExitCode int `json:"exit_code,omitempty"`
	// StderrLast is the last line of the log that starts with the Broker failure prefix, without it; empty when the log has none.
	StderrLast string `json:"stderr_last,omitempty"`
}

// BrokerSignalRequest sends a signal to the current Broker.
type BrokerSignalRequest struct {
	// Signal is the signal name or number.
	Signal string `json:"signal"`
}

// BrokerWaitRequest waits for the current Broker to exit.
type BrokerWaitRequest struct {
	// BoundMS bounds the wait for the exit.
	BoundMS int64 `json:"bound_ms,omitempty"`
}

// BrokerWaitResult reports the Broker exit code.
type BrokerWaitResult struct {
	// ExitCode is the Broker's exit code.
	ExitCode int `json:"exit_code"`
}

// BrokerLogRequest asks for the current Broker log, or every log so far.
type BrokerLogRequest struct {
	// All asks for every log rather than only the current one.
	All bool `json:"all,omitempty"`
}

// BrokerLogResult carries the log bytes, base64 encoded by encoding/json.
type BrokerLogResult struct {
	// Data holds the log bytes.
	Data []byte `json:"data"`
}

// LocalRequest calls one RPC of the local interface. Key is used by
// get_config, Since and Limit by audit.
type LocalRequest struct {
	// RPC is the local interface RPC: hello, list_brokers, status, get_config or audit.
	RPC string `json:"rpc"`
	// Key is the get_config key.
	Key string `json:"key,omitempty"`
	// Since is the RFC 3339 lower bound for audit; empty sets no lower bound.
	Since string `json:"since,omitempty"`
	// Limit caps the audit events returned; zero means no limit.
	Limit uint32 `json:"limit,omitempty"`
}

// LocalResult carries a protojson response, a protojson ErrorDetail, or the
// audit events in order.
type LocalResult struct {
	// Response is the protojson RPC response.
	Response json.RawMessage `json:"response,omitempty"`
	// ErrorDetail is the protojson error detail when the RPC failed.
	ErrorDetail json.RawMessage `json:"error_detail,omitempty"`
	// Events holds one protojson object per audit event.
	Events []json.RawMessage `json:"events,omitempty"`
}

// ProcsRequest selects the processes to list.
type ProcsRequest struct {
	// Kind selects "broker", "mcp" or "all".
	Kind string `json:"kind"`
}

// ProcInfo is one process as the agent sees it.
type ProcInfo struct {
	// PID is the process id.
	PID int `json:"pid"`
	// PPID is the parent process id.
	PPID int `json:"ppid"`
	// SID is the session id.
	SID int `json:"sid"`
	// UID is the owner's user id.
	UID int `json:"uid"`
	// Args are the process arguments.
	Args []string `json:"args,omitempty"`
	// Env holds the process environment.
	Env map[string]string `json:"env,omitempty"`
}

// WorkspaceRequest creates a workspace.
type WorkspaceRequest struct {
	// Kind selects "git", "project" or "plain".
	Kind string `json:"kind"`
	// Origin is the git origin URL.
	Origin string `json:"origin,omitempty"`
	// ProjectYAML is the project configuration written into the workspace.
	ProjectYAML string `json:"project_yaml,omitempty"`
}

// WorkspaceResult carries the workspace path.
type WorkspaceResult struct {
	// Path is the created workspace's path.
	Path string `json:"path"`
}

// FileReadRequest reads a file.
type FileReadRequest struct {
	// Path is the file to read, confined to the account home and /tmp.
	Path string `json:"path"`
}

// FileReadResult carries the file bytes. A missing file has a null Data and
// Missing set.
type FileReadResult struct {
	// Data holds the file bytes.
	Data []byte `json:"data"`
	// Missing reports that the file does not exist.
	Missing bool `json:"missing,omitempty"`
}

// FileWriteRequest writes a file.
type FileWriteRequest struct {
	// Path is the file to write, confined to the account home and /tmp.
	Path string `json:"path"`
	// Data holds the file bytes.
	Data []byte `json:"data"`
	// Mode is the file mode; zero uses 0600.
	Mode uint32 `json:"mode,omitempty"`
}

// FileListRequest lists a directory.
type FileListRequest struct {
	// Path is the directory to list, confined to the account home and /tmp.
	Path string `json:"path"`
}

// FileListResult carries the directory entry names.
type FileListResult struct {
	// Names are the directory entry names.
	Names []string `json:"names"`
}

// PortHoldRequest holds a TCP port.
type PortHoldRequest struct {
	// Addr is the address to hold, in host:port form.
	Addr string `json:"addr"`
}

// PortHoldResult names the held port and the address it is bound to.
type PortHoldResult struct {
	// ID names the held port.
	ID string `json:"id"`
	// Addr is the address it is bound to.
	Addr string `json:"addr"`
}

// PortReleaseRequest releases a held port.
type PortReleaseRequest struct {
	// ID names the held port.
	ID string `json:"id"`
}

// ProbeTCPRequest asks whether a TCP address is reachable.
type ProbeTCPRequest struct {
	// Addr is the address to probe, in host:port form.
	Addr string `json:"addr"`
	// TimeoutMS bounds the probe; zero uses the agent's default.
	TimeoutMS int64 `json:"timeout_ms,omitempty"`
}

// ProbeTCPResult reports reachability.
type ProbeTCPResult struct {
	// OK reports whether the dial succeeded.
	OK bool `json:"ok"`
}

// ConnOpenRequest opens a TCP connection and keeps it.
type ConnOpenRequest struct {
	// Addr is the address to dial, in host:port form.
	Addr string `json:"addr"`
}

// ConnOpenResult names the open connection.
type ConnOpenResult struct {
	// ID names the open connection.
	ID string `json:"id"`
}

// ConnAliveRequest checks an open connection.
type ConnAliveRequest struct {
	// ID names the open connection.
	ID string `json:"id"`
}

// ConnAliveResult reports whether the connection still carries data.
type ConnAliveResult struct {
	// Alive reports whether a read still succeeded.
	Alive bool `json:"alive"`
}

// FloodStartRequest starts a flood of bare TCP connections from a node. Each
// of Sources distinct loopback source addresses holds PerSource connections.
type FloodStartRequest struct {
	// Addr is the listener in host:port form.
	Addr string `json:"addr"`
	// Sources is the number of distinct loopback source addresses, from 1 to 200, starting at 127.0.0.2.
	Sources int `json:"sources"`
	// PerSource is the number of connections each source holds at once, from 1 to 1024.
	PerSource int `json:"per_source"`
}

// FloodStatsResult reports one reading of a running flood.
type FloodStatsResult struct {
	// Dialed is the number of completed dials.
	Dialed int64 `json:"dialed"`
	// Open is the number of connections still held open.
	Open int64 `json:"open"`
	// MaxOpen is the largest number held open at once.
	MaxOpen int64 `json:"max_open"`
	// Closed is the number of connections the listener closed, not counting those the flood closed itself when it stopped.
	Closed int64 `json:"closed"`
}

// ProbeHelloRequest sends a Hello to a Broker without a pairing key.
type ProbeHelloRequest struct {
	// Addr is the Broker's link address.
	Addr string `json:"addr"`
	// TimeoutMS bounds the probe; zero uses the agent's default.
	TimeoutMS int64 `json:"timeout_ms,omitempty"`
}

// ProbeHelloResult carries the reject reason name.
type ProbeHelloResult struct {
	// Reason is the reject reason name.
	Reason string `json:"reason"`
}

// RelayStartRequest starts the man-in-the-middle relay.
type RelayStartRequest struct {
	// Listen is the relay's listen address.
	Listen string `json:"listen"`
	// Upstream is the address the relay forwards to.
	Upstream string `json:"upstream"`
}

// RelayStatsResult reports what the relay forwarded.
type RelayStatsResult struct {
	// Streams is the number of streams served.
	Streams int `json:"streams"`
	// FramesUp is the number of frames forwarded upstream.
	FramesUp int `json:"frames_up"`
	// FramesDown is the number of frames forwarded downstream.
	FramesDown int `json:"frames_down"`
}

// ExecRequest runs a one-off command as the node account.
type ExecRequest struct {
	// Argv is the command and its arguments.
	Argv []string `json:"argv"`
	// Env adds environment variables to the command.
	Env map[string]string `json:"env,omitempty"`
	// TimeoutMS bounds the command; zero uses the agent's default.
	TimeoutMS int64 `json:"timeout_ms,omitempty"`
}

// ExecResult is the outcome of a one-off command.
type ExecResult struct {
	// ExitCode is the command's exit code.
	ExitCode int `json:"exit_code"`
	// Stdout is the command's standard output.
	Stdout string `json:"stdout"`
	// Stderr is the command's standard error.
	Stderr string `json:"stderr"`
}
