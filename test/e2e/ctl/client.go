package ctl

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

const (
	// defaultTimeout bounds one control request when the caller sets no
	// deadline.
	defaultTimeout = 30 * time.Second
	// callTimeoutSlack is added to a tool call's own timeout for the answer.
	callTimeoutSlack = 5 * time.Second
	// maxAnswerBody caps the answer the client reads.
	maxAnswerBody = 64 << 20
)

// Client drives a node agent over its control socket.
type Client struct {
	http      *http.Client
	transport *http.Transport
}

// Dial returns a client for the control socket at socket. The connection is
// made lazily on the first request.
func Dial(socket string) *Client {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			dialer := net.Dialer{}
			return dialer.DialContext(ctx, "unix", socket)
		},
		MaxIdleConns:    4,
		IdleConnTimeout: 30 * time.Second,
	}
	return &Client{
		http:      &http.Client{Transport: transport},
		transport: transport,
	}
}

// Close releases idle connections.
func (c *Client) Close() {
	c.transport.CloseIdleConnections()
}

// StartSession starts one simulated session.
func (c *Client) StartSession(ctx context.Context, req SessionStartRequest) (SessionInfo, error) {
	var out SessionInfo
	err := c.do(ctx, "/v1/session/start", req, &out)
	return out, err
}

// Call invokes a tool on a session's MCP server process. The request timeout
// gets callTimeoutSlack beyond the tool call's own timeout.
func (c *Client) Call(ctx context.Context, req SessionCallRequest) (SessionCallResult, error) {
	if req.TimeoutMS > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.TimeoutMS)*time.Millisecond+callTimeoutSlack)
		defer cancel()
	}
	var out SessionCallResult
	err := c.do(ctx, "/v1/session/call", req, &out)
	return out, err
}

// Tools lists the tools of a session's MCP server process.
func (c *Client) Tools(ctx context.Context, req SessionIDRequest) (SessionToolsResult, error) {
	var out SessionToolsResult
	err := c.do(ctx, "/v1/session/tools", req, &out)
	return out, err
}

// Frames returns the frames a session recorded.
func (c *Client) Frames(ctx context.Context, req SessionFramesRequest) (SessionFramesResult, error) {
	var out SessionFramesResult
	err := c.do(ctx, "/v1/session/frames", req, &out)
	return out, err
}

// Fake applies one operation to a simulated session.
func (c *Client) Fake(ctx context.Context, req SessionFakeRequest) error {
	return c.do(ctx, "/v1/session/fake", req, nil)
}

// Errors returns the background failures a session's simulated session
// recorded since the previous call.
func (c *Client) Errors(ctx context.Context, req SessionIDRequest) (SessionErrorsResult, error) {
	var out SessionErrorsResult
	err := c.do(ctx, "/v1/session/errors", req, &out)
	return out, err
}

// MCP applies one operation to a session's MCP server processes.
func (c *Client) MCP(ctx context.Context, req SessionMCPRequest) (SessionMCPResult, error) {
	var out SessionMCPResult
	err := c.do(ctx, "/v1/session/mcp", req, &out)
	return out, err
}

// End ends a session.
func (c *Client) End(ctx context.Context, req SessionIDRequest) error {
	return c.do(ctx, "/v1/session/end", req, nil)
}

// WaitNoConnections waits until a simulated session tracks no inbox
// connection, for at most the request's timeout. The endpoint serves the
// end-to-end test harness only.
func (c *Client) WaitNoConnections(ctx context.Context, req SessionWaitNoConnectionsRequest) (SessionWaitNoConnectionsResult, error) {
	var out SessionWaitNoConnectionsResult
	err := c.do(ctx, "/v1/session/wait_no_conns", req, &out)
	return out, err
}

// InboxConnections counts the peers connected to a session's inbox socket.
func (c *Client) InboxConnections(ctx context.Context, req SessionIDRequest) (InboxConnectionsResult, error) {
	var out InboxConnectionsResult
	err := c.do(ctx, "/v1/session/inbox_conns", req, &out)
	return out, err
}

// StartBroker starts a Broker process.
func (c *Client) StartBroker(ctx context.Context, req BrokerStartRequest) (BrokerStartResult, error) {
	var out BrokerStartResult
	err := c.do(ctx, "/v1/broker/start", req, &out)
	return out, err
}

// SignalBroker sends a signal to the current Broker.
func (c *Client) SignalBroker(ctx context.Context, req BrokerSignalRequest) error {
	return c.do(ctx, "/v1/broker/signal", req, nil)
}

// WaitBroker waits for the current Broker to exit.
func (c *Client) WaitBroker(ctx context.Context, req BrokerWaitRequest) (BrokerWaitResult, error) {
	var out BrokerWaitResult
	err := c.do(ctx, "/v1/broker/wait", req, &out)
	return out, err
}

// BrokerLog returns the current Broker log, or every log so far.
func (c *Client) BrokerLog(ctx context.Context, req BrokerLogRequest) (BrokerLogResult, error) {
	var out BrokerLogResult
	err := c.do(ctx, "/v1/broker/log", req, &out)
	return out, err
}

// Local calls one RPC of the local interface.
func (c *Client) Local(ctx context.Context, req LocalRequest) (LocalResult, error) {
	var out LocalResult
	err := c.do(ctx, "/v1/local", req, &out)
	return out, err
}

// Procs lists this account's processes.
func (c *Client) Procs(ctx context.Context, req ProcsRequest) ([]ProcInfo, error) {
	var out []ProcInfo
	err := c.do(ctx, "/v1/procs", req, &out)
	return out, err
}

// Workspace creates a workspace.
func (c *Client) Workspace(ctx context.Context, req WorkspaceRequest) (WorkspaceResult, error) {
	var out WorkspaceResult
	err := c.do(ctx, "/v1/workspace", req, &out)
	return out, err
}

// ReadFile reads a file.
func (c *Client) ReadFile(ctx context.Context, req FileReadRequest) (FileReadResult, error) {
	var out FileReadResult
	err := c.do(ctx, "/v1/file/read", req, &out)
	return out, err
}

// WriteFile writes a file.
func (c *Client) WriteFile(ctx context.Context, req FileWriteRequest) error {
	return c.do(ctx, "/v1/file/write", req, nil)
}

// ListFiles lists a directory.
func (c *Client) ListFiles(ctx context.Context, req FileListRequest) (FileListResult, error) {
	var out FileListResult
	err := c.do(ctx, "/v1/file/list", req, &out)
	return out, err
}

// HoldPort holds a TCP port.
func (c *Client) HoldPort(ctx context.Context, req PortHoldRequest) (PortHoldResult, error) {
	var out PortHoldResult
	err := c.do(ctx, "/v1/port/hold", req, &out)
	return out, err
}

// ReleasePort releases a held port.
func (c *Client) ReleasePort(ctx context.Context, req PortReleaseRequest) error {
	return c.do(ctx, "/v1/port/release", req, nil)
}

// ProbeTCP reports whether a TCP address is reachable.
func (c *Client) ProbeTCP(ctx context.Context, req ProbeTCPRequest) (ProbeTCPResult, error) {
	var out ProbeTCPResult
	err := c.do(ctx, "/v1/probe/tcp", req, &out)
	return out, err
}

// OpenConn opens a TCP connection and keeps it.
func (c *Client) OpenConn(ctx context.Context, req ConnOpenRequest) (ConnOpenResult, error) {
	var out ConnOpenResult
	err := c.do(ctx, "/v1/probe/conn_open", req, &out)
	return out, err
}

// ConnAlive checks an open connection.
func (c *Client) ConnAlive(ctx context.Context, req ConnAliveRequest) (ConnAliveResult, error) {
	var out ConnAliveResult
	err := c.do(ctx, "/v1/probe/conn_alive", req, &out)
	return out, err
}

// FloodStart starts a flood of bare TCP connections from the node.
func (c *Client) FloodStart(ctx context.Context, req FloodStartRequest) error {
	return c.do(ctx, "/v1/flood/start", req, nil)
}

// FloodStats reports the counters of the running flood.
func (c *Client) FloodStats(ctx context.Context, req struct{}) (FloodStatsResult, error) {
	var out FloodStatsResult
	err := c.do(ctx, "/v1/flood/stats", req, &out)
	return out, err
}

// FloodStop stops the running flood.
func (c *Client) FloodStop(ctx context.Context, req struct{}) error {
	return c.do(ctx, "/v1/flood/stop", req, nil)
}

// ProbeHello sends a Hello to a Broker without a pairing key.
func (c *Client) ProbeHello(ctx context.Context, req ProbeHelloRequest) (ProbeHelloResult, error) {
	var out ProbeHelloResult
	err := c.do(ctx, "/v1/probe/hello", req, &out)
	return out, err
}

// RelayStart starts the man-in-the-middle relay.
func (c *Client) RelayStart(ctx context.Context, req RelayStartRequest) error {
	return c.do(ctx, "/v1/relay/start", req, nil)
}

// RelayStats reports what the relay forwarded.
func (c *Client) RelayStats(ctx context.Context) (RelayStatsResult, error) {
	var out RelayStatsResult
	err := c.do(ctx, "/v1/relay/stats", struct{}{}, &out)
	return out, err
}

// Exec runs a one-off command as the node account.
func (c *Client) Exec(ctx context.Context, req ExecRequest) (ExecResult, error) {
	var out ExecResult
	err := c.do(ctx, "/v1/exec", req, &out)
	return out, err
}

// Shutdown asks the node agent to stop.
func (c *Client) Shutdown(ctx context.Context) error {
	return c.do(ctx, "/v1/shutdown", struct{}{}, nil)
}

// do sends one request and decodes the answer.
func (c *Client) do(ctx context.Context, path string, req, out any) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultTimeout)
		defer cancel()
	}

	var body []byte
	if req != nil {
		data, err := json.Marshal(req)
		if err != nil {
			return fmt.Errorf("ctl: encoding request to %s: %w", path, err)
		}
		body = data
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://ctl"+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("ctl: building request to %s: %w", path, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return fmt.Errorf("ctl: request to %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxAnswerBody))
	if err != nil {
		return fmt.Errorf("ctl: reading answer from %s: %w", path, err)
	}
	var envelope Envelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return fmt.Errorf("ctl: decoding answer from %s: %w", path, err)
	}
	if !envelope.OK {
		if envelope.Error == "" {
			return fmt.Errorf("ctl: %s failed", path)
		}
		return errors.New(envelope.Error)
	}
	if out != nil && len(envelope.Result) > 0 {
		if err := json.Unmarshal(envelope.Result, out); err != nil {
			return fmt.Errorf("ctl: decoding result from %s: %w", path, err)
		}
	}
	return nil
}
