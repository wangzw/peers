package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	// inboxReadTimeout bounds how long a sender may take to write its line.
	// A loaded runner can delay the write, so it uses the package fallback.
	inboxReadTimeout = eventTimeout
	// controlReadTimeout bounds how long a control client may take to send its
	// request line, keeping shutdown from waiting on an idle connection.
	controlReadTimeout = 15 * time.Second
	// controlDialTimeout bounds connecting to an agent's control socket.
	controlDialTimeout = 5 * time.Second
)

func init() {
	register("agent run", cmdAgentRun)
	register("agent call", cmdAgentCall)
	register("agent set", cmdAgentSet)
}

// agentRecord is the session record the simulated Agent writes, in the shape
// Claude Code uses: the registry keyed by pid, with the inbox socket path.
type agentRecord struct {
	PID                 int    `json:"pid"`
	Name                string `json:"name"`
	Status              string `json:"status"`
	MessagingSocketPath string `json:"messagingSocketPath"`
}

// controlRequest is one line on the control socket.
type controlRequest struct {
	Op     string          `json:"op"`
	Tool   string          `json:"tool,omitempty"`
	Args   json.RawMessage `json:"args,omitempty"`
	Name   string          `json:"name,omitempty"`
	Status string          `json:"status,omitempty"`
}

// controlResponse is the one-line answer to a control request.
type controlResponse struct {
	OK       bool   `json:"ok"`
	Text     string `json:"text,omitempty"`
	Started  string `json:"started,omitempty"`
	Returned string `json:"returned,omitempty"`
}

// recvEntry is one line of the inbox receive log.
type recvEntry struct {
	T        string `json:"t"`
	PeerPID  int    `json:"peer_pid"`
	FromName string `json:"from_name"`
	Content  string `json:"content"`
}

// inboxFrame carries the content of a user frame; other frame types decode to
// an empty content but are still logged.
type inboxFrame struct {
	Message struct {
		Content string `json:"content"`
	} `json:"message"`
}

func cmdAgentRun(args []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runAgentRun(ctx, args)
}

func cmdAgentCall(args []string) error {
	return runAgentCall(args, os.Stdout)
}

func cmdAgentSet(args []string) error {
	return runAgentSet(args, os.Stdout)
}

// agentServer is one running simulated Agent.
type agentServer struct {
	ctx        context.Context
	recordPath string
	socketPath string
	recvLog    *os.File
	inbox      net.Listener
	ctl        net.Listener
	mcp        *mcp.ClientSession

	recordMu sync.Mutex
	recvMu   sync.Mutex
	wg       sync.WaitGroup
}

// runAgentRun implements "rsprobe agent run": it registers a session record,
// listens on an inbox socket, starts a real "peers mcp" process for the
// tools and serves the control socket until ctx is cancelled.
func runAgentRun(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("agent run", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	configDir := fs.String("config-dir", "", "Claude Code configuration directory")
	name := fs.String("name", "", "session name")
	cwd := fs.String("cwd", "", "working directory of the MCP process")
	ctl := fs.String("ctl", "", "control socket path")
	recvLog := fs.String("recv-log", "", "append-only log of received frames")
	bin := fs.String("peers", "", "peers binary (default: from PATH)")
	socketPath := fs.String("socket-path", "", "inbox socket path (default: next to the control socket)")
	status := fs.String("status", "idle", "state recorded in the session record")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if *configDir == "" || *name == "" || *cwd == "" || *ctl == "" || *recvLog == "" {
		return errors.New("agent run needs --config-dir, --name, --cwd, --ctl and --recv-log")
	}

	peersBin := *bin
	if peersBin == "" {
		resolved, err := exec.LookPath("peers")
		if err != nil {
			return fmt.Errorf("peers not found on PATH: %v", err)
		}
		peersBin = resolved
	}
	if *socketPath == "" {
		*socketPath = filepath.Join(filepath.Dir(*ctl), fmt.Sprintf("%d.inbox.sock", os.Getpid()))
	}

	server := &agentServer{
		ctx:        ctx,
		recordPath: filepath.Join(*configDir, "sessions", fmt.Sprintf("%d.json", os.Getpid())),
		socketPath: *socketPath,
	}
	defer server.shutdown()

	if err := os.MkdirAll(filepath.Dir(server.recordPath), 0o700); err != nil {
		return fmt.Errorf("creating the session registry: %v", err)
	}
	record := agentRecord{
		PID:                 os.Getpid(),
		Name:                *name,
		Status:              *status,
		MessagingSocketPath: *socketPath,
	}
	if err := writeAgentRecord(server.recordPath, record); err != nil {
		return err
	}

	logFile, err := os.OpenFile(*recvLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("opening the receive log: %v", err)
	}
	server.recvLog = logFile

	inbox, err := net.Listen("unix", *socketPath)
	if err != nil {
		return fmt.Errorf("listening on the inbox socket %s: %v", *socketPath, err)
	}
	server.inbox = inbox
	server.wg.Add(1)
	go server.acceptInbox()

	cmd := exec.Command(peersBin, "mcp")
	cmd.Dir = *cwd
	cmd.Env = agentChildEnv(*socketPath, *configDir)
	client := mcp.NewClient(&mcp.Implementation{Name: "rsprobe-agent", Version: "0"}, nil)
	session, err := client.Connect(ctx, &mcp.CommandTransport{Command: cmd}, nil)
	if err != nil {
		return fmt.Errorf("starting %s mcp: %v", peersBin, err)
	}
	server.mcp = session

	control, err := net.Listen("unix", *ctl)
	if err != nil {
		return fmt.Errorf("listening on the control socket %s: %v", *ctl, err)
	}
	server.ctl = control
	server.wg.Add(1)
	go server.acceptControl()

	<-ctx.Done()
	return nil
}

// shutdown releases every resource the Agent acquired. It is safe to call
// after a partial start.
func (s *agentServer) shutdown() {
	if s.ctl != nil {
		_ = s.ctl.Close()
	}
	if s.inbox != nil {
		_ = s.inbox.Close()
	}
	s.wg.Wait()
	if s.mcp != nil {
		_ = s.mcp.Close()
	}
	if s.recvLog != nil {
		_ = s.recvLog.Close()
	}
	if s.recordPath != "" {
		_ = os.Remove(s.recordPath)
	}
	if s.socketPath != "" {
		_ = os.Remove(s.socketPath)
	}
}

func (s *agentServer) acceptInbox() {
	defer s.wg.Done()
	for {
		conn, err := s.inbox.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleInbox(conn)
		}()
	}
}

// handleInbox reads one frame line, records it and closes the connection so
// the sender sees the receiver has taken the frame.
func (s *agentServer) handleInbox(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	_ = conn.SetReadDeadline(time.Now().Add(inboxReadTimeout))

	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil && len(line) == 0 {
		return
	}
	content := ""
	var frame inboxFrame
	if err := json.Unmarshal(line, &frame); err == nil {
		content = frame.Message.Content
	}
	entry := recvEntry{
		T:        time.Now().UTC().Format(time.RFC3339Nano),
		PeerPID:  recordedPeerPID(conn, peerPID, os.Stderr),
		FromName: fromNameOf(content),
		Content:  content,
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return
	}
	s.recvMu.Lock()
	defer s.recvMu.Unlock()
	_, _ = s.recvLog.Write(append(data, '\n'))
}

func (s *agentServer) acceptControl() {
	defer s.wg.Done()
	for {
		conn, err := s.ctl.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleControl(conn)
		}()
	}
}

// handleControl serves one request line and one response line per connection.
func (s *agentServer) handleControl(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	_ = conn.SetReadDeadline(time.Now().Add(controlReadTimeout))

	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil && len(line) == 0 {
		return
	}
	_ = conn.SetReadDeadline(time.Time{})

	var req controlRequest
	if err := json.Unmarshal(line, &req); err != nil {
		writeControlResponse(conn, controlResponse{OK: false, Text: "bad request: " + err.Error()})
		return
	}

	switch req.Op {
	case "call":
		writeControlResponse(conn, s.callTool(req))
	case "set":
		writeControlResponse(conn, s.setRecord(req))
	default:
		writeControlResponse(conn, controlResponse{OK: false, Text: "unknown op " + req.Op})
	}
}

// callTool invokes one MCP tool and reports its text content.
func (s *agentServer) callTool(req controlRequest) controlResponse {
	started := time.Now().UTC()
	resp := controlResponse{Started: started.Format(time.RFC3339Nano)}
	if s.mcp == nil {
		resp.Text = "MCP process is not running"
		resp.Returned = time.Now().UTC().Format(time.RFC3339Nano)
		return resp
	}
	var args any
	if len(req.Args) > 0 {
		args = json.RawMessage(req.Args)
	}
	result, err := s.mcp.CallTool(s.ctx, &mcp.CallToolParams{Name: req.Tool, Arguments: args})
	resp.Returned = time.Now().UTC().Format(time.RFC3339Nano)
	if err != nil {
		resp.Text = err.Error()
		return resp
	}
	resp.OK = !result.IsError
	resp.Text = textContent(result.Content)
	return resp
}

// setRecord rewrites the session record in place.
func (s *agentServer) setRecord(req controlRequest) controlResponse {
	switch {
	case req.Name != "" && req.Status != "":
		return controlResponse{OK: false, Text: "set takes name or status, not both"}
	case req.Name == "" && req.Status == "":
		return controlResponse{OK: false, Text: "set needs name or status"}
	}

	s.recordMu.Lock()
	defer s.recordMu.Unlock()
	record, err := readAgentRecord(s.recordPath)
	if err != nil {
		return controlResponse{OK: false, Text: err.Error()}
	}
	if req.Name != "" {
		record.Name = req.Name
	}
	if req.Status != "" {
		record.Status = req.Status
	}
	if err := writeAgentRecord(s.recordPath, record); err != nil {
		return controlResponse{OK: false, Text: err.Error()}
	}
	return controlResponse{OK: true}
}

func writeControlResponse(conn net.Conn, resp controlResponse) {
	_ = conn.SetWriteDeadline(time.Now().Add(inboxReadTimeout))
	_ = json.NewEncoder(conn).Encode(resp)
}

// readAgentRecord reads a session record.
func readAgentRecord(path string) (agentRecord, error) {
	var record agentRecord
	data, err := os.ReadFile(path)
	if err != nil {
		return record, err
	}
	if err := json.Unmarshal(data, &record); err != nil {
		return record, fmt.Errorf("decoding the session record: %v", err)
	}
	return record, nil
}

// writeAgentRecord truncates and rewrites an existing record, preserving its
// inode the way Claude Code's in-place rewrites do.
func writeAgentRecord(path string, record agentRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encoding the session record: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("writing the session record: %v", err)
	}
	return nil
}

// agentChildEnv is the MCP process environment: the current environment with
// every inherited Claude Code session variable removed, then the two variables
// the MCP server needs.
func agentChildEnv(socketPath, configDir string) []string {
	env := os.Environ()
	out := make([]string, 0, len(env)+2)
	for _, entry := range env {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, "CLAUDE_CODE_") || name == "CLAUDE_CONFIG_DIR" {
			continue
		}
		out = append(out, entry)
	}
	return append(out,
		"CLAUDE_CODE_MESSAGING_SOCKET="+socketPath,
		"CLAUDE_CONFIG_DIR="+configDir,
	)
}

// fromNameOf returns the raw from-name attribute from the first line of an
// attribution envelope, or "" when there is none.
func fromNameOf(content string) string {
	line, _, _ := strings.Cut(content, "\n")
	const attr = `from-name="`
	idx := strings.Index(line, attr)
	if idx < 0 {
		return ""
	}
	rest := line[idx+len(attr):]
	end := strings.LastIndex(rest, `"`)
	if end < 0 {
		return ""
	}
	return rest[:end]
}

// peerCredentialUnsupported explains why a sender's pid cannot be recorded.
const peerCredentialUnsupported = "peer credentials are not available on this platform"

// recordedPeerPID returns the peer pid to write to the receive log. It reports
// an unsupported platform on warn and records 0 for an unknown sender; a frame
// is still logged, because accepting it never depends on its credentials.
func recordedPeerPID(conn net.Conn, read func(net.Conn) (int, error), warn io.Writer) int {
	pid, err := read(conn)
	if err == nil {
		return pid
	}
	if errors.Is(err, errors.ErrUnsupported) {
		_, _ = fmt.Fprintln(warn, "rsprobe: "+peerCredentialUnsupported)
	}
	return 0
}

// textContent joins the text blocks of a tool result.
func textContent(content []mcp.Content) string {
	texts := make([]string, 0, len(content))
	for _, item := range content {
		if text, ok := item.(*mcp.TextContent); ok {
			texts = append(texts, text.Text)
		}
	}
	return strings.Join(texts, "\n")
}

// runAgentCall implements "rsprobe agent call --ctl CTL <tool> [<json>]".
func runAgentCall(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("agent call", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	ctl := fs.String("ctl", "", "control socket path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *ctl == "" {
		return errors.New("agent call needs --ctl")
	}
	if fs.NArg() < 1 || fs.NArg() > 2 {
		return errors.New("usage: rsprobe agent call --ctl CTL <tool> [<json>]")
	}
	req := controlRequest{Op: "call", Tool: fs.Arg(0)}
	if fs.NArg() == 2 {
		raw := fs.Arg(1)
		if !json.Valid([]byte(raw)) {
			return fmt.Errorf("arguments are not valid JSON: %s", raw)
		}
		req.Args = json.RawMessage(raw)
	}
	resp, err := exchangeControl(*ctl, req)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(stdout).Encode(resp); err != nil {
		return err
	}
	if !resp.OK {
		return errors.New("tool call returned an error")
	}
	return nil
}

// runAgentSet implements "rsprobe agent set --ctl CTL (--name X | --status Y)".
func runAgentSet(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("agent set", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	ctl := fs.String("ctl", "", "control socket path")
	name := fs.String("name", "", "new session name")
	status := fs.String("status", "", "new session status")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *ctl == "" {
		return errors.New("agent set needs --ctl")
	}
	var nameSet, statusSet bool
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "name":
			nameSet = true
		case "status":
			statusSet = true
		}
	})
	if nameSet == statusSet {
		return errors.New("agent set needs exactly one of --name or --status")
	}
	if nameSet && *name == "" {
		return errors.New("--name must not be empty")
	}
	if statusSet && *status == "" {
		return errors.New("--status must not be empty")
	}

	resp, err := exchangeControl(*ctl, controlRequest{Op: "set", Name: *name, Status: *status})
	if err != nil {
		return err
	}
	if err := json.NewEncoder(stdout).Encode(resp); err != nil {
		return err
	}
	if !resp.OK {
		return errors.New("set failed")
	}
	return nil
}

// exchangeControl sends one request to a control socket and reads its answer.
func exchangeControl(ctl string, req controlRequest) (*controlResponse, error) {
	conn, err := net.DialTimeout("unix", ctl, controlDialTimeout)
	if err != nil {
		return nil, fmt.Errorf("control at %s: %v", ctl, err)
	}
	defer func() { _ = conn.Close() }()
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return nil, fmt.Errorf("sending to %s: %v", ctl, err)
	}
	var resp controlResponse
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return nil, fmt.Errorf("reading the answer from %s: %v", ctl, err)
	}
	return &resp, nil
}
