package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/wangzw/peers/internal/testkit/fakesession"
)

// mcpHandshakeBound is the upper bound for starting an MCP server process and
// completing the MCP handshake; it matches the 30-second startup allowance
// Claude Code gives a server.
const mcpHandshakeBound = 30 * time.Second

const (
	mcpInterruptWait = 100 * time.Millisecond
	mcpTerminateWait = 400 * time.Millisecond
	mcpKillWait      = 10 * time.Second
)

// MCPCommand names the MCP server program and its arguments. It is normally
// the peers binary with the "mcp" argument.
type MCPCommand struct {
	// Path is the program to run.
	Path string
	// Args are its arguments.
	Args []string
}

// SessionSpec describes one simulated Claude Code session: a fakesession
// record and socket plus, when StartMCP is set, a real MCP server process.
type SessionSpec struct {
	// Name and Status are written to the session record.
	Name, Status string

	// ConfigDir is the Claude configuration directory. Empty means
	// $HOME/.claude.
	ConfigDir string

	// ExportConfigDir exports CLAUDE_CONFIG_DIR to the MCP server process.
	ExportConfigDir bool

	// Workdir is the working directory of the MCP server process. Empty means
	// $HOME.
	Workdir string

	// Mode selects how the fake handles accepted connections.
	Mode fakesession.Mode

	// BusyDelay delays reading each accepted connection, simulating a busy
	// session; zero reads immediately.
	BusyDelay time.Duration

	// NoBind writes only the registry record and binds no socket.
	NoBind bool

	// SocketPath is the inbox socket path; empty means a generated one.
	SocketPath string

	// NoSocketEnv suppresses CLAUDE_CODE_MESSAGING_SOCKET in the MCP server
	// process environment.
	NoSocketEnv bool

	// Env adds environment variables to the MCP server process, on top of the
	// session's BaseEnv.
	Env map[string]string

	// StartMCP starts the MCP server process together with the simulated
	// session.
	StartMCP bool
}

// ToolInfo describes one tool as tools/list reports it.
type ToolInfo struct {
	// Name is the tool name.
	Name string `json:"name"`
	// Annotations is the annotations block tools/list returned, or nil when the
	// server sent none. A nil block carries no hints, so ReadOnly reports false.
	Annotations *ToolAnnotations `json:"annotations,omitempty"`
}

// ToolAnnotations carries the tool annotations this harness reads; the product
// sets only readOnlyHint.
type ToolAnnotations struct {
	// ReadOnlyHint reports whether the tool is annotated read-only.
	ReadOnlyHint bool `json:"readOnlyHint"`
}

// ReadOnly reports whether the tool is annotated read-only. A tool without an
// annotations block is not read-only.
func (t ToolInfo) ReadOnly() bool {
	return t.Annotations != nil && t.Annotations.ReadOnlyHint
}

// ToolResult is the outcome of one MCP tool call. A tool that reports an error
// inside its result sets IsError but still returns a nil error; err is reserved
// for protocol and transport failures.
type ToolResult struct {
	// IsError reports whether the tool call returned an error result.
	IsError bool
	// Text is every text content block joined with "\n".
	Text string
	// Structured is the JSON encoding of StructuredContent, or nil when the
	// result has none.
	Structured json.RawMessage
	// Elapsed is how long the call took.
	Elapsed time.Duration
}

// Session is one simulated Claude Code session.
type Session struct {
	tb        testing.TB
	env       BaseEnv
	mcp       MCPCommand
	spec      SessionSpec
	fake      *fakesession.Fake
	configDir string
	workdir   string

	mu       sync.Mutex
	proc     *MCPProc
	extras   []*MCPProc
	mcpCount int
	ended    bool
}

// StartSession creates the simulated session and, when spec.StartMCP is set,
// starts an MCP server process and completes the MCP handshake. The MCP
// process sees only env plus the variables named by spec, never the
// environment of the test process.
func StartSession(ctx context.Context, tb testing.TB, env BaseEnv, mcpCommand MCPCommand, spec SessionSpec) (*Session, error) {
	tb.Helper()

	configDir := spec.ConfigDir
	if configDir == "" {
		configDir = filepath.Join(env.Home, ".claude")
	}
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		return nil, fmt.Errorf("node: creating config dir %s: %w", configDir, err)
	}
	workdir := spec.Workdir
	if workdir == "" {
		workdir = env.Home
	}

	fake := fakesession.New(tb, fakesession.Options{
		ConfigDir:  configDir,
		Name:       spec.Name,
		Status:     spec.Status,
		Mode:       spec.Mode,
		BusyDelay:  spec.BusyDelay,
		NoBind:     spec.NoBind,
		SocketPath: spec.SocketPath,
	})

	s := &Session{
		tb:        tb,
		env:       env,
		mcp:       mcpCommand,
		spec:      spec,
		fake:      fake,
		configDir: configDir,
		workdir:   workdir,
	}
	tb.Cleanup(func() { _ = s.End() })

	if spec.StartMCP {
		proc, err := s.startMCP(ctx)
		if err != nil {
			fake.RemoveRecord()
			fake.Close()
			return nil, err
		}
		s.proc = proc
	}
	return s, nil
}

// Fake returns the simulated session.
func (s *Session) Fake() *fakesession.Fake { return s.fake }

// MCPPID returns the process ID of the current MCP server process, or 0 when
// there is none.
func (s *Session) MCPPID() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.proc == nil {
		return 0
	}
	return s.proc.pid
}

// Tools lists the tools exposed by the current MCP server process, with the
// annotations tools/list returned for each.
func (s *Session) Tools(ctx context.Context) ([]ToolInfo, error) {
	proc, err := s.currentProc()
	if err != nil {
		return nil, err
	}
	result, err := proc.client.ListTools(ctx, &mcp.ListToolsParams{})
	if err != nil {
		return nil, fmt.Errorf("node: listing tools: %w", err)
	}
	tools := make([]ToolInfo, 0, len(result.Tools))
	for _, tool := range result.Tools {
		info := ToolInfo{Name: tool.Name}
		if tool.Annotations != nil {
			info.Annotations = &ToolAnnotations{ReadOnlyHint: tool.Annotations.ReadOnlyHint}
		}
		tools = append(tools, info)
	}
	return tools, nil
}

// Call invokes a tool on the current MCP server process.
func (s *Session) Call(ctx context.Context, tool string, args map[string]any) (ToolResult, error) {
	proc, err := s.currentProc()
	if err != nil {
		return ToolResult{}, err
	}
	return proc.Call(ctx, tool, args)
}

// StopMCP sends sig to the current MCP server process, escalating to SIGTERM
// and then SIGKILL when the process does not exit in time, and closes the
// client session. The exit code is -1 when the process was terminated by a
// signal.
func (s *Session) StopMCP(sig syscall.Signal) (int, error) {
	s.mu.Lock()
	proc := s.proc
	s.proc = nil
	s.mu.Unlock()
	if proc == nil {
		return 0, fmt.Errorf("node: session %q has no MCP server process", s.spec.Name)
	}
	return proc.Stop(sig)
}

// RestartMCP starts a new MCP server process with the same environment and
// working directory after the previous one was stopped.
func (s *Session) RestartMCP(ctx context.Context) error {
	s.mu.Lock()
	ended := s.ended
	running := s.proc != nil
	s.mu.Unlock()
	if ended {
		return fmt.Errorf("node: session %q has ended", s.spec.Name)
	}
	if running {
		return fmt.Errorf("node: session %q already has an MCP server process", s.spec.Name)
	}

	proc, err := s.startMCP(ctx)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.proc = proc
	s.mu.Unlock()
	return nil
}

// StartExtraMCP starts another MCP server process for the session while the
// current one keeps running. The extra process is what a subagent definition
// that declares its own MCP server produces.
func (s *Session) StartExtraMCP(ctx context.Context) (*MCPProc, error) {
	s.mu.Lock()
	ended := s.ended
	s.mu.Unlock()
	if ended {
		return nil, fmt.Errorf("node: session %q has ended", s.spec.Name)
	}
	proc, err := s.startMCP(ctx)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.extras = append(s.extras, proc)
	s.mu.Unlock()
	return proc, nil
}

// End ends the session: every MCP server process is stopped with SIGINT and
// the same escalation as StopMCP, then the session record is removed and the
// inbox socket closed. It is idempotent.
func (s *Session) End() error {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return nil
	}
	s.ended = true
	proc := s.proc
	extras := s.extras
	s.proc = nil
	s.extras = nil
	s.mu.Unlock()

	var firstErr error
	for _, extra := range extras {
		if _, err := extra.Stop(syscall.SIGINT); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if proc != nil {
		if _, err := proc.Stop(syscall.SIGINT); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	s.fake.RemoveRecord()
	s.fake.Close()
	return firstErr
}

// MCPLogTail returns the last lines the current MCP server process wrote to
// standard error. The second value is false exactly when the session has no MCP
// server process, for example after StopMCP, even though its log file stays on
// disk. When an extra process runs, the main process's log is the one reported.
func (s *Session) MCPLogTail(lines int) (string, bool) {
	s.mu.Lock()
	proc := s.proc
	s.mu.Unlock()
	if proc == nil {
		return "", false
	}
	return proc.logTail(lines), true
}

func (s *Session) currentProc() (*MCPProc, error) {
	s.mu.Lock()
	proc := s.proc
	s.mu.Unlock()
	if proc == nil {
		return nil, fmt.Errorf("node: session %q has no MCP server process", s.spec.Name)
	}
	return proc, nil
}

func (s *Session) startMCP(ctx context.Context) (*MCPProc, error) {
	s.mu.Lock()
	s.mcpCount++
	n := s.mcpCount
	s.mu.Unlock()

	logPath := filepath.Join(s.env.Home, fmt.Sprintf("mcp-%d-%d.log", s.fake.PID(), n))
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("node: opening MCP log %s: %w", logPath, err)
	}

	more := make(map[string]string, len(s.spec.Env)+2)
	if !s.spec.NoSocketEnv {
		more["CLAUDE_CODE_MESSAGING_SOCKET"] = s.fake.SocketPath()
	}
	if s.spec.ExportConfigDir {
		more["CLAUDE_CONFIG_DIR"] = s.configDir
	}
	for name, value := range s.spec.Env {
		more[name] = value
	}

	cmd := exec.Command(s.mcp.Path, s.mcp.Args...)
	cmd.Dir = s.workdir
	cmd.Env = s.env.Build(more)
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{}
	setDeathSignal(cmd.SysProcAttr)

	// The process transport is used rather than mcp.CommandTransport because
	// this side has to call Wait itself to observe the 100 ms and 400 ms
	// escalation steps; CommandTransport would call Wait from its Close
	// concurrently with that watch, which the race detector rejects.
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("node: creating MCP stdout pipe: %w", err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("node: creating MCP stdin pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("node: starting MCP server %s: %w", s.mcp.Path, err)
	}

	client := mcp.NewClient(&mcp.Implementation{Name: "e2e-node", Version: "0"}, nil)
	handshakeCtx, cancel := context.WithTimeout(ctx, mcpHandshakeBound)
	defer cancel()
	clientSession, err := client.Connect(handshakeCtx, &mcp.IOTransport{Reader: stdout, Writer: stdin}, nil)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		_ = logFile.Close()
		return nil, fmt.Errorf("node: connecting to MCP server %s: %w", s.mcp.Path, err)
	}
	return newMCPProc(cmd, clientSession, logFile), nil
}

// MCPProc is one running MCP server process.
type MCPProc struct {
	cmd    *exec.Cmd
	client *mcp.ClientSession
	log    *os.File
	pid    int

	done    chan struct{}
	waitErr error

	closeOnce sync.Once
	closeErr  error
}

// newMCPProc starts waiting for the process so Stop can observe its exit. The
// command was started by startMCP; this is the only caller of its Wait.
func newMCPProc(cmd *exec.Cmd, client *mcp.ClientSession, logFile *os.File) *MCPProc {
	p := &MCPProc{
		cmd:    cmd,
		client: client,
		log:    logFile,
		pid:    cmd.Process.Pid,
		done:   make(chan struct{}),
	}
	go func() {
		p.waitErr = cmd.Wait()
		close(p.done)
	}()
	return p
}

// Call invokes a tool on this MCP server process.
func (p *MCPProc) Call(ctx context.Context, tool string, args map[string]any) (ToolResult, error) {
	if p.client == nil {
		return ToolResult{}, errors.New("node: MCP server process is not running")
	}
	start := time.Now()
	result, err := p.client.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: args})
	elapsed := time.Since(start)
	if err != nil {
		return ToolResult{Elapsed: elapsed}, fmt.Errorf("node: calling tool %q: %w", tool, err)
	}

	out := ToolResult{IsError: result.IsError, Elapsed: elapsed}
	var texts []string
	for _, content := range result.Content {
		if text, ok := content.(*mcp.TextContent); ok {
			texts = append(texts, text.Text)
		}
	}
	out.Text = strings.Join(texts, "\n")
	if result.StructuredContent != nil {
		data, err := json.Marshal(result.StructuredContent)
		if err != nil {
			return out, fmt.Errorf("node: encoding structured content of %q: %w", tool, err)
		}
		out.Structured = data
	}
	return out, nil
}

// Stop sends sig to the process, escalates to SIGTERM after mcpInterruptWait
// and then to SIGKILL after another mcpTerminateWait, closes the client session
// and returns the process exit code, or -1 when a signal terminated it. When
// the process has still not exited within mcpKillWait it returns -1 and an
// error.
func (p *MCPProc) Stop(sig syscall.Signal) (int, error) {
	if p.cmd.Process != nil {
		_ = p.cmd.Process.Signal(sig)
	}
	if !p.exited(mcpInterruptWait) {
		if p.cmd.Process != nil {
			_ = p.cmd.Process.Signal(syscall.SIGTERM)
		}
		if !p.exited(mcpTerminateWait) {
			if p.cmd.Process != nil {
				_ = p.cmd.Process.Kill()
			}
		}
	}
	if !p.exited(mcpKillWait) {
		return -1, fmt.Errorf("node: MCP server process %d did not exit", p.pid)
	}
	_ = p.closeClient()
	return p.exitCode(), nil
}

// logTail returns the last lines the process wrote to standard error, read
// through a separate descriptor because the child still has the file open.
func (p *MCPProc) logTail(lines int) string {
	if p.log == nil || lines <= 0 {
		return ""
	}
	data, err := os.ReadFile(p.log.Name())
	if err != nil {
		return ""
	}
	return tailLines(string(data), lines)
}

func tailLines(text string, n int) string {
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

func (p *MCPProc) exited(bound time.Duration) bool {
	select {
	case <-p.done:
		return true
	case <-time.After(bound):
		return false
	}
}

func (p *MCPProc) exitCode() int {
	<-p.done
	var exitErr *exec.ExitError
	switch {
	case p.waitErr == nil:
		return 0
	case errors.As(p.waitErr, &exitErr):
		return exitErr.ExitCode()
	default:
		return -1
	}
}

func (p *MCPProc) closeClient() error {
	p.closeOnce.Do(func() {
		if p.client != nil {
			p.closeErr = p.client.Close()
		}
		if p.log != nil {
			_ = p.log.Close()
		}
	})
	return p.closeErr
}
