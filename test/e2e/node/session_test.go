package node

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/testkit/fakesession"
	"github.com/wangzw/peers/test/e2e/internal/pathcmp"
)

func sessionTestEnv(t *testing.T) BaseEnv {
	t.Helper()
	return BaseEnv{Home: t.TempDir(), Path: os.Getenv("PATH")}
}

// sessionTestCommand re-executes the test binary, which TestMain turns into the
// MCP stub when E2E_NODE_STUB=mcp.
func sessionTestCommand(t *testing.T) MCPCommand {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("locating the test binary: %v", err)
	}
	return MCPCommand{Path: exe}
}

func startStubSession(t *testing.T, env BaseEnv, spec SessionSpec) *Session {
	t.Helper()
	if spec.Env == nil {
		spec.Env = make(map[string]string)
	}
	spec.Env["E2E_NODE_STUB"] = "mcp"
	// The stub is the race-instrumented test binary, whose race runtime sleeps a
	// second before exiting by default; that would let StopMCP's 100 ms and
	// 400 ms escalation kill a stub that already exited.
	if _, ok := spec.Env["GORACE"]; !ok {
		spec.Env["GORACE"] = "atexit_sleep_ms=0"
	}
	spec.StartMCP = true
	s, err := StartSession(context.Background(), t, env, sessionTestCommand(t), spec)
	if err != nil {
		t.Fatalf("starting session: %v", err)
	}
	return s
}

type sessionTestEcho struct {
	Env map[string]string `json:"env"`
	Cwd string            `json:"cwd"`
}

func callEchoEnv(t *testing.T, ctx context.Context, proc interface {
	Call(context.Context, string, map[string]any) (ToolResult, error)
}) sessionTestEcho {
	t.Helper()
	result, err := proc.Call(ctx, "echo_env", nil)
	if err != nil {
		t.Fatalf("calling echo_env: %v", err)
	}
	if result.IsError {
		t.Fatalf("echo_env returned a tool error: %s", result.Text)
	}
	var out sessionTestEcho
	if err := json.Unmarshal(result.Structured, &out); err != nil {
		t.Fatalf("decoding echo_env result %q: %v", result.Structured, err)
	}
	return out
}

// TestSessionChildEnvironmentIsExact checks that the MCP server process sees
// the complete environment the session driver built, never the environment of
// the test process.
func TestSessionChildEnvironmentIsExact(t *testing.T) {
	t.Setenv("E2E_CANARY", "1")
	ctx := context.Background()

	t.Run("default", func(t *testing.T) {
		env := sessionTestEnv(t)
		workdir := t.TempDir()
		configDir := t.TempDir()
		s := startStubSession(t, env, SessionSpec{
			Name:            "exact",
			Workdir:         workdir,
			ConfigDir:       configDir,
			ExportConfigDir: true,
		})
		defer func() { _ = s.End() }()

		out := callEchoEnv(t, ctx, s)
		if got := out.Env["CLAUDE_CODE_MESSAGING_SOCKET"]; got != s.Fake().SocketPath() {
			t.Errorf("CLAUDE_CODE_MESSAGING_SOCKET = %q, want %q", got, s.Fake().SocketPath())
		}
		cwdMatches, err := pathcmp.SamePath(out.Cwd, workdir)
		if err != nil {
			t.Fatalf("comparing the working directory: %v", err)
		}
		if !cwdMatches {
			t.Errorf("working directory = %q, want %q", out.Cwd, workdir)
		}
		if got := out.Env["HOME"]; got != env.Home {
			t.Errorf("HOME = %q, want %q", got, env.Home)
		}
		if _, ok := out.Env["E2E_CANARY"]; ok {
			t.Error("the child inherited E2E_CANARY from the test process")
		}
		if got := out.Env["CLAUDE_CONFIG_DIR"]; got != configDir {
			t.Errorf("CLAUDE_CONFIG_DIR = %q, want %q", got, configDir)
		}
	})

	t.Run("no socket env", func(t *testing.T) {
		env := sessionTestEnv(t)
		s := startStubSession(t, env, SessionSpec{Name: "no-socket-env", NoSocketEnv: true})
		defer func() { _ = s.End() }()

		out := callEchoEnv(t, ctx, s)
		if got, ok := out.Env["CLAUDE_CODE_MESSAGING_SOCKET"]; ok {
			t.Errorf("CLAUDE_CODE_MESSAGING_SOCKET = %q, want it absent", got)
		}
	})

	t.Run("no config dir export", func(t *testing.T) {
		env := sessionTestEnv(t)
		s := startStubSession(t, env, SessionSpec{Name: "no-config-env", ConfigDir: t.TempDir()})
		defer func() { _ = s.End() }()

		out := callEchoEnv(t, ctx, s)
		if got, ok := out.Env["CLAUDE_CONFIG_DIR"]; ok {
			t.Errorf("CLAUDE_CONFIG_DIR = %q, want it absent", got)
		}
	})
}

// TestSessionCallResults checks that a successful tool result carries
// structured content and that a tool error is reported in the result rather
// than as a transport error.
func TestSessionCallResults(t *testing.T) {
	ctx := context.Background()
	s := startStubSession(t, sessionTestEnv(t), SessionSpec{Name: "results"})
	defer func() { _ = s.End() }()

	ok, err := s.Call(ctx, "echo_env", nil)
	if err != nil {
		t.Fatalf("calling echo_env: %v", err)
	}
	if ok.IsError {
		t.Errorf("echo_env reported IsError")
	}
	if len(ok.Structured) == 0 {
		t.Error("echo_env returned no structured content")
	}

	failed, err := s.Call(ctx, "fail", nil)
	if err != nil {
		t.Fatalf("a tool error was returned as a transport error: %v", err)
	}
	if !failed.IsError {
		t.Error("fail reported IsError=false")
	}

	tools, err := s.Tools(ctx)
	if err != nil {
		t.Fatalf("listing tools: %v", err)
	}
	names := make([]string, len(tools))
	for i, tool := range tools {
		names[i] = tool.Name
	}
	if want := []string{"echo_env", "fail"}; !slices.Equal(names, want) {
		t.Errorf("tools = %v, want %v", names, want)
	}
}

// TestStopMCPEscalates checks that StopMCP escalates the signal until the
// process exits and returns its exit code.
func TestStopMCPEscalates(t *testing.T) {
	t.Run("ignores interrupt", func(t *testing.T) {
		logPath := filepath.Join(t.TempDir(), "signals")
		s := startStubSession(t, sessionTestEnv(t), SessionSpec{
			Name: "escalate",
			Env: map[string]string{
				"STUB_SIGNAL_LOG": logPath,
				"STUB_IGNORE_INT": "1",
			},
		})
		defer func() { _ = s.End() }()

		if _, err := s.StopMCP(syscall.SIGINT); err != nil {
			t.Fatalf("stopping the MCP server: %v", err)
		}
		got := readSignalLog(t, logPath)
		if want := []string{"interrupt", "terminated"}; !slices.Equal(got, want) {
			t.Fatalf("signals = %v, want %v", got, want)
		}
	})

	t.Run("exits on interrupt", func(t *testing.T) {
		logPath := filepath.Join(t.TempDir(), "signals")
		s := startStubSession(t, sessionTestEnv(t), SessionSpec{
			Name: "interrupt",
			Env:  map[string]string{"STUB_SIGNAL_LOG": logPath},
		})
		defer func() { _ = s.End() }()

		code, err := s.StopMCP(syscall.SIGINT)
		if err != nil {
			t.Fatalf("stopping the MCP server: %v", err)
		}
		if code != 0 {
			t.Errorf("exit code = %d, want 0", code)
		}
		got := readSignalLog(t, logPath)
		if want := []string{"interrupt"}; !slices.Equal(got, want) {
			t.Fatalf("signals = %v, want %v", got, want)
		}
	})
}

func readSignalLog(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the signal log: %v", err)
	}
	return strings.Fields(string(data))
}

// TestEndRemovesRecord checks that ending a session removes its registry
// record.
func TestEndRemovesRecord(t *testing.T) {
	s := startStubSession(t, sessionTestEnv(t), SessionSpec{Name: "end"})
	recordPath := s.Fake().RecordPath()

	if err := s.End(); err != nil {
		t.Fatalf("ending the session: %v", err)
	}
	if _, err := os.Stat(recordPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("record %s still present after End: %v", recordPath, err)
	}
}

// TestExtraMCPSharesEnvironment checks that an extra MCP server process gets
// the same environment as the first one.
func TestExtraMCPSharesEnvironment(t *testing.T) {
	ctx := context.Background()
	s := startStubSession(t, sessionTestEnv(t), SessionSpec{Name: "extra"})
	defer func() { _ = s.End() }()

	extra, err := s.StartExtraMCP(ctx)
	if err != nil {
		t.Fatalf("starting an extra MCP server: %v", err)
	}

	first := callEchoEnv(t, ctx, s)
	second := callEchoEnv(t, ctx, extra)
	if !reflect.DeepEqual(first.Env, second.Env) {
		t.Errorf("extra MCP environment differs:\nfirst: %v\nextra: %v", first.Env, second.Env)
	}
	if first.Cwd != second.Cwd {
		t.Errorf("extra MCP working directory = %q, want %q", second.Cwd, first.Cwd)
	}
}

// TestInboxConnectionsCountsWriters checks that InboxConnections reports the
// accepted inbox connections.
func TestInboxConnectionsCountsWriters(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("InboxConnections reads /proc/net/unix")
	}

	env := sessionTestEnv(t)
	s, err := StartSession(context.Background(), t, env, sessionTestCommand(t), SessionSpec{
		Name: "inbox",
		Mode: fakesession.HoldOpen,
	})
	if err != nil {
		t.Fatalf("starting session: %v", err)
	}
	defer func() { _ = s.End() }()

	conn, err := net.Dial("unix", s.Fake().SocketPath())
	if err != nil {
		t.Fatalf("dialing the inbox socket: %v", err)
	}
	frame := []byte(`{"msg_id":"m1","type":"message","message":{"content":"hi"}}` + "\n")
	if _, err := conn.Write(frame); err != nil {
		_ = conn.Close()
		t.Fatalf("writing a frame: %v", err)
	}
	if _, ok := s.Fake().WaitFrame(2 * time.Second); !ok {
		_ = conn.Close()
		t.Fatal("the session did not record the frame")
	}

	if got := sessionTestWaitInbox(t, s, 1); got != 1 {
		t.Fatalf("InboxConnections() = %d while the peer is connected, want 1", got)
	}

	s.Fake().Release()
	if err := conn.Close(); err != nil {
		t.Fatalf("closing the client connection: %v", err)
	}
	if got := sessionTestWaitInbox(t, s, 0); got != 0 {
		t.Fatalf("InboxConnections() = %d after the peer left, want 0", got)
	}
}

func sessionTestWaitInbox(t *testing.T, s *Session, want int) int {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		n, err := s.InboxConnections()
		if err != nil {
			t.Fatalf("counting inbox connections: %v", err)
		}
		if n == want || time.Now().After(deadline) {
			return n
		}
		time.Sleep(10 * time.Millisecond)
	}
}
