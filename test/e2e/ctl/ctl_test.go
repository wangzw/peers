package ctl_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/wangzw/peers/test/e2e/ctl"
	"github.com/wangzw/peers/test/e2e/internal/pathcmp"
	"github.com/wangzw/peers/test/e2e/node"
)

// TestMain turns the test binary into the stand-in programs the control tests
// start, and otherwise runs the tests.
func TestMain(m *testing.M) {
	switch mode := os.Getenv("E2E_NODE_STUB"); mode {
	case "":
		os.Exit(m.Run())
	case "mcp":
		os.Exit(runMCPStub())
	case "broker-fail":
		os.Exit(runBrokerFailStub())
	default:
		fmt.Fprintf(os.Stderr, "ctl stub: unknown mode %q\n", mode)
		os.Exit(2)
	}
}

type stubEchoOut struct {
	Env map[string]string `json:"env"`
	Cwd string            `json:"cwd"`
}

func runMCPStub() int {
	server := mcp.NewServer(&mcp.Implementation{Name: "ctl-stub", Version: "0"}, nil)
	mcp.AddTool[any, stubEchoOut](
		server,
		&mcp.Tool{Name: "echo_env", Description: "Returns the process environment and working directory."},
		func(context.Context, *mcp.CallToolRequest, any) (*mcp.CallToolResult, stubEchoOut, error) {
			env := make(map[string]string)
			for _, entry := range os.Environ() {
				name, value, _ := strings.Cut(entry, "=")
				env[name] = value
			}
			cwd, err := os.Getwd()
			if err != nil {
				return nil, stubEchoOut{}, err
			}
			return nil, stubEchoOut{Env: env, Cwd: cwd}, nil
		},
	)
	mcp.AddTool[any, any](
		server,
		&mcp.Tool{Name: "fail", Description: "Always returns an error result."},
		func(context.Context, *mcp.CallToolRequest, any) (*mcp.CallToolResult, any, error) {
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{&mcp.TextContent{Text: "stub failure"}},
			}, nil, nil
		},
	)
	if err := server.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		fmt.Fprintf(os.Stderr, "ctl stub: serving MCP: %v\n", err)
		return 1
	}
	return 0
}

func runBrokerFailStub() int {
	fmt.Fprintln(os.Stderr, `{"time":"2026-01-01T00:00:00Z","level":"INFO","event":"starting"}`)
	fmt.Fprintln(os.Stderr, "peers service run: listen address :1 is not available")
	return 4
}

func newTestAgent(t *testing.T) (node.BaseEnv, *ctl.Client) {
	t.Helper()

	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("locating the test binary: %v", err)
	}
	dir, err := os.MkdirTemp("/tmp", "apctl-")
	if err != nil {
		t.Fatalf("creating the control directory: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	env := node.BaseEnv{Home: t.TempDir(), Path: os.Getenv("PATH")}
	tb := node.NewTB("ctl-test", t.TempDir())
	agent := node.NewAgent(tb, env, executable, dir)
	t.Cleanup(func() { _ = agent.Close() })

	socket := filepath.Join(dir, "ctl.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listening on %s: %v", socket, err)
	}
	server := &http.Server{Handler: ctl.NewHandler(agent)}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })

	client := ctl.Dial(socket)
	t.Cleanup(client.Close)
	return env, client
}

// TestCtlSessionRoundTrip starts, calls, renames, reads frames from, lists tools of and ends a session through the control protocol, and checks its tools match a session started directly through the node library.
func TestCtlSessionRoundTrip(t *testing.T) {
	env, client := newTestAgent(t)
	ctx := context.Background()

	stubEnv := map[string]string{"E2E_NODE_STUB": "mcp", "GORACE": "atexit_sleep_ms=0"}
	info, err := client.StartSession(ctx, ctl.SessionStartRequest{
		Name:     "round",
		Mode:     "close-after-read",
		StartMCP: true,
		Env:      stubEnv,
	})
	if err != nil {
		t.Fatalf("starting a session: %v", err)
	}
	if info.ID == "" || info.FakePID <= 0 || info.MCPPID <= 0 {
		t.Fatalf("session info = %+v", info)
	}
	if _, err := os.Stat(info.RecordPath); err != nil {
		t.Errorf("record %s does not exist: %v", info.RecordPath, err)
	}
	if _, err := os.Stat(info.SocketPath); err != nil {
		t.Errorf("inbox socket %s does not exist: %v", info.SocketPath, err)
	}

	result, err := client.Call(ctx, ctl.SessionCallRequest{ID: info.ID, Tool: "echo_env"})
	if err != nil {
		t.Fatalf("calling echo_env: %v", err)
	}
	if result.IsError {
		t.Fatalf("echo_env returned a tool error: %s", result.Text)
	}
	var echo stubEchoOut
	if err := json.Unmarshal(result.Structured, &echo); err != nil {
		t.Fatalf("decoding echo_env: %v", err)
	}
	if echo.Env["HOME"] != env.Home {
		t.Errorf("HOME = %q, want %q", echo.Env["HOME"], env.Home)
	}
	cwdMatches, err := pathcmp.SamePath(echo.Cwd, env.Home)
	if err != nil {
		t.Fatalf("comparing the working directory: %v", err)
	}
	if !cwdMatches {
		t.Errorf("working directory = %q, want %q", echo.Cwd, env.Home)
	}

	if err := client.Fake(ctx, ctl.SessionFakeRequest{ID: info.ID, Op: "rename", Arg: "renamed"}); err != nil {
		t.Fatalf("renaming the session: %v", err)
	}

	frames, err := client.Frames(ctx, ctl.SessionFramesRequest{ID: info.ID})
	if err != nil {
		t.Fatalf("reading frames: %v", err)
	}
	if !frames.OK {
		t.Error("Frames reported OK=false")
	}
	if len(frames.Frames) != 0 {
		t.Errorf("frames = %d, want 0", len(frames.Frames))
	}

	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("locating the test binary: %v", err)
	}
	direct, err := node.StartSession(ctx, t, env, node.MCPCommand{Path: executable, Args: []string{"mcp"}}, node.SessionSpec{
		Name:      "direct",
		ConfigDir: t.TempDir(),
		StartMCP:  true,
		Env:       stubEnv,
	})
	if err != nil {
		t.Fatalf("starting the direct session: %v", err)
	}
	defer func() { _ = direct.End() }()
	directTools, err := direct.Tools(ctx)
	if err != nil {
		t.Fatalf("listing direct tools: %v", err)
	}
	ctlTools, err := client.Tools(ctx, ctl.SessionIDRequest{ID: info.ID})
	if err != nil {
		t.Fatalf("listing tools through the control protocol: %v", err)
	}
	if !slices.Equal(directTools, ctlTools.Tools) {
		t.Errorf("tools through ctl = %v, direct = %v", ctlTools.Tools, directTools)
	}

	if err := client.End(ctx, ctl.SessionIDRequest{ID: info.ID}); err != nil {
		t.Fatalf("ending the session: %v", err)
	}
	if _, err := os.Stat(info.RecordPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("record %s still present after End: %v", info.RecordPath, err)
	}
}

// TestCtlFatalBecomesErrorResponse checks that a fakesession Fatalf becomes a
// failed answer and that the agent keeps serving requests.
func TestCtlFatalBecomesErrorResponse(t *testing.T) {
	_, client := newTestAgent(t)
	ctx := context.Background()

	// A regular file cannot become a directory, so binding fails even for root.
	parent := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(parent, []byte("x"), 0o600); err != nil {
		t.Fatalf("creating the blocking file: %v", err)
	}
	_, err := client.StartSession(ctx, ctl.SessionStartRequest{
		Name:       "bad-bind",
		ConfigDir:  t.TempDir(),
		SocketPath: filepath.Join(parent, "inbox.sock"),
	})
	if err == nil {
		t.Fatal("StartSession succeeded with an unbindable socket path")
	}
	if !strings.Contains(err.Error(), "fakesession: binding") {
		t.Fatalf("error = %q, want it to contain %q", err, "fakesession: binding")
	}

	if _, err := client.Tools(ctx, ctl.SessionIDRequest{ID: "s1"}); err == nil {
		t.Fatal("the agent stopped answering after a FatalError")
	}
}

// TestCtlBrokerStartFailureIsResult checks that a Broker that exits before
// becoming ready is a product result, not a failed request.
func TestCtlBrokerStartFailureIsResult(t *testing.T) {
	_, client := newTestAgent(t)
	ctx := context.Background()

	result, err := client.StartBroker(ctx, ctl.BrokerStartRequest{
		Env:     map[string]string{"E2E_NODE_STUB": "broker-fail"},
		BoundMS: 5000,
	})
	if err != nil {
		t.Fatalf("StartBroker returned an error: %v", err)
	}
	if result.Running {
		t.Error("Running = true, want false")
	}
	if result.ExitCode != 4 {
		t.Errorf("exit code = %d, want 4", result.ExitCode)
	}
	if want := "listen address :1 is not available"; result.StderrLast != want {
		t.Errorf("stderr last line = %q, want %q", result.StderrLast, want)
	}
}

// TestCtlFilePathConfined checks that the file endpoints refuse paths outside
// the account home and /tmp, and work inside it.
func TestCtlFilePathConfined(t *testing.T) {
	env, client := newTestAgent(t)
	ctx := context.Background()

	if _, err := client.ReadFile(ctx, ctl.FileReadRequest{Path: "/etc/passwd"}); err == nil {
		t.Fatal("ReadFile accepted /etc/passwd")
	}

	path := filepath.Join(env.Home, "sub", "value.txt")
	if err := client.WriteFile(ctx, ctl.FileWriteRequest{Path: path, Data: []byte("hello"), Mode: 0o600}); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	read, err := client.ReadFile(ctx, ctl.FileReadRequest{Path: path})
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	if string(read.Data) != "hello" {
		t.Errorf("data = %q, want %q", read.Data, "hello")
	}
	listed, err := client.ListFiles(ctx, ctl.FileListRequest{Path: filepath.Dir(path)})
	if err != nil {
		t.Fatalf("listing %s: %v", filepath.Dir(path), err)
	}
	if !slices.Contains(listed.Names, "value.txt") {
		t.Errorf("names = %v, want value.txt", listed.Names)
	}
}
