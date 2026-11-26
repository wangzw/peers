package node

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// stubHandlers re-executes this test binary as a stand-in program, which
// TestMain runs when E2E_NODE_STUB names it.
var stubHandlers = map[string]func() int{}

func registerStub(name string, fn func() int) {
	stubHandlers[name] = fn
}

func init() {
	registerStub("mcp", runMCPStub)
}

// TestMain re-executes the test binary as a stand-in program when
// E2E_NODE_STUB is set, and otherwise runs the tests.
func TestMain(m *testing.M) {
	if mode := os.Getenv("E2E_NODE_STUB"); mode != "" {
		fn, ok := stubHandlers[mode]
		if !ok {
			fmt.Fprintf(os.Stderr, "node stub: unknown mode %q\n", mode)
			os.Exit(2)
		}
		os.Exit(fn())
	}
	os.Exit(m.Run())
}

type stubEchoOut struct {
	Env map[string]string `json:"env"`
	Cwd string            `json:"cwd"`
}

func runMCPStub() int {
	watchStubSignals()

	server := mcp.NewServer(&mcp.Implementation{Name: "e2e-node-stub", Version: "0"}, nil)
	mcp.AddTool[any, stubEchoOut](
		server,
		&mcp.Tool{Name: "echo_env", Description: "Returns the environment and working directory of the server process."},
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
		fmt.Fprintf(os.Stderr, "node stub: serving MCP: %v\n", err)
		return 1
	}
	return 0
}

func watchStubSignals() {
	signals := make(chan os.Signal, 4)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	ignoreInterrupt := os.Getenv("STUB_IGNORE_INT") == "1"
	logPath := os.Getenv("STUB_SIGNAL_LOG")

	go func() {
		for sig := range signals {
			if logPath != "" {
				file, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
				if err == nil {
					_, _ = file.WriteString(sig.String() + "\n")
					_ = file.Close()
				}
			}
			if sig == os.Interrupt && ignoreInterrupt {
				continue
			}
			os.Exit(0)
		}
	}()
}
