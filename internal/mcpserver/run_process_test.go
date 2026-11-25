//go:build peers_testenv

package mcpserver

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/wangzw/peers/internal/testkit/fakelocal"
	"github.com/wangzw/peers/internal/testkit/socktest"
)

func init() {
	testRoles["mcp"] = mcpRole
}

// mcpRole is the child half of the process tests: it runs the MCP server over
// stdio exactly as cmd/peers does, and exits with Run's result.
func mcpRole() int {
	if err := Run(context.Background(), Options{}); err != nil {
		return 1
	}
	return 0
}

// runProcessEnv builds a child environment from scratch: only PATH, HOME and
// the role are inherited, so the engineer's Claude Code session variables
// cannot leak into the child.
func runProcessEnv(t *testing.T, extra ...string) []string {
	t.Helper()
	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + t.TempDir(),
		"PEERS_TEST_ROLE=mcp",
	}
	return append(env, extra...)
}

// startRunProcess starts the test binary as the MCP server over stdio and
// connects an MCP client to it.
func startRunProcess(t *testing.T, env []string) (*mcp.ClientSession, *exec.Cmd) {
	t.Helper()
	cmd := exec.Command(os.Args[0])
	cmd.Env = env
	transport := &mcp.CommandTransport{Command: cmd}

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "1"}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	cs, err := client.Connect(ctx, transport, nil)
	if err != nil {
		t.Fatalf("connecting to the MCP server process: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs, cmd
}

// REQ-agent-messaging-001, REQ-agent-messaging-036: the process registers at
// startup without any tool call, and registration depends only on the two
// environment variables, so a session with no interactive terminal registers
// the same way.
func TestRunProcessServesMCP(t *testing.T) {
	sock := fakelocal.SocketPath(t)
	srv := fakelocal.Start(t, sock, fakelocal.Handlers{})
	env := runProcessEnv(t,
		"PEERS_TEST_SOCKET="+sock,
		"CLAUDE_CODE_MESSAGING_SOCKET=/s",
	)
	cs, _ := startRunProcess(t, env)

	if got := len(listTools(t, cs)); got != 13 {
		t.Fatalf("ListTools returned %d tools, want 13", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	conn, err := srv.NextRegister(ctx)
	if err != nil {
		t.Fatalf("the child did not register: %v", err)
	}
	if got := conn.First.GetInboxSocketPath(); got != "/s" {
		t.Fatalf("Register inbox_socket_path = %q, want /s", got)
	}
}

// REQ-agent-messaging-038
func TestRunProcessSurvivesMissingBroker(t *testing.T) {
	missing := socktest.Path(t, "missing.sock")
	env := runProcessEnv(t,
		"PEERS_TEST_SOCKET="+missing,
		"CLAUDE_CODE_MESSAGING_SOCKET=/s",
	)
	cs, cmd := startRunProcess(t, env)

	if got := len(listTools(t, cs)); got != 13 {
		t.Fatalf("ListTools returned %d tools, want 13", got)
	}

	var res *mcp.CallToolResult
	deadline := time.Now().Add(eventTimeout)
	for time.Now().Before(deadline) {
		res = callTool(t, cs, "list_agents", nil)
		if res.IsError && strings.HasPrefix(textContent(t, res), "list_agents failed:") {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !res.IsError {
		t.Fatal("list_agents succeeded without a Broker")
	}
	if text := textContent(t, res); !strings.Contains(text, "PEERS_TEST_SOCKET") {
		t.Errorf("list_agents error = %q, want the PEERS_TEST_SOCKET path", text)
	}

	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("the MCP server process exited after a Broker failure: %v", err)
	}
}
