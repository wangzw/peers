package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/wangzw/peers/test/e2e/ctl"
	"github.com/wangzw/peers/test/e2e/node"
)

// TestMain turns the test binary into the MCP stand-in that
// TestOneshotSessionPrintsResult passes as --bin when E2E_NODE_STUB is set.
// The node library's own tests use the same re-execution technique; this copy
// lives here because a test binary serves only its own package's TestMain.
func TestMain(m *testing.M) {
	if os.Getenv("E2E_NODE_STUB") == "mcp" {
		os.Exit(runMCPStub())
	}
	os.Exit(m.Run())
}

// stubListAgent is one entry of the stub's list_agents answer.
type stubListAgent struct {
	Address string `json:"address"`
	Status  string `json:"status"`
}

// stubListOut is the structured result of the stub's list_agents tool.
type stubListOut struct {
	Self   string          `json:"self_address"`
	Agents []stubListAgent `json:"agents"`
}

// runMCPStub serves an MCP server on standard input and output with a
// list_agents tool that always succeeds.
func runMCPStub() int {
	server := mcp.NewServer(&mcp.Implementation{Name: "e2e-node-stub", Version: "0"}, nil)
	mcp.AddTool[any, stubListOut](
		server,
		&mcp.Tool{Name: "list_agents", Description: "Returns a fixed agent listing."},
		func(context.Context, *mcp.CallToolRequest, any) (*mcp.CallToolResult, stubListOut, error) {
			return nil, stubListOut{
				Self:   "stub@hostA",
				Agents: []stubListAgent{{Address: "stub@hostA", Status: "idle"}},
			}, nil
		},
	)
	if err := server.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		fmt.Fprintf(os.Stderr, "e2e-node stub: serving MCP: %v\n", err)
		return 1
	}
	return 0
}

// TestOneshotSessionPrintsResult runs oneshot-session against the MCP stand-in
// and checks that standard output carries a parseable ToolResult with no tool
// error.
func TestOneshotSessionPrintsResult(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "e2e-node")

	build := exec.Command("go", "build", "-o", binary, ".")
	var buildStderr bytes.Buffer
	build.Stderr = &buildStderr
	if err := build.Run(); err != nil {
		t.Fatalf("building e2e-node: %v\n%s", err, buildStderr.String())
	}

	stub, err := os.Executable()
	if err != nil {
		t.Fatalf("locating the test binary: %v", err)
	}

	var stdout, stderr bytes.Buffer
	cmd := exec.Command(binary, "oneshot-session",
		"--name", "stub-session",
		"--bin", stub,
		"--env", "E2E_NODE_STUB=mcp")
	// The session writes its record and MCP log under $HOME, so give it a
	// temporary home rather than the test runner's.
	cmd.Env = append(os.Environ(), "HOME="+t.TempDir())
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("oneshot-session exited with %v\nstdout: %s\nstderr: %s", err, stdout.String(), stderr.String())
	}

	var result node.ToolResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decoding the ToolResult %q: %v", stdout.String(), err)
	}
	if result.IsError {
		t.Fatalf("list_agents returned a tool error: %s", result.Text)
	}
	var out stubListOut
	if err := json.Unmarshal(result.Structured, &out); err != nil {
		t.Fatalf("decoding the structured content %q: %v", result.Structured, err)
	}
	if out.Self != "stub@hostA" {
		t.Fatalf("self_address = %q, want stub@hostA", out.Self)
	}
	if len(out.Agents) != 1 || out.Agents[0].Address != "stub@hostA" {
		t.Fatalf("agents = %+v, want the stub's single entry", out.Agents)
	}
}

// TestWithCoverageAddsGoCoverDir checks that GOCOVERDIR joins the BaseEnv the
// command builds only when coverage is on, that it keeps the caller's entries,
// and that it does not mutate the map it was given.
func TestWithCoverageAddsGoCoverDir(t *testing.T) {
	base := node.BaseEnv{
		Home:  "/home/peer",
		Path:  "/usr/bin",
		Extra: map[string]string{"PEERS_TEST_RUNTIME_ROOT": "/tmp/root"},
	}
	if _, ok := base.Extra["GOCOVERDIR"]; ok {
		t.Fatal("the fixture already names GOCOVERDIR")
	}

	t.Setenv(coverageDirEnv, "")
	off := withCoverage(base)
	if _, ok := envValues(off.Build(nil))["GOCOVERDIR"]; ok {
		t.Fatalf("withCoverage without coverage added GOCOVERDIR: %v", off.Build(nil))
	}
	if got := off.Extra["PEERS_TEST_RUNTIME_ROOT"]; got != "/tmp/root" {
		t.Fatalf("withCoverage without coverage changed the extra map: %v", off.Extra)
	}

	t.Setenv(coverageDirEnv, "/e2e-cover")
	on := withCoverage(node.BaseEnv{Home: "/home/peer", Path: "/usr/bin", Extra: base.Extra})
	values := envValues(on.Build(nil))
	if got := values["GOCOVERDIR"]; got != "/e2e-cover" {
		t.Fatalf("GOCOVERDIR = %q, want /e2e-cover", got)
	}
	if got := values["PEERS_TEST_RUNTIME_ROOT"]; got != "/tmp/root" {
		t.Fatalf("PEERS_TEST_RUNTIME_ROOT = %q, want /tmp/root", got)
	}
	if _, ok := base.Extra["GOCOVERDIR"]; ok {
		t.Fatal("withCoverage mutated the caller's extra map")
	}

	// An environment with no extras gets one.
	bare := withCoverage(node.BaseEnv{Home: "/home/peer", Path: "/usr/bin"})
	if got := envValues(bare.Build(nil))["GOCOVERDIR"]; got != "/e2e-cover" {
		t.Fatalf("GOCOVERDIR for a bare environment = %q, want /e2e-cover", got)
	}
}

// envValues turns NAME=value strings into a map.
func envValues(env []string) map[string]string {
	values := make(map[string]string, len(env))
	for _, entry := range env {
		if name, value, ok := strings.Cut(entry, "="); ok {
			values[name] = value
		}
	}
	return values
}

// TestServeShutdown starts the command, asks it to stop over the control
// socket, and checks that it exits with zero and removes the socket.
func TestServeShutdown(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "e2e-node")

	build := exec.Command("go", "build", "-o", binary, ".")
	var buildStderr bytes.Buffer
	build.Stderr = &buildStderr
	if err := build.Run(); err != nil {
		t.Fatalf("building e2e-node: %v\n%s", err, buildStderr.String())
	}

	home := filepath.Join(dir, "home")
	if err := os.MkdirAll(home, 0o700); err != nil {
		t.Fatalf("creating the account home: %v", err)
	}
	socket := filepath.Join(dir, "ctl.sock")

	var stderr bytes.Buffer
	cmd := exec.Command(binary, "serve", "--ctl", socket, "--home", home, "--bin", binary)
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting e2e-node: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	client := ctl.Dial(socket)
	defer client.Close()

	deadline := time.Now().Add(10 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := client.Shutdown(ctx)
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			t.Fatalf("the control socket did not answer within 10s: %v\n%s", err, stderr.String())
		}
		time.Sleep(20 * time.Millisecond)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("e2e-node exited with %v\n%s", err, stderr.String())
		}
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("e2e-node did not exit after shutdown\n%s", stderr.String())
	}

	if _, err := os.Stat(socket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("control socket %s still present after shutdown: %v", socket, err)
	}
}
