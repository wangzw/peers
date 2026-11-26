// Command e2e-node is the resident node agent the end-to-end tests drive from
// the host. It exposes the node library over the control protocol on a Unix
// socket inside the container.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/wangzw/peers/test/e2e/ctl"
	"github.com/wangzw/peers/test/e2e/node"
)

const shutdownBound = 2 * time.Second

const probeHelloBound = 10 * time.Second

const oneshotCallBound = 30 * time.Second

// oneshotReadyBound bounds the wait for the first connection-loop result; a
// timeout is an error, not a starting result. It is the fallback for an event
// the command cannot force, so it is generous.
const oneshotReadyBound = 30 * time.Second

// oneshotStartingText must be a substring of the product's starting-state tool
// error; update it when the product changes the wording.
const oneshotStartingText = "still being set up"

// coverageDirEnv is the coverage directory variable the harness and syscontainer
// also read.
const coverageDirEnv = "E2E_COVER_DIR"

func withCoverage(env node.BaseEnv) node.BaseEnv {
	dir := os.Getenv(coverageDirEnv)
	if dir == "" {
		return env
	}
	extra := make(map[string]string, len(env.Extra)+1)
	for name, value := range env.Extra {
		extra[name] = value
	}
	extra["GOCOVERDIR"] = dir
	env.Extra = extra
	return env
}

const usage = "usage: e2e-node serve --ctl <socket> --home <dir> --bin <peers> [--runtime-root <dir>] [--path <PATH>]\n" +
	"       e2e-node probe-hello --addr <host:port>\n" +
	"       e2e-node oneshot-session --name <name> --bin <peers> [--env NAME=value]..."

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, usage)
		return 2
	}
	switch args[0] {
	case "serve":
		return runServe(args[1:])
	case "probe-hello":
		return runProbeHello(args[1:])
	case "oneshot-session":
		return runOneshotSession(args[1:])
	default:
		fmt.Fprintln(os.Stderr, usage)
		return 2
	}
}

func runProbeHello(args []string) int {
	flags := flag.NewFlagSet("probe-hello", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	addr := flags.String("addr", "", "Broker link address")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *addr == "" {
		fmt.Fprintln(os.Stderr, "e2e-node: probe-hello needs --addr")
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), probeHelloBound)
	defer cancel()
	reason, err := node.ProbeHello(ctx, *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e-node: probe-hello: %v\n", err)
		return 1
	}
	fmt.Printf("REASON=%s\n", reason.String())
	return 0
}

type envFlags map[string]string

func (e envFlags) String() string {
	entries := make([]string, 0, len(e))
	for name, value := range e {
		entries = append(entries, name+"="+value)
	}
	return strings.Join(entries, ",")
}

func (e envFlags) Set(pair string) error {
	name, value, ok := strings.Cut(pair, "=")
	if !ok || name == "" {
		return fmt.Errorf("want NAME=value, got %q", pair)
	}
	e[name] = value
	return nil
}

// runOneshotSession calls list_agents in one simulated session and prints the
// ToolResult as JSON; a tool error exits 0 and only an infrastructure failure
// exits 1.
func runOneshotSession(args []string) int {
	flags := flag.NewFlagSet("oneshot-session", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	name := flags.String("name", "", "session name")
	binary := flags.String("bin", "", "peers binary")
	env := envFlags{}
	flags.Var(env, "env", "NAME=value added to the MCP server environment (repeatable)")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *name == "" || *binary == "" || flags.NArg() > 0 {
		fmt.Fprintln(os.Stderr, "e2e-node: oneshot-session needs --name and --bin")
		return 2
	}

	home, err := os.UserHomeDir()
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e-node: oneshot-session: finding the account home: %v\n", err)
		return 1
	}
	tb := node.NewTB("oneshot-session", "")
	defer tb.Close()

	session, err := node.StartSession(context.Background(), tb,
		withCoverage(node.BaseEnv{Home: home, Path: os.Getenv("PATH")}),
		node.MCPCommand{Path: *binary, Args: []string{"mcp"}},
		node.SessionSpec{Name: *name, StartMCP: true, Env: env})
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e-node: oneshot-session: starting the session: %v\n", err)
		return 1
	}

	// The MCP server can report its starting state before the loop's first result.
	result, err := oneshotListAgents(context.Background(), session)
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e-node: oneshot-session: calling list_agents: %v\n", err)
		return 1
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fmt.Fprintf(os.Stderr, "e2e-node: oneshot-session: writing the result: %v\n", err)
		return 1
	}
	if err := session.End(); err != nil {
		fmt.Fprintf(os.Stderr, "e2e-node: oneshot-session: ending the session: %v\n", err)
		return 1
	}
	return 0
}

// oneshotListAgents retries list_agents every 50ms while the MCP server reports
// its starting state, up to oneshotReadyBound.
func oneshotListAgents(ctx context.Context, session *node.Session) (node.ToolResult, error) {
	deadline := time.Now().Add(oneshotReadyBound)
	for {
		callCtx, cancel := context.WithTimeout(ctx, oneshotCallBound)
		result, err := session.Call(callCtx, "list_agents", nil)
		cancel()
		if err != nil {
			return node.ToolResult{}, err
		}
		if !result.IsError || !strings.Contains(result.Text, oneshotStartingText) {
			return result, nil
		}
		if !time.Now().Before(deadline) {
			return node.ToolResult{}, fmt.Errorf("e2e-node: oneshot-session: the MCP server stayed in its starting state for %s", oneshotReadyBound)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func runServe(args []string) int {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	socket := flags.String("ctl", "", "control socket path")
	home := flags.String("home", "", "account home directory")
	binary := flags.String("bin", "", "peers binary")
	runtimeRoot := flags.String("runtime-root", "", "value of PEERS_TEST_RUNTIME_ROOT")
	path := flags.String("path", os.Getenv("PATH"), "value of PATH for child processes")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *socket == "" || *home == "" || *binary == "" {
		fmt.Fprintln(os.Stderr, "e2e-node: --ctl, --home and --bin are required")
		return 2
	}

	logDir := filepath.Dir(*socket)
	logFile, err := os.OpenFile(filepath.Join(logDir, "node.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e-node: opening the log: %v\n", err)
		return 1
	}
	defer func() { _ = logFile.Close() }()
	logf := func(format string, args ...any) {
		_, _ = fmt.Fprintf(logFile, format+"\n", args...)
	}

	// A socket left behind by a previous run makes Listen fail.
	if err := os.Remove(*socket); err != nil && !errors.Is(err, os.ErrNotExist) {
		fmt.Fprintf(os.Stderr, "e2e-node: removing the stale socket: %v\n", err)
		return 1
	}
	listener, err := net.Listen("unix", *socket)
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e-node: listening on %s: %v\n", *socket, err)
		return 1
	}
	if err := os.Chmod(*socket, 0o600); err != nil {
		_ = listener.Close()
		fmt.Fprintf(os.Stderr, "e2e-node: setting the socket mode: %v\n", err)
		return 1
	}

	extra := make(map[string]string)
	if *runtimeRoot != "" {
		extra["PEERS_TEST_RUNTIME_ROOT"] = *runtimeRoot
	}
	env := withCoverage(node.BaseEnv{Home: *home, Path: *path, Extra: extra})
	tb := node.NewTB("e2e-node", "")
	agent := node.NewAgent(tb, env, *binary, logDir)

	server := &http.Server{
		Handler:           ctl.NewHandler(agent),
		ReadHeaderTimeout: 10 * time.Second,
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	logf("e2e-node: serving on %s", *socket)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case <-signals:
			agent.Shutdown()
		case <-agent.Done():
		}
	}()

	exit := 0
	select {
	case <-agent.Done():
	case err := <-serveDone:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logf("e2e-node: serving: %v", err)
			exit = 1
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), shutdownBound)
	defer cancel()
	_ = server.Shutdown(ctx)
	if err := agent.Close(); err != nil {
		logf("e2e-node: closing: %v", err)
	}
	if err := os.Remove(*socket); err != nil && !errors.Is(err, os.ErrNotExist) {
		logf("e2e-node: removing the socket: %v", err)
	}
	logf("e2e-node: stopped")
	return exit
}
