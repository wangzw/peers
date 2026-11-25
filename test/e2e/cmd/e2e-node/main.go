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

// shutdownBound bounds the HTTP server shutdown.
const shutdownBound = 2 * time.Second

// probeHelloBound bounds one probe-hello invocation.
const probeHelloBound = 10 * time.Second

// oneshotCallBound bounds the single tool call of oneshot-session. The call
// itself answers at once while the Broker is unavailable; the bound only keeps
// a broken MCP server from hanging the command.
const oneshotCallBound = 30 * time.Second

// oneshotReadyBound bounds how long oneshot-session waits for the MCP server's
// background connection loop to finish its first selection. A tool call made
// immediately after the handshake can still see the loop's starting state. It
// is the fallback for an event the command cannot force, so it is generous; a
// timeout is reported as an error rather than returned as a starting result.
const oneshotReadyBound = 30 * time.Second

// oneshotStartingText is the part of the starting-state tool error that tells
// the loop has not classified the Broker yet.
const oneshotStartingText = "still being set up"

// coverageDirEnv names the e2e coverage directory, the same variable the
// harness and syscontainer use.
const coverageDirEnv = "E2E_COVER_DIR"

// withCoverage adds GOCOVERDIR to env when coverage collection is on, so the
// children started with env write their counters to the directory the container
// mounted. env is left untouched when coverage is off.
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

// run carries out one invocation of the command and returns its exit code.
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

// runProbeHello sends one unpaired Link Hello to addr and prints the reject
// reason as REASON=<reason>. The end-to-end tests run it inside a container,
// where the Broker's link address is reachable.
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

// envFlags collects repeated --env NAME=value flags into one map.
type envFlags map[string]string

// String renders the collected values for the flag package's usage message.
func (e envFlags) String() string {
	entries := make([]string, 0, len(e))
	for name, value := range e {
		entries = append(entries, name+"="+value)
	}
	return strings.Join(entries, ",")
}

// Set records one NAME=value pair. The first "=" splits the pair, so a value
// may contain "=".
func (e envFlags) Set(pair string) error {
	name, value, ok := strings.Cut(pair, "=")
	if !ok || name == "" {
		return fmt.Errorf("want NAME=value, got %q", pair)
	}
	e[name] = value
	return nil
}

// runOneshotSession runs one simulated Claude Code session of the account that
// invokes the command, starts an MCP server process, calls list_agents and
// prints the ToolResult as JSON. The account's own "$HOME/.claude" is
// the Claude configuration directory, so the session record and inbox socket
// live in that account's home. A tool call that fails is still a successful
// invocation: it prints the error result and exits 0. Only an infrastructure
// failure, such as a session that will not start, exits 1.
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

	// The MCP server reports its background connection loop before it reports
	// the loop's first result. Repeat the call while that starting state is
	// showing, so the printed result carries the actual outcome, and make the
	// ordinary path one call.
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

// oneshotListAgents calls list_agents once and, while the MCP server says its
// connection loop is still starting, calls it again every 50ms for up to
// oneshotReadyBound. A tool error that is not the starting state is a result,
// not a failure of the command.
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

// runServe carries out one serve invocation.
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

	// A socket left behind by a previous run would make Listen fail.
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
