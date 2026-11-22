//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakesession"
	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/syscontainer"
)

const (
	// peersBinaryPath is the peers build in the base image.
	peersBinaryPath = "/usr/local/bin/peers"
	// registerClientRoleEnv selects the registration client in TestMain.
	registerClientRoleEnv = "PEERS_E2E_ROLE"
	// registerClientRole is the role that opens a Register stream directly.
	registerClientRole = "register-client"
	// registerClientBound is the wait before the registration client gives up.
	// A registration that has not completed is reported after the Broker's
	// 10-second wait; three more seconds let that event arrive.
	registerClientBound = 13 * time.Second
	// registerClientCopiedPath is where the test binary is copied so that the
	// other account can execute it.
	registerClientCopiedPath = "/tmp/ap-e2e-register-client"
)

// TestREQBrokerConfig020_ForeignSocketOwnerRejected checks that one account
// cannot register another account's session as an agent. The first subtest
// covers a record the account cannot read; the second makes the record readable
// and checks the socket owner check that then decides the outcome.
func TestREQBrokerConfig020_ForeignSocketOwnerRejected(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Plain)
		return
	}

	t.Run("unreadable record", func(t *testing.T) {
		foreignSessionRejected(t, false)
	})
	t.Run("record readable but socket owned by another account", func(t *testing.T) {
		foreignSessionRejected(t, true)
	})
	// The positive control runs the same register-client path that the
	// refusals above use: the record exists and the socket belongs to the
	// caller's account, so the same path must report registration_complete.
	t.Run("session owner registers successfully", func(t *testing.T) {
		_ = syscontainer.StartSystemBroker(t, "broker:\n  alias: sys\n")

		configDir := "/home/peer/.claude-owner"
		fake := fakesession.New(t, fakesession.Options{
			ConfigDir:  configDir,
			Name:       "ownersession",
			SocketPath: "/tmp/ap-peer-owner.sock",
		})
		for _, path := range []string{configDir, filepath.Join(configDir, "sessions"), fake.RecordPath(), fake.SocketPath()} {
			cmd := syscontainer.Command("root", nil, "chown", "peer", path)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("syscontainer: chown peer %s: %v\n%s", path, err, strings.TrimSpace(string(out)))
			}
		}

		result := runRegisterClient(t, "peer", fake.SocketPath(), fake.ConfigDir())
		if result.Event != "complete" {
			t.Fatalf("register-client as the socket owner reported %q (reason %q), want complete",
				result.Event, result.Reason)
		}
	})
}

// foreignSessionRejected registers a peer session and then tries to register
// the same session as other. readableRecord makes the peer's registry record
// readable by other so that the socket owner check, not the read permission,
// decides the reason.
func foreignSessionRejected(t *testing.T, readableRecord bool) {
	t.Helper()

	// The configuration file must exist for the root status read to be granted.
	_ = syscontainer.StartSystemBroker(t, "broker:\n  alias: sys\n")

	a := syscontainer.NewSession(t, "peer", "a1")
	e2eSysWaitRegistered(t, a)
	aSelf := e2eSysSelfAddress(t, a)

	// The two subtests share one container. A record left by the earlier
	// subtest is unreadable by other and would make Lookup fail for every
	// record, so start from only this subtest's record.
	sessionsDir := filepath.Join(a.Fake.ConfigDir(), "sessions")
	entries, err := os.ReadDir(sessionsDir)
	if err != nil {
		t.Fatalf("listing %s: %v", sessionsDir, err)
	}
	for _, entry := range entries {
		path := filepath.Join(sessionsDir, entry.Name())
		if path == a.Fake.RecordPath() {
			continue
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			t.Fatalf("removing the stale session record %s: %v", path, err)
		}
	}

	if readableRecord {
		for _, dir := range []string{"/home/peer", a.Fake.ConfigDir(), filepath.Join(a.Fake.ConfigDir(), "sessions")} {
			if err := os.Chmod(dir, 0o755); err != nil {
				t.Fatalf("making %s readable by other: %v", dir, err)
			}
		}
		if err := os.Chmod(a.Fake.RecordPath(), 0o644); err != nil {
			t.Fatalf("making the session record readable by other: %v", err)
		}
	}

	// An MCP server for other that points at peer's configuration directory and
	// inbox socket must never register.
	foreign := startForeignMCPSession(t, "other", []string{
		"CLAUDE_CODE_MESSAGING_SOCKET=" + a.Fake.SocketPath(),
		"CLAUDE_CONFIG_DIR=" + a.Fake.ConfigDir(),
	})
	harness.Consistently(t, time.Second, "the foreign session's list_agents keeps failing", func() (bool, string) {
		text, isError := callForeignTool(t, foreign, "list_agents", map[string]any{})
		if !isError {
			return false, "list_agents succeeded: " + text
		}
		return true, ""
	})

	wantReason := "NOT_REGISTERED_REASON_SESSION_RECORD_NOT_FOUND"
	if readableRecord {
		wantReason = "NOT_REGISTERED_REASON_SOCKET_NOT_OWNED"
	}
	result := runRegisterClient(t, "other", a.Fake.SocketPath(), a.Fake.ConfigDir())
	if result.Event != "not_registered" {
		t.Fatalf("register-client reported %q, want not_registered", result.Event)
	}
	if result.Reason != wantReason {
		t.Fatalf("register-client reason = %q, want %q", result.Reason, wantReason)
	}

	harness.Consistently(t, time.Second, "the system Broker keeps one agent owned by peer", func() (bool, string) {
		st := syscontainer.RunAs(t, "root", nil, "service", "status")
		if st.ExitCode != 0 {
			return false, fmt.Sprintf("status exit %d: %s%s", st.ExitCode, st.Stdout, st.Stderr)
		}
		if !strings.Contains(st.Stdout, "Agents (1):") {
			return false, st.Stdout
		}
		line, ok := e2eStatusLine(st.Stdout, aSelf)
		if !ok {
			return false, "the peer agent is missing from:\n" + st.Stdout
		}
		if !strings.Contains(line, "peer") {
			return false, "the only agent is not peer's: " + line
		}
		return true, ""
	})

	harness.ConsistentlyPoll(t, time.Second, harness.ToolCallPollBound, "peer sees no agent from the other account", nil, func(deadline time.Time) harness.Observation {
		out, state := e2eSysListAgentsPoll(t, deadline, a, nil)
		if state != "" {
			return harness.Unobservable(state)
		}
		if len(out.Agents) != 0 {
			return harness.Observed(false, fmt.Sprintf("peer sees %d agents", len(out.Agents)))
		}
		return harness.Observed(true, "")
	})
}

// startForeignMCPSession starts "peers mcp" as account with env and
// connects an MCP client over its standard streams.
func startForeignMCPSession(t *testing.T, account string, env []string) *mcp.ClientSession {
	t.Helper()

	cmd := syscontainer.Command(account, env, peersBinaryPath, "mcp")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	client := mcp.NewClient(&mcp.Implementation{Name: "e2e-foreign", Version: "0"}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	session, err := client.Connect(ctx, &mcp.CommandTransport{Command: cmd}, nil)
	if err != nil {
		t.Fatalf("connecting to the MCP server as %s: %v\n%s", account, err, strings.TrimSpace(stderr.String()))
	}
	t.Cleanup(func() {
		_ = session.Close()
		if t.Failed() && stderr.Len() > 0 {
			t.Logf("the foreign MCP server's stderr:\n%s", strings.TrimSpace(stderr.String()))
		}
	})
	return session
}

// callForeignTool invokes a tool on a session started by
// startForeignMCPSession and returns its text and IsError flag.
func callForeignTool(t *testing.T, session *mcp.ClientSession, tool string, args map[string]any) (string, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil {
		t.Fatalf("calling %s on the foreign session: %v", tool, err)
	}
	var texts []string
	for _, content := range result.Content {
		if text, ok := content.(*mcp.TextContent); ok {
			texts = append(texts, text.Text)
		}
	}
	return strings.Join(texts, "\n"), result.IsError
}

// registerClientResult is the one-line answer of the register-client role.
type registerClientResult struct {
	Event  string `json:"event"`
	Reason string `json:"reason,omitempty"`
}

// runRegisterClient runs the test binary's register-client role as account. It
// opens the Register stream for the given inbox socket and configuration
// directory and returns the Broker's first event.
func runRegisterClient(t *testing.T, account, inboxSocket, configDir string) registerClientResult {
	t.Helper()

	exe := copyTestBinary(t)
	base := syscontainer.Command(account, []string{
		registerClientRoleEnv + "=" + registerClientRole,
		"PEERS_TEST_SOCKET=" + e2eSystemSocketPath,
		"E2E_SOCKET=" + inboxSocket,
		"E2E_CONFIG_DIR=" + configDir,
	}, exe)

	ctx, cancel := context.WithTimeout(context.Background(), registerClientBound+7*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, base.Path, base.Args[1:]...)
	cmd.Env = base.Env
	cmd.Dir = base.Dir
	cmd.SysProcAttr = base.SysProcAttr
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("running the register-client role as %s: %v\nstdout: %s\nstderr: %s",
			account, err, strings.TrimSpace(string(out)), strings.TrimSpace(stderr.String()))
	}
	line := strings.TrimSpace(strings.SplitN(string(out), "\n", 2)[0])
	var result registerClientResult
	if err := json.Unmarshal([]byte(line), &result); err != nil {
		t.Fatalf("decoding the register-client answer %q: %v", line, err)
	}
	return result
}

// copyTestBinary copies the running test binary to a world-readable path so
// that an unprivileged account can execute it.
func copyTestBinary(t *testing.T) string {
	t.Helper()

	in, err := os.Open(os.Args[0])
	if err != nil {
		t.Fatalf("opening the test binary %s: %v", os.Args[0], err)
	}
	defer func() { _ = in.Close() }()

	out, err := os.OpenFile(registerClientCopiedPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		t.Fatalf("creating %s: %v", registerClientCopiedPath, err)
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		t.Fatalf("copying the test binary: %v", err)
	}
	if err := out.Close(); err != nil {
		t.Fatalf("closing %s: %v", registerClientCopiedPath, err)
	}
	if err := os.Chmod(registerClientCopiedPath, 0o755); err != nil {
		t.Fatalf("chmod %s: %v", registerClientCopiedPath, err)
	}
	return registerClientCopiedPath
}

// runRegisterClientRole is the body of the test binary when TestMain selects
// the register-client role. It opens a Register stream, writes the first
// downstream event as one JSON line and exits.
func runRegisterClientRole() int {
	socket := os.Getenv("PEERS_TEST_SOCKET")
	inboxSocket := os.Getenv("E2E_SOCKET")
	configDir := os.Getenv("E2E_CONFIG_DIR")

	ctx, cancel := context.WithTimeout(context.Background(), registerClientBound)
	defer cancel()

	conn, err := grpc.NewClient("unix://"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return failRegisterClientRole("dial", err)
	}
	defer func() { _ = conn.Close() }()
	client := localpb.NewLocalClient(conn)

	if _, err := client.Hello(ctx, &localpb.HelloRequest{ProtocolVersion: localpb.ProtocolVersion}); err != nil {
		return failRegisterClientRole("hello", err)
	}
	stream, err := client.Register(ctx)
	if err != nil {
		return failRegisterClientRole("register", err)
	}
	if err := stream.Send(&localpb.RegisterRequest{
		InboxSocketPath: inboxSocket,
		ClaudeConfigDir: configDir,
	}); err != nil {
		return failRegisterClientRole("send", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded {
			writeRegisterClientResult(registerClientResult{Event: "timeout"})
			return 1
		}
		return failRegisterClientRole("recv", err)
	}

	switch event := resp.GetEvent().(type) {
	case *localpb.RegisterResponse_RegistrationComplete:
		writeRegisterClientResult(registerClientResult{Event: "complete"})
		return 0
	case *localpb.RegisterResponse_NotRegistered:
		writeRegisterClientResult(registerClientResult{
			Event:  "not_registered",
			Reason: event.NotRegistered.GetReason().String(),
		})
		return 0
	default:
		return failRegisterClientRole("event", fmt.Errorf("unexpected first event %T", event))
	}
}

// failRegisterClientRole writes an error answer and returns a failure code.
func failRegisterClientRole(stage string, err error) int {
	writeRegisterClientResult(registerClientResult{
		Event:  "error",
		Reason: stage + ": " + err.Error(),
	})
	return 1
}

// writeRegisterClientResult writes one JSON line to standard output.
func writeRegisterClientResult(result registerClientResult) {
	data, err := json.Marshal(result)
	if err != nil {
		fmt.Fprintf(os.Stderr, "register-client: %v\n", err)
		return
	}
	fmt.Println(string(data))
}

// e2eStatusLine returns the status table line that names address.
func e2eStatusLine(out, address string) (string, bool) {
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, address) {
			return line, true
		}
	}
	return "", false
}
