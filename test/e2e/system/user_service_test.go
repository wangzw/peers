//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/node"
	"github.com/wangzw/peers/test/e2e/syscontainer"
)

const (
	listBrokersRole = "list-brokers"
	// userServiceRecoveryBound is the acceptance bound for the Broker and its
	// link after the user manager restarts; the peer configuration redials within
	// two seconds.
	userServiceRecoveryBound = 3 * time.Second
	// noUnitQuietWindow is how long the positive control watches for a peer
	// Broker when no unit is installed; nothing can start one, so a second is
	// enough to see a wrongly enabled unit.
	noUnitQuietWindow = time.Second
)

type brokerListing struct {
	Brokers []brokerEntry `json:"brokers"`
}

type brokerEntry struct {
	Alias  string `json:"alias"`
	Status string `json:"status"`
}

// TestREQBrokerConfig015_UserServiceStartsAtLoginAndRestoresLink checks that an
// installed user service starts the Broker and restores its paired link across
// a user manager restart, without a session.
func TestREQBrokerConfig015_UserServiceStartsAtLoginAndRestoresLink(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	peerUID := syscontainer.AccountUID(t, "peer")
	uid := strconv.FormatUint(uint64(peerUID), 10)
	userEnv := []string{
		"XDG_RUNTIME_DIR=/run/user/" + uid,
		"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/" + uid + "/bus",
	}
	socket := "/tmp/peers-" + uid + "/broker.sock"

	writeAccountFile(t, "peer", "/home/peer/.peers/config.yaml",
		"broker:\n  listen: \":17445\"\nlink:\n  reconnect_min: 1s\n  reconnect_max: 2s\n")

	otherBroker := syscontainer.StartUserBroker(t, "other", "--listen", ":17444")
	t.Logf("the paired Broker runs as other with pid %d", otherBroker.PID)

	mustRun(t, "root", nil, "loginctl", "enable-linger", "peer")
	mustRun(t, "root", nil, "systemctl", "start", "user@"+uid+".service")
	waitUserManager(t, peerUID)
	mustRun(t, "root", nil, "systemctl", "restart", "user@"+uid+".service")
	harness.Consistently(t, noUnitQuietWindow, "no peer Broker starts without an installed unit", func() (bool, string) {
		if procs := peerBrokers(peerUID); len(procs) > 0 {
			return false, fmt.Sprintf("found peer Broker processes %v", procs)
		}
		return true, ""
	})

	install := runWithEnv(t, "peer", userEnv, "peers", "service", "install")
	if install.ExitCode != 0 {
		t.Fatalf("peers service install as peer: exit %d\nstdout: %s\nstderr: %s",
			install.ExitCode, strings.TrimSpace(install.Stdout), strings.TrimSpace(install.Stderr))
	}
	assertUserUnitEnabled(t, userEnv)
	assertUserUnitVerifies(t, userEnv)

	// Wait for the user manager's Broker before opening a session; a Broker a
	// session launches would survive the manager restart and hold the account
	// lock, so the test would miss the manager-started recovery path.
	syscontainer.WaitFor(t, "the peer Broker socket "+socket+" after install", func() bool {
		_, err := os.Stat(socket)
		return err == nil
	})
	beforePID := userServiceMainPID(t, userEnv)
	t.Logf("the user service was running after install with MainPID %d, without a new login", beforePID)
	if beforePID == 0 {
		t.Fatal("the user unit's MainPID is zero after the service started")
	}
	if got := solePeerBroker(t, peerUID); got != beforePID {
		t.Fatalf("MainPID = %d, but the peer Broker process is %d", beforePID, got)
	}

	otherSession := syscontainer.NewSession(t, "other", "b1")
	peerSession := syscontainer.NewSession(t, "peer", "a1")
	waitRegistered(t, otherSession)
	waitRegistered(t, peerSession)
	key := createPairingKey(t, otherSession)
	alias := connectBroker(t, peerSession, "127.0.0.1:17444", key)
	oracleExe := copyTestBinary(t)
	harness.Eventually(t, userServiceRecoveryBound, "the pair's link to "+alias+" before the restart", func() (bool, string) {
		statuses, err := listBrokerStatuses(t, oracleExe, socket)
		if err != nil {
			return false, err.Error()
		}
		return statuses[alias] == "BROKER_STATUS_CONNECTED", fmt.Sprintf("status of %s is %q", alias, statuses[alias])
	})

	closeSession(t, peerSession)
	closeSession(t, otherSession)
	syscontainer.WaitFor(t, "the sessions' MCP server processes to exit", func() bool {
		return len(syscontainer.Pids("peers", "mcp")) == 0
	})

	mustRun(t, "root", nil, "systemctl", "restart", "user@"+uid+".service")
	harness.Eventually(t, userServiceRecoveryBound, "the peer Broker and its link after the user manager restarts", func() (bool, string) {
		if procs := peerBrokers(peerUID); len(procs) == 0 {
			return false, "no peer Broker process is running"
		}
		statuses, err := listBrokerStatuses(t, oracleExe, socket)
		if err != nil {
			return false, err.Error()
		}
		return statuses[alias] == "BROKER_STATUS_CONNECTED", fmt.Sprintf("status of %s is %q", alias, statuses[alias])
	})
	afterPID := userServiceMainPID(t, userEnv)
	if afterPID == 0 {
		t.Fatal("the user unit's MainPID is zero after the user manager restarted")
	}
	if afterPID == beforePID {
		t.Fatalf("MainPID = %d after the restart, the same as before it", afterPID)
	}
	if got := solePeerBroker(t, peerUID); got != afterPID {
		t.Fatalf("MainPID = %d after the restart, but the peer Broker process is %d", afterPID, got)
	}
}

func assertUserUnitEnabled(t *testing.T, userEnv []string) {
	t.Helper()
	res := runWithEnv(t, "peer", userEnv, "systemctl", "--user", "is-enabled", "peers.service")
	if state := strings.TrimSpace(res.Stdout); res.ExitCode != 0 || state != "enabled" {
		t.Errorf("systemctl --user is-enabled peers.service = %q (exit %d), want %q\nstderr: %s",
			state, res.ExitCode, "enabled", strings.TrimSpace(res.Stderr))
	}
}

func assertUserUnitVerifies(t *testing.T, userEnv []string) {
	t.Helper()
	unitPath := userUnitPath(t)
	res := runWithEnv(t, "peer", userEnv, "systemd-analyze", "--user", "verify", unitPath)
	if res.ExitCode != 0 || res.Stdout != "" || res.Stderr != "" {
		t.Errorf("systemd-analyze --user verify %s: exit %d\nstdout: %s\nstderr: %s",
			unitPath, res.ExitCode, res.Stdout, res.Stderr)
	}
}

func userUnitPath(t *testing.T) string {
	t.Helper()
	u, err := user.Lookup("peer")
	if err != nil {
		t.Fatalf("looking up account peer: %v", err)
	}
	return filepath.Join(u.HomeDir, ".config", "systemd", "user", "peers.service")
}

func userServiceMainPID(t *testing.T, userEnv []string) int {
	t.Helper()
	res := runWithEnv(t, "peer", userEnv, "systemctl", "--user", "show", "--property", "MainPID", "--value", "peers.service")
	if res.ExitCode != 0 {
		t.Fatalf("systemctl --user show --property MainPID: exit %d\nstderr: %s", res.ExitCode, strings.TrimSpace(res.Stderr))
	}
	pid, err := strconv.Atoi(strings.TrimSpace(res.Stdout))
	if err != nil {
		t.Fatalf("MainPID %q is not a number: %v", strings.TrimSpace(res.Stdout), err)
	}
	return pid
}

func solePeerBroker(t *testing.T, peerUID uint32) int {
	t.Helper()
	procs := peerBrokers(peerUID)
	if len(procs) != 1 {
		t.Fatalf("peer Broker processes = %v, want exactly one", procs)
	}
	return procs[0].PID
}

func peerBrokers(peerUID uint32) []syscontainer.ProcInfo {
	var peer []syscontainer.ProcInfo
	for _, proc := range syscontainer.Pids("peers", "service", "run") {
		if proc.UID == peerUID {
			peer = append(peer, proc)
		}
	}
	return peer
}

// userManagerBound bounds the setup wait for the user manager's runtime
// directory and bus; it follows systemd's own startup allowance, not the
// three-second acceptance bound on the Broker and its link.
const userManagerBound = 10 * time.Second

func waitUserManager(t *testing.T, uid uint32) {
	t.Helper()

	runtimeDir := "/run/user/" + strconv.FormatUint(uint64(uid), 10)
	harness.Eventually(t, userManagerBound, "the user manager runtime directory "+runtimeDir, func() (bool, string) {
		if _, err := os.Stat(runtimeDir); err != nil {
			return false, err.Error()
		}
		return true, ""
	})
	harness.Eventually(t, userManagerBound, "the user manager bus "+runtimeDir+"/bus", func() (bool, string) {
		if _, err := os.Stat(runtimeDir + "/bus"); err != nil {
			return false, err.Error()
		}
		return true, ""
	})
}

func writeAccountFile(t *testing.T, account, path, content string) {
	t.Helper()

	u, err := user.Lookup(account)
	if err != nil {
		t.Fatalf("looking up account %q: %v", account, err)
	}
	uid, err := strconv.ParseUint(u.Uid, 10, 32)
	if err != nil {
		t.Fatalf("account %q has a non-numeric uid %q", account, u.Uid)
	}
	gid, err := strconv.ParseUint(u.Gid, 10, 32)
	if err != nil {
		t.Fatalf("account %q has a non-numeric gid %q", account, u.Gid)
	}

	dir := path[:strings.LastIndex(path, "/")]
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("creating %s: %v", dir, err)
	}
	if err := os.Chown(dir, int(uid), int(gid)); err != nil {
		t.Fatalf("chown %s: %v", dir, err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatalf("chmod %s: %v", dir, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	if err := os.Chown(path, int(uid), int(gid)); err != nil {
		t.Fatalf("chown %s: %v", path, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatalf("chmod %s: %v", path, err)
	}
}

func mustRun(t *testing.T, account string, env []string, name string, args ...string) syscontainer.Result {
	t.Helper()

	res := runWithEnv(t, account, env, name, args...)
	if res.ExitCode != 0 {
		t.Fatalf("%s %s as %s: exit %d\nstdout: %s\nstderr: %s",
			name, strings.Join(args, " "), account, res.ExitCode,
			strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
	}
	return res
}

func runWithEnv(t *testing.T, account string, env []string, name string, args ...string) syscontainer.Result {
	t.Helper()

	cmd := syscontainer.Command(account, env, name, args...)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	res := syscontainer.Result{Stdout: stdout.String(), Stderr: stderr.String()}
	if err == nil {
		return res
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("running %s as %s: %v", name, account, err)
	}
	res.ExitCode = exitErr.ExitCode()
	return res
}

func createPairingKey(t *testing.T, session *syscontainer.Session) string {
	t.Helper()

	structured, isError, text := callStructured(t, session, "create_pairing_key", nil)
	if isError {
		t.Fatalf("create_pairing_key as %s reported an error: %s", session.Account, text)
	}
	out, err := harness.Decode[mcpserver.CreatePairingKeyOut](string(structured))
	if err != nil {
		t.Fatalf("decoding create_pairing_key: %v", err)
	}
	return out.Key
}

func connectBroker(t *testing.T, session *syscontainer.Session, address, key string) string {
	t.Helper()

	structured, isError, text := callStructured(t, session, "connect_broker", map[string]any{
		"address": address,
		"key":     key,
	})
	if isError {
		t.Fatalf("connect_broker as %s reported an error: %s", session.Account, text)
	}
	out, err := harness.Decode[mcpserver.ConnectBrokerOut](string(structured))
	if err != nil {
		t.Fatalf("decoding connect_broker: %v", err)
	}
	return out.Alias
}

func callStructured(t *testing.T, session *syscontainer.Session, tool string, args map[string]any) (json.RawMessage, bool, string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	result, err := session.MCP.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil {
		t.Fatalf("calling %s as %s: %v", tool, session.Account, err)
	}
	var texts []string
	for _, content := range result.Content {
		if text, ok := content.(*mcp.TextContent); ok {
			texts = append(texts, text.Text)
		}
	}
	text := strings.Join(texts, "\n")
	if result.StructuredContent == nil {
		return nil, result.IsError, text
	}
	structured, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatalf("encoding the structured content of %s: %v", tool, err)
	}
	return structured, result.IsError, text
}

// closeSession closes the MCP client session and the simulated session, which
// releases the inbox socket so a later session with the same account and name
// can bind that path again.
func closeSession(t *testing.T, session *syscontainer.Session) {
	t.Helper()

	if err := session.MCP.Close(); err != nil {
		t.Fatalf("closing the MCP session as %s: %v", session.Account, err)
	}
	session.Fake.Close()
}

func listBrokerStatuses(t *testing.T, exe, socket string) (map[string]string, error) {
	t.Helper()

	base := syscontainer.Command("peer", []string{
		registerClientRoleEnv + "=" + listBrokersRole,
		"PEERS_TEST_SOCKET=" + socket,
	}, exe)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, base.Path, base.Args[1:]...)
	cmd.Env = base.Env
	cmd.Dir = base.Dir
	cmd.SysProcAttr = base.SysProcAttr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("running the list-brokers role as peer: %w", err)
	}
	var listing brokerListing
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(out))), &listing); err != nil {
		return nil, fmt.Errorf("decoding the list-brokers answer %q: %w", strings.TrimSpace(string(out)), err)
	}
	statuses := make(map[string]string, len(listing.Brokers))
	for _, broker := range listing.Brokers {
		statuses[broker.Alias] = broker.Status
	}
	return statuses, nil
}

func runListBrokersRole() int {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	oracle, err := node.DialOracle(os.Getenv("PEERS_TEST_SOCKET"))
	if err != nil {
		return failListBrokersRole(err)
	}
	defer func() { _ = oracle.Close() }()

	resp, err := oracle.ListBrokers(ctx)
	if err != nil {
		return failListBrokersRole(err)
	}
	listing := brokerListing{Brokers: make([]brokerEntry, 0, len(resp.GetBrokers()))}
	for _, broker := range resp.GetBrokers() {
		listing.Brokers = append(listing.Brokers, brokerEntry{
			Alias:  broker.GetAlias(),
			Status: broker.GetStatus().String(),
		})
	}
	data, err := json.Marshal(listing)
	if err != nil {
		return failListBrokersRole(err)
	}
	fmt.Println(string(data))
	return 0
}

func failListBrokersRole(err error) int {
	fmt.Fprintf(os.Stderr, "list-brokers: %v\n", err)
	return 1
}
