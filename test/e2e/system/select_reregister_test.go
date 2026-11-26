//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/internal/procsig"
	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/node"
	"github.com/wangzw/peers/test/e2e/syscontainer"
)

const (
	srSystemSocket  = "/run/peers/broker.sock"
	srSystemService = "peers.service"
	// srLaunchBudget is the reconnect deadline after a selection round that probed a
	// stale system socket; waits add harness.EventTimeout as the fallback.
	srLaunchBudget = mcpserver.SystemRetryDeadline + mcpserver.ConnBackoffCap + 5*time.Second
	// srRestartBudget is systemd's RestartSec=5 plus a small margin; a wait that
	// uses it adds harness.EventTimeout, and its list_agents waits also cover
	// ListAgentsWaitBound because one stalled poll can run for
	// ListAgentsPollBound.
	srRestartBudget  = 5*time.Second + 3*time.Second
	statusClientRole = "status-client"
	// srStatusBound bounds one status-client run, matching the Broker call bound.
	srStatusBound = 10 * time.Second
)

func srInit(t *testing.T) bool {
	t.Helper()
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Systemd)
		return false
	}
	return true
}

func srSetup(t *testing.T) *syscontainer.Proc {
	t.Helper()
	syscontainer.StartSystemService(t, "")
	srWriteAccountConfig(t, "peer", "broker:\n  listen: \":17444\"\n")
	return syscontainer.StartUserBroker(t, "peer", "--listen", ":17444")
}

func srWriteAccountConfig(t *testing.T, account, content string) {
	t.Helper()
	u, err := user.Lookup(account)
	if err != nil {
		t.Fatalf("looking up account %q: %v", account, err)
	}
	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		t.Fatalf("account %q has a non-numeric uid %q", account, u.Uid)
	}
	gid, err := strconv.Atoi(u.Gid)
	if err != nil {
		t.Fatalf("account %q has a non-numeric gid %q", account, u.Gid)
	}
	dir := filepath.Join(u.HomeDir, ".peers")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("creating %s: %v", dir, err)
	}
	if err := os.Chown(dir, uid, gid); err != nil {
		t.Fatalf("chown %s: %v", dir, err)
	}
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	if err := os.Chown(path, uid, gid); err != nil {
		t.Fatalf("chown %s: %v", path, err)
	}
}

func srWaitRegistered(t *testing.T, s *syscontainer.Session, what string) {
	t.Helper()
	harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "session "+what+" to register", func(deadline time.Time) (bool, string) {
		_, isError, err := s.PollCall(t, deadline, "list_agents", map[string]any{})
		if err != nil {
			return false, err.Error()
		}
		return !isError, ""
	})
}

func srMustStatus(t *testing.T, account string) string {
	t.Helper()
	res := syscontainer.RunAs(t, account, nil, "service", "status")
	if res.ExitCode != 0 {
		t.Fatalf("status as %s exited %d: %s", account, res.ExitCode, strings.TrimSpace(res.Stderr))
	}
	return res.Stdout
}

func srAgentAddress(status, name string) string {
	for _, line := range strings.Split(status, "\n") {
		fields := strings.Fields(line)
		if len(fields) > 0 && strings.Contains(fields[0], name) {
			return fields[0]
		}
	}
	return ""
}

func srIdentityHash(t *testing.T, account string) string {
	t.Helper()
	u, err := user.Lookup(account)
	if err != nil {
		t.Fatalf("looking up account %q: %v", account, err)
	}
	path := filepath.Join(u.HomeDir, ".peers", "identity.key")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// srAgentID reads the ID through the status-client role as account, because a
// user Broker admits only its own account's peer credentials.
func srAgentID(t *testing.T, account, address string) string {
	t.Helper()
	base := syscontainer.Command(account, []string{
		registerClientRoleEnv + "=" + statusClientRole,
		"PEERS_TEST_SOCKET=" + syscontainer.UserSocketPath(t, account),
		"E2E_ADDRESS=" + address,
	}, copyTestBinary(t))

	ctx, cancel := context.WithTimeout(context.Background(), srStatusBound)
	defer cancel()
	cmd := exec.CommandContext(ctx, base.Path, base.Args[1:]...)
	cmd.Env = base.Env
	cmd.Dir = base.Dir
	cmd.SysProcAttr = base.SysProcAttr

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("reading the agent ID of %q as %s: %v\nstdout: %s\nstderr: %s",
			address, account, err, strings.TrimSpace(string(out)), strings.TrimSpace(stderr.String()))
	}
	id := strings.TrimSpace(string(out))
	if id == "" {
		t.Fatalf("the status-client role printed no agent ID for %q", address)
	}
	return id
}

func runStatusClientRole() int {
	oracle, err := node.DialOracle(os.Getenv("PEERS_TEST_SOCKET"))
	if err != nil {
		return failStatusClientRole(err)
	}
	defer func() { _ = oracle.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), srStatusBound)
	defer cancel()
	resp, err := oracle.Status(ctx)
	if err != nil {
		return failStatusClientRole(err)
	}
	address := os.Getenv("E2E_ADDRESS")
	for _, entry := range resp.GetAgents() {
		if entry.GetAddress() == address {
			fmt.Println(entry.GetAgentId())
			return 0
		}
	}
	return failStatusClientRole(fmt.Errorf("the Broker reports no agent at %q: %v", address, resp.GetAgents()))
}

func failStatusClientRole(err error) int {
	fmt.Fprintln(os.Stderr, "status-client:", err)
	return 1
}

func srBrokerProcs(t *testing.T, account string) []syscontainer.ProcInfo {
	t.Helper()
	uid := syscontainer.AccountUID(t, account)
	var out []syscontainer.ProcInfo
	for _, proc := range syscontainer.Pids("peers", "service", "run") {
		if proc.UID == uid {
			out = append(out, proc)
		}
	}
	return out
}

type srProc struct {
	PID  int
	PPID int
	UID  uint32
	Args []string
}

func srMCPPIDs(t *testing.T, account string) []int {
	t.Helper()
	uid := syscontainer.AccountUID(t, account)
	var procs []srProc
	for _, proc := range syscontainer.Pids("peers", "mcp") {
		procs = append(procs, srProc{PID: proc.PID, PPID: proc.PPID, UID: proc.UID, Args: proc.Args})
	}
	return srSelectMCPPIDs(procs, uid, os.Getpid(), syscontainer.PeersBinary())
}

// srSelectMCPPIDs keeps a process only when launcher is its parent, its uid matches,
// and its argv is exactly exe and "mcp"; a forked-but-not-yet-exec'd child of the
// server inherits the server's argv and has the server as its parent, so it is excluded.
func srSelectMCPPIDs(procs []srProc, uid uint32, launcher int, exe string) []int {
	var out []int
	for _, p := range procs {
		if p.PPID != launcher || p.UID != uid {
			continue
		}
		if len(p.Args) != 2 || p.Args[0] != exe || p.Args[1] != "mcp" {
			continue
		}
		out = append(out, p.PID)
	}
	sort.Ints(out)
	return out
}

// TestSrSelectMCPPIDs checks that the MCP server filter accepts only the
// launcher's exact peers mcp process and rejects a child of the server, another
// account, another command or an inexact argv.
func TestSrSelectMCPPIDs(t *testing.T) {
	const (
		exe      = "/usr/local/bin/peers"
		launcher = 1000
		uid      = 2000
	)
	procs := []srProc{
		{PID: 101, PPID: launcher, UID: uid, Args: []string{exe, "mcp"}},
		{PID: 102, PPID: 101, UID: uid, Args: []string{exe, "mcp"}},
		{PID: 103, PPID: launcher, UID: uid + 1, Args: []string{exe, "mcp"}},
		{PID: 104, PPID: launcher, UID: uid, Args: []string{exe, "service", "run"}},
		{PID: 105, PPID: launcher, UID: uid, Args: []string{"/tmp/peers", "mcp"}},
		{PID: 106, PPID: launcher, UID: uid, Args: []string{exe, "mcp", "--verbose"}},
		{PID: 107, PPID: launcher, UID: uid, Args: []string{"sh", "-c", "peers mcp"}},
		{PID: 108, PPID: launcher + 1, UID: uid, Args: []string{exe, "mcp"}},
	}
	if got, want := srSelectMCPPIDs(procs, uid, launcher, exe), []int{101}; !slices.Equal(got, want) {
		t.Fatalf("srSelectMCPPIDs = %v, want %v", got, want)
	}
}

// TestSrSelectMCPPIDsReconnect checks that a server that reconnected under a
// new pid is still selected.
func TestSrSelectMCPPIDsReconnect(t *testing.T) {
	const (
		exe      = "/usr/local/bin/peers"
		launcher = 1000
		uid      = 2000
	)
	before := []srProc{{PID: 101, PPID: launcher, UID: uid, Args: []string{exe, "mcp"}}}
	after := []srProc{
		{PID: 109, PPID: launcher, UID: uid, Args: []string{exe, "mcp"}},
		{PID: 110, PPID: 109, UID: uid, Args: []string{exe, "mcp"}},
	}
	if got, want := srSelectMCPPIDs(before, uid, launcher, exe), []int{101}; !slices.Equal(got, want) {
		t.Fatalf("before = %v, want %v", got, want)
	}
	if got, want := srSelectMCPPIDs(after, uid, launcher, exe), []int{109}; !slices.Equal(got, want) {
		t.Fatalf("after = %v, want %v", got, want)
	}
}

func srMainPID() (int, error) {
	out, err := syscontainer.Command("root", nil, "systemctl", "show", "-p", "MainPID", srSystemService).Output()
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(out), "\n") {
		value, ok := strings.CutPrefix(strings.TrimSpace(line), "MainPID=")
		if !ok {
			continue
		}
		return strconv.Atoi(value)
	}
	return 0, fmt.Errorf("systemctl show printed no MainPID")
}

// TestSelectOrderReq026 checks that a session uses its account's user Broker
// ahead of the system Broker and falls back to the system Broker when none
// runs, and that AGENT_PEER_SOCKET does not change the choice.
func TestSelectOrderReq026(t *testing.T) {
	if !srInit(t) {
		return
	}
	userBroker := srSetup(t)

	s1 := syscontainer.NewSession(t, "peer", "s1")
	srWaitRegistered(t, s1, "s1")
	if strings.Contains(srMustStatus(t, "root"), "s1") {
		t.Fatal("s1 registered on the system Broker although peer's user Broker was running")
	}
	if !strings.Contains(srMustStatus(t, "peer"), "s1") {
		t.Fatal("s1 is not on peer's user Broker")
	}

	s2 := syscontainer.NewSession(t, "peer", "s2",
		syscontainer.WithEnv("AGENT_PEER_SOCKET="+srSystemSocket))
	srWaitRegistered(t, s2, "s2")
	if strings.Contains(srMustStatus(t, "root"), "s2") {
		t.Fatal("s2 registered on the system Broker because AGENT_PEER_SOCKET named it")
	}
	if !strings.Contains(srMustStatus(t, "peer"), "s2") {
		t.Fatal("s2 is not on peer's user Broker")
	}

	userBroker.Stop(t)
	s3 := syscontainer.NewSession(t, "peer", "s3")
	srWaitRegistered(t, s3, "s3")
	if !strings.Contains(srMustStatus(t, "root"), "s3") {
		t.Fatal("s3 is not on the system Broker after peer's user Broker stopped")
	}
	if procs := srBrokerProcs(t, "peer"); len(procs) != 0 {
		t.Fatalf("peer runs a user Broker although it was stopped: %v", procs)
	}

	s4 := syscontainer.NewSession(t, "other", "s4")
	srWaitRegistered(t, s4, "s4")
	if !strings.Contains(srMustStatus(t, "root"), "s4") {
		t.Fatal("s4 is not on the system Broker")
	}
	if procs := srBrokerProcs(t, "other"); len(procs) != 0 {
		t.Fatalf("other runs a user Broker although the system Broker was available: %v", procs)
	}
}

// TestReregisterUserBrokerReq037 checks that after its user Broker is killed a
// session reports no Broker and rejoins with the same identity once the Broker
// restarts, without moving to the system Broker.
func TestReregisterUserBrokerReq037(t *testing.T) {
	if !srInit(t) {
		return
	}
	userBroker := srSetup(t)

	s1 := syscontainer.NewSession(t, "peer", "s1")
	srWaitRegistered(t, s1, "s1")
	mcpBefore := srMCPPIDs(t, "peer")
	if len(mcpBefore) == 0 {
		t.Fatal("peer has no MCP server process after s1 started")
	}
	status := srMustStatus(t, "peer")
	wantAddress := srAgentAddress(status, "s1")
	if wantAddress == "" {
		t.Fatal("peer's status does not list s1")
	}
	wantAgentID := srAgentID(t, "peer", wantAddress)
	wantKey := srIdentityHash(t, "peer")

	if err := procsig.SendSignal(userBroker.PID, syscall.SIGKILL); err != nil {
		t.Fatalf("killing peer's user Broker %d: %v", userBroker.PID, err)
	}

	harness.EventuallyPoll(t, srLaunchBudget+harness.EventTimeout, "s1 reports that no Broker is available", func(deadline time.Time) (bool, string) {
		text, isError, err := s1.PollCall(t, deadline, "list_agents", map[string]any{})
		if err != nil {
			return false, err.Error()
		}
		if !isError {
			return false, "list_agents succeeded without a Broker"
		}
		for _, want := range []string{"no Broker is available", "peers service install", "peers service run"} {
			if !strings.Contains(text, want) {
				return false, text
			}
		}
		for _, proc := range srBrokerProcs(t, "peer") {
			if proc.PID != userBroker.PID {
				return false, fmt.Sprintf("a new user Broker process %d appeared", proc.PID)
			}
		}
		return true, ""
	})

	syscontainer.StartUserBroker(t, "peer", "--listen", ":17444")
	harness.EventuallyPoll(t, srLaunchBudget+harness.EventTimeout, "s1 registers again with the restarted user Broker", func(deadline time.Time) (bool, string) {
		_, isError, err := s1.PollCall(t, deadline, "list_agents", map[string]any{})
		if err != nil {
			return false, err.Error()
		}
		if isError {
			return false, "list_agents still fails"
		}
		return true, ""
	})
	if got := srMCPPIDs(t, "peer"); !slices.Equal(got, mcpBefore) {
		t.Fatalf("the MCP server processes changed from %v to %v; the session reconnected instead of recovering", mcpBefore, got)
	}

	if got := srIdentityHash(t, "peer"); got != wantKey {
		t.Fatalf("identity.key changed across the Broker restart: sha256 %s, want %s", got, wantKey)
	}
	status = srMustStatus(t, "peer")
	if got := srAgentAddress(status, "s1"); got != wantAddress {
		t.Fatalf("s1's Agent address after the restart is %q, want %q", got, wantAddress)
	}
	if got := srAgentID(t, "peer", wantAddress); got != wantAgentID {
		t.Fatalf("s1's Agent ID after the restart is %q, want %q", got, wantAgentID)
	}
	if strings.Contains(srMustStatus(t, "root"), "s1") {
		t.Fatal("s1 moved to the system Broker after its user Broker was killed")
	}
}

// TestReregisterSystemBrokerReq037 checks that systemd restarts the system
// Broker, the session rejoins it, and no user Broker starts while the system
// Broker is down.
func TestReregisterSystemBrokerReq037(t *testing.T) {
	if !srInit(t) {
		return
	}
	syscontainer.StartSystemService(t, "")

	s3 := syscontainer.NewSession(t, "other", "s3")
	srWaitRegistered(t, s3, "s3")
	if !strings.Contains(srMustStatus(t, "root"), "s3") {
		t.Fatal("s3 is not on the system Broker")
	}
	if procs := srBrokerProcs(t, "other"); len(procs) != 0 {
		t.Fatalf("other runs a user Broker although the system Broker was available: %v", procs)
	}

	oldPID, err := srMainPID()
	if err != nil {
		t.Fatalf("reading the system Broker's MainPID: %v", err)
	}
	if oldPID == 0 {
		t.Fatal("systemd reports MainPID 0 before the kill")
	}
	// Kill MainPID directly, not through systemctl kill: systemd 252 returns EIO when
	// cgroup.procs holds an unmapped pid, and the helpers carry Pdeathsig=SIGKILL
	// so they need no signal.
	if !procsig.Alive(oldPID) {
		t.Fatalf("the system Broker %d is not running before the kill", oldPID)
	}
	if err := procsig.SendSignal(oldPID, syscall.SIGKILL); err != nil {
		t.Fatalf("killing the system Broker %d: %v", oldPID, err)
	}

	var newPID int
	harness.EventuallyPoll(t, max(srRestartBudget+harness.EventTimeout, harness.ListAgentsWaitBound), "systemd restarts the system Broker and s3 registers again", func(deadline time.Time) (bool, string) {
		pid, err := srMainPID()
		if err != nil {
			return false, err.Error()
		}
		if pid == 0 || pid == oldPID {
			return false, fmt.Sprintf("MainPID is %d, want a pid other than %d", pid, oldPID)
		}
		_, isError, callErr := s3.PollCall(t, deadline, "list_agents", map[string]any{})
		if callErr != nil {
			return false, callErr.Error()
		}
		if isError {
			return false, "list_agents still fails"
		}
		newPID = pid
		return true, ""
	})
	if newPID == oldPID {
		t.Fatal("the system Broker was not restarted")
	}
	if !strings.Contains(srMustStatus(t, "root"), "s3") {
		t.Fatal("s3 is not registered on the restarted system Broker")
	}
	if procs := srBrokerProcs(t, "other"); len(procs) != 0 {
		t.Fatalf("other started a user Broker while the system Broker was down: %v", procs)
	}
}
