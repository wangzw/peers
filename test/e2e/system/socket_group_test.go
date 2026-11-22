//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"slices"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/syscontainer"
)

const (
	// sgBinary is the peers build the system image provides.
	sgBinary = "/usr/local/bin/peers"
	// sgConfigPath is the system Broker's configuration file.
	sgConfigPath = "/etc/peers/config.yaml"
	// sgSocketPath is the system Broker's local socket.
	sgSocketPath = "/run/peers/broker.sock"
	// sgUsersGID is the gid of peers-users in the system image.
	sgUsersGID = 2001
	// sgOtherUID is the uid of other in the system image.
	sgOtherUID = 2000
	// sgNoBrokerBudget is the fallback for the first failed selection and tool
	// call when no Broker may serve the account; no retry timer is involved.
	sgNoBrokerBudget = harness.EventTimeout
	// sgRegisterBudget bounds the reconnect after the user starts a Broker:
	// mcpserver.ConnBackoffCap is the longest wait, and the margin covers the
	// dial, Hello and registration.
	sgRegisterBudget = mcpserver.ConnBackoffCap + 5*time.Second
)

// TestREQBrokerConfig027_GroupRestrictsUse checks that the group and mode of
// the system Broker's local socket decide which accounts may use it. peer is
// in peers-users and other is not, so other's session reports that no
// Broker is available until the user starts other's own user Broker; it
// never registers with the system Broker.
func TestREQBrokerConfig027_GroupRestrictsUse(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Plain)
		return
	}

	syscontainer.StartSystemBroker(t,
		"broker:\n  local_socket_group: peers-users\n  local_socket_mode: \"0660\"\n")
	syscontainer.WaitFor(t, "the system socket to be 0660 peers-users", func() bool {
		mode, gid := sgSocketModeGID(t, sgSocketPath)
		return mode == 0o660 && gid == sgUsersGID
	})

	// other may not use the system socket and starts no Broker of its own, so
	// its session first reports that no Broker is available. The user then
	// starts other's user Broker on the port its configuration names.
	sgWriteOtherConfig(t)

	a := syscontainer.NewSession(t, "peer", "alpha")
	sgWaitRegistered(t, a)

	c := syscontainer.NewSession(t, "other", "charlie")
	mcpBefore := sgMCPPIDs(t, "other")
	if len(mcpBefore) == 0 {
		t.Fatal("other has no MCP server process after charlie started")
	}
	harness.EventuallyPoll(t, sgNoBrokerBudget, "other's session reports that no Broker is available", func(deadline time.Time) (bool, string) {
		text, isError, err := c.PollCall(t, deadline, "list_agents", map[string]any{})
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
		return true, ""
	})
	syscontainer.StartUserBroker(t, "other")
	harness.EventuallyPoll(t, sgRegisterBudget, "charlie registers on the Broker the user started", func(deadline time.Time) (bool, string) {
		_, isError, err := c.PollCall(t, deadline, "list_agents", map[string]any{})
		if err != nil {
			return false, err.Error()
		}
		if isError {
			return false, "list_agents still fails"
		}
		return true, ""
	})
	if got := sgMCPPIDs(t, "other"); !slices.Equal(got, mcpBefore) {
		t.Fatalf("the MCP server processes changed from %v to %v; the session reconnected instead of recovering", mcpBefore, got)
	}

	syscontainer.WaitFor(t, "the system Broker to list peer's agent", func() bool {
		res := syscontainer.RunAs(t, "root", nil, "service", "status")
		return res.ExitCode == 0 && strings.Contains(res.Stdout, "alpha@")
	})

	// other registered with its own user Broker, so the system Broker
	// must never list its agent. Registration has just completed, so one
	// second is ample for a leaked registration to appear.
	harness.Consistently(t, time.Second, "the system Broker never lists other's agent", func() (bool, string) {
		res := syscontainer.RunAs(t, "root", nil, "service", "status")
		if res.ExitCode != 0 {
			return false, "status failed: " + res.Stderr
		}
		return !strings.Contains(res.Stdout, "charlie"), res.Stdout
	})

	syscontainer.WaitFor(t, "other's user Broker process", func() bool {
		return sgHasUserBroker(sgOtherUID)
	})
	for _, proc := range syscontainer.Pids("peers", "service", "run", "--system") {
		if proc.UID == sgOtherUID {
			t.Errorf("other runs a Broker with --system (pid %d)", proc.PID)
		}
	}

	sgAssertNotPermitted(t, "other", sgSocketPath)

	out, err := syscontainer.Command("root", nil, "ss", "-ltn").Output()
	if err != nil {
		t.Fatalf("syscontainer: ss -ltn: %v", err)
	}
	if !strings.Contains(string(out), ":17444") {
		t.Errorf("ss -ltn does not show other's Broker listening on 17444:\n%s", out)
	}
}

// TestSystemSocketGroupReappliedOnReload checks that changing the socket's
// group and mode in the configuration file takes effect on the socket that is
// already bound, without a restart.
func TestSystemSocketGroupReappliedOnReload(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Plain)
		return
	}

	syscontainer.StartSystemBroker(t, "")
	syscontainer.WaitFor(t, "the default socket to be 0666 root", func() bool {
		mode, gid := sgSocketModeGID(t, sgSocketPath)
		return mode == 0o666 && gid == 0
	})

	sgWriteSystemConfig(t,
		"broker:\n  local_socket_group: peers-users\n  local_socket_mode: \"0660\"\n")

	syscontainer.WaitFor(t, "the reloaded socket to be 0660 peers-users", func() bool {
		mode, gid := sgSocketModeGID(t, sgSocketPath)
		return mode == 0o660 && gid == sgUsersGID
	})

	sgAssertNotPermitted(t, "other", sgSocketPath)
	if res := syscontainer.RunAs(t, "peer",
		[]string{"PEERS_TEST_SOCKET=" + sgSocketPath}, "service", "status"); res.ExitCode != 0 {
		t.Errorf("status as peer through the reloaded system socket: exit %d\n%s", res.ExitCode, res.Stderr)
	}
}

// TestSystemSocketGroupUnknownRejected checks that an unknown group is refused
// both when it is set at run time and when it is used at startup.
func TestSystemSocketGroupUnknownRejected(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Plain)
		return
	}

	// The startup failure runs first, because the account lock admits only one
	// Broker at a time.
	sgWriteSystemConfig(t, "broker:\n  local_socket_group: no-such-group\n")
	res := sgRun(t, 5*time.Second,
		syscontainer.Command("root", nil, sgBinary, "service", "run", "--system"))
	if res.ExitCode == 0 {
		t.Fatalf("the system Broker started with an unknown broker.local_socket_group; stdout:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stderr, "broker.local_socket_group") {
		t.Errorf("the startup error does not name broker.local_socket_group:\n%s", res.Stderr)
	}

	const valid = "broker:\n  local_socket_group: peers-users\n  local_socket_mode: \"0660\"\n"
	syscontainer.StartSystemBroker(t, valid)
	syscontainer.WaitFor(t, "the system socket to be 0660 peers-users", func() bool {
		mode, gid := sgSocketModeGID(t, sgSocketPath)
		return mode == 0o660 && gid == sgUsersGID
	})

	root := syscontainer.NewSession(t, "root", "admin")
	sgWaitConnected(t, root)

	text, isError := root.Call(t, "set_config",
		map[string]any{"key": "broker.local_socket_group", "value": "no-such-group"})
	if !isError {
		t.Fatalf("set_config accepted an unknown group: %s", text)
	}
	if !strings.Contains(text, "(code: invalid_argument)") {
		t.Errorf("set_config error = %q, want it to carry (code: invalid_argument)", text)
	}

	text, isError = root.Call(t, "get_config",
		map[string]any{"key": "broker.local_socket_group"})
	if isError {
		t.Fatalf("get_config after the refused set: %s", text)
	}
	var got struct {
		Entries []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"entries"`
	}
	if err := json.Unmarshal([]byte(text), &got); err != nil {
		t.Fatalf("decoding get_config result %q: %v", text, err)
	}
	if len(got.Entries) != 1 || got.Entries[0].Value != "peers-users" {
		t.Errorf("broker.local_socket_group after the refused set = %q, want peers-users", text)
	}

	// The refused change must leave the socket's group and mode untouched. The
	// refused set is already complete, so a one-second observation is enough.
	harness.Consistently(t, time.Second, "the socket group stays peers-users", func() (bool, string) {
		mode, gid := sgSocketModeGID(t, sgSocketPath)
		return mode == 0o660 && gid == sgUsersGID,
			"mode " + mode.String() + " gid " + strconv.FormatUint(uint64(gid), 10)
	})
}

// sgAssertNotPermitted checks that a status call through the system socket as
// account fails and that the error names the missing permission. A failure
// alone would not prove the group check produced it rather than some other
// problem.
func sgAssertNotPermitted(t *testing.T, account, path string) {
	t.Helper()
	res := syscontainer.RunAs(t, account, []string{"PEERS_TEST_SOCKET=" + path}, "service", "status")
	if res.ExitCode == 0 {
		t.Errorf("status as %s through the system socket succeeded; stdout:\n%s", account, res.Stdout)
		return
	}
	if !strings.Contains(res.Stderr, "may not connect") {
		t.Errorf("status as %s through the system socket does not explain the refusal:\n%s", account, res.Stderr)
	}
}

// sgSocketModeGID returns the permission bits and group of path.
func sgSocketModeGID(t *testing.T, path string) (os.FileMode, uint32) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("syscontainer: stat %s: %v", path, err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("syscontainer: %s has no owner information", path)
	}
	return info.Mode().Perm(), uint32(stat.Gid)
}

// sgWriteSystemConfig writes yaml to the system configuration file as root
// with mode 0600, creating the directory when needed.
func sgWriteSystemConfig(t *testing.T, yaml string) {
	t.Helper()
	if err := os.MkdirAll("/etc/peers", 0o755); err != nil {
		t.Fatalf("syscontainer: creating /etc/peers: %v", err)
	}
	if err := os.WriteFile(sgConfigPath, []byte(yaml), 0o600); err != nil {
		t.Fatalf("syscontainer: writing %s: %v", sgConfigPath, err)
	}
	if err := os.Chmod(sgConfigPath, 0o600); err != nil {
		t.Fatalf("syscontainer: chmod %s: %v", sgConfigPath, err)
	}
	if err := os.Chown(sgConfigPath, 0, 0); err != nil {
		t.Fatalf("syscontainer: chown %s: %v", sgConfigPath, err)
	}
}

// sgWriteOtherConfig gives other a configuration file that makes its
// user Broker listen on 17444.
func sgWriteOtherConfig(t *testing.T) {
	t.Helper()
	dir := "/home/other/.peers"
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("syscontainer: creating %s: %v", dir, err)
	}
	path := dir + "/config.yaml"
	if err := os.WriteFile(path, []byte("broker:\n  listen: \":17444\"\n"), 0o600); err != nil {
		t.Fatalf("syscontainer: writing %s: %v", path, err)
	}
	for _, p := range []string{dir, path} {
		if err := os.Chown(p, sgOtherUID, sgOtherUID); err != nil {
			t.Fatalf("syscontainer: chown %s: %v", p, err)
		}
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatalf("syscontainer: chmod %s: %v", dir, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatalf("syscontainer: chmod %s: %v", path, err)
	}
}

// sgWaitRegistered waits until the session's MCP server has registered it.
// sgMCPPIDs returns the account's MCP server pids, sorted.
func sgMCPPIDs(t *testing.T, account string) []int {
	t.Helper()
	uid := syscontainer.AccountUID(t, account)
	var out []int
	for _, proc := range syscontainer.Pids("peers", "mcp") {
		if proc.UID == uid {
			out = append(out, proc.PID)
		}
	}
	sort.Ints(out)
	return out
}

// sgWaitRegistered waits until the session's MCP server has registered it.
// EventuallyPoll lifts the three-second allowance to ToolCallWaitBound so one
// poll call gets its own bound.
func sgWaitRegistered(t *testing.T, session *syscontainer.Session) {
	t.Helper()
	harness.EventuallyPoll(t, harness.EventTimeout, "the session "+session.Account+" to register", func(deadline time.Time) (bool, string) {
		text, isError, err := session.PollCall(t, deadline, "list_agents", map[string]any{})
		if err != nil {
			return false, err.Error()
		}
		return !isError, text
	})
}

// sgWaitConnected waits until the session's MCP server has reached a Broker.
// EventuallyPoll lifts the three-second allowance for the same reason as
// sgWaitRegistered.
func sgWaitConnected(t *testing.T, session *syscontainer.Session) {
	t.Helper()
	harness.EventuallyPoll(t, harness.EventTimeout, "the MCP server of "+session.Account+" to reach its Broker",
		func(deadline time.Time) (bool, string) {
			text, isError, err := session.PollCall(t, deadline, "list_brokers", map[string]any{})
			if err != nil {
				return false, err.Error()
			}
			return !isError, text
		})
}

// sgHasUserBroker reports whether a non-system Broker runs as uid.
func sgHasUserBroker(uid uint32) bool {
	for _, proc := range syscontainer.Pids("peers", "service", "run") {
		if proc.UID == uid {
			return true
		}
	}
	return false
}

// sgRun runs cmd and returns its result. A command that has not exited within
// timeout is killed and fails the test.
func sgRun(t *testing.T, timeout time.Duration, cmd *exec.Cmd) syscontainer.Result {
	t.Helper()
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("syscontainer: starting %q: %v", cmd.Args, err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case err := <-done:
		result := syscontainer.Result{Stdout: stdout.String(), Stderr: stderr.String()}
		if err != nil {
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) {
				t.Fatalf("syscontainer: running %q: %v", cmd.Args, err)
			}
			result.ExitCode = exitErr.ExitCode()
		}
		return result
	case <-time.After(timeout):
		_ = cmd.Process.Kill()
		err := <-done
		t.Fatalf("syscontainer: %q did not exit within %s (wait: %v); stderr:\n%s",
			cmd.Args, timeout, err, strings.TrimSpace(stderr.String()))
		return syscontainer.Result{}
	}
}
