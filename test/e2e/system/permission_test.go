//go:build e2e

package e2e

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/node"
	"github.com/wangzw/peers/test/e2e/syscontainer"
)

const (
	// permConfigPath is the system Broker's configuration file.
	permConfigPath = "/etc/peers/config.yaml"
	// permSocketPath is the system Broker's local socket.
	permSocketPath = "/run/peers/broker.sock"
	// permPeerListen is the user Broker's listen address in the pairing
	// test.
	permPeerListen = "127.0.0.1:17444"
)

// TestREQBrokerConfig023_NoPermissionStillWorksOnSystemBroker checks that an
// account with no permission on the configuration file can still register,
// join and leave topics, list what it may see, and exchange messages on the
// system Broker.
func TestREQBrokerConfig023_NoPermissionStillWorksOnSystemBroker(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Plain)
		return
	}

	syscontainer.StartSystemBroker(t, "broker:\n  alias: sys\n")

	peer := syscontainer.NewSession(t, "peer", "peeragent")
	permWaitRegistered(t, peer)
	// other leaves CLAUDE_CONFIG_DIR unset so this test also covers the
	// account's default configuration directory.
	other := syscontainer.NewSession(t, "other", "otheragent", syscontainer.WithoutConfigDir())
	permWaitRegistered(t, other)

	for _, call := range []struct {
		session *syscontainer.Session
		tool    string
		args    map[string]any
	}{
		{other, "list_agents", map[string]any{}},
		{other, "list_topics", map[string]any{}},
		{other, "list_brokers", map[string]any{}},
		{peer, "join_topic", map[string]any{"name": "e2e/p"}},
		{other, "join_topic", map[string]any{"name": "e2e/p"}},
		{other, "leave_topic", map[string]any{"name": "e2e/p"}},
		{other, "join_topic", map[string]any{"name": "e2e/p"}},
	} {
		if text, isError := call.session.Call(t, call.tool, call.args); isError {
			t.Fatalf("%s as %s reported an error: %s", call.tool, call.session.Account, text)
		}
	}

	to := permSelfAddress(t, peer)
	const message = "hello from an account without config permission"
	if text, isError := other.Call(t, "send_message",
		map[string]any{"to": to, "message": message}); isError {
		t.Fatalf("send_message as other reported an error: %s", text)
	}
	frame, ok := peer.Fake.WaitFrame(harness.EventTimeout)
	if !ok {
		t.Fatalf("peer received no message from other")
	}
	if !strings.Contains(frame.Content, message) {
		t.Errorf("peer's frame content = %q, want it to contain %q", frame.Content, message)
	}
}

// TestREQBrokerConfig021_WriteNeedsConfigWritePermission checks that every RPC
// that changes configuration state is refused without write permission on the
// configuration file, and that a grant takes effect without restarting the
// caller's MCP server.
func TestREQBrokerConfig021_WriteNeedsConfigWritePermission(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Plain)
		return
	}

	syscontainer.StartSystemBroker(t, "broker:\n  alias: sys\n")
	syscontainer.StartUserBroker(t, "peer", "--listen", ":17444")

	peer := syscontainer.NewSession(t, "peer", "peeragent")
	permWaitRegistered(t, peer)
	other := syscontainer.NewSession(t, "other", "otheragent")
	permWaitConnected(t, other)
	root := syscontainer.NewSession(t, "root", "admin")
	permWaitConnected(t, root)

	deniedKey := permPairingKey(t, peer)
	hashBefore := permFileSHA256(t, permConfigPath)

	for _, call := range []struct {
		tool string
		args map[string]any
	}{
		{"set_config", map[string]any{"key": "broker.alias", "value": "c-alias"}},
		{"set_broker_online", map[string]any{"online": false}},
		{"create_pairing_key", map[string]any{}},
		{"connect_broker", map[string]any{"address": permPeerListen, "key": deniedKey}},
		{"disconnect_broker", map[string]any{"broker": "peer-user"}},
	} {
		text, isError := other.Call(t, call.tool, call.args)
		if !isError {
			t.Errorf("%s as other without write permission succeeded: %s", call.tool, text)
			continue
		}
		if !strings.Contains(text, "(code: permission_denied)") {
			t.Errorf("%s error = %q, want (code: permission_denied)", call.tool, text)
		}
	}

	// The terminal commands that change the offline switch need the same write
	// permission and no less (REQ-broker-config-066).
	for _, command := range []string{"offline", "online"} {
		res := syscontainer.RunAs(t, "other", nil, "service", command, "--system")
		if res.ExitCode == 0 {
			t.Errorf("peers %s as other without write permission succeeded: %s", command, res.Stdout)
			continue
		}
		combined := res.Stdout + res.Stderr
		if !strings.Contains(combined, "permission_denied") {
			t.Errorf("peers %s error = %q, want permission_denied", command, combined)
		}
		if !strings.Contains(combined, permConfigPath) {
			t.Errorf("peers %s error = %q, want it to name %s", command, combined, permConfigPath)
		}
	}

	if hashAfter := permFileSHA256(t, permConfigPath); hashAfter != hashBefore {
		t.Errorf("the refused calls changed the configuration file:\nbefore %s\nafter  %s", hashBefore, hashAfter)
	}

	text, isError := root.Call(t, "list_brokers", map[string]any{})
	if isError {
		t.Fatalf("list_brokers as root: %s", text)
	}
	var listed struct {
		Brokers []struct {
			Alias  string `json:"alias"`
			IsPeer bool   `json:"is_peer"`
		} `json:"brokers"`
	}
	if err := json.Unmarshal([]byte(text), &listed); err != nil {
		t.Fatalf("decoding list_brokers result %q: %v", text, err)
	}
	for _, broker := range listed.Brokers {
		if broker.IsPeer {
			t.Errorf("the system Broker lists peer %q after the refused connect", broker.Alias)
		}
	}

	// Pair the system Broker with peer's user Broker as root first, so a
	// refused unpair from other has a visible pair to preserve.
	text, isError = root.Call(t, "connect_broker",
		map[string]any{"address": permPeerListen, "key": permPairingKey(t, peer)})
	if isError {
		t.Fatalf("connect_broker as root: %s", text)
	}
	var paired struct {
		Alias string `json:"alias"`
	}
	if err := json.Unmarshal([]byte(text), &paired); err != nil {
		t.Fatalf("decoding connect_broker result %q: %v", text, err)
	}
	if paired.Alias == "" {
		t.Fatalf("connect_broker as root returned no peer alias: %s", text)
	}
	harness.EventuallyPoll(t, harness.EventTimeout, "the system Broker to report the paired peer as connected",
		func(deadline time.Time) (bool, string) {
			status, isPeer, ok, state := permBrokerStatusPoll(t, deadline, root, paired.Alias)
			if state != "" {
				return false, state
			}
			return ok && isPeer && status == "connected", status
		})

	// A refused unpair must leave the peer connected, the Broker's alias
	// unchanged and no pairing key in the audit log.
	text, isError = other.Call(t, "disconnect_broker", map[string]any{"broker": paired.Alias})
	if !isError {
		t.Fatalf("disconnect_broker as other without write permission succeeded: %s", text)
	}
	if !strings.Contains(text, "(code: permission_denied)") {
		t.Errorf("disconnect_broker error = %q, want (code: permission_denied)", text)
	}
	if status, isPeer, ok := permBrokerStatus(t, root, paired.Alias); !ok || !isPeer || status != "connected" {
		t.Errorf("after the refused disconnect, peer %q has status %q (is_peer %v, found %v), want connected",
			paired.Alias, status, isPeer, ok)
	}
	if got := permConfigValue(t, root, "broker.alias"); got != "sys" {
		t.Errorf("broker.alias after the refused disconnect = %q, want %q", got, "sys")
	}

	var audit string
	harness.Eventually(t, harness.EventTimeout, "the system Broker's audit log to record the pairing", func() (bool, string) {
		res := syscontainer.RunAs(t, "root", nil, "service", "audit")
		if res.ExitCode != 0 {
			return false, res.Stderr
		}
		audit = res.Stdout
		return strings.Contains(audit, "pairing_succeeded"), audit
	})
	if strings.Contains(audit, "pairing_key_created") {
		t.Errorf("the system Broker's audit log records pairing_key_created:\n%s", audit)
	}

	// other's account is added to peers-admins in the account database; the
	// running MCP server keeps its old group list, so only a fresh check of the
	// database can see the grant.
	permGrantAdmins(t, "0660")

	if text, isError := other.Call(t, "set_config",
		map[string]any{"key": "broker.alias", "value": "c-alias"}); isError {
		t.Fatalf("set_config as other after the grant: %s", text)
	}
	if text, isError := other.Call(t, "create_pairing_key", map[string]any{}); isError {
		t.Fatalf("create_pairing_key as other after the grant: %s", text)
	}

	key := permPairingKey(t, peer)
	text, isError = other.Call(t, "connect_broker",
		map[string]any{"address": permPeerListen, "key": key})
	if isError {
		t.Fatalf("connect_broker as other after the grant: %s", text)
	}
	var connected struct {
		Alias string `json:"alias"`
	}
	if err := json.Unmarshal([]byte(text), &connected); err != nil {
		t.Fatalf("decoding connect_broker result %q: %v", text, err)
	}
	if connected.Alias == "" {
		t.Fatalf("connect_broker returned no peer alias: %s", text)
	}
	if text, isError := other.Call(t, "disconnect_broker",
		map[string]any{"broker": connected.Alias}); isError {
		t.Fatalf("disconnect_broker as other after the grant: %s", text)
	}

	// The grant belongs to other's account. peer is not in peers-admins, so
	// a fresh call from peer against the system Broker is still refused.
	sysPeer := syscontainer.NewSession(t, "peer", "syspeer",
		syscontainer.WithEnv("PEERS_TEST_SOCKET="+permSocketPath))
	permWaitConnected(t, sysPeer)
	text, isError = sysPeer.Call(t, "set_config",
		map[string]any{"key": "broker.alias", "value": "peer-alias"})
	if !isError {
		t.Errorf("set_config as peer after the grant to other succeeded: %s", text)
	} else if !strings.Contains(text, "(code: permission_denied)") {
		t.Errorf("set_config as peer after the grant to other = %q, want (code: permission_denied)", text)
	}
}

// TestREQBrokerConfig022_ReadNeedsConfigReadPermission checks that get_config
// is refused without read permission, that a group grant takes effect without
// a restart, and that an access control list entry is honoured even when the
// permission bits alone would deny it.
func TestREQBrokerConfig022_ReadNeedsConfigReadPermission(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Plain)
		return
	}

	syscontainer.StartSystemBroker(t, "broker:\n  alias: sys\n")

	other := syscontainer.NewSession(t, "other", "otheragent")
	permWaitConnected(t, other)

	text, isError := other.Call(t, "get_config", map[string]any{})
	if !isError {
		t.Fatalf("get_config as other without read permission succeeded: %s", text)
	}
	if !strings.Contains(text, "(code: permission_denied)") {
		t.Errorf("get_config as other without read permission = %q, want (code: permission_denied)", text)
	}
	if strings.Contains(text, "broker.alias") || strings.Contains(text, "entries") {
		t.Errorf("the refused get_config result names configuration values: %s", text)
	}

	permGrantAdmins(t, "0640")

	text, isError = other.Call(t, "get_config", map[string]any{})
	if isError {
		t.Fatalf("get_config as other after the grant: %s", text)
	}
	if !strings.Contains(text, "broker.alias") {
		t.Errorf("get_config as other after the grant does not name broker.alias: %s", text)
	}

	// peer is not in peers-admins and the mode stays 0640, so the read is
	// refused until the ACL entry below grants it.
	peer := syscontainer.NewSession(t, "peer", "peeragent")
	permWaitConnected(t, peer)
	text, isError = peer.Call(t, "get_config", map[string]any{})
	if !isError {
		t.Fatalf("get_config as peer before the ACL entry succeeded: %s", text)
	}
	if !strings.Contains(text, "(code: permission_denied)") {
		t.Errorf("get_config as peer before the ACL entry = %q, want (code: permission_denied)", text)
	}

	// The ACL grants peer read access while the mode stays 0640; only the ACL
	// decides the outcome.
	permRunRoot(t, "setfacl", "-m", "u:peer:r", permConfigPath)
	if info, err := os.Stat(permConfigPath); err != nil {
		t.Fatalf("syscontainer: stat %s: %v", permConfigPath, err)
	} else if got := info.Mode().Perm(); got != 0o640 {
		t.Errorf("the configuration file mode = %04o after setfacl, want 0640", got)
	}

	text, isError = peer.Call(t, "get_config", map[string]any{})
	if isError {
		t.Fatalf("get_config as peer after the ACL entry: %s", text)
	}
	if !strings.Contains(text, "broker.alias") {
		t.Errorf("get_config as peer after the ACL entry does not name broker.alias: %s", text)
	}
}

// TestREQBrokerConfig010_StatusWithoutReadPermissionShowsOwnAgents checks that
// status without read permission returns only the caller's own agents but still
// lists the paired peer and its connection state, while an account that may
// read the configuration sees both the agents and the same peer.
func TestREQBrokerConfig010_StatusWithoutReadPermissionShowsOwnAgents(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Plain)
		return
	}

	syscontainer.StartSystemBroker(t, "broker:\n  alias: sys\n")
	// A user Broker gives the system Broker a peer to show in status.
	peerBroker := syscontainer.StartUserBroker(t, "peer", "--listen", ":17444")

	peer := syscontainer.NewSession(t, "peer", "peeragent",
		syscontainer.WithEnv("PEERS_TEST_SOCKET="+permSocketPath))
	permWaitRegistered(t, peer)
	other := syscontainer.NewSession(t, "other", "otheragent")
	permWaitRegistered(t, other)
	pairing := syscontainer.NewSession(t, "peer", "pairing")
	permWaitRegistered(t, pairing)
	for _, call := range []struct {
		session *syscontainer.Session
		topic   string
	}{
		{peer, "e2e/peer"},
		{other, "e2e/other"},
	} {
		if text, isError := call.session.Call(t, "join_topic",
			map[string]any{"name": call.topic}); isError {
			t.Fatalf("join_topic as %s: %s", call.session.Account, text)
		}
	}

	root := syscontainer.NewSession(t, "root", "admin")
	permWaitConnected(t, root)
	text, isError := root.Call(t, "connect_broker",
		map[string]any{"address": permPeerListen, "key": permPairingKey(t, pairing)})
	if isError {
		t.Fatalf("connect_broker as root: %s", text)
	}
	var connected struct {
		Alias string `json:"alias"`
	}
	if err := json.Unmarshal([]byte(text), &connected); err != nil {
		t.Fatalf("decoding connect_broker result %q: %v", text, err)
	}
	if connected.Alias == "" {
		t.Fatalf("connect_broker as root returned no peer alias: %s", text)
	}

	// root may read the configuration file, so its status lists both agents
	// with their topics and the paired peer, connected.
	harness.Eventually(t, harness.EventTimeout, "the root status to list both agents, their topics and the connected peer", func() (bool, string) {
		res := syscontainer.RunAs(t, "root", nil, "service", "status")
		line, found := permStatusPeerLine(res.Stdout, connected.Alias)
		ok := res.ExitCode == 0 &&
			strings.Contains(res.Stdout, "peeragent@sys") &&
			strings.Contains(res.Stdout, "otheragent@sys") &&
			strings.Contains(res.Stdout, "e2e/peer") &&
			strings.Contains(res.Stdout, "e2e/other") &&
			strings.Contains(res.Stdout, "Peers (") &&
			found &&
			strings.Contains(line, "connected") &&
			strings.Contains(line, "established")
		return ok, res.Stdout + res.Stderr
	})

	// Break the peer's network. Both accounts must still list the peer, now not
	// connected.
	peerBroker.Stop(t)

	// other may not read the configuration file, so its status keeps only its
	// own agent and topic. The peer and its connection state stay visible.
	harness.Eventually(t, harness.EventTimeout, "other's status to show its own agent, its topic and the disconnected peer", func() (bool, string) {
		res := syscontainer.RunAs(t, "other", nil, "service", "status", "--system")
		if res.ExitCode != 0 {
			return false, res.Stderr
		}
		line, found := permStatusPeerLine(res.Stdout, connected.Alias)
		ok := strings.Contains(res.Stdout, "otheragent@sys") &&
			strings.Contains(res.Stdout, "e2e/other") &&
			!strings.Contains(res.Stdout, "peeragent") &&
			!strings.Contains(res.Stdout, "e2e/peer") &&
			strings.Contains(res.Stdout, "Peers (") &&
			found &&
			!strings.Contains(line, "connected") &&
			!strings.Contains(line, "established")
		return ok, res.Stdout + res.Stderr
	})

	// The filtered result is stable: other's status must keep excluding peer's
	// agent and peer's topic, while the peer row stays and stays disconnected.
	// Both agents are registered already, so a one-second observation is enough.
	harness.Consistently(t, time.Second, "other's status never shows peer's agent and keeps the disconnected peer", func() (bool, string) {
		res := syscontainer.RunAs(t, "other", nil, "service", "status", "--system")
		if res.ExitCode != 0 {
			return false, res.Stderr
		}
		if strings.Contains(res.Stdout, "peeragent") || strings.Contains(res.Stdout, "e2e/peer") {
			return false, res.Stdout
		}
		line, found := permStatusPeerLine(res.Stdout, connected.Alias)
		if !found || strings.Contains(line, "connected") || strings.Contains(line, "established") {
			return false, res.Stdout
		}
		return true, res.Stdout
	})

	// Grant other read permission through the configuration file's group.
	// other's status must now list both agents with their topics and the paired
	// peer, still not connected.
	permGrantAdmins(t, "0640")
	harness.Eventually(t, harness.EventTimeout, "other's status to list both agents, their topics and the disconnected peer", func() (bool, string) {
		res := syscontainer.RunAs(t, "other", nil, "service", "status", "--system")
		if res.ExitCode != 0 {
			return false, res.Stderr
		}
		line, found := permStatusPeerLine(res.Stdout, connected.Alias)
		ok := strings.Contains(res.Stdout, "peeragent@sys") &&
			strings.Contains(res.Stdout, "otheragent@sys") &&
			strings.Contains(res.Stdout, "e2e/peer") &&
			strings.Contains(res.Stdout, "e2e/other") &&
			strings.Contains(res.Stdout, "Peers (") &&
			found &&
			!strings.Contains(line, "connected") &&
			!strings.Contains(line, "established")
		return ok, res.Stdout + res.Stderr
	})
}

// TestREQBrokerConfig009_AuditNeedsReadPermissionOnSystemBroker checks the
// system half of REQ-broker-config-009: an account with read permission on the
// system Broker's configuration file sees every audit record in the terminal,
// an account without it is refused, and a grant takes effect without a restart.
func TestREQBrokerConfig009_AuditNeedsReadPermissionOnSystemBroker(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Plain)
		return
	}

	syscontainer.StartSystemBroker(t, "broker:\n  alias: sys\n")

	root := syscontainer.NewSession(t, "root", "admin")
	permWaitConnected(t, root)

	// Produce three different event kinds before comparing, so a log with a
	// single record cannot satisfy the record-for-record check.
	if text, isError := root.Call(t, "set_config",
		map[string]any{"key": "broker.alias", "value": "sys2"}); isError {
		t.Fatalf("set_config as root: %s", text)
	}
	permPairingKey(t, root)
	// Nothing listens on 127.0.0.1:1, so the connect fails after dialing
	// started, which records pairing_failed.
	if text, isError := root.Call(t, "connect_broker",
		map[string]any{"address": "127.0.0.1:1", "key": "not-a-pairing-key"}); !isError {
		t.Fatalf("connect_broker to an unreachable address succeeded: %s", text)
	}

	// root's Oracle reads the same records the terminal command reads, without
	// going through the audit subcommand, so the comparison below is against
	// the Broker's log itself.
	var events []*localpb.AuditEvent
	harness.Eventually(t, harness.EventTimeout, "the system Broker's audit log to record the three event kinds", func() (bool, string) {
		events = permAuditRecords(t)
		seen := make(map[localpb.AuditEventKind]bool, len(events))
		for _, event := range events {
			seen[event.GetEvent()] = true
		}
		ok := seen[localpb.AuditEventKind_AUDIT_EVENT_KIND_CONFIG_SET] &&
			seen[localpb.AuditEventKind_AUDIT_EVENT_KIND_PAIRING_KEY_CREATED] &&
			seen[localpb.AuditEventKind_AUDIT_EVENT_KIND_PAIRING_FAILED]
		return ok, fmt.Sprintf("%d records", len(events))
	})

	denied := syscontainer.RunAs(t, "other", nil, "service", "audit", "--system")
	if denied.ExitCode == 0 {
		t.Errorf("audit as other without read permission succeeded:\n%s", denied.Stdout)
	}
	if !strings.Contains(denied.Stderr, "permission_denied") {
		t.Errorf("audit as other without read permission does not explain the denial:\n%s", denied.Stderr)
	}

	permGrantAdmins(t, "0640")

	res := syscontainer.RunAs(t, "other", nil, "service", "audit", "--system")
	if res.ExitCode != 0 {
		t.Fatalf("peers service audit as other exited %d: %s", res.ExitCode, strings.TrimSpace(res.Stderr))
	}
	lines := permNonEmptyLines(res.Stdout)
	if len(lines) != len(events) {
		t.Fatalf("peers service audit printed %d lines for %d audit records:\n%s", len(lines), len(events), res.Stdout)
	}
	for i, event := range events {
		if name := permAuditKindName(event.GetEvent()); !strings.Contains(lines[i], name) {
			t.Fatalf("audit line %d is %q, want it to name %s", i+1, lines[i], name)
		}
	}

	// The grant goes to other's account. peer is not a member, so its audit
	// read is still refused and the reason names the missing permission.
	peerDenied := syscontainer.RunAs(t, "peer", nil, "service", "audit", "--system")
	if peerDenied.ExitCode == 0 {
		t.Errorf("audit as peer without read permission succeeded:\n%s", peerDenied.Stdout)
	}
	if !strings.Contains(peerDenied.Stderr, "permission_denied") {
		t.Errorf("audit as peer without read permission does not explain the denial:\n%s", peerDenied.Stderr)
	}
}

// permAuditRecords reads every audit record from the system Broker through its
// local interface. The test process runs as root, so the read permission check
// passes and the result is the Broker's full log.
func permAuditRecords(t *testing.T) []*localpb.AuditEvent {
	t.Helper()
	oracle, err := node.DialOracle(permSocketPath)
	if err != nil {
		t.Fatalf("dialing the system Broker's local interface: %v", err)
	}
	defer func() {
		if err := oracle.Close(); err != nil && t.Failed() {
			t.Logf("closing the system Broker's local interface: %v", err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	events, err := oracle.Audit(ctx, nil)
	if err != nil {
		t.Fatalf("reading the system Broker's audit log: %v", err)
	}
	return events
}

// permAuditKindName renders an audit event kind the way the terminal does.
func permAuditKindName(k localpb.AuditEventKind) string {
	if name, ok := localpb.AuditEventKind_name[int32(k)]; ok {
		return strings.ToLower(strings.TrimPrefix(name, "AUDIT_EVENT_KIND_"))
	}
	return strconv.Itoa(int(k))
}

// permNonEmptyLines splits output into its non-empty lines.
func permNonEmptyLines(s string) []string {
	var lines []string
	for _, line := range strings.Split(s, "\n") {
		if strings.TrimSpace(line) != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// permWaitRegistered waits until the session's MCP server has registered it.
// EventuallyPoll lifts the three-second allowance to ToolCallWaitBound so one
// poll call gets its own bound.
func permWaitRegistered(t *testing.T, session *syscontainer.Session) {
	t.Helper()
	harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "the session "+session.Account+" to register", func(deadline time.Time) (bool, string) {
		text, isError, err := session.PollCall(t, deadline, "list_agents", map[string]any{})
		if err != nil {
			return false, err.Error()
		}
		return !isError, text
	})
}

// permWaitConnected waits until the session's MCP server has reached a Broker.
// EventuallyPoll lifts the three-second allowance for the same reason as
// permWaitRegistered.
func permWaitConnected(t *testing.T, session *syscontainer.Session) {
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

// permSelfAddress returns the session's own agent address.
func permSelfAddress(t *testing.T, session *syscontainer.Session) string {
	t.Helper()
	text, isError := session.Call(t, "list_agents", map[string]any{})
	if isError {
		t.Fatalf("list_agents as %s: %s", session.Account, text)
	}
	var out struct {
		Self string `json:"self_address"`
	}
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		t.Fatalf("decoding list_agents result %q: %v", text, err)
	}
	if out.Self == "" {
		t.Fatalf("list_agents as %s returned no self_address: %s", session.Account, text)
	}
	return out.Self
}

// permConfigValue returns one configuration value read from the session's
// Broker.
func permConfigValue(t *testing.T, session *syscontainer.Session, key string) string {
	t.Helper()
	text, isError := session.Call(t, "get_config", map[string]any{"key": key})
	if isError {
		t.Fatalf("get_config %s as %s: %s", key, session.Account, text)
	}
	var out struct {
		Entries []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"entries"`
	}
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		t.Fatalf("decoding get_config result %q: %v", text, err)
	}
	if len(out.Entries) != 1 || out.Entries[0].Key != key {
		t.Fatalf("get_config %s as %s returned %s", key, session.Account, text)
	}
	return out.Entries[0].Value
}

// permStatusPeerLine returns the status table line naming alias.
func permStatusPeerLine(stdout, alias string) (string, bool) {
	for _, line := range strings.Split(stdout, "\n") {
		if strings.Contains(line, alias) {
			return line, true
		}
	}
	return "", false
}

// permBrokerStatus returns the status of the peer named alias in the session's
// list_brokers result.
func permBrokerStatus(t *testing.T, session *syscontainer.Session, alias string) (status string, isPeer, found bool) {
	t.Helper()
	text, isError := session.Call(t, "list_brokers", map[string]any{})
	if isError {
		t.Fatalf("list_brokers as %s: %s", session.Account, text)
	}
	var out struct {
		Brokers []struct {
			Alias  string `json:"alias"`
			IsPeer bool   `json:"is_peer"`
			Status string `json:"status"`
		} `json:"brokers"`
	}
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		t.Fatalf("decoding list_brokers result %q: %v", text, err)
	}
	for _, broker := range out.Brokers {
		if broker.Alias == alias {
			return broker.Status, broker.IsPeer, true
		}
	}
	return "", false, false
}

// permBrokerStatusPoll is permBrokerStatus for a wait callback: the call is
// bounded by the wait's deadline, and a transport or tool error is returned as
// state instead of failing the test.
func permBrokerStatusPoll(t *testing.T, deadline time.Time, session *syscontainer.Session, alias string) (status string, isPeer, found bool, state string) {
	t.Helper()
	out, state := sysPollDecode[mcpserver.ListBrokersOut](t, deadline, session, "list_brokers", map[string]any{})
	if state != "" {
		return "", false, false, state
	}
	for _, broker := range out.Brokers {
		if broker.Alias == alias {
			return broker.Status, broker.IsPeer, true, ""
		}
	}
	return "", false, false, ""
}

// permPairingKey creates a one-time pairing key at the session's Broker.
func permPairingKey(t *testing.T, session *syscontainer.Session) string {
	t.Helper()
	text, isError := session.Call(t, "create_pairing_key", map[string]any{})
	if isError {
		t.Fatalf("create_pairing_key as %s: %s", session.Account, text)
	}
	var out struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		t.Fatalf("decoding create_pairing_key result %q: %v", text, err)
	}
	if out.Key == "" {
		t.Fatalf("create_pairing_key as %s returned no key: %s", session.Account, text)
	}
	return out.Key
}

// permFileSHA256 returns the hexadecimal SHA-256 of path's bytes.
func permFileSHA256(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("syscontainer: reading %s: %v", path, err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// permGrantAdmins adds other to peers-admins and gives that group access to
// the configuration file with mode. It changes neither other's running MCP
// server nor the Broker.
func permGrantAdmins(t *testing.T, mode string) {
	t.Helper()
	permRunRoot(t, "chgrp", "peers-admins", permConfigPath)
	permRunRoot(t, "chmod", mode, permConfigPath)
	permRunRoot(t, "usermod", "-aG", "peers-admins", "other")
}

// permRunRoot runs a command as root and fails the test when it does not
// succeed.
func permRunRoot(t *testing.T, argv ...string) {
	t.Helper()
	cmd := syscontainer.Command("root", nil, argv[0], argv[1:]...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("syscontainer: %s: %v\n%s", strings.Join(argv, " "), err, strings.TrimSpace(string(out)))
	}
}
