//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/syscontainer"
)

const (
	// termAdminCallWait is the fallback for one terminal command and its Broker
	// round trip; no product timer governs it, so it only decides how long a
	// command that never answers takes to fail.
	termAdminCallWait = harness.EventTimeout

	// termAdminMCPHandshakeWait and termAdminToolCallWait mirror the bounds the
	// syscontainer package keeps unexported.
	termAdminMCPHandshakeWait = 30 * time.Second
	termAdminToolCallWait     = 15 * time.Second

	// linkDialTimeout and linkReconnectMin mirror the defaults of
	// link.dial_timeout and link.reconnect_min, which the system Broker
	// configuration leaves unset.
	linkDialTimeout  = 5 * time.Second
	linkReconnectMin = time.Second

	// termAdminLinkWait covers two dial attempts with one reconnect backoff
	// between them.
	termAdminLinkWait = 2*linkDialTimeout + linkReconnectMin

	// termAdminConnectedWait bounds host A's wait for host B's connected marker;
	// it covers host B's socket wait and broker list, the connect command's own
	// dial at link.dial_timeout, the wait for the link to come up, and the wait
	// for the pairing audit record, the commands counted at termAdminCallWait.
	termAdminConnectedWait = 3*termAdminCallWait + linkDialTimeout + termAdminLinkWait

	// termAdminPairingWait bounds host B's wait for the pairing file; it covers
	// host A's Broker start, the no-write session's MCP handshake, its
	// registration wait and tool calls, the refused pairing-key request, and the
	// key creation and peer listing, the terminal commands at termAdminCallWait.
	termAdminPairingWait = termAdminCallWait + termAdminMCPHandshakeWait +
		(termAdminCallWait + termAdminToolCallWait) + termAdminToolCallWait +
		2*termAdminCallWait

	// termAdminDoneWait bounds host B's wait for host A's completion marker; it
	// counts the peer wait at termAdminLinkWait and eight command fallbacks at
	// termAdminCallWait.
	termAdminDoneWait = termAdminLinkWait + 8*termAdminCallWait

	termAdminSystemConfig = "/etc/peers/config.yaml"

	termAdminPeerListen = ":17444"

	termAdminConfigKey         = "directory.max_records"
	termAdminConfigOld         = "1000"
	termAdminConfigNew         = "900"
	termAdminDeniedConfigValue = "800"

	// termAdminAdvertise is host A's advertised address: the internal network
	// has no route out, so the Broker cannot derive a dialable address from its
	// wildcard listen, and the name is the network alias host B resolves.
	termAdminAdvertise = "hostA:17443"

	// termAdminPairingFile carries host A's address and pairing key to host B,
	// termAdminConnectedFile announces that host B paired, and termAdminDoneFile
	// lets host B keep its Broker alive until host A has finished the audit and
	// disconnect assertions.
	termAdminPairingFile   = "pairing"
	termAdminConnectedFile = "connected"
	termAdminDoneFile      = "done"
)

var e2ePairOptions = syscontainer.PairOptions{SharedDir: syscontainer.PairSharedMount}

// TestREQBrokerConfig039_TerminalAdminCommands checks that a session without
// write permission is refused a pairing key, root manages the pairing and
// configuration from the terminal, both terminals list the peer connected, and
// the audit log records the operations in order.
func TestREQBrokerConfig039_TerminalAdminCommands(t *testing.T) {
	if !syscontainer.InContainer() {
		t.Parallel()
		syscontainer.RunPairTest(t, e2ePairOptions)
		return
	}

	switch syscontainer.PairRole() {
	case syscontainer.RoleA:
		termAdminHostA(t)
	case syscontainer.RoleB:
		termAdminHostB(t)
	default:
		t.Fatalf("the pair test runs in a pair container as role A or B")
	}
}

func termAdminHostA(t *testing.T) {
	defer syscontainer.PairGuard(t)()

	syscontainer.StartSystemBroker(t, "broker:\n  alias: sys\n  advertise: \""+termAdminAdvertise+"\"\n")

	other := syscontainer.NewSession(t, "other", "no-write")
	termAdminWaitRegistered(t, other)
	text, isError := other.Call(t, "create_pairing_key", map[string]any{})
	if !isError {
		t.Fatalf("create_pairing_key as other without write permission succeeded: %s", text)
	}
	termAdminRequireDenied(t, "create_pairing_key as other", text)

	created := syscontainer.RunAs(t, "root", nil, "key", "create")
	termAdminRequireOK(t, created, "peers key create as root")
	key, shown := termAdminPairingKey(t, created.Stdout)
	address := termAdminAdvertisedAddress(t, shown)

	// The empty-listing check runs before the key is published, so host B cannot
	// have paired when it runs.
	termAdminRequireNoBrokers(t, "root", nil, "host A")
	syscontainer.PairWrite(t, termAdminPairingFile, []byte(address+"\n"+key+"\n"))

	syscontainer.PairAwait(t, termAdminConnectedFile, termAdminConnectedWait)
	hostAPeer := termAdminWaitPeer(t, "root", nil, "host A", "acceptor")

	got := syscontainer.RunAs(t, "root", nil, "config", "get", termAdminConfigKey)
	termAdminRequireOK(t, got, "peers config get as root")
	wantGet := termAdminConfigKey + " = " + termAdminConfigOld
	if !strings.Contains(got.Stdout, wantGet) {
		t.Fatalf("config get does not print %q:\n%s", wantGet, got.Stdout)
	}

	set := syscontainer.RunAs(t, "root", nil, "config", "set", termAdminConfigKey, termAdminConfigNew)
	termAdminRequireOK(t, set, "peers config set as root")
	wantSet := termAdminConfigKey + ": " + termAdminConfigOld + " -> " + termAdminConfigNew
	if got := strings.TrimSpace(set.Stdout); got != wantSet {
		t.Fatalf("config set printed %q, want %q", got, wantSet)
	}

	gotAfter := syscontainer.RunAs(t, "root", nil, "config", "get", termAdminConfigKey)
	termAdminRequireOK(t, gotAfter, "peers config get as root after the change")
	wantGetAfter := termAdminConfigKey + " = " + termAdminConfigNew
	if !strings.Contains(gotAfter.Stdout, wantGetAfter) {
		t.Fatalf("config get does not print %q after the change:\n%s", wantGetAfter, gotAfter.Stdout)
	}

	disconnected := syscontainer.RunAs(t, "root", nil, "broker", "disconnect", hostAPeer.alias)
	termAdminRequireOK(t, disconnected, "peers broker disconnect as root")
	wantDisconnect := "Disconnected " + hostAPeer.alias
	if got := strings.TrimSpace(disconnected.Stdout); got != wantDisconnect {
		t.Fatalf("broker disconnect printed %q, want %q", got, wantDisconnect)
	}

	termAdminRequireNoBrokers(t, "root", nil, "host A")

	// pairing_succeeded on this side is written by the accepting side and has no
	// executing account; the match is an ordered subsequence, so other records
	// may interleave.
	termAdminWaitAudit(t, "root", nil, "host A", []termAdminEvent{
		{event: "pairing_key_created", account: "root"},
		{event: "pairing_succeeded"},
		{event: "config_set", account: "root"},
		{event: "unpaired", account: "root"},
	})

	denied := syscontainer.RunAs(t, "other", nil, "config", "set", termAdminConfigKey, termAdminDeniedConfigValue)
	if denied.ExitCode != 1 {
		t.Fatalf("config set as other without write permission exited %d, want 1\nstdout:\n%s\nstderr:\n%s",
			denied.ExitCode, denied.Stdout, denied.Stderr)
	}
	termAdminRequireDenied(t, "config set as other", denied.Stdout+"\n"+denied.Stderr)
	if !strings.Contains(denied.Stderr, termAdminSystemConfig) {
		t.Errorf("config set as other does not name the configuration file %s:\n%s",
			termAdminSystemConfig, denied.Stderr)
	}

	afterDenied := syscontainer.RunAs(t, "root", nil, "config", "get", termAdminConfigKey)
	termAdminRequireOK(t, afterDenied, "peers config get after the refused change")
	if want := termAdminConfigKey + " = " + termAdminConfigNew; !strings.Contains(afterDenied.Stdout, want) {
		t.Fatalf("config get after the refused change does not print %q:\n%s", want, afterDenied.Stdout)
	}

	audit := syscontainer.RunAs(t, "root", nil, "service", "audit")
	termAdminRequireOK(t, audit, "peers service audit as root")
	for _, event := range []string{"pairing_key_created", "config_set"} {
		if termAdminHasAuditEvent(audit.Stdout, event, "other") {
			t.Errorf("host A's audit records %s by other, but the call was refused:\n%s", event, audit.Stdout)
		}
	}

	syscontainer.PairWrite(t, termAdminDoneFile, []byte("done\n"))
}

func termAdminHostB(t *testing.T) {
	defer syscontainer.PairGuard(t)()

	syscontainer.StartUserBroker(t, "peer", "--listen", termAdminPeerListen)
	peerEnv := []string{"PEERS_TEST_SOCKET=" + syscontainer.UserSocketPath(t, "peer")}

	termAdminRequireNoBrokers(t, "peer", peerEnv, "host B")

	data := syscontainer.PairAwait(t, termAdminPairingFile, termAdminPairingWait)
	address, key := termAdminParsePairing(t, data)
	connected := syscontainer.RunAs(t, "peer", peerEnv, "broker", "connect", address, "--key", key)
	termAdminRequireOK(t, connected, "peers broker connect as peer on the other host")
	pairedAlias, pairedFingerprint := termAdminConnectOutput(t, connected.Stdout)

	hostBPeer := termAdminWaitPeer(t, "peer", peerEnv, "host B", "dialer")
	if hostBPeer.alias != pairedAlias || hostBPeer.fingerprint != pairedFingerprint {
		t.Errorf("host B lists peer %q (%s), want the paired alias %q and fingerprint %s",
			hostBPeer.alias, hostBPeer.fingerprint, pairedAlias, pairedFingerprint)
	}

	termAdminWaitAudit(t, "peer", peerEnv, "host B", []termAdminEvent{
		{event: "pairing_succeeded", account: "peer"},
	})
	syscontainer.PairWrite(t, termAdminConnectedFile, []byte("connected\n"))

	// Keep host B's Broker alive until host A has listed the peer and run the
	// disconnect, so the assertions there see the link still up.
	syscontainer.PairAwait(t, termAdminDoneFile, termAdminDoneWait)

	harness.Eventually(t, termAdminCallWait, "host B's broker list to stop showing the other Broker connected",
		func() (bool, string) {
			res := syscontainer.RunAs(t, "peer", peerEnv, "broker", "list")
			if res.ExitCode != 0 {
				return false, fmt.Sprintf("exit %d: %s", res.ExitCode, res.Stderr)
			}
			return !termAdminHasConnectedRow(res.Stdout), res.Stdout
		})
}

func termAdminParsePairing(t *testing.T, data []byte) (address, key string) {
	t.Helper()
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) != 2 || lines[0] == "" || lines[1] == "" {
		t.Fatalf("the shared pairing file is %q, want an address line and a key line", data)
	}
	return lines[0], lines[1]
}

// termAdminEvent is one expected audit record; an empty account matches a
// record written without an executing account.
type termAdminEvent struct {
	event   string
	account string
}

type termAdminPeerRow struct {
	alias       string
	fingerprint string
	peer        string
	status      string
	direction   string
}

func termAdminRequireOK(t *testing.T, res syscontainer.Result, what string) {
	t.Helper()
	if res.ExitCode != 0 {
		t.Fatalf("%s: exit %d\nstdout:\n%s\nstderr:\n%s",
			what, res.ExitCode, res.Stdout, res.Stderr)
	}
}

func termAdminRequireDenied(t *testing.T, what, text string) {
	t.Helper()
	if !strings.Contains(text, "permission_denied") {
		t.Fatalf("%s is not reported as permission_denied: %s", what, text)
	}
}

func termAdminWaitRegistered(t *testing.T, session *syscontainer.Session) {
	t.Helper()
	harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "the session of "+session.Account+" to register", func(deadline time.Time) (bool, string) {
		text, isError, err := session.PollCall(t, deadline, "list_agents", map[string]any{})
		if err != nil {
			return false, err.Error()
		}
		return !isError, text
	})
}

func termAdminPairingKey(t *testing.T, stdout string) (key string, shown []string) {
	t.Helper()

	var expires string
	inAddresses := false
	for _, line := range strings.Split(stdout, "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(trimmed, "Key:"):
			key = strings.TrimSpace(strings.TrimPrefix(trimmed, "Key:"))
		case strings.HasPrefix(trimmed, "Expires:"):
			expires = strings.TrimSpace(strings.TrimPrefix(trimmed, "Expires:"))
		case trimmed == "Addresses:":
			inAddresses = true
		case inAddresses && trimmed != "":
			shown = append(shown, trimmed)
		}
	}

	if key == "" {
		t.Fatalf("key create printed no key:\n%s", stdout)
	}
	if expires == "" {
		t.Fatalf("key create printed no expiry:\n%s", stdout)
	}
	expiry, err := time.Parse(time.RFC3339, expires)
	if err != nil {
		t.Fatalf("key create printed an expiry %q that is not RFC 3339: %v", expires, err)
	}
	if !strings.HasSuffix(expires, "Z") {
		t.Errorf("key create printed the expiry %q, want UTC with a Z suffix", expires)
	}
	if !expiry.After(time.Now()) {
		t.Errorf("key create printed the expiry %q, which is not in the future", expires)
	}
	if len(shown) == 0 {
		t.Fatalf("key create printed no address:\n%s", stdout)
	}
	return key, shown
}

// termAdminAdvertisedAddress returns the printed address whose dial part is
// termAdminAdvertise; the command prints each address as "name(host:port)".
func termAdminAdvertisedAddress(t *testing.T, shown []string) string {
	t.Helper()
	for _, entry := range shown {
		dial := entry
		if open := strings.LastIndex(entry, "("); open > 0 && strings.HasSuffix(entry, ")") {
			dial = entry[open+1 : len(entry)-1]
		}
		if dial == termAdminAdvertise {
			return entry
		}
	}
	t.Fatalf("key create printed addresses %v, none dials %s", shown, termAdminAdvertise)
	return ""
}

func termAdminRequireNoBrokers(t *testing.T, account string, env []string, host string) {
	t.Helper()
	res := syscontainer.RunAs(t, account, env, "broker", "list")
	termAdminRequireOK(t, res, "peers broker list as "+account+" on "+host)
	if !strings.Contains(res.Stdout, "Brokers: none") {
		t.Fatalf("%s's broker list does not print \"Brokers: none\":\n%s", host, res.Stdout)
	}
}

func termAdminConnectOutput(t *testing.T, stdout string) (alias, fingerprint string) {
	t.Helper()

	const prefix = "Paired with "
	line := strings.TrimSpace(stdout)
	if !strings.HasPrefix(line, prefix) {
		t.Fatalf("broker connect printed %q, want %q<alias> (<fingerprint>)", line, prefix)
	}
	rest := strings.TrimPrefix(line, prefix)
	open := strings.LastIndex(rest, " (")
	if open < 0 || !strings.HasSuffix(rest, ")") {
		t.Fatalf("broker connect printed %q, want %q<alias> (<fingerprint>)", line, prefix)
	}
	alias = strings.TrimSpace(rest[:open])
	fingerprint = rest[open+2 : len(rest)-1]
	if alias == "" {
		t.Fatalf("broker connect printed no alias: %q", line)
	}
	if !termAdminIsFingerprint(fingerprint) {
		t.Fatalf("broker connect printed fingerprint %q, want 64 lowercase hexadecimal digits", fingerprint)
	}
	return alias, fingerprint
}

func termAdminWaitPeer(t *testing.T, account string, env []string, host, wantDirection string) termAdminPeerRow {
	t.Helper()

	var row termAdminPeerRow
	harness.Eventually(t, termAdminLinkWait, host+"'s broker list to show the other Broker connected", func() (bool, string) {
		res := syscontainer.RunAs(t, account, env, "broker", "list")
		if res.ExitCode != 0 {
			return false, fmt.Sprintf("exit %d: %s", res.ExitCode, res.Stderr)
		}
		found, ok := termAdminConnectedPeer(res.Stdout, wantDirection)
		if !ok {
			return false, res.Stdout
		}
		row = found
		return true, ""
	})
	return row
}

func termAdminConnectedPeer(stdout, direction string) (termAdminPeerRow, bool) {
	for _, line := range strings.Split(stdout, "\n") {
		fields := strings.Fields(line)
		// ALIAS, FINGERPRINT, PEER, STATUS, DIRECTION, then the addresses.
		if len(fields) < 6 || fields[0] == "ALIAS" {
			continue
		}
		row := termAdminPeerRow{
			alias:       fields[0],
			fingerprint: fields[1],
			peer:        fields[2],
			status:      fields[3],
			direction:   fields[4],
		}
		if row.peer == "yes" && row.status == "connected" && row.direction == direction {
			return row, true
		}
	}
	return termAdminPeerRow{}, false
}

func termAdminHasConnectedRow(stdout string) bool {
	for _, line := range strings.Split(stdout, "\n") {
		fields := strings.Fields(line)
		// ALIAS, FINGERPRINT, PEER, STATUS, DIRECTION, then the addresses.
		if len(fields) < 6 || fields[0] == "ALIAS" {
			continue
		}
		if fields[3] == "connected" {
			return true
		}
	}
	return false
}

func termAdminWaitAudit(t *testing.T, account string, env []string, host string, wants []termAdminEvent) {
	t.Helper()

	harness.Eventually(t, termAdminCallWait, host+"'s audit log to record "+termAdminEventNames(wants)+" in order",
		func() (bool, string) {
			res := syscontainer.RunAs(t, account, env, "service", "audit")
			if res.ExitCode != 0 {
				return false, fmt.Sprintf("exit %d: %s", res.ExitCode, res.Stderr)
			}
			return termAdminContainsOrder(res.Stdout, wants), res.Stdout
		})
}

func termAdminEventNames(wants []termAdminEvent) string {
	names := make([]string, 0, len(wants))
	for _, want := range wants {
		name := want.event
		if want.account != "" {
			name += " by " + want.account
		}
		names = append(names, name)
	}
	return "[" + strings.Join(names, ", ") + "]"
}

func termAdminContainsOrder(stdout string, wants []termAdminEvent) bool {
	next := 0
	for _, line := range strings.Split(stdout, "\n") {
		if next == len(wants) {
			break
		}
		event, account := termAdminAuditEvent(line)
		if event == wants[next].event && account == wants[next].account {
			next++
		}
	}
	return next == len(wants)
}

func termAdminHasAuditEvent(stdout, event, account string) bool {
	for _, line := range strings.Split(stdout, "\n") {
		gotEvent, gotAccount := termAdminAuditEvent(line)
		if gotEvent == event && gotAccount == account {
			return true
		}
	}
	return false
}

// termAdminAuditEvent returns the event name and the account of one audit line:
// the time, the event name, then key=value fields.
func termAdminAuditEvent(line string) (event, account string) {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return "", ""
	}
	event = fields[1]
	for _, field := range fields[2:] {
		if strings.HasPrefix(field, "account=") {
			account = strings.Trim(strings.TrimPrefix(field, "account="), "\"")
		}
	}
	return event, account
}

func termAdminIsFingerprint(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}
