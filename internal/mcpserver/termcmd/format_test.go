package termcmd

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/mcpserver/pairingprompt"
)

// TestWriteStatusListsAgentsAndTopics checks the two tables, their sorting and
// the fingerprint, status and address columns.
func TestWriteStatusListsAgentsAndTopics(t *testing.T) {
	fingerprint := make([]byte, 32)
	for i := range fingerprint {
		fingerprint[i] = byte(i + 1)
	}

	resp := &localpb.StatusResponse{
		Agents: []*localpb.AgentStatusEntry{
			{
				AgentId: "bob",
				Address: "bob@h",
				Account: "wangzw",
				Topics:  []string{"t2"},
				Status:  localpb.AgentStatus_AGENT_STATUS_BUSY,
			},
			{
				AgentId: "alice",
				Address: "alice@h",
				Account: "wangzw",
				Topics:  []string{"t1", "t0"},
				Status:  localpb.AgentStatus_AGENT_STATUS_IDLE,
			},
		},
		Peers: []*localpb.BrokerInfo{
			{
				Alias:       "host-b",
				Fingerprint: fingerprint,
				Status:      localpb.BrokerStatus_BROKER_STATUS_CONNECTABLE,
				Role:        localpb.PeerRole_PEER_ROLE_DIALER,
				LinkState:   localpb.LinkState_LINK_STATE_BACKOFF,
				AdvertisedAddresses: []*localpb.AdvertisedAddress{
					{Kind: "direct", Addr: "10.0.0.2:17443"},
					{Kind: "relay", Addr: "10.0.0.4:17443"},
				},
			},
		},
	}

	var buf bytes.Buffer
	if err := WriteStatus(&buf, resp); err != nil {
		t.Fatalf("WriteStatus: %v", err)
	}

	want := "Agents (2):\n" +
		"  ADDRESS  ACCOUNT  STATUS  TOPICS  PREVIOUS NAME\n" +
		"  alice@h  wangzw   idle    t0,t1   -\n" +
		"  bob@h    wangzw   busy    t2      -\n" +
		"Peers (1):\n" +
		"  ALIAS   FINGERPRINT                                                       ROLE    STATUS       LINK     ADDRESSES                                    DETAIL\n" +
		"  host-b  0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20  dialer  connectable  backoff  direct 10.0.0.2:17443, relay 10.0.0.4:17443  -\n"
	if got := buf.String(); got != want {
		t.Errorf("WriteStatus output =\n%q\nwant\n%q", got, want)
	}
}

// TestWriteStatusEmptySections checks that empty agent and peer sections are
// written as "none".
func TestWriteStatusEmptySections(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteStatus(&buf, &localpb.StatusResponse{}); err != nil {
		t.Fatalf("WriteStatus: %v", err)
	}
	if got, want := buf.String(), "Agents: none\nPeers: none\n"; got != want {
		t.Errorf("WriteStatus output = %q, want %q", got, want)
	}
}

// TestWriteStatusBrokerState checks the status lines and section hiding for each
// degraded and offline combination.
func TestWriteStatusBrokerState(t *testing.T) {
	const reason = "listen address :17443 is unavailable: address already in use"
	const fix = "peers config set broker.listen <free address>"
	const degradedLocal = "Broker: degraded (local only): " + reason + "\nFix: " + fix + "\n"
	const degradedDiagnostics = "Broker: degraded (diagnostics only): " + reason + "\nFix: " + fix + "\n"
	const offline = "Broker: offline; messages from other hosts are refused. Run peers service online to accept them.\n"
	const sections = "Agents: none\nPeers: none\n"

	cases := []struct {
		name string
		resp *localpb.StatusResponse
		want string
	}{
		{
			name: "local only",
			resp: &localpb.StatusResponse{Degraded: &localpb.DegradedState{
				Level:  localpb.DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY,
				Reason: reason,
				Fix:    fix,
			}},
			want: degradedLocal + sections,
		},
		{
			name: "diagnostics only",
			resp: &localpb.StatusResponse{Degraded: &localpb.DegradedState{
				Level:  localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY,
				Reason: reason,
				Fix:    fix,
			}},
			want: degradedDiagnostics,
		},
		{
			name: "offline",
			resp: &localpb.StatusResponse{Offline: true},
			want: offline + sections,
		},
		{
			name: "local only and offline",
			resp: &localpb.StatusResponse{
				Offline: true,
				Degraded: &localpb.DegradedState{
					Level:  localpb.DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY,
					Reason: reason,
					Fix:    fix,
				},
			},
			want: degradedLocal + offline + sections,
		},
		{
			name: "normal stays unchanged",
			resp: &localpb.StatusResponse{},
			want: sections,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteStatus(&buf, tc.resp); err != nil {
				t.Fatalf("WriteStatus: %v", err)
			}
			if got := buf.String(); got != tc.want {
				t.Errorf("WriteStatus output = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestWriteStatusDiagnosticsOnlyKeepsUpdateLineOut checks that a diagnostics
// only Broker prints exactly its two status lines.
func TestWriteStatusDiagnosticsOnlyKeepsUpdateLineOut(t *testing.T) {
	resp := &localpb.StatusResponse{
		CurrentVersion:   "0.2.0",
		AvailableVersion: "0.3.0",
		Degraded: &localpb.DegradedState{
			Level:  localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY,
			Reason: "config parse failed",
			Fix:    "fix the config",
		},
	}
	var buf bytes.Buffer
	if err := WriteStatus(&buf, resp); err != nil {
		t.Fatalf("WriteStatus: %v", err)
	}
	want := "Broker: degraded (diagnostics only): config parse failed\nFix: fix the config\n"
	if got := buf.String(); got != want {
		t.Errorf("WriteStatus output = %q, want %q", got, want)
	}
}

// TestWriteStatusPeersOmitted checks that a status with only agents still ends
// with the empty peers line.
func TestWriteStatusPeersOmitted(t *testing.T) {
	resp := &localpb.StatusResponse{
		Agents: []*localpb.AgentStatusEntry{
			{Address: "alice@h", Status: localpb.AgentStatus_AGENT_STATUS_IDLE},
		},
	}
	var buf bytes.Buffer
	if err := WriteStatus(&buf, resp); err != nil {
		t.Fatalf("WriteStatus: %v", err)
	}
	if got := buf.String(); !strings.Contains(got, "Agents (1):") || !strings.HasSuffix(got, "Peers: none\n") {
		t.Errorf("WriteStatus output = %q, want an Agents section followed by %q", got, "Peers: none\n")
	}
}

// TestWriteStatusFingerprintFullHex checks that a fingerprint is written as its
// full lowercase hex.
func TestWriteStatusFingerprintFullHex(t *testing.T) {
	fingerprint := bytes.Repeat([]byte{0xab}, 32)
	resp := &localpb.StatusResponse{
		Peers: []*localpb.BrokerInfo{{Fingerprint: fingerprint}},
	}
	var buf bytes.Buffer
	if err := WriteStatus(&buf, resp); err != nil {
		t.Fatalf("WriteStatus: %v", err)
	}
	want := strings.Repeat("ab", 32)
	if !strings.Contains(buf.String(), want) {
		t.Errorf("WriteStatus output = %q, want it to contain the full lowercase fingerprint %q", buf.String(), want)
	}
}

// TestWriteStatusSortsPeers checks that peers are listed in alias order.
func TestWriteStatusSortsPeers(t *testing.T) {
	resp := &localpb.StatusResponse{
		Peers: []*localpb.BrokerInfo{
			{Alias: "host-z", Fingerprint: bytes.Repeat([]byte{0x02}, 32)},
			{Alias: "host-a", Fingerprint: bytes.Repeat([]byte{0x01}, 32)},
		},
	}
	var buf bytes.Buffer
	if err := WriteStatus(&buf, resp); err != nil {
		t.Fatalf("WriteStatus: %v", err)
	}
	got := buf.String()
	first := strings.Index(got, "host-a")
	last := strings.Index(got, "host-z")
	if first < 0 || last < 0 {
		t.Fatalf("WriteStatus output = %q, want both peers listed", got)
	}
	if first > last {
		t.Errorf("WriteStatus output = %q, want host-a before host-z", got)
	}
}

// TestWriteAuditEventFields checks the audit line's time, name and fields for a
// configuration change.
func TestWriteAuditEventFields(t *testing.T) {
	ev := &localpb.AuditEvent{
		Time:     timestamppb.New(time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)),
		Event:    localpb.AuditEventKind_AUDIT_EVENT_KIND_CONFIG_SET,
		Actor:    "alice@h",
		Account:  "wangzw",
		Key:      "send.request_timeout",
		OldValue: &localpb.ConfigValue{Value: &localpb.ConfigValue_DurationValue{DurationValue: durationpb.New(30 * time.Second)}},
		NewValue: &localpb.ConfigValue{Value: &localpb.ConfigValue_DurationValue{DurationValue: durationpb.New(time.Minute)}},
	}

	var buf bytes.Buffer
	if err := WriteAuditEvent(&buf, ev); err != nil {
		t.Fatalf("WriteAuditEvent: %v", err)
	}
	want := "2026-11-18T10:00:00Z config_set actor=alice@h account=wangzw key=send.request_timeout old=30s new=1m0s\n"
	if got := buf.String(); got != want {
		t.Errorf("WriteAuditEvent output = %q, want %q", got, want)
	}
}

// TestWriteAuditEventQuotingAndCount checks that a quoted detail and a non-zero
// count are written and a zero count is omitted.
func TestWriteAuditEventQuotingAndCount(t *testing.T) {
	fingerprint := bytes.Repeat([]byte{0xcd}, 32)
	ev := &localpb.AuditEvent{
		Time:   timestamppb.New(time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)),
		Event:  localpb.AuditEventKind_AUDIT_EVENT_KIND_PAIRING_FAILED,
		Peer:   &localpb.PeerRef{Alias: "host-b", Fingerprint: fingerprint},
		Count:  3,
		Detail: `bad key "x"`,
	}

	var buf bytes.Buffer
	if err := WriteAuditEvent(&buf, ev); err != nil {
		t.Fatalf("WriteAuditEvent: %v", err)
	}
	got := buf.String()

	peer := "peer=host-b(" + strings.Repeat("cd", 32) + ")"
	for _, want := range []string{peer, "count=3", `detail="bad key \"x\""`} {
		if !strings.Contains(got, want) {
			t.Errorf("WriteAuditEvent output = %q, want it to contain %q", got, want)
		}
	}

	ev.Count = 0
	buf.Reset()
	if err := WriteAuditEvent(&buf, ev); err != nil {
		t.Fatalf("WriteAuditEvent: %v", err)
	}
	if strings.Contains(buf.String(), "count=") {
		t.Errorf("WriteAuditEvent output = %q, want no count field when the count is zero", buf.String())
	}
}

// TestWriteConfigEntries checks that entries are sorted by key, a secret shows
// "set" and an unset value shows "-".
func TestWriteConfigEntries(t *testing.T) {
	entries := []*localpb.ConfigEntry{
		{Key: "topics.auto_join", Value: &localpb.ConfigValue{Value: &localpb.ConfigValue_BoolValue{BoolValue: true}}},
		{Key: "pairing.static_key", Value: &localpb.ConfigValue{Value: &localpb.ConfigValue_SecretIsSet{SecretIsSet: true}}},
		{Key: "broker.advertise"},
		{Key: "broker.alias", Value: &localpb.ConfigValue{Value: &localpb.ConfigValue_StringValue{StringValue: "hostA"}}},
	}

	var buf bytes.Buffer
	if err := WriteConfigEntries(&buf, entries); err != nil {
		t.Fatalf("WriteConfigEntries: %v", err)
	}

	want := "broker.advertise = -\n" +
		"broker.alias = hostA\n" +
		"pairing.static_key = set\n" +
		"topics.auto_join = true\n"
	if got := buf.String(); got != want {
		t.Errorf("WriteConfigEntries output = %q, want %q", got, want)
	}
}

// TestWriteConfigEntriesSecretUnset checks that an unset secret is written as
// "unset".
func TestWriteConfigEntriesSecretUnset(t *testing.T) {
	entries := []*localpb.ConfigEntry{
		{Key: "pairing.static_key", Value: &localpb.ConfigValue{Value: &localpb.ConfigValue_SecretIsSet{SecretIsSet: false}}},
	}

	var buf bytes.Buffer
	if err := WriteConfigEntries(&buf, entries); err != nil {
		t.Fatalf("WriteConfigEntries: %v", err)
	}
	if got, want := buf.String(), "pairing.static_key = unset\n"; got != want {
		t.Errorf("WriteConfigEntries output = %q, want %q", got, want)
	}
}

// TestWriteConfigEntriesEmpty checks that no entries write nothing.
func TestWriteConfigEntriesEmpty(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteConfigEntries(&buf, nil); err != nil {
		t.Fatalf("WriteConfigEntries: %v", err)
	}
	if buf.Len() != 0 {
		t.Errorf("WriteConfigEntries wrote %q, want nothing", buf.String())
	}
}

// TestWriteConfigChange checks the change line for a previous value and for an
// unset old value.
func TestWriteConfigChange(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		oldValue *localpb.ConfigValue
		newValue *localpb.ConfigValue
		want     string
	}{
		{
			name:     "from a previous value",
			key:      "topics.auto_join",
			oldValue: &localpb.ConfigValue{Value: &localpb.ConfigValue_BoolValue{BoolValue: false}},
			newValue: &localpb.ConfigValue{Value: &localpb.ConfigValue_BoolValue{BoolValue: true}},
			want:     "topics.auto_join: false -> true\n",
		},
		{
			name:     "from unset",
			key:      "broker.alias",
			oldValue: nil,
			newValue: &localpb.ConfigValue{Value: &localpb.ConfigValue_StringValue{StringValue: "hostA"}},
			want:     "broker.alias: - -> hostA\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteConfigChange(&buf, tt.key, tt.oldValue, tt.newValue); err != nil {
				t.Fatalf("WriteConfigChange: %v", err)
			}
			if got := buf.String(); got != tt.want {
				t.Errorf("WriteConfigChange output = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestWritePairingKey checks the key, expiry, addresses and multi-address
// connect prompt.
func TestWritePairingKey(t *testing.T) {
	resp := &localpb.CreatePairingKeyResponse{
		Key:            "one-time-key",
		ExpiresAt:      timestamppb.New(time.Date(2026, 11, 18, 10, 15, 0, 0, time.UTC)),
		ShownAddresses: []string{"hostA:17443", "10.0.0.2:17443"},
	}

	var buf bytes.Buffer
	if err := WritePairingKey(&buf, resp); err != nil {
		t.Fatalf("WritePairingKey: %v", err)
	}

	want := "Key: one-time-key\n" +
		"Expires: 2026-11-18T10:15:00Z\n" +
		"Addresses:\n" +
		"  hostA:17443\n" +
		"  10.0.0.2:17443\n" +
		"\n" +
		"To pair, paste this into a Claude Code session on the other host:\n" +
		"  Connect to the peers Broker at hostA:17443 or 10.0.0.2:17443 using the one-time pairing key one-time-key, choosing whichever address is reachable from this host.\n" +
		"Or run this on the other host:\n" +
		"  peers broker connect hostA:17443 --key one-time-key\n" +
		"  If that address is not reachable from the other host, use 10.0.0.2:17443 instead.\n"
	if got := buf.String(); got != want {
		t.Errorf("WritePairingKey output = %q, want %q", got, want)
	}
}

// TestWritePairingKeySingleAddress checks the connect prompt for a single
// address.
func TestWritePairingKeySingleAddress(t *testing.T) {
	resp := &localpb.CreatePairingKeyResponse{
		Key:            "one-time-key",
		ExpiresAt:      timestamppb.New(time.Date(2026, 11, 18, 10, 15, 0, 0, time.UTC)),
		ShownAddresses: []string{"laptop(10.12.72.11:17443)"},
	}

	var buf bytes.Buffer
	if err := WritePairingKey(&buf, resp); err != nil {
		t.Fatalf("WritePairingKey: %v", err)
	}

	want := "Key: one-time-key\n" +
		"Expires: 2026-11-18T10:15:00Z\n" +
		"Addresses:\n" +
		"  laptop(10.12.72.11:17443)\n" +
		"\n" +
		"To pair, paste this into a Claude Code session on the other host:\n" +
		"  Connect to the peers Broker at laptop(10.12.72.11:17443) using the one-time pairing key one-time-key.\n" +
		"Or run this on the other host:\n" +
		"  peers broker connect 'laptop(10.12.72.11:17443)' --key one-time-key\n"
	if got := buf.String(); got != want {
		t.Errorf("WritePairingKey output = %q, want %q", got, want)
	}
}

// TestWritePairingKeyUsesSharedConnectPrompt checks that WritePairingKey appends
// the prompt pairingprompt.ConnectPrompt returns.
func TestWritePairingKeyUsesSharedConnectPrompt(t *testing.T) {
	resp := &localpb.CreatePairingKeyResponse{
		Key:            "one-time-key",
		ShownAddresses: []string{"hostA:17443", "10.0.0.2:17443"},
	}

	var buf bytes.Buffer
	if err := WritePairingKey(&buf, resp); err != nil {
		t.Fatalf("WritePairingKey: %v", err)
	}

	prompt := pairingprompt.ConnectPrompt(resp.GetShownAddresses(), resp.GetKey())
	if prompt == "" {
		t.Fatal("ConnectPrompt returned nothing for two addresses")
	}
	if got := buf.String(); !strings.HasSuffix(got, "\n\n"+prompt+"\n") {
		t.Errorf("WritePairingKey output = %q, want it to end with the shared prompt %q", got, prompt)
	}
}

// TestWritePairingKeyEmpty checks the output for a nil response.
func TestWritePairingKeyEmpty(t *testing.T) {
	var buf bytes.Buffer
	if err := WritePairingKey(&buf, nil); err != nil {
		t.Fatalf("WritePairingKey: %v", err)
	}
	want := "Key: -\nExpires: -\nAddresses:\n"
	if got := buf.String(); got != want {
		t.Errorf("WritePairingKey output = %q, want %q", got, want)
	}
}

// TestWritePairingKeyNoAddressNoPrompt checks that a response without an address
// prints no prompt.
func TestWritePairingKeyNoAddressNoPrompt(t *testing.T) {
	resp := &localpb.CreatePairingKeyResponse{Key: "one-time-key"}

	var buf bytes.Buffer
	if err := WritePairingKey(&buf, resp); err != nil {
		t.Fatalf("WritePairingKey: %v", err)
	}

	want := "Key: one-time-key\nExpires: -\nAddresses:\n"
	if got := buf.String(); got != want {
		t.Errorf("WritePairingKey output = %q, want %q", got, want)
	}
}

// TestWriteBrokerList checks the table's alias order and its peer, status,
// direction and address columns.
func TestWriteBrokerList(t *testing.T) {
	fingerprint := func(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }
	brokers := []*localpb.BrokerInfo{
		{
			Alias:       "hostB",
			Fingerprint: fingerprint(0xcd),
			IsPeer:      true,
			Status:      localpb.BrokerStatus_BROKER_STATUS_CONNECTABLE,
			Role:        localpb.PeerRole_PEER_ROLE_ACCEPTOR,
			AdvertisedAddresses: []*localpb.AdvertisedAddress{
				{Kind: "direct", Addr: "hostb.example:17443"},
			},
		},
		{
			Alias:       "hostA",
			Fingerprint: fingerprint(0xab),
			IsPeer:      true,
			Status:      localpb.BrokerStatus_BROKER_STATUS_CONNECTED,
			Role:        localpb.PeerRole_PEER_ROLE_DIALER,
			AdvertisedAddresses: []*localpb.AdvertisedAddress{
				{Kind: "direct", Addr: "hosta.example:17443"},
				{Kind: "relay", Addr: "relay.example:17443"},
			},
		},
		{
			Alias:       "hostC",
			Fingerprint: fingerprint(0x01),
			IsPeer:      false,
			Status:      localpb.BrokerStatus_BROKER_STATUS_NOT_DIRECTLY_REACHABLE,
		},
	}

	var buf bytes.Buffer
	if err := WriteBrokerList(&buf, brokers, &localpb.LocalBrokerState{}); err != nil {
		t.Fatalf("WriteBrokerList: %v", err)
	}

	want := "  ALIAS  FINGERPRINT" + strings.Repeat(" ", 55) + "PEER  STATUS" + strings.Repeat(" ", 18) + "DIRECTION  ADDRESSES\n" +
		"  hostA  " + strings.Repeat("ab", 32) + "  yes   connected" + strings.Repeat(" ", 15) + "dialer     direct hosta.example:17443, relay relay.example:17443\n" +
		"  hostB  " + strings.Repeat("cd", 32) + "  yes   connectable" + strings.Repeat(" ", 13) + "acceptor   direct hostb.example:17443\n" +
		"  hostC  " + strings.Repeat("01", 32) + "  no    not_directly_reachable  -          -\n"
	if got := buf.String(); got != want {
		t.Errorf("WriteBrokerList output =\n%q\nwant\n%q", got, want)
	}
}

// TestWriteBrokerListEmpty checks that no Brokers write "Brokers: none".
func TestWriteBrokerListEmpty(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteBrokerList(&buf, nil, &localpb.LocalBrokerState{}); err != nil {
		t.Fatalf("WriteBrokerList: %v", err)
	}
	if got, want := buf.String(), "Brokers: none\n"; got != want {
		t.Errorf("WriteBrokerList output = %q, want %q", got, want)
	}
}

// TestWriteBrokerListWithoutLocalState checks that a response without local state
// is rejected and nothing is written.
func TestWriteBrokerListWithoutLocalState(t *testing.T) {
	var buf bytes.Buffer
	err := WriteBrokerList(&buf, nil, nil)
	if err == nil {
		t.Fatal("WriteBrokerList returned no error without local state")
	}
	if !strings.Contains(err.Error(), "the Broker did not report its local state") {
		t.Errorf("error = %q, want the missing-local-state text", err)
	}
	if buf.Len() != 0 {
		t.Errorf("WriteBrokerList wrote %q, want nothing", buf.String())
	}
}

// TestWriteBrokerListBrokerState covers the local state broker list shows before
// the table: local only degradation, offline, and both together.
func TestWriteBrokerListBrokerState(t *testing.T) {
	const reason = "listen address :17443 is unavailable: address already in use"
	const fix = "peers config set broker.listen <free address>"
	const degradedLocal = "Broker: degraded (local only): " + reason + "\nFix: " + fix + "\n"
	const offline = "Broker: offline; messages from other hosts are refused. Run peers service online to accept them.\n"
	const table = "Brokers: none\n"

	cases := []struct {
		name  string
		local *localpb.LocalBrokerState
		want  string
	}{
		{name: "online and not degraded", local: &localpb.LocalBrokerState{}, want: table},
		{
			name: "local only",
			local: &localpb.LocalBrokerState{Degraded: &localpb.DegradedState{
				Level:  localpb.DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY,
				Reason: reason,
				Fix:    fix,
			}},
			want: degradedLocal + table,
		},
		{name: "offline", local: &localpb.LocalBrokerState{Offline: true}, want: offline + table},
		{
			name: "local only and offline",
			local: &localpb.LocalBrokerState{
				Offline: true,
				Degraded: &localpb.DegradedState{
					Level:  localpb.DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY,
					Reason: reason,
					Fix:    fix,
				},
			},
			want: degradedLocal + offline + table,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteBrokerList(&buf, nil, tc.local); err != nil {
				t.Fatalf("WriteBrokerList: %v", err)
			}
			if got := buf.String(); got != tc.want {
				t.Errorf("WriteBrokerList output = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestWritePairingResult checks the pairing result line with alias and
// fingerprint.
func TestWritePairingResult(t *testing.T) {
	resp := &localpb.ConnectResponse{
		Alias:       "hostB",
		Fingerprint: bytes.Repeat([]byte{0xcd}, 32),
	}
	var buf bytes.Buffer
	if err := WritePairingResult(&buf, resp); err != nil {
		t.Fatalf("WritePairingResult: %v", err)
	}
	want := "Paired with hostB (" + strings.Repeat("cd", 32) + ")\n"
	if got := buf.String(); got != want {
		t.Errorf("WritePairingResult output = %q, want %q", got, want)
	}
}

// TestWriteWarnings checks that each warning is written on its own line and an
// empty list writes nothing.
func TestWriteWarnings(t *testing.T) {
	warnings := []*localpb.Warning{
		{Message: "fingerprint mismatch: directory card differs"},
		{Message: "second warning"},
	}
	var buf bytes.Buffer
	if err := WriteWarnings(&buf, warnings); err != nil {
		t.Fatalf("WriteWarnings: %v", err)
	}
	want := "warning: fingerprint mismatch: directory card differs\n" +
		"warning: second warning\n"
	if got := buf.String(); got != want {
		t.Errorf("WriteWarnings output = %q, want %q", got, want)
	}

	buf.Reset()
	if err := WriteWarnings(&buf, nil); err != nil {
		t.Fatalf("WriteWarnings: %v", err)
	}
	if buf.Len() != 0 {
		t.Errorf("WriteWarnings wrote %q, want nothing", buf.String())
	}
}

// TestFormatConfigValueKinds checks the rendering of every ConfigValue kind and
// the empty forms.
func TestFormatConfigValueKinds(t *testing.T) {
	cases := []struct {
		name  string
		value *localpb.ConfigValue
		want  string
	}{
		{"nil", nil, ""},
		{"no oneof", &localpb.ConfigValue{}, ""},
		{"string", &localpb.ConfigValue{Value: &localpb.ConfigValue_StringValue{StringValue: "hello"}}, "hello"},
		{"bool true", &localpb.ConfigValue{Value: &localpb.ConfigValue_BoolValue{BoolValue: true}}, "true"},
		{"bool false", &localpb.ConfigValue{Value: &localpb.ConfigValue_BoolValue{BoolValue: false}}, "false"},
		{"int", &localpb.ConfigValue{Value: &localpb.ConfigValue_IntValue{IntValue: -42}}, "-42"},
		{"duration", &localpb.ConfigValue{Value: &localpb.ConfigValue_DurationValue{DurationValue: durationpb.New(90 * time.Second)}}, "1m30s"},
		{"string list", &localpb.ConfigValue{Value: &localpb.ConfigValue_StringListValue{StringListValue: &localpb.StringList{Values: []string{"a", "b"}}}}, "[a, b]"},
		{"secret set", &localpb.ConfigValue{Value: &localpb.ConfigValue_SecretIsSet{SecretIsSet: true}}, "<set>"},
		{"secret unset", &localpb.ConfigValue{Value: &localpb.ConfigValue_SecretIsSet{SecretIsSet: false}}, "<unset>"},
	}
	for _, tc := range cases {
		if got := FormatConfigValue(tc.value); got != tc.want {
			t.Errorf("FormatConfigValue(%s) = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestWriteStatusShowsAvailableUpdate checks that an available version adds the
// update line after the sections.
func TestWriteStatusShowsAvailableUpdate(t *testing.T) {
	resp := &localpb.StatusResponse{
		CurrentVersion:   "0.1.1",
		AvailableVersion: "0.1.2",
	}
	var buf bytes.Buffer
	if err := WriteStatus(&buf, resp); err != nil {
		t.Fatalf("WriteStatus: %v", err)
	}
	want := "Agents: none\nPeers: none\n" +
		"Update: 0.1.2 available (current 0.1.1); run peers update\n"
	if got := buf.String(); got != want {
		t.Errorf("WriteStatus output = %q, want %q", got, want)
	}
}

// TestWriteStatusOmitsUpdateLineWithoutAvailableVersion checks that no update
// line is written without an available version.
func TestWriteStatusOmitsUpdateLineWithoutAvailableVersion(t *testing.T) {
	resp := &localpb.StatusResponse{CurrentVersion: "0.1.1"}
	var buf bytes.Buffer
	if err := WriteStatus(&buf, resp); err != nil {
		t.Fatalf("WriteStatus: %v", err)
	}
	if got := buf.String(); strings.Contains(got, "Update:") {
		t.Errorf("WriteStatus output = %q, want no update line", got)
	}
}

func escapeForTest(r rune) string {
	if r < 0x100 {
		return fmt.Sprintf(`\x%02x`, r)
	}
	return fmt.Sprintf(`\u%04x`, r)
}

func assertNoControlCharacters(t *testing.T, out string, chars ...rune) {
	t.Helper()
	for _, r := range chars {
		if strings.ContainsRune(out, r) {
			t.Errorf("output carries the raw character U+%04X: %q", r, out)
		}
		if want := escapeForTest(r); !strings.Contains(out, want) {
			t.Errorf("output is missing the escaped form %q: %q", want, out)
		}
	}
}

// TestTerminalOutputNeutralizesControlCharacters checks that control and format
// characters in strings from the Broker are escaped in the list, status, warning,
// degraded, audit and pairing key output.
func TestTerminalOutputNeutralizesControlCharacters(t *testing.T) {
	esc, bel, bidi := rune(0x1b), rune(0x07), rune(0x202e)
	evil := string(esc) + "]52;c;AAAA" + string(bel) + "x" + string(bidi) + "y"

	t.Run("WriteBrokerList address", func(t *testing.T) {
		brokers := []*localpb.BrokerInfo{{
			Alias:               "peer",
			AdvertisedAddresses: []*localpb.AdvertisedAddress{{Kind: "direct", Addr: evil}},
		}}
		var buf bytes.Buffer
		if err := WriteBrokerList(&buf, brokers, &localpb.LocalBrokerState{}); err != nil {
			t.Fatalf("WriteBrokerList: %v", err)
		}
		assertNoControlCharacters(t, buf.String(), esc, bel, bidi)
	})

	t.Run("WriteStatus peer status detail", func(t *testing.T) {
		resp := &localpb.StatusResponse{Peers: []*localpb.BrokerInfo{{
			Alias:        "peer",
			StatusDetail: "line\nnext" + string(esc),
		}}}
		var buf bytes.Buffer
		if err := WriteStatus(&buf, resp); err != nil {
			t.Fatalf("WriteStatus: %v", err)
		}
		out := buf.String()
		assertNoControlCharacters(t, out, esc)
		if want := "line next" + escapeForTest(esc); !strings.Contains(out, want) {
			t.Errorf("status detail = %q, want %q", out, want)
		}
	})

	t.Run("WriteWarnings", func(t *testing.T) {
		var buf bytes.Buffer
		if err := WriteWarnings(&buf, []*localpb.Warning{{Message: "warn " + evil}}); err != nil {
			t.Fatalf("WriteWarnings: %v", err)
		}
		assertNoControlCharacters(t, buf.String(), esc, bel, bidi)
	})

	t.Run("WriteDegraded", func(t *testing.T) {
		var buf bytes.Buffer
		if err := WriteDegraded(&buf, localpb.DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY, "reason "+evil, "fix "+string(bel)); err != nil {
			t.Fatalf("WriteDegraded: %v", err)
		}
		assertNoControlCharacters(t, buf.String(), esc, bel, bidi)
	})

	t.Run("WriteAuditEvent unquoted detail", func(t *testing.T) {
		var buf bytes.Buffer
		if err := WriteAuditEvent(&buf, &localpb.AuditEvent{Detail: "x" + string(esc)}); err != nil {
			t.Fatalf("WriteAuditEvent: %v", err)
		}
		out := buf.String()
		assertNoControlCharacters(t, out, esc)
		if want := "detail=x" + escapeForTest(esc); !strings.Contains(out, want) {
			t.Errorf("audit line = %q, want %q", out, want)
		}
	})

	t.Run("WriteStatus update line versions", func(t *testing.T) {
		resp := &localpb.StatusResponse{
			CurrentVersion:   "0.1.1" + string(esc),
			AvailableVersion: "0.1.2" + string(bel),
		}
		var buf bytes.Buffer
		if err := WriteStatus(&buf, resp); err != nil {
			t.Fatalf("WriteStatus: %v", err)
		}
		out := buf.String()
		if !strings.Contains(out, "Update: ") {
			t.Fatalf("output has no update line: %q", out)
		}
		assertNoControlCharacters(t, out, esc, bel)
	})

	t.Run("WritePairingKey address line", func(t *testing.T) {
		resp := &localpb.CreatePairingKeyResponse{Key: "k", ShownAddresses: []string{"host(10.0.0.1:1)" + string(esc)}}
		var buf bytes.Buffer
		if err := WritePairingKey(&buf, resp); err != nil {
			t.Fatalf("WritePairingKey: %v", err)
		}
		for _, line := range strings.Split(buf.String(), "\n") {
			if !strings.Contains(line, "host(10.0.0.1:1)") {
				continue
			}
			assertNoControlCharacters(t, line, esc)
			return
		}
		t.Fatal("WritePairingKey printed no address line")
	})
}

// TestSanitizeText checks how sanitizeText renders control, format and invalid
// UTF-8 input.
func TestSanitizeText(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"plain text", "host(10.0.0.1:1)", "host(10.0.0.1:1)"},
		{"multi-byte text", "节点一", "节点一"},
		{"tab newline carriage return", "a\tb\nc\rd", "a b c d"},
		{"C0 escape", "\x1b[31m", `\x1b[31m`},
		{"DEL", "a\x7fb", `a\x7fb`},
		{"C1 next line", "a\u0085b", `a\x85b`},
		{"bidi override", "a" + string(rune(0x202e)) + "b", "a" + "\x5c" + "u202eb"},
		{"format character above U+FFFF", "a" + string(rune(0xE0041)) + "b", `a\U000e0041b`},
		{"line separator", "a" + string(rune(0x2028)) + "b", "a b"},
		{"paragraph separator", "a" + string(rune(0x2029)) + "b", "a b"},
		{"invalid UTF-8", "a\xffb", `a\xffb`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := sanitizeText(tc.in); got != tc.want {
				t.Errorf("sanitizeText(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestSortBrokersBreaksAliasTiesByFingerprint checks that brokers sharing an
// alias are ordered by fingerprint.
func TestSortBrokersBreaksAliasTiesByFingerprint(t *testing.T) {
	low := bytes.Repeat([]byte{0x01}, 32)
	high := bytes.Repeat([]byte{0xff}, 32)
	brokers := []*localpb.BrokerInfo{
		{Alias: "host-a", Fingerprint: high},
		{Alias: "host-a", Fingerprint: low},
	}

	sorted := sortBrokers(brokers)

	if got, want := fingerprintHex(sorted[0].GetFingerprint()), fingerprintHex(low); got != want {
		t.Errorf("first fingerprint = %s, want the lower one %s", got, want)
	}
	if got, want := fingerprintHex(sorted[1].GetFingerprint()), fingerprintHex(high); got != want {
		t.Errorf("second fingerprint = %s, want the higher one %s", got, want)
	}
	if got, want := fingerprintHex(brokers[0].GetFingerprint()), fingerprintHex(high); got != want {
		t.Errorf("input order changed: first fingerprint = %s, want %s", got, want)
	}
}
