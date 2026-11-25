package termcmd

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// errWriter is the write failure every reporter must return to its caller.
var errWriter = errors.New("terminal write failed")

// failWriter accepts allow successful writes, then fails every write with
// errWriter. A negative allow lets every write through.
type failWriter struct {
	allow int
	calls int
}

func (w *failWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.allow >= 0 && w.calls > w.allow {
		return 0, errWriter
	}
	return len(p), nil
}

// TestWriteErrorsArePropagated returns the writer's error from every write each
// reporter makes.
func TestWriteErrorsArePropagated(t *testing.T) {
	status := &localpb.StatusResponse{
		Agents:           []*localpb.AgentStatusEntry{{Address: "alice@host-a", Account: "a", Status: localpb.AgentStatus_AGENT_STATUS_IDLE}},
		Peers:            []*localpb.BrokerInfo{{Alias: "peer1"}},
		AvailableVersion: "0.2.0",
		CurrentVersion:   "0.1.0",
	}
	brokerListLocal := &localpb.LocalBrokerState{}
	brokers := []*localpb.BrokerInfo{{Alias: "peer1"}}
	auditEvent := &localpb.AuditEvent{Event: localpb.AuditEventKind_AUDIT_EVENT_KIND_CONFIG_SET, Actor: "a"}
	configEntries := []*localpb.ConfigEntry{
		{Key: "a", Value: &localpb.ConfigValue{Value: &localpb.ConfigValue_StringValue{StringValue: "1"}}},
		{Key: "b", Value: &localpb.ConfigValue{Value: &localpb.ConfigValue_BoolValue{BoolValue: true}}},
	}
	pairingKey := &localpb.CreatePairingKeyResponse{
		Key:            "one-time",
		ExpiresAt:      timestamppb.New(time.Unix(0, 0)),
		ShownAddresses: []string{"host-a:1234"},
	}

	reporters := []struct {
		name string
		fn   func(io.Writer) error
	}{
		{"WriteStatus", func(w io.Writer) error { return WriteStatus(w, status) }},
		{"WriteStatusEmpty", func(w io.Writer) error { return WriteStatus(w, &localpb.StatusResponse{}) }},
		{"WriteStatusState", func(w io.Writer) error {
			return WriteStatus(w, &localpb.StatusResponse{
				Degraded: &localpb.DegradedState{Level: localpb.DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY, Reason: "r", Fix: "f"},
				Offline:  true,
			})
		}},
		{"WriteDegraded", func(w io.Writer) error {
			return WriteDegraded(w, localpb.DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY, "r", "f")
		}},
		{"WriteAuditEvent", func(w io.Writer) error { return WriteAuditEvent(w, auditEvent) }},
		{"WriteConfigEntries", func(w io.Writer) error { return WriteConfigEntries(w, configEntries) }},
		{"WriteConfigChange", func(w io.Writer) error {
			return WriteConfigChange(w, "k", configEntries[0].GetValue(), configEntries[1].GetValue())
		}},
		{"WritePairingKey", func(w io.Writer) error { return WritePairingKey(w, pairingKey) }},
		{"WriteBrokerList", func(w io.Writer) error { return WriteBrokerList(w, brokers, brokerListLocal) }},
		{"WritePairingResult", func(w io.Writer) error {
			return WritePairingResult(w, &localpb.ConnectResponse{Alias: "peer1"})
		}},
		{"WriteWarnings", func(w io.Writer) error {
			return WriteWarnings(w, []*localpb.Warning{{Message: "careful"}, {Message: "again"}})
		}},
	}

	for _, reporter := range reporters {
		t.Run(reporter.name, func(t *testing.T) {
			var count failWriter
			count.allow = -1
			if err := reporter.fn(&count); err != nil {
				t.Fatalf("unlimited writes failed: %v", err)
			}
			if count.calls == 0 {
				t.Fatal("the reporter made no write")
			}
			for allow := 0; allow < count.calls; allow++ {
				w := &failWriter{allow: allow}
				err := reporter.fn(w)
				if !errors.Is(err, errWriter) {
					t.Fatalf("with writes 1..%d allowed and write %d failing, error = %v, want %v",
						allow, allow+1, err, errWriter)
				}
			}
		})
	}
}

// TestWriteBrokerListDiagnosticsOnlyWritesNoTable writes only the state lines for
// a diagnostics-only Broker.
func TestWriteBrokerListDiagnosticsOnlyWritesNoTable(t *testing.T) {
	local := &localpb.LocalBrokerState{Degraded: &localpb.DegradedState{
		Level:  localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY,
		Reason: "config parse failed",
		Fix:    "fix the config",
	}}
	var buf bytes.Buffer
	if err := WriteBrokerList(&buf, []*localpb.BrokerInfo{{Alias: "peer1"}}, local); err != nil {
		t.Fatalf("WriteBrokerList: %v", err)
	}
	if strings.Contains(buf.String(), "Brokers") || strings.Contains(buf.String(), "peer1") {
		t.Fatalf("output = %q, want only the diagnostics lines", buf.String())
	}
	if !strings.Contains(buf.String(), "degraded (diagnostics only)") {
		t.Fatalf("output = %q, want the degraded line", buf.String())
	}
}

// TestStatusEntriesSortsByAgentIDWithinAddress orders agents by agent id inside
// one address.
func TestStatusEntriesSortsByAgentIDWithinAddress(t *testing.T) {
	resp := &localpb.StatusResponse{Agents: []*localpb.AgentStatusEntry{
		{Address: "same@host", AgentId: "b"},
		{Address: "same@host", AgentId: "a"},
	}}
	agents, _ := statusEntries(resp)
	if len(agents) != 2 || agents[0].GetAgentId() != "a" || agents[1].GetAgentId() != "b" {
		t.Fatalf("statusEntries order = %v, want agent ids a then b", agents)
	}
}

// TestPeerSortKeyFallsBackToFingerprint uses the fingerprint when a Broker has
// no alias.
func TestPeerSortKeyFallsBackToFingerprint(t *testing.T) {
	p := &localpb.BrokerInfo{Fingerprint: []byte{0xab, 0xcd}}
	if got := peerSortKey(p); got != "abcd" {
		t.Fatalf("peerSortKey = %q, want abcd", got)
	}
	if got := peerSortKey(&localpb.BrokerInfo{Alias: "z"}); got != "z" {
		t.Fatalf("peerSortKey(alias) = %q, want z", got)
	}
}

// TestFormatAddressesVariants joins the kind and address of every listed
// address.
func TestFormatAddressesVariants(t *testing.T) {
	tests := []struct {
		name string
		in   []*localpb.AdvertisedAddress
		want string
	}{
		{"none", nil, "-"},
		{"both empty", []*localpb.AdvertisedAddress{{}}, "-"},
		{"address only", []*localpb.AdvertisedAddress{{Addr: "host:1"}}, "host:1"},
		{"kind only", []*localpb.AdvertisedAddress{{Kind: "ssh"}}, "ssh"},
		{"kind and address", []*localpb.AdvertisedAddress{{Kind: "ssh", Addr: "host:1"}}, "ssh host:1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatAddresses(tt.in); got != tt.want {
				t.Fatalf("formatAddresses = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestFormatPeerRefVariants joins the alias and fingerprint of a peer reference.
func TestFormatPeerRefVariants(t *testing.T) {
	tests := []struct {
		name string
		in   *localpb.PeerRef
		want string
	}{
		{"nil", nil, ""},
		{"alias and fingerprint", &localpb.PeerRef{Alias: "a", Fingerprint: []byte{0x01}}, "a(01)"},
		{"alias only", &localpb.PeerRef{Alias: "a"}, "a"},
		{"fingerprint only", &localpb.PeerRef{Fingerprint: []byte{0x02}}, "02"},
		{"neither", &localpb.PeerRef{}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatPeerRef(tt.in); got != tt.want {
				t.Fatalf("formatPeerRef = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestEnumLabelUnknownValue prints an unknown value as its number and a known one
// without its prefix.
func TestEnumLabelUnknownValue(t *testing.T) {
	if got := enumLabel(map[int32]string{}, "X_", 7); got != "7" {
		t.Fatalf("enumLabel(unknown) = %q, want 7", got)
	}
	if got := enumLabel(map[int32]string{1: "X_ONE"}, "X_", 1); got != "one" {
		t.Fatalf("enumLabel(known) = %q, want one", got)
	}
}

// TestAgentStatusNameUnspecified renders an unspecified agent status as the empty
// cell.
func TestAgentStatusNameUnspecified(t *testing.T) {
	if got := agentStatusName(localpb.AgentStatus_AGENT_STATUS_UNSPECIFIED); got != emptyCell {
		t.Fatalf("agentStatusName(unspecified) = %q, want %q", got, emptyCell)
	}
}

// TestWriteConfigChangeRendersDuration renders a duration value on the change
// line.
func TestWriteConfigChangeRendersDuration(t *testing.T) {
	var buf bytes.Buffer
	old := &localpb.ConfigValue{Value: &localpb.ConfigValue_DurationValue{DurationValue: durationpb.New(90 * time.Minute)}}
	newValue := &localpb.ConfigValue{Value: &localpb.ConfigValue_StringValue{StringValue: "x"}}
	if err := WriteConfigChange(&buf, "k", old, newValue); err != nil {
		t.Fatalf("WriteConfigChange: %v", err)
	}
	if got, want := buf.String(), "k: 1h30m0s -> x\n"; got != want {
		t.Fatalf("WriteConfigChange = %q, want %q", got, want)
	}
}
