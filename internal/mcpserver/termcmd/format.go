package termcmd

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/mcpserver/pairingprompt"
)

// Table layout shared by both status sections.
const (
	tableMinWidth = 0
	tableTabWidth = 8
	tablePadding  = 2
)

const emptyCell = "-"

// WriteStatus writes resp as the two sections shown by "peers service status":
// a table of registered agents followed by a table of peers. A Broker that runs
// degraded or offline gets status lines before the sections. A degraded Broker
// that runs diagnostics only prints just those lines and no sections.
//
// Each section is sorted and its columns are aligned with text/tabwriter. An
// empty section is written as "Agents: none" or "Peers: none".
func WriteStatus(w io.Writer, resp *localpb.StatusResponse) error {
	if degraded := resp.GetDegraded(); degraded != nil {
		if err := WriteDegraded(w, degraded.GetLevel(), degraded.GetReason(), degraded.GetFix()); err != nil {
			return err
		}
		if degraded.GetLevel() == localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY {
			return nil
		}
	}
	if resp.GetOffline() {
		if _, err := io.WriteString(w, "Broker: offline; messages from other hosts are refused. Run peers service online to accept them.\n"); err != nil {
			return err
		}
	}

	agents, peers := statusEntries(resp)

	if len(agents) == 0 {
		if _, err := io.WriteString(w, "Agents: none\n"); err != nil {
			return err
		}
	} else {
		if _, err := fmt.Fprintf(w, "Agents (%d):\n", len(agents)); err != nil {
			return err
		}
		rows := make([][]string, 0, len(agents))
		for _, a := range agents {
			rows = append(rows, []string{
				cell(a.GetAddress()),
				cell(a.GetAccount()),
				agentStatusName(a.GetStatus()),
				cell(strings.Join(sortedTopics(a.GetTopics()), ",")),
				cell(a.GetPreviousName()),
			})
		}
		header := []string{"ADDRESS", "ACCOUNT", "STATUS", "TOPICS", "PREVIOUS NAME"}
		if err := writeTable(w, header, rows); err != nil {
			return err
		}
	}

	if len(peers) == 0 {
		if _, err := io.WriteString(w, "Peers: none\n"); err != nil {
			return err
		}
	} else {
		if _, err := fmt.Fprintf(w, "Peers (%d):\n", len(peers)); err != nil {
			return err
		}
		rows := make([][]string, 0, len(peers))
		for _, p := range peers {
			rows = append(rows, []string{
				cell(p.GetAlias()),
				cell(fingerprintHex(p.GetFingerprint())),
				peerRoleName(p.GetRole()),
				brokerStatusName(p.GetStatus()),
				linkStateName(p.GetLinkState()),
				formatAddresses(p.GetAdvertisedAddresses()),
				cell(sanitizeDetail(p.GetStatusDetail())),
			})
		}
		header := []string{"ALIAS", "FINGERPRINT", "ROLE", "STATUS", "LINK", "ADDRESSES", "DETAIL"}
		if err := writeTable(w, header, rows); err != nil {
			return err
		}
	}

	if available := resp.GetAvailableVersion(); available != "" {
		if _, err := fmt.Fprintf(w, "Update: %s available (current %s); run peers update\n",
			available, resp.GetCurrentVersion()); err != nil {
			return err
		}
	}

	return nil
}

// WriteDegraded writes the two lines that say why the Broker runs degraded:
// the level and cause on the first line and the repair on the second. It is
// the only place these lines are built, so status, broker list and the service
// commands print the same text.
func WriteDegraded(w io.Writer, level localpb.DegradedLevel, reason, fix string) error {
	if _, err := fmt.Fprintf(w, "Broker: degraded (%s): %s\n",
		localpb.DegradedLevelLabel(level), reason); err != nil {
		return err
	}
	_, err := fmt.Fprintf(w, "Fix: %s\n", fix)
	return err
}

// WriteAuditEvent writes ev as one line: the event time, the event name and
// the non-empty fields, in a fixed order. Events arrive in time order from the
// server and are written in arrival order.
func WriteAuditEvent(w io.Writer, ev *localpb.AuditEvent) error {
	if ev == nil {
		ev = &localpb.AuditEvent{}
	}

	var b strings.Builder
	b.WriteString(auditTime(ev.GetTime()))
	b.WriteByte(' ')
	b.WriteString(auditEventName(ev.GetEvent()))

	appendField(&b, "actor", ev.GetActor())
	appendField(&b, "account", ev.GetAccount())
	if peer := formatPeerRef(ev.GetPeer()); peer != "" {
		appendField(&b, "peer", peer)
	}
	appendField(&b, "key", ev.GetKey())
	if v := FormatConfigValue(ev.GetOldValue()); v != "" {
		b.WriteString(" old=")
		b.WriteString(quoteValue(v))
	}
	if v := FormatConfigValue(ev.GetNewValue()); v != "" {
		b.WriteString(" new=")
		b.WriteString(quoteValue(v))
	}
	if ev.GetCount() != 0 {
		b.WriteString(" count=")
		b.WriteString(strconv.FormatUint(ev.GetCount(), 10))
	}
	appendField(&b, "detail", ev.GetDetail())
	b.WriteByte('\n')

	_, err := io.WriteString(w, b.String())
	return err
}

// WriteConfigEntries writes one line per configuration entry, sorted by key,
// as "<key> = <value>". A secret entry shows only whether it is set.
func WriteConfigEntries(w io.Writer, entries []*localpb.ConfigEntry) error {
	sorted := append([]*localpb.ConfigEntry(nil), entries...)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].GetKey() < sorted[j].GetKey()
	})
	for _, e := range sorted {
		if _, err := fmt.Fprintf(w, "%s = %s\n", e.GetKey(), configValueText(e.GetValue())); err != nil {
			return err
		}
	}
	return nil
}

// WriteConfigChange writes the line shown after one configuration change:
// "<key>: <old> -> <new>".
func WriteConfigChange(w io.Writer, key string, oldValue, newValue *localpb.ConfigValue) error {
	_, err := fmt.Fprintf(w, "%s: %s -> %s\n", key, configValueText(oldValue), configValueText(newValue))
	return err
}

// configValueText renders one configuration value for the terminal. A secret
// value shows "set" or "unset" and never its contents; every other value uses
// FormatConfigValue and the empty convention.
func configValueText(v *localpb.ConfigValue) string {
	if secret, ok := v.GetValue().(*localpb.ConfigValue_SecretIsSet); ok {
		if secret.SecretIsSet {
			return "set"
		}
		return "unset"
	}
	return cell(FormatConfigValue(v))
}

// WritePairingKey writes a new one-time pairing key: the key, its expiry as
// RFC 3339 in UTC, the addresses the other Broker can dial (each indented by two
// spaces), and then the paste-ready connect prompt built by the same function
// the MCP tool uses. A Broker with no address shows no prompt.
func WritePairingKey(w io.Writer, resp *localpb.CreatePairingKeyResponse) error {
	if resp == nil {
		resp = &localpb.CreatePairingKeyResponse{}
	}
	if _, err := fmt.Fprintf(w, "Key: %s\n", cell(resp.GetKey())); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Expires: %s\n", auditTime(resp.GetExpiresAt())); err != nil {
		return err
	}
	if _, err := io.WriteString(w, "Addresses:\n"); err != nil {
		return err
	}
	for _, addr := range resp.GetShownAddresses() {
		if _, err := fmt.Fprintf(w, "  %s\n", addr); err != nil {
			return err
		}
	}
	if prompt := pairingprompt.ConnectPrompt(resp.GetShownAddresses(), resp.GetKey()); prompt != "" {
		if _, err := fmt.Fprintf(w, "\n%s\n", prompt); err != nil {
			return err
		}
	}
	return nil
}

// WriteBrokerList writes the Brokers named by brokers as a table sorted by
// alias. A Broker shows whether it is a peer, its status, which side dialed and
// its advertised addresses. With no Brokers it writes "Brokers: none".
//
// local is the state of the Broker that answered; every peers Broker reports
// it, so a nil value is a protocol violation and the command fails without
// writing anything. The status lines it produces come before the table, exactly
// as WriteStatus writes them.
func WriteBrokerList(w io.Writer, brokers []*localpb.BrokerInfo, local *localpb.LocalBrokerState) error {
	if local == nil {
		return errors.New("the Broker did not report its local state")
	}
	if degraded := local.GetDegraded(); degraded != nil {
		if err := WriteDegraded(w, degraded.GetLevel(), degraded.GetReason(), degraded.GetFix()); err != nil {
			return err
		}
		if degraded.GetLevel() == localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY {
			return nil
		}
	}
	if local.GetOffline() {
		if _, err := io.WriteString(w, "Broker: offline; messages from other hosts are refused. Run peers service online to accept them.\n"); err != nil {
			return err
		}
	}

	if len(brokers) == 0 {
		_, err := io.WriteString(w, "Brokers: none\n")
		return err
	}

	sorted := append([]*localpb.BrokerInfo(nil), brokers...)
	sort.SliceStable(sorted, func(i, j int) bool {
		left, right := peerSortKey(sorted[i]), peerSortKey(sorted[j])
		if left != right {
			return left < right
		}
		return fingerprintHex(sorted[i].GetFingerprint()) < fingerprintHex(sorted[j].GetFingerprint())
	})

	rows := make([][]string, 0, len(sorted))
	for _, b := range sorted {
		rows = append(rows, []string{
			cell(b.GetAlias()),
			cell(fingerprintHex(b.GetFingerprint())),
			yesNo(b.GetIsPeer()),
			brokerStatusName(b.GetStatus()),
			peerRoleName(b.GetRole()),
			formatAddresses(b.GetAdvertisedAddresses()),
		})
	}
	header := []string{"ALIAS", "FINGERPRINT", "PEER", "STATUS", "DIRECTION", "ADDRESSES"}
	return writeTable(w, header, rows)
}

// WritePairingResult writes the line shown after a successful pairing:
// "Paired with <alias> (<fingerprint>)".
func WritePairingResult(w io.Writer, resp *localpb.ConnectResponse) error {
	if resp == nil {
		resp = &localpb.ConnectResponse{}
	}
	_, err := fmt.Fprintf(w, "Paired with %s (%s)\n",
		cell(resp.GetAlias()), cell(fingerprintHex(resp.GetFingerprint())))
	return err
}

// WriteWarnings writes each warning as one line prefixed with "warning: ".
func WriteWarnings(w io.Writer, warnings []*localpb.Warning) error {
	for _, warning := range warnings {
		if _, err := fmt.Fprintf(w, "warning: %s\n", warning.GetMessage()); err != nil {
			return err
		}
	}
	return nil
}

// FormatConfigValue renders one configuration value for the terminal. A nil
// value and a ConfigValue with no oneof member set render as the empty string,
// which makes the caller omit the field.
func FormatConfigValue(v *localpb.ConfigValue) string {
	if v == nil {
		return ""
	}
	switch value := v.GetValue().(type) {
	case *localpb.ConfigValue_StringValue:
		return value.StringValue
	case *localpb.ConfigValue_BoolValue:
		return strconv.FormatBool(value.BoolValue)
	case *localpb.ConfigValue_IntValue:
		return strconv.FormatInt(value.IntValue, 10)
	case *localpb.ConfigValue_DurationValue:
		if value.DurationValue == nil {
			return ""
		}
		return value.DurationValue.AsDuration().String()
	case *localpb.ConfigValue_StringListValue:
		if value.StringListValue == nil {
			return ""
		}
		return "[" + strings.Join(value.StringListValue.GetValues(), ", ") + "]"
	case *localpb.ConfigValue_SecretIsSet:
		if value.SecretIsSet {
			return "<set>"
		}
		return "<unset>"
	default:
		return ""
	}
}

func statusEntries(resp *localpb.StatusResponse) ([]*localpb.AgentStatusEntry, []*localpb.BrokerInfo) {
	if resp == nil {
		return nil, nil
	}
	agents := append([]*localpb.AgentStatusEntry(nil), resp.GetAgents()...)
	peers := append([]*localpb.BrokerInfo(nil), resp.GetPeers()...)

	sort.SliceStable(agents, func(i, j int) bool {
		if agents[i].GetAddress() != agents[j].GetAddress() {
			return agents[i].GetAddress() < agents[j].GetAddress()
		}
		return agents[i].GetAgentId() < agents[j].GetAgentId()
	})
	sort.SliceStable(peers, func(i, j int) bool {
		left, right := peerSortKey(peers[i]), peerSortKey(peers[j])
		if left != right {
			return left < right
		}
		return fingerprintHex(peers[i].GetFingerprint()) < fingerprintHex(peers[j].GetFingerprint())
	})
	return agents, peers
}

// peerSortKey orders peers by alias. A peer without a local alias sorts by its
// fingerprint, matching the "-" that takes the alias column.
func peerSortKey(p *localpb.BrokerInfo) string {
	if alias := p.GetAlias(); alias != "" {
		return alias
	}
	return fingerprintHex(p.GetFingerprint())
}

func writeTable(w io.Writer, header []string, rows [][]string) error {
	tw := tabwriter.NewWriter(w, tableMinWidth, tableTabWidth, tablePadding, ' ', 0)
	if _, err := fmt.Fprintf(tw, "  %s\n", strings.Join(header, "\t")); err != nil {
		return err
	}
	for _, row := range rows {
		if _, err := fmt.Fprintf(tw, "  %s\n", strings.Join(row, "\t")); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func cell(v string) string {
	if v == "" {
		return emptyCell
	}
	return v
}

func yesNo(v bool) string {
	if v {
		return "yes"
	}
	return "no"
}

func sortedTopics(topics []string) []string {
	out := append([]string(nil), topics...)
	sort.Strings(out)
	return out
}

func fingerprintHex(fp []byte) string {
	return hex.EncodeToString(fp)
}

func formatAddresses(addresses []*localpb.AdvertisedAddress) string {
	parts := make([]string, 0, len(addresses))
	for _, a := range addresses {
		if a == nil {
			continue
		}
		kind, addr := a.GetKind(), a.GetAddr()
		switch {
		case kind == "" && addr == "":
			continue
		case kind == "":
			parts = append(parts, addr)
		case addr == "":
			parts = append(parts, kind)
		default:
			parts = append(parts, kind+" "+addr)
		}
	}
	if len(parts) == 0 {
		return emptyCell
	}
	return strings.Join(parts, ", ")
}

func sanitizeDetail(detail string) string {
	return strings.NewReplacer("\t", " ", "\n", " ", "\r", " ").Replace(detail)
}

func formatPeerRef(p *localpb.PeerRef) string {
	if p == nil {
		return ""
	}
	alias := p.GetAlias()
	fp := fingerprintHex(p.GetFingerprint())
	switch {
	case alias != "" && fp != "":
		return alias + "(" + fp + ")"
	case alias != "":
		return alias
	default:
		return fp
	}
}

func appendField(b *strings.Builder, name, value string) {
	if value == "" {
		return
	}
	b.WriteByte(' ')
	b.WriteString(name)
	b.WriteByte('=')
	b.WriteString(quoteValue(value))
}

// quoteValue quotes a field value that would otherwise be ambiguous on one
// line: an empty value and values containing a space, a double quote, an
// equals sign, or a line break.
func quoteValue(v string) string {
	if v == "" || strings.ContainsAny(v, " \"=\n\r\t") {
		return strconv.Quote(v)
	}
	return v
}

func auditTime(ts *timestamppb.Timestamp) string {
	if ts == nil {
		return emptyCell
	}
	return ts.AsTime().UTC().Format(time.RFC3339)
}

// enumLabel turns a generated proto enum into the lowercase snake case name
// used on the terminal. The prefix is stripped; a value the generated map does
// not know is rendered as its number.
func enumLabel(names map[int32]string, prefix string, v int32) string {
	if name, ok := names[v]; ok {
		return strings.ToLower(strings.TrimPrefix(name, prefix))
	}
	return strconv.Itoa(int(v))
}

func agentStatusName(v localpb.AgentStatus) string {
	if v == localpb.AgentStatus_AGENT_STATUS_UNSPECIFIED {
		return emptyCell
	}
	return enumLabel(localpb.AgentStatus_name, "AGENT_STATUS_", int32(v))
}

func brokerStatusName(v localpb.BrokerStatus) string {
	if v == localpb.BrokerStatus_BROKER_STATUS_UNSPECIFIED {
		return emptyCell
	}
	return enumLabel(localpb.BrokerStatus_name, "BROKER_STATUS_", int32(v))
}

func peerRoleName(v localpb.PeerRole) string {
	if v == localpb.PeerRole_PEER_ROLE_UNSPECIFIED {
		return emptyCell
	}
	return enumLabel(localpb.PeerRole_name, "PEER_ROLE_", int32(v))
}

func linkStateName(v localpb.LinkState) string {
	if v == localpb.LinkState_LINK_STATE_UNSPECIFIED {
		return emptyCell
	}
	return enumLabel(localpb.LinkState_name, "LINK_STATE_", int32(v))
}

func auditEventName(v localpb.AuditEventKind) string {
	return enumLabel(localpb.AuditEventKind_name, "AUDIT_EVENT_KIND_", int32(v))
}
