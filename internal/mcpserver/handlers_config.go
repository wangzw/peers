package mcpserver

import (
	"context"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// GetConfig forwards get_config to the Broker and renders every returned value
// as text. Reading the configuration needs read permission on the Broker's
// config file but no agent identity, so a session whose registration has not
// completed may still call it.
func (h *handlers) GetConfig(ctx context.Context, in GetConfigIn) (GetConfigOut, error) {
	st, err := h.preflight("get_config", callGeneric, false)
	if err != nil {
		return GetConfigOut{}, err
	}
	ctx, cancel := withRPCTimeout(ctx, h.rpcTimeout, handlerRPCTimeout)
	defer cancel()
	resp, err := st.Client.GetConfig(ctx, &localpb.GetConfigRequest{Key: in.Key})
	if err != nil {
		return GetConfigOut{}, rpcError("get_config", callGeneric, permRead, err)
	}
	entries := make([]ConfigEntryOut, 0, len(resp.GetEntries()))
	for _, e := range resp.GetEntries() {
		entries = append(entries, ConfigEntryOut{Key: e.GetKey(), Value: renderValue(e.GetValue())})
	}
	return GetConfigOut{Entries: entries}, nil
}

// SetConfig forwards set_config to the Broker. Value is the raw text the model
// gave; the Broker parses it according to the key's type. The agent id is sent
// only for the audit record and may be empty.
func (h *handlers) SetConfig(ctx context.Context, in SetConfigIn) (SetConfigOut, error) {
	st, err := h.preflight("set_config", callGeneric, false)
	if err != nil {
		return SetConfigOut{}, err
	}
	ctx, cancel := withRPCTimeout(ctx, h.rpcTimeout, handlerRPCTimeout)
	defer cancel()
	resp, err := st.Client.SetConfig(ctx, &localpb.SetConfigRequest{
		Key:     in.Key,
		Value:   in.Value,
		AgentId: st.AgentID,
	})
	if err != nil {
		return SetConfigOut{}, rpcError("set_config", callGeneric, permWrite, err)
	}
	return SetConfigOut{
		Key: in.Key,
		Old: renderValue(resp.GetOldValue()),
		New: renderValue(resp.GetNewValue()),
	}, nil
}

// SetBrokerOnline forwards set_broker_online to the Broker as a change of
// broker.offline. The agent id is sent only for the audit record. Changed
// compares the values the Broker reports before and after the write.
func (h *handlers) SetBrokerOnline(ctx context.Context, in SetBrokerOnlineIn) (SetBrokerOnlineOut, error) {
	st, err := h.preflight("set_broker_online", callGeneric, false)
	if err != nil {
		return SetBrokerOnlineOut{}, err
	}
	// broker.offline is the opposite of the tool's online flag.
	value := "true"
	if in.Online {
		value = "false"
	}
	ctx, cancel := withRPCTimeout(ctx, h.rpcTimeout, handlerRPCTimeout)
	defer cancel()
	resp, err := st.Client.SetConfig(ctx, &localpb.SetConfigRequest{
		Key:     "broker.offline",
		Value:   value,
		AgentId: st.AgentID,
	})
	if err != nil {
		return SetBrokerOnlineOut{}, rpcError("set_broker_online", callGeneric, permWrite, err)
	}
	return SetBrokerOnlineOut{
		Online:  in.Online,
		Changed: resp.GetOldValue().GetBoolValue() != resp.GetNewValue().GetBoolValue(),
	}, nil
}

// renderValue renders one configuration value the way the config file writes
// it. A nil value and a value whose oneof is unset render as the empty string.
func renderValue(v *localpb.ConfigValue) string {
	if v == nil {
		return ""
	}
	switch x := v.GetValue().(type) {
	case *localpb.ConfigValue_StringValue:
		return x.StringValue
	case *localpb.ConfigValue_BoolValue:
		if x.BoolValue {
			return "true"
		}
		return "false"
	case *localpb.ConfigValue_IntValue:
		return strconv.FormatInt(x.IntValue, 10)
	case *localpb.ConfigValue_DurationValue:
		if x.DurationValue == nil {
			return ""
		}
		return renderDuration(x.DurationValue.AsDuration())
	case *localpb.ConfigValue_StringListValue:
		if x.StringListValue == nil {
			return "[]"
		}
		return renderStringList(x.StringListValue.GetValues())
	case *localpb.ConfigValue_SecretIsSet:
		if x.SecretIsSet {
			return "(set)"
		}
		return "(not set)"
	default:
		return ""
	}
}

// renderDuration drops trailing zero-valued units from time.Duration.String():
// "10m0s" becomes "10m", "1h0m0s" becomes "1h", "1h30m0s" becomes "1h30m". The
// zero must stand for the whole unit, so the "0" in "10s" or "1h30m" is kept.
func renderDuration(d time.Duration) string {
	s := d.String()
	s = trimZeroUnit(s, "s", "hmsµ")
	s = trimZeroUnit(s, "m", "h")
	return s
}

// trimZeroUnit removes a trailing "<unit>" whose value is exactly zero, when
// unitLetters names the unit immediately before it. A preceding digit means the
// zero is part of a multi-digit number and is kept.
func trimZeroUnit(s, unit, unitLetters string) string {
	suffix := "0" + unit
	if !strings.HasSuffix(s, suffix) {
		return s
	}
	i := len(s) - len(suffix)
	if i == 0 {
		return s // the whole duration is zero, as in "0s"
	}
	if strings.ContainsRune(unitLetters, rune(s[i-1])) {
		return s[:i]
	}
	return s
}

// renderStringList renders a list as a YAML flow sequence: "[a, b]", or "[]"
// when empty. Elements containing a comma, a bracket, whitespace, or nothing at
// all are double-quoted.
func renderStringList(values []string) string {
	if len(values) == 0 {
		return "[]"
	}
	parts := make([]string, len(values))
	for i, v := range values {
		if needsYAMLQuote(v) {
			parts[i] = `"` + v + `"`
		} else {
			parts[i] = v
		}
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func needsYAMLQuote(s string) bool {
	if s == "" {
		return true
	}
	if strings.ContainsAny(s, ",[]") {
		return true
	}
	return strings.ContainsFunc(s, unicode.IsSpace)
}
