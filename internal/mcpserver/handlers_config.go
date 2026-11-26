package mcpserver

import (
	"context"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// GetConfig forwards get_config to the Broker and renders every value as text;
// it needs read permission but no agent identity.
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

// SetConfig forwards set_config to the Broker; Value is the raw text the model
// gave, which the Broker parses according to the key's type.
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
// broker.offline; Changed compares the values the Broker reports before and
// after the write.
func (h *handlers) SetBrokerOnline(ctx context.Context, in SetBrokerOnlineIn) (SetBrokerOnlineOut, error) {
	st, err := h.preflight("set_broker_online", callGeneric, false)
	if err != nil {
		return SetBrokerOnlineOut{}, err
	}
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

// renderDuration drops trailing zero-valued units from time.Duration.String();
// a zero that is part of a multi-digit number, as in "10s", is kept.
func renderDuration(d time.Duration) string {
	s := d.String()
	s = trimZeroUnit(s, "s", "hmsµ")
	s = trimZeroUnit(s, "m", "h")
	return s
}

// trimZeroUnit removes a trailing zero-valued unit only when unitLetters names
// the unit immediately before it, so the zero in "10s" or "1h30m" is kept.
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

// renderStringList renders values as a YAML flow sequence, double-quoting an
// element that is empty or contains a comma, a bracket or whitespace.
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
