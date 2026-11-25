package broker

import (
	"errors"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func toConfigValue(v config.Value) *localpb.ConfigValue {
	switch v.Kind {
	case config.KindString:
		return &localpb.ConfigValue{
			Value: &localpb.ConfigValue_StringValue{StringValue: v.String},
		}
	case config.KindBool:
		return &localpb.ConfigValue{
			Value: &localpb.ConfigValue_BoolValue{BoolValue: v.Bool},
		}
	case config.KindInt:
		return &localpb.ConfigValue{
			Value: &localpb.ConfigValue_IntValue{IntValue: v.Int},
		}
	case config.KindDuration:
		return &localpb.ConfigValue{
			Value: &localpb.ConfigValue_DurationValue{DurationValue: durationpb.New(v.Duration)},
		}
	case config.KindStringList:
		values := make([]string, len(v.Strings))
		copy(values, v.Strings)
		return &localpb.ConfigValue{
			Value: &localpb.ConfigValue_StringListValue{
				StringListValue: &localpb.StringList{Values: values},
			},
		}
	case config.KindSecret:
		return &localpb.ConfigValue{
			Value: &localpb.ConfigValue_SecretIsSet{SecretIsSet: v.SecretIsSet},
		}
	default:
		return nil
	}
}

func toAuditEventKind(k config.EventKind) localpb.AuditEventKind {
	switch k {
	case config.EventConfigSet:
		return localpb.AuditEventKind_AUDIT_EVENT_KIND_CONFIG_SET
	case config.EventConfigFileChanged:
		return localpb.AuditEventKind_AUDIT_EVENT_KIND_CONFIG_FILE_CHANGED
	case config.EventPairingKeyCreated:
		return localpb.AuditEventKind_AUDIT_EVENT_KIND_PAIRING_KEY_CREATED
	case config.EventPairingSucceeded:
		return localpb.AuditEventKind_AUDIT_EVENT_KIND_PAIRING_SUCCEEDED
	case config.EventPairingFailed:
		return localpb.AuditEventKind_AUDIT_EVENT_KIND_PAIRING_FAILED
	case config.EventLinkUp:
		return localpb.AuditEventKind_AUDIT_EVENT_KIND_LINK_UP
	case config.EventLinkDown:
		return localpb.AuditEventKind_AUDIT_EVENT_KIND_LINK_DOWN
	case config.EventUnpaired:
		return localpb.AuditEventKind_AUDIT_EVENT_KIND_UNPAIRED
	case config.EventConnectionRejected:
		return localpb.AuditEventKind_AUDIT_EVENT_KIND_CONNECTION_REJECTED
	case config.EventRejectedByPeer:
		return localpb.AuditEventKind_AUDIT_EVENT_KIND_REJECTED_BY_PEER
	case config.EventLocalAliasAssigned:
		return localpb.AuditEventKind_AUDIT_EVENT_KIND_LOCAL_ALIAS_ASSIGNED
	case config.EventConfigRejected:
		return localpb.AuditEventKind_AUDIT_EVENT_KIND_CONFIG_REJECTED
	case config.EventUpdateApplied:
		return localpb.AuditEventKind_AUDIT_EVENT_KIND_UPDATE_APPLIED
	case config.EventUpdateFailed:
		return localpb.AuditEventKind_AUDIT_EVENT_KIND_UPDATE_FAILED
	default:
		return localpb.AuditEventKind_AUDIT_EVENT_KIND_UNSPECIFIED
	}
}

func toAuditEvent(e config.Event) *localpb.AuditEvent {
	out := &localpb.AuditEvent{
		Time:    timestamppb.New(e.Time),
		Event:   toAuditEventKind(e.Event),
		Actor:   e.Actor,
		Account: e.Account,
		Key:     e.Key,
		Count:   e.Count,
		Detail:  e.Detail,
	}
	if e.Peer != nil {
		fingerprint := make([]byte, len(e.Peer.Fingerprint))
		copy(fingerprint, e.Peer.Fingerprint[:])
		out.Peer = &localpb.PeerRef{
			Fingerprint: fingerprint,
			Alias:       e.Peer.Alias,
		}
	}
	if e.Old != nil {
		out.OldValue = toConfigValue(*e.Old)
	}
	if e.New != nil {
		out.NewValue = toConfigValue(*e.New)
	}
	return out
}

func configErrorCode(err error) localpb.ErrorCode {
	switch {
	case errors.Is(err, config.ErrUnknownKey),
		errors.Is(err, config.ErrSecretKey),
		errors.Is(err, config.ErrListenOverridden),
		errors.Is(err, config.ErrInvalidValue),
		errors.Is(err, config.ErrInvalidCombination),
		errors.Is(err, config.ErrTooManyAddresses):
		return localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT
	case errors.Is(err, config.ErrLowPortDenied):
		return localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED
	default:
		return localpb.ErrorCode_ERROR_CODE_INTERNAL
	}
}
