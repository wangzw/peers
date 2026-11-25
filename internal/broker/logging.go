package broker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"
)

// Event names for the structured runtime log; each value is the "event" field
// of one log record.
const (
	EvBrokerStarted                 = "broker_started"
	EvBrokerStopping                = "broker_stopping"
	EvStartupFailed                 = "startup_failed"
	EvDegradedEntered               = "degraded_entered"
	EvDegradedLeft                  = "degraded_left"
	EvDegradedReasonChanged         = "degraded_reason_changed"
	EvLockLost                      = "account_lock_lost"
	EvAccountLockTouchFailed        = "account_lock_touch_failed"
	EvAccountLockTouchRecovered     = "account_lock_touch_recovered"
	EvRuntimeDirTouchFailed         = "runtime_dir_touch_failed"
	EvRuntimeDirTouchRecovered      = "runtime_dir_touch_recovered"
	EvLocalSocketTouchFailed        = "local_socket_touch_failed"
	EvLocalSocketTouchRecovered     = "local_socket_touch_recovered"
	EvRuntimeDirFallback            = "runtime_dir_fallback"
	EvLocalSocketBindRetry          = "local_socket_bind_retry"
	EvLocalSocketRebound            = "local_socket_rebound"
	EvLocalSocketRebindFailed       = "local_socket_rebind_failed"
	EvConnectionRejected            = "local_connection_rejected"
	EvHelloVersionMismatch          = "hello_version_mismatch"
	EvAgentRegistered               = "agent_registered"
	EvAgentUnregistered             = "agent_unregistered"
	EvRegistrationWaiting           = "registration_waiting"
	EvRegisterEventsOverflow        = "register_events_overflow"
	EvAgentRenamed                  = "agent_renamed"
	EvAgentSocketChanged            = "agent_socket_changed"
	EvRecordRereadFailed            = "session_record_reread_failed"
	EvRegistryWatchFailed           = "session_registry_watch_failed"
	EvLogLevelChanged               = "log_level_changed"
	EvUpdateCheckFailed             = "update_check_failed"
	EvRemoteMemberInvalid           = "remote_member_invalid"
	EvSessionProtocolUnrecognized   = "session_protocol_unrecognized"
	EvPeerDeliveryErrorUnrecognized = "peer_delivery_error_unrecognized"
	EvNameOwnerTableFull            = "name_owner_table_full"
	EvNameOwnersUnreadable          = "name_owners_unreadable"
	EvNameOwnersWriteFailed         = "name_owners_write_failed"
	EvDeliveryRefusedOwner          = "delivery_refused_owner"
)

// Event names of the local interface limits.
const (
	EvLocalConnectionLimitReached = "local_connection_limit_reached"
	EvRegisterLimitReached        = "register_limit_reached"
)

// NewLogger returns a logger that writes one JSON object per line: the message
// key becomes "event", the time is rendered in UTC with nanosecond precision,
// and the level is read from level on every record. Writes to w are serialized,
// so an unprotected writer is safe to pass.
func NewLogger(w io.Writer, level *slog.LevelVar) *slog.Logger {
	handler := slog.NewJSONHandler(&lockedWriter{w: w}, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) != 0 {
				return a
			}
			switch a.Key {
			case slog.MessageKey:
				a.Key = "event"
			case slog.TimeKey:
				a.Value = slog.StringValue(a.Value.Time().UTC().Format(time.RFC3339Nano))
			}
			return a
		},
	})
	return slog.New(eventHandler{inner: handler})
}

// eventHandler keeps the "event" key unique: a string "event" attribute names
// the record and the message moves to "message", while a non-string one keeps
// its data under "event_details". An attribute inside a group is left alone.
type eventHandler struct {
	inner   slog.Handler
	event   *slog.Value
	message bool
	group   bool
}

func (h eventHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h eventHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if h.group {
		return eventHandler{inner: h.inner.WithAttrs(attrs), event: h.event, message: h.message, group: true}
	}
	kept, event, message := splitEventAttrs(attrs, h.event)
	return eventHandler{
		inner:   h.inner.WithAttrs(kept),
		event:   event,
		message: h.message || message,
	}
}

func (h eventHandler) WithGroup(name string) slog.Handler {
	return eventHandler{inner: h.inner.WithGroup(name), event: h.event, message: h.message, group: true}
}

func (h eventHandler) Handle(ctx context.Context, r slog.Record) error {
	event := h.event
	hasMessage := h.message
	record := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	if h.group {
		// Record attributes are written inside the group, so they cannot
		// collide with the top-level event key.
		r.Attrs(func(a slog.Attr) bool {
			record.AddAttrs(a)
			return true
		})
	} else {
		attrs := make([]slog.Attr, 0, r.NumAttrs())
		r.Attrs(func(a slog.Attr) bool {
			attrs = append(attrs, a)
			return true
		})
		var message bool
		attrs, event, message = splitEventAttrs(attrs, event)
		hasMessage = hasMessage || message
		record.AddAttrs(attrs...)
	}
	if event != nil {
		if !hasMessage && record.Message != "" && record.Message != event.String() {
			record.AddAttrs(slog.String("message", record.Message))
		}
		record.Message = event.String()
	}
	return h.inner.Handle(ctx, record)
}

// splitEventAttrs resolves the event key of a top-level attribute list after
// flattening empty-name groups; the message result reports an explicit
// "message" attribute the record must not duplicate.
func splitEventAttrs(attrs []slog.Attr, event *slog.Value) (kept []slog.Attr, ev *slog.Value, message bool) {
	ev = event
	for _, a := range flattenTopLevel(attrs) {
		switch {
		case a.Key == "event" && a.Value.Kind() == slog.KindString:
			value := a.Value
			ev = &value
		case a.Key == "event":
			a.Key = "event_details"
			kept = append(kept, a)
		case a.Key == "message":
			message = true
			kept = append(kept, a)
		default:
			kept = append(kept, a)
		}
	}
	return kept, ev, message
}

// flattenTopLevel inlines empty-name groups, which the JSON handler writes at
// the current level.
func flattenTopLevel(attrs []slog.Attr) []slog.Attr {
	flat := make([]slog.Attr, 0, len(attrs))
	for _, a := range attrs {
		if a.Key == "" && a.Value.Kind() == slog.KindGroup {
			flat = append(flat, flattenTopLevel(a.Value.Group())...)
			continue
		}
		flat = append(flat, a)
	}
	return flat
}

type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (lw *lockedWriter) Write(p []byte) (int, error) {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	return lw.w.Write(p)
}

// ParseLogLevel converts a configured log level name to a slog.Level, accepting
// the lower-case names debug, info, warn and error.
func ParseLogLevel(s string) (slog.Level, error) {
	switch s {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("cannot parse log level %q (want debug, info, warn or error)", s)
	}
}
