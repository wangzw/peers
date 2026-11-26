package mcpserver

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
)

type callKind int

const (
	callGeneric callKind = iota
	callSend
	callConnect
	callUpdate
)

type permNeed int

const (
	permNone permNeed = iota
	permRead
	permWrite
)

// Texts shown to the model. The local-state texts are format strings; the rest
// are used verbatim.
const (
	textStarting = "the connection to the Broker is still being set up; retry shortly"
	// textUnavailable is the whole sentence for a failed attempt at a user or
	// test-named Broker, with the original error substituted.
	textUnavailable         = "the Broker is unavailable: %s; retrying in the background."
	textUnavailableNoReason = "the Broker is unavailable; retrying in the background."
	// textBrokerRefusedConnection replaces the unavailable text when the socket
	// answered but the Broker closed the handshake.
	textBrokerRefusedConnection   = "the Broker is running but refused this connection; this account or the whole host may have reached its connection limit, so retry shortly."
	textNoBroker                  = "no Broker is available for this account; if a system Broker is installed on this host, ask the administrator to start it. Otherwise start a user Broker with `peers service install`, or run `peers service run` in a terminal outside the Claude Code session (a Broker started inside the session exits with code 6)"
	textNoUserBrokerSystemRunning = "no user Broker is running for this account; a system Broker is running, so retry with `--system`"
	textVersionMismatch           = "%s. To fix it, %s"
	textNotRegistered             = "this session is not registered as an agent: %s"

	textSendLost    = "lost contact with the Broker before it answered; the message may have been delivered, and sending it again may deliver it twice"
	textConnectLost = "lost contact with the Broker before it answered. The pairing may still have completed; call list_brokers to check."
	textGenericLost = "the Broker did not answer: %s"
	textPeerOffline = "the other Broker is offline and refuses messages from other hosts; sending the same message again will not deliver it. Tell the other user by another means if you can."

	textNeedRead  = "The calling account needs read permission on the Broker's config file."
	textNeedWrite = "The calling account needs write permission on the Broker's config file."

	textNoDefaultTopic = "this session has no default topic; pass a topic in `name` to choose one"
)

// The text for an empty system selection depends on whether the account is
// root.
const (
	textNoSystemBrokerNonRoot = "the system Broker is unavailable; ask the administrator to start it"
	textNoSystemBrokerRoot    = "the system Broker is unavailable; start it with `peers service start --system`"
)

func systemBrokerText(isRoot bool) string {
	if isRoot {
		return textNoSystemBrokerRoot
	}
	return textNoSystemBrokerNonRoot
}

var textNoSystemBroker = systemBrokerText(os.Geteuid() == 0)

// textSystemBrokerUnresponsive replaces the no-Broker text when the system
// Broker's socket answered but the attempt still failed.
const textSystemBrokerUnresponsive = "the system Broker is running but is not answering: %s; this session retries in the background, and if this persists, ask the administrator to check it."

// textSystemBrokerUnreachable replaces the unresponsive text when the probe
// could not reach the system Broker's socket at all.
const textSystemBrokerUnreachable = "cannot reach the system Broker at %s: %s; this session retries in the background."

func unavailableText(reason string) string {
	return fmt.Sprintf(textUnavailable, reason)
}

func systemBrokerUnresponsiveText(reason string) string {
	return fmt.Sprintf(textSystemBrokerUnresponsive, reason)
}

func systemBrokerUnreachableText(path, reason string) string {
	return fmt.Sprintf(textSystemBrokerUnreachable, path, reason)
}

var errNoBroker = errors.New(textNoBroker)

var errNoSystemBroker = errors.New(textNoSystemBroker)

var errNoUserBrokerSystemRunning = errors.New(textNoUserBrokerSystemRunning)

func openingPhrase(tool string, kind callKind) string {
	switch kind {
	case callSend:
		return "Not sent"
	case callConnect:
		return "Not connected"
	case callUpdate:
		return textUpdateFailed
	default:
		return tool + " failed"
	}
}

func openingPhraseForCode(tool string, kind callKind, code localpb.ErrorCode) string {
	if code == localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN {
		return "Delivery unknown"
	}
	return openingPhrase(tool, kind)
}

func toolError(phrase, body string, code localpb.ErrorCode) error {
	return fmt.Errorf("%s: %s (code: %s)", phrase, body, codeName(code))
}

// notRegisteredReason turns a not_registered reason into the sentence shown to
// the model; a non-empty detail from the Broker wins.
func notRegisteredReason(r localpb.NotRegisteredReason, detail string) string {
	if detail != "" {
		return detail
	}
	switch r {
	case localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SESSION_RECORD_NOT_FOUND:
		return "no Claude Code session record matches this session's inbox socket"
	case localpb.NotRegisteredReason_NOT_REGISTERED_REASON_NAME_UNUSABLE:
		return "the session name is unusable (empty, or contains @ or *); rename the session"
	case localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SOCKET_NOT_OWNED:
		return "the inbox socket is not owned by this account"
	case localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SOCKET_PATH_MISSING:
		return "the session had no inbox socket when this MCP server started; reconnect the MCP server with /mcp"
	case localpb.NotRegisteredReason_NOT_REGISTERED_REASON_BROKER_DEGRADED:
		return "the Broker is running in a degraded, diagnostics-only mode; it cannot serve this call until the startup failure is fixed"
	case localpb.NotRegisteredReason_NOT_REGISTERED_REASON_ACCOUNT_HELPER_FAILED:
		return "the Broker's helper for this account failed to start or stopped responding; ask the administrator to check the Broker log"
	default:
		return "registration is still in progress"
	}
}

func rpcError(tool string, kind callKind, perm permNeed, err error) error {
	d := errorDetail(err)
	if d == nil {
		switch kind {
		case callSend:
			return toolError("Delivery unknown", textSendLost, localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN)
		case callConnect:
			return toolError("Not connected", textConnectLost, localpb.ErrorCode_ERROR_CODE_INTERNAL)
		default:
			return toolError(openingPhrase(tool, kind), fmt.Sprintf(textGenericLost, rpcMessage(err)), localpb.ErrorCode_ERROR_CODE_INTERNAL)
		}
	}

	phrase := openingPhraseForCode(tool, kind, d.GetCode())
	body := phrase + ": " + stripResultPrefix(d.GetMessage())
	out := body + " (code: " + codeName(d.GetCode())
	if fp := d.GetOriginatingBrokerFingerprint(); len(fp) > 0 {
		out += "; reported by Broker " + hex.EncodeToString(fp)
	}
	out += ")"
	switch {
	case d.GetCode() == localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED && perm == permRead:
		out += ". " + textNeedRead
	case d.GetCode() == localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED && perm == permWrite:
		out += ". " + textNeedWrite
	case d.GetCode() == localpb.ErrorCode_ERROR_CODE_BROKER_OFFLINE:
		out += ". " + textPeerOffline
	}
	return errors.New(out)
}

// stripResultPrefix removes a result phrase the Broker already put on a
// message, so a message naming a different outcome does not keep its own.
func stripResultPrefix(message string) string {
	switch {
	case strings.HasPrefix(message, "Not sent: "):
		return strings.TrimPrefix(message, "Not sent: ")
	case strings.HasPrefix(message, "Delivery unknown: "):
		return strings.TrimPrefix(message, "Delivery unknown: ")
	case strings.HasPrefix(message, "Not connected: "):
		return strings.TrimPrefix(message, "Not connected: ")
	}
	return message
}

func errorDetail(err error) *localpb.ErrorDetail {
	s, _ := status.FromError(err)
	if s == nil {
		return nil
	}
	for _, detail := range s.Details() {
		if d, ok := detail.(*localpb.ErrorDetail); ok {
			return d
		}
	}
	return nil
}

func rpcMessage(err error) string {
	return status.Convert(err).Message()
}

// codeName returns the lower-case name of an error code; UNSPECIFIED and
// values outside the closed set are reported as internal.
func codeName(c localpb.ErrorCode) string {
	switch c {
	case localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT:
		return "invalid_argument"
	case localpb.ErrorCode_ERROR_CODE_TOO_LARGE:
		return "too_large"
	case localpb.ErrorCode_ERROR_CODE_NOT_FOUND:
		return "not_found"
	case localpb.ErrorCode_ERROR_CODE_AMBIGUOUS:
		return "ambiguous"
	case localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE:
		return "not_visible"
	case localpb.ErrorCode_ERROR_CODE_PEER_UNREACHABLE:
		return "peer_unreachable"
	case localpb.ErrorCode_ERROR_CODE_INBOX_UNWRITABLE:
		return "inbox_unwritable"
	case localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN:
		return "delivery_unknown"
	case localpb.ErrorCode_ERROR_CODE_PAIRING_REJECTED:
		return "pairing_rejected"
	case localpb.ErrorCode_ERROR_CODE_NEEDS_LOCAL_ALIAS:
		return "needs_local_alias"
	case localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED:
		return "permission_denied"
	case localpb.ErrorCode_ERROR_CODE_NOT_REGISTERED:
		return "not_registered"
	case localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH:
		return "version_mismatch"
	case localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED:
		return "quota_exceeded"
	case localpb.ErrorCode_ERROR_CODE_INTERNAL:
		return "internal"
	case localpb.ErrorCode_ERROR_CODE_BROKER_OFFLINE:
		return "broker_offline"
	case localpb.ErrorCode_ERROR_CODE_BROKER_DEGRADED:
		return "broker_degraded"
	default:
		return "internal"
	}
}
