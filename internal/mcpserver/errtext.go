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

// callKind selects the opening phrase of the text shown to the model.
type callKind int

const (
	callGeneric callKind = iota
	callSend
	callConnect
	callUpdate
)

// permNeed is the config-file permission a tool call requires.
type permNeed int

const (
	permNone permNeed = iota
	permRead
	permWrite
)

// Texts shown to the model. The local-state texts are format strings; the rest
// are used verbatim.
const (
	textStarting    = "the connection to the Broker is still being set up; retry shortly"
	textUnavailable = "the Broker is unavailable: %s; retrying in the background"
	// textBrokerRefusedConnection replaces the unavailable text when the socket
	// answered but the Broker closed the handshake. A connection limit at the
	// handshake gives the client no ErrorDetail, so the text names only a
	// likely cause and never sends the reader to start a running Broker.
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

// The text for an empty system selection depends on the account: root is
// the administrator, so it is told the command to run, while every other
// account must ask the administrator.
const (
	textNoSystemBrokerNonRoot = "the system Broker is unavailable; ask the administrator to start it"
	textNoSystemBrokerRoot    = "the system Broker is unavailable; start it with `peers service start --system`"
)

// systemBrokerText returns the text for an empty system selection. Root
// needs no sudo for the command it names.
func systemBrokerText(isRoot bool) string {
	if isRoot {
		return textNoSystemBrokerRoot
	}
	return textNoSystemBrokerNonRoot
}

// textNoSystemBroker is the text for this account, chosen once at startup.
var textNoSystemBroker = systemBrokerText(os.Geteuid() == 0)

// errNoBroker is the selection result when no Broker socket answers. Its text
// names the two ways to start a Broker, so the model can tell the user.
var errNoBroker = errors.New(textNoBroker)

// errNoSystemBroker is the selection result when the system installation was selected,
// by --system or as root's default, and no system Broker answers.
var errNoSystemBroker = errors.New(textNoSystemBroker)

// errNoUserBrokerSystemRunning is the default-level selection result when this
// account's user Broker is not running but the system Broker is.
// The text names the level option that reaches the running Broker.
var errNoUserBrokerSystemRunning = errors.New(textNoUserBrokerSystemRunning)

// openingPhrase returns the phrase that starts the error text for a call kind.
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

// openingPhraseForCode returns the phrase for a Broker error. The phrase
// follows the outcome the error code describes: a delivery_unknown result is
// unknown whichever tool asked, and every other outcome follows the call kind.
func openingPhraseForCode(tool string, kind callKind, code localpb.ErrorCode) string {
	if code == localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN {
		return "Delivery unknown"
	}
	return openingPhrase(tool, kind)
}

// toolError builds "<phrase>: <body> (code: <name>)".
func toolError(phrase, body string, code localpb.ErrorCode) error {
	return fmt.Errorf("%s: %s (code: %s)", phrase, body, codeName(code))
}

// notRegisteredReason turns a not_registered event reason into the sentence
// shown to the model. A non-empty detail from the Broker names the specific
// cause and wins over the general sentence for the reason.
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

// rpcError turns an error from a Local RPC into the error shown to the model.
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
// message. The phrase rpcError adds is chosen from the error code, so a message
// that names a different outcome must not keep its own phrase.
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

// errorDetail returns the ErrorDetail carried in the gRPC status details, or
// nil when there is none.
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

// rpcMessage returns the gRPC status message of err, which falls back to the
// error text when err carries no status.
func rpcMessage(err error) string {
	return status.Convert(err).Message()
}

// codeName returns the lower-case name of a stable error code. UNSPECIFIED and
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
