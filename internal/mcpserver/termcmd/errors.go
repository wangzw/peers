package termcmd

import (
	"encoding/hex"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// restartWithThisBinary is the advice the terminal commands give when the
// running Broker speaks another local protocol version. The Broker's own
// version_mismatch answer ends with restart advice of its own, which this
// replaces so the user hears about the binary in hand.
const restartWithThisBinary = "restart the Broker with this binary"

// VersionMismatchError reports that the Broker on the other end of the local
// socket speaks a different local protocol version than this binary.
type VersionMismatchError struct {
	Broker uint32
	Client uint32
}

func (e *VersionMismatchError) Error() string {
	return fmt.Sprintf("version_mismatch: the Broker speaks local protocol version %d and this peers binary speaks %d; %s", e.Broker, e.Client, restartWithThisBinary)
}

// Describe turns an error returned by the Local service into the sentence shown
// to the user. The returned text starts with the name of the system error code
// when the error carries an ErrorDetail, so a permission failure reads
// "permission_denied: ..." rather than the gRPC code name. A nil error
// describes as the empty string, and an error that is not a gRPC status keeps
// its own text.
func Describe(err error) string {
	if err == nil {
		return ""
	}

	st, ok := status.FromError(err)
	if !ok {
		return sanitizeText(err.Error())
	}

	for _, detail := range st.Details() {
		ed, ok := detail.(*localpb.ErrorDetail)
		if !ok {
			continue
		}
		message := sanitizeText(ed.GetMessage())
		if ed.GetCode() == localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH {
			// The Broker states both versions and names its own restart advice;
			// keep the versions and replace the advice.
			message = strings.TrimSuffix(message, "; restart the Broker") + "; " + restartWithThisBinary
		}
		msg := enumLabel(localpb.ErrorCode_name, "ERROR_CODE_", int32(ed.GetCode())) + ": " + message
		if fp := ed.GetOriginatingBrokerFingerprint(); len(fp) > 0 {
			msg += " (reported by Broker " + hex.EncodeToString(fp) + ")"
		}
		return msg
	}

	if st.Code() == codes.Unavailable {
		return "cannot reach the Broker: " + sanitizeText(st.Message())
	}
	return st.Code().String() + ": " + sanitizeText(st.Message())
}

// errorCode returns the stable Broker error code carried in err, or
// ERROR_CODE_UNSPECIFIED when err carries no ErrorDetail. A terminal command
// uses it to add the command-line form of a hint that the Broker states in
// terms of the MCP tools.
func errorCode(err error) localpb.ErrorCode {
	st, ok := status.FromError(err)
	if !ok {
		return localpb.ErrorCode_ERROR_CODE_UNSPECIFIED
	}
	for _, detail := range st.Details() {
		if ed, ok := detail.(*localpb.ErrorDetail); ok {
			return ed.GetCode()
		}
	}
	return localpb.ErrorCode_ERROR_CODE_UNSPECIFIED
}
