package termcmd

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
)

const restartWithThisBinary = "restart the Broker with this binary"

// VersionMismatchError reports that the Broker on the other end of the local
// socket speaks a different local protocol version than this binary.
type VersionMismatchError struct {
	// Broker is the local protocol version the Broker reports.
	Broker uint32
	// Client is the local protocol version this binary speaks.
	Client uint32
}

// Error reports the two versions and the restart advice.
func (e *VersionMismatchError) Error() string {
	return fmt.Sprintf("version_mismatch: the Broker speaks local protocol version %d and this peers binary speaks %d; %s", e.Broker, e.Client, restartWithThisBinary)
}

// Describe turns an error from the Local service into the sentence shown to the
// user. An error carrying an ErrorDetail starts with the system error code name,
// so a permission failure reads "permission_denied: ..." rather than the gRPC
// code name, and it names the originating Broker when the detail carries one; an
// unavailable status returns "cannot reach the Broker: " with its message; a nil
// error describes as the empty string, and an error that is not a gRPC status
// keeps its own text.
func Describe(err error) string {
	if err == nil {
		return ""
	}

	st, ok := status.FromError(err)
	if !ok {
		return sanitizeText(err.Error())
	}

	if ed := errorDetailOf(st); ed != nil {
		message := sanitizeText(ed.GetMessage())
		if ed.GetCode() == localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH {
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

func describedError(err error) error {
	return errors.New(Describe(err))
}

func errorDetailOf(st *status.Status) *localpb.ErrorDetail {
	for _, detail := range st.Details() {
		if ed, ok := detail.(*localpb.ErrorDetail); ok {
			return ed
		}
	}
	return nil
}

func errorCode(err error) localpb.ErrorCode {
	st, ok := status.FromError(err)
	if !ok {
		return localpb.ErrorCode_ERROR_CODE_UNSPECIFIED
	}
	if ed := errorDetailOf(st); ed != nil {
		return ed.GetCode()
	}
	return localpb.ErrorCode_ERROR_CODE_UNSPECIFIED
}
