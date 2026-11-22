package broker

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// Error is a Local interface error. Code is the wire error code; Origin is set
// only when the error was relayed from another Broker and carries that Broker's
// fingerprint. Handlers return an *Error (or an error wrapping one) and send it
// to the client through toStatus.
type Error struct {
	Code    localpb.ErrorCode
	Message string
	Origin  []byte
}

// Error renders the code as the part of its enum name after ERROR_CODE_,
// lowercased, followed by the message, for example "not_registered: no session
// record". An unknown numeric code renders as its decimal value.
func (e *Error) Error() string {
	return errorCodeName(e.Code) + ": " + e.Message
}

// errorCodeName renders a wire error code as the part of its enum name after
// ERROR_CODE_, lowercased. An unknown numeric code renders as its decimal value.
func errorCodeName(code localpb.ErrorCode) string {
	return strings.ToLower(strings.TrimPrefix(code.String(), "ERROR_CODE_"))
}

// Errorf returns an *Error with a formatted message.
func Errorf(code localpb.ErrorCode, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// toStatus converts err into a gRPC status error carrying exactly one
// localpb.ErrorDetail. An *Error anywhere in the chain keeps its code and
// message; context.Canceled becomes codes.Canceled without a detail; any other
// error becomes ERROR_CODE_INTERNAL with the message "internal error: ...".
func toStatus(err error) error {
	if err == nil {
		return nil
	}

	var e *Error
	if errors.As(err, &e) {
		st := status.New(grpcCode(e.Code), e.Message)
		detailed, detailErr := st.WithDetails(&localpb.ErrorDetail{
			Code:                         e.Code,
			Message:                      e.Message,
			OriginatingBrokerFingerprint: e.Origin,
		})
		if detailErr != nil {
			// A detail that cannot be marshaled is dropped; the caller still
			// sees the code and message.
			return st.Err()
		}
		return detailed.Err()
	}

	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, err.Error())
	}

	msg := "internal error: " + err.Error()
	st := status.New(codes.Internal, msg)
	detailed, detailErr := st.WithDetails(&localpb.ErrorDetail{
		Code:    localpb.ErrorCode_ERROR_CODE_INTERNAL,
		Message: msg,
	})
	if detailErr != nil {
		return st.Err()
	}
	return detailed.Err()
}

// grpcCode maps a wire error code to the gRPC status code the local interface
// reports for it. Unknown numeric codes map to codes.Internal.
func grpcCode(code localpb.ErrorCode) codes.Code {
	switch code {
	case localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
		localpb.ErrorCode_ERROR_CODE_TOO_LARGE:
		return codes.InvalidArgument
	case localpb.ErrorCode_ERROR_CODE_NOT_FOUND:
		return codes.NotFound
	case localpb.ErrorCode_ERROR_CODE_AMBIGUOUS,
		localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE,
		localpb.ErrorCode_ERROR_CODE_NOT_REGISTERED,
		localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH,
		localpb.ErrorCode_ERROR_CODE_NEEDS_LOCAL_ALIAS,
		localpb.ErrorCode_ERROR_CODE_PAIRING_REJECTED,
		localpb.ErrorCode_ERROR_CODE_NOT_UNDER_SERVICE_MANAGER,
		localpb.ErrorCode_ERROR_CODE_BROKER_OFFLINE,
		localpb.ErrorCode_ERROR_CODE_BROKER_DEGRADED:
		return codes.FailedPrecondition
	case localpb.ErrorCode_ERROR_CODE_UPDATE_IN_PROGRESS:
		return codes.Aborted
	case localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED:
		return codes.PermissionDenied
	case localpb.ErrorCode_ERROR_CODE_PEER_UNREACHABLE,
		localpb.ErrorCode_ERROR_CODE_INBOX_UNWRITABLE:
		return codes.Unavailable
	case localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN:
		return codes.Unknown
	case localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED:
		return codes.ResourceExhausted
	default:
		// ERROR_CODE_INTERNAL and ERROR_CODE_UNSPECIFIED, and any code this
		// version does not know.
		return codes.Internal
	}
}
