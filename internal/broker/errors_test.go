package broker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// TestToStatusCarriesExactlyOneErrorDetail checks that toStatus carries exactly
// one ErrorDetail with the code, message and originating fingerprint.
func TestToStatusCarriesExactlyOneErrorDetail(t *testing.T) {
	err := toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_NOT_REGISTERED, "x"))
	s, ok := status.FromError(err)
	if !ok {
		t.Fatalf("status.FromError(%v) did not recognize a status error", err)
	}
	if s.Code() != codes.FailedPrecondition {
		t.Errorf("code = %v, want %v", s.Code(), codes.FailedPrecondition)
	}
	if s.Message() != "x" {
		t.Errorf("message = %q, want %q", s.Message(), "x")
	}
	details := s.Details()
	if len(details) != 1 {
		t.Fatalf("details = %d, want exactly 1", len(details))
	}
	d, ok := details[0].(*localpb.ErrorDetail)
	if !ok {
		t.Fatalf("detail type = %T, want *localpb.ErrorDetail", details[0])
	}
	if d.GetCode() != localpb.ErrorCode_ERROR_CODE_NOT_REGISTERED {
		t.Errorf("detail code = %v, want %v", d.GetCode(), localpb.ErrorCode_ERROR_CODE_NOT_REGISTERED)
	}
	if d.GetMessage() != "x" {
		t.Errorf("detail message = %q, want %q", d.GetMessage(), "x")
	}
	if len(d.GetOriginatingBrokerFingerprint()) != 0 {
		t.Errorf("local error carries a fingerprint: %x", d.GetOriginatingBrokerFingerprint())
	}

	origin := []byte{0xde, 0xad, 0xbe, 0xef}
	relayed, ok := status.FromError(toStatus(&Error{
		Code:    localpb.ErrorCode_ERROR_CODE_NOT_FOUND,
		Message: "z",
		Origin:  origin,
	}))
	if !ok {
		t.Fatal("status.FromError did not recognize the relayed status error")
	}
	relayedDetails := relayed.Details()
	if len(relayedDetails) != 1 {
		t.Fatalf("relayed details = %d, want exactly 1", len(relayedDetails))
	}
	rd, ok := relayedDetails[0].(*localpb.ErrorDetail)
	if !ok {
		t.Fatalf("relayed detail type = %T, want *localpb.ErrorDetail", relayedDetails[0])
	}
	if !bytes.Equal(rd.GetOriginatingBrokerFingerprint(), origin) {
		t.Errorf("fingerprint = %x, want %x", rd.GetOriginatingBrokerFingerprint(), origin)
	}
}

// TestToStatusWrappedError checks that an *Error wrapped in another error keeps
// its code and message.
func TestToStatusWrappedError(t *testing.T) {
	wrapped := fmt.Errorf("wrap: %w", Errorf(localpb.ErrorCode_ERROR_CODE_NOT_FOUND, "y"))
	err := toStatus(wrapped)
	s, ok := status.FromError(err)
	if !ok {
		t.Fatalf("status.FromError(%v) did not recognize a status error", err)
	}
	if s.Code() != codes.NotFound {
		t.Errorf("code = %v, want %v", s.Code(), codes.NotFound)
	}
	details := s.Details()
	if len(details) != 1 {
		t.Fatalf("details = %d, want exactly 1", len(details))
	}
	d, ok := details[0].(*localpb.ErrorDetail)
	if !ok {
		t.Fatalf("detail type = %T, want *localpb.ErrorDetail", details[0])
	}
	if d.GetCode() != localpb.ErrorCode_ERROR_CODE_NOT_FOUND {
		t.Errorf("detail code = %v, want %v", d.GetCode(), localpb.ErrorCode_ERROR_CODE_NOT_FOUND)
	}
	if d.GetMessage() != "y" {
		t.Errorf("detail message = %q, want %q", d.GetMessage(), "y")
	}
}

// TestToStatusPlainErrorIsInternal checks that an error that is not an *Error
// becomes internal.
func TestToStatusPlainErrorIsInternal(t *testing.T) {
	err := toStatus(errors.New("boom"))
	s, ok := status.FromError(err)
	if !ok {
		t.Fatalf("status.FromError(%v) did not recognize a status error", err)
	}
	if s.Code() != codes.Internal {
		t.Errorf("code = %v, want %v", s.Code(), codes.Internal)
	}
	if s.Message() != "internal error: boom" {
		t.Errorf("message = %q, want %q", s.Message(), "internal error: boom")
	}
	details := s.Details()
	if len(details) != 1 {
		t.Fatalf("details = %d, want exactly 1", len(details))
	}
	d, ok := details[0].(*localpb.ErrorDetail)
	if !ok {
		t.Fatalf("detail type = %T, want *localpb.ErrorDetail", details[0])
	}
	if d.GetCode() != localpb.ErrorCode_ERROR_CODE_INTERNAL {
		t.Errorf("detail code = %v, want %v", d.GetCode(), localpb.ErrorCode_ERROR_CODE_INTERNAL)
	}
	if d.GetMessage() != "internal error: boom" {
		t.Errorf("detail message = %q, want %q", d.GetMessage(), "internal error: boom")
	}
}

// TestToStatusCodeTable checks the wire error code to gRPC status code mapping
// for every code.
func TestToStatusCodeTable(t *testing.T) {
	cases := []struct {
		code localpb.ErrorCode
		name string
		want codes.Code
	}{
		{localpb.ErrorCode_ERROR_CODE_UNSPECIFIED, "unspecified", codes.Internal},
		{localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT, "invalid_argument", codes.InvalidArgument},
		{localpb.ErrorCode_ERROR_CODE_TOO_LARGE, "too_large", codes.InvalidArgument},
		{localpb.ErrorCode_ERROR_CODE_NOT_FOUND, "not_found", codes.NotFound},
		{localpb.ErrorCode_ERROR_CODE_AMBIGUOUS, "ambiguous", codes.FailedPrecondition},
		{localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE, "not_visible", codes.FailedPrecondition},
		{localpb.ErrorCode_ERROR_CODE_PEER_UNREACHABLE, "peer_unreachable", codes.Unavailable},
		{localpb.ErrorCode_ERROR_CODE_INBOX_UNWRITABLE, "inbox_unwritable", codes.Unavailable},
		{localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN, "delivery_unknown", codes.Unknown},
		{localpb.ErrorCode_ERROR_CODE_PAIRING_REJECTED, "pairing_rejected", codes.FailedPrecondition},
		{localpb.ErrorCode_ERROR_CODE_NEEDS_LOCAL_ALIAS, "needs_local_alias", codes.FailedPrecondition},
		{localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED, "permission_denied", codes.PermissionDenied},
		{localpb.ErrorCode_ERROR_CODE_NOT_REGISTERED, "not_registered", codes.FailedPrecondition},
		{localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH, "version_mismatch", codes.FailedPrecondition},
		{localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED, "quota_exceeded", codes.ResourceExhausted},
		{localpb.ErrorCode_ERROR_CODE_INTERNAL, "internal", codes.Internal},
		{localpb.ErrorCode_ERROR_CODE_NOT_UNDER_SERVICE_MANAGER, "not_under_service_manager", codes.FailedPrecondition},
		{localpb.ErrorCode_ERROR_CODE_UPDATE_IN_PROGRESS, "update_in_progress", codes.Aborted},
		{localpb.ErrorCode_ERROR_CODE_BROKER_OFFLINE, "broker_offline", codes.FailedPrecondition},
		{localpb.ErrorCode_ERROR_CODE_BROKER_DEGRADED, "broker_degraded", codes.FailedPrecondition},
	}
	if len(cases) != 20 {
		t.Fatalf("table covers %d error codes, want all 20", len(cases))
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := Errorf(tc.code, "m")
			if got, want := e.Error(), tc.name+": m"; got != want {
				t.Errorf("Error() = %q, want %q", got, want)
			}

			s, ok := status.FromError(toStatus(e))
			if !ok {
				t.Fatalf("status.FromError did not recognize a status error")
			}
			if s.Code() != tc.want {
				t.Errorf("code = %v, want %v", s.Code(), tc.want)
			}
			details := s.Details()
			if len(details) != 1 {
				t.Fatalf("details = %d, want exactly 1", len(details))
			}
			d, ok := details[0].(*localpb.ErrorDetail)
			if !ok {
				t.Fatalf("detail type = %T, want *localpb.ErrorDetail", details[0])
			}
			if d.GetCode() != tc.code {
				t.Errorf("detail code = %v, want %v", d.GetCode(), tc.code)
			}
		})
	}
}

// TestUnknownErrorCode checks that an unknown numeric error code maps to
// internal and renders as its decimal value.
func TestUnknownErrorCode(t *testing.T) {
	const code = localpb.ErrorCode(99)
	e := Errorf(code, "m")
	if got, want := e.Error(), "99: m"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}

	s, ok := status.FromError(toStatus(e))
	if !ok {
		t.Fatalf("status.FromError did not recognize a status error")
	}
	if s.Code() != codes.Internal {
		t.Errorf("code = %v, want %v", s.Code(), codes.Internal)
	}
	details := s.Details()
	if len(details) != 1 {
		t.Fatalf("details = %d, want exactly 1", len(details))
	}
	d, ok := details[0].(*localpb.ErrorDetail)
	if !ok {
		t.Fatalf("detail type = %T, want *localpb.ErrorDetail", details[0])
	}
	if d.GetCode() != code {
		t.Errorf("detail code = %v, want %v", d.GetCode(), code)
	}
}

// TestToStatusNilAndCanceled checks that nil stays nil and context.Canceled
// maps to Canceled without a detail.
func TestToStatusNilAndCanceled(t *testing.T) {
	if err := toStatus(nil); err != nil {
		t.Errorf("toStatus(nil) = %v, want nil", err)
	}

	for _, err := range []error{
		context.Canceled,
		fmt.Errorf("stop: %w", context.Canceled),
	} {
		s, ok := status.FromError(toStatus(err))
		if !ok {
			t.Fatalf("status.FromError did not recognize a status error for %v", err)
		}
		if s.Code() != codes.Canceled {
			t.Errorf("code = %v, want %v", s.Code(), codes.Canceled)
		}
		if len(s.Details()) != 0 {
			t.Errorf("canceled error carries %d details, want none", len(s.Details()))
		}
		if s.Message() != err.Error() {
			t.Errorf("message = %q, want %q", s.Message(), err.Error())
		}
	}
}
