package termcmd

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// TestDescribeErrorDetail checks that an ErrorDetail becomes its code label
// plus message and appends the reporting Broker.
func TestDescribeErrorDetail(t *testing.T) {
	detail := &localpb.ErrorDetail{
		Code:    localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED,
		Message: "read permission on /etc/peers/config.yaml is required",
	}
	st, err := status.New(codes.PermissionDenied, "x").WithDetails(detail)
	if err != nil {
		t.Fatalf("WithDetails: %v", err)
	}
	want := "permission_denied: read permission on /etc/peers/config.yaml is required"
	if got := Describe(st.Err()); got != want {
		t.Errorf("Describe = %q, want %q", got, want)
	}

	fingerprint := bytes.Repeat([]byte{0xef}, 32)
	detail.OriginatingBrokerFingerprint = fingerprint
	st, err = status.New(codes.PermissionDenied, "x").WithDetails(detail)
	if err != nil {
		t.Fatalf("WithDetails: %v", err)
	}
	want += " (reported by Broker " + strings.Repeat("ef", 32) + ")"
	if got := Describe(st.Err()); got != want {
		t.Errorf("Describe = %q, want %q", got, want)
	}
}

// TestDescribePlainGRPCError checks the text for an unavailable and an internal
// gRPC error.
func TestDescribePlainGRPCError(t *testing.T) {
	if got := Describe(status.Error(codes.Unavailable, "connection refused")); !strings.HasPrefix(got, "cannot reach the Broker: ") {
		t.Errorf("Describe(Unavailable) = %q, want it to start with %q", got, "cannot reach the Broker: ")
	}
	if got, want := Describe(status.Error(codes.Internal, "x")), "Internal: x"; got != want {
		t.Errorf("Describe(Internal) = %q, want %q", got, want)
	}
}

// TestDescribeNilAndPlainError checks that nil describes as empty and a plain
// error keeps its text.
func TestDescribeNilAndPlainError(t *testing.T) {
	if got := Describe(nil); got != "" {
		t.Errorf("Describe(nil) = %q, want the empty string", got)
	}
	if got, want := Describe(errors.New("boom")), "boom"; got != want {
		t.Errorf("Describe(plain) = %q, want %q", got, want)
	}
}

// TestDescribeNeutralizesControlCharacters checks that control and format
// characters in an ErrorDetail message are escaped.
func TestDescribeNeutralizesControlCharacters(t *testing.T) {
	esc, bel, bidi := rune(0x1b), rune(0x07), rune(0x202e)
	detail := &localpb.ErrorDetail{
		Code:    localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
		Message: "bad" + string(esc) + "]0;title" + string(bel) + " value" + string(bidi),
	}
	st, err := status.New(codes.InvalidArgument, "x").WithDetails(detail)
	if err != nil {
		t.Fatalf("WithDetails: %v", err)
	}
	got := Describe(st.Err())
	for _, r := range []rune{esc, bel, bidi} {
		if strings.ContainsRune(got, r) {
			t.Errorf("Describe = %q, carries the raw character U+%04X", got, r)
		}
		if want := escapeForTest(r); !strings.Contains(got, want) {
			t.Errorf("Describe = %q, want the escaped form %q", got, want)
		}
	}
}

// TestVersionMismatchErrorText checks that the error names both versions and the
// restart advice.
func TestVersionMismatchErrorText(t *testing.T) {
	err := &VersionMismatchError{Broker: 2, Client: 1}
	for _, want := range []string{"version_mismatch", "2", "1", "restart the Broker"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("VersionMismatchError.Error() = %q, want it to contain %q", err.Error(), want)
		}
	}
}
