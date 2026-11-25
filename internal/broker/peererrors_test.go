package broker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/pairing"
)

// TestConnectErrorMapping checks that each pairing error maps to the expected local error code and message.
func TestConnectErrorMapping(t *testing.T) {
	const address = "198.51.100.4:17443"

	tests := []struct {
		name       string
		localAlias string
		err        error
		wantCode   localpb.ErrorCode
		wantMsg    string
		contains   []string
		notContain []string
	}{
		{
			name:     "invalid address",
			err:      pairing.ErrInvalidAddress,
			wantCode: localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
			contains: []string{address, "address:port"},
		},
		{
			name:     "wrapped invalid address",
			err:      fmt.Errorf("x: %w", pairing.ErrInvalidAddress),
			wantCode: localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
		},
		{
			name:       "invalid local alias characters",
			localAlias: "bad alias",
			err:        pairing.ErrInvalidAlias,
			wantCode:   localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
			contains:   []string{"invalid local alias", "use 1 to 24 letters"},
			notContain: []string{"longer than 24 characters"},
		},
		{
			name:       "local alias longer than 24 characters",
			localAlias: strings.Repeat("a", 25),
			err:        pairing.ErrInvalidAlias,
			wantCode:   localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
			contains:   []string{"invalid local alias: longer than 24 characters"},
		},
		{
			name:       "local alias with non-ASCII characters",
			localAlias: strings.Repeat("é", 13),
			err:        pairing.ErrInvalidAlias,
			wantCode:   localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
			contains:   []string{"invalid local alias", "use 1 to 24 letters"},
			notContain: []string{"longer than 24 characters"},
		},
		{
			name:     "wrapped needs local alias",
			err:      fmt.Errorf("w: %w", pairing.ErrNeedsLocalAlias),
			wantCode: localpb.ErrorCode_ERROR_CODE_NEEDS_LOCAL_ALIAS,
			contains: []string{"already this Broker's alias or a peer's alias"},
			notContain: []string{
				"local_alias",
				"connect_broker",
			},
		},
		{
			name:     "pairing error carries detail",
			err:      &pairing.PairingError{Reason: pairing.ReasonKeyInvalid, Detail: "the pairing key is invalid"},
			wantCode: localpb.ErrorCode_ERROR_CODE_PAIRING_REJECTED,
			wantMsg:  "the pairing key is invalid",
		},
		{
			name:     "wrapped pairing error",
			err:      fmt.Errorf("dial: %w", &pairing.PairingError{Reason: pairing.ReasonTimeout, Detail: "handshake timed out"}),
			wantCode: localpb.ErrorCode_ERROR_CODE_PAIRING_REJECTED,
			wantMsg:  "handshake timed out",
		},
		{
			name:     "pairing error without detail",
			err:      &pairing.PairingError{Reason: pairing.ReasonTryLater},
			wantCode: localpb.ErrorCode_ERROR_CODE_PAIRING_REJECTED,
			wantMsg:  "pairing was refused",
		},
		{
			name:     "generic pairing rejection",
			err:      fmt.Errorf("x: %w", pairing.ErrPairingRejected),
			wantCode: localpb.ErrorCode_ERROR_CODE_PAIRING_REJECTED,
			wantMsg:  "x: pairing rejected",
		},
		{
			name:     "protocol error is a rejection",
			err:      pairing.ErrProtocol,
			wantCode: localpb.ErrorCode_ERROR_CODE_PAIRING_REJECTED,
			contains: []string{"broke the pairing protocol"},
		},
		{
			name:     "peer unreachable names the address",
			err:      pairing.ErrPeerUnreachable,
			wantCode: localpb.ErrorCode_ERROR_CODE_PEER_UNREACHABLE,
			contains: []string{address},
		},
		{
			name:     "wrapped peer unreachable",
			err:      fmt.Errorf("dial: %w", pairing.ErrPeerUnreachable),
			wantCode: localpb.ErrorCode_ERROR_CODE_PEER_UNREACHABLE,
		},
		{
			name:     "peer table error is internal",
			err:      pairing.ErrStore,
			wantCode: localpb.ErrorCode_ERROR_CODE_INTERNAL,
		},
		{
			name:     "not open is internal",
			err:      pairing.ErrNotOpen,
			wantCode: localpb.ErrorCode_ERROR_CODE_INTERNAL,
		},
		{
			name:     "unknown error is internal and does not leak as rejection",
			err:      errors.New("boom"),
			wantCode: localpb.ErrorCode_ERROR_CODE_INTERNAL,
			contains: []string{"boom"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := connectError(address, tt.localAlias, tt.err)
			if got == nil {
				t.Fatalf("connectError(%v) = nil, want an *Error", tt.err)
			}
			if got.Code != tt.wantCode {
				t.Errorf("Code = %v, want %v", got.Code, tt.wantCode)
			}
			if tt.wantMsg != "" && got.Message != tt.wantMsg {
				t.Errorf("Message = %q, want %q", got.Message, tt.wantMsg)
			}
			for _, want := range tt.contains {
				if !strings.Contains(got.Message, want) {
					t.Errorf("Message = %q, want it to contain %q", got.Message, want)
				}
			}
			for _, bad := range tt.notContain {
				if strings.Contains(got.Message, bad) {
					t.Errorf("Message = %q, want it not to contain %q", got.Message, bad)
				}
			}
		})
	}
}

// TestConnectErrorDropsNotConnectedPrefix checks that a PairingError's detail is passed without its "Not connected: " prefix.
func TestConnectErrorDropsNotConnectedPrefix(t *testing.T) {
	err := &pairing.PairingError{Reason: pairing.ReasonVerificationFailed, Detail: "verification failed"}
	got := connectError("h(192.0.2.1:1)", "", err)
	if got == nil {
		t.Fatal("connectError returned nil")
	}
	if strings.Contains(got.Message, "Not connected") {
		t.Errorf("Message = %q, want no %q prefix", got.Message, "Not connected")
	}
	if got.Message != "verification failed" {
		t.Errorf("Message = %q, want %q", got.Message, "verification failed")
	}
}

// TestConnectErrorCanceledIsNil checks that a canceled call is left to toStatus.
func TestConnectErrorCanceledIsNil(t *testing.T) {
	for _, err := range []error{
		context.Canceled,
		fmt.Errorf("dial: %w", context.Canceled),
	} {
		if got := connectError("h:1", "", err); got != nil {
			t.Errorf("connectError(%v) = %v, want nil", err, got)
		}
	}
}

// TestPairingKeyErrorNotListening checks that a Broker that does not listen
// returns invalid_argument naming the empty broker.listen, and a store failure
// returns internal.
func TestPairingKeyErrorNotListening(t *testing.T) {
	got := pairingKeyError(pairing.ErrNotListening)
	if got == nil {
		t.Fatal("pairingKeyError(ErrNotListening) = nil")
	}
	if got.Code != localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT {
		t.Errorf("Code = %v, want invalid_argument", got.Code)
	}
	for _, want := range []string{"is not listening", "broker.listen is empty"} {
		if !strings.Contains(got.Message, want) {
			t.Errorf("Message = %q, want it to contain %q", got.Message, want)
		}
	}
	for _, unwanted := range []string{"connect_broker", "local_alias"} {
		if strings.Contains(got.Message, unwanted) {
			t.Errorf("Message = %q, want it to carry no MCP name %q", got.Message, unwanted)
		}
	}

	got = pairingKeyError(fmt.Errorf("write key: %w", pairing.ErrStore))
	if got == nil {
		t.Fatal("pairingKeyError(ErrStore) = nil")
	}
	if got.Code != localpb.ErrorCode_ERROR_CODE_INTERNAL {
		t.Errorf("Code = %v, want internal", got.Code)
	}
	if !strings.Contains(got.Message, "creating a pairing key failed") {
		t.Errorf("Message = %q, want it to name the operation", got.Message)
	}
}

// TestUnpairErrorIsInternal checks that a peer table failure while unpairing is reported as internal and names the operation.
func TestUnpairErrorIsInternal(t *testing.T) {
	got := unpairError(pairing.ErrStore)
	if got == nil {
		t.Fatal("unpairError(ErrStore) = nil")
	}
	if got.Code != localpb.ErrorCode_ERROR_CODE_INTERNAL {
		t.Errorf("Code = %v, want internal", got.Code)
	}
	if !strings.Contains(got.Message, "unpairing failed") {
		t.Errorf("Message = %q, want it to name the operation", got.Message)
	}
}
