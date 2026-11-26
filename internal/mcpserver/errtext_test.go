package mcpserver

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
)

func detailStatus(code codes.Code, d *localpb.ErrorDetail) error {
	s, err := status.New(code, d.GetMessage()).WithDetails(d)
	if err != nil {
		panic(err)
	}
	return s.Err()
}

func newHandlers(st Status) *handlers {
	store := &statusStore{}
	store.Update(func(s *Status) { *s = st })
	return &handlers{st: store, log: nil}
}

// TestPreflight checks the error for each session phase and the two ways a
// connected session may answer a tool call.
func TestPreflight(t *testing.T) {
	const detail = "dial unix /run/peers/broker.sock: no such file or directory"
	const unavailableDetail = "the Broker is unavailable: " + detail + "; retrying in the background."
	const mismatchDetail = "the running Broker speaks local protocol version 2; this MCP server speaks version 1"
	const remedy = "restart the Broker of this account with the same peers version as this MCP server; this session reconnects by itself"

	tests := []struct {
		name      string
		status    Status
		tool      string
		kind      callKind
		needAgent bool
		want      string
		wantNoErr bool
	}{
		{
			name:   "starting",
			status: Status{Phase: PhaseStarting},
			tool:   "list_agents",
			kind:   callGeneric,
			want:   "list_agents failed: the connection to the Broker is still being set up; retry shortly (code: internal)",
		},
		{
			name:   "unavailable generic",
			status: Status{Phase: PhaseUnavailable, Detail: unavailableDetail},
			tool:   "list_agents",
			kind:   callGeneric,
			want:   "list_agents failed: " + unavailableDetail + " (code: internal)",
		},
		{
			name:   "unavailable send",
			status: Status{Phase: PhaseUnavailable, Detail: unavailableDetail},
			tool:   "send_message",
			kind:   callSend,
			want:   "Not sent: " + unavailableDetail + " (code: internal)",
		},
		{
			name:   "unavailable without a reason",
			status: Status{Phase: PhaseUnavailable},
			tool:   "list_agents",
			kind:   callGeneric,
			want:   "list_agents failed: the Broker is unavailable; retrying in the background. (code: internal)",
		},
		{
			name:   "no Broker",
			status: Status{Phase: PhaseNoBroker},
			tool:   "list_agents",
			kind:   callGeneric,
			want:   "list_agents failed: " + textNoBroker + " (code: internal)",
		},
		{
			name:   "no system Broker",
			status: Status{Phase: PhaseNoBroker, Detail: textNoSystemBroker},
			tool:   "list_agents",
			kind:   callGeneric,
			want:   "list_agents failed: " + textNoSystemBroker + " (code: internal)",
		},
		{
			name: "Broker rejected the registration",
			status: Status{
				Phase:      PhaseBrokerRejected,
				Detail:     "cannot register: account alice already has 1 open registrations",
				RejectCode: localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED,
			},
			tool: "list_agents",
			kind: callGeneric,
			want: "list_agents failed: cannot register: account alice already has 1 open registrations (code: quota_exceeded)",
		},
		{
			name: "Broker refused the handshake",
			status: Status{
				Phase:      PhaseBrokerRejected,
				Detail:     "the Broker is running but refused this connection; this account or the whole host may have reached its connection limit, so retry shortly.",
				RejectCode: localpb.ErrorCode_ERROR_CODE_INTERNAL,
			},
			tool: "list_agents",
			kind: callGeneric,
			want: "list_agents failed: the Broker is running but refused this connection; this account or the whole host may have reached its connection limit, so retry shortly. (code: internal)",
		},
		{
			name:   "version mismatch",
			status: Status{Phase: PhaseVersionMismatch, Detail: mismatchDetail, Remedy: remedy},
			tool:   "list_agents",
			kind:   callGeneric,
			want:   "list_agents failed: " + mismatchDetail + ". To fix it, " + remedy + " (code: version_mismatch)",
		},
		{
			name: "connected not registered",
			status: Status{
				Phase:         PhaseConnected,
				NotRegistered: localpb.NotRegisteredReason_NOT_REGISTERED_REASON_NAME_UNUSABLE,
			},
			tool:      "list_agents",
			kind:      callGeneric,
			needAgent: true,
			want:      "list_agents failed: this session is not registered as an agent: the session name is unusable (empty, or contains @ or *); rename the session (code: not_registered)",
		},
		{
			name: "connected not registered send",
			status: Status{
				Phase:         PhaseConnected,
				NotRegistered: localpb.NotRegisteredReason_NOT_REGISTERED_REASON_UNSPECIFIED,
			},
			tool:      "send_message",
			kind:      callSend,
			needAgent: true,
			want:      "Not sent: this session is not registered as an agent: registration is still in progress (code: not_registered)",
		},
		{
			name: "connected degraded with an agent needed",
			status: Status{
				Phase:               PhaseConnected,
				NotRegistered:       localpb.NotRegisteredReason_NOT_REGISTERED_REASON_BROKER_DEGRADED,
				NotRegisteredDetail: "config parse failed",
			},
			tool:      "list_agents",
			kind:      callGeneric,
			needAgent: true,
			want:      "list_agents failed: config parse failed (code: broker_degraded)",
		},
		{
			name: "connected degraded without needAgent",
			status: Status{
				Phase:               PhaseConnected,
				NotRegistered:       localpb.NotRegisteredReason_NOT_REGISTERED_REASON_BROKER_DEGRADED,
				NotRegisteredDetail: "config parse failed",
			},
			tool:      "list_brokers",
			kind:      callGeneric,
			needAgent: false,
			wantNoErr: true,
		},
		{
			name: "connected not registered without needAgent",
			status: Status{
				Phase:         PhaseConnected,
				NotRegistered: localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SESSION_RECORD_NOT_FOUND,
			},
			tool:      "list_brokers",
			kind:      callGeneric,
			needAgent: false,
			wantNoErr: true,
		},
		{
			name:      "connected registered",
			status:    Status{Phase: PhaseConnected, AgentID: "id1"},
			tool:      "list_agents",
			kind:      callGeneric,
			needAgent: true,
			wantNoErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHandlers(tt.status)
			gotStatus, err := h.preflight(tt.tool, tt.kind, tt.needAgent)
			if tt.wantNoErr {
				if err != nil {
					t.Fatalf("preflight returned error %q, want none", err)
				}
				if gotStatus.Phase != tt.status.Phase {
					t.Errorf("returned Status phase %v, want %v", gotStatus.Phase, tt.status.Phase)
				}
				return
			}
			if err == nil {
				t.Fatal("preflight returned no error")
			}
			if got := err.Error(); got != tt.want {
				t.Errorf("preflight error = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestNoSystemBrokerTextNamesTheRightActor checks which actor each empty
// system-Broker text names.
func TestNoSystemBrokerTextNamesTheRightActor(t *testing.T) {
	cases := []struct {
		name string
		root bool
		text string
	}{
		{name: "non-root asks the administrator", text: textNoSystemBrokerNonRoot},
		{name: "root runs the command", root: true, text: textNoSystemBrokerRoot},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := systemBrokerText(tc.root); got != tc.text {
				t.Fatalf("systemBrokerText(%t) = %q, want %q", tc.root, got, tc.text)
			}
			st := &statusStore{}
			st.Update(func(s *Status) {
				s.Phase = PhaseNoBroker
				s.Detail = tc.text
			})
			h := &handlers{st: st}

			_, err := h.preflight("list_agents", callGeneric, false)
			if err == nil {
				t.Fatal("preflight returned nil for PhaseNoBroker")
			}
			want := "list_agents failed: " + tc.text + " (code: internal)"
			if got := err.Error(); got != want {
				t.Fatalf("preflight error = %q, want %q", got, want)
			}
			for _, unwanted := range []string{"peers service install", "peers service run"} {
				if strings.Contains(err.Error(), unwanted) {
					t.Fatalf("error = %q, must not mention %q", err, unwanted)
				}
			}
			if tc.root && !strings.Contains(err.Error(), "peers service start --system") {
				t.Fatalf("error = %q, want it to name the command root runs", err)
			}
			if !tc.root && !strings.Contains(err.Error(), "ask the administrator") {
				t.Fatalf("error = %q, want it to name the administrator", err)
			}
		})
	}
}

// TestPreflightVersionRemedies checks the fix clause for a version mismatch with
// each remedy.
func TestPreflightVersionRemedies(t *testing.T) {
	const (
		detail = "the running Broker speaks local protocol version 2; this MCP server speaks version 1"
		user   = "restart the Broker of this account with the same peers version as this MCP server; this session reconnects by itself"
		system = "ask the administrator to restart the Broker system service with the same peers version as this MCP server; this session reconnects by itself"
		env    = "restart the Broker at /tmp/x/broker.sock with the same peers version as this MCP server; this session reconnects by itself"
	)
	for _, remedy := range []string{user, system, env} {
		h := newHandlers(Status{Phase: PhaseVersionMismatch, Detail: detail, Remedy: remedy})
		_, err := h.preflight("list_agents", callGeneric, true)
		if err == nil {
			t.Fatalf("preflight with remedy %q returned no error", remedy)
		}
		got := err.Error()
		if !strings.HasPrefix(got, "list_agents failed: the running Broker speaks local protocol version") {
			t.Errorf("error %q does not start with the version mismatch phrase", got)
		}
		if !strings.Contains(got, "restart the Broker") {
			t.Errorf("error %q does not say to restart the Broker", got)
		}
		if !strings.Contains(got, "To fix it, "+remedy) {
			t.Errorf("error %q does not contain the remedy clause %q", got, remedy)
		}
		if !strings.Contains(got, "(code: version_mismatch)") {
			t.Errorf("error %q does not carry the version_mismatch code", got)
		}
	}
}

// TestPreflightNotRegisteredReasons checks the sentence for every reason a
// Broker may report on the registration stream.
func TestPreflightNotRegisteredReasons(t *testing.T) {
	tests := []struct {
		reason localpb.NotRegisteredReason
		want   string
	}{
		{
			reason: localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SESSION_RECORD_NOT_FOUND,
			want:   "no Claude Code session record matches this session's inbox socket",
		},
		{
			reason: localpb.NotRegisteredReason_NOT_REGISTERED_REASON_NAME_UNUSABLE,
			want:   "the session name is unusable (empty, or contains @ or *); rename the session",
		},
		{
			reason: localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SOCKET_NOT_OWNED,
			want:   "the inbox socket is not owned by this account",
		},
		{
			reason: localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SOCKET_PATH_MISSING,
			want:   "the session had no inbox socket when this MCP server started; reconnect the MCP server with /mcp",
		},
		{
			reason: localpb.NotRegisteredReason_NOT_REGISTERED_REASON_ACCOUNT_HELPER_FAILED,
			want:   "the Broker's helper for this account failed to start or stopped responding; ask the administrator to check the Broker log",
		},
		{
			reason: localpb.NotRegisteredReason_NOT_REGISTERED_REASON_UNSPECIFIED,
			want:   "registration is still in progress",
		},
	}
	for _, tt := range tests {
		h := newHandlers(Status{Phase: PhaseConnected, NotRegistered: tt.reason})
		_, err := h.preflight("list_agents", callGeneric, true)
		if err == nil {
			t.Fatalf("reason %v: preflight returned no error", tt.reason)
		}
		got := err.Error()
		if !strings.Contains(got, tt.want) {
			t.Errorf("reason %v: error %q does not contain %q", tt.reason, got, tt.want)
		}
		if !strings.HasSuffix(got, "(code: not_registered)") {
			t.Errorf("reason %v: error %q does not end with the code suffix", tt.reason, got)
		}
	}
}

// TestPreflightAccountHelperFailure checks the text for a helper the Broker
// could not start.
func TestPreflightAccountHelperFailure(t *testing.T) {
	h := newHandlers(Status{
		Phase:         PhaseConnected,
		NotRegistered: localpb.NotRegisteredReason_NOT_REGISTERED_REASON_ACCOUNT_HELPER_FAILED,
	})
	_, err := h.preflight("list_agents", callGeneric, true)
	if err == nil {
		t.Fatal("preflight returned no error")
	}
	want := "list_agents failed: this session is not registered as an agent: " +
		"the Broker's helper for this account failed to start or stopped responding; ask the administrator to check the Broker log (code: not_registered)"
	if got := err.Error(); got != want {
		t.Errorf("preflight error = %q, want %q", got, want)
	}
	if strings.Contains(err.Error(), "no Claude Code session record") {
		t.Errorf("error %q describes a missing session record, want the helper failure", err)
	}
}

// TestPreflightBrokerDegradedWithoutDetail checks the sentence and code when a
// degraded Broker sends no detail.
func TestPreflightBrokerDegradedWithoutDetail(t *testing.T) {
	h := newHandlers(Status{
		Phase:         PhaseConnected,
		NotRegistered: localpb.NotRegisteredReason_NOT_REGISTERED_REASON_BROKER_DEGRADED,
	})
	_, err := h.preflight("list_agents", callGeneric, true)
	if err == nil {
		t.Fatal("preflight returned no error")
	}
	want := "list_agents failed: the Broker is running in a degraded, diagnostics-only mode; it cannot serve this call until the startup failure is fixed (code: broker_degraded)"
	if got := err.Error(); got != want {
		t.Errorf("preflight error = %q, want %q", got, want)
	}
}

// TestPreflightNotRegisteredDetail checks that a Broker-supplied detail wins
// over the general sentence for the reason.
func TestPreflightNotRegisteredDetail(t *testing.T) {
	t.Run("detail wins", func(t *testing.T) {
		h := newHandlers(Status{
			Phase:               PhaseConnected,
			NotRegistered:       localpb.NotRegisteredReason_NOT_REGISTERED_REASON_NAME_UNUSABLE,
			NotRegisteredDetail: `the session name contains "@"`,
		})
		_, err := h.preflight("list_agents", callGeneric, true)
		if err == nil {
			t.Fatal("preflight returned no error")
		}
		want := `list_agents failed: this session is not registered as an agent: the session name contains "@" (code: not_registered)`
		if got := err.Error(); got != want {
			t.Errorf("preflight error = %q, want %q", got, want)
		}
	})

	t.Run("no detail keeps the sentence", func(t *testing.T) {
		h := newHandlers(Status{
			Phase:         PhaseConnected,
			NotRegistered: localpb.NotRegisteredReason_NOT_REGISTERED_REASON_NAME_UNUSABLE,
		})
		_, err := h.preflight("list_agents", callGeneric, true)
		if err == nil {
			t.Fatal("preflight returned no error")
		}
		want := "list_agents failed: this session is not registered as an agent: " +
			"the session name is unusable (empty, or contains @ or *); rename the session (code: not_registered)"
		if got := err.Error(); got != want {
			t.Errorf("preflight error = %q, want %q", got, want)
		}
	})
}

// TestRPCError checks the conversion of Local RPC failures, including the
// phrases for sends and pairings and the permission hints.
func TestRPCError(t *testing.T) {
	fp := bytes.Repeat([]byte{0xab}, 32)
	fpHex := hex.EncodeToString(fp)

	tests := []struct {
		name          string
		tool          string
		kind          callKind
		perm          permNeed
		err           error
		wantExact     string
		wantPrefix    string
		wantContains  []string
		wantNotPrefix string
	}{
		{
			name:       "send delivery unknown",
			tool:       "send_message",
			kind:       callSend,
			err:        detailStatus(codes.FailedPrecondition, &localpb.ErrorDetail{Code: localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN, Message: "no answer"}),
			wantExact:  "Delivery unknown: no answer (code: delivery_unknown)",
			wantPrefix: "Delivery unknown:",
		},
		{
			name:       "send not visible",
			tool:       "send_message",
			kind:       callSend,
			err:        detailStatus(codes.FailedPrecondition, &localpb.ErrorDetail{Code: localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE, Message: "no shared topic"}),
			wantExact:  "Not sent: no shared topic (code: not_visible)",
			wantPrefix: "Not sent:",
		},
		{
			name:       "connect pairing rejected",
			tool:       "connect_broker",
			kind:       callConnect,
			err:        detailStatus(codes.FailedPrecondition, &localpb.ErrorDetail{Code: localpb.ErrorCode_ERROR_CODE_PAIRING_REJECTED, Message: "key expired"}),
			wantExact:  "Not connected: key expired (code: pairing_rejected)",
			wantPrefix: "Not connected:",
		},
		{
			name:         "generic permission denied read",
			tool:         "get_config",
			kind:         callGeneric,
			perm:         permRead,
			err:          detailStatus(codes.PermissionDenied, &localpb.ErrorDetail{Code: localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED, Message: "denied"}),
			wantExact:    "get_config failed: denied (code: permission_denied). The calling account needs read permission on the Broker's config file.",
			wantContains: []string{"read permission"},
		},
		{
			name:         "generic permission denied write",
			tool:         "set_config",
			kind:         callGeneric,
			perm:         permWrite,
			err:          detailStatus(codes.PermissionDenied, &localpb.ErrorDetail{Code: localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED, Message: "denied"}),
			wantExact:    "set_config failed: denied (code: permission_denied). The calling account needs write permission on the Broker's config file.",
			wantContains: []string{"write permission"},
		},
		{
			name:       "fingerprint reported",
			tool:       "list_agents",
			kind:       callGeneric,
			err:        detailStatus(codes.FailedPrecondition, &localpb.ErrorDetail{Code: localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE, Message: "m", OriginatingBrokerFingerprint: fp}),
			wantExact:  "list_agents failed: m (code: not_visible; reported by Broker " + fpHex + ")",
			wantPrefix: "list_agents failed:",
		},
		{
			name:       "send without details",
			tool:       "send_message",
			kind:       callSend,
			err:        status.Error(codes.Unavailable, "connection lost"),
			wantExact:  "Delivery unknown: " + textSendLost + " (code: delivery_unknown)",
			wantPrefix: "Delivery unknown:",
		},
		{
			name:      "connect without details",
			tool:      "connect_broker",
			kind:      callConnect,
			err:       status.Error(codes.Unavailable, "connection lost"),
			wantExact: "Not connected: " + textConnectLost + " (code: internal)",
		},
		{
			name:      "generic without details",
			tool:      "list_agents",
			kind:      callGeneric,
			err:       status.Error(codes.Unavailable, "connection lost"),
			wantExact: "list_agents failed: the Broker did not answer: connection lost (code: internal)",
		},
		{
			name:      "version mismatch",
			tool:      "list_agents",
			kind:      callGeneric,
			err:       detailStatus(codes.FailedPrecondition, &localpb.ErrorDetail{Code: localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH, Message: "incompatible"}),
			wantExact: "list_agents failed: incompatible (code: version_mismatch)",
		},
		{
			name: "send to an offline peer",
			tool: "send_message",
			kind: callSend,
			err: detailStatus(codes.FailedPrecondition, &localpb.ErrorDetail{
				Code:                         localpb.ErrorCode_ERROR_CODE_BROKER_OFFLINE,
				Message:                      "Broker hostB is offline and refuses messages from other hosts",
				OriginatingBrokerFingerprint: fp,
			}),
			wantExact: "Not sent: Broker hostB is offline and refuses messages from other hosts (code: broker_offline; reported by Broker " + fpHex + "). " + textPeerOffline,
		},
		{
			name:      "create a pairing key on a local-only Broker",
			tool:      "create_pairing_key",
			kind:      callGeneric,
			err:       detailStatus(codes.FailedPrecondition, &localpb.ErrorDetail{Code: localpb.ErrorCode_ERROR_CODE_BROKER_DEGRADED, Message: "listen address :17443 is unavailable; fix broker.listen"}),
			wantExact: "create_pairing_key failed: listen address :17443 is unavailable; fix broker.listen (code: broker_degraded)",
		},
		{
			name:      "a call the degraded Broker refuses",
			tool:      "list_brokers",
			kind:      callGeneric,
			err:       detailStatus(codes.FailedPrecondition, &localpb.ErrorDetail{Code: localpb.ErrorCode_ERROR_CODE_BROKER_DEGRADED, Message: "config parse failed"}),
			wantExact: "list_brokers failed: config parse failed (code: broker_degraded)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := rpcError(tt.tool, tt.kind, tt.perm, tt.err)
			if err == nil {
				t.Fatal("rpcError returned nil")
			}
			got := err.Error()
			if tt.wantExact != "" && got != tt.wantExact {
				t.Errorf("rpcError = %q, want %q", got, tt.wantExact)
			}
			if tt.wantPrefix != "" && !strings.HasPrefix(got, tt.wantPrefix) {
				t.Errorf("rpcError = %q, want prefix %q", got, tt.wantPrefix)
			}
			for _, want := range tt.wantContains {
				if !strings.Contains(got, want) {
					t.Errorf("rpcError = %q, want it to contain %q", got, want)
				}
			}
		})
	}
}

// TestErrorSuffixFormat checks the code and fingerprint suffix that end-to-end
// tests parse.
func TestErrorSuffixFormat(t *testing.T) {
	fp := bytes.Repeat([]byte{0x0f}, 32)
	fpHex := hex.EncodeToString(fp)
	if len(fpHex) != 64 {
		t.Fatalf("fingerprint hex length = %d, want 64", len(fpHex))
	}

	t.Run("code and fingerprint", func(t *testing.T) {
		err := rpcError("list_agents", callGeneric, permNone, detailStatus(codes.FailedPrecondition, &localpb.ErrorDetail{
			Code:                         localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE,
			Message:                      "m",
			OriginatingBrokerFingerprint: fp,
		}))
		want := "list_agents failed: m (code: not_visible; reported by Broker " + fpHex + ")"
		if got := err.Error(); got != want {
			t.Errorf("error = %q, want %q", got, want)
		}
	})

	t.Run("code only", func(t *testing.T) {
		err := rpcError("list_agents", callGeneric, permNone, detailStatus(codes.FailedPrecondition, &localpb.ErrorDetail{
			Code:    localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE,
			Message: "m",
		}))
		if got := err.Error(); !strings.HasSuffix(got, " (code: not_visible)") {
			t.Errorf("error = %q, want it to end with %q", got, " (code: not_visible)")
		}
	})

	t.Run("hint after both suffixes", func(t *testing.T) {
		err := rpcError("get_config", callGeneric, permRead, detailStatus(codes.PermissionDenied, &localpb.ErrorDetail{
			Code:                         localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED,
			Message:                      "denied",
			OriginatingBrokerFingerprint: fp,
		}))
		want := "get_config failed: denied (code: permission_denied; reported by Broker " + fpHex + "). " + textNeedRead
		if got := err.Error(); got != want {
			t.Errorf("error = %q, want %q", got, want)
		}
	})
}

// TestRPCErrorNoDoublePrefix checks that a message which already starts with a
// result phrase is not prefixed a second time.
func TestRPCErrorNoDoublePrefix(t *testing.T) {
	t.Run("not sent", func(t *testing.T) {
		err := rpcError("send_message", callSend, permNone, detailStatus(codes.NotFound, &localpb.ErrorDetail{
			Code:    localpb.ErrorCode_ERROR_CODE_NOT_FOUND,
			Message: "Not sent: no agent named x on hostB",
		}))
		want := "Not sent: no agent named x on hostB (code: not_found)"
		if got := err.Error(); got != want {
			t.Errorf("error = %q, want %q", got, want)
		}
	})

	t.Run("delivery unknown", func(t *testing.T) {
		err := rpcError("send_message", callSend, permNone, detailStatus(codes.FailedPrecondition, &localpb.ErrorDetail{
			Code:    localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN,
			Message: "Delivery unknown: no reply",
		}))
		want := "Delivery unknown: no reply (code: delivery_unknown)"
		if got := err.Error(); got != want {
			t.Errorf("error = %q, want %q", got, want)
		}
	})

	t.Run("not connected", func(t *testing.T) {
		err := rpcError("connect_broker", callConnect, permNone, detailStatus(codes.FailedPrecondition, &localpb.ErrorDetail{
			Code:    localpb.ErrorCode_ERROR_CODE_PAIRING_REJECTED,
			Message: "Not connected: key expired",
		}))
		want := "Not connected: key expired (code: pairing_rejected)"
		if got := err.Error(); got != want {
			t.Errorf("error = %q, want %q", got, want)
		}
	})

	t.Run("plain message gets the phrase", func(t *testing.T) {
		err := rpcError("send_message", callSend, permNone, detailStatus(codes.NotFound, &localpb.ErrorDetail{
			Code:    localpb.ErrorCode_ERROR_CODE_NOT_FOUND,
			Message: "no agent named x",
		}))
		want := "Not sent: no agent named x (code: not_found)"
		if got := err.Error(); got != want {
			t.Errorf("error = %q, want %q", got, want)
		}
	})
}

// TestRPCErrorVersionMismatchHasNoRestartHint checks that rpcError appends no
// restart hint to a version_mismatch error.
func TestRPCErrorVersionMismatchHasNoRestartHint(t *testing.T) {
	fp := bytes.Repeat([]byte{0x3c}, 32)
	fpHex := hex.EncodeToString(fp)

	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "local Broker",
			err: detailStatus(codes.FailedPrecondition, &localpb.ErrorDetail{
				Code:    localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH,
				Message: "the running Broker speaks local protocol version 2, restart it",
			}),
			want: "list_agents failed: the running Broker speaks local protocol version 2, restart it (code: version_mismatch)",
		},
		{
			name: "reported by a peer Broker",
			err: detailStatus(codes.FailedPrecondition, &localpb.ErrorDetail{
				Code:                         localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH,
				Message:                      "link protocol version mismatch",
				OriginatingBrokerFingerprint: fp,
			}),
			want: "list_agents failed: link protocol version mismatch (code: version_mismatch; reported by Broker " + fpHex + ")",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rpcError("list_agents", callGeneric, permNone, tt.err).Error()
			if got != tt.want {
				t.Errorf("error = %q, want %q", got, tt.want)
			}
			if strings.Contains(got, "Restart the Broker") {
				t.Errorf("error %q carries a restart hint appended by rpcError", got)
			}
		})
	}
}

// TestRPCErrorOpeningPhraseFollowsCode checks that the opening phrase follows
// the error code and call kind, not the Broker's message.
func TestRPCErrorOpeningPhraseFollowsCode(t *testing.T) {
	tests := []struct {
		name string
		tool string
		kind callKind
		code localpb.ErrorCode
		msg  string
		want string
	}{
		{
			name: "send message says not connected",
			tool: "send_message",
			kind: callSend,
			code: localpb.ErrorCode_ERROR_CODE_NOT_FOUND,
			msg:  "Not connected: no agent named x",
			want: "Not sent: no agent named x (code: not_found)",
		},
		{
			name: "send result unknown says not sent",
			tool: "send_message",
			kind: callSend,
			code: localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN,
			msg:  "Not sent: no reply",
			want: "Delivery unknown: no reply (code: delivery_unknown)",
		},
		{
			name: "pairing says not sent",
			tool: "connect_broker",
			kind: callConnect,
			code: localpb.ErrorCode_ERROR_CODE_PAIRING_REJECTED,
			msg:  "Not sent: handshake broke",
			want: "Not connected: handshake broke (code: pairing_rejected)",
		},
		{
			name: "generic says delivery unknown",
			tool: "list_agents",
			kind: callGeneric,
			code: localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE,
			msg:  "Delivery unknown: m",
			want: "list_agents failed: m (code: not_visible)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := rpcError(tt.tool, tt.kind, permNone, detailStatus(codes.FailedPrecondition, &localpb.ErrorDetail{
				Code:    tt.code,
				Message: tt.msg,
			}))
			if got := err.Error(); got != tt.want {
				t.Errorf("error = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestCodeName checks every value of the closed error-code set.
func TestCodeName(t *testing.T) {
	tests := []struct {
		code localpb.ErrorCode
		want string
	}{
		{localpb.ErrorCode_ERROR_CODE_UNSPECIFIED, "internal"},
		{localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT, "invalid_argument"},
		{localpb.ErrorCode_ERROR_CODE_TOO_LARGE, "too_large"},
		{localpb.ErrorCode_ERROR_CODE_NOT_FOUND, "not_found"},
		{localpb.ErrorCode_ERROR_CODE_AMBIGUOUS, "ambiguous"},
		{localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE, "not_visible"},
		{localpb.ErrorCode_ERROR_CODE_PEER_UNREACHABLE, "peer_unreachable"},
		{localpb.ErrorCode_ERROR_CODE_INBOX_UNWRITABLE, "inbox_unwritable"},
		{localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN, "delivery_unknown"},
		{localpb.ErrorCode_ERROR_CODE_PAIRING_REJECTED, "pairing_rejected"},
		{localpb.ErrorCode_ERROR_CODE_NEEDS_LOCAL_ALIAS, "needs_local_alias"},
		{localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED, "permission_denied"},
		{localpb.ErrorCode_ERROR_CODE_NOT_REGISTERED, "not_registered"},
		{localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH, "version_mismatch"},
		{localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED, "quota_exceeded"},
		{localpb.ErrorCode_ERROR_CODE_INTERNAL, "internal"},
		{localpb.ErrorCode_ERROR_CODE_BROKER_OFFLINE, "broker_offline"},
		{localpb.ErrorCode_ERROR_CODE_BROKER_DEGRADED, "broker_degraded"},
	}
	if len(tests) != 18 {
		t.Fatalf("covered %d enum values, want 18", len(tests))
	}
	for _, tt := range tests {
		if got := codeName(tt.code); got != tt.want {
			t.Errorf("codeName(%v) = %q, want %q", tt.code, got, tt.want)
		}
	}
}
