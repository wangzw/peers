package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakelocal"
)

// TestBrokerStatusNames names each Broker status in words.
func TestBrokerStatusNames(t *testing.T) {
	tests := []struct {
		in   localpb.BrokerStatus
		want string
	}{
		{localpb.BrokerStatus_BROKER_STATUS_CONNECTED, "connected"},
		{localpb.BrokerStatus_BROKER_STATUS_CONNECTABLE, "connectable"},
		{localpb.BrokerStatus_BROKER_STATUS_NOT_DIRECTLY_REACHABLE, "not_directly_reachable"},
		{localpb.BrokerStatus_BROKER_STATUS_UNSPECIFIED, ""},
	}
	for _, tt := range tests {
		if got := brokerStatusName(tt.in); got != tt.want {
			t.Fatalf("brokerStatusName(%v) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// TestBrokerRoleNames names each peer role in words.
func TestBrokerRoleNames(t *testing.T) {
	tests := []struct {
		in   localpb.PeerRole
		want string
	}{
		{localpb.PeerRole_PEER_ROLE_DIALER, "dialer"},
		{localpb.PeerRole_PEER_ROLE_ACCEPTOR, "acceptor"},
		{localpb.PeerRole_PEER_ROLE_UNSPECIFIED, ""},
	}
	for _, tt := range tests {
		if got := peerRoleName(tt.in); got != tt.want {
			t.Fatalf("peerRoleName(%v) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// TestBrokerLinkStateNames names each link state in words.
func TestBrokerLinkStateNames(t *testing.T) {
	tests := []struct {
		in   localpb.LinkState
		want string
	}{
		{localpb.LinkState_LINK_STATE_DIALING, "dialing"},
		{localpb.LinkState_LINK_STATE_WAITING, "waiting"},
		{localpb.LinkState_LINK_STATE_HANDSHAKING, "handshaking"},
		{localpb.LinkState_LINK_STATE_ESTABLISHED, "established"},
		{localpb.LinkState_LINK_STATE_BACKOFF, "backoff"},
		{localpb.LinkState_LINK_STATE_REJECTED, "rejected"},
		{localpb.LinkState_LINK_STATE_UNSPECIFIED, ""},
	}
	for _, tt := range tests {
		if got := linkStateName(tt.in); got != tt.want {
			t.Fatalf("linkStateName(%v) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// TestHandlerPreflightErrors checks that the broker, config and update tools fail
// with their opening phrase and the starting text before the Broker is ready.
func TestHandlerPreflightErrors(t *testing.T) {
	h := newHandlers(Status{Phase: PhaseStarting})
	tests := []struct {
		name  string
		run   func() error
		first string
	}{
		{"ListBrokers", func() error { _, err := h.ListBrokers(t.Context(), NoArgs{}); return err }, "list_brokers failed"},
		{"ConnectBroker", func() error {
			_, err := h.ConnectBroker(t.Context(), ConnectBrokerIn{Address: "a", Key: "k"})
			return err
		}, "Not connected"},
		{"DisconnectBroker", func() error { _, err := h.DisconnectBroker(t.Context(), DisconnectBrokerIn{}); return err }, "disconnect_broker failed"},
		{"CreatePairingKey", func() error { _, err := h.CreatePairingKey(t.Context(), NoArgs{}); return err }, "create_pairing_key failed"},
		{"GetConfig", func() error { _, err := h.GetConfig(t.Context(), GetConfigIn{}); return err }, "get_config failed"},
		{"SetConfig", func() error { _, err := h.SetConfig(t.Context(), SetConfigIn{}); return err }, "set_config failed"},
		{"SetBrokerOnline", func() error { _, err := h.SetBrokerOnline(t.Context(), SetBrokerOnlineIn{}); return err }, "set_broker_online failed"},
		{"UpdateBroker", func() error { _, err := h.UpdateBroker(t.Context(), NoArgs{}); return err }, "update_broker failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.run()
			if err == nil {
				t.Fatalf("%s returned nil error before the Broker is ready", tt.name)
			}
			if !strings.Contains(err.Error(), tt.first) || !strings.Contains(err.Error(), textStarting) {
				t.Fatalf("%s error = %q, want it to start with %q and name the starting state", tt.name, err, tt.first)
			}
		})
	}
}

// TestUpdateBrokerErrorEmptyMessages checks the sentence permission denied, failed
// precondition and aborted each produce for an empty message, and that other
// status codes fall back to the generic text.
func TestUpdateBrokerErrorEmptyMessages(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "permission denied",
			err:  status.Error(codes.PermissionDenied, ""),
			want: toolError(textUpdateFailed, "permission denied", localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED).Error() + ". " + textNeedWrite,
		},
		{
			name: "failed precondition",
			err:  status.Error(codes.FailedPrecondition, ""),
			want: fmt.Sprintf("%s: %s (code: failed_precondition). %s", textUpdateFailed, "the Broker cannot update itself in its current state", textUpdateTerminal),
		},
		{
			name: "aborted",
			err:  status.Error(codes.Aborted, ""),
			want: fmt.Sprintf("%s: %s (code: aborted)", textUpdateFailed, textUpdateAborted),
		},
		{
			name: "other",
			err:  status.Error(codes.Internal, "boom"),
			want: fmt.Sprintf("%s: %s (code: internal)", textUpdateFailed, fmt.Sprintf(textGenericLost, "boom")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := updateBrokerError(tt.err)
			if err == nil || err.Error() != tt.want {
				t.Fatalf("updateBrokerError = %q, want %q", err, tt.want)
			}
		})
	}
}

type lockErrRecorder struct {
	*topicRecorderStub
	err error
}

func (r *lockErrRecorder) lockTopic(context.Context) error { return r.err }

// TestTopicHandlersReportLockErrors reports a failed topic lock with the tool's
// opening phrase.
func TestTopicHandlersReportLockErrors(t *testing.T) {
	errLock := errors.New("the topic lock is busy")

	t.Run("named join", func(t *testing.T) {
		h, _, _ := newTopicsHandlers(t, fakelocal.Handlers{}, "agent-1")
		h.topics = &lockErrRecorder{topicRecorderStub: &topicRecorderStub{}, err: errLock}
		_, err := h.JoinTopic(t.Context(), JoinTopicIn{Name: "x"})
		if err == nil || !strings.Contains(err.Error(), "join_topic failed") || !strings.Contains(err.Error(), errLock.Error()) {
			t.Fatalf("JoinTopic error = %v, want the lock failure", err)
		}
	})

	t.Run("default join", func(t *testing.T) {
		h, _, _ := newTopicsHandlers(t, fakelocal.Handlers{}, "agent-1")
		h.sessionTopics = func(context.Context) []string { return []string{"x"} }
		h.topics = &lockErrRecorder{topicRecorderStub: &topicRecorderStub{}, err: errLock}
		_, err := h.JoinTopic(t.Context(), JoinTopicIn{})
		if err == nil || !strings.Contains(err.Error(), "join_topic failed") || !strings.Contains(err.Error(), errLock.Error()) {
			t.Fatalf("JoinTopic(default) error = %v, want the lock failure", err)
		}
	})

	t.Run("default leave", func(t *testing.T) {
		h, _, _ := newTopicsHandlers(t, fakelocal.Handlers{}, "agent-1")
		h.sessionTopics = func(context.Context) []string { return []string{"x"} }
		h.topics = &lockErrRecorder{topicRecorderStub: &topicRecorderStub{}, err: errLock}
		_, err := h.LeaveTopic(t.Context(), LeaveTopicIn{})
		if err == nil || !strings.Contains(err.Error(), "leave_topic failed") || !strings.Contains(err.Error(), errLock.Error()) {
			t.Fatalf("LeaveTopic(default) error = %v, want the lock failure", err)
		}
	})
}

// TestLeaveDefaultTopicsReportsListError reports a failed ListTopics during a
// nameless leave with the leave_topic opening phrase.
func TestLeaveDefaultTopicsReportsListError(t *testing.T) {
	h, _, _ := newTopicsHandlers(t, fakelocal.Handlers{
		ListTopics: func(context.Context, *localpb.ListTopicsRequest) (*localpb.ListTopicsResponse, error) {
			return nil, status.Error(codes.Unavailable, "list unavailable")
		},
	}, "agent-1")
	h.sessionTopics = func(context.Context) []string { return []string{"x"} }

	_, err := h.LeaveTopic(t.Context(), LeaveTopicIn{})
	if err == nil || !strings.Contains(err.Error(), "leave_topic failed") {
		t.Fatalf("LeaveTopic error = %v, want the leave_topic failure", err)
	}
	if !strings.Contains(err.Error(), "list unavailable") {
		t.Fatalf("LeaveTopic error = %v, want the listing failure", err)
	}
}
