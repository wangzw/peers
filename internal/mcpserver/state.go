package mcpserver

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// Phase is the state of the background connection to the Broker.
type Phase int

const (
	// PhaseStarting means nothing has been tried yet.
	PhaseStarting Phase = iota
	// PhaseUnavailable means Broker selection or connection failed; Detail is
	// the whole text shown to the model, including the reason and the
	// background retry.
	PhaseUnavailable
	// PhaseNoBroker means no Broker is available for this account; Detail is
	// empty for the general text and holds the text for a system Broker when
	// the session was using the system Broker.
	PhaseNoBroker
	// PhaseVersionMismatch means Hello found an incompatible local protocol
	// version; Detail names both versions and Remedy says how to fix it.
	PhaseVersionMismatch
	// PhaseBrokerRejected means the Broker answered but refused this
	// connection or registration. Detail is the Broker's own explanation when
	// it gave one and the local text for a refused handshake otherwise;
	// RejectCode is the Broker's code, or internal for a refusal without one.
	PhaseBrokerRejected
	// PhaseConnected means Hello succeeded; registration may still be pending.
	PhaseConnected
)

// Status is a snapshot of the background connection state.
type Status struct {
	Phase  Phase
	Detail string
	Remedy string              // PhaseVersionMismatch only: the "To fix it, ..." clause for the selected kind
	Client localpb.LocalClient // non-nil only in PhaseConnected
	// Conn is the gRPC connection behind Client, used by the update hint to
	// call UpdateStatus. It is non-nil only in PhaseConnected.
	Conn          *grpc.ClientConn
	AgentID       string // non-empty only after registration_complete
	Address       string
	NotRegistered localpb.NotRegisteredReason // last reason the Broker sent; UNSPECIFIED if none yet
	// NotRegisteredDetail is the Broker's specific explanation of
	// NotRegistered; empty when the Broker sent none.
	NotRegisteredDetail string
	// RejectCode is the error code the Broker gave for a rejection, read only
	// in PhaseBrokerRejected; internal when the refusal carried no ErrorDetail.
	RejectCode localpb.ErrorCode
}

// statusStore holds the current Status and is safe for concurrent use.
type statusStore struct {
	mu sync.Mutex
	st Status
}

// Get returns a copy of the current Status.
func (s *statusStore) Get() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.st
}

// Update calls f while holding the lock.
func (s *statusStore) Update(f func(*Status)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f(&s.st)
}

// rememberedTopic is a topic this process joined, kept so that a later
// registration can restore the membership.
type rememberedTopic struct {
	Name        string
	Public      bool
	Description string
}

// topicRecorder records and forgets joined topics. lockTopic and unlockTopic
// serialize a Broker membership change and the memory write it causes with the
// restore of the same topic after a registration. lockTopic waits for the lock
// or returns the context's error, so a call stops instead of queueing.
type topicRecorder interface {
	Remember(t rememberedTopic)
	Forget(name string)
	lockTopic(ctx context.Context) error
	unlockTopic()
}

// handlerRPCTimeout bounds one Broker RPC of a tool whose Broker side does no
// network I/O and runs no timer: get_config, set_config, set_broker_online,
// list_brokers, disconnect_broker and create_pairing_key read or write a small
// local file or answer from memory. The value matches topicRPCTimeout and
// termcmd.UnaryTimeout. Tests override it through handlers.rpcTimeout.
const handlerRPCTimeout = 10 * time.Second

// withRPCTimeout returns ctx bounded by timeout, using fallback when timeout is
// zero. The caller defers the cancel.
func withRPCTimeout(ctx context.Context, timeout, fallback time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		timeout = fallback
	}
	return context.WithTimeout(ctx, timeout)
}

// handlers is the shared skeleton of the forwarding tool handlers.
type handlers struct {
	st     *statusStore
	topics topicRecorder
	log    *slog.Logger
	// sessionTopics returns this session's default topics for a nameless
	// join_topic or leave_topic. nil means the session has none.
	sessionTopics func(context.Context) []string
	// rpcTimeout bounds one Broker RPC of a tool whose Broker side does no
	// network I/O and runs no timer. Zero means handlerRPCTimeout; tests set a
	// short value.
	rpcTimeout time.Duration
	// listAgentsTimeout bounds list_agents. Zero means listAgentsRPCTimeout.
	listAgentsTimeout time.Duration
	// connectTimeout bounds connect_broker. Zero means connectBrokerRPCTimeout.
	connectTimeout time.Duration
	// sendTimeout bounds send_message. Zero means sendMessageRPCTimeout.
	sendTimeout time.Duration
	// updateTimeout bounds update_broker. Zero means updateBrokerRPCTimeout.
	updateTimeout time.Duration
}

// preflight returns the Status a tool call may use, or an error whose text is
// shown to the model when the call must not reach the Broker.
func (h *handlers) preflight(tool string, kind callKind, needAgent bool) (Status, error) {
	st := h.st.Get()
	phrase := openingPhrase(tool, kind)
	switch st.Phase {
	case PhaseStarting:
		return st, toolError(phrase, textStarting, localpb.ErrorCode_ERROR_CODE_INTERNAL)
	case PhaseUnavailable:
		body := st.Detail
		if body == "" {
			body = textUnavailableNoReason
		}
		return st, toolError(phrase, body, localpb.ErrorCode_ERROR_CODE_INTERNAL)
	case PhaseNoBroker:
		body := st.Detail
		if body == "" {
			body = textNoBroker
		}
		return st, toolError(phrase, body, localpb.ErrorCode_ERROR_CODE_INTERNAL)
	case PhaseVersionMismatch:
		return st, toolError(phrase, fmt.Sprintf(textVersionMismatch, st.Detail, st.Remedy), localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH)
	case PhaseBrokerRejected:
		return st, toolError(phrase, st.Detail, st.RejectCode)
	case PhaseConnected:
		if needAgent && st.AgentID == "" {
			if st.NotRegistered == localpb.NotRegisteredReason_NOT_REGISTERED_REASON_BROKER_DEGRADED {
				return st, toolError(phrase, notRegisteredReason(st.NotRegistered, st.NotRegisteredDetail), localpb.ErrorCode_ERROR_CODE_BROKER_DEGRADED)
			}
			return st, toolError(phrase, fmt.Sprintf(textNotRegistered, notRegisteredReason(st.NotRegistered, st.NotRegisteredDetail)), localpb.ErrorCode_ERROR_CODE_NOT_REGISTERED)
		}
	}
	return st, nil
}
