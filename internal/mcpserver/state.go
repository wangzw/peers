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
	// the whole text shown to the model.
	PhaseUnavailable
	// PhaseNoBroker means no Broker is available for this account; Detail holds
	// the system Broker text when the session was using it, and is empty
	// otherwise.
	PhaseNoBroker
	// PhaseVersionMismatch means Hello found an incompatible local protocol
	// version; Detail names both versions and Remedy says how to fix it.
	PhaseVersionMismatch
	// PhaseBrokerRejected means the Broker answered but refused this connection
	// or registration; Detail is its explanation or the local refusal text, and
	// RejectCode is its code (internal when none).
	PhaseBrokerRejected
	// PhaseConnected means Hello succeeded; registration may still be pending.
	PhaseConnected
)

// Status is a snapshot of the background connection state.
type Status struct {
	Phase  Phase               // the state of the background connection to the Broker
	Detail string              // the phase's explanation text; see the Phase constants
	Remedy string              // PhaseVersionMismatch only: the "To fix it, ..." clause for the selected kind
	Client localpb.LocalClient // non-nil only in PhaseConnected
	// Conn is the gRPC connection behind Client, used by the update hint to
	// call UpdateStatus. It is non-nil only in PhaseConnected.
	Conn          *grpc.ClientConn
	AgentID       string                      // non-empty only after registration_complete
	Address       string                      // the agent address; empty until registration_complete
	NotRegistered localpb.NotRegisteredReason // last reason the Broker sent; UNSPECIFIED if none yet
	// NotRegisteredDetail is the Broker's specific explanation of
	// NotRegistered; empty when the Broker sent none.
	NotRegisteredDetail string
	// RejectCode is the error code the Broker gave for a rejection, read only
	// in PhaseBrokerRejected; internal when the refusal carried no ErrorDetail.
	RejectCode localpb.ErrorCode
}

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

type rememberedTopic struct {
	Name        string
	Public      bool
	Description string
}

// lockTopic and unlockTopic serialize a Broker membership change and its memory
// write with the restore of the same topic after a registration; lockTopic
// waits for the lock or returns the context's error.
type topicRecorder interface {
	Remember(t rememberedTopic)
	Forget(name string)
	lockTopic(ctx context.Context) error
	unlockTopic()
}

// handlerRPCTimeout bounds one Broker RPC of a tool whose Broker side does no
// network I/O and runs no timer; it equals topicRPCTimeout and
// termcmd.UnaryTimeout.
const handlerRPCTimeout = 10 * time.Second

func withRPCTimeout(ctx context.Context, timeout, fallback time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		timeout = fallback
	}
	return context.WithTimeout(ctx, timeout)
}

type handlers struct {
	st                *statusStore
	topics            topicRecorder
	log               *slog.Logger
	sessionTopics     func(context.Context) []string
	rpcTimeout        time.Duration
	listAgentsTimeout time.Duration
	connectTimeout    time.Duration
	sendTimeout       time.Duration
	updateTimeout     time.Duration
}

// preflight returns the Status a tool call may use, or the error whose text is
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
