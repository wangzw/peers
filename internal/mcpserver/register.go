package mcpserver

import (
	"context"
	"log/slog"
	"sync"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// completeFunc runs in its own goroutine after each registration_complete; calls
// are serialized and a newer registration cancels the previous context.
type completeFunc func(ctx context.Context, c localpb.LocalClient, agentID string, autoJoin bool)

func runRegistration(ctx context.Context, c localpb.LocalClient, first *localpb.RegisterRequest,
	st *statusStore, onComplete completeFunc, log *slog.Logger) error {

	streamCtx, cancel := context.WithCancel(ctx)
	// Only the goroutine reading the stream touches regCancel, so it needs no
	// lock; it holds the context of the registration that is currently valid,
	// not of the stream.
	var regCancel context.CancelFunc
	stopGeneration := func() {
		if regCancel != nil {
			regCancel()
			regCancel = nil
		}
	}
	// Deferred in this order so that every onComplete sees its ctx cancelled
	// before wg.Wait, and no stale identity survives after return.
	var wg sync.WaitGroup
	defer clearRegistration(st)
	defer func() {
		// Publish the lost connection before waiting for the in-flight
		// completion and before clearing the identity, so a tool call meanwhile
		// is told the connection is gone.
		stopGeneration()
		loopSetStatus(st, PhaseUnavailable, unavailableText(loopLostText), "", nil)
		wg.Wait()
	}()
	defer cancel()

	stream, err := c.Register(streamCtx)
	if err != nil {
		return err
	}
	if err := stream.Send(first); err != nil {
		_ = stream.CloseSend()
		return err
	}

	serial := make(chan struct{}, 1)

	for {
		resp, err := stream.Recv()
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				_ = stream.CloseSend()
				return ctxErr
			}
			return err
		}

		switch event := resp.GetEvent().(type) {
		case *localpb.RegisterResponse_RegistrationComplete:
			complete := event.RegistrationComplete
			agentID := complete.GetAgentId()
			st.Update(func(s *Status) {
				s.AgentID = agentID
				s.Address = complete.GetAgentAddress()
				s.NotRegistered = localpb.NotRegisteredReason_NOT_REGISTERED_REASON_UNSPECIFIED
				s.NotRegisteredDetail = ""
			})
			autoJoin := complete.GetAutoJoin()
			stopGeneration()
			regCtx, cancelReg := context.WithCancel(streamCtx)
			regCancel = cancelReg
			wg.Add(1)
			go func() {
				defer wg.Done()
				runCompletion(regCtx, serial, func() {
					onComplete(regCtx, c, agentID, autoJoin)
				})
			}()

		case *localpb.RegisterResponse_NotRegistered:
			// A revoked registration stops its restore so none keeps joining as
			// the revoked identity.
			stopGeneration()
			reason := event.NotRegistered.GetReason()
			detail := event.NotRegistered.GetDetail()
			st.Update(func(s *Status) {
				s.AgentID = ""
				s.Address = ""
				s.NotRegistered = reason
				s.NotRegisteredDetail = detail
			})

		case *localpb.RegisterResponse_AgentChanged:
			changed := event.AgentChanged
			st.Update(func(s *Status) {
				if s.AgentID != "" && s.AgentID == changed.GetAgentId() {
					s.Address = changed.GetAgentAddress()
				}
			})

		default:
			if log != nil {
				log.Debug("ignoring an unknown Register event")
			}
		}
	}
}

// runCompletion runs f once the serial slot is free, unless the registration
// context is done first.
func runCompletion(regCtx context.Context, serial chan struct{}, f func()) {
	select {
	case serial <- struct{}{}:
	case <-regCtx.Done():
		return
	}
	defer func() { <-serial }()
	if regCtx.Err() != nil {
		// Both select cases can be ready at once, so do not run the callback for
		// a registration that is gone or has been replaced.
		return
	}
	f()
}

func clearRegistration(st *statusStore) {
	st.Update(func(s *Status) {
		s.AgentID = ""
		s.Address = ""
		s.NotRegistered = localpb.NotRegisteredReason_NOT_REGISTERED_REASON_UNSPECIFIED
		s.NotRegisteredDetail = ""
	})
}
