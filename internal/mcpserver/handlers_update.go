package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// Texts of update_broker results and errors. The result sentences and the
// "Upgrade failed" opening phrase are the wording of design-mcpserver.
const (
	textUpdateRestarting = "Upgrading the Broker from %s to %s; the Broker will restart and the connection will recover automatically. " +
		"Reopen this session to use the new MCP server."
	textUpdateCurrent  = "The Broker is already up to date (%s)."
	textUpdateFailed   = "Upgrade failed"
	textUpdateTerminal = "Run `peers update` in a terminal instead."
	textUpdateAborted  = "an update is already in progress"
	// textUpdateTimeoutReason states that the Broker did not finish within the
	// bound. It is the body of the tool error, which adds the code.
	// textUpdateTimeoutOutcome names the two states the Broker can be left in
	// and how to check which one it is. The deadline reaches ApplyUpdate, so
	// the install either saw the cancellation and rolled back, leaving the
	// running version unchanged, or had already completed and the Broker is
	// restarting with the new version.
	textUpdateTimeoutReason  = "the Broker did not complete the upgrade within 4 minutes"
	textUpdateTimeoutOutcome = "The upgrade was either abandoned, leaving the running version unchanged (a replacement that had started is rolled back), or completed, leaving the Broker restarting with the new version. To check the result, run `peers --version`, run `peers service status`, or run `peers update` in a terminal."
)

// updateBrokerRPCTimeout bounds update_broker. The Broker spends at most 30s
// looking up the available version (updateCheckRequestTimeout), 30s on the
// preflight checks (precheckTimeout), about 100s downloading a 100 MiB release
// at 1 MiB/s and 30s on the post-update step (postUpdateTimeout); the remaining
// 50s is margin. The text the handler returns after this bound is
// textUpdateTimeoutReason, whose "4 minutes" must stay in step with this value.
// The deadline reaches the Broker's ApplyUpdate, so reaching this bound
// cancels the install: a replacement that had started is rolled back and the
// running version is unchanged. That is the expected behavior; the bound
// exists so that a slow or stuck upgrade does not leave the tool call open.
const updateBrokerRPCTimeout = 4 * time.Minute

// UpdateBroker asks the Broker to upgrade itself to the latest release and
// restart. It returns as soon as the Broker answers: the Broker sends the reply
// before it schedules the restart, so the result describes what is about to
// happen rather than what has finished. No agent identity is needed; the Broker
// judges the caller by the account on the other end of the local socket.
func (h *handlers) UpdateBroker(ctx context.Context, in NoArgs) (UpdateBrokerOut, error) {
	st, err := h.preflight("update_broker", callGeneric, false)
	if err != nil {
		return UpdateBrokerOut{}, err
	}

	ctx, cancel := withRPCTimeout(ctx, h.updateTimeout, updateBrokerRPCTimeout)
	defer cancel()
	resp, err := st.Client.ApplyUpdate(ctx, &localpb.ApplyUpdateRequest{})
	if err != nil {
		return UpdateBrokerOut{}, updateBrokerError(err)
	}

	out := UpdateBrokerOut{
		FromVersion: resp.GetFromVersion(),
		ToVersion:   resp.GetToVersion(),
		Restarting:  resp.GetRestarting(),
	}
	if out.Restarting {
		out.Result = fmt.Sprintf(textUpdateRestarting, out.FromVersion, out.ToVersion)
	} else {
		out.Result = fmt.Sprintf(textUpdateCurrent, out.FromVersion)
	}
	return out, nil
}

// updateBrokerError turns an ApplyUpdate failure into the text shown to the
// model. The Broker describes these outcomes with bare gRPC status codes rather
// than an ErrorDetail, so the status code selects the text; every other failure
// goes through the shared mapping.
func updateBrokerError(err error) error {
	s := status.Convert(err)
	message := stripResultPrefix(s.Message())
	switch s.Code() {
	case codes.PermissionDenied:
		if message == "" {
			message = "permission denied"
		}
		return errors.New(toolError(textUpdateFailed, message, localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED).Error() + ". " + textNeedWrite)
	case codes.FailedPrecondition:
		if message == "" {
			message = "the Broker cannot update itself in its current state"
		}
		return fmt.Errorf("%s: %s (code: failed_precondition). %s", textUpdateFailed, message, textUpdateTerminal)
	case codes.Aborted:
		if message == "" {
			message = textUpdateAborted
		}
		return fmt.Errorf("%s: %s (code: aborted)", textUpdateFailed, message)
	case codes.DeadlineExceeded:
		// The reason goes through toolError like the other cases, which adds
		// the code; the outcome sentence is appended as prose addressed to the
		// reader.
		return errors.New(toolError(textUpdateFailed, textUpdateTimeoutReason, localpb.ErrorCode_ERROR_CODE_INTERNAL).Error() + ". " + textUpdateTimeoutOutcome)
	default:
		return rpcError("update_broker", callUpdate, permWrite, err)
	}
}
