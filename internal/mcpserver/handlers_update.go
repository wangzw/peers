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

// Texts of update_broker results and errors.
const (
	textUpdateRestarting = "Upgrading the Broker from %s to %s; the Broker will restart and the connection will recover automatically. " +
		"Reopen this session to use the new MCP server."
	textUpdateCurrent        = "The Broker is already up to date (%s)."
	textUpdateFailed         = "Upgrade failed"
	textUpdateTerminal       = "Run `peers update` in a terminal instead."
	textUpdateAborted        = "an update is already in progress"
	textUpdateTimeoutReason  = "the Broker did not complete the upgrade within 4 minutes"
	textUpdateTimeoutOutcome = "The upgrade was either abandoned, leaving the running version unchanged (a replacement that had started is rolled back), or completed, leaving the Broker restarting with the new version. To check the result, run `peers --version`, run `peers service status`, or run `peers update` in a terminal."
)

// updateBrokerRPCTimeout bounds update_broker; its "4 minutes" must stay in
// step with textUpdateTimeoutReason. The deadline reaches the Broker's
// ApplyUpdate, so reaching it cancels the install and rolls back a replacement
// that had started.
const updateBrokerRPCTimeout = 4 * time.Minute

// UpdateBroker asks the Broker to upgrade itself and restart. It returns as
// soon as the Broker answers, so the result describes what is about to happen.
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
		return errors.New(toolError(textUpdateFailed, textUpdateTimeoutReason, localpb.ErrorCode_ERROR_CODE_INTERNAL).Error() + ". " + textUpdateTimeoutOutcome)
	default:
		return rpcError("update_broker", callUpdate, permWrite, err)
	}
}
