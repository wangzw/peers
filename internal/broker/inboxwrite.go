package broker

import (
	"context"
	"errors"
	"time"

	"github.com/wangzw/peers/internal/inbox"
)

type writeOutcome int

const (
	// written means the frame was written and the receiver closed the
	// connection.
	written writeOutcome = iota
	// notWritten means nothing reached the session, so retrying is safe.
	notWritten
	// uncertain means the frame may have been received. Retrying could deliver
	// the message twice, so it is never retried.
	uncertain
)

func (d *delivery) writeInbox(ctx context.Context, deadline time.Time, target agentView, sig, text, msgID string) writeOutcome {
	settings := d.deps.Settings.Send()

	for attempt := 0; attempt <= settings.InboxRetries; attempt++ {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return notWritten
		}
		if target.Inbox == nil {
			d.logInboxError("no_inbox_writer", target)
			return notWritten
		}

		opCtx, cancel := context.WithDeadline(ctx, deadline)
		err := target.Inbox.Deliver(opCtx, target.Session, sig, text, msgID, min(settings.InboxTimeout, remaining))
		cancel()

		switch {
		case err == nil:
			return written
		case errors.Is(err, inbox.ErrUncertain):
			return uncertain
		case errors.Is(err, inbox.ErrBadSignature):
			// This Broker generates the signature, so a bad one is a defect
			// rather than a transient failure; retrying would repeat it.
			d.logInboxError("bad_signature", target)
			return notWritten
		case errors.Is(err, inbox.ErrNotWritten):
			if attempt == settings.InboxRetries {
				return notWritten
			}
			timer := time.NewTimer(min(settings.InboxBackoff, time.Until(deadline)))
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return notWritten
			}
		default:
			return uncertain
		}
	}
	return notWritten
}

func (d *delivery) logInboxError(event string, target agentView) {
	d.logger().Error(event, "target", target.Name)
}
