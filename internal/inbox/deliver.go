package inbox

import (
	"context"
	"errors"
	"net"
	"strings"
	"time"

	ccsock "github.com/PeterSR/claude-code-socket-transport"
)

// Deliver writes one signed message to the session's inbox socket.
//
// It returns nil when the frame was written and the receiver closed the
// connection; ErrNotWritten when nothing was written; ErrUncertain when the
// frame may have been received; and ErrBadSignature when sig is empty, would be
// changed by the receiver, or exceeds the byte limit, in which case nothing is
// written.
func Deliver(ctx context.Context, s Session, sig, text, msgID string, timeout time.Duration) error {
	if sig == "" || CleanSignature(sig) != sig || len(sig) > maxFromNameBytes {
		return ErrBadSignature
	}
	if s.SocketPath == "" {
		return ErrNotWritten
	}

	client := &ccsock.Client{
		NoAuth:  true,
		Timeout: timeout,
	}
	_, err := client.SendToSocket(ctx, s.SocketPath, ccsock.Message{
		Text:      escapeBody(text),
		Priority:  ccsock.PriorityNext,
		FromName:  sig,
		From:      "",
		SessionID: "",
		MsgID:     msgID,
	})
	if err == nil {
		return nil
	}
	return classifySendError(err)
}

// classifySendError maps the transport library's errors onto the delivery
// result sentinels.
//
// The library does not export a classification and does not report how many
// bytes conn.Write wrote, so write-phase failures are conservatively treated as
// uncertain: retrying could deliver the message twice. The error prefixes are
// those of the vendored snapshot; the tests in deliver_test.go pin each one and
// fail first if the library is updated.
func classifySendError(err error) error {
	if err == nil {
		return nil
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		return ErrNotWritten
	}

	msg := err.Error()
	switch {
	// Connection establishment failed; nothing was written.
	case strings.HasPrefix(msg, "ccsock: connecting to"):
		return ErrNotWritten
	// Encoding and argument checks run before the socket is opened. The
	// message id is generated there too when the caller left it empty.
	case strings.HasPrefix(msg, "ccsock: encoding frame"),
		strings.HasPrefix(msg, "ccsock: frame is"),
		strings.HasPrefix(msg, "ccsock: message text is empty"),
		strings.HasPrefix(msg, "ccsock: message From"),
		strings.HasPrefix(msg, "ccsock: unknown priority"),
		strings.HasPrefix(msg, "ccsock: generating message id"):
		return ErrNotWritten
	// The line was written and the wait for the receiver's close timed out.
	case strings.HasPrefix(msg, "ccsock: waiting for"),
		errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, context.Canceled):
		return ErrUncertain
	// The half-close failed after the line was written.
	case strings.HasPrefix(msg, "ccsock: half-closing"):
		return ErrUncertain
	// A write-phase failure may have written some or all of the line.
	case strings.HasPrefix(msg, "ccsock: writing to"):
		return ErrUncertain
	case errors.As(err, &opErr):
		return ErrUncertain
	default:
		return ErrUncertain
	}
}
