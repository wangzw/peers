package inbox

import (
	"context"
	"errors"
	"net"
	"strings"
	"time"

	ccsock "github.com/PeterSR/claude-code-socket-transport"
)

// Deliver writes one signed message to the session's inbox socket. It returns
// nil when the frame was written and the receiver closed the connection,
// ErrNotWritten when nothing was written, ErrUncertain when the frame may have
// been received, and ErrBadSignature when sig is empty, would change in the
// receiver's cleaning, or exceeds the receiver's byte limit; ErrBadSignature is
// decided before any write, so nothing is sent. msgID is the caller's message id
// and should be a canonical UUID. timeout bounds the whole delivery, including
// the dial, the write and waiting for the receiver to close, and an earlier ctx
// deadline wins.
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

// classifySendError maps the transport library's errors onto the delivery result
// sentinels. Encoding, argument checks and message-id generation run before the
// socket is opened, so their failures and the dial's are ErrNotWritten; the
// library does not report how many bytes it wrote, so any write-phase failure,
// named below or not, is ErrUncertain. The prefixes below are those of the
// vendored snapshot and must be updated when the library is upgraded.
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
	case strings.HasPrefix(msg, "ccsock: connecting to"):
		return ErrNotWritten
	case strings.HasPrefix(msg, "ccsock: encoding frame"),
		strings.HasPrefix(msg, "ccsock: frame is"),
		strings.HasPrefix(msg, "ccsock: message text is empty"),
		strings.HasPrefix(msg, "ccsock: message From"),
		strings.HasPrefix(msg, "ccsock: unknown priority"),
		strings.HasPrefix(msg, "ccsock: generating message id"):
		return ErrNotWritten
	case strings.HasPrefix(msg, "ccsock: waiting for"),
		errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, context.Canceled):
		return ErrUncertain
	case strings.HasPrefix(msg, "ccsock: half-closing"):
		return ErrUncertain
	case strings.HasPrefix(msg, "ccsock: writing to"):
		return ErrUncertain
	}
	return ErrUncertain
}
