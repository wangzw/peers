package helper

import (
	"context"
	"time"

	"github.com/wangzw/peers/internal/inbox"
)

// SessionOps is the set of inbox operations the account helper runs as its account.
type SessionOps interface {
	// Lookup returns the session whose record carries socketPath, or inbox.ErrNoRecord.
	Lookup(configDir, socketPath string) (inbox.Session, error)
	// Reread returns the current record of s, or inbox.ErrNoRecord or inbox.ErrPartial.
	Reread(s inbox.Session) (inbox.Session, error)
	// Watch reports changes under configDir through onChange and returns a function that stops it; an error resolving configDir or installing the file watcher is returned.
	Watch(configDir string, onChange func(pid int)) (stop func(), err error)
	// Deliver writes text to s's inbox socket under sig and msgID, bounded by timeout; it returns inbox.ErrNotWritten, inbox.ErrUncertain or inbox.ErrBadSignature.
	Deliver(ctx context.Context, s inbox.Session, sig, text, msgID string, timeout time.Duration) error
}

// InboxOps implements SessionOps with package inbox.
type InboxOps struct{}

// Lookup returns the session whose record carries socketPath, or inbox.ErrNoRecord.
func (InboxOps) Lookup(configDir, socketPath string) (inbox.Session, error) {
	return inbox.Lookup(configDir, socketPath)
}

// Reread returns the current record of s, or inbox.ErrNoRecord or inbox.ErrPartial.
func (InboxOps) Reread(s inbox.Session) (inbox.Session, error) {
	return inbox.Reread(s)
}

// Watch reports changes under configDir through onChange and returns a function that stops it; an error resolving configDir or installing the file watcher is returned.
func (InboxOps) Watch(configDir string, onChange func(pid int)) (stop func(), err error) {
	return inbox.Watch(configDir, onChange)
}

// Deliver writes text to s's inbox socket under sig and msgID, bounded by timeout; it returns inbox.ErrNotWritten, inbox.ErrUncertain or inbox.ErrBadSignature.
func (InboxOps) Deliver(ctx context.Context, s inbox.Session, sig, text, msgID string, timeout time.Duration) error {
	return inbox.Deliver(ctx, s, sig, text, msgID, timeout)
}

var _ SessionOps = InboxOps{}
