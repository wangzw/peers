package broker

import (
	"context"
	"time"

	"github.com/wangzw/peers/internal/inbox"
)

// sessionCallTimeout bounds one SessionBackend call, because an account's
// registry events are handled by one serial goroutine: a helper the account
// stopped must not hold that goroutine and every later event behind it.
const sessionCallTimeout = time.Second

// sessionCallContext bounds ctx by sessionCallTimeout for one SessionBackend
// call.
func sessionCallContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, sessionCallTimeout)
}

// SessionBackend performs inbox operations for one account: in-process for a
// user Broker, through a helper process running as the account for a system
// Broker. Callers pass the ctx of the Local RPC being served; an implementation
// that talks to a helper process must propagate it.
type SessionBackend interface {
	// Lookup returns the session record whose socket path is socketPath in
	// configDir.
	Lookup(ctx context.Context, configDir, socketPath string) (inbox.Session, error)
	// Reread returns the current record for a session.
	Reread(ctx context.Context, s inbox.Session) (inbox.Session, error)
	// Watch calls onChange for each change in configDir and returns a function
	// that stops watching.
	Watch(ctx context.Context, configDir string, onChange func(pid int)) (stop func(), err error)
	// Deliver writes the signed message to the session's inbox socket, bounded
	// by timeout.
	Deliver(ctx context.Context, s inbox.Session, sig, text, msgID string, timeout time.Duration) error
}

// SessionRouter hands out the SessionBackend of an account.
type SessionRouter interface {
	// Acquire returns the backend for uid and a release function the caller
	// must call when it stops using it; release is never nil and may be called
	// more than once. A router may share one backend between concurrent
	// holders, so release ends this caller's use rather than tearing the
	// backend down.
	Acquire(ctx context.Context, uid uint32) (b SessionBackend, release func(), err error)
}

// UserRouter returns the router of a user Broker, which drives the inbox
// package in-process; it is used when Deps.Sessions is nil.
func UserRouter() SessionRouter {
	return userRouter{}
}

type userBackend struct{}

func (userBackend) Lookup(_ context.Context, configDir, socketPath string) (inbox.Session, error) {
	return inbox.Lookup(configDir, socketPath)
}

func (userBackend) Reread(_ context.Context, s inbox.Session) (inbox.Session, error) {
	return inbox.Reread(s)
}

func (userBackend) Watch(_ context.Context, configDir string, onChange func(pid int)) (func(), error) {
	return inbox.Watch(configDir, onChange)
}

func (userBackend) Deliver(ctx context.Context, s inbox.Session, sig, text, msgID string, timeout time.Duration) error {
	return inbox.Deliver(ctx, s, sig, text, msgID, timeout)
}

type userRouter struct{}

func (userRouter) Acquire(context.Context, uint32) (SessionBackend, func(), error) {
	return userBackend{}, func() {}, nil
}
