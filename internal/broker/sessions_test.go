package broker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/inbox"
)

// These compile-time checks keep the user backend and router satisfying their
// interfaces, and fail if the inbox signatures change.
var (
	_ SessionBackend = userBackend{}
	_ SessionRouter  = userRouter{}

	_ func(string, string) (inbox.Session, error)                                       = inbox.Lookup
	_ func(inbox.Session) (inbox.Session, error)                                        = inbox.Reread
	_ func(string, func(int)) (func(), error)                                           = inbox.Watch
	_ func(context.Context, inbox.Session, string, string, string, time.Duration) error = inbox.Deliver
)

// TestSessionBackendSignatures exists so the compile-time signature checks
// above have a named test in the suite.
func TestSessionBackendSignatures(t *testing.T) {}

// TestUserRouterServesAnyUID checks that the user router hands out a backend for
// any uid and that it forwards to inbox.
func TestUserRouterServesAnyUID(t *testing.T) {
	backend, release, err := UserRouter().Acquire(context.Background(), 4242)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if backend == nil {
		t.Fatal("Acquire returned a nil SessionBackend")
	}
	if release == nil {
		t.Fatal("Acquire returned a nil release function")
	}
	release()

	_, err = backend.Lookup(context.Background(), t.TempDir(), "")
	if !errors.Is(err, inbox.ErrNoRecord) {
		t.Fatalf("Lookup with an empty socket path: got %v, want inbox.ErrNoRecord", err)
	}
}
