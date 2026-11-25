package link

import (
	"errors"
	"fmt"
	"testing"

	"github.com/wangzw/peers/internal/pairing"
)

var _ Peers = (*pairing.Pairing)(nil)

// TestErrQueueFullIsNotEstablished checks that ErrQueueFull wraps
// ErrNotEstablished one way only.
func TestErrQueueFullIsNotEstablished(t *testing.T) {
	if !errors.Is(ErrQueueFull, ErrNotEstablished) {
		t.Fatalf("errors.Is(ErrQueueFull, ErrNotEstablished) = false, want true")
	}
	if errors.Is(ErrNotEstablished, ErrQueueFull) {
		t.Fatalf("errors.Is(ErrNotEstablished, ErrQueueFull) = true, want false")
	}
}

// TestUnsupportedErrorMessage checks the error text and that errors.As recovers
// the unsupported kind.
func TestUnsupportedErrorMessage(t *testing.T) {
	e := &UnsupportedError{Kind: 301}
	if got, want := e.Error(), "the peer does not support request kind 301"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}

	wrapped := fmt.Errorf("x: %w", e)
	var got *UnsupportedError
	if !errors.As(wrapped, &got) {
		t.Fatalf("errors.As did not recover *UnsupportedError from %v", wrapped)
	}
	if got.Kind != 301 {
		t.Fatalf("recovered kind = %d, want 301", got.Kind)
	}
}
