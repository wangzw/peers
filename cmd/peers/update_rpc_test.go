package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/wangzw/peers/internal/mcpserver/termcmd"
	"github.com/wangzw/peers/internal/testkit/fakelocal"
)

func startApplyUpdateBroker(t *testing.T, answer func(*fakelocal.Handlers)) (string, *fakelocal.Server) {
	t.Helper()
	path := fakelocal.SocketPath(t)
	h := fakelocal.AdminHandlers()
	answer(&h)
	srv := fakelocal.Start(t, path, h)

	previous := findBroker
	findBroker = fakelocal.Selector(path)
	t.Cleanup(func() { findBroker = previous })
	return path, srv
}

func withUserSocketPath(t *testing.T, path string) {
	t.Helper()
	previous := updateUserSocketPath
	updateUserSocketPath = func() string { return path }
	t.Cleanup(func() { updateUserSocketPath = previous })
}

func fixedEUID(euid int) func() int {
	return func() int { return euid }
}

// TestApplyUpdateThroughBrokerUsesSystemBrokerWithoutUserBroker checks that an
// account with no user Broker applies the update through the system Broker.
func TestApplyUpdateThroughBrokerUsesSystemBrokerWithoutUserBroker(t *testing.T) {
	newUpdateTestEnv(t)
	path, srv := startApplyUpdateBroker(t, func(h *fakelocal.Handlers) {
		h.ApplyUpdate = fakelocal.ApplyUpdateAnswer("0.1.1", "0.1.2", true)
	})
	withUserSocketPath(t, path+".user")

	got, err := applyUpdateThroughBroker(t.Context(), fixedEUID(1000))
	if err != nil {
		t.Fatalf("applyUpdateThroughBroker: %v", err)
	}
	if got.FromVersion != "0.1.1" || got.ToVersion != "0.1.2" || !got.Restarting {
		t.Fatalf("result = %+v, want 0.1.1 -> 0.1.2 restarting", got)
	}
	if n := wireMethodCalls(srv, "ApplyUpdate"); n != 1 {
		t.Fatalf("ApplyUpdate calls = %d, want 1 on the system Broker", n)
	}
	if got.UserLevel {
		t.Error("UserLevel = true for the system Broker, want false")
	}
}

// TestApplyUpdateThroughBrokerMarksTheUserLevel checks that this account's own
// user Broker marks the result as a user update.
func TestApplyUpdateThroughBrokerMarksTheUserLevel(t *testing.T) {
	newUpdateTestEnv(t)
	path, _ := startApplyUpdateBroker(t, func(h *fakelocal.Handlers) {
		h.ApplyUpdate = fakelocal.ApplyUpdateAnswer("0.1.1", "0.1.2", true)
	})
	withUserSocketPath(t, path)

	got, err := applyUpdateThroughBroker(t.Context(), fixedEUID(1000))
	if err != nil {
		t.Fatalf("applyUpdateThroughBroker: %v", err)
	}
	if !got.UserLevel {
		t.Error("UserLevel = false for this account's user Broker, want true")
	}
}

// TestApplyUpdateThroughBrokerFallsBackForUserBroker checks that a declining
// user Broker falls back to a local replacement.
func TestApplyUpdateThroughBrokerFallsBackForUserBroker(t *testing.T) {
	newUpdateTestEnv(t)
	path, _ := startApplyUpdateBroker(t, func(h *fakelocal.Handlers) {
		h.ApplyUpdate = fakelocal.ApplyUpdateFailure(codes.FailedPrecondition, "this Broker cannot replace and restart its binary")
	})
	withUserSocketPath(t, path)

	_, err := applyUpdateThroughBroker(t.Context(), fixedEUID(1000))
	if !errors.Is(err, errUserLevelUpdateUnsupported) {
		t.Fatalf("applyUpdateThroughBroker error = %v, want the user fallback", err)
	}
}

// TestApplyUpdateThroughBrokerRootUsesSystemBroker checks that root's refusal
// from the system Broker stays an error, not a local replacement.
func TestApplyUpdateThroughBrokerRootUsesSystemBroker(t *testing.T) {
	newUpdateTestEnv(t)
	path, _ := startApplyUpdateBroker(t, func(h *fakelocal.Handlers) {
		h.ApplyUpdate = fakelocal.ApplyUpdateFailure(codes.FailedPrecondition, "the Broker is not managed by a service manager")
	})
	withUserSocketPath(t, path)

	_, err := applyUpdateThroughBroker(t.Context(), fixedEUID(0))
	if err == nil {
		t.Fatal("applyUpdateThroughBroker returned no error, want the refusal")
	}
	if errors.Is(err, errUserLevelUpdateUnsupported) {
		t.Fatalf("applyUpdateThroughBroker error = %v, want the refusal without a local fallback", err)
	}
	if !strings.Contains(err.Error(), "FailedPrecondition") {
		t.Fatalf("error = %q, want the Broker's failed_precondition", err)
	}
}

// TestApplyUpdateThroughBrokerReportsOtherErrors checks that a failure other
// than failed_precondition is reported, not turned into a local replacement.
func TestApplyUpdateThroughBrokerReportsOtherErrors(t *testing.T) {
	newUpdateTestEnv(t)
	path, _ := startApplyUpdateBroker(t, func(h *fakelocal.Handlers) {
		h.ApplyUpdate = fakelocal.ApplyUpdateFailure(codes.PermissionDenied, "the account cannot write the Broker configuration")
	})
	withUserSocketPath(t, path)

	_, err := applyUpdateThroughBroker(t.Context(), fixedEUID(1000))
	if err == nil {
		t.Fatal("applyUpdateThroughBroker returned no error, want the permission failure")
	}
	if errors.Is(err, errUserLevelUpdateUnsupported) {
		t.Fatalf("applyUpdateThroughBroker error = %v, want no local fallback for a permission failure", err)
	}
	if !strings.Contains(err.Error(), "PermissionDenied") {
		t.Fatalf("error = %q, want the Broker's permission failure", err)
	}
}

// TestApplyUpdateThroughBrokerWithoutRunningBroker checks that with no running
// Broker the command falls back to a local replacement.
func TestApplyUpdateThroughBrokerWithoutRunningBroker(t *testing.T) {
	newUpdateTestEnv(t)
	previous := findBroker
	findBroker = func(context.Context) (string, termcmd.Dialer, error) {
		return "", nil, errors.New("no Broker is running")
	}
	t.Cleanup(func() { findBroker = previous })

	_, err := applyUpdateThroughBroker(t.Context(), fixedEUID(1000))
	if !errors.Is(err, errNoRunningBroker) {
		t.Fatalf("applyUpdateThroughBroker error = %v, want the no-running-Broker fallback", err)
	}
}
