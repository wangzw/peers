package main

import (
	"strings"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/wangzw/peers/internal/testkit/fakelocal"
)

func startUpdateStatusBroker(t *testing.T, h fakelocal.Handlers) *fakelocal.Server {
	t.Helper()
	path := fakelocal.SocketPath(t)
	srv := fakelocal.Start(t, path, h)

	previous := findBroker
	findBroker = fakelocal.Selector(path)
	t.Cleanup(func() { findBroker = previous })
	return srv
}

// TestFetchBrokerUpdateStatusReadsTheRunningVersion checks that the command's
// status dep returns the version the selected Broker reports and asks it once.
func TestFetchBrokerUpdateStatusReadsTheRunningVersion(t *testing.T) {
	srv := startUpdateStatusBroker(t, fakelocal.Handlers{UpdateStatus: fakelocal.UpdateStatusAnswer("0.1.2")})

	got, err := fetchBrokerUpdateStatus(t.Context())
	if err != nil {
		t.Fatalf("fetchBrokerUpdateStatus: %v", err)
	}
	if got.CurrentVersion != "0.1.2" {
		t.Errorf("CurrentVersion = %q, want 0.1.2", got.CurrentVersion)
	}
	if n := wireMethodCalls(srv, "UpdateStatus"); n != 1 {
		t.Errorf("UpdateStatus calls = %d, want 1", n)
	}
}

// TestFetchBrokerUpdateStatusReportsTheCallError checks that a failed status
// call returns an error carrying the Broker's message.
func TestFetchBrokerUpdateStatusReportsTheCallError(t *testing.T) {
	startUpdateStatusBroker(t, fakelocal.Handlers{UpdateStatus: fakelocal.UpdateStatusFailure(codes.Unavailable, "the Broker is starting")})

	_, err := fetchBrokerUpdateStatus(t.Context())
	if err == nil {
		t.Fatal("fetchBrokerUpdateStatus succeeded although the call failed")
	}
	if !strings.Contains(err.Error(), "the Broker is starting") {
		t.Errorf("error = %q, want the Broker's message", err)
	}
}
