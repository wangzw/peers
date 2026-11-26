package main

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/update"
)

// TestUpdateLocalReportsUpToDate reports an up-to-date release without applying
// an update.
func TestUpdateLocalReportsUpToDate(t *testing.T) {
	newUpdateTestEnv(t)
	stub := &stubDeps{
		brokerUpdateErr: userLevelRefusal,
		latestRelease:   update.Release{Version: "0.1.1"},
		newerResult:     false,
	}
	a, stdout, stderr, _ := newDispatchApp("0.1.1")
	a.deps = stub.deps()

	if got := a.run(context.Background(), []string{"update", "--yes"}); got != exitOK {
		t.Fatalf("run(update --yes) = %d, want %d (stderr %q)", got, exitOK, stderr.String())
	}
	if want := "peers 0.1.1 is up to date\n"; stdout.String() != want {
		t.Errorf("stdout = %q, want %q", stdout.String(), want)
	}
	if len(stub.applyCalls) != 0 {
		t.Errorf("apply calls = %d, want none when the release is not newer", len(stub.applyCalls))
	}
	if stub.latestCalls != 1 {
		t.Errorf("latest calls = %d, want 1", stub.latestCalls)
	}
}

// TestUpdateLocalExecutableFailure reports a failed executable lookup before any
// release lookup.
func TestUpdateLocalExecutableFailure(t *testing.T) {
	newUpdateTestEnv(t)
	updateExecutable = func() (string, error) { return "", errors.New("no executable") }
	stub := &stubDeps{brokerUpdateErr: userLevelRefusal}
	a, _, stderr, _ := newDispatchApp("0.1.1")
	a.deps = stub.deps()

	if got := a.run(context.Background(), []string{"update", "--yes"}); got != exitFailure {
		t.Fatalf("run(update --yes) = %d, want %d", got, exitFailure)
	}
	if want := "cannot locate the executable: no executable"; !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
	}
	if stub.latestCalls != 0 || len(stub.applyCalls) != 0 {
		t.Errorf("latest calls = %d, apply calls = %d, want none before the executable is known",
			stub.latestCalls, len(stub.applyCalls))
	}
}

// TestUpdateLocalResolveFailure reports a failed symlink resolution of the
// executable.
func TestUpdateLocalResolveFailure(t *testing.T) {
	newUpdateTestEnv(t)
	updateEvalSymlinks = func(string) (string, error) { return "", errors.New("broken link") }
	stub := &stubDeps{brokerUpdateErr: userLevelRefusal}
	a, _, stderr, _ := newDispatchApp("0.1.1")
	a.deps = stub.deps()

	if got := a.run(context.Background(), []string{"update", "--yes"}); got != exitFailure {
		t.Fatalf("run(update --yes) = %d, want %d", got, exitFailure)
	}
	if want := "cannot resolve the executable"; !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
	}
	if want := "broken link"; !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want the resolver's error %q", stderr.String(), want)
	}
}

// TestUpdateLocalLatestFailure reports a failed release lookup without applying
// an update.
func TestUpdateLocalLatestFailure(t *testing.T) {
	newUpdateTestEnv(t)
	stub := &stubDeps{
		brokerUpdateErr: userLevelRefusal,
		latestErr:       errors.New("the release endpoint is unavailable"),
	}
	a, _, stderr, _ := newDispatchApp("0.1.1")
	a.deps = stub.deps()

	if got := a.run(context.Background(), []string{"update", "--yes"}); got != exitFailure {
		t.Fatalf("run(update --yes) = %d, want %d", got, exitFailure)
	}
	if want := "the release endpoint is unavailable"; !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
	}
	if len(stub.applyCalls) != 0 {
		t.Errorf("apply calls = %d, want none after a failed lookup", len(stub.applyCalls))
	}
}

// TestUpdateRestartTimeoutReportsTheLastError reports the timeout with the last
// polling error.
func TestUpdateRestartTimeoutReportsTheLastError(t *testing.T) {
	newUpdateTestEnv(t)
	updateRestartWait = 0
	stub := &stubDeps{
		brokerUpdate:    brokerUpdateResult{FromVersion: "0.1.1", ToVersion: "0.1.2", Restarting: true},
		brokerStatusErr: errors.New("connection refused"),
	}
	a, _, stderr, _ := newDispatchApp("0.1.1")
	a.deps = stub.deps()

	if got := a.run(context.Background(), []string{"update", "--yes"}); got != exitFailure {
		t.Fatalf("run(update --yes) = %d, want %d", got, exitFailure)
	}
	errOut := stderr.String()
	if !strings.Contains(errOut, "did not report version 0.1.2") {
		t.Errorf("stderr = %q, want the timeout version", errOut)
	}
	if want := "(last error: connection refused)"; !strings.Contains(errOut, want) {
		t.Errorf("stderr = %q, want the last poll error %q", errOut, want)
	}
}

// TestUpdateRestartStopsWhenTheContextIsCancelled returns the cancellation after
// one status poll.
func TestUpdateRestartStopsWhenTheContextIsCancelled(t *testing.T) {
	newUpdateTestEnv(t)
	updateRestartWait = time.Hour
	updateStatusPollDelay = time.Hour
	stub := &stubDeps{brokerStatus: brokerUpdateStatus{CurrentVersion: "0.1.1"}}
	a, _, _, _ := newDispatchApp("0.1.1")
	a.deps = stub.deps()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := a.reportBrokerUpdate(ctx, brokerUpdateResult{FromVersion: "0.1.1", ToVersion: "0.1.2", Restarting: true})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("reportBrokerUpdate = %v, want context.Canceled", err)
	}
	if stub.brokerStatusCalls != 1 {
		t.Errorf("UpdateStatus calls = %d, want 1 before the cancellation", stub.brokerStatusCalls)
	}
}

// TestReadAnswerPropagatesReadErrors returns the reader's error.
func TestReadAnswerPropagatesReadErrors(t *testing.T) {
	broken := errors.New("read failed")
	if _, err := readAnswer(errorReader{err: broken}); !errors.Is(err, broken) {
		t.Errorf("readAnswer = %v, want the reader's error", err)
	}
}

// TestUpdateDefaultClientUsesTheDefaultTransport checks that the default update
// client uses http.DefaultTransport.
func TestUpdateDefaultClientUsesTheDefaultTransport(t *testing.T) {
	client := updateNewClient()
	if client.Transport != http.DefaultTransport {
		t.Errorf("updateNewClient().Transport = %T, want http.DefaultTransport", client.Transport)
	}
}
