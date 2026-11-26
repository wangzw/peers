package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func newBrokerCmdApp(stub *stubDeps) (*app, *bytes.Buffer, *bytes.Buffer) {
	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)
	return &app{
		stdout:  stdout,
		stderr:  stderr,
		version: "v-test",
		deps:    stub.deps(),
	}, stdout, stderr
}

func runServiceRun(a *app, args ...string) int {
	return a.run(context.Background(), append([]string{"service", "run"}, args...))
}

func brokerLastLine(s string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	return lines[len(lines)-1]
}

// TestBrokerPassesListenOverride checks that --listen replaces the configured
// address for this run.
func TestBrokerPassesListenOverride(t *testing.T) {
	stub := &stubDeps{}
	a, _, _ := newBrokerCmdApp(stub)

	code := runServiceRun(a, "--listen", "127.0.0.1:17444")
	if code != exitOK {
		t.Fatalf("service run returned %d, want %d", code, exitOK)
	}
	if len(stub.brokerCalls) != 1 {
		t.Fatalf("runBroker called %d times, want 1", len(stub.brokerCalls))
	}
	got := stub.brokerCalls[0]
	if got.Listen != "127.0.0.1:17444" {
		t.Errorf("Listen = %q, want %q", got.Listen, "127.0.0.1:17444")
	}
	if got.Version != a.version {
		t.Errorf("Version = %q, want %q", got.Version, a.version)
	}
}

// TestBrokerWithoutListenLeavesConfigInCharge checks that without --listen the
// configuration supplies the address.
func TestBrokerWithoutListenLeavesConfigInCharge(t *testing.T) {
	stub := &stubDeps{}
	a, _, _ := newBrokerCmdApp(stub)

	if code := runServiceRun(a); code != exitOK {
		t.Fatalf("service run returned %d, want %d", code, exitOK)
	}
	if len(stub.brokerCalls) != 1 {
		t.Fatalf("runBroker called %d times, want 1", len(stub.brokerCalls))
	}
	if got := stub.brokerCalls[0].Listen; got != "" {
		t.Errorf("Listen = %q, want empty", got)
	}
}

// TestBrokerRejectsEmptyListen checks that an empty --listen is a usage error.
func TestBrokerRejectsEmptyListen(t *testing.T) {
	for _, args := range [][]string{{"--listen", ""}, {"--listen="}} {
		stub := &stubDeps{}
		a, _, stderr := newBrokerCmdApp(stub)

		if code := runServiceRun(a, args...); code != exitUsage {
			t.Fatalf("service run %q returned %d, want %d", args, code, exitUsage)
		}
		if !strings.Contains(stderr.String(), "--listen requires a non-empty address") {
			t.Errorf("stderr = %q, want the empty --listen explanation", stderr.String())
		}
		if len(stub.brokerCalls) != 0 {
			t.Errorf("runBroker called %d times, want 0", len(stub.brokerCalls))
		}
	}
}

// TestBrokerStartupFailureExitCodes checks that each startup failure keeps its
// own exit code and its reason.
func TestBrokerStartupFailureExitCodes(t *testing.T) {
	cases := []struct {
		failure startFailure
		code    int
	}{
		{failAlreadyRunning, exitAlreadyRunning},
		{failListenUnavailable, exitListenUnavailable},
		{failInvalidConfig, exitInvalidConfig},
		{failInSession, exitInSession},
		{failNotRoot, exitNotRoot},
		{failRuntimeDir, exitRuntimeDir},
		{failInvalidListen, exitUsage},
		{failOther, exitFailure},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("failure_%d", tc.failure), func(t *testing.T) {
			stub := &stubDeps{
				brokerErr:      errors.New("startup failed"),
				classifyResult: tc.failure,
			}
			a, _, stderr := newBrokerCmdApp(stub)

			if code := runServiceRun(a); code != tc.code {
				t.Fatalf("service run returned %d, want %d", code, tc.code)
			}
			const want = "peers service run: startup failed"
			if tc.code == exitUsage {
				if !strings.Contains(stderr.String(), want+"\n") {
					t.Errorf("stderr = %q, want the diagnostic %q", stderr.String(), want)
				}
				if !strings.Contains(stderr.String(), "Run 'peers service run --help' for usage.") {
					t.Errorf("stderr = %q, want the usage hint", stderr.String())
				}
			} else if brokerLastLine(stderr.String()) != want {
				t.Errorf("last stderr line = %q, want %q", brokerLastLine(stderr.String()), want)
			}
		})
	}
}

// TestBrokerPassesSystemFlag checks that --system reaches the Broker.
func TestBrokerPassesSystemFlag(t *testing.T) {
	stub := &stubDeps{
		brokerErr:      errors.New("startup failed"),
		classifyResult: failOther,
	}
	a, _, _ := newBrokerCmdApp(stub)

	if code := runServiceRun(a, "--system"); code != exitFailure {
		t.Fatalf("service run returned %d, want %d", code, exitFailure)
	}
	if len(stub.brokerCalls) != 1 {
		t.Fatalf("runBroker called %d times, want 1", len(stub.brokerCalls))
	}
	if !stub.brokerCalls[0].System {
		t.Error("System = false, want true")
	}
}

// TestBrokerPassesUserFlag checks that --user selects the user installation
// even for root.
func TestBrokerPassesUserFlag(t *testing.T) {
	stub := &stubDeps{euid: func() int { return 0 }}
	a, _, _ := newBrokerCmdApp(stub)

	if code := runServiceRun(a, "--user"); code != exitOK {
		t.Fatalf("service run returned %d, want %d", code, exitOK)
	}
	if len(stub.brokerCalls) != 1 {
		t.Fatalf("runBroker called %d times, want 1", len(stub.brokerCalls))
	}
	if stub.brokerCalls[0].System {
		t.Error("System = true, want false")
	}
}

// TestBrokerPassesAllowDegraded checks that --allow-degraded reaches the Broker.
func TestBrokerPassesAllowDegraded(t *testing.T) {
	stub := &stubDeps{}
	a, _, _ := newBrokerCmdApp(stub)

	if code := runServiceRun(a, "--allow-degraded"); code != exitOK {
		t.Fatalf("service run returned %d, want %d", code, exitOK)
	}
	if len(stub.brokerCalls) != 1 {
		t.Fatalf("runBroker called %d times, want 1", len(stub.brokerCalls))
	}
	if !stub.brokerCalls[0].AllowDegraded {
		t.Error("AllowDegraded = false, want true")
	}
}

// TestBrokerHelpMentionsAllowDegraded checks that the usage lists
// --allow-degraded.
func TestBrokerHelpMentionsAllowDegraded(t *testing.T) {
	stub := &stubDeps{}
	a, stdout, _ := newBrokerCmdApp(stub)

	if code := runServiceRun(a, "-h"); code != exitOK {
		t.Fatalf("service run returned %d, want %d", code, exitOK)
	}
	if !strings.Contains(stdout.String(), "--allow-degraded") {
		t.Errorf("usage = %q, want it to mention --allow-degraded", stdout.String())
	}
}

// TestBrokerRejectsPositionalArgument checks that a positional argument is a
// usage error.
func TestBrokerRejectsPositionalArgument(t *testing.T) {
	stub := &stubDeps{}
	a, _, stderr := newBrokerCmdApp(stub)

	if code := runServiceRun(a, "extra"); code != exitUsage {
		t.Fatalf("service run returned %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), `unexpected argument "extra"`) {
		t.Errorf("stderr = %q, want the unexpected argument message", stderr.String())
	}
	if len(stub.brokerCalls) != 0 {
		t.Errorf("runBroker called %d times, want 0", len(stub.brokerCalls))
	}
}

// TestBrokerHelpGoesToStdout checks that --help prints the usage to standard
// output.
func TestBrokerHelpGoesToStdout(t *testing.T) {
	stub := &stubDeps{}
	a, stdout, stderr := newBrokerCmdApp(stub)

	if code := runServiceRun(a, "-h"); code != exitOK {
		t.Fatalf("service run returned %d, want %d", code, exitOK)
	}
	if !strings.Contains(stdout.String(), "peers service run") {
		t.Errorf("stdout = %q, want the service run usage", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Errorf("stderr = %q, want empty", stderr.String())
	}
	if len(stub.brokerCalls) != 0 {
		t.Errorf("runBroker called %d times, want 0", len(stub.brokerCalls))
	}
}

// TestBrokerRejectsBothLevelFlags checks that --system with --user is a usage
// error.
func TestBrokerRejectsBothLevelFlags(t *testing.T) {
	stub := &stubDeps{}
	a, _, stderr := newBrokerCmdApp(stub)

	if code := runServiceRun(a, "--system", "--user"); code != exitUsage {
		t.Fatalf("service run returned %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), "mutually exclusive") {
		t.Errorf("stderr = %q, want the level-flag conflict", stderr.String())
	}
	if len(stub.brokerCalls) != 0 {
		t.Errorf("runBroker called %d times, want 0", len(stub.brokerCalls))
	}
}
