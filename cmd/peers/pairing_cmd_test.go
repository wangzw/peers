package main

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func pairingCalls(s *termcmdStub) int {
	return len(s.pairingKeyCalls) + len(s.peerListCalls) +
		len(s.peerConnectCalls) + len(s.peerDisconnectCalls)
}

func usageDiagnostic(path, reason string) string {
	return path + ": " + reason + "\nRun '" + path + " --help' for usage.\n"
}

func findCommand(t *testing.T, a *app, path ...string) *cobra.Command {
	t.Helper()
	cmd, _, err := a.newRootCommand().Find(path)
	if err != nil {
		t.Fatalf("find %q: %v", path, err)
	}
	if cmd.Name() != path[len(path)-1] {
		t.Fatalf("find %q reached %q", path, cmd.CommandPath())
	}
	return cmd
}

// TestTopLevelUsageListsPairingCommands checks that the top-level help lists the
// pairing commands.
func TestTopLevelUsageListsPairingCommands(t *testing.T) {
	a, stdout, stderr := newTermcmdApp(&termcmdStub{})

	if got := a.run(context.Background(), []string{"--help"}); got != exitOK {
		t.Fatalf("run(--help) = %d, want %d", got, exitOK)
	}
	for _, want := range []string{"Pairing:", "broker", "key"} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("usage does not mention %q:\n%s", want, stdout.String())
		}
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

// TestPairingKeyCreateCallsRunner checks that key create reaches the runner with
// the standard output writer.
func TestPairingKeyCreateCallsRunner(t *testing.T) {
	stub := &termcmdStub{}
	a, stdout, stderr := newTermcmdApp(stub)

	if got := a.run(context.Background(), []string{"key", "create"}); got != exitOK {
		t.Fatalf("run(key create) = %d, want %d (stderr %q)", got, exitOK, stderr.String())
	}
	if len(stub.pairingKeyCalls) != 1 {
		t.Fatalf("runPairingKey called %d times, want 1", len(stub.pairingKeyCalls))
	}
	if stub.pairingKeyCalls[0] != stdout {
		t.Fatalf("runPairingKey got writer %T, want app.stdout", stub.pairingKeyCalls[0])
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("stdout = %q, stderr = %q, want both empty", stdout.String(), stderr.String())
	}
}

// TestPeerListCallsRunner checks that broker list reaches the runner with the
// standard output writer.
func TestPeerListCallsRunner(t *testing.T) {
	stub := &termcmdStub{}
	a, stdout, stderr := newTermcmdApp(stub)

	if got := a.run(context.Background(), []string{"broker", "list"}); got != exitOK {
		t.Fatalf("run(broker list) = %d, want %d (stderr %q)", got, exitOK, stderr.String())
	}
	if len(stub.peerListCalls) != 1 {
		t.Fatalf("runPeerList called %d times, want 1", len(stub.peerListCalls))
	}
	if stub.peerListCalls[0] != stdout {
		t.Fatalf("runPeerList got writer %T, want app.stdout", stub.peerListCalls[0])
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("stdout = %q, stderr = %q, want both empty", stdout.String(), stderr.String())
	}
}

// TestPeerConnectCallsRunner checks that broker connect passes the address, key
// and alias to the runner in either flag order.
func TestPeerConnectCallsRunner(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want peerConnectCall
	}{
		{
			name: "address before flags",
			args: []string{"broker", "connect", "peer.example:17443", "--key", "secret"},
			want: peerConnectCall{address: "peer.example:17443", key: "secret"},
		},
		{
			name: "flags before address",
			args: []string{"broker", "connect", "--key", "secret", "peer.example:17443"},
			want: peerConnectCall{address: "peer.example:17443", key: "secret"},
		},
		{
			name: "equals forms",
			args: []string{"broker", "connect", "peer.example:17443", "--key=secret", "--alias=web"},
			want: peerConnectCall{address: "peer.example:17443", key: "secret", alias: "web"},
		},
		{
			name: "alias after address",
			args: []string{"broker", "connect", "peer.example:17443", "--key", "secret", "--alias", "web"},
			want: peerConnectCall{address: "peer.example:17443", key: "secret", alias: "web"},
		},
		{
			name: "hostname form",
			args: []string{"broker", "connect", "web(peer.example:17443)", "--key=secret"},
			want: peerConnectCall{address: "web(peer.example:17443)", key: "secret"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &termcmdStub{}
			a, stdout, stderr := newTermcmdApp(stub)

			if got := a.run(context.Background(), tc.args); got != exitOK {
				t.Fatalf("run(%q) = %d, want %d (stderr %q)", tc.args, got, exitOK, stderr.String())
			}
			if len(stub.peerConnectCalls) != 1 {
				t.Fatalf("runPeerConnect called %d times, want 1", len(stub.peerConnectCalls))
			}
			got := stub.peerConnectCalls[0]
			if got.address != tc.want.address || got.key != tc.want.key || got.alias != tc.want.alias {
				t.Fatalf("runPeerConnect got (%q, %q, %q), want (%q, %q, %q)",
					got.address, got.key, got.alias, tc.want.address, tc.want.key, tc.want.alias)
			}
			if got.out != stdout {
				t.Fatalf("runPeerConnect got writer %T, want app.stdout", got.out)
			}
			if got.warn != stderr {
				t.Fatalf("runPeerConnect got warn %T, want app.stderr", got.warn)
			}
			if stdout.Len() != 0 || stderr.Len() != 0 {
				t.Fatalf("stdout = %q, stderr = %q, want both empty", stdout.String(), stderr.String())
			}
		})
	}
}

// TestPeerConnectRejectsSingleDashKey checks that -key is a usage error and the
// runner is not reached.
func TestPeerConnectRejectsSingleDashKey(t *testing.T) {
	stub := &termcmdStub{}
	a, stdout, stderr := newTermcmdApp(stub)

	if got := a.run(context.Background(), []string{"broker", "connect", "-key", "secret", "peer.example:17443"}); got != exitUsage {
		t.Fatalf("run(broker connect -key …) = %d, want %d (stderr %q)", got, exitUsage, stderr.String())
	}
	if len(stub.peerConnectCalls) != 0 {
		t.Fatalf("runPeerConnect called %d times, want 0", len(stub.peerConnectCalls))
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	if !strings.Contains(stderr.String(), "-key") {
		t.Fatalf("stderr = %q, want it to name the rejected option", stderr.String())
	}
}

// TestPeerConnectWarningsReachStderr checks that a runner warning reaches
// standard error with a "warning: " prefix and the command still exits 0.
func TestPeerConnectWarningsReachStderr(t *testing.T) {
	stub := &termcmdStub{peerConnectWarnings: []string{"peer is already paired"}}
	a, stdout, stderr := newTermcmdApp(stub)

	if got := a.run(context.Background(), []string{"broker", "connect", "peer.example:17443", "--key", "secret"}); got != exitOK {
		t.Fatalf("run(broker connect) = %d, want %d", got, exitOK)
	}
	if want := "warning: peer is already paired\n"; stderr.String() != want {
		t.Fatalf("stderr = %q, want %q", stderr.String(), want)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
}

// TestPeerDisconnectCallsRunner checks that broker disconnect passes the broker
// name to the runner.
func TestPeerDisconnectCallsRunner(t *testing.T) {
	stub := &termcmdStub{}
	a, stdout, stderr := newTermcmdApp(stub)

	if got := a.run(context.Background(), []string{"broker", "disconnect", "web"}); got != exitOK {
		t.Fatalf("run(broker disconnect) = %d, want %d (stderr %q)", got, exitOK, stderr.String())
	}
	if len(stub.peerDisconnectCalls) != 1 {
		t.Fatalf("runPeerDisconnect called %d times, want 1", len(stub.peerDisconnectCalls))
	}
	if stub.peerDisconnectCalls[0] != "web" {
		t.Fatalf("runPeerDisconnect broker = %q, want web", stub.peerDisconnectCalls[0])
	}
	if stub.peerDisconnectOut[0] != stdout {
		t.Fatalf("runPeerDisconnect got writer %T, want app.stdout", stub.peerDisconnectOut[0])
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("stdout = %q, stderr = %q, want both empty", stdout.String(), stderr.String())
	}
}

// TestBrokerDisconnectCompletionListsPairedBrokers checks that the disconnect
// argument completes with the aliases and fingerprints the Broker reports.
func TestBrokerDisconnectCompletionListsPairedBrokers(t *testing.T) {
	names := []string{"web", strings.Repeat("ab", 32)}
	stub := &termcmdStub{peerNames: names}
	a, _, _ := newTermcmdApp(stub)
	disconnect := findCommand(t, a, "broker", "disconnect")

	got, directive := disconnect.ValidArgsFunction(disconnect, nil, "")
	if stub.peerNamesCalls != 1 {
		t.Fatalf("peerNames called %d times, want 1", stub.peerNamesCalls)
	}
	if !slices.Equal(got, names) {
		t.Fatalf("completions = %q, want %q", got, names)
	}
	if directive != cobra.ShellCompDirectiveNoFileComp {
		t.Fatalf("directive = %v, want NoFileComp", directive)
	}
}

// TestBrokerDisconnectCompletionOmitsCandidatesOnError checks that a Broker that
// cannot be queried yields no candidates and writes no error.
func TestBrokerDisconnectCompletionOmitsCandidatesOnError(t *testing.T) {
	stub := &termcmdStub{peerNamesErr: errors.New("cannot reach the Broker")}
	a, _, stderr := newTermcmdApp(stub)
	disconnect := findCommand(t, a, "broker", "disconnect")

	got, directive := disconnect.ValidArgsFunction(disconnect, nil, "")
	if len(got) != 0 {
		t.Fatalf("completions = %q, want none", got)
	}
	if directive != cobra.ShellCompDirectiveNoFileComp {
		t.Fatalf("directive = %v, want NoFileComp", directive)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

// TestPairingCommandErrorsExitOne checks that a runtime error on the broker and
// key commands writes one line and exits 1.
func TestPairingCommandErrorsExitOne(t *testing.T) {
	errDenied := errors.New("permission_denied: not allowed")
	cases := []struct {
		name    string
		args    []string
		command string
		prepare func(*termcmdStub)
	}{
		{
			name: "key create", args: []string{"key", "create"}, command: "key create",
			prepare: func(s *termcmdStub) { s.pairingKeyErr = errDenied },
		},
		{
			name: "broker list", args: []string{"broker", "list"}, command: "broker list",
			prepare: func(s *termcmdStub) { s.peerListErr = errDenied },
		},
		{
			name: "broker connect", args: []string{"broker", "connect", "a:1", "--key", "k"}, command: "broker connect",
			prepare: func(s *termcmdStub) { s.peerConnectErr = errDenied },
		},
		{
			name: "broker disconnect", args: []string{"broker", "disconnect", "web"}, command: "broker disconnect",
			prepare: func(s *termcmdStub) { s.peerDisconnectErr = errDenied },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &termcmdStub{}
			tc.prepare(stub)
			a, stdout, stderr := newTermcmdApp(stub)

			if got := a.run(context.Background(), tc.args); got != exitFailure {
				t.Fatalf("run(%q) = %d, want %d", tc.args, got, exitFailure)
			}
			if want := "peers " + tc.command + ": permission_denied: not allowed\n"; stderr.String() != want {
				t.Fatalf("stderr = %q, want %q", stderr.String(), want)
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
		})
	}
}

// TestPairingUsageErrors checks the usage diagnostic for each malformed broker
// and key invocation.
func TestPairingUsageErrors(t *testing.T) {
	cases := []struct {
		name       string
		args       []string
		wantStderr string
		wantUsage  string
	}{
		{
			name:      "key without verb",
			args:      []string{"key"},
			wantUsage: "peers key",
		},
		{
			name:       "key unknown verb",
			args:       []string{"key", "bogus"},
			wantStderr: usageDiagnostic("peers key", `unknown command "bogus" for "peers key"`),
		},
		{
			name:       "key create extra argument",
			args:       []string{"key", "create", "x"},
			wantStderr: usageDiagnostic("peers key create", `unexpected argument "x"`),
		},
		{
			name:      "broker without verb",
			args:      []string{"broker"},
			wantUsage: "peers broker",
		},
		{
			name:       "broker unknown verb",
			args:       []string{"broker", "bogus"},
			wantStderr: usageDiagnostic("peers broker", `unknown command "bogus" for "peers broker"`),
		},
		{
			name:       "broker list extra argument",
			args:       []string{"broker", "list", "x"},
			wantStderr: usageDiagnostic("peers broker list", `unexpected argument "x"`),
		},
		{
			name:       "broker connect missing address",
			args:       []string{"broker", "connect", "--key", "k"},
			wantStderr: usageDiagnostic("peers broker connect", "expected <address>"),
		},
		{
			name:       "broker connect missing key",
			args:       []string{"broker", "connect", "a:1"},
			wantStderr: usageDiagnostic("peers broker connect", "missing --key"),
		},
		{
			name:       "broker connect empty key",
			args:       []string{"broker", "connect", "a:1", "--key="},
			wantStderr: usageDiagnostic("peers broker connect", "missing --key"),
		},
		{
			name:       "broker connect two addresses",
			args:       []string{"broker", "connect", "a", "b", "--key", "k"},
			wantStderr: usageDiagnostic("peers broker connect", "unexpected positional argument at position 2"),
		},
		{
			name:       "broker connect unknown flag",
			args:       []string{"broker", "connect", "a", "--key", "k", "--bogus"},
			wantStderr: usageDiagnostic("peers broker connect", "unknown flag: --bogus"),
		},
		{
			name:       "broker connect missing key value",
			args:       []string{"broker", "connect", "a", "--key"},
			wantStderr: usageDiagnostic("peers broker connect", "flag needs an argument: --key"),
		},
		{
			name:       "broker connect missing alias value",
			args:       []string{"broker", "connect", "a", "--key", "k", "--alias"},
			wantStderr: usageDiagnostic("peers broker connect", "flag needs an argument: --alias"),
		},
		{
			name:       "broker disconnect missing broker",
			args:       []string{"broker", "disconnect"},
			wantStderr: usageDiagnostic("peers broker disconnect", "expected <broker>"),
		},
		{
			name:       "broker disconnect extra argument",
			args:       []string{"broker", "disconnect", "a", "b"},
			wantStderr: usageDiagnostic("peers broker disconnect", "unexpected positional argument at position 2"),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &termcmdStub{}
			a, stdout, stderr := newTermcmdApp(stub)

			if got := a.run(context.Background(), tc.args); got != exitUsage {
				t.Fatalf("run(%q) = %d, want %d", tc.args, got, exitUsage)
			}
			if calls := pairingCalls(stub); calls != 0 {
				t.Fatalf("dependencies called %d times, want 0", calls)
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			if tc.wantStderr != "" {
				if got := stderr.String(); got != tc.wantStderr {
					t.Fatalf("stderr = %q, want %q", got, tc.wantStderr)
				}
				return
			}
			if !strings.Contains(stderr.String(), "Usage:") || !strings.Contains(stderr.String(), tc.wantUsage) {
				t.Fatalf("stderr = %q, want the %s usage", stderr.String(), tc.wantUsage)
			}
		})
	}
}

// TestPeerConnectExtraArgumentIsNotEchoed checks that a pairing key left out of
// --key is reported by position and not echoed.
func TestPeerConnectExtraArgumentIsNotEchoed(t *testing.T) {
	const key = "ABCD-EFGH-IJKL-MNOP"
	stub := &termcmdStub{}
	a, stdout, stderr := newTermcmdApp(stub)

	if got := a.run(context.Background(), []string{"broker", "connect", "a:1", key}); got != exitUsage {
		t.Fatalf("run(broker connect a:1 <key>) = %d, want %d", got, exitUsage)
	}
	if calls := pairingCalls(stub); calls != 0 {
		t.Fatalf("dependencies called %d times, want 0", calls)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	if strings.Contains(stderr.String(), key) {
		t.Fatalf("stderr repeats the extra argument: %q", stderr.String())
	}
	if !strings.Contains(stderr.String(), "position 2") {
		t.Fatalf("stderr = %q, want the position of the extra argument", stderr.String())
	}
}

// TestPairingHelp checks that each broker and key command prints its usage on
// -h or --help.
func TestPairingHelp(t *testing.T) {
	cases := []struct {
		name      string
		args      []string
		wantUsage string
	}{
		{"key -h", []string{"key", "-h"}, "peers key"},
		{"key create --help", []string{"key", "create", "--help"}, "peers key create"},
		{"key create after argument", []string{"key", "create", "x", "--help"}, "peers key create"},
		{"broker -h", []string{"broker", "-h"}, "peers broker"},
		{"broker list --help", []string{"broker", "list", "--help"}, "peers broker list"},
		{"broker list after argument", []string{"broker", "list", "x", "--help"}, "peers broker list"},
		{"broker connect --help", []string{"broker", "connect", "--help"}, "peers broker connect"},
		{"broker connect after address", []string{"broker", "connect", "a:1", "--help"}, "peers broker connect"},
		{"broker disconnect -h", []string{"broker", "disconnect", "-h"}, "peers broker disconnect"},
		{"broker disconnect after arguments", []string{"broker", "disconnect", "a", "b", "--help"}, "peers broker disconnect"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &termcmdStub{}
			a, stdout, stderr := newTermcmdApp(stub)

			if got := a.run(context.Background(), tc.args); got != exitOK {
				t.Fatalf("run(%q) = %d, want %d (stderr %q)", tc.args, got, exitOK, stderr.String())
			}
			if calls := pairingCalls(stub); calls != 0 {
				t.Fatalf("dependencies called %d times, want 0", calls)
			}
			if !strings.Contains(stdout.String(), tc.wantUsage) {
				t.Fatalf("stdout = %q, want it to contain %q", stdout.String(), tc.wantUsage)
			}
			if stderr.Len() != 0 {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}
