package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/mcpserver/termcmd"
)

type configGetCall struct {
	key string
	out io.Writer
}

type configSetCall struct {
	key   string
	value string
	out   io.Writer
}

type peerConnectCall struct {
	address string
	key     string
	alias   string
	out     io.Writer
	warn    io.Writer
}

type termcmdStub struct {
	statusCalls    int
	statusSystem   bool
	statusExplicit bool
	statusOut      io.Writer
	statusErr      error

	auditCalls    int
	auditSystem   bool
	auditExplicit bool
	auditOut      io.Writer
	auditLimit    uint32
	auditErr      error

	configGetCalls      []configGetCall
	configGetErr        error
	configSetCalls      []configSetCall
	configSetErr        error
	setOnlineCalls      []setOnlineCall
	setOnlineErr        error
	pairingKeyCalls     []io.Writer
	pairingKeyErr       error
	peerListCalls       []io.Writer
	peerListErr         error
	peerConnectCalls    []peerConnectCall
	peerConnectWarnings []string
	peerConnectErr      error
	peerDisconnectCalls []string
	peerDisconnectOut   []io.Writer
	peerDisconnectErr   error
	peerNames           []string
	peerNamesErr        error
	peerNamesCalls      int
}

func (s *termcmdStub) deps() deps {
	return deps{
		euid: func() int { return 1000 },
		runStatus: func(_ context.Context, system, explicit bool, stdout io.Writer) error {
			s.statusCalls++
			s.statusSystem = system
			s.statusExplicit = explicit
			s.statusOut = stdout
			return s.statusErr
		},
		runAudit: func(_ context.Context, system, explicit bool, stdout io.Writer, limit uint32) error {
			s.auditCalls++
			s.auditSystem = system
			s.auditExplicit = explicit
			s.auditOut = stdout
			s.auditLimit = limit
			return s.auditErr
		},
		runConfigGet: func(_ context.Context, key string, stdout io.Writer) error {
			s.configGetCalls = append(s.configGetCalls, configGetCall{key: key, out: stdout})
			return s.configGetErr
		},
		runConfigSet: func(_ context.Context, key, value string, stdout io.Writer) error {
			s.configSetCalls = append(s.configSetCalls, configSetCall{key: key, value: value, out: stdout})
			return s.configSetErr
		},
		runSetOnline: func(_ context.Context, system, explicit, online bool, stdout io.Writer) error {
			s.setOnlineCalls = append(s.setOnlineCalls, setOnlineCall{system: system, explicit: explicit, online: online, out: stdout})
			return s.setOnlineErr
		},
		runPairingKeyCreate: func(_ context.Context, stdout io.Writer) error {
			s.pairingKeyCalls = append(s.pairingKeyCalls, stdout)
			return s.pairingKeyErr
		},
		runPeerList: func(_ context.Context, stdout io.Writer) error {
			s.peerListCalls = append(s.peerListCalls, stdout)
			return s.peerListErr
		},
		runPeerConnect: func(_ context.Context, address, key, alias string, stdout, warn io.Writer) error {
			s.peerConnectCalls = append(s.peerConnectCalls, peerConnectCall{
				address: address, key: key, alias: alias, out: stdout, warn: warn,
			})
			for _, warning := range s.peerConnectWarnings {
				_, _ = fmt.Fprintf(warn, "warning: %s\n", warning)
			}
			return s.peerConnectErr
		},
		runPeerDisconnect: func(_ context.Context, broker string, stdout io.Writer) error {
			s.peerDisconnectCalls = append(s.peerDisconnectCalls, broker)
			s.peerDisconnectOut = append(s.peerDisconnectOut, stdout)
			return s.peerDisconnectErr
		},
		peerNames: func(context.Context) ([]string, error) {
			s.peerNamesCalls++
			return s.peerNames, s.peerNamesErr
		},
	}
}

func newTermcmdApp(s *termcmdStub) (*app, *bytes.Buffer, *bytes.Buffer) {
	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)
	return &app{stdout: stdout, stderr: stderr, deps: s.deps()}, stdout, stderr
}

func runService(a *app, args ...string) int {
	return a.run(context.Background(), append([]string{"service"}, args...))
}

// TestStatusCallsRunner checks that service status reaches the runner with the
// standard output writer.
func TestStatusCallsRunner(t *testing.T) {
	stub := &termcmdStub{}
	a, stdout, stderr := newTermcmdApp(stub)

	if got := runService(a, "status"); got != exitOK {
		t.Fatalf("service status returned %d, want %d", got, exitOK)
	}
	if stub.statusCalls != 1 {
		t.Fatalf("runStatus called %d times, want 1", stub.statusCalls)
	}
	if stub.statusOut != stdout {
		t.Fatalf("runStatus got writer %T, want app.stdout", stub.statusOut)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

// TestStatusErrorExitsOne checks that a status error writes one line and exits
// 1.
func TestStatusErrorExitsOne(t *testing.T) {
	stub := &termcmdStub{statusErr: errors.New("permission_denied: x")}
	a, stdout, stderr := newTermcmdApp(stub)

	if got := runService(a, "status"); got != exitFailure {
		t.Fatalf("service status returned %d, want %d", got, exitFailure)
	}
	if want := "peers service status: permission_denied: x\n"; stderr.String() != want {
		t.Fatalf("stderr = %q, want %q", stderr.String(), want)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
}

// TestStatusDiagnosticsOnlyExitsOne checks that a diagnostics-only status fails
// without adding a line to either stream.
func TestStatusDiagnosticsOnlyExitsOne(t *testing.T) {
	stub := &termcmdStub{statusErr: termcmd.ErrDiagnosticsOnly}
	a, stdout, stderr := newTermcmdApp(stub)

	if got := runService(a, "status"); got != exitFailure {
		t.Fatalf("service status returned %d, want %d", got, exitFailure)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty; the runner wrote the status itself", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

// TestStatusRejectsUnknownFlag checks that an unknown status flag is a usage
// error before the runner is reached.
func TestStatusRejectsUnknownFlag(t *testing.T) {
	stub := &termcmdStub{}
	a, stdout, stderr := newTermcmdApp(stub)

	if got := runService(a, "status", "--bogus"); got != exitUsage {
		t.Fatalf("service status returned %d, want %d", got, exitUsage)
	}
	if stub.statusCalls != 0 {
		t.Fatalf("runStatus called %d times, want 0", stub.statusCalls)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	if want := usageDiagnostic("peers service status", "unknown flag: --bogus"); stderr.String() != want {
		t.Fatalf("stderr = %q, want %q", stderr.String(), want)
	}
}

// TestStatusRejectsArguments checks that service status rejects a positional
// argument.
func TestStatusRejectsArguments(t *testing.T) {
	stub := &termcmdStub{}
	a, _, stderr := newTermcmdApp(stub)

	if got := runService(a, "status", "x"); got != exitUsage {
		t.Fatalf("service status returned %d, want %d", got, exitUsage)
	}
	if stub.statusCalls != 0 {
		t.Fatalf("runStatus called %d times, want 0", stub.statusCalls)
	}
	if want := usageDiagnostic("peers service status", `unexpected argument "x"`); stderr.String() != want {
		t.Fatalf("stderr = %q, want %q", stderr.String(), want)
	}
}

// TestAuditRejectsArguments checks that service audit rejects a positional
// argument.
func TestAuditRejectsArguments(t *testing.T) {
	stub := &termcmdStub{}
	a, _, stderr := newTermcmdApp(stub)

	if got := runService(a, "audit", "x"); got != exitUsage {
		t.Fatalf("service audit returned %d, want %d", got, exitUsage)
	}
	if stub.auditCalls != 0 {
		t.Fatalf("runAudit called %d times, want 0", stub.auditCalls)
	}
	if want := usageDiagnostic("peers service audit", `unexpected argument "x"`); stderr.String() != want {
		t.Fatalf("stderr = %q, want %q", stderr.String(), want)
	}
}

// TestAuditLimit checks that --limit is read as a decimal count, with a leading
// zero meaning decimal.
func TestAuditLimit(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want uint32
	}{
		{"default is unlimited", nil, 0},
		{"separate value", []string{"--limit", "5"}, 5},
		{"equals zero", []string{"--limit=0"}, 0},
		{"leading zero is decimal", []string{"--limit", "010"}, 10},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &termcmdStub{}
			a, _, _ := newTermcmdApp(stub)

			if got := runService(a, append([]string{"audit"}, tc.args...)...); got != exitOK {
				t.Fatalf("service audit returned %d, want %d", got, exitOK)
			}
			if stub.auditCalls != 1 {
				t.Fatalf("runAudit called %d times, want 1", stub.auditCalls)
			}
			if stub.auditLimit != tc.want {
				t.Fatalf("runAudit got limit %d, want %d", stub.auditLimit, tc.want)
			}
		})
	}
}

// TestAuditLimitRange checks that an out-of-range, negative or non-numeric
// --limit is a usage error.
func TestAuditLimitRange(t *testing.T) {
	cases := []struct {
		name   string
		args   []string
		prefix string
	}{
		{"above uint32", []string{"--limit", "4294967296"}, "peers service audit: "},
		{"negative", []string{"--limit", "-1"}, "peers service audit: "},
		{"not a number", []string{"--limit", "x"}, "peers service audit: "},
		{"single dash", []string{"-limit", "7"}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &termcmdStub{}
			a, _, stderr := newTermcmdApp(stub)

			if got := runService(a, append([]string{"audit"}, tc.args...)...); got != exitUsage {
				t.Fatalf("service audit returned %d, want %d", got, exitUsage)
			}
			if stub.auditCalls != 0 {
				t.Fatalf("runAudit called %d times, want 0", stub.auditCalls)
			}
			if tc.prefix != "" && !strings.HasPrefix(stderr.String(), tc.prefix) {
				t.Fatalf("stderr = %q, want it to start with %q", stderr.String(), tc.prefix)
			}
			if stderr.Len() == 0 {
				t.Fatal("stderr is empty, want the reason")
			}
		})
	}
}

// TestAuditErrorExitsOne checks that an audit error writes one line and exits 1.
func TestAuditErrorExitsOne(t *testing.T) {
	stub := &termcmdStub{auditErr: errors.New("permission_denied: x")}
	a, stdout, stderr := newTermcmdApp(stub)

	if got := runService(a, "audit"); got != exitFailure {
		t.Fatalf("service audit returned %d, want %d", got, exitFailure)
	}
	if want := "peers service audit: permission_denied: x\n"; stderr.String() != want {
		t.Fatalf("stderr = %q, want %q", stderr.String(), want)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
}

// TestLevelCommandsPassTheExplicitLevel checks that each level-aware command
// reports the selected level and whether a flag named it.
func TestLevelCommandsPassTheExplicitLevel(t *testing.T) {
	cases := []struct {
		args         []string
		wantSystem   bool
		wantExplicit bool
	}{
		{[]string{"status"}, false, false},
		{[]string{"status", "--system"}, true, true},
		{[]string{"status", "--user"}, false, true},
		{[]string{"audit", "--system"}, true, true},
		{[]string{"online", "--system"}, true, true},
		{[]string{"offline", "--user"}, false, true},
	}
	for _, tc := range cases {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			stub := &termcmdStub{}
			a, _, stderr := newTermcmdApp(stub)
			if got := runService(a, tc.args...); got != exitOK {
				t.Fatalf("service %s = %d, want %d (stderr %q)", strings.Join(tc.args, " "), got, exitOK, stderr)
			}

			var gotSystem, gotExplicit bool
			switch tc.args[0] {
			case "status":
				gotSystem, gotExplicit = stub.statusSystem, stub.statusExplicit
			case "audit":
				gotSystem, gotExplicit = stub.auditSystem, stub.auditExplicit
			default:
				if len(stub.setOnlineCalls) != 1 {
					t.Fatalf("runSetOnline called %d times, want 1", len(stub.setOnlineCalls))
				}
				call := stub.setOnlineCalls[0]
				gotSystem, gotExplicit = call.system, call.explicit
			}
			if gotSystem != tc.wantSystem || gotExplicit != tc.wantExplicit {
				t.Errorf("level passed = (system %v, explicit %v), want (%v, %v)",
					gotSystem, gotExplicit, tc.wantSystem, tc.wantExplicit)
			}
		})
	}
}

// TestServiceLevelFlagsAreMutuallyExclusive checks that --system and --user
// together are a usage error on every level-aware command.
func TestServiceLevelFlagsAreMutuallyExclusive(t *testing.T) {
	for _, name := range []string{"status", "audit", "online", "offline"} {
		t.Run(name, func(t *testing.T) {
			stub := &termcmdStub{}
			a, _, stderr := newTermcmdApp(stub)

			if got := runService(a, name, "--system", "--user"); got != exitUsage {
				t.Fatalf("service %s returned %d, want %d", name, got, exitUsage)
			}
			if !strings.Contains(stderr.String(), "mutually exclusive") {
				t.Fatalf("stderr = %q, want the level-flag conflict", stderr.String())
			}
			if stub.statusCalls != 0 || stub.auditCalls != 0 || len(stub.setOnlineCalls) != 0 {
				t.Fatalf("a runner was called for a usage error")
			}
		})
	}
}
