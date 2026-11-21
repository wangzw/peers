package main

import (
	"bytes"
	"context"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/spf13/cobra"
)

// The release contract: the version variable keeps its package path, name and
// type so -ldflags "-X main.version=<version>" keeps working.
var _ string = version

// TestRunGuardedTurnsAPanicIntoExitFailure keeps a crash out of exit code 2:
// the update contract reads 2 as "this binary has no post-update command" and
// would continue an update whose step crashed. The Go runtime's own panic exit
// status is 2, so the entry point must recover first.
func TestRunGuardedTurnsAPanicIntoExitFailure(t *testing.T) {
	var stderr bytes.Buffer
	code := runGuarded(&stderr, func() int { panic("boom") })
	if code != exitFailure {
		t.Fatalf("runGuarded(panic) = %d, want %d", code, exitFailure)
	}
	if got, want := stderr.String(), "peers: internal error: boom\n"; got != want {
		t.Errorf("stderr = %q, want %q", got, want)
	}
}

func newDispatchApp(ver string) (*app, *bytes.Buffer, *bytes.Buffer, *stubDeps) {
	stub := &stubDeps{}
	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)
	a := &app{
		stdin:   strings.NewReader(""),
		stdout:  stdout,
		stderr:  stderr,
		version: ver,
		deps:    stub.deps(),
	}
	return a, stdout, stderr, stub
}

func TestVersionPrintsInjectedVersion(t *testing.T) {
	a, stdout, stderr, _ := newDispatchApp("v1.2.3")

	if got := a.run(context.Background(), []string{"--version"}); got != exitOK {
		t.Fatalf("run(--version) = %d, want %d", got, exitOK)
	}
	if want := "peers v1.2.3\n"; stdout.String() != want {
		t.Fatalf("stdout = %q, want %q", stdout.String(), want)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestMainVersionContract(t *testing.T) {
	if version != "dev" {
		t.Fatalf("version = %q, want the default %q", version, "dev")
	}

	a, stdout, stderr, _ := newDispatchApp(version)
	if got := a.run(context.Background(), []string{"--version"}); got != exitOK {
		t.Fatalf("run(--version) = %d, want %d", got, exitOK)
	}
	if want := "peers dev\n"; stdout.String() != want {
		t.Fatalf("stdout = %q, want %q", stdout.String(), want)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestVersionRejectsExtraArguments(t *testing.T) {
	a, _, _, _ := newDispatchApp("v1.2.3")

	if got := a.run(context.Background(), []string{"--version", "x"}); got != exitUsage {
		t.Fatalf("run(--version x) = %d, want %d", got, exitUsage)
	}
}

func TestNoArgumentsPrintsUsageToStderr(t *testing.T) {
	a, stdout, stderr, _ := newDispatchApp("dev")

	if got := a.run(context.Background(), nil); got != exitUsage {
		t.Fatalf("run() = %d, want %d", got, exitUsage)
	}
	if !strings.Contains(stderr.String(), "Usage:") {
		t.Fatalf("stderr = %q, want the usage text", stderr.String())
	}
	if !strings.Contains(stderr.String(), "peers [command]") {
		t.Fatalf("stderr = %q, want the command tree in the usage text", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
}

func TestHelpPrintsUsageToStdout(t *testing.T) {
	for _, arg := range []string{"-h", "--help", "help"} {
		t.Run(arg, func(t *testing.T) {
			a, stdout, stderr, _ := newDispatchApp("dev")

			if got := a.run(context.Background(), []string{arg}); got != exitOK {
				t.Fatalf("run(%s) = %d, want %d", arg, got, exitOK)
			}
			out := stdout.String()
			if !strings.Contains(out, "Usage:") {
				t.Fatalf("stdout = %q, want the usage text", out)
			}
			if strings.Contains(out, "internal") {
				t.Fatalf("stdout leaks the hidden internal commands: %q", out)
			}
			if strings.Contains(out, "mcp") {
				t.Fatalf("stdout leaks the hidden mcp command: %q", out)
			}
			if stderr.Len() != 0 {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

func TestHelpListsTheThreeCommandGroups(t *testing.T) {
	a, stdout, stderr, _ := newDispatchApp("dev")

	if got := a.run(context.Background(), []string{"-h"}); got != exitOK {
		t.Fatalf("run(-h) = %d, want %d", got, exitOK)
	}
	out := stdout.String()
	for _, want := range []string{"Pairing:", "Local Broker:", "Installation:", "completion"} {
		if !strings.Contains(out, want) {
			t.Errorf("help does not contain %q:\n%s", want, out)
		}
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

// TestCommandShortDescriptionsStartWithCapitalLetter keeps the help listing
// consistent. Every command in the tree, hidden ones included, says what it
// does in one line that starts with a capital letter; a command without a
// Short is caught by the same walk.
func TestCommandShortDescriptionsStartWithCapitalLetter(t *testing.T) {
	a, _, _, _ := newDispatchApp("dev")

	var walk func(cmd *cobra.Command)
	walk = func(cmd *cobra.Command) {
		if cmd.Short == "" {
			t.Errorf("%s: Short is empty", cmd.CommandPath())
		} else if first, _ := utf8.DecodeRuneInString(cmd.Short); !unicode.IsUpper(first) {
			t.Errorf("%s: Short = %q, want it to start with a capital letter", cmd.CommandPath(), cmd.Short)
		}
		for _, child := range cmd.Commands() {
			walk(child)
		}
	}
	walk(a.newRootCommand())
}

func TestCompletionBashWritesAScript(t *testing.T) {
	a, stdout, stderr, _ := newDispatchApp("dev")

	if got := a.run(context.Background(), []string{"completion", "bash"}); got != exitOK {
		t.Fatalf("run(completion bash) = %d, want %d", got, exitOK)
	}
	if !strings.Contains(stdout.String(), "bash completion V2 for peers") {
		t.Fatalf("stdout does not look like a bash completion script:\n%s", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestUnknownOptionIsUsageError(t *testing.T) {
	a, stdout, stderr, _ := newDispatchApp("dev")

	if got := a.run(context.Background(), []string{"--bogus"}); got != exitUsage {
		t.Fatalf("run(--bogus) = %d, want %d", got, exitUsage)
	}
	if !strings.Contains(stderr.String(), "unknown flag") {
		t.Fatalf("stderr = %q, want it to name the unknown flag", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
}

func TestUsageNamesBrokerFlags(t *testing.T) {
	a, stdout, _, _ := newDispatchApp("dev")

	if got := a.run(context.Background(), []string{"service", "run", "--help"}); got != exitOK {
		t.Fatalf("run(service run --help) = %d, want %d", got, exitOK)
	}
	for _, want := range []string{"peers service run", "--listen string", "--allow-degraded", "--system", "--user"} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("usage does not contain %q:\n%s", want, stdout.String())
		}
	}
}

func TestUsageNamesServiceInstallFlags(t *testing.T) {
	a, stdout, _, _ := newDispatchApp("dev")

	if got := a.run(context.Background(), []string{"service", "install", "--help"}); got != exitOK {
		t.Fatalf("run(service install --help) = %d, want %d", got, exitOK)
	}
	for _, want := range []string{"peers service install", "--admin-group string", "--system", "--user"} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("usage does not contain %q:\n%s", want, stdout.String())
		}
	}
}

func TestUnknownCommand(t *testing.T) {
	a, _, stderr, _ := newDispatchApp("dev")

	if got := a.run(context.Background(), []string{"frobnicate"}); got != exitUsage {
		t.Fatalf("run(frobnicate) = %d, want %d", got, exitUsage)
	}
	if !strings.Contains(stderr.String(), `unknown command "frobnicate"`) {
		t.Fatalf("stderr = %q, want it to name the unknown command", stderr.String())
	}
	if !strings.Contains(stderr.String(), "Run 'peers --help' for usage.") {
		t.Fatalf("stderr = %q, want the help hint for a usage error", stderr.String())
	}
}

// TestParentCommandsExitWithUsage covers the fallback for a command that takes
// subcommands but has no body: the completion group and a missing help topic
// are usage errors, and completion rejects an unknown shell and an extra
// argument.
func TestParentCommandsExitWithUsage(t *testing.T) {
	cases := []struct {
		args []string
		// wantStderr, when set, is the exact diagnostic. Empty means only that
		// some usage or reason must be written.
		wantStderr string
	}{
		{args: []string{"completion"}},
		{
			args:       []string{"completion", "bogus"},
			wantStderr: usageDiagnostic("peers completion", `unknown command "bogus" for "peers completion"`),
		},
		{
			args:       []string{"completion", "bash", "x"},
			wantStderr: usageDiagnostic("peers completion bash", `unknown command "x" for "peers completion bash"`),
		},
		{
			args:       []string{"help", "bogus"},
			wantStderr: usageDiagnostic("peers help", `unknown help topic "bogus"`),
		},
	}
	for _, tc := range cases {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			a, stdout, stderr, _ := newDispatchApp("dev")

			if got := a.run(context.Background(), tc.args); got != exitUsage {
				t.Fatalf("run(%q) = %d, want %d (stderr %q)", tc.args, got, exitUsage, stderr.String())
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
			if stderr.Len() == 0 {
				t.Fatal("stderr is empty, want the usage or the reason")
			}
		})
	}
}

// TestCompletionDoesNotOfferHelp checks that the hidden help command stays out
// of the shell completion candidates.
func TestCompletionDoesNotOfferHelp(t *testing.T) {
	a, stdout, stderr, _ := newDispatchApp("dev")

	if got := a.run(context.Background(), []string{"__complete", ""}); got != exitOK {
		t.Fatalf("run(__complete \"\") = %d, want %d (stderr %q)", got, exitOK, stderr.String())
	}
	for _, line := range strings.Split(stdout.String(), "\n") {
		name, _, _ := strings.Cut(line, "\t")
		if name == "help" || name == "help-topic" {
			t.Fatalf("completion offered %q:\n%s", name, stdout.String())
		}
	}
}

func TestUnknownInternalCommandIsHidden(t *testing.T) {
	cases := []struct {
		args []string
		want string
	}{
		{
			args: []string{"internal"},
			want: usageDiagnostic("peers internal", `unknown command ""`),
		},
		{
			args: []string{"internal", "nope"},
			want: usageDiagnostic("peers internal", `unknown command "nope"`),
		},
	}
	for _, tc := range cases {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			a, stdout, stderr, _ := newDispatchApp("dev")

			if got := a.run(context.Background(), tc.args); got != exitUsage {
				t.Fatalf("run(%q) = %d, want %d", tc.args, got, exitUsage)
			}
			if got := stderr.String(); got != tc.want {
				t.Fatalf("stderr = %q, want %q", got, tc.want)
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
		})
	}
}

func TestSignalContextCancelsOnSIGTERM(t *testing.T) {
	ctx, stop := newSignalContext(context.Background())
	defer stop()

	//procsig:allow sends SIGTERM to this test's own process
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("send SIGTERM: %v", err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(processTimeout):
		t.Fatalf("context was not cancelled within %s of SIGTERM", processTimeout)
	}
}
