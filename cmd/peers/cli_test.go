package main

import (
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func withTerminalAnswer(t *testing.T, a *app, answer string) {
	t.Helper()
	previous := isTerminal
	isTerminal = func(io.Reader) bool { return true }
	t.Cleanup(func() { isTerminal = previous })
	a.stdin = strings.NewReader(answer)
}

// TestExitErrorCarriesTheCodeAndCause checks that exitError reports its cause
// and unwraps to it.
func TestExitErrorCarriesTheCodeAndCause(t *testing.T) {
	cause := errors.New("boom")
	err := &exitError{code: exitUsage, err: cause}

	if err.Error() != "boom" {
		t.Errorf("Error() = %q, want %q", err.Error(), "boom")
	}
	if !errors.Is(err, cause) {
		t.Error("errors.Is did not reach the cause")
	}
	if got := (&exitError{code: exitFailure}).Error(); got == "" {
		t.Error("Error() is empty without a cause")
	}
}

// TestCommandResult checks that commandResult maps the success code to nil and
// other codes to an exitError.
func TestCommandResult(t *testing.T) {
	if err := commandResult(exitOK); err != nil {
		t.Fatalf("commandResult(exitOK) = %v, want nil", err)
	}
	err := commandResult(exitServiceDegraded)
	var exit *exitError
	if !errors.As(err, &exit) {
		t.Fatalf("commandResult(%d) = %v, want an exitError", exitServiceDegraded, err)
	}
	if exit.code != exitServiceDegraded {
		t.Fatalf("exit code = %d, want %d", exit.code, exitServiceDegraded)
	}
}

func levelTestCommand(t *testing.T, args ...string) (*levelFlags, error) {
	t.Helper()
	cmd := &cobra.Command{
		Use:  "test",
		RunE: func(*cobra.Command, []string) error { return nil },
	}
	flags := addLevelFlags(cmd)
	cmd.SetArgs(args)
	return flags, cmd.Execute()
}

// TestLevelFlagsSystem checks the level each flag combination selects for root
// and a non-root account.
func TestLevelFlagsSystem(t *testing.T) {
	cases := []struct {
		name string
		args []string
		euid int
		want bool
	}{
		{"default root", nil, 0, true},
		{"default account", nil, 1000, false},
		{"system flag as account", []string{"--system"}, 1000, true},
		{"user flag as root", []string{"--user"}, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			flags, err := levelTestCommand(t, tc.args...)
			if err != nil {
				t.Fatalf("Execute(%q): %v", tc.args, err)
			}
			got, err := flags.system(tc.euid)
			if err != nil {
				t.Fatalf("system(%d): %v", tc.euid, err)
			}
			if got != tc.want {
				t.Errorf("system(%d) = %t, want %t", tc.euid, got, tc.want)
			}
		})
	}
}

// TestLevelFlagsAreMutuallyExclusive checks that giving both level flags is a
// usage error.
func TestLevelFlagsAreMutuallyExclusive(t *testing.T) {
	flags, err := levelTestCommand(t, "--system", "--user")
	if err != nil {
		t.Fatalf("Execute(--system --user): %v", err)
	}
	_, err = flags.system(1000)
	var exit *exitError
	if !errors.As(err, &exit) {
		t.Fatalf("system() error = %v, want an exitError", err)
	}
	if exit.code != exitUsage {
		t.Fatalf("exit code = %d, want %d", exit.code, exitUsage)
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("error = %q, want it to explain the conflict", err.Error())
	}
}

// TestAddYesFlag checks that -y and --yes set the yes flag.
func TestAddYesFlag(t *testing.T) {
	for _, arg := range []string{"-y", "--yes"} {
		t.Run(arg, func(t *testing.T) {
			cmd := &cobra.Command{Use: "test"}
			yes := addYesFlag(cmd)
			if err := cmd.Flags().Parse([]string{arg}); err != nil {
				t.Fatalf("Parse(%q): %v", arg, err)
			}
			if !*yes {
				t.Errorf("%s did not set the yes flag", arg)
			}
		})
	}
}

// TestConfirmSkipsTheQuestionWithYes checks that yes returns true without
// writing a prompt.
func TestConfirmSkipsTheQuestionWithYes(t *testing.T) {
	a, stdout, stderr, _ := newDispatchApp("dev")

	confirmed, err := a.confirm("Continue? [y/N] ", true)
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if !confirmed {
		t.Fatal("confirm returned false with yes=true")
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want no prompt", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want no prompt", stderr.String())
	}
}

// TestConfirmWritesTheQuestionToStderr checks that the question goes to stderr
// and an empty answer declines.
func TestConfirmWritesTheQuestionToStderr(t *testing.T) {
	a, stdout, stderr, _ := newDispatchApp("dev")
	withTerminalAnswer(t, a, "\n")

	confirmed, err := a.confirm("Continue? [y/N] ", false)
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if confirmed {
		t.Fatal("confirm returned true for an empty answer")
	}
	if want := "Continue? [y/N] "; stderr.String() != want {
		t.Fatalf("stderr = %q, want %q", stderr.String(), want)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want no question", stdout.String())
	}
}

// TestConfirmAnswersYesFromTheTerminal checks that an answer of y confirms.
func TestConfirmAnswersYesFromTheTerminal(t *testing.T) {
	a, stdout, stderr, _ := newDispatchApp("dev")
	withTerminalAnswer(t, a, "y\n")

	confirmed, err := a.confirm("Continue? [y/N] ", false)
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if !confirmed {
		t.Fatal("confirm returned false for an answer of y")
	}
	if want := "Continue? [y/N] "; stderr.String() != want {
		t.Fatalf("stderr = %q, want %q", stderr.String(), want)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want no question", stdout.String())
	}
}

func devNullStdin(t *testing.T, a *app) {
	t.Helper()
	f, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatalf("opening %s: %v", os.DevNull, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	a.stdin = f
}

// TestConfirmTreatsDevNullAsNoTerminal checks that /dev/null is not a terminal
// and confirm wraps errNeedsYes.
func TestConfirmTreatsDevNullAsNoTerminal(t *testing.T) {
	a, _, _, _ := newDispatchApp("dev")
	devNullStdin(t, a)

	confirmed, err := a.confirm("Continue? [y/N] ", false)
	if err == nil {
		t.Fatal("confirm treated /dev/null as a terminal")
	}
	if confirmed {
		t.Fatal("confirm returned true without an answer")
	}
	if !errors.Is(err, errNeedsYes) {
		t.Fatalf("error = %v, want it to wrap errNeedsYes", err)
	}
}

// TestConfirmWithoutTerminalNamesYes checks that without a terminal the error
// mentions --yes and wraps errNeedsYes.
func TestConfirmWithoutTerminalNamesYes(t *testing.T) {
	a, _, _, _ := newDispatchApp("dev")

	confirmed, err := a.confirm("Continue? [y/N] ", false)
	if err == nil {
		t.Fatal("confirm succeeded without a terminal and without --yes")
	}
	if confirmed {
		t.Fatal("confirm returned true without an answer")
	}
	if !strings.Contains(err.Error(), "--yes") {
		t.Fatalf("error = %q, want it to mention --yes", err.Error())
	}
	if !errors.Is(err, errNeedsYes) {
		t.Fatalf("error = %v, want it to wrap errNeedsYes", err)
	}
}
