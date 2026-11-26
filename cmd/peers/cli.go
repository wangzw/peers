package main

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
)

// Help group IDs; every visible command belongs to one of them.
const (
	groupPairing = "pairing"
	groupLocal   = "local"
	groupInstall = "install"
)

var errNeedsYes = errors.New("standard input is not a terminal; pass --yes to proceed without asking")

type exitError struct {
	code int
	err  error
}

func (e *exitError) Error() string {
	if e.err == nil {
		return fmt.Sprintf("exit status %d", e.code)
	}
	return e.err.Error()
}

func (e *exitError) Unwrap() error { return e.err }

// usageError marks err as a usage error; an error that already carries a code
// is returned unchanged.
func usageError(err error) error {
	if err == nil {
		return nil
	}
	var exit *exitError
	if errors.As(err, &exit) {
		return err
	}
	return &exitError{code: exitUsage, err: err}
}

func usageErrorf(format string, args ...any) error {
	return &exitError{code: exitUsage, err: fmt.Errorf(format, args...)}
}

func commandResult(code int) error {
	if code == exitOK {
		return nil
	}
	return &exitError{code: code}
}

type levelFlags struct {
	systemFlag bool
	userFlag   bool
}

// addLevelFlags adds --system and --user. The exclusion is checked in the
// command's RunE because cobra's own flag-group errors cannot map to exitUsage.
func addLevelFlags(cmd *cobra.Command) *levelFlags {
	f := &levelFlags{}
	cmd.Flags().BoolVar(&f.systemFlag, "system", false, "operate on the system installation")
	cmd.Flags().BoolVar(&f.userFlag, "user", false, "operate on this account's installation")
	return f
}

func (f *levelFlags) explicit() bool {
	return f.systemFlag || f.userFlag
}

func (f *levelFlags) system(euid int) (bool, error) {
	if f.systemFlag && f.userFlag {
		return false, usageErrorf("--system and --user are mutually exclusive")
	}
	if f.systemFlag {
		return true, nil
	}
	if f.userFlag {
		return false, nil
	}
	return euid == 0, nil
}

func addYesFlag(cmd *cobra.Command) *bool {
	return cmd.Flags().BoolP("yes", "y", false, "do not ask for confirmation")
}

func noPositionalArgs(_ *cobra.Command, args []string) error {
	if len(args) > 0 {
		return usageErrorf("unexpected argument %q", args[0])
	}
	return nil
}

func (a *app) confirm(prompt string, yes bool) (bool, error) {
	if yes {
		return true, nil
	}
	if !isTerminal(a.stdin) {
		return false, errNeedsYes
	}
	_, _ = fmt.Fprint(a.stderr, prompt)
	answer, err := readAnswer(a.stdin)
	if err != nil {
		return false, fmt.Errorf("cannot read the answer: %w", err)
	}
	return isYes(answer), nil
}
