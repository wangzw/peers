// Command rsprobe drives and observes real Claude Code sessions for the
// real-session probes.
package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
)

var commands = map[string]func(args []string) error{}

// register adds a subcommand. It is called from init functions only.
func register(name string, run func(args []string) error) {
	if _, dup := commands[name]; dup {
		panic("rsprobe: duplicate command " + name)
	}
	commands[name] = run
}

func main() {
	os.Exit(dispatch(os.Args[1:], os.Stderr))
}

func dispatch(args []string, stderr io.Writer) int {
	if len(args) >= 2 {
		if run, ok := commands[args[0]+" "+args[1]]; ok {
			return report(run(args[2:]), stderr)
		}
	}
	if len(args) >= 1 {
		if run, ok := commands[args[0]]; ok {
			return report(run(args[1:]), stderr)
		}
	}
	names := make([]string, 0, len(commands))
	for name := range commands {
		names = append(names, name)
	}
	sort.Strings(names)
	_, _ = fmt.Fprintln(stderr, "usage: rsprobe <command> [args]; commands:")
	for _, name := range names {
		_, _ = fmt.Fprintln(stderr, "  "+name)
	}
	return 2
}

// exitError carries the exit code a subcommand needs beyond the default 1. The
// scan-secret command uses 2 for an unreadable directory so the workflow can
// tell a scan failure from a token hit.
type exitError struct {
	code int
	err  error
}

func (e *exitError) Error() string { return e.err.Error() }

func (e *exitError) Unwrap() error { return e.err }

// ExitCode reports the process exit code for this error.
func (e *exitError) ExitCode() int { return e.code }

func report(err error, stderr io.Writer) int {
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "rsprobe: %v\n", err)
		var coded interface{ ExitCode() int }
		if errors.As(err, &coded) {
			return coded.ExitCode()
		}
		return 1
	}
	return 0
}
