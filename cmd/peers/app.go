package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
)

// app holds the process's standard streams, the version to report, and the
// upstream operations the subcommands dispatch to.
type app struct {
	stdin   io.Reader
	stdout  io.Writer
	stderr  io.Writer
	version string
	deps    deps
}

// signalRestoreHook runs after the first signal has cancelled ctx and the
// default signal handling has been restored. Production builds keep the no-op
// value; the peers_testenv build points it at a channel so a subprocess
// test can observe the restore instead of waiting a fixed interval.
var signalRestoreHook = func() {}

// newSignalContext cancels the returned context on SIGINT or SIGTERM. After the
// first signal restores the default handling, so a second signal terminates the
// process immediately.
func newSignalContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ctx.Done()
		stop()
		signalRestoreHook()
	}()
	return ctx, stop
}

// run executes one command line and returns the process exit code. A failing
// command is reported as "peers <command path>: <reason>"; a usage error adds a
// line naming the command's help, which cobra's own diagnostics do not provide
// once errors are silenced.
func (a *app) run(ctx context.Context, args []string) int {
	root := a.newRootCommand()
	root.SetArgs(args)

	cmd, err := root.ExecuteContextC(ctx)
	if err == nil {
		return exitOK
	}

	path := "peers"
	if cmd != nil {
		path = cmd.CommandPath()
	}

	var exit *exitError
	if errors.As(err, &exit) {
		// A nil cause means the command already printed its own diagnostics.
		if exit.err != nil {
			_, _ = fmt.Fprintf(a.stderr, "%s: %v\n", path, exit.err)
		}
		if exit.code == exitUsage && exit.err != nil {
			_, _ = fmt.Fprintf(a.stderr, "Run '%s --help' for usage.\n", path)
		}
		return exit.code
	}

	_, _ = fmt.Fprintf(a.stderr, "%s: %v\n", path, err)
	return exitFailure
}

// addGenManCommand, when non-nil, adds the hidden man-page generator. Only the
// peers_genman build sets it, so the release binary does not carry cobra's doc
// package.
var addGenManCommand func(root *cobra.Command, a *app)

// newRootCommand builds the cobra command tree. A fresh tree is built for every
// invocation, so no parsed state carries over between calls.
func (a *app) newRootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:   "peers",
		Short: "Run the MCP server, the local Broker and the terminal commands",
		Long: "peers runs an MCP server for one Claude Code session, the local Broker, " +
			"and the terminal commands that manage pairings and the installation.",
		SilenceErrors: true,
		SilenceUsage:  true,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				return usageErrorf("unknown command %q for %q", args[0], cmd.CommandPath())
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			if showVersion, _ := cmd.Flags().GetBool("version"); showVersion {
				_, _ = fmt.Fprintf(a.stdout, "peers %s\n", a.version)
				return nil
			}
			// A bare invocation is a usage error. Print the tree's usage to
			// standard error, as the hand-written dispatch did.
			_, _ = fmt.Fprint(a.stderr, cmd.UsageString())
			return &exitError{code: exitUsage}
		},
	}
	root.SetOut(a.stdout)
	root.SetErr(a.stderr)
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return usageError(err)
	})
	root.Flags().Bool("version", false, "print the version and exit")

	root.AddGroup(
		&cobra.Group{ID: groupPairing, Title: "Pairing:"},
		&cobra.Group{ID: groupLocal, Title: "Local Broker:"},
		&cobra.Group{ID: groupInstall, Title: "Installation:"},
	)
	// The help command belongs to no product group. It stays hidden, and the
	// usage template below drops cobra's exemption for a command named "help",
	// so it appears under none of the three headings and not under "Additional
	// Commands". Its GroupID is set only so cobra's group check accepts it; it
	// is never rendered.
	helpCmd := &cobra.Command{
		Use:     "help [command]",
		Short:   "Help about any command",
		Hidden:  true,
		GroupID: groupInstall,
		RunE: func(c *cobra.Command, args []string) error {
			target, remaining, err := c.Root().Find(args)
			if err != nil || target == nil || len(remaining) > 0 {
				return usageErrorf("unknown help topic %q", strings.Join(args, " "))
			}
			target.InitDefaultHelpFlag()
			return target.Help()
		},
	}
	root.SetHelpCommand(helpCmd)
	// Cobra's default usage template lists a hidden command named "help" as an
	// available command. Replace that exemption with the availability check
	// alone so the headings list only real commands.
	root.SetUsageTemplate(strings.ReplaceAll(
		root.UsageTemplate(),
		`(or .IsAvailableCommand (eq .Name "help"))`,
		`.IsAvailableCommand`,
	))
	// Cobra's completion lists the help command regardless of Hidden, because
	// the candidate filter admits it by identity rather than by availability.
	// The command is resolved before this hook runs, so removing it here only
	// drops it from the candidate list.
	root.PersistentPreRun = func(cmd *cobra.Command, _ []string) {
		if cmd.Name() == cobra.ShellCompRequestCmd {
			root.RemoveCommand(helpCmd)
		}
	}
	root.SetCompletionCommandGroupID(groupInstall)

	addPairingCommands(root, a)
	addConfigCommands(root, a)
	addServiceCommands(root, a)
	addInstallationCommands(root, a)
	a.addMCPCommand(root)
	a.addInternalCommands(root)
	if addGenManCommand != nil {
		addGenManCommand(root, a)
	}

	// Add the built-in completion command (bash, zsh, fish, powershell).
	root.InitDefaultCompletionCmd()
	a.applyExitCodeContract(root)
	return root
}

// applyExitCodeContract walks the finished command tree and makes every usage
// error exit with exitUsage:
//
//   - an Args check that fails is wrapped as a usage error, so cobra's own
//     argument-count and unknown-argument errors are mapped consistently;
//   - a parent command that takes subcommands and has no body of its own gets
//     one that names the unknown subcommand, or prints its usage to standard
//     error when it is invoked bare, and exits with exitUsage.
//
// Option parsing errors are handled by the root command's flag error function,
// which cobra applies to every command below it. The hidden __complete command
// is added by cobra's ExecuteC, after this walk, so its own Args check is not
// wrapped: a malformed completion request therefore exits with code 1 rather
// than the usage code 2. The completion scripts only call it internally, so
// that exit code is not part of the command-line contract.
func (a *app) applyExitCodeContract(cmd *cobra.Command) {
	if cmd.Args != nil {
		check := cmd.Args
		cmd.Args = func(c *cobra.Command, args []string) error {
			return usageError(check(c, args))
		}
	}
	if cmd.HasSubCommands() && !cmd.Runnable() {
		cmd.RunE = func(c *cobra.Command, args []string) error {
			if len(args) > 0 {
				return usageErrorf("unknown command %q for %q", args[0], c.CommandPath())
			}
			_, _ = fmt.Fprint(a.stderr, c.UsageString())
			return &exitError{code: exitUsage}
		}
	}
	for _, child := range cmd.Commands() {
		a.applyExitCodeContract(child)
	}
}
