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

type app struct {
	stdin   io.Reader
	stdout  io.Writer
	stderr  io.Writer
	version string
	deps    deps
}

// signalRestoreHook runs after the first signal has cancelled ctx and the
// default handling is restored. Only the peers_testenv build replaces the no-op
// value, with a channel a subprocess test observes.
var signalRestoreHook = func() {}

// newSignalContext cancels the returned context on the first SIGINT or SIGTERM;
// after that the default handling is restored, so a second signal terminates
// the process immediately.
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
// line naming the command's help.
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

// addGenManCommand adds the hidden man-page generator; only the peers_genman
// build sets it, so the release binary does not carry cobra's doc package.
var addGenManCommand func(root *cobra.Command, a *app)

// newRootCommand builds a fresh cobra command tree, so no parsed state carries
// over between invocations.
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
	// The hidden help command belongs to no product group: the usage template
	// below drops cobra's exemption for it. Its GroupID only satisfies cobra's
	// group check and is never rendered.
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
	root.SetUsageTemplate(strings.ReplaceAll(
		root.UsageTemplate(),
		`(or .IsAvailableCommand (eq .Name "help"))`,
		`.IsAvailableCommand`,
	))
	// Cobra's completion lists the hidden help command regardless of Hidden; the
	// command is resolved before this hook runs, so this only drops it from the
	// candidates.
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

	root.InitDefaultCompletionCmd()
	a.applyExitCodeContract(root)
	return root
}

// applyExitCodeContract makes every usage error in the finished tree exit with
// exitUsage. Cobra adds its hidden __complete command after this walk, so a
// malformed completion request exits 1, not 2.
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
