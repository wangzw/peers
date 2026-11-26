package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/spf13/cobra"
)

const mcpUsage = "usage: peers mcp"

// The name cannot change and the command must not appear in the help listing,
// because Claude Code starts the MCP server with "peers mcp".
func (a *app) addMCPCommand(root *cobra.Command) {
	root.AddCommand(&cobra.Command{
		Use:                "mcp",
		Short:              "Run the MCP server for one Claude Code session",
		Hidden:             true,
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return commandResult(a.mcpCmd(cmd.Context(), args))
		},
	})
}

// mcpCmd runs the MCP server over the process's own stdin and stdout; nothing
// else may be written to stdout, because that stream carries the MCP protocol.
func (a *app) mcpCmd(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("mcp", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, _ = fmt.Fprintln(a.stdout, mcpUsage)
			return exitOK
		}
		_, _ = fmt.Fprintf(a.stderr, "%s\npeers mcp: %s\n", mcpUsage, err)
		return exitUsage
	}
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintf(a.stderr, "%s\npeers mcp: unexpected argument %q\n", mcpUsage, fs.Arg(0))
		return exitUsage
	}
	if err := a.deps.runMCP(ctx); err != nil {
		_, _ = fmt.Fprintf(a.stderr, "peers mcp: %s\n", err)
		return exitFailure
	}
	return exitOK
}
