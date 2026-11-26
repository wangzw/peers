package main

import (
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/wangzw/peers/internal/mcpserver/termcmd"
)

// parseAuditLimit parses in base 10 because pflag's Uint64 would read a leading
// zero as an octal prefix.
func parseAuditLimit(value string) (uint64, error) {
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, err
	}
	if parsed > math.MaxUint32 {
		return 0, fmt.Errorf("--limit must be at most %d", uint64(math.MaxUint32))
	}
	return parsed, nil
}

func (a *app) serviceStatusCommand() *cobra.Command {
	var level *levelFlags
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show the status of the selected Broker",
		Args:  noPositionalArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			system, err := level.system(a.deps.euid())
			if err != nil {
				return err
			}
			if err := a.deps.runStatus(cmd.Context(), system, level.explicit(), a.stdout); err != nil {
				// The status already names the cause and the repair, so a
				// diagnostics-only Broker fails without repeating them.
				if errors.Is(err, termcmd.ErrDiagnosticsOnly) {
					return commandResult(exitFailure)
				}
				return err
			}
			return nil
		},
	}
	level = addLevelFlags(cmd)
	return cmd
}

func (a *app) serviceAuditCommand() *cobra.Command {
	var (
		limitText string
		level     *levelFlags
	)
	cmd := &cobra.Command{
		Use:   "audit",
		Short: "Read the audit log from the selected Broker",
		Args:  noPositionalArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			system, err := level.system(a.deps.euid())
			if err != nil {
				return err
			}
			limit, err := parseAuditLimit(limitText)
			if err != nil {
				return usageError(err)
			}
			return a.deps.runAudit(cmd.Context(), system, level.explicit(), a.stdout, uint32(limit))
		},
	}
	level = addLevelFlags(cmd)
	cmd.Flags().StringVar(&limitText, "limit", "0", "maximum number of events to read; 0 means all")
	return cmd
}
