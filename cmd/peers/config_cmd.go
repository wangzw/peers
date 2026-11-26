package main

import (
	"strings"

	"github.com/spf13/cobra"

	"github.com/wangzw/peers/internal/broker"
)

func (a *app) newConfigCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "config",
		Short:   "Read and change the Broker configuration",
		GroupID: groupLocal,
	}
	cmd.AddCommand(
		a.newConfigListCommand(),
		a.newConfigGetCommand(),
		a.newConfigSetCommand(),
	)
	return cmd
}

func (a *app) newConfigListCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List every configuration item",
		Args:  noPositionalArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.deps.runConfigGet(cmd.Context(), "", a.stdout)
		},
	}
}

func (a *app) newConfigGetCommand() *cobra.Command {
	return &cobra.Command{
		Use:               "get <key>",
		Short:             "Show one configuration item",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeConfigKey,
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.deps.runConfigGet(cmd.Context(), args[0], a.stdout)
		},
	}
}

func (a *app) newConfigSetCommand() *cobra.Command {
	return &cobra.Command{
		Use:               "set <key> <value>",
		Short:             "Change one configuration item",
		Args:              cobra.ExactArgs(2),
		ValidArgsFunction: completeConfigKey,
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.deps.runConfigSet(cmd.Context(), args[0], args[1], a.stdout)
		},
	}
}

func completeConfigKey(_ *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if len(args) > 0 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	names := broker.ConfigKeys()
	matches := make([]string, 0, len(names))
	for _, name := range names {
		if strings.HasPrefix(name, toComplete) {
			matches = append(matches, name)
		}
	}
	return matches, cobra.ShellCompDirectiveNoFileComp
}
