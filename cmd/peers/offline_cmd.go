package main

import (
	"github.com/spf13/cobra"
)

func (a *app) serviceOnlineCommand() *cobra.Command {
	return a.serviceSetOnlineCommand("online", true, "Make the selected Broker accept messages from other hosts")
}

func (a *app) serviceOfflineCommand() *cobra.Command {
	return a.serviceSetOnlineCommand("offline", false, "Make the selected Broker refuse messages from other hosts")
}

func (a *app) serviceSetOnlineCommand(name string, online bool, short string) *cobra.Command {
	var level *levelFlags
	cmd := &cobra.Command{
		Use:   name,
		Short: short,
		Args:  noPositionalArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			system, err := level.system(a.deps.euid())
			if err != nil {
				return err
			}
			return a.deps.runSetOnline(cmd.Context(), system, level.explicit(), online, a.stdout)
		},
	}
	level = addLevelFlags(cmd)
	return cmd
}
