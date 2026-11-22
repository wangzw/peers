package main

import (
	"github.com/spf13/cobra"
)

// addBrokerAndKeyCommands adds the "broker" and "key" noun commands. Both are
// direct children of the root command and belong to the Pairing help group.
func (a *app) addBrokerAndKeyCommands(root *cobra.Command) {
	root.AddCommand(a.newBrokerCommand(), a.newKeyCommand())
}

// newBrokerCommand builds "peers broker", which groups the commands that list,
// pair with and unpair other Brokers.
func (a *app) newBrokerCommand() *cobra.Command {
	broker := &cobra.Command{
		Use:     "broker",
		Short:   "Manage the Brokers this host is paired with",
		GroupID: groupPairing,
	}
	broker.AddCommand(
		&cobra.Command{
			Use:   "list",
			Short: "List the Brokers this host knows about",
			Args:  noPositionalArgs,
			RunE: func(cmd *cobra.Command, _ []string) error {
				return a.deps.runPeerList(cmd.Context(), a.stdout)
			},
		},
		a.newBrokerConnectCommand(),
		a.newBrokerDisconnectCommand(),
	)
	return broker
}

// newBrokerConnectCommand builds "peers broker connect <address> --key <key>
// [--alias <alias>]". The address may come before or after the flags.
func (a *app) newBrokerConnectCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "connect <address>",
		Short: "Pair with another Broker",
		Args:  oneArgument("<address>"),
		RunE: func(cmd *cobra.Command, args []string) error {
			key, _ := cmd.Flags().GetString("key")
			alias, _ := cmd.Flags().GetString("alias")
			if key == "" {
				return usageErrorf("missing --key")
			}
			return a.deps.runPeerConnect(cmd.Context(), args[0], key, alias, a.stdout, a.stderr)
		},
	}
	cmd.Flags().String("key", "", "one-time pairing key printed by the other Broker")
	cmd.Flags().String("alias", "", "local alias for the paired Broker")
	return cmd
}

// newBrokerDisconnectCommand builds "peers broker disconnect <broker>". The
// argument is completed with the aliases and fingerprints of the paired
// Brokers. A Broker that cannot be reached yields no candidates and no error.
func (a *app) newBrokerDisconnectCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "disconnect <broker>",
		Short: "Unpair from a Broker by alias or fingerprint",
		Args:  oneArgument("<broker>"),
		ValidArgsFunction: func(cmd *cobra.Command, args []string, _ string) ([]string, cobra.ShellCompDirective) {
			if len(args) > 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			names, err := a.deps.peerNames(cmd.Context())
			if err != nil {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return names, cobra.ShellCompDirectiveNoFileComp
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.deps.runPeerDisconnect(cmd.Context(), args[0], a.stdout)
		},
	}
	return cmd
}

// newKeyCommand builds "peers key", which groups the pairing key commands.
func (a *app) newKeyCommand() *cobra.Command {
	key := &cobra.Command{
		Use:     "key",
		Short:   "Manage the one-time pairing keys",
		GroupID: groupPairing,
	}
	key.AddCommand(&cobra.Command{
		Use:   "create",
		Short: "Create a one-time pairing key",
		Args:  noPositionalArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.deps.runPairingKeyCreate(cmd.Context(), a.stdout)
		},
	})
	return key
}

// oneArgument requires exactly one positional argument. The second positional
// is reported by position alone and never echoed: it is usually a pairing key
// whose --key flag was left out.
func oneArgument(what string) cobra.PositionalArgs {
	return func(_ *cobra.Command, args []string) error {
		switch {
		case len(args) == 0:
			return usageErrorf("expected %s", what)
		case len(args) > 1:
			return usageErrorf("unexpected positional argument at position 2")
		}
		return nil
	}
}
