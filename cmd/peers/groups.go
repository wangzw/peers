package main

import (
	"github.com/spf13/cobra"
)

func addPairingCommands(root *cobra.Command, a *app) {
	a.addBrokerAndKeyCommands(root)
}

func addConfigCommands(root *cobra.Command, a *app) {
	root.AddCommand(a.newConfigCommand())
}

func addServiceCommands(root *cobra.Command, a *app) {
	root.AddCommand(a.serviceCommand())
}

func addInstallationCommands(root *cobra.Command, a *app) {
	root.AddCommand(newUpdateCommand(a), newUninstallCommand(a))
}
