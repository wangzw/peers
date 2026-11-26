package main

import (
	"github.com/spf13/cobra"
)

// Startup failures keep the exit codes the service manager relies on.
func (a *app) serviceRunCommand() *cobra.Command {
	var (
		listen        string
		allowDegraded bool
		level         *levelFlags
	)
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run the local Broker in the foreground",
		Long:  "run starts the local Broker in the foreground. The service definitions start the Broker this way.",
		Args:  noPositionalArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			system, err := level.system(a.deps.euid())
			if err != nil {
				return err
			}
			if cmd.Flags().Changed("listen") && listen == "" {
				return usageErrorf("--listen requires a non-empty address; to stop listening, set broker.listen to an empty value in the configuration")
			}

			err = a.deps.runBroker(cmd.Context(), brokerOptions{
				System:        system,
				Listen:        listen,
				AllowDegraded: allowDegraded,
				Version:       a.version,
				LogOutput:     a.stderr,
			})
			if err == nil {
				return nil
			}

			failure := a.deps.classifyBrokerErr(err)
			return &exitError{code: exitCodeFor(failure), err: err}
		},
	}
	level = addLevelFlags(cmd)
	cmd.Flags().StringVar(&listen, "listen", "", "listen address, overriding broker.listen")
	cmd.Flags().BoolVar(&allowDegraded, "allow-degraded", false, "keep running and report failures on the local socket instead of exiting")
	return cmd
}
