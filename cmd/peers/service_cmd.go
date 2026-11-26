package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/wangzw/peers/internal/broker/service"
	"github.com/wangzw/peers/internal/mcpserver/termcmd"
)

// These are test seams replacing the installation steps; the executing account
// comes from deps.euid.
var (
	defaultOptionsFn = service.DefaultOptions
	installFn        = service.Install
	uninstallFn      = service.Uninstall
	startFn          = service.Start
	stopFn           = service.Stop
	restartFn        = service.Restart
	installedFn      = service.Installed
)

func (a *app) serviceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "service",
		Short:   "Run and manage the local Broker service",
		Long:    "service runs the local Broker in the foreground and installs, controls and inspects its service.",
		GroupID: groupLocal,
	}
	cmd.AddCommand(
		a.serviceRunCommand(),
		a.serviceInstallCommand(),
		a.serviceRemoveCommand(),
		a.serviceControlCommand("start", "Start the local Broker service"),
		a.serviceControlCommand("stop", "Stop the local Broker service"),
		a.serviceControlCommand("restart", "Restart the local Broker service"),
		a.serviceStatusCommand(),
		a.serviceOnlineCommand(),
		a.serviceOfflineCommand(),
		a.serviceAuditCommand(),
	)
	return cmd
}

func (a *app) serviceInstallCommand() *cobra.Command {
	var (
		adminGroup string
		level      *levelFlags
	)
	cmd := &cobra.Command{
		Use:   "install",
		Short: "Install the local Broker service",
		Args:  noPositionalArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			system, err := level.system(a.deps.euid())
			if err != nil {
				return err
			}
			if cmd.Flags().Changed("admin-group") && !system {
				return usageErrorf("--admin-group requires --system")
			}
			if !service.ValidAdminGroupName(adminGroup) {
				return usageErrorf("invalid --admin-group %q: a group name must start with a lowercase letter or underscore and contain only lowercase letters, digits, underscores and hyphens", adminGroup)
			}

			opts, err := defaultOptionsFn(system)
			if err != nil {
				return err
			}
			// Only an explicit --admin-group reaches the installation, so a group
			// the administrator accepted can be told from the default.
			if cmd.Flags().Changed("admin-group") {
				opts.AdminGroup = adminGroup
			}
			result, err := installFn(opts)
			if err != nil {
				var degraded *service.DegradedError
				if errors.As(err, &degraded) {
					a.printInstalled(result, system)
					a.printServiceDegraded(degraded.State)
					return commandResult(exitServiceDegraded)
				}
				return err
			}
			a.printInstalled(result, system)
			return nil
		},
	}
	level = addLevelFlags(cmd)
	cmd.Flags().StringVar(&adminGroup, "admin-group", service.DefaultAdminGroup, "group that administers a system Broker")
	return cmd
}

func (a *app) serviceRemoveCommand() *cobra.Command {
	var level *levelFlags
	cmd := &cobra.Command{
		Use:   "remove",
		Short: "Remove the local Broker service",
		Args:  noPositionalArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			system, err := level.system(a.deps.euid())
			if err != nil {
				return err
			}
			opts, err := defaultOptionsFn(system)
			if err != nil {
				return err
			}
			result, err := uninstallFn(opts)
			if err != nil {
				return err
			}
			a.printUninstalled(result)
			return nil
		},
	}
	level = addLevelFlags(cmd)
	return cmd
}

func (a *app) serviceControlCommand(name, short string) *cobra.Command {
	var level *levelFlags
	cmd := &cobra.Command{
		Use:   name,
		Short: short,
		Args:  noPositionalArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			system, err := level.system(a.deps.euid())
			if err != nil {
				return err
			}
			opts, err := defaultOptionsFn(system)
			if err != nil {
				return err
			}

			var operation func(service.Options) (service.Result, error)
			switch name {
			case "start":
				operation = startFn
			case "stop":
				operation = stopFn
			case "restart":
				operation = restartFn
			}
			_, err = operation(opts)
			if err == nil {
				a.printServiceAction(name, system)
				return nil
			}

			var degraded *service.DegradedError
			if errors.As(err, &degraded) {
				a.printServiceAction(name, system)
				a.printServiceDegraded(degraded.State)
				return commandResult(exitServiceDegraded)
			}
			if errors.Is(err, service.ErrNotInstalled) {
				return a.serviceNotInstalledError(name, system)
			}
			return err
		},
	}
	level = addLevelFlags(cmd)
	return cmd
}

func (a *app) serviceNotInstalledError(command string, system bool) error {
	if !system && a.deps.euid() != 0 {
		systemOpts, err := defaultOptionsFn(true)
		if err == nil {
			if installed, instErr := installedFn(systemOpts); instErr == nil && installed {
				return fmt.Errorf("no user service is installed, but the system service is; run `sudo peers service %s --system`", command)
			}
		}
	}

	level, install := "user", "peers service install"
	if system {
		level, install = "system", "sudo peers service install --system"
	}
	return fmt.Errorf("no %s service is installed; install it with `%s`", level, install)
}

func (a *app) printServiceAction(command string, system bool) {
	past := map[string]string{"start": "started", "stop": "stopped", "restart": "restarted"}[command]
	level := "user"
	if system {
		level = "system"
	}
	_, _ = fmt.Fprintf(a.stdout, "The %s Broker service has %s.\n", level, past)
}

func (a *app) printInstalled(result service.Result, system bool) {
	level, start, sudo := "user service", "login", ""
	if system {
		level, start, sudo = "system service", "boot", "sudo "
	}
	_, _ = fmt.Fprintf(a.stdout, "peers %s installed (%s, starts at %s)\n", a.version, level, start)
	_, _ = fmt.Fprintf(a.stdout, "  %-13s%s\n", "binary:", result.Binary)
	configState := "kept"
	if result.ConfigNew {
		configState = "generated"
	}
	_, _ = fmt.Fprintf(a.stdout, "  %-13s%s (%s)\n", "config:", result.ConfigPath, configState)
	if system && result.AdminGroup != "" {
		_, _ = fmt.Fprintf(a.stdout, "  %-13s%s (%s)\n", "admin group:", result.AdminGroup, adminGroupState(result))
	}
	_, _ = fmt.Fprintf(a.stdout, "  %-13s%s\n", "run log:", result.RunLog)
	_, _ = fmt.Fprintf(a.stdout, "  %-13s%s\n", "audit log:", result.AuditLog)
	_, _ = fmt.Fprintf(a.stdout, "  %-13s%speers service start\n", "start:", sudo)
	_, _ = fmt.Fprintf(a.stdout, "  %-13s%speers service stop\n", "stop:", sudo)
	_, _ = fmt.Fprintf(a.stdout, "  %-13s%speers service restart\n", "restart:", sudo)
	_, _ = fmt.Fprintf(a.stdout, "  %-13speers uninstall\n", "uninstall:")
	if !result.ConfigNew && result.AdminGroup != "" {
		_, _ = fmt.Fprintf(a.stdout, "To delegate administration, run: chgrp %s %s && chmod 0660 %s\n",
			result.AdminGroup, result.ConfigPath, result.ConfigPath)
	}
}

func adminGroupState(result service.Result) string {
	switch {
	case result.AdminGroupCreated:
		return "created"
	case !result.AdminGroupMembersQueried:
		return "existing"
	case len(result.AdminGroupMembers) > 0:
		return "existing, members: " + strings.Join(result.AdminGroupMembers, ", ")
	default:
		return "existing, no members listed"
	}
}

func (a *app) printServiceDegraded(state service.DegradedState) {
	_ = termcmd.WriteDegraded(a.stderr, state.Level, state.Reason, state.Fix)
}

func (a *app) printUninstalled(result service.Result) {
	if !result.Removed {
		_, _ = fmt.Fprintln(a.stdout, "The peers service is not installed.")
		return
	}
	if result.Domain != "" {
		_, _ = fmt.Fprintf(a.stdout, "uninstalled %s from %s\n", service.Label, result.Domain)
		return
	}
	_, _ = fmt.Fprintf(a.stdout, "uninstalled %s\n", service.UnitName)
}
