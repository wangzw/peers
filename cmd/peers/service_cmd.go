package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/wangzw/peers/internal/broker/service"
	"github.com/wangzw/peers/internal/mcpserver/termcmd"
)

// The test seams replace the installation steps so the command tests can check
// the option mapping and the exit-code mapping without touching the system. The
// executing account comes from deps.euid.
var (
	defaultOptionsFn = service.DefaultOptions
	installFn        = service.Install
	uninstallFn      = service.Uninstall
	startFn          = service.Start
	stopFn           = service.Stop
	restartFn        = service.Restart
	installedFn      = service.Installed
)

// serviceCommand builds the "service" group: everything that acts on the local
// Broker. The group itself has no body; the tree-wide fallback prints its usage
// and rejects an unknown verb.
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

// serviceInstallCommand builds "service install". install enables the service
// and starts it immediately. The level comes from --system/--user or the
// executing account; --admin-group is accepted only with --system.
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
			// Only an explicit --admin-group reaches the installation, so it can
			// tell a group the administrator accepted from the default one.
			if cmd.Flags().Changed("admin-group") {
				opts.AdminGroup = adminGroup
			}
			result, err := installFn(opts)
			if err != nil {
				var degraded *service.DegradedError
				if errors.As(err, &degraded) {
					// The service is installed and running. The summary still
					// applies; the degraded lines explain what is wrong and the
					// exit code tells the installer to keep going.
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

// serviceRemoveCommand builds "service remove", which stops the service and
// removes its definition but keeps the binary and the data.
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

// serviceControlCommand builds "service start", "service stop" or
// "service restart" for the level selected by --system/--user or the executing
// account.
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

// serviceNotInstalledError explains that the command's level has no service. A
// non-root account that only has the system install is pointed at the
// sudo form of the same command instead of installing anything.
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

// printServiceAction reports the one-line result of a successful command.
func (a *app) printServiceAction(command string, system bool) {
	past := map[string]string{"start": "started", "stop": "stopped", "restart": "restarted"}[command]
	level := "user"
	if system {
		level = "system"
	}
	_, _ = fmt.Fprintf(a.stdout, "The %s Broker service has %s.\n", level, past)
}

// printInstalled reports a successful installation: the level, the files it
// uses, the commands that control it and how to remove it. system selects the
// level's wording and the sudo prefix. A kept system configuration adds
// the command that hands it to the administration group.
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

// adminGroupState describes how the installation got the administration group:
// created by this run, reused with the members it lists, reused with none, or
// reused without reading the members because the decision was already settled.
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

// printServiceDegraded prints the two status lines the service commands share
// with the terminal's degraded display.
func (a *app) printServiceDegraded(state service.DegradedState) {
	_ = termcmd.WriteDegraded(a.stderr, state.Level, state.Reason, state.Fix)
}

// printUninstalled reports what remove deleted, or that nothing was installed.
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
