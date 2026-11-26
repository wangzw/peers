package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wangzw/peers/internal/broker/service"
)

const uninstallDataPrompt = "Also remove configuration, identity, audit and service logs? [y/N] "

// uninstallSystemPrompt is written to standard error, like every other
// confirmation question.
const uninstallSystemPrompt = "Remove the system installation through sudo? [y/N] "

// The test seams replace the installation steps so the command can be checked
// without touching the system.
var (
	uninstallOptionsFn            = service.DefaultOptions
	uninstallInstalledFn          = service.Installed
	uninstallServiceFn            = service.Uninstall
	uninstallServiceBinaryFn      = service.ServiceBinary
	uninstallRemoveRuntimeDirFn   = service.RemoveRuntimeDir
	uninstallPurgeDataFn          = service.PurgeData
	uninstallOtherAccountsFn      = service.OtherAccountBrokers
	uninstallMCPFn                = defaultRemoveMCPRegistration
	uninstallMCPLookupFn          = defaultLookupPeerMCP
	uninstallExecutableFn         = os.Executable
	uninstallEvalSymlinksFn       = filepath.EvalSymlinks
	uninstallGetenvFn             = os.Getenv
	uninstallFileOwnerFn          = fileOwner
	uninstallRemoveFileFn         = os.Remove
	uninstallSystemInstalledCheck = func(opts service.Options) bool {
		installed, err := service.Installed(opts)
		return err == nil && installed
	}
	uninstallSudoFn = runSudoUninstall
)

func newUninstallCommand(a *app) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "uninstall",
		Short:   "Remove the peers installation",
		GroupID: groupInstall,
		Args:    noPositionalArgs,
	}
	purge := cmd.Flags().Bool("purge", false, "also remove configuration, identity, audit and service logs")
	yes := addYesFlag(cmd)
	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		return a.uninstallCmd(cmd.Context(), *purge, *yes)
	}
	return cmd
}

func mcpPeerCommand(account, exe string) bool {
	reg := uninstallMCPLookupFn(account)
	if reg.command == "" {
		return false
	}
	command := reg.command
	target := exe
	if resolved, err := uninstallEvalSymlinksFn(exe); err == nil {
		target = resolved
	}
	return command == target || command == exe
}

func installationBinary(opts service.Options, account, exe string) (string, bool) {
	defaultPath := defaultInstallPath(opts)
	if !samePath(exe, defaultPath) && !samePath(exe, rootedPath(opts, defaultPath)) {
		return "", false
	}
	if mcpPeerCommand(account, exe) {
		return resolvePath(exe), true
	}
	if mcpPeerCommand(account, defaultPath) {
		return resolvePath(defaultPath), true
	}
	return "", false
}

func samePath(a, b string) bool {
	return filepath.Clean(a) == filepath.Clean(b)
}

func defaultInstallPath(opts service.Options) string {
	if opts.System {
		return "/usr/local/bin/peers"
	}
	return filepath.Join(opts.Home, ".local", "bin", "peers")
}

func defaultLookupPeerMCP(account string) mcpRegistration {
	return lookupPeerMCP(runMCPCommand, account)
}

func defaultRemoveMCPRegistration(account, binary string) (string, error) {
	return removeMCPRegistration(runMCPCommand, account, binary)
}

func runMCPCommand(name string, args ...string) ([]byte, error) {
	return exec.Command(name, args...).CombinedOutput()
}

func (a *app) uninstallCmd(ctx context.Context, purge, yes bool) error {
	userOpts, err := uninstallOptionsFn(false)
	if err != nil {
		return uninstallError("cannot determine the options", err)
	}
	if !filepath.IsAbs(userOpts.Home) {
		return uninstallError("cannot determine the options",
			fmt.Errorf("the home directory %q is not an absolute path", userOpts.Home))
	}
	systemOpts, err := uninstallOptionsFn(true)
	if err != nil {
		return uninstallError("cannot determine the options", err)
	}

	if a.deps.euid() == 0 {
		return a.uninstallAsRoot(ctx, systemOpts, purge, yes)
	}
	return a.uninstallAsAccount(ctx, userOpts, systemOpts, purge, yes)
}

func (a *app) uninstallAsRoot(ctx context.Context, opts service.Options, purge, yes bool) error {
	noServiceBinary := ""
	installed, err := uninstallInstalledFn(opts)
	if err != nil {
		return uninstallError("cannot inspect the installation", err)
	}
	if !installed {
		exe, err := uninstallExecutableFn()
		if err != nil {
			return uninstallError("cannot locate the executable", err)
		}
		accounts := []string{""}
		if user := uninstallGetenvFn("SUDO_USER"); user != "" && user != "root" {
			accounts = append(accounts, user)
		}
		for _, account := range accounts {
			if binary, ok := installationBinary(opts, account, exe); ok {
				noServiceBinary = binary
				installed = true
				break
			}
		}
	}
	if !installed {
		_, _ = fmt.Fprintln(a.stdout, "peers is not installed")
		return nil
	}
	return a.uninstallLevel(ctx, opts, true, purge, yes, noServiceBinary)
}

func (a *app) uninstallAsAccount(ctx context.Context, userOpts, systemOpts service.Options, purge, yes bool) error {
	noServiceBinary := ""
	installed, err := uninstallInstalledFn(userOpts)
	if err != nil {
		return uninstallError("cannot inspect the installation", err)
	}
	if !installed {
		exe, err := uninstallExecutableFn()
		if err != nil {
			return uninstallError("cannot locate the executable", err)
		}
		if binary, ok := installationBinary(userOpts, "", exe); ok {
			noServiceBinary = binary
			installed = true
		}
	}
	if installed {
		if err := a.uninstallLevel(ctx, userOpts, false, purge, yes, noServiceBinary); err != nil {
			return err
		}
		if uninstallSystemInstalledCheck(systemOpts) {
			_, _ = fmt.Fprintf(a.stdout, "A system installation is still present; run: %s\n", sudoUninstallCommand(purge))
		}
		return nil
	}

	systemInstalled, err := uninstallInstalledFn(systemOpts)
	if err != nil {
		return uninstallError("cannot inspect the installation", err)
	}
	if !systemInstalled {
		_, _ = fmt.Fprintln(a.stdout, "peers is not installed")
		return nil
	}

	if yes {
		return a.sudoUninstall(ctx, purge, true)
	}
	if isTerminal(a.stdin) {
		confirmed, err := a.confirm(uninstallSystemPrompt, false)
		if err != nil {
			return uninstallError("cannot confirm the system uninstall", err)
		}
		if confirmed {
			// The answer approves only the sudo rerun, so the child asks the
			// data question on the caller's terminal.
			return a.sudoUninstall(ctx, purge, false)
		}
		_, _ = fmt.Fprintf(a.stdout, "Run: %s\n", sudoUninstallCommand(purge))
		return nil
	}
	return fmt.Errorf("only a system installation is present; run: %s", sudoUninstallCommand(purge))
}

// sudoUninstall forwards --yes only when the caller gave it, and runs the
// current executable's path rather than the name on PATH, because sudo's
// secure_path may not contain it.
func (a *app) sudoUninstall(ctx context.Context, purge, yes bool) error {
	exe, err := uninstallExecutableFn()
	if err != nil {
		return uninstallError("cannot locate the executable", err)
	}
	args := []string{"uninstall"}
	if purge {
		args = append(args, "--purge")
	}
	if yes {
		args = append(args, "--yes")
	}
	if err := uninstallSudoFn(ctx, exe, args, a.stdin, a.stdout, a.stderr); err != nil {
		return uninstallError("cannot run sudo", err)
	}
	return nil
}

func runSudoUninstall(ctx context.Context, exe string, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	cmd := exec.CommandContext(ctx, "sudo", append([]string{exe}, args...)...)
	cmd.Stdin = stdin
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	err := cmd.Run()
	if err == nil {
		return nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return &exitError{code: exitErr.ExitCode()}
	}
	return err
}

func sudoUninstallCommand(purge bool) string {
	command := "sudo peers uninstall"
	if purge {
		command += " --purge"
	}
	return command + " --yes"
}

func (a *app) uninstallLevel(ctx context.Context, opts service.Options, system, purge, yes bool, binary string) error {
	account := ""
	if system {
		account = uninstallGetenvFn("SUDO_USER")
		if account == "root" {
			account = ""
		}
	}

	// Confirmation comes after the level is known but before anything is read
	// or removed. Without a terminal, a command that would remove data needs
	// --yes and fails before touching anything otherwise.
	purgeData := purge
	if !yes {
		if !isTerminal(a.stdin) {
			return errNeedsYes
		}
		if !purgeData {
			confirmed, err := a.confirm(uninstallDataPrompt, false)
			if err != nil {
				return uninstallError("cannot confirm the data removal", err)
			}
			purgeData = confirmed
		}
	}

	if binary == "" {
		path, ok, err := uninstallServiceBinaryFn(opts)
		if err != nil {
			return uninstallError("cannot read the service definition", err)
		}
		if ok {
			binary = path
		} else {
			exe, err := uninstallExecutableFn()
			if err != nil {
				return uninstallError("cannot locate the executable", err)
			}
			if candidate, ok := installationBinary(opts, account, exe); ok {
				binary = candidate
			} else {
				_, _ = fmt.Fprintf(a.stdout, "kept the running executable %s: it is not the installation binary at %s\n",
					exe, defaultInstallPath(opts))
			}
		}
	}

	result, err := uninstallServiceFn(opts)
	if err != nil {
		return uninstallError("cannot stop and remove the service", err)
	}
	if result.Removed {
		_, _ = fmt.Fprintf(a.stdout, "stopped and removed the service %s\n", result.UnitPath)
	} else {
		_, _ = fmt.Fprintln(a.stdout, "the service was not installed")
	}

	if binary == "" {
		_, _ = fmt.Fprintln(a.stdout, "skipped the peer MCP registration: the installation's binary is unknown")
	} else if system && account == "" {
		_, _ = fmt.Fprintln(a.stdout, "skipped the peer MCP registration; run claude mcp remove -s user peer in each account that registered it")
	} else {
		outcome, err := uninstallMCPFn(account, binary)
		if err != nil {
			return uninstallError("cannot remove the MCP registration", err)
		}
		_, _ = fmt.Fprintln(a.stdout, outcome)
	}

	if err := a.removeInstallationFiles(opts, system); err != nil {
		return uninstallError("cannot remove the shell completions and man pages", err)
	}

	removedDirs, err := uninstallRemoveRuntimeDirFn(opts)
	if err != nil {
		return uninstallError("cannot remove the runtime directory", err)
	}
	if len(removedDirs) == 0 {
		_, _ = fmt.Fprintln(a.stdout, "the runtime directory was already gone")
	}
	for _, path := range removedDirs {
		_, _ = fmt.Fprintf(a.stdout, "removed %s\n", path)
	}

	if purgeData {
		removed, purgeErr := uninstallPurgeDataFn(opts)
		for _, entry := range removed {
			if strings.HasPrefix(entry, "kept ") {
				_, _ = fmt.Fprintln(a.stdout, entry)
				continue
			}
			_, _ = fmt.Fprintf(a.stdout, "removed %s\n", entry)
		}
		if purgeErr != nil {
			return uninstallError("cannot remove the data", purgeErr)
		}
		if len(removed) == 0 {
			_, _ = fmt.Fprintln(a.stdout, "no configuration, data or logs were present")
		}
	} else {
		_, _ = fmt.Fprintf(a.stdout, "kept configuration, data and logs: %s\n", strings.Join(keptPaths(opts), ", "))
	}

	if system {
		accounts, err := uninstallOtherAccountsFn(opts)
		if err != nil {
			return uninstallError("cannot list other accounts' Brokers", err)
		}
		if len(accounts) > 0 {
			_, _ = fmt.Fprintf(a.stdout, "Other accounts run a user Broker: %s.\n", strings.Join(accounts, ", "))
			if binary != "" {
				_, _ = fmt.Fprintf(a.stdout, "Their Brokers will fail to start after %s is removed.\n", binary)
			} else {
				_, _ = fmt.Fprintln(a.stdout, "Their Brokers may fail to start if the shared binary is removed.")
			}
		}
	}

	if binary != "" {
		if err := a.removeServiceBinary(opts, system, binary); err != nil {
			return uninstallError("cannot remove the binary", err)
		}
	}

	if system {
		_, _ = fmt.Fprintln(a.stdout, "each account that registered the peer MCP server must remove it with: claude mcp remove -s user peer")
	}
	return nil
}

// removeInstallationFiles deletes the shell completion files the install script
// wrote for the level and, for a system installation, this project's man pages.
// A completion is removed only when its content carries cobra's marker for this
// program; any other file or a directory at those paths is kept, and every
// removed and kept path is reported. A path that is already gone is not an error.
func (a *app) removeInstallationFiles(opts service.Options, system bool) error {
	for _, path := range completionFiles(opts, system) {
		if err := a.removeCompletion(path); err != nil {
			return err
		}
	}
	if system {
		// The narrow patterns keep another program's man page such as peerstat.1.
		for _, pattern := range []string{"peers.1", "peers-*.1"} {
			glob := filepath.Join(rootedPath(opts, "/usr/local/share/man/man1"), pattern)
			pages, err := filepath.Glob(glob)
			if err != nil {
				return fmt.Errorf("cannot list %s: %w", glob, err)
			}
			for _, path := range pages {
				if err := a.removeInstallationFile(path); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (a *app) removeCompletion(path string) error {
	info, err := os.Lstat(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return fmt.Errorf("cannot inspect %s: %w", path, err)
	}
	if info.IsDir() {
		_, _ = fmt.Fprintf(a.stdout, "kept %s: it is a directory\n", path)
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("cannot read %s: %w", path, err)
	}
	if !generatedCompletion(data) {
		_, _ = fmt.Fprintf(a.stdout, "kept %s: it was not written by peers\n", path)
		return nil
	}
	return a.removeInstallationFile(path)
}

func generatedCompletion(data []byte) bool {
	for _, marker := range []string{
		"# bash completion V2 for peers",
		"#compdef peers",
		"# fish completion for peers",
	} {
		if bytes.Contains(data, []byte(marker)) {
			return true
		}
	}
	return false
}

func (a *app) removeInstallationFile(path string) error {
	info, err := os.Lstat(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return fmt.Errorf("cannot inspect %s: %w", path, err)
	}
	if info.IsDir() {
		_, _ = fmt.Fprintf(a.stdout, "kept %s: it is a directory\n", path)
		return nil
	}
	if err := os.Remove(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("cannot remove %s: %w", path, err)
	}
	_, _ = fmt.Fprintf(a.stdout, "removed %s\n", path)
	return nil
}

// The user installation never writes the zsh completion, so a user-level
// uninstall never removes it.
func completionFiles(opts service.Options, system bool) []string {
	if system {
		return []string{
			rootedPath(opts, "/usr/local/share/bash-completion/completions/peers"),
			rootedPath(opts, "/usr/local/share/zsh/site-functions/_peers"),
			rootedPath(opts, "/usr/local/share/fish/vendor_completions.d/peers.fish"),
		}
	}
	return []string{
		rootedPath(opts, filepath.Join(opts.Home, ".local", "share", "bash-completion", "completions", "peers")),
		rootedPath(opts, filepath.Join(opts.Home, ".config", "fish", "completions", "peers.fish")),
	}
}

func (a *app) removeServiceBinary(opts service.Options, system bool, binary string) error {
	path := rootedPath(opts, binary)
	if _, err := os.Lstat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			_, _ = fmt.Fprintf(a.stdout, "the binary %s was already gone\n", binary)
			return nil
		}
		return fmt.Errorf("cannot inspect %s: %w", binary, err)
	}
	if reason := serviceBinaryIdentity(opts, binary); reason != "" {
		_, _ = fmt.Fprintf(a.stdout, "kept the binary %s: %s\n", binary, reason)
		return nil
	}
	if !system {
		uid, err := uninstallFileOwnerFn(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				_, _ = fmt.Fprintf(a.stdout, "the binary %s was already gone\n", binary)
				return nil
			}
			return fmt.Errorf("cannot inspect %s: %w", binary, err)
		}
		if uid != uint32(opts.EUID) {
			_, _ = fmt.Fprintf(a.stdout, "kept the binary %s: it belongs to another account\n", binary)
			return nil
		}
	}
	if err := uninstallRemoveFileFn(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			_, _ = fmt.Fprintf(a.stdout, "the binary %s was already gone\n", binary)
			return nil
		}
		return fmt.Errorf("cannot remove %s: %w", binary, err)
	}
	_, _ = fmt.Fprintf(a.stdout, "removed the binary %s\n", binary)
	return nil
}

// serviceBinaryIdentity returns why binary is not this installation's peers
// binary, or an empty string when it is. A file qualifies only by the name
// peers and by being the running executable; nothing else is deleted with
// root's permission, and an unrelated file is never executed to find out.
func serviceBinaryIdentity(opts service.Options, binary string) string {
	manual := "remove it manually with: rm " + binary
	if !filepath.IsAbs(binary) {
		return "it is not an absolute path; " + manual
	}
	if filepath.Base(binary) != "peers" {
		return "its name is not peers; " + manual
	}
	path := rootedPath(opts, binary)
	info, err := os.Stat(path)
	if err != nil {
		return "it could not be inspected: " + err.Error() + "; " + manual
	}
	if exe, err := uninstallExecutableFn(); err == nil {
		if exeInfo, err := os.Stat(exe); err == nil && os.SameFile(info, exeInfo) {
			return ""
		}
	}
	return "it is not this peers binary; " + manual
}

func keptPaths(opts service.Options) []string {
	var paths []string
	if opts.System {
		paths = append(paths, "/etc/peers", "/var/lib/peers")
		if opts.GOOS == "darwin" {
			paths = append(paths, "/var/log/peers")
		} else {
			paths = append(paths, "/var/log/peers/audit.log")
		}
		return paths
	}
	paths = append(paths, filepath.Join(opts.Home, ".peers"))
	if opts.GOOS == "darwin" {
		paths = append(paths, filepath.Join(opts.Home, "Library", "Logs", "peers"))
	}
	return paths
}

func rootedPath(opts service.Options, path string) string {
	if opts.Root == "" {
		return path
	}
	return filepath.Join(opts.Root, path)
}

func resolvePath(path string) string {
	if resolved, err := uninstallEvalSymlinksFn(path); err == nil {
		return resolved
	}
	return path
}

func fileOwner(path string) (uint32, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return 0, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, fmt.Errorf("no owner information for %s", path)
	}
	return st.Uid, nil
}

func uninstallError(step string, err error) error {
	return fmt.Errorf("%s: %w", step, err)
}
