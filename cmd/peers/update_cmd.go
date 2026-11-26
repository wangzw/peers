package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/wangzw/peers/internal/broker/service"
	"github.com/wangzw/peers/internal/broker/update"
)

// updateLocalTakesEffectHint follows a local replacement; its wording is fixed
// and a change must update the tests.
const updateLocalTakesEffectHint = "The new binary takes effect the next time the Broker starts."

// updateSessionHint follows a Broker-applied update; its wording is fixed and a
// change must update the tests.
const updateSessionHint = "Sessions that are already open keep the old MCP server until they are reopened; this Broker's connections recover automatically."

// updateRestartWait bounds the wait for a restarted Broker to report the new
// version; 60 seconds covers the service manager's restart interval and the
// Broker's startup and readiness. It is a variable so a test can shorten it.
var updateRestartWait = 60 * time.Second

// updateStatusPollDelay separates UpdateStatus polls while the Broker restarts;
// it is a variable so a test can shorten it.
var updateStatusPollDelay = 250 * time.Millisecond

// These are test seams for the parts of the update command that reach the
// operating system.
var (
	updateExecutable   = os.Executable
	updateEvalSymlinks = filepath.EvalSymlinks
	updateNewClient    = func() *http.Client { return &http.Client{Transport: http.DefaultTransport} }
	updateGOOS         = runtime.GOOS
	updateGOARCH       = runtime.GOARCH
)

func newUpdateCommand(a *app) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "update",
		Short:   "Install the latest release of peers",
		GroupID: groupInstall,
		Args:    noPositionalArgs,
	}
	yes := addYesFlag(cmd)
	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		return a.updateCmd(cmd.Context(), *yes)
	}
	return cmd
}

func (a *app) updateCmd(ctx context.Context, yes bool) error {
	current := strings.TrimPrefix(a.version, "v")
	if !update.IsReleaseVersion(current) {
		return fmt.Errorf("version %q is not a release build; only release builds can update themselves", a.version)
	}

	if !yes {
		confirmed, err := a.confirm(fmt.Sprintf("peers %s is installed. Update to the latest release? [y/N] ", current), false)
		if err != nil {
			return err
		}
		if !confirmed {
			_, _ = fmt.Fprintln(a.stdout, "Update canceled; nothing was changed.")
			return nil
		}
	}

	result, err := a.deps.applyUpdateViaBroker(ctx)
	if err != nil {
		if errors.Is(err, errUserLevelUpdateUnsupported) || errors.Is(err, errNoRunningBroker) {
			return a.updateLocal(ctx, current)
		}
		return err
	}
	return a.reportBrokerUpdate(ctx, result)
}

// updateLocal replaces this account's binary in place and starts or stops no
// service, which may have been stopped on purpose.
func (a *app) updateLocal(ctx context.Context, current string) error {
	target, err := updateTarget()
	if err != nil {
		return err
	}
	if err := checkTargetDirWritable(filepath.Dir(target)); err != nil {
		// The framework prefixes the first line with the command path; the
		// sudo hint is a separate line.
		return fmt.Errorf("%w\nrun \"sudo peers update\" to update a system installation", err)
	}

	api := a.deps.updateAPIBase()
	client := updateNewClient()
	release, err := a.deps.updateLatest(ctx, client, api)
	if err != nil {
		return err
	}
	if !a.deps.updateNewer(current, release.Version) {
		_, _ = fmt.Fprintf(a.stdout, "peers %s is up to date\n", current)
		return nil
	}

	_, _ = fmt.Fprintf(a.stdout, "peers %s is installed; version %s is available.\n", current, release.Version)
	if release.URL != "" {
		_, _ = fmt.Fprintf(a.stdout, "Release: %s\n", release.URL)
	}

	if err := a.deps.updateApply(ctx, update.ApplyOptions{
		Target:  target,
		Version: release.Version,
		API:     api,
		Client:  client,
		GOOS:    updateGOOS,
		GOARCH:  updateGOARCH,
	}); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(a.stdout, updateLocalTakesEffectHint)
	return nil
}

func (a *app) reportBrokerUpdate(ctx context.Context, result brokerUpdateResult) error {
	from := strings.TrimPrefix(result.FromVersion, "v")
	to := strings.TrimPrefix(result.ToVersion, "v")
	if to == "" {
		_, _ = fmt.Fprintf(a.stdout, "peers %s is up to date\n", from)
		return nil
	}
	if !result.Restarting {
		_, _ = fmt.Fprintf(a.stdout, "peers %s is installed.\n", to)
		_, _ = fmt.Fprintln(a.stdout, updateSessionHint)
		return nil
	}

	_, _ = fmt.Fprintf(a.stdout, "The Broker is restarting with version %s.\n", to)
	if err := a.waitForBrokerVersion(ctx, to, result.UserLevel); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stdout, "The Broker is running peers %s.\n", to)
	_, _ = fmt.Fprintln(a.stdout, updateSessionHint)
	return nil
}

func (a *app) waitForBrokerVersion(ctx context.Context, want string, userLevel bool) error {
	want = strings.TrimPrefix(want, "v")
	deadline := time.Now().Add(updateRestartWait)
	var lastErr error
	for {
		status, err := a.deps.brokerUpdateStatus(ctx)
		switch {
		case err != nil:
			lastErr = err
		case strings.TrimPrefix(status.CurrentVersion, "v") == want:
			return nil
		default:
			lastErr = nil
		}
		if !time.Now().Before(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(updateStatusPollDelay):
		}
	}
	if lastErr != nil {
		return fmt.Errorf("the Broker did not report version %s within %s (last error: %v); check the service log: %s", want, humanDuration(updateRestartWait), lastErr, updateLogHint(userLevel))
	}
	return fmt.Errorf("the Broker did not report version %s within %s; check the service log: %s", want, humanDuration(updateRestartWait), updateLogHint(userLevel))
}

func humanDuration(d time.Duration) string {
	seconds := int64(d / time.Second)
	if seconds == 1 {
		return "1 second"
	}
	return fmt.Sprintf("%d seconds", seconds)
}

func updateLogHint(userLevel bool) string {
	switch {
	case updateGOOS == "darwin" && userLevel:
		return "~/Library/Logs/peers/broker.log (post-update helper: ~/Library/Logs/peers/" + service.LaunchdHelperLogName + ")"
	case updateGOOS == "darwin":
		return "/var/log/peers/broker.log (post-update helper: /var/log/peers/" + service.LaunchdHelperLogName + ")"
	case userLevel:
		return "journalctl --user -u " + service.UnitName
	default:
		return "journalctl -u " + service.UnitName
	}
}

func updateTarget() (string, error) {
	exe, err := updateExecutable()
	if err != nil {
		return "", fmt.Errorf("cannot locate the executable: %w", err)
	}
	real, err := updateEvalSymlinks(exe)
	if err != nil {
		return "", fmt.Errorf("cannot resolve the executable %q: %w", exe, err)
	}
	return real, nil
}

// checkTargetDirWritable proves the directory will accept the temporary file
// and rename Apply needs, before anything is downloaded.
func checkTargetDirWritable(dir string) error {
	f, err := os.CreateTemp(dir, ".peers-update-check-*")
	if err != nil {
		return fmt.Errorf("cannot write to %s: %w", dir, err)
	}
	name := f.Name()
	closeErr := f.Close()
	removeErr := os.Remove(name)
	if closeErr != nil {
		return fmt.Errorf("cannot write to %s: %w", dir, closeErr)
	}
	if removeErr != nil {
		return fmt.Errorf("cannot remove the probe file in %s: %w", dir, removeErr)
	}
	return nil
}

// isTerminal is a variable so a test can answer a confirmation question without
// a pseudo-terminal.
var isTerminal = func(r io.Reader) bool {
	f, ok := r.(*os.File)
	if !ok {
		return false
	}
	return term.IsTerminal(int(f.Fd()))
}

// readAnswer reads one line from r; end of input before a newline still counts
// as the answer, not an error.
func readAnswer(r io.Reader) (string, error) {
	line, err := bufio.NewReader(r).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	return strings.TrimSpace(line), nil
}

func isYes(answer string) bool {
	return strings.EqualFold(answer, "y") || strings.EqualFold(answer, "yes")
}
