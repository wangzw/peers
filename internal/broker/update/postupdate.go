package update

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/wangzw/peers/internal/broker/service"
)

const (
	// postUpdateTimeout bounds the new binary's post-update step. Rewriting a
	// unit and asking the service manager to reload it is quick; a binary that
	// hangs must not keep the previous version from coming back.
	postUpdateTimeout = 30 * time.Second

	// postUpdateExitNotNeeded is the exit code of a binary that has no
	// post-update command. The contract reserves 2 for exactly that: a later
	// release may drop the command, and the update then has no post-install
	// step to run.
	postUpdateExitNotNeeded = 2
)

// PostUpdateOptions names the installed binary whose post-update step runs, the
// service level whose unit it rewrites and the bound on the step. A zero
// Timeout keeps postUpdateTimeout; a test shortens it instead of waiting for
// the production bound. The caller passes its own pid to the step, which needs
// it to start the detached macOS helper.
type PostUpdateOptions struct {
	Binary  string
	Level   string
	Timeout time.Duration
}

// checkLevel requires level to name one of the two service levels.
func checkLevel(level string) error {
	if level != "system" && level != "user" {
		return fmt.Errorf("service level %q is not system or user", level)
	}
	return nil
}

// Install installs o's release and then runs the new binary's hidden
// post-update command at level, before anything restarts it. The previous
// binary is kept beside the target and the main unit file is saved before the
// step runs: a failed, timed-out or canceled step restores both, so the caller
// can report the failure and leave the old version in place. Restoring the unit
// here is what covers a step killed by its bound or by a canceled context,
// because that step never reaches its own restore.
func Install(ctx context.Context, o ApplyOptions, level string) error {
	if err := checkLevel(level); err != nil {
		return err
	}
	backup, err := backupTarget(o.Target)
	if err != nil {
		return err
	}
	if err := Apply(ctx, o); err != nil {
		// Apply promises to leave the target untouched on failure, but it can
		// fail after the rename while syncing the directory. Restoring the
		// hard link covers both cases and is a no-op when the target never
		// changed, because the backup names the same inode.
		if restoreErr := restoreTarget(backup, o.Target); restoreErr != nil {
			return restoreFailure(err, restoreErr)
		}
		return err
	}
	// The step rewrites the level's main unit in the new binary's format. Save
	// its bytes first: the step may be killed before its own restore runs, and
	// a reboot would then load the next version's definition with the previous
	// binary.
	serviceOpts, err := service.PostUpdateOptions(level == "system")
	var savedUnit service.SavedUnit
	if err == nil {
		savedUnit, err = service.SaveMainUnit(serviceOpts)
	}
	if err != nil {
		return restoreAfterStepSetupFailure(
			fmt.Errorf("saving the service definition before the post-update step: %w", err), backup, o.Target)
	}
	if err := RunPostUpdate(ctx, PostUpdateOptions{Binary: o.Target, Level: level}); err != nil {
		return rollBackInstall(err, backup, o.Target, serviceOpts, savedUnit)
	}
	// The new binary and its rewritten unit are in place; a leftover rollback
	// name is untidy but harmless, and failing the update over it would report
	// a failure that did not happen.
	_ = os.Remove(backup)
	return nil
}

// restoreFailure reports cause along with why the previous binary could not be
// put back.
func restoreFailure(cause, restoreErr error) error {
	return fmt.Errorf("%w; restoring the previous binary failed: %v", cause, restoreErr)
}

// restoreAfterStepSetupFailure restores the previous binary when the unit
// guard could not be prepared. The post-update step did not run, so the unit
// file is still the one the guard would have saved.
func restoreAfterStepSetupFailure(cause error, backup, target string) error {
	if err := restoreTarget(backup, target); err != nil {
		return restoreFailure(cause, err)
	}
	return fmt.Errorf("%w; the previous binary was restored", cause)
}

// rollBackInstall undoes an install whose post-update step failed. The previous
// binary comes back and the main unit file returns to the bytes read before the
// step. The step's own restore covers a failure it survives; this restore also
// covers a step killed by its bound or by a canceled context, which never
// reaches that code. On Linux the restore reloads the unit database after
// writing the old definition; macOS's detached helper notices the rolled-back
// binary by its inode and abandons the reload on its own.
func rollBackInstall(cause error, backup, target string, opts service.Options, saved service.SavedUnit) error {
	binaryErr := restoreTarget(backup, target)
	unitErr := service.RestoreMainUnit(opts, saved)
	switch {
	case binaryErr == nil && unitErr == nil:
		return fmt.Errorf("%w; the previous binary was restored, and so was the service definition", cause)
	case binaryErr == nil:
		return fmt.Errorf("%w; the previous binary was restored, but the service definition could not be restored: %v", cause, unitErr)
	case unitErr == nil:
		return fmt.Errorf("%w; the service definition was restored", restoreFailure(cause, binaryErr))
	default:
		return fmt.Errorf("%w; the service definition could not be restored: %v", restoreFailure(cause, binaryErr), unitErr)
	}
}

// postUpdateBound returns the bound on the post-update step: the caller's
// Timeout when it set one, postUpdateTimeout otherwise.
func postUpdateBound(o PostUpdateOptions) time.Duration {
	if o.Timeout > 0 {
		return o.Timeout
	}
	return postUpdateTimeout
}

// RunPostUpdate runs binary's "internal post-update --level level", bounded by
// the configured timeout and ctx. Exit code 2 means the binary has no
// post-update command, which is not an error; every other non-zero code is a
// failure.
func RunPostUpdate(ctx context.Context, o PostUpdateOptions) error {
	if o.Binary == "" {
		return errors.New("no binary to run the post-update step")
	}
	if err := checkLevel(o.Level); err != nil {
		return err
	}

	runCtx, cancel := context.WithTimeout(ctx, postUpdateBound(o))
	defer cancel()
	var stderr bytes.Buffer
	cmd := exec.CommandContext(runCtx, o.Binary, "internal", "post-update", "--level", o.Level, "--pid", strconv.Itoa(os.Getpid()))
	// The step runs the service manager commands itself, so it inherits this
	// process's environment: a user Broker carries the session bus and
	// runtime directory those commands need. Its working directory is "/" so a
	// system service does not depend on a directory that may be removed.
	cmd.Dir = "/"
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		return nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == postUpdateExitNotNeeded {
		return nil
	}
	switch runCtx.Err() {
	case context.DeadlineExceeded:
		return fmt.Errorf("the post-update step timed out: %w", runCtx.Err())
	case context.Canceled:
		return fmt.Errorf("the post-update step was canceled: %w", runCtx.Err())
	}
	if detail := strings.TrimSpace(stderr.String()); detail != "" {
		return fmt.Errorf("the post-update step failed: %w (stderr: %s)", err, detail)
	}
	return fmt.Errorf("the post-update step failed: %w", err)
}

// backupTarget hard-links target to a fresh name beside it and returns that
// name. A hard link keeps the old inode intact without copying the binary, and
// it leaves target itself in place while the replacement is prepared.
func backupTarget(target string) (string, error) {
	f, err := os.CreateTemp(filepath.Dir(target), ".peers-rollback-*")
	if err != nil {
		return "", fmt.Errorf("reserving a rollback name next to %s: %w", target, err)
	}
	name := f.Name()
	if err := f.Close(); err != nil {
		_ = os.Remove(name)
		return "", fmt.Errorf("closing %s: %w", name, err)
	}
	if err := os.Remove(name); err != nil {
		return "", fmt.Errorf("removing the reserved rollback name %s: %w", name, err)
	}
	if err := os.Link(target, name); err != nil {
		return "", fmt.Errorf("saving a rollback copy of %s: %w", target, err)
	}
	return name, nil
}

// restoreTarget renames backup over target and flushes the directory entry, so
// the previous binary survives a crash right after a failed update.
func restoreTarget(backup, target string) error {
	if err := os.Rename(backup, target); err != nil {
		return fmt.Errorf("restoring %s from %s: %w", target, backup, err)
	}
	return syncDir(filepath.Dir(target))
}
