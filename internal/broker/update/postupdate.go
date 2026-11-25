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
	// postUpdateTimeout bounds the new binary's post-update step so a hung
	// binary cannot keep the previous version from coming back.
	postUpdateTimeout = 30 * time.Second

	// postUpdateExitNotNeeded is the exit code a binary without the post-update
	// command returns; 2 is the agreed code, do not change it.
	postUpdateExitNotNeeded = 2
)

// PostUpdateOptions names the installed binary whose post-update step runs, the
// service level whose unit it rewrites and the bound on the step.
type PostUpdateOptions struct {
	Binary  string        // the installed binary to run
	Level   string        // "system" or "user"
	Timeout time.Duration // zero means postUpdateTimeout
}

func checkLevel(level string) error {
	if level != "system" && level != "user" {
		return fmt.Errorf("service level %q is not system or user", level)
	}
	return nil
}

// postUpdateOptions and saveMainUnit are variables so a test can fail either
// pre-step unit guard.
var (
	postUpdateOptions = service.PostUpdateOptions
	saveMainUnit      = service.SaveMainUnit
)

// Install installs o's release, then runs the new binary's hidden post-update
// command at level before anything restarts it. A failed, timed-out or canceled
// step restores the previous binary and the saved main unit, so the caller sees
// the failure and the old version stays in place.
func Install(ctx context.Context, o ApplyOptions, level string) error {
	if err := checkLevel(level); err != nil {
		return err
	}
	backup, err := backupTarget(o.Target)
	if err != nil {
		return err
	}
	if err := Apply(ctx, o); err != nil {
		// Apply can fail after the rename while syncing the directory, so
		// restore the hard link; it is a no-op when the target never changed,
		// because the backup names the same inode.
		if restoreErr := restoreTarget(backup, o.Target); restoreErr != nil {
			return restoreFailure(err, restoreErr)
		}
		return err
	}
	// The step rewrites the level's main unit in the new binary's format. Save
	// its bytes first: the step may be killed before its own restore runs, and
	// a reboot would then load the next version's definition with the previous
	// binary.
	serviceOpts, err := postUpdateOptions(level == "system")
	var savedUnit service.SavedUnit
	if err == nil {
		savedUnit, err = saveMainUnit(serviceOpts)
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

func restoreFailure(cause, restoreErr error) error {
	return fmt.Errorf("%w; restoring the previous binary failed: %v", cause, restoreErr)
}

func restoreAfterStepSetupFailure(cause error, backup, target string) error {
	if err := restoreTarget(backup, target); err != nil {
		return restoreFailure(cause, err)
	}
	return fmt.Errorf("%w; the previous binary was restored", cause)
}

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
	// The step inherits this process's environment so a user Broker passes the
	// session bus and runtime directory the service manager commands need, and
	// runs in "/" so a system service does not depend on a removed directory.
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

func restoreTarget(backup, target string) error {
	if err := os.Rename(backup, target); err != nil {
		return fmt.Errorf("restoring %s from %s: %w", target, backup, err)
	}
	return syncDir(filepath.Dir(target))
}
