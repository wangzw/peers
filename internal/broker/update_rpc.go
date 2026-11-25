package broker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/broker/service"
	"github.com/wangzw/peers/internal/broker/update"
	"github.com/wangzw/peers/internal/config"
)

// updateShutdownGrace is how long the Broker waits after answering ApplyUpdate
// before it shuts down.
const updateShutdownGrace = 500 * time.Millisecond

// ApplyUpdate installs the newest formal release over the running binary, has
// the new binary rewrite its level's service unit, and arranges the restart
// that makes it take effect. The request names neither a version nor a source.
// The Broker refuses when no service manager runs it; only one update runs at a
// time. Access is enforced by the interceptor, not here.
func (b *Broker) ApplyUpdate(ctx context.Context, _ *localpb.ApplyUpdateRequest) (*localpb.ApplyUpdateResponse, error) {
	caller, ok := CallerFrom(ctx)
	if !ok {
		return nil, toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_INTERNAL, "caller identity is missing"))
	}

	serviceManager := b.deps.ServiceManager
	if serviceManager == nil {
		system := b.deps.System
		serviceManager = func() bool { return detectServiceManager(system) }
	}
	if !serviceManager() || b.deps.RequestShutdown == nil {
		return nil, toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_NOT_UNDER_SERVICE_MANAGER,
			"the Broker is not running under a service manager, so it cannot restart itself with the new binary; install it as a service and run the update against that service (for example, \"peers service install\" or \"sudo peers service install --system\")"))
	}

	if !b.updateBusy.CompareAndSwap(false, true) {
		return nil, toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_UPDATE_IN_PROGRESS,
			"another update is already in progress"))
	}
	// A failure releases the slot so the caller may retry. A success keeps it:
	// the process is about to exit and must not start a second download.
	installed := false
	defer func() {
		if !installed {
			b.updateBusy.Store(false)
		}
	}()

	current := b.deps.Version
	release, err := b.lookupLatestRelease(ctx)
	if err != nil {
		b.auditUpdateFailed(caller.Username, "looking up the latest release: "+err.Error())
		return nil, toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_INTERNAL,
			"looking up the latest release failed: %v", err))
	}
	if !update.Newer(current, release.Version) {
		return &localpb.ApplyUpdateResponse{FromVersion: current}, nil
	}

	target, err := installedExecutable()
	if err != nil {
		b.auditUpdateFailed(caller.Username, err.Error())
		return nil, toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_INTERNAL, "%v", err))
	}
	install := b.deps.InstallRelease
	if install == nil {
		install = update.Install
	}
	// The new binary rewrites the unit of the level this Broker runs at. The
	// step runs before the restart below; a failure rolls the binary back and
	// leaves the process running, so the caller learns why instead of losing
	// the working version.
	if err := install(ctx, update.ApplyOptions{
		Target:  target,
		Version: release.Version,
		API:     b.deps.UpdateAPI,
		Client:  b.deps.UpdateHTTPClient,
		GOOS:    runtime.GOOS,
		GOARCH:  runtime.GOARCH,
	}, serviceLevel(b.deps.System)); err != nil {
		b.auditUpdateFailed(caller.Username, "installing "+release.Version+": "+err.Error())
		return nil, toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_INTERNAL,
			"installing %s failed: %v", release.Version, err))
	}

	b.deps.Config.Audit(updateAppliedEvent(caller.Username, current, release.Version))
	installed = true
	// The response is written by gRPC once this handler returns. The grace
	// timer runs on the injected clock, so a test advances it instead of
	// waiting.
	b.deps.Clock.AfterFunc(updateShutdownGrace, b.deps.RequestShutdown)
	return &localpb.ApplyUpdateResponse{
		FromVersion: current,
		ToVersion:   release.Version,
		Restarting:  true,
	}, nil
}

func serviceLevel(system bool) string {
	if system {
		return "system"
	}
	return "user"
}

// installedExecutable resolves symbolic links so the replacement lands on the
// file the service runs.
func installedExecutable() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("locating the executable: %w", err)
	}
	real, err := filepath.EvalSymlinks(exe)
	if err != nil {
		return "", fmt.Errorf("resolving the executable %q: %w", exe, err)
	}
	return real, nil
}

// detectServiceManager reports whether the service manager of this platform
// and level started this process. Environment variables are not evidence: a
// terminal can inherit systemd's INVOCATION_ID or launchd's XPC_SERVICE_NAME.
func detectServiceManager(system bool) bool {
	return runningUnderServiceManager(runtime.GOOS, system, os.Getpid(), service.MainPID)
}

func runningUnderServiceManager(goos string, system bool, pid int, mainPID func(bool) (int, bool)) bool {
	switch goos {
	case "linux", "darwin":
		servicePID, ok := mainPID(system)
		return ok && servicePID == pid
	default:
		return false
	}
}

// updateAppliedEvent builds the audit record for an installed release; it
// carries only the versions, never a download URL.
func updateAppliedEvent(account, oldVersion, newVersion string) config.Event {
	oldValue := config.Value{Kind: config.KindString, String: oldVersion}
	newValue := config.Value{Kind: config.KindString, String: newVersion}
	return config.Event{
		Event:   config.EventUpdateApplied,
		Account: account,
		Old:     &oldValue,
		New:     &newValue,
	}
}

// auditUpdateFailed records a failed update attempt. detail is the error text,
// which may name a URL; the download itself never enters the log.
func (b *Broker) auditUpdateFailed(account, detail string) {
	b.deps.Config.Audit(config.Event{
		Event:   config.EventUpdateFailed,
		Account: account,
		Detail:  detail,
	})
}
