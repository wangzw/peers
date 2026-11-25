package service

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"
)

// ErrNotInstalled reports that opts' level has no service, as Installed judges
// it. Start, Stop and Restart return it, possibly wrapped, before running any
// service-manager command.
var ErrNotInstalled = errors.New("no service is installed at this level")

// Start starts opts' level service, waits for the Broker like Install and
// probes Status. A degraded Broker yields a complete Result and a
// *DegradedError; the service is left running.
func Start(opts Options) (Result, error) {
	res, domain, err := serviceTarget(opts, true)
	if err != nil {
		return res, err
	}
	res.Domain = domain

	started := time.Now()
	switch opts.GOOS {
	case "linux":
		if err := runSystemctlCommand(opts, &res, "start", UnitName); err != nil {
			return res, err
		}
	case "darwin":
		if launchctlDomainLoaded(opts, domain) {
			if err := runLaunchctlCommand(opts, domain, &res, "kickstart"); err != nil {
				return res, err
			}
		} else {
			if err := enableAndBootstrap(opts, domain, &res); err != nil {
				return res, err
			}
		}
	}

	deadline, err := waitReady(opts, domain, started)
	if err != nil {
		return res, err
	}
	return res, checkDegraded(opts, deadline)
}

// Stop stops opts' level service without disabling it. On macOS it waits until
// launchd has unloaded the job before returning.
func Stop(opts Options) (Result, error) {
	res, domain, err := serviceTarget(opts, false)
	if err != nil {
		return res, err
	}
	res.Domain = domain

	switch opts.GOOS {
	case "linux":
		if err := runSystemctlCommand(opts, &res, "stop", UnitName); err != nil {
			return res, err
		}
	case "darwin":
		if domain == "" {
			// A user service can only be loaded in gui/<uid>. Without that
			// domain nothing is loaded, so stopping it is already done; the
			// plist stays installed.
			return res, nil
		}
		if err := bootoutService(opts, domain, &res); err != nil {
			return res, err
		}
		budget := stopBound(opts)
		if err := waitServiceUnloaded(opts, domain, time.Now().Add(budget), budget, unloadAdvice(opts, domain, "service stop")); err != nil {
			return res, err
		}
	}

	return res, nil
}

// Restart restarts opts' level service; on macOS it re-reads the plist.
// Readiness and the degraded check are the same as Start.
func Restart(opts Options) (Result, error) {
	res, domain, err := serviceTarget(opts, true)
	if err != nil {
		return res, err
	}
	res.Domain = domain

	started := time.Now()
	switch opts.GOOS {
	case "linux":
		if err := runSystemctlCommand(opts, &res, "restart", UnitName); err != nil {
			return res, err
		}
	case "darwin":
		// Refuse a loaded job whose plist is missing before bootout, or the
		// bootstrap that follows has no file to load.
		plist, err := unitPath(opts)
		if err != nil {
			return res, err
		}
		if _, statErr := os.Lstat(rooted(opts, plist)); statErr != nil {
			if errors.Is(statErr, fs.ErrNotExist) {
				return res, fmt.Errorf("cannot restart the service: it is loaded in %s but its plist %s is missing; run `%s` to restore it before restarting", domain, plist, installHint(opts))
			}
			return res, fmt.Errorf("cannot inspect %s: %w", plist, statErr)
		}
		if err := bootoutService(opts, domain, &res); err != nil {
			return res, err
		}
		// launchctl bootout returns while the job is still being torn down, and
		// an immediate bootstrap fails with exit 5.
		budget := stopBound(opts)
		if err := waitServiceUnloaded(opts, domain, time.Now().Add(budget), budget, unloadAdvice(opts, domain, "service restart")); err != nil {
			return res, err
		}
		if err := enableAndBootstrap(opts, domain, &res); err != nil {
			return res, err
		}
	}

	deadline, err := waitReady(opts, domain, started)
	if err != nil {
		return res, err
	}
	return res, checkDegraded(opts, deadline)
}

func serviceTarget(opts Options, needsDomain bool) (Result, string, error) {
	var res Result
	if err := checkPlatform(opts, "run the service command"); err != nil {
		return res, "", err
	}

	installed, err := Installed(opts)
	if err != nil {
		return res, "", err
	}
	if !installed {
		return res, "", notInstalledError(opts)
	}
	if opts.GOOS != "darwin" {
		return res, "", nil
	}
	if opts.System {
		return res, "system", nil
	}
	domain := serviceDomain(false, opts.UID)
	if launchctlDomainExists(opts, domain) {
		return res, domain, nil
	}
	if !needsDomain {
		return res, "", nil
	}
	return res, "", fmt.Errorf("cannot start the service: launchd has no %s domain for uid %d; run this command from the user's graphical login session", domain, opts.UID)
}

func notInstalledError(opts Options) error {
	level := "user service"
	if opts.System {
		level = "system service"
	}
	return fmt.Errorf("%w (%s)", ErrNotInstalled, level)
}

func enableAndBootstrap(opts Options, domain string, res *Result) error {
	path, err := unitPath(opts)
	if err != nil {
		return err
	}
	if err := runLaunchctlCommand(opts, domain, res, "enable"); err != nil {
		return err
	}

	cmdline := "launchctl bootstrap " + domain + " " + path
	res.Commands = append(res.Commands, cmdline)
	out, err := bootstrapService(opts, domain, path)
	if err != nil {
		return commandError("cannot run "+cmdline, out, err)
	}
	return nil
}

func checkDegraded(opts Options, deadline time.Time) error {
	state, degraded, err := waitStatus(opts, deadline)
	if err != nil {
		return fmt.Errorf("cannot read the status of the running service: %w; %s", err, statusHint(opts))
	}
	if degraded {
		return &DegradedError{State: state}
	}
	return nil
}
