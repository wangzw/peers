package service

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
)

// Uninstall stops and removes the installed service, keeping configuration,
// identity and audit logs. It stops the service before removing its definition,
// so a failed stop leaves a state a later uninstall can finish. When nothing is
// installed it succeeds and reports Removed=false.
func Uninstall(opts Options) (Result, error) {
	var res Result

	if err := checkPlatform(opts, "uninstall the service"); err != nil {
		return res, err
	}
	if err := checkRoot(opts, "remove"); err != nil {
		return res, err
	}

	path, err := unitPath(opts)
	if err != nil {
		return res, err
	}
	res.UnitPath = path

	switch opts.GOOS {
	case "linux":
		return uninstallLinux(opts, path, &res)
	case "darwin":
		return uninstallDarwin(opts, path, &res)
	}
	return res, nil
}

// A reload failure restores the removed unit file, so a later uninstall can
// finish.
func uninstallLinux(opts Options, path string, res *Result) (Result, error) {
	rootedPath := rooted(opts, path)
	fileExists, err := serviceFileExists(opts, path)
	if err != nil {
		return *res, err
	}

	if _, err := opts.LookPath("systemctl"); err != nil {
		if fileExists {
			return *res, fmt.Errorf("cannot find systemctl (is systemd installed?): %w", err)
		}
		return *res, nil
	}
	installed, err := Installed(opts)
	if err != nil {
		return *res, err
	}
	if !installed {
		return *res, nil
	}

	// Stop and disable first: if this fails the unit file stays for a retry,
	// because the running process may still hold the account lock.
	if err := runSystemctlCommand(opts, res, "disable", "--now", UnitName); err != nil {
		return *res, err
	}

	var content []byte
	if fileExists {
		var err error
		content, err = os.ReadFile(rootedPath)
		if err != nil {
			return *res, fmt.Errorf("cannot read %s: %w", path, err)
		}
		if err := os.Remove(rootedPath); err != nil {
			return *res, fmt.Errorf("cannot remove %s: %w", path, err)
		}
	}
	if err := runSystemctlCommand(opts, res, "daemon-reload"); err != nil {
		if content != nil {
			if restoreErr := writeFileAtomic(rootedPath, string(content), 0o644); restoreErr != nil {
				return *res, fmt.Errorf("%w; also could not restore %s: %v", err, path, restoreErr)
			}
		}
		return *res, err
	}

	res.Removed = true
	return *res, nil
}

// A bootout failure other than "not loaded" keeps the plist for a retry.
func uninstallDarwin(opts Options, path string, res *Result) (Result, error) {
	var domain string
	if opts.System {
		domain = "system"
	} else if gui := serviceDomain(false, opts.UID); launchctlDomainExists(opts, gui) {
		domain = gui
	}
	res.Domain = domain

	fileExists, err := serviceFileExists(opts, path)
	if err != nil {
		return *res, err
	}
	if !fileExists && (domain == "" || !launchctlDomainLoaded(opts, domain)) {
		return *res, nil
	}

	if domain != "" {
		if err := bootoutService(opts, domain, res); err != nil {
			return *res, err
		}
	}
	rootedPath := rooted(opts, path)
	if err := os.Remove(rootedPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return *res, fmt.Errorf("cannot remove %s: %w", path, err)
	}

	res.Removed = true
	return *res, nil
}
