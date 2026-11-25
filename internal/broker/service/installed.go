package service

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
)

// Installed reports whether opts' level has a service: its unit file or plist
// exists, or the service manager still has it enabled or loaded. It never
// changes anything. An error means the platform is unsupported or the unit
// file or plist could not be inspected; a failing service-manager query counts
// as not enabled or loaded.
func Installed(opts Options) (bool, error) {
	if err := checkPlatform(opts, "check for an installed service"); err != nil {
		return false, err
	}

	path, err := unitPath(opts)
	if err != nil {
		return false, err
	}
	fileExists, err := serviceFileExists(opts, path)
	if err != nil {
		return false, err
	}
	if fileExists {
		return true, nil
	}

	switch opts.GOOS {
	case "linux":
		return systemctlReportsInstalled(opts), nil
	case "darwin":
		return launchctlDomainLoaded(opts, serviceDomain(opts.System, opts.UID)), nil
	}
	return false, nil
}

func serviceFileExists(opts Options, path string) (bool, error) {
	_, statErr := os.Lstat(rooted(opts, path))
	if statErr == nil {
		return true, nil
	}
	if errors.Is(statErr, fs.ErrNotExist) {
		return false, nil
	}
	return false, fmt.Errorf("cannot inspect %s: %w", path, statErr)
}

func systemctlReportsInstalled(opts Options) bool {
	for _, args := range [][]string{
		{"is-enabled", UnitName},
		{"is-active", "--quiet", UnitName},
	} {
		if _, err := opts.Run("systemctl", systemdArgs(opts, args...)...); err == nil {
			return true
		}
	}
	return false
}

func launchctlDomainLoaded(opts Options, domain string) bool {
	_, err := opts.Run("launchctl", "print", domain+"/"+Label)
	return err == nil
}
