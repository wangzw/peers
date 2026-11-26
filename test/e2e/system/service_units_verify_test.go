//go:build e2e

package e2e

import (
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/wangzw/peers/test/e2e/syscontainer"
)

// TestSystemServiceUnitsVerify checks that the user unit installed through the
// product's subcommand is enabled and accepted by systemd without a warning.
func TestSystemServiceUnitsVerify(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	peerUID := syscontainer.AccountUID(t, "peer")
	uid := strconv.FormatUint(uint64(peerUID), 10)
	userEnv := []string{
		"XDG_RUNTIME_DIR=/run/user/" + uid,
		"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/" + uid + "/bus",
	}

	// enable and verify both ask the account's user manager, so start it and
	// wait for its bus.
	mustRun(t, "root", nil, "loginctl", "enable-linger", "peer")
	mustRun(t, "root", nil, "systemctl", "start", "user@"+uid+".service")
	waitUserManager(t, peerUID)

	install := runWithEnv(t, "peer", userEnv, releaseBinary, "service", "install")
	if install.ExitCode != 0 {
		t.Fatalf("peers service install as peer: exit %d\nstdout: %s\nstderr: %s",
			install.ExitCode, strings.TrimSpace(install.Stdout), strings.TrimSpace(install.Stderr))
	}

	res := runWithEnv(t, "peer", userEnv, "systemctl", "--user", "is-enabled", "peers.service")
	if state := strings.TrimSpace(res.Stdout); res.ExitCode != 0 || state != "enabled" {
		t.Errorf("systemctl --user is-enabled peers.service = %q (exit %d), want %q\nstderr: %s",
			state, res.ExitCode, "enabled", strings.TrimSpace(res.Stderr))
	} else {
		t.Logf("systemctl --user is-enabled peers.service = %q", state)
	}

	u, err := user.Lookup("peer")
	if err != nil {
		t.Fatalf("looking up account peer: %v", err)
	}
	unitPath := filepath.Join(u.HomeDir, ".config", "systemd", "user", "peers.service")
	res = runWithEnv(t, "peer", userEnv, "systemd-analyze", "--user", "verify", unitPath)
	if res.ExitCode != 0 || res.Stdout != "" || res.Stderr != "" {
		t.Errorf("systemd-analyze --user verify %s: exit %d\nstdout: %s\nstderr: %s",
			unitPath, res.ExitCode, res.Stdout, res.Stderr)
	} else {
		t.Logf("systemd-analyze --user verify %s produced no output", unitPath)
	}
}
