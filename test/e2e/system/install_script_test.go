//go:build e2e

package e2e

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/syscontainer"
)

const (
	// installAccount exists only through NSS, so the script, sudo and the user
	// manager reach it.
	installAccount = "diruser"
	installUID     = uint32(2100)
	installHome    = "/home/diruser"

	installUserPrefix   = installHome + "/.local/bin/peers"
	installSystemPrefix = "/usr/local/bin/peers"

	installArchiveDir = "/var/lib/peers-install-archive"

	installClaudePath = "/usr/local/bin/claude"

	// installStateDir is world-writable because the script runs claude as the
	// ordinary account in both scenarios.
	installStateDir = "/var/lib/peers-install-state"

	// installSudoersPath has no dot, which sudo requires of a file in
	// /etc/sudoers.d.
	installSudoersPath = "/etc/sudoers.d/peers-install-script"

	// installPath includes /usr/local/bin, which syscontainer.Command's default
	// account PATH omits.
	installPath = "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"

	// installServiceStartBound bounds the wait for the Broker that the freshly
	// installed service starts; service install returns after starting it, so the
	// wait only covers systemd dispatching the unit.
	installServiceStartBound = 10 * time.Second

	installStagedDir = "install-script-e2e"

	// installExpectedVersion is the version harness.BuildBinary stamps into the
	// image's binary.
	installExpectedVersion = "peers e2e"

	// These paths are the state a refused install must leave untouched.
	installUserConfigDir   = installHome + "/.peers"
	installSystemConfigDir = "/etc/peers"
	installUserUnitPath    = installHome + "/.config/systemd/user/peers.service"
	installSystemUnitPath  = "/etc/systemd/system/peers.service"

	installUserBashCompletion   = installHome + "/.local/share/bash-completion/completions/peers"
	installUserFishCompletion   = installHome + "/.config/fish/completions/peers.fish"
	installSystemBashCompletion = "/usr/local/share/bash-completion/completions/peers"
	installSystemZshCompletion  = "/usr/local/share/zsh/site-functions/_peers"
	installSystemFishCompletion = "/usr/local/share/fish/vendor_completions.d/peers.fish"
	installSystemManPage        = "/usr/local/share/man/man1/peers.1"
	installSystemUpdateManPage  = "/usr/local/share/man/man1/peers-update.1"

	installLevelPrompt   = "Install for every account on this host? [y/N]"
	installUpgradePrompt = "Upgrade the system installation? [Y/n]"
)

const (
	installManPage       = ".TH PEERS 1\n.SH NAME\npeers - test stub\n"
	installUpdateManPage = ".TH PEERS-UPDATE 1\n.SH NAME\npeers-update - test stub\n"
)

const installArchiveReadme = "peers release archive for the install-script end-to-end test.\n"

// installFakeClaudeScript records each call and answers claude mcp get peer
// from a marker an earlier mcp add wrote.
const installFakeClaudeScript = `#!/bin/sh
state=/var/lib/peers-install-state
log="$state/invocations"
registered="$state/registered-peer"
mkdir -p "$state" 2>/dev/null || true
printf '%s\t%s\t%s\t%s\t%s\n' "$(id -u)" "${USER:-}" "${HOME:-}" "${SUDO_USER:-}" "$*" >> "$log" 2>/dev/null || true

if [ "$1" = "mcp" ] && [ "$2" = "get" ] && [ "$3" = "peer" ]; then
	if [ -f "$registered" ]; then
		printf 'peer:\n  Scope: User\n  Command: %s mcp\n' "$(cat "$registered")"
		exit 0
	fi
	printf 'No MCP server found with name: peer\n' >&2
	exit 1
fi

if [ "$1" = "mcp" ] && [ "$2" = "add" ]; then
	previous=
	path=
	for argument in "$@"; do
		if [ "$previous" = "--" ]; then
			path=$argument
		fi
		previous=$argument
	done
	if [ -n "$path" ]; then
		printf '%s\n' "$path" > "$registered"
	fi
	exit 0
fi

exit 0
`

// TestREQBrokerConfig041_UserInstallScript checks that a user install places the
// binary at the user default path, starts the user service, registers the MCP
// server once, and stays idempotent on a second run.
func TestREQBrokerConfig041_UserInstallScript(t *testing.T) {
	if !syscontainer.InContainer() {
		stageInstallArchive(t)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	assertInstallImageHasNoFish(t)
	installFakeClaude(t)
	archive := prepareInstallArchive(t)
	startInstallUserManager(t)

	t.Run("REQ-broker-config-041 user install", func(t *testing.T) {
		res := runInstallScriptAs(t, installAccount, installUserEnv(), archive, "sh", "./install.sh")
		assertUserInstall(t)
		assertUserInstallCompletions(t, res.Stdout+res.Stderr)
		assertInstallMCPAdd(t, 1, installUserPrefix, installUID, installHome)
		assertInstallMCPGet(t, 1, installUID, installHome)
	})

	t.Run("REQ-broker-config-041 second user install is idempotent", func(t *testing.T) {
		runInstallScriptAs(t, installAccount, installUserEnv(), archive, "sh", "./install.sh")
		assertUserInstall(t)
		assertUserInstallCompletions(t, "")
		assertInstallMCPAdd(t, 1, installUserPrefix, installUID, installHome)
		assertInstallMCPGet(t, 2, installUID, installHome)
	})
}

// TestREQBrokerConfig041_SystemInstallScript checks that a system install through
// sudo runs the system service, leaves a root-owned binary, registers the MCP
// server once for the account sudo ran as, and stays idempotent on a second run.
func TestREQBrokerConfig041_SystemInstallScript(t *testing.T) {
	if !syscontainer.InContainer() {
		stageInstallArchive(t)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	assertInstallImageHasNoFish(t)
	installFakeClaude(t)
	archive := prepareInstallArchive(t)
	removePreinstalledSystemBinary(t)
	configureInstallSudo(t)

	t.Run("REQ-broker-config-041 root user install is refused", func(t *testing.T) {
		assertInstallScriptRefusesRoot(t, archive)
	})

	t.Run("REQ-broker-config-041 system install", func(t *testing.T) {
		runSystemInstallScript(t, archive)
		assertSystemInstall(t)
		assertSystemInstallCompletionsAndMan(t)
		assertInstallMCPAdd(t, 1, installSystemPrefix, installUID, installHome)
		assertInstallMCPGet(t, 1, installUID, installHome)
	})

	t.Run("REQ-broker-config-041 second system install is idempotent", func(t *testing.T) {
		runSystemInstallScript(t, archive)
		assertSystemInstall(t)
		assertSystemInstallCompletionsAndMan(t)
		assertInstallMCPAdd(t, 1, installSystemPrefix, installUID, installHome)
		assertInstallMCPGet(t, 2, installUID, installHome)
	})
}

// TestREQBrokerConfig041_SystemInstallSkipsForeignCompletionDirectory checks that
// a system install leaves a completion directory owned by another account alone,
// warns with the manual command, and still succeeds.
func TestREQBrokerConfig041_SystemInstallSkipsForeignCompletionDirectory(t *testing.T) {
	if !syscontainer.InContainer() {
		stageInstallArchive(t)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	assertInstallImageHasNoFish(t)
	installFakeClaude(t)
	archive := prepareInstallArchive(t)
	removePreinstalledSystemBinary(t)
	configureInstallSudo(t)

	// 65534 is neither root nor the account that ran sudo, so the install must
	// leave this directory alone.
	zshDir := filepath.Dir(installSystemZshCompletion)
	mustRun(t, "root", nil, "mkdir", "-p", zshDir)
	mustRun(t, "root", nil, "chown", "65534:65534", zshDir)
	mustRun(t, "root", nil, "chmod", "0755", zshDir)

	res := runInstallScriptAs(t, installAccount, installUserEnv(), archive, "sudo", "-n", "sh", "./install.sh", "--system")
	if res.ExitCode != 0 {
		t.Fatalf("the install failed although only the zsh completion should be skipped: exit %d\nstdout: %s\nstderr: %s",
			res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
	}

	output := res.Stdout + res.Stderr
	for _, want := range []string{"skipped the zsh completion", "is not owned by root", zshDir, "install it with:"} {
		if !strings.Contains(output, want) {
			t.Errorf("output does not contain %q:\n%s", want, output)
		}
	}
	if _, err := os.Stat(installSystemZshCompletion); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the zsh completion %s was written into a directory owned by another account: %v", installSystemZshCompletion, err)
	}
	for _, path := range []string{installSystemBashCompletion, installSystemManPage, installSystemUpdateManPage} {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("the install skipped %s as well: %v", path, err)
		}
	}
}

// TestREQBrokerConfig041_SystemInstallWritesIntoTheSudoUsersDirectory checks that a
// system install into a completion directory owned by the sudo user writes the
// file as that account, not as root.
func TestREQBrokerConfig041_SystemInstallWritesIntoTheSudoUsersDirectory(t *testing.T) {
	if !syscontainer.InContainer() {
		stageInstallArchive(t)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	assertInstallImageHasNoFish(t)
	installFakeClaude(t)
	archive := prepareInstallArchive(t)
	removePreinstalledSystemBinary(t)
	configureInstallSudo(t)

	for _, dir := range []string{
		filepath.Dir(installSystemZshCompletion),
		filepath.Dir(installSystemManPage),
	} {
		mustRun(t, "root", nil, "mkdir", "-p", dir)
		mustRun(t, "root", nil, "chown", "2100:2100", dir)
		mustRun(t, "root", nil, "chmod", "0755", dir)
	}

	res := runInstallScriptAs(t, installAccount, installUserEnv(), archive, "sudo", "-n", "sh", "./install.sh", "--system")
	if res.ExitCode != 0 {
		t.Fatalf("the install failed: exit %d\nstdout: %s\nstderr: %s",
			res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
	}
	for _, path := range []string{installSystemZshCompletion, installSystemManPage} {
		uid, gid, _ := fileOwnerAndMode(t, path)
		if uid != installUID || gid != installUID {
			t.Errorf("%s owner = %d:%d, want %d:%d; root created it instead of %s",
				path, uid, gid, installUID, installUID, installAccount)
		}
	}
}

// TestREQBrokerConfig041_InstallScriptFilesRemovedByUninstall checks that
// uninstall removes both levels' completion files and the system man pages.
func TestREQBrokerConfig041_InstallScriptFilesRemovedByUninstall(t *testing.T) {
	if !syscontainer.InContainer() {
		stageInstallArchive(t)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	assertInstallImageHasNoFish(t)
	installFakeClaude(t)
	archive := prepareInstallArchive(t)
	startInstallUserManager(t)

	t.Run("REQ-broker-config-041 user files are removed", func(t *testing.T) {
		res := runInstallScriptAs(t, installAccount, installUserEnv(), archive, "sh", "./install.sh", "--user")
		if res.ExitCode != 0 {
			t.Fatalf("user install: exit %d\nstdout:\n%s\nstderr:\n%s",
				res.ExitCode, res.Stdout, res.Stderr)
		}
		assertUserInstallCompletions(t, res.Stdout+res.Stderr)

		env := append(installUserEnv(), "PATH="+installHome+"/.local/bin:"+installPath)
		res = runInstallScriptAs(t, installAccount, env, "/", installUserPrefix, "uninstall", "--yes")
		if res.ExitCode != 0 {
			t.Fatalf("user uninstall: exit %d\nstdout:\n%s\nstderr:\n%s",
				res.ExitCode, res.Stdout, res.Stderr)
		}
		assertUserInstallCompletionsRemoved(t)
	})

	t.Run("REQ-broker-config-041 system files are removed", func(t *testing.T) {
		removePreinstalledSystemBinary(t)
		configureInstallSudo(t)
		runSystemInstallScript(t, archive)
		assertSystemInstallCompletionsAndMan(t)

		res := runInstallScriptAs(t, "root", nil, "/", installSystemPrefix, "uninstall", "--purge", "--yes")
		if res.ExitCode != 0 {
			t.Fatalf("system uninstall: exit %d\nstdout:\n%s\nstderr:\n%s",
				res.ExitCode, res.Stdout, res.Stderr)
		}
		assertSystemInstallCompletionsAndManRemoved(t)
	})
}

// TestREQBrokerConfig044_InstallScriptLevelPromptAcceptsSystemLevel checks that
// answering the level question with y gives the ordinary account a system install
// with its MCP registration.
func TestREQBrokerConfig044_InstallScriptLevelPromptAcceptsSystemLevel(t *testing.T) {
	if !syscontainer.InContainer() {
		stageInstallArchive(t)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	installFakeClaude(t)
	archive := prepareInstallArchive(t)
	removePreinstalledSystemBinary(t)
	configureInstallSudo(t)

	t.Run("REQ-broker-config-044 y installs for every account", func(t *testing.T) {
		res := runInstallScriptWithTerminal(t, installAccount, installUserEnv(), archive, "y\n", "sh", "./install.sh")
		if res.ExitCode != 0 {
			t.Fatalf("install with y on the terminal: exit %d\n%s", res.ExitCode, res.Output)
		}
		if !strings.Contains(res.Output, installLevelPrompt) {
			t.Errorf("the terminal did not show %q:\n%s", installLevelPrompt, res.Output)
		}
		assertSystemInstall(t)
		assertInstallMCPAdd(t, 1, installSystemPrefix, installUID, installHome)

		again := runInstallScriptWithTerminal(t, installAccount, installUserEnv(), archive, "y\n", "sh", "./install.sh")
		if again.ExitCode != 0 {
			t.Fatalf("second install with y on the terminal: exit %d\n%s", again.ExitCode, again.Output)
		}
		if !strings.Contains(again.Output, installUpgradePrompt) {
			t.Errorf("the terminal did not show %q:\n%s", installUpgradePrompt, again.Output)
		}
		assertSystemInstall(t)
		assertInstallMCPAdd(t, 1, installSystemPrefix, installUID, installHome)
		assertInstallMCPGet(t, 2, installUID, installHome)
	})
}

// TestREQBrokerConfig044_InstallScriptLevelPromptDefaultsToUser checks that pressing
// Enter at the level question gives the account a user install.
func TestREQBrokerConfig044_InstallScriptLevelPromptDefaultsToUser(t *testing.T) {
	if !syscontainer.InContainer() {
		stageInstallArchive(t)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	installFakeClaude(t)
	archive := prepareInstallArchive(t)
	startInstallUserManager(t)

	t.Run("REQ-broker-config-044 enter installs for this account", func(t *testing.T) {
		res := runInstallScriptWithTerminal(t, installAccount, installUserEnv(), archive, "\n", "sh", "./install.sh")
		if res.ExitCode != 0 {
			t.Fatalf("install with Enter on the terminal: exit %d\n%s", res.ExitCode, res.Output)
		}
		if !strings.Contains(res.Output, installLevelPrompt) {
			t.Errorf("the terminal did not show %q:\n%s", installLevelPrompt, res.Output)
		}
		assertUserInstall(t)
		assertInstallMCPAdd(t, 1, installUserPrefix, installUID, installHome)
		if _, err := os.Stat(installSystemUnitPath); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("the user install left the system unit %s behind: %v", installSystemUnitPath, err)
		}
		if procs := syscontainer.Pids("peers", "service", "run", "--system"); len(procs) != 0 {
			t.Errorf("the user install started system Brokers: %v", procs)
		}
	})
}

// TestREQBrokerConfig045_InstallScriptRootDoesNotAsk checks that the script run as
// root installs for every account without a level question.
func TestREQBrokerConfig045_InstallScriptRootDoesNotAsk(t *testing.T) {
	if !syscontainer.InContainer() {
		stageInstallArchive(t)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	installFakeClaude(t)
	archive := prepareInstallArchive(t)
	removePreinstalledSystemBinary(t)

	t.Run("REQ-broker-config-045 root installs for every account without asking", func(t *testing.T) {
		res := runInstallScriptAs(t, "root", nil, archive, "sh", "./install.sh")
		if res.ExitCode != 0 {
			t.Fatalf("root install: exit %d\nstdout:\n%s\nstderr:\n%s",
				res.ExitCode, res.Stdout, res.Stderr)
		}
		combined := res.Stdout + res.Stderr
		for _, prompt := range []string{installLevelPrompt, installUpgradePrompt} {
			if strings.Contains(combined, prompt) {
				t.Errorf("the root install asked %q:\n%s", prompt, combined)
			}
		}
		assertSystemInstall(t)
		if _, err := os.Stat(installUserUnitPath); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("the root install left a user unit at %s: %v", installUserUnitPath, err)
		}
	})
}

// TestREQBrokerConfig046_InstallScriptWithoutTerminal checks that the script without
// a controlling terminal installs for this account without waiting for input.
func TestREQBrokerConfig046_InstallScriptWithoutTerminal(t *testing.T) {
	if !syscontainer.InContainer() {
		stageInstallArchive(t)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	installFakeClaude(t)
	archive := prepareInstallArchive(t)
	startInstallUserManager(t)

	t.Run("REQ-broker-config-046 no terminal installs for this account", func(t *testing.T) {
		// The command starts in a new session with stdin from /dev/null, so it
		// sees no controlling terminal.
		res := runInstallScriptAs(t, installAccount, installUserEnv(), archive, "sh", "./install.sh")
		if res.ExitCode != 0 {
			t.Fatalf("no-terminal install: exit %d\nstdout:\n%s\nstderr:\n%s",
				res.ExitCode, res.Stdout, res.Stderr)
		}
		combined := res.Stdout + res.Stderr
		if strings.Contains(combined, installLevelPrompt) {
			t.Errorf("the no-terminal install asked the level question:\n%s", combined)
		}
		assertUserInstall(t)
		if _, err := os.Stat(installSystemUnitPath); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("the no-terminal install left the system unit %s behind: %v", installSystemUnitPath, err)
		}
	})
}

// TestREQBrokerConfig047_InstallScriptUpgradePrompt checks that Enter upgrades the
// system install and keeps its configuration while n leaves everything unchanged.
func TestREQBrokerConfig047_InstallScriptUpgradePrompt(t *testing.T) {
	if !syscontainer.InContainer() {
		stageInstallArchive(t)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	installFakeClaude(t)
	archive := prepareInstallArchive(t)
	removePreinstalledSystemBinary(t)
	configureInstallSudo(t)
	runSystemInstallScript(t, archive)
	assertSystemInstall(t)

	configPath := filepath.Join(installSystemConfigDir, "config.yaml")
	configBefore := readFile(t, configPath)

	t.Run("REQ-broker-config-047 enter upgrades the system install", func(t *testing.T) {
		res := runInstallScriptWithTerminal(t, installAccount, installUserEnv(), archive, "\n", "sh", "./install.sh")
		if res.ExitCode != 0 {
			t.Fatalf("upgrade with Enter on the terminal: exit %d\n%s", res.ExitCode, res.Output)
		}
		if !strings.Contains(res.Output, installUpgradePrompt) {
			t.Errorf("the terminal did not show %q:\n%s", installUpgradePrompt, res.Output)
		}
		if strings.Contains(res.Output, installLevelPrompt) {
			t.Errorf("the terminal showed the install question instead of the upgrade question:\n%s", res.Output)
		}
		// Both binaries are the same build, so the script reports a reinstall
		// rather than a replacement.
		reinstalled := "Reinstalled " + installExpectedVersion + "."
		if !strings.Contains(res.Output, reinstalled) {
			t.Errorf("the upgrade run did not report the reinstalled version %q:\n%s", reinstalled, res.Output)
		}
		assertSystemInstall(t)
		if got := readFile(t, configPath); got != configBefore {
			t.Errorf("the upgrade changed the configuration:\n got: %q\nwant: %q", got, configBefore)
		}
		assertInstallMCPAdd(t, 1, installSystemPrefix, installUID, installHome)
	})

	t.Run("REQ-broker-config-047 n leaves the installation unchanged", func(t *testing.T) {
		beforeHash := sha256.Sum256([]byte(readFile(t, installSystemPrefix)))
		res := runInstallScriptWithTerminal(t, installAccount, installUserEnv(), archive, "n\n", "sh", "./install.sh")
		if res.ExitCode != 0 {
			t.Fatalf("declining the upgrade: exit %d\n%s", res.ExitCode, res.Output)
		}
		if !strings.Contains(res.Output, installUpgradePrompt) {
			t.Errorf("the terminal did not show %q:\n%s", installUpgradePrompt, res.Output)
		}
		if !strings.Contains(res.Output, "install.sh: the system installation was left unchanged.") {
			t.Errorf("declining the upgrade did not say the system installation was unchanged:\n%s", res.Output)
		}
		if strings.Contains(res.Output, "--user") {
			t.Errorf("declining the upgrade suggested --user, which would bind the same default port:\n%s", res.Output)
		}
		if got := readFile(t, configPath); got != configBefore {
			t.Errorf("declining the upgrade changed the configuration:\n got: %q\nwant: %q", got, configBefore)
		}
		if afterHash := sha256.Sum256([]byte(readFile(t, installSystemPrefix))); afterHash != beforeHash {
			t.Errorf("declining the upgrade replaced the system binary")
		}
		waitInstallUnitState(t, "root", nil, "active")
	})
}

// TestREQBrokerConfig048_InstallScriptBothFlags checks that --system and --user
// together are a usage error that changes nothing.
func TestREQBrokerConfig048_InstallScriptBothFlags(t *testing.T) {
	if !syscontainer.InContainer() {
		stageInstallArchive(t)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	archive := prepareInstallArchive(t)
	removePreinstalledSystemBinary(t)

	t.Run("REQ-broker-config-048 both flags are a usage error", func(t *testing.T) {
		res := runInstallScriptAs(t, installAccount, installUserEnv(), archive,
			"sh", "./install.sh", "--system", "--user")
		if res.ExitCode != 2 {
			t.Fatalf("--system --user: exit %d, want 2\nstdout:\n%s\nstderr:\n%s",
				res.ExitCode, res.Stdout, res.Stderr)
		}
		if !strings.Contains(res.Stdout+res.Stderr, "--system and --user cannot be used together") {
			t.Errorf("the usage error does not name the conflict:\n%s", res.Stdout+res.Stderr)
		}
		for _, path := range []string{installUserPrefix, installSystemPrefix, installUserUnitPath, installSystemUnitPath} {
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("the usage error left %s behind: %v", path, err)
			}
		}
	})
}

// TestREQBrokerConfig048_InstallScriptUserFlag checks that --user skips the level
// question and installs for the account.
func TestREQBrokerConfig048_InstallScriptUserFlag(t *testing.T) {
	if !syscontainer.InContainer() {
		stageInstallArchive(t)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	installFakeClaude(t)
	archive := prepareInstallArchive(t)
	startInstallUserManager(t)

	t.Run("REQ-broker-config-048 --user skips the question", func(t *testing.T) {
		res := runInstallScriptAs(t, installAccount, installUserEnv(), archive,
			"sh", "./install.sh", "--user")
		if res.ExitCode != 0 {
			t.Fatalf("--user: exit %d\nstdout:\n%s\nstderr:\n%s",
				res.ExitCode, res.Stdout, res.Stderr)
		}
		if combined := res.Stdout + res.Stderr; strings.Contains(combined, installLevelPrompt) {
			t.Errorf("--user asked the level question:\n%s", combined)
		}
		assertUserInstall(t)
		assertInstallMCPAdd(t, 1, installUserPrefix, installUID, installHome)
	})
}

// TestREQBrokerConfig048_InstallScriptSystemFlag checks that --system skips the
// level question and installs for every account.
func TestREQBrokerConfig048_InstallScriptSystemFlag(t *testing.T) {
	if !syscontainer.InContainer() {
		stageInstallArchive(t)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	installFakeClaude(t)
	archive := prepareInstallArchive(t)
	removePreinstalledSystemBinary(t)
	configureInstallSudo(t)

	t.Run("REQ-broker-config-048 --system skips the question", func(t *testing.T) {
		res := runInstallScriptAs(t, installAccount, installUserEnv(), archive,
			"sh", "./install.sh", "--system")
		if res.ExitCode != 0 {
			t.Fatalf("--system: exit %d\nstdout:\n%s\nstderr:\n%s",
				res.ExitCode, res.Stdout, res.Stderr)
		}
		if combined := res.Stdout + res.Stderr; strings.Contains(combined, installLevelPrompt) {
			t.Errorf("--system asked the level question:\n%s", combined)
		}
		assertSystemInstall(t)
		assertInstallMCPAdd(t, 1, installSystemPrefix, installUID, installHome)
	})
}

// TestREQBrokerConfig050_InstallScriptUserSummary checks the summary a user install
// prints and that the commands it prints run.
func TestREQBrokerConfig050_InstallScriptUserSummary(t *testing.T) {
	if !syscontainer.InContainer() {
		stageInstallArchive(t)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	installFakeClaude(t)
	archive := prepareInstallArchive(t)
	startInstallUserManager(t)

	t.Run("REQ-broker-config-050 user summary", func(t *testing.T) {
		res := runInstallScriptAs(t, installAccount, installUserEnv(), archive,
			"sh", "./install.sh", "--user")
		if res.ExitCode != 0 {
			t.Fatalf("user install: exit %d\nstdout:\n%s\nstderr:\n%s",
				res.ExitCode, res.Stdout, res.Stderr)
		}
		assertInstallScriptSummary(t, res.Stdout, "user service", "login", map[string]string{
			"binary:":    installUserPrefix,
			"config:":    installHome + "/.peers/config.yaml (generated)",
			"run log:":   "journalctl --user -u peers.service",
			"audit log:": installHome + "/.peers/audit.log",
			"start:":     "peers service start",
			"stop:":      "peers service stop",
			"restart:":   "peers service restart",
			"uninstall:": "peers uninstall",
		})
		assertInstallScriptCommandsRun(t, res.Stdout, installAccount,
			append(installUserEnv(), "PATH="+installHome+"/.local/bin:"+installPath), installHome)
	})
}

// TestREQBrokerConfig050_InstallScriptSystemSummary checks the same summary for a
// system install.
func TestREQBrokerConfig050_InstallScriptSystemSummary(t *testing.T) {
	if !syscontainer.InContainer() {
		stageInstallArchive(t)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	installFakeClaude(t)
	archive := prepareInstallArchive(t)
	removePreinstalledSystemBinary(t)
	configureInstallSudo(t)

	t.Run("REQ-broker-config-050 system summary", func(t *testing.T) {
		res := runInstallScriptAs(t, installAccount, installUserEnv(), archive,
			"sh", "./install.sh", "--system")
		if res.ExitCode != 0 {
			t.Fatalf("system install: exit %d\nstdout:\n%s\nstderr:\n%s",
				res.ExitCode, res.Stdout, res.Stderr)
		}
		assertInstallScriptSummary(t, res.Stdout, "system service", "boot", map[string]string{
			"binary:":    installSystemPrefix,
			"config:":    installSystemConfigDir + "/config.yaml (generated)",
			"run log:":   "journalctl -u peers.service",
			"audit log:": systemAuditPath,
			"start:":     "sudo peers service start",
			"stop:":      "sudo peers service stop",
			"restart:":   "sudo peers service restart",
			"uninstall:": "peers uninstall",
		})
		assertInstallScriptCommandsRun(t, res.Stdout, "root", nil, "/")
	})
}

// assertInstallScriptSummary checks the summary the install script printed and that
// every path it names exists.
func assertInstallScriptSummary(t *testing.T, output, level, start string, wants map[string]string) {
	t.Helper()
	assertInstallSummary(t, output, level, start, false, wants)
	for _, label := range []string{"binary:", "config:", "audit log:"} {
		path := strings.Fields(summaryValue(t, output, label))[0]
		if _, err := os.Stat(path); err != nil {
			t.Errorf("the summary prints %s, but it does not exist: %v", path, err)
		}
	}
}

// assertInstallScriptCommandsRun runs the start, stop, restart and uninstall
// commands as the install script printed them.
func assertInstallScriptCommandsRun(t *testing.T, output, account string, env []string, dir string) {
	t.Helper()
	for _, label := range []string{"start:", "stop:", "restart:", "uninstall:"} {
		args := printedCommandArgs(t, summaryValue(t, output, label))
		// The summary prints "peers uninstall" for a person at a terminal; the
		// test passes --yes to skip the data question.
		if label == "uninstall:" {
			args = append(args, "--yes")
		}
		res := runInstallScriptAs(t, account, env, dir, "peers", args...)
		if res.ExitCode != 0 {
			t.Errorf("peers %s from the summary: exit %d\nstdout:\n%s\nstderr:\n%s",
				strings.Join(args, " "), res.ExitCode, res.Stdout, res.Stderr)
		}
	}
}

type installClaudeCall struct {
	UID      uint32
	User     string
	Home     string
	SudoUser string
	Args     string
}

func stageInstallArchive(t *testing.T) {
	t.Helper()

	rel := stagedArchiveRel(t)
	for _, file := range []struct {
		name   string
		source string
		mode   os.FileMode
	}{
		{"install.sh", "scripts/install.sh", 0o755},
		{"LICENSE", "LICENSE", 0o644},
	} {
		data := readRepositoryFile(t, filepath.Join("..", "..", "..", file.source))
		syscontainer.StageArtifact(t, filepath.Join(rel, file.name), data, file.mode)
	}
	syscontainer.StageArtifact(t, filepath.Join(rel, "README.md"), []byte(installArchiveReadme), 0o644)
	syscontainer.StageArtifact(t, filepath.Join(rel, "man", "peers.1"), []byte(installManPage), 0o644)
	syscontainer.StageArtifact(t, filepath.Join(rel, "man", "peers-update.1"), []byte(installUpdateManPage), 0o644)
}

func readRepositoryFile(t *testing.T, rel string) []byte {
	t.Helper()

	path, err := filepath.Abs(rel)
	if err != nil {
		t.Fatalf("resolving %s: %v", rel, err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s for the release archive: %v", path, err)
	}
	return data
}

func stagedArchiveRel(t *testing.T) string {
	t.Helper()

	var b strings.Builder
	for _, r := range strings.ToLower(t.Name()) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	return filepath.Join(installStagedDir, b.String())
}

func installUserEnv() []string {
	uid := strconv.FormatUint(uint64(installUID), 10)
	return []string{
		"PATH=" + installPath,
		"XDG_RUNTIME_DIR=/run/user/" + uid,
		"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/" + uid + "/bus",
	}
}

func prepareInstallArchive(t *testing.T) string {
	t.Helper()

	if err := os.RemoveAll(installArchiveDir); err != nil {
		t.Fatalf("clearing the archive directory %s: %v", installArchiveDir, err)
	}
	if err := os.MkdirAll(installArchiveDir, 0o755); err != nil {
		t.Fatalf("creating the archive directory %s: %v", installArchiveDir, err)
	}

	staged := syscontainer.ArtifactPath(stagedArchiveRel(t))
	files := []struct {
		source string
		name   string
		mode   os.FileMode
	}{
		{installSystemPrefix, "peers", 0o755},
		{filepath.Join(staged, "install.sh"), "install.sh", 0o755},
		{filepath.Join(staged, "LICENSE"), "LICENSE", 0o644},
		{filepath.Join(staged, "README.md"), "README.md", 0o644},
		{filepath.Join(staged, "man", "peers.1"), filepath.Join("man", "peers.1"), 0o644},
		{filepath.Join(staged, "man", "peers-update.1"), filepath.Join("man", "peers-update.1"), 0o644},
	}
	for _, file := range files {
		if err := copyInstallFile(file.source, filepath.Join(installArchiveDir, file.name), file.mode); err != nil {
			t.Fatalf("building the release archive: %v", err)
		}
	}
	return installArchiveDir
}

func copyInstallFile(src, dst string, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("creating %s: %w", filepath.Dir(dst), err)
	}
	data, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("reading %s: %w", src, err)
	}
	if err := os.WriteFile(dst, data, mode); err != nil {
		return fmt.Errorf("writing %s: %w", dst, err)
	}
	if err := os.Chmod(dst, mode); err != nil {
		return fmt.Errorf("chmod %s: %w", dst, err)
	}
	return nil
}

func installFakeClaude(t *testing.T) {
	t.Helper()

	if err := os.RemoveAll(installStateDir); err != nil {
		t.Fatalf("clearing the fake claude's state directory %s: %v", installStateDir, err)
	}
	if err := os.MkdirAll(installStateDir, 0o777); err != nil {
		t.Fatalf("creating the fake claude's state directory %s: %v", installStateDir, err)
	}
	if err := os.Chmod(installStateDir, 0o777); err != nil {
		t.Fatalf("chmod %s: %v", installStateDir, err)
	}
	if err := os.WriteFile(installClaudePath, []byte(installFakeClaudeScript), 0o755); err != nil {
		t.Fatalf("writing the fake claude %s: %v", installClaudePath, err)
	}
	if err := os.Chmod(installClaudePath, 0o755); err != nil {
		t.Fatalf("chmod %s: %v", installClaudePath, err)
	}
}

func removePreinstalledSystemBinary(t *testing.T) {
	t.Helper()

	if err := os.Remove(installSystemPrefix); err != nil {
		t.Fatalf("removing the image's built-in %s: %v", installSystemPrefix, err)
	}
	if _, err := os.Stat(installSystemPrefix); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("%s still exists after its removal: %v", installSystemPrefix, err)
	}
}

func configureInstallSudo(t *testing.T) {
	t.Helper()

	entry := "Defaults !use_pty\n" + installAccount + " ALL=(ALL) NOPASSWD: ALL\n"
	if err := os.WriteFile(installSudoersPath, []byte(entry), 0o440); err != nil {
		t.Fatalf("writing %s: %v", installSudoersPath, err)
	}
	if err := os.Chmod(installSudoersPath, 0o440); err != nil {
		t.Fatalf("chmod %s: %v", installSudoersPath, err)
	}
	res := runWithEnv(t, "root", nil, "visudo", "-cf", installSudoersPath)
	if res.ExitCode != 0 {
		t.Fatalf("visudo -cf %s: exit %d\nstdout: %s\nstderr: %s",
			installSudoersPath, res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
	}
}

func startInstallUserManager(t *testing.T) {
	t.Helper()

	uid := strconv.FormatUint(uint64(installUID), 10)
	mustRun(t, "root", nil, "loginctl", "enable-linger", installAccount)
	mustRun(t, "root", nil, "systemctl", "start", "user@"+uid+".service")
	waitUserManager(t, installUID)
}

func assertInstallScriptRefusesRoot(t *testing.T, archive string) {
	t.Helper()

	res := runInstallScriptAs(t, installAccount, installUserEnv(), archive, "sudo", "-n", "sh", "./install.sh", "--user")
	if res.ExitCode == 0 {
		t.Fatalf("sudo sh ./install.sh --user as %s exited 0, want a refusal\nstdout: %s\nstderr: %s",
			installAccount, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
	}
	combined := res.Stdout + res.Stderr
	for _, want := range []string{"must not run as root", "drop --user"} {
		if !strings.Contains(combined, want) {
			t.Errorf("output = %q, want it to contain %q", combined, want)
		}
	}

	for _, path := range []string{
		installUserPrefix,
		installSystemPrefix,
		installUserUnitPath,
		installSystemUnitPath,
		installUserConfigDir,
		installSystemConfigDir,
	} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("a refused install left %s behind: %v", path, err)
		}
	}

	invocations := filepath.Join(installStateDir, "invocations")
	data, err := os.ReadFile(invocations)
	if err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.Contains(line, "\tmcp add") {
				t.Errorf("a refused install registered the MCP server: %q", line)
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reading the fake claude's invocation log %s: %v", invocations, err)
	}
}

func runSystemInstallScript(t *testing.T, archive string) {
	t.Helper()

	res := runInstallScriptAs(t, installAccount, installUserEnv(), archive, "sudo", "-n", "sh", "./install.sh", "--system")
	if res.ExitCode != 0 {
		t.Fatalf("sudo sh ./install.sh --system as %s: exit %d\nstdout: %s\nstderr: %s",
			installAccount, res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
	}
}

func runInstallScriptAs(t *testing.T, account string, env []string, dir, name string, args ...string) syscontainer.Result {
	t.Helper()

	cmd := syscontainer.Command(account, env, name, args...)
	cmd.Dir = dir
	// A new session has no controlling terminal, so a run without --system or
	// --user exercises the no-terminal level choice.
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setsid = true
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	res := syscontainer.Result{Stdout: stdout.String(), Stderr: stderr.String()}
	if err == nil {
		return res
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("running %s %s as %s: %v", name, strings.Join(args, " "), account, err)
	}
	res.ExitCode = exitErr.ExitCode()
	return res
}

type installTerminalResult struct {
	Output   string
	ExitCode int
}

// runInstallScriptWithTerminal runs name under script(1) so the install script can
// read /dev/tty, typing answer on that terminal while the command's own stdin is
// /dev/null.
func runInstallScriptWithTerminal(t *testing.T, account string, env []string, dir, answer, name string, args ...string) installTerminalResult {
	t.Helper()

	if _, err := exec.LookPath("script"); err != nil {
		t.Fatalf("script(1) is required to run the level prompt: %v", err)
	}
	inner := name
	for _, arg := range args {
		inner += " " + installShellQuote(arg)
	}
	inner += " </dev/null"

	cmd := syscontainer.Command(account, env, "script", "-q", "-e", "-c", inner, "/dev/null")
	cmd.Dir = dir
	cmd.Stdin = strings.NewReader(answer)
	var output strings.Builder
	cmd.Stdout = &output
	cmd.Stderr = &output
	err := cmd.Run()
	res := installTerminalResult{Output: output.String()}
	if err == nil {
		return res
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("running %s under a terminal as %s: %v", name, account, err)
	}
	res.ExitCode = exitErr.ExitCode()
	return res
}

func installShellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

// assertInstallImageHasNoFish fails if the image starts carrying fish, which would
// let the completion absence checks pass without exercising the skip.
func assertInstallImageHasNoFish(t *testing.T) {
	t.Helper()
	if path, err := exec.LookPath("fish"); err == nil {
		t.Fatalf("the e2e image now has fish at %s; the completion assertions must cover both cases", path)
	}
}

func assertUserInstallCompletions(t *testing.T, output string) {
	t.Helper()
	if _, err := os.Stat(installUserBashCompletion); err != nil {
		t.Errorf("the user completion %s is missing: %v", installUserBashCompletion, err)
	}
	if _, err := os.Stat(installUserFishCompletion); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the user install wrote the fish completion %s although the image has no fish: %v", installUserFishCompletion, err)
	}
	if _, err := os.Stat(filepath.Join(installHome, ".config", "fish")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the user install created the fish configuration directory although the image has no fish: %v", err)
	}
	if _, err := os.Stat(filepath.Join(installHome, ".local", "share", "zsh", "site-functions", "_peers")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the user install wrote the zsh completion: %v", err)
	}
	if output != "" {
		for _, want := range []string{"the zsh completion was not installed", "completion zsh"} {
			if !strings.Contains(output, want) {
				t.Errorf("output does not contain %q:\n%s", want, output)
			}
		}
	}
}

func assertSystemInstallCompletionsAndMan(t *testing.T) {
	t.Helper()
	for _, path := range []string{
		installSystemBashCompletion,
		installSystemZshCompletion,
		installSystemManPage,
		installSystemUpdateManPage,
	} {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("the system installation file %s is missing: %v", path, err)
		}
	}
	if _, err := os.Stat(installSystemFishCompletion); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the system install wrote the fish completion %s although the image has no fish: %v", installSystemFishCompletion, err)
	}
	if _, err := os.Stat("/usr/local/share/fish"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the system install created the fish configuration directory although the image has no fish: %v", err)
	}
}

func assertUserInstallCompletionsRemoved(t *testing.T) {
	t.Helper()
	for _, path := range []string{installUserBashCompletion, installUserFishCompletion} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("the user completion %s still exists: %v", path, err)
		}
	}
	if _, err := os.Stat(filepath.Join(installHome, ".config", "fish")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the fish configuration directory remains after the user uninstall: %v", err)
	}
}

func assertSystemInstallCompletionsAndManRemoved(t *testing.T) {
	t.Helper()
	for _, path := range []string{
		installSystemBashCompletion,
		installSystemZshCompletion,
		installSystemFishCompletion,
		installSystemManPage,
		installSystemUpdateManPage,
	} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("the system installation file %s still exists: %v", path, err)
		}
	}
	if _, err := os.Stat("/usr/local/share/fish"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the fish configuration directory remains after the system uninstall: %v", err)
	}
}

func assertUserInstall(t *testing.T) {
	t.Helper()

	assertInstallBinary(t, installUserPrefix, installUID)
	assertInstallVersion(t, installAccount, installUserEnv(), installUserPrefix)
	waitInstallSocket(t, "/tmp/peers-"+strconv.FormatUint(uint64(installUID), 10)+"/broker.sock")
	waitInstallUnitState(t, installAccount, installUserEnv(), "active", "--user")

	found := false
	for _, proc := range syscontainer.Pids("peers", "service", "run") {
		if proc.UID == installUID {
			found = true
		}
	}
	if !found {
		t.Fatalf("no Broker process runs as uid %d: %v", installUID, syscontainer.Pids("peers", "service", "run"))
	}
}

func assertSystemInstall(t *testing.T) {
	t.Helper()

	uid, gid, mode := fileOwnerAndMode(t, installSystemPrefix)
	if uid != 0 || gid != 0 {
		t.Errorf("%s owner = %d:%d, want 0:0", installSystemPrefix, uid, gid)
	}
	if mode != 0o755 {
		t.Errorf("%s mode = %#o, want 0755", installSystemPrefix, mode)
	}
	if mode&0o022 != 0 {
		t.Errorf("%s mode = %#o, want no group or other write permission", installSystemPrefix, mode)
	}
	assertInstallVersion(t, "root", nil, installSystemPrefix)
	waitInstallSocket(t, "/run/peers/broker.sock")
	waitInstallUnitState(t, "root", nil, "active")

	if procs := syscontainer.Pids("peers", "service", "run", "--system"); len(procs) == 0 {
		t.Fatalf("no system Broker process is running")
	}
}

func assertInstallBinary(t *testing.T, path string, wantUID uint32) {
	t.Helper()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("%s is not a regular file: %v", path, info.Mode())
	}
	if got := info.Mode().Perm(); got != 0o755 {
		t.Errorf("%s mode = %#o, want 0755", path, got)
	}
	uid, _, _ := fileOwnerAndMode(t, path)
	if uid != wantUID {
		t.Errorf("%s owner = %d, want %d", path, uid, wantUID)
	}
}

func assertInstallVersion(t *testing.T, account string, env []string, binary string) {
	t.Helper()

	res := runWithEnv(t, account, env, binary, "--version")
	if res.ExitCode != 0 {
		t.Fatalf("%s --version as %s: exit %d\nstdout: %s\nstderr: %s",
			binary, account, res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
	}
	if out := strings.TrimSpace(res.Stdout); out != installExpectedVersion {
		t.Errorf("%s --version as %s = %q, want %q", binary, account, out, installExpectedVersion)
	}
}

func waitInstallSocket(t *testing.T, socket string) {
	t.Helper()

	harness.Eventually(t, installServiceStartBound, "the Broker socket "+socket, func() (bool, string) {
		if _, err := os.Stat(socket); err != nil {
			return false, err.Error()
		}
		return true, ""
	})
}

// waitInstallUnitState waits until systemctl reports want; scopeArgs selects the
// user manager ("--user") or the system manager.
func waitInstallUnitState(t *testing.T, account string, env []string, want string, scopeArgs ...string) {
	t.Helper()

	args := append(append([]string{}, scopeArgs...), "is-active", "peers.service")
	harness.Eventually(t, installServiceStartBound, "peers.service to be "+want, func() (bool, string) {
		res := runWithEnv(t, account, env, "systemctl", args...)
		state := strings.TrimSpace(res.Stdout)
		return state == want, fmt.Sprintf("is-active = %q (exit %d, stderr %s)",
			state, res.ExitCode, strings.TrimSpace(res.Stderr))
	})
}

// assertInstallMCPAdd checks that the fake claude received exactly want mcp add
// calls, all as wantUID with wantHome, each naming binary.
func assertInstallMCPAdd(t *testing.T, want int, binary string, wantUID uint32, wantHome string) {
	t.Helper()

	wantArgs := " -- " + binary + " mcp"
	calls := installMCPAddCalls(t)
	if len(calls) != want {
		t.Fatalf("fake claude saw %d mcp add calls, want %d; invocations:\n%s",
			len(calls), want, installClaudeLog(t))
	}
	for _, call := range calls {
		if !strings.HasSuffix(call.Args, wantArgs) {
			t.Errorf("mcp add call %q does not end with %q", call.Args, wantArgs)
		}
		if call.UID != wantUID {
			t.Errorf("%q ran as uid %d, want %d", call.Args, call.UID, wantUID)
		}
		if call.Home != wantHome {
			t.Errorf("%q ran with HOME %q, want %q", call.Args, call.Home, wantHome)
		}
	}
}

func installMCPAddCalls(t *testing.T) []installClaudeCall {
	t.Helper()

	var matching []installClaudeCall
	for _, call := range readInstallClaudeCalls(t) {
		if strings.HasPrefix(call.Args, "mcp add") {
			matching = append(matching, call)
		}
	}
	return matching
}

// assertInstallMCPGet checks that the fake claude received exactly want mcp get
// calls, all as wantUID with wantHome.
func assertInstallMCPGet(t *testing.T, want int, wantUID uint32, wantHome string) {
	t.Helper()

	const wantArgs = "mcp get peer"
	calls := installMCPCalls(t, wantArgs)
	if len(calls) != want {
		t.Fatalf("fake claude saw %q %d times, want %d; invocations:\n%s",
			wantArgs, len(calls), want, installClaudeLog(t))
	}
	for _, call := range calls {
		if call.UID != wantUID {
			t.Errorf("%q ran as uid %d, want %d", wantArgs, call.UID, wantUID)
		}
		if call.Home != wantHome {
			t.Errorf("%q ran with HOME %q, want %q", wantArgs, call.Home, wantHome)
		}
	}
}

func installMCPCalls(t *testing.T, args string) []installClaudeCall {
	t.Helper()

	var matching []installClaudeCall
	for _, call := range readInstallClaudeCalls(t) {
		if call.Args == args {
			matching = append(matching, call)
		}
	}
	return matching
}

func readInstallClaudeCalls(t *testing.T) []installClaudeCall {
	t.Helper()

	path := filepath.Join(installStateDir, "invocations")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the fake claude's invocation log %s: %v", path, err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	calls := make([]installClaudeCall, 0, len(lines))
	for i, line := range lines {
		if line == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 5 {
			t.Fatalf("invocation %d has %d fields, want 5: %q", i+1, len(fields), line)
		}
		uid, err := strconv.ParseUint(fields[0], 10, 32)
		if err != nil {
			t.Fatalf("invocation %d has a non-numeric uid %q: %v", i+1, fields[0], err)
		}
		calls = append(calls, installClaudeCall{
			UID:      uint32(uid),
			User:     fields[1],
			Home:     fields[2],
			SudoUser: fields[3],
			Args:     fields[4],
		})
	}
	return calls
}

func installClaudeLog(t *testing.T) string {
	t.Helper()

	calls := readInstallClaudeCalls(t)
	var b strings.Builder
	for _, call := range calls {
		fmt.Fprintf(&b, "uid=%d user=%q home=%q sudo_user=%q args=%q\n",
			call.UID, call.User, call.Home, call.SudoUser, call.Args)
	}
	return b.String()
}
