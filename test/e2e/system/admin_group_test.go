//go:build e2e

package e2e

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/syscontainer"
)

const (
	// agGroup is the default administration group a system install
	// provides for the Broker's configuration file.
	agGroup = "peers"
	// agConfigPath is the system Broker's configuration file.
	agConfigPath = "/etc/peers/config.yaml"
	// agSeedMember is the account the confirmation case adds to the group
	// before installing, so the install meets an existing membership.
	agSeedMember = "peer"
	// agNonMember is an ordinary account that never joins the group.
	agNonMember = "other"
	// agPeerListen is the listen address of the user Broker the system
	// Broker pairs with.
	agPeerListen = "127.0.0.1:17445"
	// agServiceBound bounds the wait for the installed system Broker to answer.
	agServiceBound = 10 * time.Second
	// agAuditBound bounds the wait for an audit record to appear.
	agAuditBound = 3 * time.Second
	// agDefaultPort is the port broker.listen defaults to.
	agDefaultPort = 17443
	// agPrivilegedPort and agRootOnlyPort are below 1024, so only root may
	// bind them.
	agPrivilegedPort = 80
	agRootOnlyPort   = 81
	// agListenerHold is how long a refused change must leave the listener as
	// it was.
	agListenerHold = time.Second
	// agDialBound bounds one TCP dial used to observe a listener.
	agDialBound = 200 * time.Millisecond
)

// TestREQBrokerConfig043_SystemInstallAdminGroup covers REQ-broker-config-043:
// a system install provides the administration group, gives a new
// configuration file to that group with mode 0660, and asks the account that
// ran sudo whether to join the group, defaulting to no. A member manages the
// Broker without sudo, except for a privileged listen port, which needs root
// and a configuration file that only root can write. An account outside the
// group is refused, and a reinstall leaves an existing configuration file
// alone.
func TestREQBrokerConfig043_SystemInstallAdminGroup(t *testing.T) {
	if !syscontainer.InContainer() {
		stageInstallArchive(t)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	installFakeClaude(t)
	prepareInstallArchive(t)
	removePreinstalledSystemBinary(t)
	configureInstallSudo(t)
	agSeedGroup(t)

	// The acceptance answers the level question and the admin-group question
	// on the same terminal. Run the archive's script without a level flag so
	// that it asks, re-executes itself through sudo and then asks for the
	// group.
	t.Run("the level and group questions share one terminal", func(t *testing.T) {
		agAssertNotInGroup(t, installAccount)
		res := runInstallScriptWithTerminal(t, installAccount, installUserEnv(), installArchiveDir,
			"y\ny\n", "sh", "./install.sh")
		if res.ExitCode != 0 {
			t.Fatalf("sh ./install.sh with y,y on a terminal: exit %d\n%s", res.ExitCode, res.Output)
		}
		if !strings.Contains(res.Output, installLevelPrompt) {
			t.Errorf("the terminal did not show %q:\n%s", installLevelPrompt, res.Output)
		}
		if !agAskedToJoin(res.Output) {
			t.Errorf("the install did not ask whether to join %s on the same terminal:\n%s", agGroup, res.Output)
		}
		agWaitSystemBroker(t)
		agAssertConfigFile(t)
		agAssertInGroup(t, installAccount)

		// The subtests below start from an account outside the group.
		agRunRoot(t, "gpasswd", "-d", installAccount, agGroup)
	})

	t.Run("default without a terminal leaves the group unchanged", func(t *testing.T) {
		baseline := agGroupLine(t)
		res := agRunInstall(t, "", false)
		if res.ExitCode != 0 {
			t.Fatalf("sudo sh ./install.sh --system without a terminal: exit %d\nstdout: %s\nstderr: %s",
				res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
		}
		if agAskedToJoin(res.Stdout + res.Stderr) {
			t.Errorf("the install asked whether to join %s without a terminal; output:\n%s",
				agGroup, res.Stdout+res.Stderr)
		}
		agWaitSystemBroker(t)
		agAssertConfigFile(t)
		if after := agGroupLine(t); after != baseline {
			t.Errorf("a default answer changed the group:\nbefore %s\nafter  %s", baseline, after)
		}
		if agGroupHas(t, installAccount) {
			t.Errorf("a default answer added %s to %s", installAccount, agGroup)
		}
		if !strings.Contains(res.Stdout, agManualJoinCommand()) {
			t.Errorf("the install output does not print the join command %q:\n%s", agManualJoinCommand(), res.Stdout)
		}
	})

	t.Run("a terminal answer of Enter leaves the group unchanged", func(t *testing.T) {
		baseline := agGroupLine(t)
		res := agRunInstall(t, "\n", true)
		if res.ExitCode != 0 {
			t.Fatalf("sudo sh ./install.sh --system with Enter on a terminal: exit %d\nstdout: %s\nstderr: %s",
				res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
		}
		if !agAskedToJoin(res.Stdout + res.Stderr) {
			t.Errorf("the install did not ask whether to join %s on a terminal; output:\n%s",
				agGroup, res.Stdout+res.Stderr)
		}
		if after := agGroupLine(t); after != baseline {
			t.Errorf("the Enter answer changed the group:\nbefore %s\nafter  %s", baseline, after)
		}
		if agGroupHas(t, installAccount) {
			t.Errorf("the Enter answer added %s to %s", installAccount, agGroup)
		}
		if !strings.Contains(res.Stdout, agManualJoinCommand()) {
			t.Errorf("the install output does not print the join command %q:\n%s", agManualJoinCommand(), res.Stdout)
		}
		agAssertConfigFile(t)
	})

	t.Run("conflicting join flags are a usage error", func(t *testing.T) {
		baseline := agGroupLine(t)
		res := agRunInstall(t, "", false, "--join-admin-group", "--no-admin-group")
		if res.ExitCode != 2 {
			t.Errorf("conflicting join flags: exit %d, want 2\nstdout: %s\nstderr: %s",
				res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
		}
		for _, flag := range []string{"--join-admin-group", "--no-admin-group"} {
			if !strings.Contains(res.Stdout+res.Stderr, flag) {
				t.Errorf("the usage error does not name %s; output:\n%s", flag, res.Stdout+res.Stderr)
			}
		}
		if after := agGroupLine(t); after != baseline {
			t.Errorf("conflicting join flags changed the group:\nbefore %s\nafter  %s", baseline, after)
		}
	})

	t.Run("the join flag adds the account without a terminal", func(t *testing.T) {
		agAssertNotInGroup(t, installAccount)
		res := agRunInstall(t, "", false, "--join-admin-group")
		if res.ExitCode != 0 {
			t.Fatalf("sudo sh ./install.sh --system --join-admin-group: exit %d\nstdout: %s\nstderr: %s",
				res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
		}
		if agAskedToJoin(res.Stdout + res.Stderr) {
			t.Errorf("--join-admin-group asked a question; output:\n%s", res.Stdout+res.Stderr)
		}
		agAssertInGroup(t, installAccount)
	})

	t.Run("a terminal answer of yes adds the account", func(t *testing.T) {
		if agGroupHas(t, installAccount) {
			agRunRoot(t, "gpasswd", "-d", installAccount, agGroup)
		}
		agAssertNotInGroup(t, installAccount)
		res := agRunInstall(t, "y\n", true)
		if res.ExitCode != 0 {
			t.Fatalf("sudo sh ./install.sh --system with y on a terminal: exit %d\nstdout: %s\nstderr: %s",
				res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
		}
		if !agAskedToJoin(res.Stdout + res.Stderr) {
			t.Errorf("the install did not ask whether to join %s on a terminal; output:\n%s",
				agGroup, res.Stdout+res.Stderr)
		}
		agAssertInGroup(t, installAccount)
		agAssertConfigFile(t)
	})

	t.Run("the account manages the Broker without sudo", func(t *testing.T) {
		if !agGroupHas(t, installAccount) {
			t.Fatalf("%s is not in %s, so the management calls cannot run", installAccount, agGroup)
		}
		syscontainer.StartUserBroker(t, agSeedMember, "--listen", agPeerListen)
		key := agCreatePairingKey(t, agSeedMember)

		res := syscontainer.RunAs(t, installAccount, nil, "key", "create")
		if res.ExitCode != 0 {
			t.Fatalf("peers key create as %s: exit %d\nstdout: %s\nstderr: %s",
				installAccount, res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
		}
		if !strings.Contains(res.Stdout, "Key: ") {
			t.Errorf("key create as %s printed %q, want a key line", installAccount, res.Stdout)
		}

		res = syscontainer.RunAs(t, installAccount, nil, "config", "set", "broker.alias", "admin-e2e")
		if res.ExitCode != 0 {
			t.Fatalf("peers config set as %s: exit %d\nstdout: %s\nstderr: %s",
				installAccount, res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
		}
		if !strings.Contains(res.Stdout, "broker.alias:") {
			t.Errorf("config set as %s printed %q, want the changed item", installAccount, res.Stdout)
		}

		res = syscontainer.RunAs(t, installAccount, nil, "broker", "connect", agPeerListen, "--key", key)
		if res.ExitCode != 0 {
			t.Fatalf("peers broker connect as %s: exit %d\nstdout: %s\nstderr: %s",
				installAccount, res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
		}
		if !strings.Contains(res.Stdout, "Paired with ") {
			t.Errorf("broker connect as %s printed %q, want the paired alias", installAccount, res.Stdout)
		}

		agWaitAudit(t, []string{"pairing_key_created", "config_set", "pairing_succeeded"}, installAccount)
	})

	t.Run("a group member cannot set a privileged listen port", func(t *testing.T) {
		if !agGroupHas(t, installAccount) {
			t.Fatalf("%s is not in %s, so it does not hold the delegated permission", installAccount, agGroup)
		}
		defaultAddr := agLoopback(agDefaultPort)
		privilegedAddr := agLoopback(agPrivilegedPort)
		agWaitPort(t, defaultAddr, true)
		if agPortOpen(privilegedAddr) {
			t.Fatalf("%s is already in use before the refused change", privilegedAddr)
		}
		before := agReadConfigState(t)

		res := syscontainer.RunAs(t, installAccount, nil, "config", "set", "broker.listen", ":"+strconv.Itoa(agPrivilegedPort))
		if res.ExitCode == 0 {
			t.Fatalf("peers config set broker.listen :%d as %s succeeded:\n%s",
				agPrivilegedPort, installAccount, res.Stdout)
		}
		combined := res.Stdout + res.Stderr
		if !strings.Contains(combined, "permission_denied") {
			t.Errorf("config set broker.listen :%d as %s = %q, want permission_denied",
				agPrivilegedPort, installAccount, combined)
		}
		if !strings.Contains(combined, "broker.listen") {
			t.Errorf("the refusal does not name broker.listen; output:\n%s", combined)
		}
		if after := agReadConfigState(t); after != before {
			t.Errorf("the refused low-port change altered the configuration file:\nbefore %+v\nafter  %+v",
				before, after)
		}
		harness.Consistently(t, agListenerHold,
			"the refused low-port change to leave the listener on "+defaultAddr,
			func() (bool, string) {
				if !agPortOpen(defaultAddr) {
					return false, defaultAddr + " stopped accepting connections"
				}
				if agPortOpen(privilegedAddr) {
					return false, privilegedAddr + " started accepting connections"
				}
				return true, ""
			})
	})

	t.Run("root is refused while the configuration file is group writable", func(t *testing.T) {
		defaultAddr := agLoopback(agDefaultPort)
		privilegedAddr := agLoopback(agPrivilegedPort)
		agWaitPort(t, defaultAddr, true)
		if agPortOpen(privilegedAddr) {
			t.Fatalf("%s is already in use before the refused change", privilegedAddr)
		}
		before := agReadConfigState(t)

		res := syscontainer.RunAs(t, "root", nil, "config", "set", "broker.listen", ":"+strconv.Itoa(agPrivilegedPort))
		if res.ExitCode == 0 {
			t.Fatalf("peers config set broker.listen :%d as root succeeded on a group-writable file:\n%s",
				agPrivilegedPort, res.Stdout)
		}
		if combined := res.Stdout + res.Stderr; !strings.Contains(combined, "permission_denied") {
			t.Errorf("config set broker.listen :%d as root = %q, want permission_denied",
				agPrivilegedPort, combined)
		}
		if after := agReadConfigState(t); after != before {
			t.Errorf("the refused low-port change altered the configuration file:\nbefore %+v\nafter  %+v",
				before, after)
		}
	})

	t.Run("a root-only configuration file still takes a low port", func(t *testing.T) {
		agRunRoot(t, "chgrp", "root", agConfigPath)
		agRunRoot(t, "chmod", "0600", agConfigPath)

		res := syscontainer.RunAs(t, "root", nil, "config", "set", "broker.listen", ":"+strconv.Itoa(agRootOnlyPort))
		if res.ExitCode != 0 {
			t.Fatalf("peers config set broker.listen :%d as root over a root-only file: exit %d\nstdout: %s\nstderr: %s",
				agRootOnlyPort, res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
		}
		agWaitPort(t, agLoopback(agRootOnlyPort), true)
	})

	t.Run("an account outside the group is refused", func(t *testing.T) {
		agAssertNotInGroup(t, agNonMember)
		syscontainer.StartUserBroker(t, agSeedMember, "--listen", agPeerListen)
		key := agCreatePairingKey(t, agSeedMember)
		before := agReadConfigState(t)

		for _, call := range []struct {
			name string
			args []string
		}{
			{"key create", []string{"key", "create"}},
			{"config set", []string{"config", "set", "broker.alias", "other-alias"}},
			{"broker connect", []string{"broker", "connect", agPeerListen, "--key", key}},
		} {
			res := syscontainer.RunAs(t, agNonMember, nil, call.args...)
			if res.ExitCode == 0 {
				t.Errorf("peers %s as %s outside %s succeeded:\n%s",
					call.name, agNonMember, agGroup, res.Stdout)
				continue
			}
			combined := res.Stdout + res.Stderr
			if !strings.Contains(combined, "permission_denied") {
				t.Errorf("peers %s as %s = %q, want permission_denied", call.name, agNonMember, combined)
			}
			if !strings.Contains(combined, agConfigPath) {
				t.Errorf("peers %s as %s does not name %s; output:\n%s",
					call.name, agNonMember, agConfigPath, combined)
			}
		}

		if after := agReadConfigState(t); after != before {
			t.Errorf("refused calls changed the configuration file:\nbefore %+v\nafter  %+v", before, after)
		}
	})

	t.Run("reinstalling keeps an existing configuration's group and mode", func(t *testing.T) {
		agRunRoot(t, "chgrp", "root", agConfigPath)
		agRunRoot(t, "chmod", "0600", agConfigPath)
		before := agReadConfigState(t)
		baseline := agGroupLine(t)

		res := agRunInstall(t, "", false, "--no-admin-group")
		if res.ExitCode != 0 {
			t.Fatalf("reinstall over an existing configuration: exit %d\nstdout: %s\nstderr: %s",
				res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
		}
		if after := agReadConfigState(t); after != before {
			t.Errorf("the reinstall changed the existing configuration file:\nbefore %+v\nafter  %+v", before, after)
		}
		if after := agGroupLine(t); after != baseline {
			t.Errorf("the reinstall changed the group:\nbefore %s\nafter  %s", baseline, after)
		}
	})
}

// TestREQBrokerConfig043_ExistingAdminGroupNeedsConfirmation covers the rule
// that an existing administration group which lists members is only reused
// when the administrator names it with --admin-group. The refused installation
// writes no configuration file; the accepted one keeps the members. Naming the
// group settles the decision, so the member lookup is skipped and the summary
// only says the group was reused.
func TestREQBrokerConfig043_ExistingAdminGroupNeedsConfirmation(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	agSeedGroup(t)
	agRunRoot(t, "usermod", "-aG", agGroup, agSeedMember)
	if _, err := os.Stat(agConfigPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("precondition: %s already exists: %v", agConfigPath, err)
	}

	res := syscontainer.RunAs(t, "root", nil, "service", "install", "--system")
	if res.ExitCode == 0 {
		t.Fatalf("service install --system reused an unnamed group with members:\n%s", res.Stdout)
	}
	want := `cannot use the existing group "peers" as the administration group: its member ` + agSeedMember +
		` would be able to manage the system Broker; pass --admin-group peers to accept them, or pass --admin-group with another name`
	if !strings.Contains(res.Stderr, want) {
		t.Errorf("install stderr = %q, want it to contain %q", res.Stderr, want)
	}
	if _, err := os.Stat(agConfigPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the refused installation wrote %s: %v", agConfigPath, err)
	}

	res = syscontainer.RunAs(t, "root", nil, "service", "install", "--system", "--admin-group", agGroup)
	if res.ExitCode != 0 {
		t.Fatalf("service install --system --admin-group %s: exit %d\nstdout: %s\nstderr: %s",
			agGroup, res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
	}
	if got, want := summaryValue(t, res.Stdout, "admin group:"), agGroup+" (existing)"; got != want {
		t.Errorf("summary admin group = %q, want %q", got, want)
	}
	agWaitSystemBroker(t)
	agAssertConfigFile(t)
	if !agGroupHas(t, agSeedMember) {
		t.Errorf("the accepted installation changed the group's members: %s", agGroupLine(t))
	}
}

// agConfigState is the part of a file's state a reinstall must preserve.
type agConfigState struct {
	sum  string
	uid  uint32
	gid  uint32
	mode os.FileMode
}

// agReadConfigState reads the configuration file's bytes, owner, group and mode.
func agReadConfigState(t *testing.T) agConfigState {
	t.Helper()
	data, err := os.ReadFile(agConfigPath)
	if err != nil {
		t.Fatalf("reading %s: %v", agConfigPath, err)
	}
	info, err := os.Stat(agConfigPath)
	if err != nil {
		t.Fatalf("stat %s: %v", agConfigPath, err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("%s has no owner information", agConfigPath)
	}
	sum := sha256.Sum256(data)
	return agConfigState{
		sum:  hex.EncodeToString(sum[:]),
		uid:  stat.Uid,
		gid:  stat.Gid,
		mode: info.Mode().Perm(),
	}
}

// agAssertConfigFile checks that a new configuration file belongs to root, has
// the administration group and is readable and writable by that group only.
func agAssertConfigFile(t *testing.T) {
	t.Helper()
	state := agReadConfigState(t)
	wantGID := agGroupGID(t)
	if state.uid != 0 || state.gid != wantGID {
		t.Errorf("%s owner = %d:%d, want 0:%d", agConfigPath, state.uid, state.gid, wantGID)
	}
	if state.mode != 0o660 {
		t.Errorf("%s mode = %#o, want 0660", agConfigPath, state.mode)
	}
}

// agSeedGroup creates the administration group without members, so the first
// install reuses a group it did not create. A group with members is only reused
// when the administrator names it, which
// TestREQBrokerConfig043_ExistingAdminGroupNeedsConfirmation covers.
func agSeedGroup(t *testing.T) {
	t.Helper()
	if !agGroupExists(t) {
		agRunRoot(t, "groupadd", "--system", agGroup)
	}
}

// agGroupExists reports whether the group is in the account database.
func agGroupExists(t *testing.T) bool {
	t.Helper()
	cmd := syscontainer.Command("root", nil, "getent", "group", agGroup)
	return cmd.Run() == nil
}

// agGroupLine returns the group's getent line.
func agGroupLine(t *testing.T) string {
	t.Helper()
	return strings.TrimSpace(agRunRootOut(t, "getent", "group", agGroup))
}

// agGroupGID returns the group's id.
func agGroupGID(t *testing.T) uint32 {
	t.Helper()
	fields := strings.Split(agGroupLine(t), ":")
	if len(fields) != 4 {
		t.Fatalf("getent group %s = %q, want four fields", agGroup, agGroupLine(t))
	}
	gid, err := strconv.ParseUint(fields[2], 10, 32)
	if err != nil {
		t.Fatalf("group %s has a non-numeric gid %q: %v", agGroup, fields[2], err)
	}
	return uint32(gid)
}

// agGroupHas reports whether account is a direct member of the group.
func agGroupHas(t *testing.T, account string) bool {
	t.Helper()
	for _, member := range agGroupMembers(t) {
		if member == account {
			return true
		}
	}
	return false
}

// agGroupMembers returns the group's direct member list.
func agGroupMembers(t *testing.T) []string {
	t.Helper()
	fields := strings.Split(agGroupLine(t), ":")
	if len(fields) != 4 {
		t.Fatalf("getent group %s = %q, want four fields", agGroup, agGroupLine(t))
	}
	if fields[3] == "" {
		return nil
	}
	return strings.Split(fields[3], ",")
}

// agAssertInGroup checks both the group's member list and the group list the
// account database reports.
func agAssertInGroup(t *testing.T, account string) {
	t.Helper()
	if !agGroupHas(t, account) {
		t.Errorf("%s is not a member of %s: %s", account, agGroup, agGroupLine(t))
	}
	gid := strconv.FormatUint(uint64(agGroupGID(t)), 10)
	if groups := strings.Fields(agRunRootOut(t, "getent", "initgroups", account)); !agContains(groups, gid) {
		t.Errorf("getent initgroups %s = %v, want it to include %s", account, groups, gid)
	}
}

// agAssertNotInGroup checks that account is absent from the group.
func agAssertNotInGroup(t *testing.T, account string) {
	t.Helper()
	if agGroupHas(t, account) {
		t.Fatalf("precondition: %s is already a member of %s", account, agGroup)
	}
}

// agContains reports whether values holds want.
func agContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// agAskedToJoin reports whether the install output carries a question that
// offers to add an account to the administration group.
func agAskedToJoin(output string) bool {
	for _, line := range strings.Split(output, "\n") {
		if !strings.Contains(line, "?") {
			continue
		}
		if strings.Contains(line, agGroup) || strings.Contains(line, installAccount) {
			return true
		}
	}
	return false
}

// agRunInstall runs install.sh --system as the ordinary account through sudo.
// tty gives the process a pseudo-terminal through script, and answer is
// written to its standard input. The extra arguments follow --system.
func agRunInstall(t *testing.T, answer string, tty bool, args ...string) syscontainer.Result {
	t.Helper()

	name := "sudo"
	argv := append([]string{"-n", "sh", "./install.sh", "--system"}, args...)
	if tty {
		inner := "sudo " + strings.Join(append([]string{"-n", "sh", "./install.sh", "--system"}, args...), " ")
		name = "script"
		argv = []string{"-q", "-e", "-c", inner, "/dev/null"}
	}

	cmd := syscontainer.Command(installAccount, installUserEnv(), name, argv...)
	if !tty {
		// Detach from any controlling terminal so this case really has none:
		// the script must neither prompt nor read the answer from stdin.
		if cmd.SysProcAttr == nil {
			cmd.SysProcAttr = &syscall.SysProcAttr{}
		}
		cmd.SysProcAttr.Setsid = true
	}
	cmd.Dir = installArchiveDir
	cmd.Stdin = strings.NewReader(answer)
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
		t.Fatalf("running %s %s as %s: %v", name, strings.Join(argv, " "), installAccount, err)
	}
	res.ExitCode = exitErr.ExitCode()
	return res
}

// agManualJoinCommand is the command the install prints when it leaves the
// account out of the admin group.
func agManualJoinCommand() string {
	return "sudo usermod -aG " + agGroup + " " + installAccount
}

// agLoopback returns the loopback address of port.
func agLoopback(port int) string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
}

// agPortOpen reports whether a TCP connection to addr succeeds.
func agPortOpen(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, agDialBound)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// agWaitPort waits until addr accepts connections (want true) or stops
// accepting them (want false).
func agWaitPort(t *testing.T, addr string, want bool) {
	t.Helper()
	state := "stop"
	if want {
		state = "start"
	}
	harness.Eventually(t, agServiceBound, addr+" to "+state+" accepting connections", func() (bool, string) {
		return agPortOpen(addr) == want, ""
	})
}

// agWaitSystemBroker waits until the installed system Broker answers the root
// account's terminal commands.
func agWaitSystemBroker(t *testing.T) {
	t.Helper()
	harness.Eventually(t, agServiceBound, "the installed system Broker to answer broker list", func() (bool, string) {
		res := syscontainer.RunAs(t, "root", nil, "broker", "list")
		return res.ExitCode == 0, res.Stdout + res.Stderr
	})
}

// agCreatePairingKey creates a one-time pairing key at account's own Broker.
func agCreatePairingKey(t *testing.T, account string) string {
	t.Helper()
	res := syscontainer.RunAs(t, account, nil, "key", "create")
	if res.ExitCode != 0 {
		t.Fatalf("peers key create as %s: exit %d\nstdout: %s\nstderr: %s",
			account, res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
	}
	for _, line := range strings.Split(res.Stdout, "\n") {
		if key := strings.TrimPrefix(line, "Key: "); key != line && strings.TrimSpace(key) != "" {
			return strings.TrimSpace(key)
		}
	}
	t.Fatalf("peers key create as %s printed no key:\n%s", account, res.Stdout)
	return ""
}

// agWaitAudit waits until the audit log holds every event with account as its
// account.
func agWaitAudit(t *testing.T, events []string, account string) {
	t.Helper()
	harness.Eventually(t, agAuditBound, "the audit log to record "+strings.Join(events, ", ")+" for "+account,
		func() (bool, string) {
			res := syscontainer.RunAs(t, "root", nil, "service", "audit")
			if res.ExitCode != 0 {
				return false, res.Stderr
			}
			for _, event := range events {
				if !agAuditHas(res.Stdout, event, account) {
					return false, res.Stdout
				}
			}
			return true, res.Stdout
		})
}

// agAuditHas reports whether one audit line names event and account.
func agAuditHas(audit, event, account string) bool {
	want := "account=" + account
	for _, line := range strings.Split(audit, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[1] != event {
			continue
		}
		if strings.Contains(line, want) {
			return true
		}
	}
	return false
}

// agRunRoot runs a command as root and fails the test when it does not succeed.
func agRunRoot(t *testing.T, argv ...string) {
	t.Helper()
	_ = agRunRootOut(t, argv...)
}

// agRunRootOut runs a command as root and returns its combined output.
func agRunRootOut(t *testing.T, argv ...string) string {
	t.Helper()
	cmd := syscontainer.Command("root", nil, argv[0], argv[1:]...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v\n%s", strings.Join(argv, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out)
}
