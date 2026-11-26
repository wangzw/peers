//go:build e2e

package e2e

import (
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/syscontainer"
)

const (
	// adAccount, adGroup and adShareGroup exist only in the extrausers source;
	// adShareGroup is a supplementary group of adAccount, not its primary group.
	adAccount    = "diruser"
	adUID        = 2100
	adGroup      = "dirgroup"
	adGID        = 2100
	adShareGroup = "dirshare"
	adShareGID   = 2101
	adSocketPath = "/run/peers/broker.sock"
	adConfigPath = "/etc/peers/config.yaml"
)

// TestSystemAccountsFromExtrausers checks that the system Broker resolves
// accounts and groups through the NSS account database rather than by reading
// /etc/passwd and /etc/group.
func TestSystemAccountsFromExtrausers(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	for _, file := range []string{"/etc/passwd", "/etc/group"} {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("reading %s: %v", file, err)
		}
		for _, name := range []string{adAccount, adGroup, adShareGroup} {
			if strings.Contains(string(data), name+":") {
				t.Fatalf("%s names %s, so it does not test the extrausers source", file, name)
			}
		}
	}
	passwd := adRunRootOut(t, "getent", "passwd", adAccount)
	fields := strings.Split(strings.TrimSpace(passwd), ":")
	if len(fields) != 7 || fields[2] != strconv.Itoa(adUID) {
		t.Fatalf("getent passwd %s = %q, want uid %d", adAccount, passwd, adUID)
	}
	groupLine := adRunRootOut(t, "getent", "group", adGroup)
	groupFields := strings.Split(strings.TrimSpace(groupLine), ":")
	if len(groupFields) != 4 || groupFields[2] != strconv.Itoa(adGID) {
		t.Fatalf("getent group %s = %q, want gid %d", adGroup, groupLine, adGID)
	}
	shareLine := adRunRootOut(t, "getent", "group", adShareGroup)
	shareFields := strings.Split(strings.TrimSpace(shareLine), ":")
	if len(shareFields) != 4 || shareFields[2] != strconv.Itoa(adShareGID) {
		t.Fatalf("getent group %s = %q, want gid %d", adShareGroup, shareLine, adShareGID)
	}
	initgroups := adRunRootOut(t, "getent", "initgroups", adAccount)
	if !strings.Contains(initgroups, strconv.Itoa(adShareGID)) {
		t.Fatalf("getent initgroups %s = %q, want it to include %d", adAccount, initgroups, adShareGID)
	}

	syscontainer.StartSystemBroker(t,
		"broker:\n  alias: sys\n  local_socket_group: dirgroup\n  local_socket_mode: \"0660\"\n")

	syscontainer.WaitFor(t, "the system socket to be 0660 dirgroup", func() bool {
		mode, gid := adSocketModeGID(t, adSocketPath)
		return mode == 0o660 && gid == adGID
	})

	session := syscontainer.NewSession(t, adAccount, "dsuser")
	adWaitRegistered(t, session)
	syscontainer.WaitFor(t, "the account helper of "+adAccount, func() bool {
		for _, proc := range syscontainer.Pids("internal", "account-helper") {
			if proc.UID == adUID {
				return true
			}
		}
		return false
	})

	text, isError := session.Call(t, "get_config", map[string]any{})
	if !isError {
		t.Fatalf("get_config as %s without permission succeeded: %s", adAccount, text)
	}
	if !strings.Contains(text, "(code: permission_denied)") {
		t.Fatalf("get_config as %s without permission = %q, want permission_denied", adAccount, text)
	}

	adRunRoot(t, "chgrp", adShareGroup, adConfigPath)
	adRunRoot(t, "chmod", "0640", adConfigPath)

	text, isError = session.Call(t, "get_config", map[string]any{})
	if isError {
		t.Fatalf("get_config as %s after granting %s read access: %s", adAccount, adShareGroup, text)
	}
	if !strings.Contains(text, "broker.alias") {
		t.Errorf("get_config as %s does not name broker.alias: %s", adAccount, text)
	}
}

func adSocketModeGID(t *testing.T, path string) (os.FileMode, uint32) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("%s has no owner information", path)
	}
	return info.Mode().Perm(), stat.Gid
}

func adWaitRegistered(t *testing.T, session *syscontainer.Session) {
	t.Helper()
	harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "the session "+session.Account+" to register",
		func(deadline time.Time) (bool, string) {
			text, isError, err := session.PollCall(t, deadline, "list_agents", map[string]any{})
			if err != nil {
				return false, err.Error()
			}
			return !isError, text
		})
}

func adRunRoot(t *testing.T, argv ...string) {
	t.Helper()
	_ = adRunRootOut(t, argv...)
}

func adRunRootOut(t *testing.T, argv ...string) string {
	t.Helper()
	cmd := syscontainer.Command("root", nil, argv[0], argv[1:]...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v\n%s", strings.Join(argv, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out)
}
