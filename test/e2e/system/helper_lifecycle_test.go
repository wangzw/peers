//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/procsig"
	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/syscontainer"
)

// helperLifecycleBound covers the helper pool's one-second minimum backoff plus
// the process exit and registry watch; helperLifecycleWait adds the harness
// fallback, and helperLifecycleListWait takes the larger of that and
// ListAgentsWaitBound.
const (
	helperLifecycleBound    = 3 * time.Second
	helperLifecycleWait     = helperLifecycleBound + harness.EventTimeout
	helperLifecycleListWait = max(helperLifecycleWait, harness.ListAgentsWaitBound)
)

// TestSystemAccountHelperLifecycle checks that the per-account helper exits
// when the account has no session and is replaced after SIGKILL without losing
// the registry watch.
func TestSystemAccountHelperLifecycle(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Plain)
		return
	}

	syscontainer.StartSystemBroker(t, "broker:\n  alias: sys\n")
	otherUID := syscontainer.AccountUID(t, "other")

	first := syscontainer.NewSession(t, "other", "helped")
	e2eSysWaitRegistered(t, first)
	helper := waitAccountHelper(t, otherUID, 0)
	t.Logf("other's account helper runs as pid %d", helper.PID)

	closeSession(t, first)
	harness.Eventually(t, helperLifecycleWait, "other's account helper to exit after the session ends",
		func() (bool, string) {
			helpers := accountHelpers(otherUID)
			return len(helpers) == 0, fmt.Sprintf("helper processes = %v", helpers)
		})

	second := syscontainer.NewSession(t, "other", "helped2")
	e2eSysWaitRegistered(t, second)
	killed := waitAccountHelper(t, otherUID, 0)

	if err := procsig.SendSignal(killed.PID, syscall.SIGKILL); err != nil {
		t.Fatalf("kill -9 the account helper %d: %v", killed.PID, err)
	}
	rebuilt := waitAccountHelper(t, otherUID, killed.PID)
	if rebuilt.PID == killed.PID {
		t.Fatalf("the account helper still has pid %d after SIGKILL", killed.PID)
	}
	t.Logf("the account helper %d was replaced by %d", killed.PID, rebuilt.PID)

	second.Fake.Rename("renamed")
	harness.EventuallyPoll(t, helperLifecycleListWait, "the renamed session to reach list_agents",
		func(deadline time.Time) (bool, string) {
			out, state := e2eSysListAgentsPoll(t, deadline, second, nil)
			if state != "" {
				return false, state
			}
			if strings.HasPrefix(out.Self, "renamed@") {
				t.Logf("list_agents reports the renamed session as %s", out.Self)
				return true, out.Self
			}
			return false, out.Self
		})
}

func accountHelpers(uid uint32) []syscontainer.ProcInfo {
	var helpers []syscontainer.ProcInfo
	for _, proc := range syscontainer.Pids("peers", "internal", "account-helper") {
		if proc.UID == uid {
			helpers = append(helpers, proc)
		}
	}
	return helpers
}

func waitAccountHelper(t *testing.T, uid uint32, excludePID int) syscontainer.ProcInfo {
	t.Helper()
	var helper syscontainer.ProcInfo
	harness.Eventually(t, helperLifecycleWait, "the account helper of other",
		func() (bool, string) {
			helpers := accountHelpers(uid)
			for _, proc := range helpers {
				if proc.PID != excludePID {
					helper = proc
					return true, ""
				}
			}
			return false, fmt.Sprintf("helper processes = %v", helpers)
		})
	return helper
}
