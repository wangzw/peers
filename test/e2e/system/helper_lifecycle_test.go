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

// helperLifecycleBound is the product timer: the pool respawns after its
// one-second minimum backoff, and the small margin covers the process exit and
// the registry watch. A wait adds harness.EventTimeout as the fallback, and a
// wait that polls list_agents also covers ListAgentsWaitBound because one
// stalled poll can run for ListAgentsPollBound.
const (
	helperLifecycleBound    = 3 * time.Second
	helperLifecycleWait     = helperLifecycleBound + harness.EventTimeout
	helperLifecycleListWait = max(helperLifecycleWait, harness.ListAgentsWaitBound)
)

// TestSystemAccountHelperLifecycle checks the per-account helper process of the
// system Broker. It must exit once the account has no session, and a helper
// killed with SIGKILL must be replaced without losing the account's registry
// watch: a later rename of the session still reaches list_agents.
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

	// Ending the session ends its Register stream, which releases the
	// account's backend and watch; nothing then keeps the helper alive.
	closeSession(t, first)
	harness.Eventually(t, helperLifecycleWait, "other's account helper to exit after the session ends",
		func() (bool, string) {
			helpers := accountHelpers(otherUID)
			return len(helpers) == 0, fmt.Sprintf("helper processes = %v", helpers)
		})

	// The next registration starts a new helper on demand.
	second := syscontainer.NewSession(t, "other", "helped2")
	e2eSysWaitRegistered(t, second)
	killed := waitAccountHelper(t, otherUID, 0)

	// SIGKILL must not lose the account's watch. The pool respawns the helper
	// and restores the watch, so a rename after the crash is still seen.
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

// accountHelpers returns the running account helpers of uid.
func accountHelpers(uid uint32) []syscontainer.ProcInfo {
	var helpers []syscontainer.ProcInfo
	for _, proc := range syscontainer.Pids("peers", "internal", "account-helper") {
		if proc.UID == uid {
			helpers = append(helpers, proc)
		}
	}
	return helpers
}

// waitAccountHelper waits for an account helper of uid whose pid is not
// excludePID and returns it.
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
