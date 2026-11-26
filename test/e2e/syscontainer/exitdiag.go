// This file carries no e2e build tag so that the exit-status diagnostic and its
// host-side test compile without one.

package syscontainer

import (
	"fmt"
	"strings"
	"time"

	"github.com/wangzw/peers/test/e2e/harness"
)

// killedExitCode is the exit status of a process killed by SIGKILL; the
// container's own logs usually end before the kill, so the daemon's event
// stream is what explains it.
const killedExitCode = 137

// exitDiagnosticCommand returns the docker events query for the project's owner
// label from started until now; --until is required because docker events
// without it waits for new events and would hang the failure report.
func exitDiagnosticCommand(owner string, started, now time.Time) []string {
	return []string{
		"events",
		"--filter", "label=" + harness.OwnerLabel + "=" + owner,
		"--since", started.UTC().Format(time.RFC3339Nano),
		"--until", now.UTC().Format(time.RFC3339Nano),
	}
}

// formatExitDiagnostic renders the events of a run that exited with
// killedExitCode and returns "" for any other exit status.
func formatExitDiagnostic(code int, events string) string {
	if code != killedExitCode {
		return ""
	}
	events = strings.TrimSpace(events)
	if events == "" {
		events = "(no events)"
	}
	return fmt.Sprintf("the inner process ended with exit code %d; docker events for the project's owner label since it started:\n%s",
		code, events)
}

func collectExitDiagnostic(code int, run func(args ...string) string, owner string, started, now time.Time) string {
	if code != killedExitCode {
		return ""
	}
	return formatExitDiagnostic(code, run(exitDiagnosticCommand(owner, started, now)...))
}
