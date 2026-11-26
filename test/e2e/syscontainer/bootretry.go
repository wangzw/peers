// This file carries no e2e build tag so that the retry decision and its
// host-side test compile and run without one.

// Package syscontainer runs the system end-to-end tests inside containers. The
// host test process forwards each test to a fresh container of the requested
// service, so tests that need root, several accounts, or systemd see the
// environment the system Broker is designed for.
package syscontainer

import (
	"fmt"
	"strings"
	"time"
)

const (
	// systemdBootRebuildDelay is how long a container that is still
	// initializing is left before it is rebuilt. The stall becomes observable
	// only when the wait probe's own bound expires, so this value is also that
	// probe's bound.
	systemdBootRebuildDelay = 10 * time.Second

	// systemdBootRebuilds bounds how many times a systemd container is
	// recreated after it fails to finish booting; only the two failure modes
	// matched in shouldRebuildSystemd are retried, and any other failure is a
	// real defect.
	systemdBootRebuilds = 1
)

// systemdBootObservation is what the wait knows about one failed readiness
// probe when it decides about a rebuild.
type systemdBootObservation struct {
	exited        bool
	exitCode      int
	logsEmpty     bool
	probeTimedOut bool
	state         string
	elapsed       time.Duration
	rebuilds      int
}

// shouldRebuildSystemd reports whether the observation is one of the two
// observed boot failures and a rebuild is still allowed.
func shouldRebuildSystemd(o systemdBootObservation) bool {
	if o.rebuilds >= systemdBootRebuilds {
		return false
	}
	if o.exited {
		return o.exitCode == 255 && o.logsEmpty
	}
	return o.probeTimedOut && o.state == "initializing" && o.elapsed >= systemdBootRebuildDelay
}

// systemdProbeResult is one interpreted is-system-running --wait result.
type systemdProbeResult struct {
	state    string
	ready    bool
	exited   bool
	timedOut bool
}

// interpretSystemdState turns one is-system-running result into a probe result;
// the text decides readiness, not the exit code, because systemctl exits 1 for
// degraded although a degraded system is ready for the tests. A probe killed by
// its own bound is a timeout, and an error whose output does not name a settled
// state is reported with the error text.
func interpretSystemdState(output string, err error, timedOut bool) systemdProbeResult {
	text := strings.TrimSpace(output)
	if text == "running" || text == "degraded" {
		return systemdProbeResult{state: text, ready: true}
	}
	if timedOut {
		return systemdProbeResult{timedOut: true}
	}
	if err != nil {
		return systemdProbeResult{
			state:  fmt.Sprintf("%v: %s", err, text),
			exited: strings.Contains(text, "is not running"),
		}
	}
	return systemdProbeResult{state: fmt.Sprintf("state %q", text)}
}
