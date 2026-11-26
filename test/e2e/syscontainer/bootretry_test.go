package syscontainer

import (
	"errors"
	"testing"
	"time"
)

// TestShouldRebuildSystemd checks that only the two observed boot failures are
// rebuilt and every other failure is not.
func TestShouldRebuildSystemd(t *testing.T) {
	cases := []struct {
		name string
		obs  systemdBootObservation
		want bool
	}{
		{
			name: "exit 255 with empty logs is rebuilt",
			obs:  systemdBootObservation{exited: true, exitCode: 255, logsEmpty: true},
			want: true,
		},
		{
			name: "exit 255 with logs is not rebuilt",
			obs:  systemdBootObservation{exited: true, exitCode: 255},
			want: false,
		},
		{
			name: "exit 1 is not rebuilt",
			obs:  systemdBootObservation{exited: true, exitCode: 1, logsEmpty: true},
			want: false,
		},
		{
			name: "exit 137 is not rebuilt",
			obs:  systemdBootObservation{exited: true, exitCode: 137, logsEmpty: true},
			want: false,
		},
		{
			name: "a timed-out probe with an initializing state is rebuilt",
			obs: systemdBootObservation{
				probeTimedOut: true,
				state:         "initializing",
				elapsed:       systemdBootRebuildDelay,
			},
			want: true,
		},
		{
			name: "an initializing state before the delay is not rebuilt",
			obs: systemdBootObservation{
				probeTimedOut: true,
				state:         "initializing",
				elapsed:       systemdBootRebuildDelay - time.Second,
			},
			want: false,
		},
		{
			name: "a maintenance state is not rebuilt",
			obs: systemdBootObservation{
				probeTimedOut: true,
				state:         "maintenance",
				elapsed:       time.Minute,
			},
			want: false,
		},
		{
			name: "a stopping state is not rebuilt",
			obs: systemdBootObservation{
				probeTimedOut: true,
				state:         "stopping",
				elapsed:       time.Minute,
			},
			want: false,
		},
		{
			name: "an initializing state after a probe that did not time out is not rebuilt",
			obs: systemdBootObservation{
				state:   "initializing",
				elapsed: time.Minute,
			},
			want: false,
		},
		{
			name: "an exited container is not rebuilt twice",
			obs:  systemdBootObservation{exited: true, exitCode: 255, logsEmpty: true, rebuilds: systemdBootRebuilds},
			want: false,
		},
		{
			name: "a stalled container is not rebuilt twice",
			obs: systemdBootObservation{
				probeTimedOut: true,
				state:         "initializing",
				elapsed:       time.Minute,
				rebuilds:      systemdBootRebuilds,
			},
			want: false,
		},
	}
	for _, tc := range cases {
		if got := shouldRebuildSystemd(tc.obs); got != tc.want {
			t.Errorf("%s: shouldRebuildSystemd(%+v) = %t, want %t", tc.name, tc.obs, got, tc.want)
		}
	}
}

// TestInterpretSystemdState checks the probe interpretation, including degraded and a probe timeout.
func TestInterpretSystemdState(t *testing.T) {
	cases := []struct {
		name      string
		output    string
		err       error
		timedOut  bool
		wantState string
		wantReady bool
		wantExit  bool
		wantTime  bool
	}{
		{
			name:      "running is ready",
			output:    "running\n",
			wantState: "running",
			wantReady: true,
		},
		{
			name:      "degraded exits 1 but is ready",
			output:    "degraded\n",
			err:       errors.New("exit status 1"),
			wantState: "degraded",
			wantReady: true,
		},
		{
			name:      "a state that is neither is not ready",
			output:    "initializing\n",
			wantState: `state "initializing"`,
		},
		{
			name:      "maintenance is not a boot failure to rebuild",
			output:    "maintenance\n",
			wantState: `state "maintenance"`,
		},
		{
			name:      "stopping is not a boot failure to rebuild",
			output:    "stopping\n",
			wantState: `state "stopping"`,
		},
		{
			name:     "the probe's own timeout is reported as a timeout",
			err:      errors.New("signal: killed"),
			timedOut: true,
			wantTime: true,
		},
		{
			name:      "an exited container is reported as exited",
			output:    "service \"systemd\" is not running\n",
			err:       errors.New("exit status 1"),
			wantState: "exit status 1: service \"systemd\" is not running",
			wantExit:  true,
		},
	}
	for _, tc := range cases {
		got := interpretSystemdState(tc.output, tc.err, tc.timedOut)
		if got.ready != tc.wantReady {
			t.Errorf("%s: ready = %t, want %t", tc.name, got.ready, tc.wantReady)
		}
		if got.exited != tc.wantExit {
			t.Errorf("%s: exited = %t, want %t", tc.name, got.exited, tc.wantExit)
		}
		if got.timedOut != tc.wantTime {
			t.Errorf("%s: timedOut = %t, want %t", tc.name, got.timedOut, tc.wantTime)
		}
		if got.state != tc.wantState {
			t.Errorf("%s: state = %q, want %q", tc.name, got.state, tc.wantState)
		}
	}
}
