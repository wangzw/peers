package broker

import (
	"context"
	"errors"
	"net/http"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/broker/update"
	"github.com/wangzw/peers/internal/config"
)

func updateCheckCalls() (func(context.Context, *http.Client, string) (update.Release, error), <-chan struct{}) {
	calls := make(chan struct{}, 16)
	return func(context.Context, *http.Client, string) (update.Release, error) {
		calls <- struct{}{}
		return update.Release{Version: "0.1.2", URL: "https://example.test/release/0.1.2"}, nil
	}, calls
}

func expectCheck(t *testing.T, calls <-chan struct{}, want bool) {
	t.Helper()
	select {
	case <-calls:
		if !want {
			t.Fatal("the update check ran, want it not to run")
		}
	default:
		if want {
			t.Fatal("the update check did not run")
		}
	}
}

// TestUpdateCheckWaitsThenRepeatsAtInterval checks that the first check waits
// the initial delay and later checks repeat at the default interval.
func TestUpdateCheckWaitsThenRepeatsAtInterval(t *testing.T) {
	latest, calls := updateCheckCalls()
	tb := newTestBroker(t, func(d *Deps) {
		d.Version = "0.1.1"
		d.LatestRelease = latest
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tb.b.startUpdateChecks(ctx)

	tb.clock.Advance(updateCheckInitialDelay() - time.Nanosecond)
	expectCheck(t, calls, false)

	tb.clock.Advance(time.Nanosecond)
	expectCheck(t, calls, true)

	tb.clock.Advance(updateCheckDefaultInterval - time.Nanosecond)
	expectCheck(t, calls, false)

	tb.clock.Advance(time.Nanosecond)
	expectCheck(t, calls, true)
}

// TestUpdateCheckDefaultInitialDelayIsOneMinute pins the release default at
// exactly one minute.
func TestUpdateCheckDefaultInitialDelayIsOneMinute(t *testing.T) {
	latest, calls := updateCheckCalls()
	tb := newTestBroker(t, func(d *Deps) {
		d.Version = "0.1.1"
		d.LatestRelease = latest
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tb.b.startUpdateChecks(ctx)

	// Keep the literal time.Minute: this pins the release default, so
	// updateCheckInitialDelay() would make the test compare the hook to itself.
	tb.clock.Advance(time.Minute - time.Nanosecond)
	expectCheck(t, calls, false)

	tb.clock.Advance(time.Nanosecond)
	expectCheck(t, calls, true)
}

// TestUpdateCheckDisabledSkipsLookups checks that a disabled check makes no
// lookup and re-enabling takes effect at the next period.
func TestUpdateCheckDisabledSkipsLookups(t *testing.T) {
	latest, calls := updateCheckCalls()
	tb := newTestBroker(t, func(d *Deps) {
		d.Version = "0.1.1"
		d.LatestRelease = latest
	})
	tb.cfg.setValue("update.check", config.Value{Kind: config.KindBool, Bool: false})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tb.b.startUpdateChecks(ctx)

	tb.clock.Advance(updateCheckInitialDelay())
	expectCheck(t, calls, false)
	tb.clock.Advance(updateCheckDefaultInterval)
	expectCheck(t, calls, false)

	tb.cfg.setValue("update.check", config.Value{Kind: config.KindBool, Bool: true})
	tb.clock.Advance(updateCheckDefaultInterval)
	expectCheck(t, calls, true)
}

// TestUpdateCheckDisabledClearsAvailableVersion checks that disabling the
// check clears the available version at once.
func TestUpdateCheckDisabledClearsAvailableVersion(t *testing.T) {
	latest := func(context.Context, *http.Client, string) (update.Release, error) {
		return update.Release{Version: "0.1.2", URL: "https://example.test/release"}, nil
	}
	tb := newTestBroker(t, func(d *Deps) {
		d.Version = "0.1.1"
		d.LatestRelease = latest
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tb.b.startUpdateChecks(ctx)
	tb.clock.Advance(updateCheckInitialDelay())

	resp, err := tb.client.UpdateStatus(context.Background(), &localpb.UpdateStatusRequest{})
	if err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}
	if got := resp.GetAvailableVersion(); got != "0.1.2" {
		t.Fatalf("AvailableVersion = %q before disabling, want 0.1.2", got)
	}
	status, err := tb.client.Status(context.Background(), &localpb.StatusRequest{})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if got := status.GetAvailableVersion(); got != "0.1.2" {
		t.Fatalf("Status.AvailableVersion = %q before disabling, want 0.1.2", got)
	}

	tb.cfg.setValue("update.check", config.Value{Kind: config.KindBool, Bool: false})

	resp, err = tb.client.UpdateStatus(context.Background(), &localpb.UpdateStatusRequest{})
	if err != nil {
		t.Fatalf("UpdateStatus after disabling: %v", err)
	}
	if got := resp.GetAvailableVersion(); got != "" {
		t.Errorf("AvailableVersion = %q after disabling, want empty", got)
	}
	status, err = tb.client.Status(context.Background(), &localpb.StatusRequest{})
	if err != nil {
		t.Fatalf("Status after disabling: %v", err)
	}
	if got := status.GetAvailableVersion(); got != "" {
		t.Errorf("Status.AvailableVersion = %q after disabling, want empty", got)
	}
}

// TestUpdateCheckConcurrentWithStatusReads checks that periodic update checks
// concurrent with Status and UpdateStatus reads do not race.
func TestUpdateCheckConcurrentWithStatusReads(t *testing.T) {
	latest := func(context.Context, *http.Client, string) (update.Release, error) {
		return update.Release{Version: "0.1.2", URL: "https://example.test/release"}, nil
	}
	tb := newTestBroker(t, func(d *Deps) {
		d.Version = "0.1.1"
		d.LatestRelease = latest
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tb.b.startUpdateChecks(ctx)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := tb.client.Status(context.Background(), &localpb.StatusRequest{}); err != nil {
					errs <- err
					return
				}
				if _, err := tb.client.UpdateStatus(context.Background(), &localpb.UpdateStatusRequest{}); err != nil {
					errs <- err
					return
				}
				runtime.Gosched()
			}
		}()
	}

	for i := 0; i < 200; i++ {
		tb.clock.Advance(updateCheckDefaultInterval)
	}
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent status read: %v", err)
	}
}

// TestUpdateCheckUsesNewIntervalAfterNextPeriod checks that a changed interval
// applies from the period after the next check.
func TestUpdateCheckUsesNewIntervalAfterNextPeriod(t *testing.T) {
	latest, calls := updateCheckCalls()
	tb := newTestBroker(t, func(d *Deps) {
		d.Version = "0.1.1"
		d.LatestRelease = latest
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tb.b.startUpdateChecks(ctx)

	tb.clock.Advance(updateCheckInitialDelay())
	expectCheck(t, calls, true)

	tb.cfg.setValue("update.check_interval", config.Value{Kind: config.KindDuration, Duration: 2 * time.Hour})
	tb.clock.Advance(updateCheckDefaultInterval)
	expectCheck(t, calls, true)

	tb.clock.Advance(2*time.Hour - time.Nanosecond)
	expectCheck(t, calls, false)
	tb.clock.Advance(time.Nanosecond)
	expectCheck(t, calls, true)
}

// TestUpdateCheckSkipsNonReleaseBuilds checks that a non-release build never
// checks and reports no available version.
func TestUpdateCheckSkipsNonReleaseBuilds(t *testing.T) {
	for _, version := range []string{"", "dev", "e2e", "0.1.0-SNAPSHOT"} {
		t.Run(version, func(t *testing.T) {
			latest, calls := updateCheckCalls()
			tb := newTestBroker(t, func(d *Deps) {
				d.Version = version
				d.LatestRelease = latest
			})
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			tb.b.startUpdateChecks(ctx)

			tb.clock.Advance(updateCheckInitialDelay())
			tb.clock.Advance(updateCheckDefaultInterval * 3)
			expectCheck(t, calls, false)

			resp, err := tb.client.UpdateStatus(context.Background(), &localpb.UpdateStatusRequest{})
			if err != nil {
				t.Fatalf("UpdateStatus: %v", err)
			}
			if got := resp.GetAvailableVersion(); got != "" {
				t.Errorf("AvailableVersion = %q, want empty", got)
			}
		})
	}
}

// TestUpdateCheckFailureOnlyLogsAtDebug checks that a failed check logs only
// at debug, writes no audit and leaves other calls working.
func TestUpdateCheckFailureOnlyLogsAtDebug(t *testing.T) {
	latest := func(context.Context, *http.Client, string) (update.Release, error) {
		return update.Release{}, errors.New("network is down")
	}
	tb := newTestBroker(t, func(d *Deps) {
		d.Version = "0.1.1"
		d.LatestRelease = latest
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tb.b.startUpdateChecks(ctx)

	tb.clock.Advance(updateCheckInitialDelay())

	found := false
	for _, rec := range tb.logLines(t) {
		if rec["event"] != EvUpdateCheckFailed {
			continue
		}
		found = true
		if got := rec["level"]; got != "DEBUG" {
			t.Errorf("update failure logged at level %v, want DEBUG", got)
		}
	}
	if !found {
		t.Fatalf("no %s record: %v", EvUpdateCheckFailed, tb.logLines(t))
	}
	if len(tb.cfg.audits) != 0 {
		t.Errorf("update failure wrote %d audit events, want none", len(tb.cfg.audits))
	}

	if _, err := tb.client.Status(context.Background(), &localpb.StatusRequest{}); err != nil {
		t.Fatalf("Status after a failed update check: %v", err)
	}
}

// TestUpdateCheckStoresOnlyNewerRelease checks that only a newer release is
// stored and reported.
func TestUpdateCheckStoresOnlyNewerRelease(t *testing.T) {
	cases := []struct {
		name      string
		candidate string
		want      string
	}{
		{"newer", "0.1.2", "0.1.2"},
		{"same", "0.1.1", ""},
		{"older", "0.1.0", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			latest := func(context.Context, *http.Client, string) (update.Release, error) {
				return update.Release{Version: tc.candidate, URL: "https://example.test/release"}, nil
			}
			tb := newTestBroker(t, func(d *Deps) {
				d.Version = "0.1.1"
				d.LatestRelease = latest
			})
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			tb.b.startUpdateChecks(ctx)
			tb.clock.Advance(updateCheckInitialDelay())

			resp, err := tb.client.UpdateStatus(context.Background(), &localpb.UpdateStatusRequest{})
			if err != nil {
				t.Fatalf("UpdateStatus: %v", err)
			}
			if got := resp.GetCurrentVersion(); got != "0.1.1" {
				t.Errorf("CurrentVersion = %q, want 0.1.1", got)
			}
			if got := resp.GetAvailableVersion(); got != tc.want {
				t.Errorf("AvailableVersion = %q, want %q", got, tc.want)
			}
			if tc.want == "" {
				if got := resp.GetReleaseUrl(); got != "" {
					t.Errorf("ReleaseUrl = %q, want empty", got)
				}
				return
			}
			if got := resp.GetReleaseUrl(); got != "https://example.test/release" {
				t.Errorf("ReleaseUrl = %q, want the release page", got)
			}

			status, err := tb.client.Status(context.Background(), &localpb.StatusRequest{})
			if err != nil {
				t.Fatalf("Status: %v", err)
			}
			if got := status.GetAvailableVersion(); got != tc.want {
				t.Errorf("Status.AvailableVersion = %q, want %q", got, tc.want)
			}
			if got := status.GetCurrentVersion(); got != "0.1.1" {
				t.Errorf("Status.CurrentVersion = %q, want 0.1.1", got)
			}
		})
	}
}

// TestUpdateStatusNeedsNoAgentIdentity checks that UpdateStatus answers without
// an agent identity.
func TestUpdateStatusNeedsNoAgentIdentity(t *testing.T) {
	tb := newTestBroker(t, func(d *Deps) {
		d.Version = "0.1.1"
		d.LatestRelease = func(context.Context, *http.Client, string) (update.Release, error) {
			return update.Release{Version: "0.1.2", URL: "https://example.test/release"}, nil
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tb.b.startUpdateChecks(ctx)
	tb.clock.Advance(updateCheckInitialDelay())

	resp, err := tb.client.UpdateStatus(context.Background(), &localpb.UpdateStatusRequest{})
	if err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}
	if got := resp.GetAvailableVersion(); got != "0.1.2" {
		t.Errorf("AvailableVersion = %q, want 0.1.2", got)
	}
}
