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

// updateCheckCalls returns a LatestRelease fake that reports every lookup on
// the returned channel.
func updateCheckCalls() (func(context.Context, *http.Client, string) (update.Release, error), <-chan struct{}) {
	calls := make(chan struct{}, 16)
	return func(context.Context, *http.Client, string) (update.Release, error) {
		calls <- struct{}{}
		return update.Release{Version: "0.1.2", URL: "https://example.test/release/0.1.2"}, nil
	}, calls
}

// expectCheck fails unless the next check runs within no time, because the
// fake clock runs callbacks synchronously during Advance.
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

// TestUpdateCheckDefaultInitialDelayIsOneMinute pins the product default: a
// release build waits exactly one minute before its first check. It uses the
// literal minute instead of the updateCheckInitialDelay symbol, so a change to
// the test hook cannot silently move the release default.
func TestUpdateCheckDefaultInitialDelayIsOneMinute(t *testing.T) {
	latest, calls := updateCheckCalls()
	tb := newTestBroker(t, func(d *Deps) {
		d.Version = "0.1.1"
		d.LatestRelease = latest
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tb.b.startUpdateChecks(ctx)

	tb.clock.Advance(time.Minute - time.Nanosecond)
	expectCheck(t, calls, false)

	tb.clock.Advance(time.Nanosecond)
	expectCheck(t, calls, true)
}

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

	// Turning the check back on takes effect at the next period, without a
	// restart.
	tb.cfg.setValue("update.check", config.Value{Kind: config.KindBool, Bool: true})
	tb.clock.Advance(updateCheckDefaultInterval)
	expectCheck(t, calls, true)
}

// TestUpdateCheckDisabledClearsAvailableVersion covers the decision that
// turning update.check off clears the result already held: status and
// UpdateStatus report no available version at once, without advancing the
// clock to the next period or restarting the Broker.
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

// TestUpdateCheckConcurrentWithStatusReads runs the periodic check's writes
// while Status and UpdateStatus read the same Broker from other goroutines, so
// the race detector covers the shared update state.
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

	// The check already scheduled uses the old interval; the period after it
	// uses the new one.
	tb.cfg.setValue("update.check_interval", config.Value{Kind: config.KindDuration, Duration: 2 * time.Hour})
	tb.clock.Advance(updateCheckDefaultInterval)
	expectCheck(t, calls, true)

	tb.clock.Advance(2*time.Hour - time.Nanosecond)
	expectCheck(t, calls, false)
	tb.clock.Advance(time.Nanosecond)
	expectCheck(t, calls, true)
}

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

	// The failed check does not affect the other local calls.
	if _, err := tb.client.Status(context.Background(), &localpb.StatusRequest{}); err != nil {
		t.Fatalf("Status after a failed update check: %v", err)
	}
}

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

	// No agent registration and no configuration access precede this call.
	resp, err := tb.client.UpdateStatus(context.Background(), &localpb.UpdateStatusRequest{})
	if err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}
	if got := resp.GetAvailableVersion(); got != "0.1.2" {
		t.Errorf("AvailableVersion = %q, want 0.1.2", got)
	}
}
