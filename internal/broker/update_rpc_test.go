package broker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/broker/update"
	"github.com/wangzw/peers/internal/config"
)

const capturedBrokerWait = runWaitBound

func fixedRelease(version string) func(context.Context, *http.Client, string) (update.Release, error) {
	return func(context.Context, *http.Client, string) (update.Release, error) {
		return update.Release{Version: version, URL: "https://example.test/release/" + version}, nil
	}
}

func applyUpdateAudits(tb *testBroker) []config.Event {
	return append([]config.Event(nil), tb.cfg.audits...)
}

// TestApplyUpdateNeedsServiceManager checks that without a service manager the
// update is refused before any lookup, download or shutdown.
func TestApplyUpdateNeedsServiceManager(t *testing.T) {
	lookedUp := false
	shutdown := false
	tb := newTestBroker(t, func(d *Deps) {
		d.Version = "0.1.1"
		d.ServiceManager = func() bool { return false }
		d.LatestRelease = func(context.Context, *http.Client, string) (update.Release, error) {
			lookedUp = true
			return update.Release{Version: "0.1.2"}, nil
		}
		d.RequestShutdown = func() { shutdown = true }
	})

	_, err := tb.b.ApplyUpdate(accessCallerContext(1000, "alice"), &localpb.ApplyUpdateRequest{})
	if err == nil {
		t.Fatal("ApplyUpdate succeeded without a service manager")
	}
	if code := accessErrorCode(t, err); code != localpb.ErrorCode_ERROR_CODE_NOT_UNDER_SERVICE_MANAGER {
		t.Errorf("code = %v, want NOT_UNDER_SERVICE_MANAGER", code)
	}
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Errorf("gRPC code = %v, want FailedPrecondition", got)
	}
	if !strings.Contains(err.Error(), "service install") {
		t.Errorf("message %q does not say how to install the service", err)
	}
	if lookedUp {
		t.Error("the Broker looked up a release although it cannot restart")
	}
	if shutdown {
		t.Error("the Broker asked to shut down although it cannot restart")
	}
	if audits := applyUpdateAudits(tb); len(audits) != 0 {
		t.Errorf("wrote %d audit events, want none", len(audits))
	}
}

// TestApplyUpdateAlreadyLatest checks that an up-to-date Broker answers an
// empty to_version, changes nothing and releases the update slot.
func TestApplyUpdateAlreadyLatest(t *testing.T) {
	applied := false
	shutdown := false
	tb := newTestBroker(t, func(d *Deps) {
		d.Version = "0.1.1"
		d.ServiceManager = func() bool { return true }
		d.LatestRelease = fixedRelease("0.1.1")
		d.InstallRelease = func(context.Context, update.ApplyOptions, string) error {
			applied = true
			return nil
		}
		d.RequestShutdown = func() { shutdown = true }
	})

	resp, err := tb.b.ApplyUpdate(accessCallerContext(1000, "alice"), &localpb.ApplyUpdateRequest{})
	if err != nil {
		t.Fatalf("ApplyUpdate: %v", err)
	}
	if resp.GetFromVersion() != "0.1.1" {
		t.Errorf("from_version = %q, want 0.1.1", resp.GetFromVersion())
	}
	if resp.GetToVersion() != "" {
		t.Errorf("to_version = %q, want empty", resp.GetToVersion())
	}
	if resp.GetRestarting() {
		t.Error("restarting = true for an up-to-date Broker")
	}
	if applied {
		t.Error("the Broker installed a release although it is current")
	}
	if audits := applyUpdateAudits(tb); len(audits) != 0 {
		t.Errorf("wrote %d audit events, want none", len(audits))
	}
	tb.clock.Advance(updateShutdownGrace)
	if shutdown {
		t.Error("an up-to-date Broker asked to shut down")
	}

	if _, err := tb.b.ApplyUpdate(accessCallerContext(1000, "alice"), &localpb.ApplyUpdateRequest{}); err != nil {
		t.Errorf("second ApplyUpdate: %v", err)
	}
}

// TestApplyUpdateApplyFailure checks that a failed install leaves the process
// running and writes update_failed.
func TestApplyUpdateApplyFailure(t *testing.T) {
	shutdown := false
	installs := 0
	tb := newTestBroker(t, func(d *Deps) {
		d.Version = "0.1.1"
		d.ServiceManager = func() bool { return true }
		d.LatestRelease = fixedRelease("0.1.2")
		d.InstallRelease = func(context.Context, update.ApplyOptions, string) error {
			installs++
			return errors.New("checksum mismatch for peers_0.1.2_linux_amd64.tar.gz")
		}
		d.RequestShutdown = func() { shutdown = true }
	})

	_, err := tb.b.ApplyUpdate(accessCallerContext(1000, "alice"), &localpb.ApplyUpdateRequest{})
	if err == nil {
		t.Fatal("ApplyUpdate succeeded although the install failed")
	}
	if got := status.Code(err); got != codes.Internal {
		t.Errorf("gRPC code = %v, want Internal", got)
	}
	if !strings.Contains(err.Error(), "installing 0.1.2 failed") {
		t.Errorf("message %q does not name the failed install", err)
	}

	audits := applyUpdateAudits(tb)
	if len(audits) != 1 {
		t.Fatalf("wrote %d audit events, want 1", len(audits))
	}
	failed := audits[0]
	if failed.Event != config.EventUpdateFailed {
		t.Errorf("event = %q, want update_failed", failed.Event)
	}
	if failed.Account != "alice" {
		t.Errorf("account = %q, want alice", failed.Account)
	}
	if !strings.Contains(failed.Detail, "checksum mismatch") {
		t.Errorf("detail = %q, want the checksum failure", failed.Detail)
	}
	if failed.Old != nil || failed.New != nil {
		t.Error("update_failed carries version values, want detail only")
	}

	tb.clock.Advance(updateShutdownGrace)
	if shutdown {
		t.Error("a failed install asked the Broker to shut down")
	}

	if _, err := tb.b.ApplyUpdate(accessCallerContext(1000, "alice"), &localpb.ApplyUpdateRequest{}); err == nil {
		t.Fatal("the retry succeeded, want the install failure again")
	}
	if installs != 2 {
		t.Errorf("installer called %d times, want 2", installs)
	}
}

// TestApplyUpdateSucceeds checks that a service-managed Broker installs the
// release, writes update_applied and shuts down only after the grace period.
func TestApplyUpdateSucceeds(t *testing.T) {
	var mu sync.Mutex
	var shutdowns int
	tb := newTestBroker(t, func(d *Deps) {
		d.Version = "0.1.1"
		d.ServiceManager = func() bool { return true }
		d.LatestRelease = fixedRelease("0.1.2")
		d.InstallRelease = func(_ context.Context, o update.ApplyOptions, _ string) error {
			if o.Version != "0.1.2" {
				t.Errorf("Apply version = %q, want 0.1.2", o.Version)
			}
			if o.GOOS != runtime.GOOS || o.GOARCH != runtime.GOARCH {
				t.Errorf("Apply platform = %s/%s, want %s/%s", o.GOOS, o.GOARCH, runtime.GOOS, runtime.GOARCH)
			}
			want, err := installedExecutable()
			if err != nil {
				t.Errorf("installedExecutable: %v", err)
			} else if o.Target != want {
				t.Errorf("Apply target = %q, want %q", o.Target, want)
			}
			return nil
		}
		d.RequestShutdown = func() {
			mu.Lock()
			shutdowns++
			mu.Unlock()
		}
	})

	resp, err := tb.b.ApplyUpdate(accessCallerContext(1000, "alice"), &localpb.ApplyUpdateRequest{})
	if err != nil {
		t.Fatalf("ApplyUpdate: %v", err)
	}
	if resp.GetFromVersion() != "0.1.1" || resp.GetToVersion() != "0.1.2" || !resp.GetRestarting() {
		t.Errorf("response = %+v, want from 0.1.1 to 0.1.2 restarting", resp)
	}

	audits := applyUpdateAudits(tb)
	if len(audits) != 1 {
		t.Fatalf("wrote %d audit events, want 1", len(audits))
	}
	applied := audits[0]
	if applied.Event != config.EventUpdateApplied {
		t.Errorf("event = %q, want update_applied", applied.Event)
	}
	if applied.Account != "alice" {
		t.Errorf("account = %q, want alice", applied.Account)
	}
	if applied.Old == nil || applied.Old.String != "0.1.1" {
		t.Errorf("old = %+v, want 0.1.1", applied.Old)
	}
	if applied.New == nil || applied.New.String != "0.1.2" {
		t.Errorf("new = %+v, want 0.1.2", applied.New)
	}

	if got := shutdownCount(&mu, &shutdowns); got != 0 {
		t.Fatalf("shutdowns = %d before the grace period, want 0", got)
	}
	tb.clock.Advance(updateShutdownGrace - time.Nanosecond)
	if got := shutdownCount(&mu, &shutdowns); got != 0 {
		t.Errorf("shutdowns = %d before the grace period ended, want 0", got)
	}
	tb.clock.Advance(time.Nanosecond)
	if got := shutdownCount(&mu, &shutdowns); got != 1 {
		t.Errorf("shutdowns = %d after the grace period, want 1", got)
	}

	_, err = tb.b.ApplyUpdate(accessCallerContext(1000, "alice"), &localpb.ApplyUpdateRequest{})
	if code := accessErrorCode(t, err); code != localpb.ErrorCode_ERROR_CODE_UPDATE_IN_PROGRESS {
		t.Errorf("second update code = %v, want UPDATE_IN_PROGRESS", code)
	}
}

func shutdownCount(mu *sync.Mutex, count *int) int {
	mu.Lock()
	defer mu.Unlock()
	return *count
}

// TestApplyUpdatePassesServiceLevel checks that the post-update step runs at
// the Broker's own service level.
func TestApplyUpdatePassesServiceLevel(t *testing.T) {
	for _, tc := range []struct {
		system bool
		want   string
	}{
		{system: false, want: "user"},
		{system: true, want: "system"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			var level string
			tb := newTestBroker(t, func(d *Deps) {
				d.Version = "0.1.1"
				d.System = tc.system
				d.ServiceManager = func() bool { return true }
				d.LatestRelease = fixedRelease("0.1.2")
				d.InstallRelease = func(_ context.Context, _ update.ApplyOptions, got string) error {
					level = got
					return nil
				}
				d.RequestShutdown = func() {}
			})

			if _, err := tb.b.ApplyUpdate(accessCallerContext(1000, "alice"), &localpb.ApplyUpdateRequest{}); err != nil {
				t.Fatalf("ApplyUpdate: %v", err)
			}
			if level != tc.want {
				t.Errorf("post-update level = %q, want %q", level, tc.want)
			}
		})
	}
}

// TestApplyUpdatePostUpdateFailure checks that a failed post-update step
// reaches the caller, writes update_failed and does not restart the Broker.
func TestApplyUpdatePostUpdateFailure(t *testing.T) {
	shutdown := false
	installs := 0
	tb := newTestBroker(t, func(d *Deps) {
		d.Version = "0.1.1"
		d.ServiceManager = func() bool { return true }
		d.LatestRelease = fixedRelease("0.1.2")
		d.InstallRelease = func(context.Context, update.ApplyOptions, string) error {
			installs++
			return errors.New("the post-update step failed: exit status 3; the previous binary was restored")
		}
		d.RequestShutdown = func() { shutdown = true }
	})

	_, err := tb.b.ApplyUpdate(accessCallerContext(1000, "alice"), &localpb.ApplyUpdateRequest{})
	if err == nil {
		t.Fatal("ApplyUpdate succeeded although the post-update step failed")
	}
	if !strings.Contains(err.Error(), "post-update") {
		t.Errorf("message %q does not name the post-update step", err)
	}
	if !strings.Contains(err.Error(), "previous binary was restored") {
		t.Errorf("message %q does not say the binary was restored", err)
	}

	audits := applyUpdateAudits(tb)
	if len(audits) != 1 {
		t.Fatalf("wrote %d audit events, want 1", len(audits))
	}
	if audits[0].Event != config.EventUpdateFailed {
		t.Errorf("event = %q, want update_failed", audits[0].Event)
	}
	if !strings.Contains(audits[0].Detail, "post-update") {
		t.Errorf("detail = %q, want the post-update failure", audits[0].Detail)
	}

	tb.clock.Advance(updateShutdownGrace)
	if shutdown {
		t.Error("a failed post-update step asked the Broker to shut down")
	}

	if _, err := tb.b.ApplyUpdate(accessCallerContext(1000, "alice"), &localpb.ApplyUpdateRequest{}); err == nil {
		t.Fatal("the retry succeeded, want the post-update failure again")
	}
	if installs != 2 {
		t.Errorf("installer called %d times, want 2", installs)
	}
}

// TestApplyUpdateConcurrentIsAborted checks that a second request during an
// install is aborted.
func TestApplyUpdateConcurrentIsAborted(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	tb := newTestBroker(t, func(d *Deps) {
		d.Version = "0.1.1"
		d.ServiceManager = func() bool { return true }
		d.LatestRelease = fixedRelease("0.1.2")
		d.InstallRelease = func(context.Context, update.ApplyOptions, string) error {
			close(entered)
			<-release
			return nil
		}
		d.RequestShutdown = func() {}
	})

	first := make(chan error, 1)
	go func() {
		_, err := tb.b.ApplyUpdate(accessCallerContext(1000, "alice"), &localpb.ApplyUpdateRequest{})
		first <- err
	}()
	<-entered

	_, err := tb.b.ApplyUpdate(accessCallerContext(1001, "bob"), &localpb.ApplyUpdateRequest{})
	if err == nil {
		t.Fatal("the concurrent update was allowed")
	}
	if code := accessErrorCode(t, err); code != localpb.ErrorCode_ERROR_CODE_UPDATE_IN_PROGRESS {
		t.Errorf("code = %v, want UPDATE_IN_PROGRESS", code)
	}
	if got := status.Code(err); got != codes.Aborted {
		t.Errorf("gRPC code = %v, want Aborted", got)
	}

	close(release)
	if err := <-first; err != nil {
		t.Fatalf("first ApplyUpdate: %v", err)
	}
	if audits := applyUpdateAudits(tb); len(audits) != 1 {
		t.Errorf("wrote %d audit events, want 1 applied record", len(audits))
	}
}

// TestRunRequestShutdownExitsNormally checks that the shutdown hook Run
// installs makes Run return nil.
func TestRunRequestShutdownExitsNormally(t *testing.T) {
	captured := make(chan *Broker, 1)
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.Components = []Component{&orderComponent{
			name:     "capture",
			order:    &orderLog{},
			captured: captured,
		}}
	})
	rt.start()
	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q, want %q", got, "ok\n")
	}

	var b *Broker
	select {
	case b = <-captured:
	case <-time.After(capturedBrokerWait):
		t.Fatal("the component never received the Broker")
	}
	if b.deps.RequestShutdown == nil {
		t.Fatal("Run installed no shutdown hook")
	}
	b.deps.RequestShutdown()

	if err := rt.waitErr(); err != nil {
		t.Fatalf("Run error = %v, want nil", err)
	}
}

// TestRunningUnderServiceManager checks that the answer depends on the service
// manager's main pid and not on inherited environment variables.
func TestRunningUnderServiceManager(t *testing.T) {
	const selfPID = 4242
	answer := func(pid int) func(bool) (int, bool) {
		return func(bool) (int, bool) { return pid, true }
	}
	noAnswer := func(bool) (int, bool) { return 0, false }

	cases := []struct {
		name    string
		goos    string
		system  bool
		env     map[string]string
		mainPID func(bool) (int, bool)
		want    bool
	}{
		{
			name: "systemd unit running this process", goos: "linux", system: true,
			env:     map[string]string{"INVOCATION_ID": "abc123"},
			mainPID: answer(selfPID), want: true,
		},
		{
			name: "launchd job running this process", goos: "darwin", system: false,
			env:     map[string]string{"XPC_SERVICE_NAME": "io.github.wangzw.peers"},
			mainPID: answer(selfPID), want: true,
		},
		{
			name: "INVOCATION_ID set but another process is the service", goos: "linux", system: true,
			env:     map[string]string{"INVOCATION_ID": "abc123"},
			mainPID: answer(selfPID + 1), want: false,
		},
		{
			name: "XPC_SERVICE_NAME=0 but another process is the service", goos: "darwin", system: false,
			env:     map[string]string{"XPC_SERVICE_NAME": "0"},
			mainPID: answer(selfPID + 1), want: false,
		},
		{
			name: "no service reports a pid", goos: "linux", system: true,
			env:     map[string]string{"INVOCATION_ID": "abc123"},
			mainPID: noAnswer, want: false,
		},
		{
			name: "launchd query fails", goos: "darwin", system: false,
			env:     map[string]string{"XPC_SERVICE_NAME": "0"},
			mainPID: noAnswer, want: false,
		},
		{
			name: "terminal with no service environment", goos: "linux", system: false,
			mainPID: noAnswer, want: false,
		},
		{
			name: "terminal with an inherited launchd value", goos: "darwin", system: false,
			env:     map[string]string{"XPC_SERVICE_NAME": "application.com.apple.Terminal.123"},
			mainPID: answer(selfPID + 1), want: false,
		},
		{
			name: "unknown platform", goos: "freebsd", system: false,
			env:     map[string]string{"INVOCATION_ID": "abc", "XPC_SERVICE_NAME": "x"},
			mainPID: answer(selfPID), want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for key, value := range tc.env {
				t.Setenv(key, value)
			}
			if got := runningUnderServiceManager(tc.goos, tc.system, selfPID, tc.mainPID); got != tc.want {
				t.Errorf("runningUnderServiceManager(%q, system=%v) = %v, want %v", tc.goos, tc.system, got, tc.want)
			}
		})
	}
}

// TestRunningUnderServiceManagerAsksTheBrokersLevel checks that the query goes
// to the service manager of the Broker's own level.
func TestRunningUnderServiceManagerAsksTheBrokersLevel(t *testing.T) {
	for _, system := range []bool{false, true} {
		t.Run(fmt.Sprintf("system=%v", system), func(t *testing.T) {
			var asked bool
			mainPID := func(level bool) (int, bool) {
				asked = level
				return 4242, true
			}
			if !runningUnderServiceManager("linux", system, 4242, mainPID) {
				t.Fatal("the service's own pid was not accepted")
			}
			if asked != system {
				t.Errorf("queried system=%v, want %v", asked, system)
			}
		})
	}
}
