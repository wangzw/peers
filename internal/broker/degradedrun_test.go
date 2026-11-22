package broker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
)

func diagnosticsLoadErr(rt *runTest) error {
	audit := filepath.Join(rt.dir, "audit.log")
	return &config.DataUnavailableError{
		Paths: config.PathSet{
			RuntimeDir: rt.dir,
			ConfigFile: filepath.Join(rt.dir, "config.yaml"),
			DataDir:    filepath.Join(rt.dir, "data"),
			AuditLog:   audit,
		},
		LocalSocketMode: "0666",
		Err:             fmt.Errorf("config: cannot open audit log %s: permission denied", audit),
	}
}

func waitForAnswer(t *testing.T, sock string) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("unix", sock, 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("the local socket %s did not answer", sock)
}

func localClient(t *testing.T, sock string) localpb.LocalClient {
	t.Helper()
	cc, err := grpc.NewClient("unix://"+sock, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return localpb.NewLocalClient(cc)
}

func statusAt(t *testing.T, sock string) (*localpb.StatusResponse, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	return localClient(t, sock).Status(ctx, &localpb.StatusRequest{})
}

func waitForStatus(t *testing.T, sock string, check func(*localpb.StatusResponse) bool) *localpb.StatusResponse {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	var last error
	for time.Now().Before(deadline) {
		resp, err := statusAt(t, sock)
		if err == nil && check(resp) {
			return resp
		}
		last = err
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("status did not reach the expected state (last error: %v)", last)
	return nil
}

func TestRunDataUnavailableEntersDiagnosticsAndRecovers(t *testing.T) {
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.AllowDegraded = true
		rt.opts.Version = "1.2.3"
	})
	rt.cfg.setLoadErr(diagnosticsLoadErr(rt))
	rt.start()

	sock := rt.socketPath()
	waitForAnswer(t, sock)

	resp := waitForStatus(t, sock, func(s *localpb.StatusResponse) bool {
		return s.GetDegraded() != nil
	})
	if resp.GetDegraded().GetLevel() != localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY {
		t.Fatalf("level = %v, want diagnostics_only", resp.GetDegraded().GetLevel())
	}
	if resp.GetCurrentVersion() != "1.2.3" {
		t.Errorf("current version = %q", resp.GetCurrentVersion())
	}
	if len(resp.GetAgents()) != 0 || len(resp.GetPeers()) != 0 {
		t.Errorf("diagnostics Status returned agents or peers: %+v", resp)
	}

	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	_, err := localClient(t, sock).ListAgents(ctx, &localpb.ListAgentsRequest{})
	cancel()
	if detail := errorDetailOf(t, err); detail.GetCode() != localpb.ErrorCode_ERROR_CODE_BROKER_DEGRADED {
		t.Errorf("ListAgents code = %v, want broker_degraded", detail.GetCode())
	}

	// Fix the configuration and drive the minute retry by hand.
	rt.cfg.setLoadErr(nil)
	rt.ticks <- time.Now()
	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q, want ok", got)
	}
	waitForStatus(t, sock, func(s *localpb.StatusResponse) bool { return s.GetDegraded() == nil })

	ctx, cancel = context.WithTimeout(context.Background(), eventTimeout)
	_, err = localClient(t, sock).ListBrokers(ctx, &localpb.ListBrokersRequest{})
	cancel()
	if err != nil {
		t.Fatalf("ListBrokers after recovery: %v", err)
	}

	rt.cancel()
	if err := rt.waitErr(); err != nil {
		t.Fatalf("Run after cancel: %v", err)
	}
	lock, err := AcquireAccountLock(rt.cfg.AccountLock())
	if err != nil {
		t.Fatalf("the lock was not released: %v", err)
	}
	lock.Release()
}

func linkAssembler(runner LinkRunner) Assembler {
	return &fakeAssembler{name: "link", fn: func(_ context.Context, _ ConfigSource, _ *Deps, asm *Assembly) ([]Component, error) {
		asm.Runner = runner
		return nil, nil
	}}
}

func TestRunDiagnosticsToLocalOnlyToNormal(t *testing.T) {
	runner := newFakeRunner(&orderLog{})
	runner.listenErr = errors.New("address already in use")
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.AllowDegraded = true
		rt.opts.Assemblers = []Assembler{linkAssembler(runner)}
	})
	rt.cfg.setValue("broker.listen", config.Value{Kind: config.KindString, String: "127.0.0.1:17443"})
	rt.cfg.setLoadErr(diagnosticsLoadErr(rt))
	rt.start()

	sock := rt.socketPath()
	waitForAnswer(t, sock)
	waitForStatus(t, sock, func(s *localpb.StatusResponse) bool { return s.GetDegraded() != nil })

	rt.cfg.setLoadErr(nil)
	rt.ticks <- time.Now()
	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q, want ok", got)
	}
	waitForStatus(t, sock, func(s *localpb.StatusResponse) bool {
		return s.GetDegraded() != nil &&
			s.GetDegraded().GetLevel() == localpb.DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY
	})

	waitSubscribed(t, rt.cfg, "broker.listen")
	runner.listenErr = nil
	rt.cfg.setValue("broker.listen", config.Value{Kind: config.KindString, String: "127.0.0.1:18000"})
	waitForStatus(t, sock, func(s *localpb.StatusResponse) bool { return s.GetDegraded() == nil })

	rt.cancel()
	if err := rt.waitErr(); err != nil {
		t.Fatalf("Run after cancel: %v", err)
	}
}

func TestRunListenFailureEntersLocalOnly(t *testing.T) {
	runner := newFakeRunner(&orderLog{})
	runner.listenErr = errors.New("address already in use")
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.AllowDegraded = true
		rt.opts.Assemblers = []Assembler{linkAssembler(runner)}
	})
	rt.cfg.setValue("broker.listen", config.Value{Kind: config.KindString, String: "127.0.0.1:17443"})
	rt.start()

	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q, want ok on a local-only start", got)
	}
	sock := rt.socketPath()
	resp := waitForStatus(t, sock, func(s *localpb.StatusResponse) bool { return s.GetDegraded() != nil })
	if resp.GetDegraded().GetLevel() != localpb.DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY {
		t.Fatalf("level = %v, want local_only", resp.GetDegraded().GetLevel())
	}

	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	_, err := localClient(t, sock).CreatePairingKey(ctx, &localpb.CreatePairingKeyRequest{})
	cancel()
	if detail := errorDetailOf(t, err); detail.GetCode() != localpb.ErrorCode_ERROR_CODE_BROKER_DEGRADED {
		t.Errorf("CreatePairingKey code = %v, want broker_degraded", detail.GetCode())
	}

	waitSubscribed(t, rt.cfg, "broker.listen")
	runner.listenErr = nil
	rt.cfg.setValue("broker.listen", config.Value{Kind: config.KindString, String: "127.0.0.1:18000"})
	waitForStatus(t, sock, func(s *localpb.StatusResponse) bool { return s.GetDegraded() == nil })

	rt.cancel()
	if err := rt.waitErr(); err != nil {
		t.Fatalf("Run after cancel: %v", err)
	}
}

func TestRunDiagnosticsRetryKeepsRunningOnBadConfig(t *testing.T) {
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.AllowDegraded = true
	})
	rt.cfg.setLoadErr(diagnosticsLoadErr(rt))
	rt.start()

	sock := rt.socketPath()
	waitForAnswer(t, sock)
	waitForStatus(t, sock, func(s *localpb.StatusResponse) bool {
		return s.GetDegraded() != nil && strings.Contains(s.GetDegraded().GetReason(), "audit")
	})

	// While already diagnostics only, a configuration that becomes invalid
	// updates the reason instead of exiting.
	rt.cfg.setLoadErr(fmt.Errorf("%w: broker.listen: bad", config.ErrInvalidValue))
	rt.ticks <- time.Now()
	waitForStatus(t, sock, func(s *localpb.StatusResponse) bool {
		return s.GetDegraded() != nil && strings.Contains(s.GetDegraded().GetReason(), "broker.listen")
	})
	select {
	case err := <-rt.done:
		t.Fatalf("Run exited on an invalid retry configuration: %v", err)
	default:
	}

	rt.cfg.setLoadErr(nil)
	rt.ticks <- time.Now()
	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q, want ok", got)
	}
	waitForStatus(t, sock, func(s *localpb.StatusResponse) bool { return s.GetDegraded() == nil })

	rt.cancel()
	if err := rt.waitErr(); err != nil {
		t.Fatalf("Run after cancel: %v", err)
	}
}

func TestRunDataUnavailableWithoutAllowDegradedExits(t *testing.T) {
	rt := newRunTest(t)
	rt.cfg.setLoadErr(diagnosticsLoadErr(rt))
	rt.start()
	err := rt.waitErr()
	if !errors.Is(err, config.ErrDataUnavailable) {
		t.Fatalf("Run error = %v, want the data-unavailable class", err)
	}
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Run error = %v, want the configuration-failure shape", err)
	}
	lock, lockErr := AcquireAccountLock(rt.cfg.AccountLock())
	if lockErr != nil {
		t.Fatalf("the lock was not released: %v", lockErr)
	}
	lock.Release()
}

func TestRunAssemblerDataUnavailableDegrades(t *testing.T) {
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.AllowDegraded = true
		rt.opts.Assemblers = []Assembler{&fakeAssembler{name: "pairing", fn: func(_ context.Context, _ ConfigSource, _ *Deps, _ *Assembly) ([]Component, error) {
			return nil, wrapDataUnavailable(dataKindIdentity, errors.New("open identity.key: permission denied"))
		}}}
	})
	rt.start()
	sock := rt.socketPath()
	waitForAnswer(t, sock)
	waitForStatus(t, sock, func(s *localpb.StatusResponse) bool {
		return s.GetDegraded() != nil &&
			s.GetDegraded().GetLevel() == localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY &&
			s.GetDegraded().GetStep() == "identity"
	})
	rt.cancel()
	if err := rt.waitErr(); err != nil {
		t.Fatalf("Run after cancel: %v", err)
	}
}

func TestRunAssemblerNonDataFailureStaysFatal(t *testing.T) {
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.AllowDegraded = true
		rt.opts.Assemblers = []Assembler{&fakeAssembler{name: "pairing", fn: func(_ context.Context, _ ConfigSource, _ *Deps, _ *Assembly) ([]Component, error) {
			return nil, errors.New("wiring exploded")
		}}}
	})
	rt.start()
	err := rt.waitErr()
	if err == nil || !strings.Contains(err.Error(), "wiring exploded") {
		t.Fatalf("Run error = %v, want the assembly failure", err)
	}
	if errors.Is(err, config.ErrDataUnavailable) {
		t.Fatalf("a non-data failure was classified as data unavailable: %v", err)
	}
	lock, lockErr := AcquireAccountLock(rt.cfg.AccountLock())
	if lockErr != nil {
		t.Fatalf("the lock was not released: %v", lockErr)
	}
	lock.Release()
}

func TestRunBeforeLockFailureExitsWithAllowDegraded(t *testing.T) {
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.AllowDegraded = true
		rt.opts.Deps.InSession = func() bool { return true }
	})
	rt.start()
	if err := rt.waitErr(); !errors.Is(err, ErrInSession) {
		t.Fatalf("Run error = %v, want ErrInSession", err)
	}
}

func TestRunLowPortIsListenFailure(t *testing.T) {
	runner := newFakeRunner(&orderLog{})
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.Assemblers = []Assembler{linkAssembler(runner)}
	})
	rt.cfg.setValue("broker.listen", config.Value{Kind: config.KindString, String: "127.0.0.1:80"})
	rt.cfg.lowPortErr = config.ErrLowPortDenied
	rt.start()
	err := rt.waitErr()
	if !errors.Is(err, ErrListenUnavailable) {
		t.Fatalf("Run error = %v, want ErrListenUnavailable", err)
	}
	if !errors.Is(err, config.ErrLowPortDenied) {
		t.Fatalf("Run error = %v, want it to carry ErrLowPortDenied", err)
	}
	lock, lockErr := AcquireAccountLock(rt.cfg.AccountLock())
	if lockErr != nil {
		t.Fatalf("the lock was not released: %v", lockErr)
	}
	lock.Release()
}

func TestRunLowPortDegradesLocallyAndRecovers(t *testing.T) {
	runner := newFakeRunner(&orderLog{})
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.AllowDegraded = true
		rt.opts.Assemblers = []Assembler{linkAssembler(runner)}
	})
	rt.cfg.setValue("broker.listen", config.Value{Kind: config.KindString, String: "127.0.0.1:80"})
	rt.cfg.lowPortErr = config.ErrLowPortDenied
	rt.start()

	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q, want ok", got)
	}
	sock := rt.socketPath()
	waitForStatus(t, sock, func(s *localpb.StatusResponse) bool {
		return s.GetDegraded() != nil &&
			s.GetDegraded().GetLevel() == localpb.DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY
	})

	waitSubscribed(t, rt.cfg, "broker.listen")
	rt.cfg.lowPortErr = nil
	rt.cfg.setValue("broker.listen", config.Value{Kind: config.KindString, String: "127.0.0.1:18000"})
	waitForStatus(t, sock, func(s *localpb.StatusResponse) bool { return s.GetDegraded() == nil })

	rt.cancel()
	if err := rt.waitErr(); err != nil {
		t.Fatalf("Run after cancel: %v", err)
	}
}

// assertFatalStartup checks that Run exited with the configuration-failure
// shape, bound no local socket and released the account lock.
func assertFatalStartup(t *testing.T, rt *runTest, want error) {
	t.Helper()
	err := rt.waitErr()
	if err == nil {
		t.Fatal("Run succeeded, want a fatal startup failure")
	}
	if want != nil && !errors.Is(err, want) {
		t.Fatalf("Run error = %v, want %v", err, want)
	}
	if _, statErr := os.Stat(rt.socketPath()); !os.IsNotExist(statErr) {
		t.Errorf("broker.sock exists, want no socket (stat error %v)", statErr)
	}
	lock, lockErr := AcquireAccountLock(rt.cfg.AccountLock())
	if lockErr != nil {
		t.Fatalf("the lock was not released: %v", lockErr)
	}
	lock.Release()
}

// TestRunConfigFailuresStayFatalWithAllowDegraded pins the startup
// classification: --allow-degraded covers only the data-directory class and a
// listen bind failure. A configuration that cannot be parsed or validated
// still exits, because the diagnostics interface must not be driven by
// untrusted values.
func TestRunConfigFailuresStayFatalWithAllowDegraded(t *testing.T) {
	t.Run("parse error", func(t *testing.T) {
		rt := newRunTest(t, func(rt *runTest) { rt.opts.AllowDegraded = true })
		rt.cfg.setLoadErr(errors.New("config file parse error: yaml: line 1: did not find expected node content"))
		rt.start()
		assertFatalStartup(t, rt, ErrInvalidConfig)
	})
	t.Run("invalid value", func(t *testing.T) {
		rt := newRunTest(t, func(rt *runTest) { rt.opts.AllowDegraded = true })
		rt.cfg.setLoadErr(fmt.Errorf("%w: log.level: unknown level", config.ErrInvalidValue))
		rt.start()
		assertFatalStartup(t, rt, ErrInvalidConfig)
	})
}

// TestRunUnknownSocketGroupStaysFatalWithAllowDegraded covers the system
// form's check after a successful Load: a group that does not exist is a
// configuration failure, not a degraded state.
func TestRunUnknownSocketGroupStaysFatalWithAllowDegraded(t *testing.T) {
	name := "peers-no-such-group-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.AllowDegraded = true
		rt.opts.System = true
		rt.opts.Deps.Geteuid = func() int { return 0 }
		rt.cfg.setValue("broker.local_socket_group", config.Value{Kind: config.KindString, String: name})
		rt.cfg.setValue("broker.local_socket_mode", config.Value{Kind: config.KindString, String: "0660"})
	})
	rt.start()
	err := rt.waitErr()
	if err == nil || !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Run error = %v, want ErrInvalidConfig", err)
	}
	if !strings.Contains(err.Error(), "broker.local_socket_group") {
		t.Fatalf("Run error = %v, want it to name broker.local_socket_group", err)
	}
	if _, statErr := os.Stat(rt.socketPath()); !os.IsNotExist(statErr) {
		t.Errorf("broker.sock exists, want no socket (stat error %v)", statErr)
	}
	lock, lockErr := AcquireAccountLock(rt.cfg.AccountLock())
	if lockErr != nil {
		t.Fatalf("the lock was not released: %v", lockErr)
	}
	lock.Release()
}

// TestRunDegradedReasonHidesConfigValues checks that a diagnostics-only retry
// that fails on an invalid configuration value reports the key and the error
// class, never the value itself.
func TestRunDegradedReasonHidesConfigValues(t *testing.T) {
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.AllowDegraded = true
	})
	rt.cfg.setLoadErr(diagnosticsLoadErr(rt))
	rt.start()

	sock := rt.socketPath()
	waitForAnswer(t, sock)
	waitForStatus(t, sock, func(s *localpb.StatusResponse) bool { return s.GetDegraded() != nil })

	rt.cfg.setLoadErr(fmt.Errorf("%w: broker.advertise: host %q contains an invalid character",
		config.ErrInvalidValue, "bad!host"))
	rt.ticks <- time.Now()
	resp := waitForStatus(t, sock, func(s *localpb.StatusResponse) bool {
		return s.GetDegraded() != nil && strings.Contains(s.GetDegraded().GetReason(), "broker.advertise")
	})
	if reason := resp.GetDegraded().GetReason(); strings.Contains(reason, "bad!host") {
		t.Fatalf("degraded reason %q contains the configured value", reason)
	}

	rt.cancel()
	if err := rt.waitErr(); err != nil {
		t.Fatalf("Run after cancel: %v", err)
	}
}

// recordRunner is a LinkRunner whose Listen blocks when the test asks and
// records every address in order.
type recordRunner struct {
	mu     sync.Mutex
	calls  []string
	errs   map[string]error
	blocks map[string]chan struct{}
}

func newRecordRunner() *recordRunner {
	return &recordRunner{errs: make(map[string]error), blocks: make(map[string]chan struct{})}
}

func (r *recordRunner) Listen(address string) error {
	r.mu.Lock()
	r.calls = append(r.calls, address)
	err := r.errs[address]
	block := r.blocks[address]
	r.mu.Unlock()
	if block != nil {
		<-block
	}
	return err
}

func (r *recordRunner) Start() {}
func (r *recordRunner) Close() {}

func (r *recordRunner) setErr(address string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs[address] = err
}

func (r *recordRunner) blockOn(address string) chan struct{} {
	ch := make(chan struct{})
	r.mu.Lock()
	defer r.mu.Unlock()
	r.blocks[address] = ch
	return ch
}

func (r *recordRunner) addresses() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

func (r *recordRunner) waitCall(t *testing.T, n int, what string) string {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for time.Now().Before(deadline) {
		addresses := r.addresses()
		if len(addresses) > n {
			return addresses[n]
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s (calls: %v)", what, r.addresses())
	return ""
}

// TestRunLocalOnlyRecoveryReconcilesAddressChange binds A successfully while a
// broker.listen change to B arrives. The change is delivered while the config
// component's rebind is suppressed, so recovery must re-read the current value
// and bind B before it lifts suppression.
func TestRunLocalOnlyRecoveryReconcilesAddressChange(t *testing.T) {
	const (
		addrA = "127.0.0.1:17443"
		addrB = "127.0.0.1:18000"
	)
	runner := newRecordRunner()
	runner.setErr(addrA, errors.New("address already in use"))
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.AllowDegraded = true
		rt.opts.Assemblers = []Assembler{linkAssembler(runner)}
	})
	rt.cfg.setValue("broker.listen", config.Value{Kind: config.KindString, String: addrA})
	rt.start()

	sock := rt.socketPath()
	waitForStatus(t, sock, func(s *localpb.StatusResponse) bool {
		return s.GetDegraded() != nil && strings.Contains(s.GetDegraded().GetReason(), addrA)
	})

	// The next attempt at A is held open; B is written while it runs.
	release := runner.blockOn(addrA)
	runner.setErr(addrA, nil)
	if got := runner.waitCall(t, 1, "the retry at the original address"); got != addrA {
		t.Fatalf("retry address = %q, want %q", got, addrA)
	}
	rt.cfg.setValue("broker.listen", config.Value{Kind: config.KindString, String: addrB})
	close(release)

	waitForStatus(t, sock, func(s *localpb.StatusResponse) bool { return s.GetDegraded() == nil })
	addresses := runner.addresses()
	if len(addresses) == 0 || addresses[len(addresses)-1] != addrB {
		t.Fatalf("bound addresses = %v, want the last bind at %s", addresses, addrB)
	}

	rt.cancel()
	if err := rt.waitErr(); err != nil {
		t.Fatalf("Run after cancel: %v", err)
	}
}

// TestRunLocalOnlyRecoveryCatchesChangeAfterUnsuppress delivers a
// broker.listen change in the window between lifting suppression and the final
// read. The value is current but no subscriber forwarded it, so only reading
// after unsuppression catches it.
func TestRunLocalOnlyRecoveryCatchesChangeAfterUnsuppress(t *testing.T) {
	const (
		addrA = "127.0.0.1:17443"
		addrB = "127.0.0.1:18000"
	)
	runner := newRecordRunner()
	runner.setErr(addrA, errors.New("address already in use"))
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.AllowDegraded = true
		rt.opts.Assemblers = []Assembler{linkAssembler(runner)}
		rt.opts.afterUnsuppress = func() {
			rt.cfg.setValueQuietly("broker.listen", config.Value{Kind: config.KindString, String: addrB})
		}
	})
	rt.cfg.setValue("broker.listen", config.Value{Kind: config.KindString, String: addrA})
	rt.start()

	sock := rt.socketPath()
	waitForStatus(t, sock, func(s *localpb.StatusResponse) bool {
		return s.GetDegraded() != nil && strings.Contains(s.GetDegraded().GetReason(), addrA)
	})

	runner.setErr(addrA, nil)
	rt.cfg.fireKey("broker.listen",
		config.Value{Kind: config.KindString, String: addrA},
		config.Value{Kind: config.KindString, String: addrA})

	waitForStatus(t, sock, func(s *localpb.StatusResponse) bool { return s.GetDegraded() == nil })
	addresses := runner.addresses()
	if len(addresses) == 0 || addresses[len(addresses)-1] != addrB {
		t.Fatalf("bound addresses = %v, want the quiet change at %s to be bound", addresses, addrB)
	}

	rt.cancel()
	if err := rt.waitErr(); err != nil {
		t.Fatalf("Run after cancel: %v", err)
	}
}

// TestRunRecoveryFailureNamesTheNewAddress checks that a rebind failure during
// recovery reports the address that failed, not the one bound a moment ago,
// and says the old listener is still serving.
func TestRunRecoveryFailureNamesTheNewAddress(t *testing.T) {
	const (
		addrA = "127.0.0.1:17443"
		addrB = "127.0.0.1:18000"
	)
	runner := newRecordRunner()
	runner.setErr(addrA, errors.New("address already in use"))
	failures := make(chan [2]string, 4)
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.AllowDegraded = true
		rt.opts.Assemblers = []Assembler{linkAssembler(runner)}
		rt.opts.afterUnsuppress = func() {
			rt.cfg.setValueQuietly("broker.listen", config.Value{Kind: config.KindString, String: addrB})
		}
		rt.opts.afterListenFailure = func(addr, bound string) {
			failures <- [2]string{addr, bound}
		}
	})
	rt.cfg.setValue("broker.listen", config.Value{Kind: config.KindString, String: addrA})
	rt.start()

	sock := rt.socketPath()
	waitForStatus(t, sock, func(s *localpb.StatusResponse) bool {
		return s.GetDegraded() != nil && strings.Contains(s.GetDegraded().GetReason(), addrA)
	})

	runner.setErr(addrA, nil)
	runner.setErr(addrB, errors.New("permission denied"))
	rt.cfg.fireKey("broker.listen",
		config.Value{Kind: config.KindString, String: addrA},
		config.Value{Kind: config.KindString, String: addrA})

	// Read the retry's report directly instead of polling: the immediate
	// failure must name the new address and the listener still holding the old
	// one.
	deadline := time.After(eventTimeout)
	for {
		select {
		case got := <-failures:
			// Skip failures before a bind succeeded; the first one with a
			// bound address is the reconcile that must name the new address.
			if got[1] != addrA {
				continue
			}
			if got[0] != addrB {
				t.Fatalf("failure reported address %q, want the new address %q", got[0], addrB)
			}
		case <-deadline:
			t.Fatal("the reconcile failure was not reported")
		}
		break
	}
	resp := waitForStatus(t, sock, func(s *localpb.StatusResponse) bool {
		return s.GetDegraded() != nil &&
			strings.Contains(s.GetDegraded().GetReason(), addrB) &&
			strings.Contains(s.GetDegraded().GetReason(), "permission denied")
	})
	reason := resp.GetDegraded().GetReason()
	if strings.Contains(reason, "listen address "+addrA+" is unavailable") {
		t.Fatalf("reason %q still names the old address as unavailable", reason)
	}
	if !strings.Contains(reason, "previous address "+addrA+" is still bound") {
		t.Fatalf("reason %q does not say the old listener still serves", reason)
	}

	rt.cancel()
	if err := rt.waitErr(); err != nil {
		t.Fatalf("Run after cancel: %v", err)
	}
}

// TestRunLocalOnlyReasonFollowsRetry refreshes the reported reason when a later
// bind attempt fails for a different reason or address.
func TestRunLocalOnlyReasonFollowsRetry(t *testing.T) {
	const (
		addrA = "127.0.0.1:17443"
		addrB = "127.0.0.1:18000"
	)
	runner := newRecordRunner()
	runner.setErr(addrA, errors.New("address already in use"))
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.AllowDegraded = true
		rt.opts.Assemblers = []Assembler{linkAssembler(runner)}
	})
	rt.cfg.setValue("broker.listen", config.Value{Kind: config.KindString, String: addrA})
	rt.start()

	sock := rt.socketPath()
	waitForStatus(t, sock, func(s *localpb.StatusResponse) bool {
		return s.GetDegraded() != nil && strings.Contains(s.GetDegraded().GetReason(), addrA)
	})

	runner.setErr(addrB, errors.New("permission denied"))
	rt.cfg.setValue("broker.listen", config.Value{Kind: config.KindString, String: addrB})
	waitForStatus(t, sock, func(s *localpb.StatusResponse) bool {
		return s.GetDegraded() != nil &&
			strings.Contains(s.GetDegraded().GetReason(), addrB) &&
			strings.Contains(s.GetDegraded().GetReason(), "permission denied")
	})

	rt.cancel()
	if err := rt.waitErr(); err != nil {
		t.Fatalf("Run after cancel: %v", err)
	}
}

// TestRunComponentStartFailureStopsAllComponents checks that a failing Start
// also stops the failing component and every component not started yet.
func TestRunComponentStartFailureStopsAllComponents(t *testing.T) {
	order := &orderLog{}
	one := &orderComponent{name: "one", order: order}
	two := &orderComponent{name: "two", order: order, startErr: errors.New("boom")}
	three := &orderComponent{name: "three", order: order}
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.Components = []Component{one, two, three}
	})
	rt.start()

	err := rt.waitErr()
	if err == nil || !strings.Contains(err.Error(), "two") {
		t.Fatalf("Run error = %v, want the failing component", err)
	}
	entries := order.list()
	for _, want := range []string{"one.Stop", "two.Stop", "three.Stop"} {
		if !slices.Contains(entries, want) {
			t.Fatalf("startup order %v does not contain %s", entries, want)
		}
	}
}

// TestRunDiagnosticsRebindsRemovedSocket checks the periodic socket presence
// check while diagnostics only, matching the full service.
func TestRunDiagnosticsRebindsRemovedSocket(t *testing.T) {
	rt := newRunTest(t, func(rt *runTest) { rt.opts.AllowDegraded = true })
	rt.cfg.setLoadErr(diagnosticsLoadErr(rt))
	rt.start()

	sock := rt.socketPath()
	waitForAnswer(t, sock)
	if err := os.Remove(sock); err != nil {
		t.Fatalf("removing the socket: %v", err)
	}
	rt.ticks <- time.Now()
	waitForAnswer(t, sock)

	resp := waitForStatus(t, sock, func(s *localpb.StatusResponse) bool { return s.GetDegraded() != nil })
	if resp.GetDegraded().GetLevel() != localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY {
		t.Fatalf("level = %v, want diagnostics_only", resp.GetDegraded().GetLevel())
	}

	rt.cancel()
	if err := rt.waitErr(); err != nil {
		t.Fatalf("Run after cancel: %v", err)
	}
}

// TestRunDiagnosticsRebindsSocketRemovedAfterListen checks that entering
// diagnostics only recovers when the socket file is removed between ListenUnix
// and the chmod that follows it.
func TestRunDiagnosticsRebindsSocketRemovedAfterListen(t *testing.T) {
	rt := newRunTest(t, func(rt *runTest) { rt.opts.AllowDegraded = true })
	rt.cfg.setLoadErr(diagnosticsLoadErr(rt))
	sock := rt.socketPath()
	var removed atomic.Bool
	defer swapAfterListen(func() {
		if !removed.Swap(true) {
			_ = os.Remove(sock)
		}
	})()
	rt.start()

	waitForLocalSocketOrRunError(t, rt, sock)
	if !removed.Load() {
		t.Fatal("the test seam never removed the socket")
	}
	mode, gid := socketPerms(t, sock)
	if mode != 0o600 {
		t.Fatalf("socket mode = %04o, want 0600", mode)
	}
	if gid != uint32(os.Getegid()) {
		t.Fatalf("socket group = %d, want the binding process's %d", gid, os.Getegid())
	}
	resp := waitForStatus(t, sock, func(s *localpb.StatusResponse) bool { return s.GetDegraded() != nil })
	if resp.GetDegraded().GetLevel() != localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY {
		t.Fatalf("level = %v, want diagnostics_only", resp.GetDegraded().GetLevel())
	}

	rt.cancel()
	if err := rt.waitErr(); err != nil {
		t.Fatalf("Run after cancel: %v", err)
	}
}

// TestRunDiagnosticsReappliesSocketPermissions checks that changing the
// configured socket mode takes effect on the next retry, while the socket
// already serves diagnostics only.
func TestRunDiagnosticsReappliesSocketPermissions(t *testing.T) {
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.AllowDegraded = true
		rt.opts.System = true
		rt.opts.Deps.Geteuid = func() int { return 0 }
	})
	rt.cfg.setLoadErr(diagnosticsLoadErr(rt))
	rt.start()

	sock := rt.socketPath()
	waitForAnswer(t, sock)
	if mode, _ := socketPerms(t, sock); mode != 0o666 {
		t.Fatalf("socket mode = %04o, want the configured 0666", mode)
	}

	de := diagnosticsLoadErr(rt).(*config.DataUnavailableError)
	de.LocalSocketMode = "0640"
	rt.cfg.setLoadErr(de)
	rt.ticks <- time.Now()
	waitForReg(t, "the socket mode to follow the retry configuration", func() bool {
		mode, _ := socketPerms(t, sock)
		return mode == 0o640
	})

	rt.cancel()
	if err := rt.waitErr(); err != nil {
		t.Fatalf("Run after cancel: %v", err)
	}
}

// TestRunDiagnosticsReasonChangeLogsUpdate checks that only the first entry
// writes degraded_entered; a later reason writes degraded_reason_changed.
func TestRunDiagnosticsReasonChangeLogsUpdate(t *testing.T) {
	rt := newRunTest(t, func(rt *runTest) { rt.opts.AllowDegraded = true })
	rt.cfg.setLoadErr(diagnosticsLoadErr(rt))
	rt.start()

	sock := rt.socketPath()
	waitForAnswer(t, sock)
	waitForStatus(t, sock, func(s *localpb.StatusResponse) bool {
		return s.GetDegraded() != nil && strings.Contains(s.GetDegraded().GetReason(), "audit")
	})

	rt.cfg.setLoadErr(fmt.Errorf("%w: broker.advertise: invalid", config.ErrInvalidValue))
	rt.ticks <- time.Now()
	waitForStatus(t, sock, func(s *localpb.StatusResponse) bool {
		return s.GetDegraded() != nil && strings.Contains(s.GetDegraded().GetReason(), "broker.advertise")
	})

	// The status reports the new reason before the log entry is written, so
	// wait for the entry instead of reading the buffer once.
	waitForReg(t, "the degraded_reason_changed entry", func() bool {
		return strings.Contains(rt.logs.String(), EvDegradedReasonChanged)
	})

	logs := rt.logs.String()
	if got := strings.Count(logs, EvDegradedEntered); got != 1 {
		t.Errorf("degraded_entered appears %d times, want 1", got)
	}
	if got := strings.Count(logs, EvDegradedReasonChanged); got != 1 {
		t.Errorf("degraded_reason_changed appears %d times, want 1", got)
	}
	if got := strings.Count(logs, EvStartupFailed); got != 1 {
		t.Errorf("startup_failed appears %d times, want only the first entry", got)
	}

	rt.cancel()
	if err := rt.waitErr(); err != nil {
		t.Fatalf("Run after cancel: %v", err)
	}
}

// TestRunDiagnosticsToLocalOnlyLogsFinalLevel checks that leaving diagnostics
// for local only does not write a fieldless degraded_left; the entry records
// the level that remains.
func TestRunDiagnosticsToLocalOnlyLogsFinalLevel(t *testing.T) {
	runner := newRecordRunner()
	runner.setErr("127.0.0.1:17443", errors.New("address already in use"))
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.AllowDegraded = true
		rt.opts.Assemblers = []Assembler{linkAssembler(runner)}
	})
	rt.cfg.setValue("broker.listen", config.Value{Kind: config.KindString, String: "127.0.0.1:17443"})
	rt.cfg.setLoadErr(diagnosticsLoadErr(rt))
	rt.start()

	sock := rt.socketPath()
	waitForAnswer(t, sock)
	waitForStatus(t, sock, func(s *localpb.StatusResponse) bool { return s.GetDegraded() != nil })

	rt.cfg.setLoadErr(nil)
	rt.ticks <- time.Now()
	waitForStatus(t, sock, func(s *localpb.StatusResponse) bool {
		return s.GetDegraded() != nil &&
			s.GetDegraded().GetLevel() == localpb.DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY
	})

	// The status reaches local only before the second entry is written, so wait
	// for it instead of reading the buffer once.
	waitForReg(t, "the local-only degraded_entered entry", func() bool {
		return strings.Count(rt.logs.String(), EvDegradedEntered) >= 2
	})

	logs := rt.logs.String()
	if strings.Contains(logs, EvDegradedLeft) {
		t.Errorf("degraded_left was written while local-only remains:\n%s", logs)
	}
	if got := strings.Count(logs, EvDegradedEntered); got != 2 {
		t.Errorf("degraded_entered appears %d times, want diagnostics then local only", got)
	}

	rt.cancel()
	if err := rt.waitErr(); err != nil {
		t.Fatalf("Run after cancel: %v", err)
	}
}

func waitSubscribed(t *testing.T, cfg *fakeConfig, key string) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for time.Now().Before(deadline) {
		cfg.mu.Lock()
		n := 0
		for _, k := range cfg.subscribedKeys {
			if k == key {
				n++
			}
		}
		cfg.mu.Unlock()
		if n > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no subscriber was registered for %s", key)
}
