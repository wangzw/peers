package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestDataUnavailableErrorWithoutCause checks that DataUnavailableError
// without a cause still reports and unwraps to ErrDataUnavailable.
func TestDataUnavailableErrorWithoutCause(t *testing.T) {
	e := &DataUnavailableError{}
	if got := e.Error(); got != ErrDataUnavailable.Error() {
		t.Fatalf("Error() = %q, want %q", got, ErrDataUnavailable.Error())
	}
	if !errors.Is(e, ErrDataUnavailable) {
		t.Fatal("errors.Is(DataUnavailableError{}, ErrDataUnavailable) = false")
	}
	if causes := e.Unwrap(); len(causes) != 1 {
		t.Fatalf("Unwrap() has %d causes, want 1", len(causes))
	}
}

// TestLoadReportsRecordedAccountInitFailure checks that Load reports an
// account-initialization failure recorded earlier and consumes it.
func TestLoadReportsRecordedAccountInitFailure(t *testing.T) {
	installCoreForTest(t, nil)
	boom := errors.New("account runtime directory unavailable")
	setAccountInitErr(boom)
	t.Cleanup(func() { _ = takeAccountInitErr() })

	if err := Load(ModeUser, ""); !errors.Is(err, boom) {
		t.Fatalf("Load() error = %v, want the recorded account-init error", err)
	}
	if c := currentCore(); c != nil {
		t.Fatal("Load published a core after the account-init failure")
	}
	if pending := takeAccountInitErr(); pending != nil {
		t.Fatalf("the account-init error was not consumed, still %v", pending)
	}
}

// TestLoadClosesTheCoreWhenLoadFails checks that Load closes the core it built
// and publishes nothing when load fails.
func TestLoadClosesTheCoreWhenLoadFails(t *testing.T) {
	installCoreForTest(t, nil)
	base := t.TempDir()
	oldRoot := runtimeRoot
	oldHome := homeDirSource
	runtimeRoot = func(int) string { return filepath.Join(base, "runtime") }
	homeDirSource = func() (string, error) { return "", errors.New("no home directory") }
	t.Cleanup(func() {
		runtimeRoot = oldRoot
		homeDirSource = oldHome
	})
	if err := takeAccountInitErr(); err != nil {
		t.Fatalf("a previous test left an account-init error: %v", err)
	}

	err := Load(ModeUser, "")
	if err == nil || !strings.Contains(err.Error(), "cannot determine the home directory") {
		t.Fatalf("Load() error = %v, want the path-resolution failure", err)
	}
	if c := currentCore(); c != nil {
		t.Fatal("Load published a core after load failed")
	}
}

// TestLoadPublishesCoreAndCloseReleasesIt checks that Load publishes the core
// it builds and Close releases it.
func TestLoadPublishesCoreAndCloseReleasesIt(t *testing.T) {
	installCoreForTest(t, nil)
	t.Cleanup(Close)

	base := t.TempDir()
	home := filepath.Join(base, "home")
	if err := os.MkdirAll(home, 0o700); err != nil {
		t.Fatal(err)
	}
	oldRoot := runtimeRoot
	oldHome := homeDirSource
	runtimeRoot = func(int) string { return filepath.Join(base, "runtime") }
	homeDirSource = func() (string, error) { return home, nil }
	t.Cleanup(func() {
		runtimeRoot = oldRoot
		homeDirSource = oldHome
	})
	if err := takeAccountInitErr(); err != nil {
		t.Fatalf("a previous test left an account-init error: %v", err)
	}

	if err := Load(ModeUser, ""); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if v, err := Get("broker.listen"); err != nil || v.String != ":17443" {
		t.Fatalf("Get(broker.listen) = %+v, %v, want the default", v, err)
	}
	wantConfig := filepath.Join(home, ".peers", "config.yaml")
	if got := Paths().ConfigFile; got != wantConfig {
		t.Fatalf("Paths().ConfigFile = %q, want %q", got, wantConfig)
	}

	Close()
	if c := currentCore(); c != nil {
		t.Fatal("Close did not release the loaded core")
	}
	if Paths() != (PathSet{}) {
		t.Fatalf("Paths() after Close = %+v, want the zero PathSet", Paths())
	}
	if _, err := Get("broker.listen"); !errors.Is(err, ErrNotLoaded) {
		t.Fatalf("Get after Close error = %v, want ErrNotLoaded", err)
	}
}

// TestSetBeforeLoadReportsNotLoaded checks that Set before Load reports
// ErrNotLoaded.
func TestSetBeforeLoadReportsNotLoaded(t *testing.T) {
	installCoreForTest(t, nil)
	if _, err := Set(Actor{}, "broker.public", boolValue(true)); !errors.Is(err, ErrNotLoaded) {
		t.Fatalf("Set before Load error = %v, want ErrNotLoaded", err)
	}
}

// panicText runs fn, requires it to panic, and returns the panic text so a test
// can tell the guarded branches apart.
func panicText(t *testing.T, fn func()) string {
	t.Helper()
	var text string
	func() {
		defer func() {
			if r := recover(); r != nil {
				if s, ok := r.(string); ok {
					text = s
				} else {
					text = fmt.Sprint(r)
				}
			}
		}()
		fn()
	}()
	if text == "" {
		t.Fatal("expected a panic, got none")
	}
	return text
}

// TestRegistrationEntryPointsGuardNilAndUnknownKey checks that each
// registration entry point refuses a nil callback and an unknown key with its
// own panic message.
func TestRegistrationEntryPointsGuardNilAndUnknownKey(t *testing.T) {
	c := newCore()
	c.logger = testLogger()
	installCoreForTest(t, c)

	cases := []struct {
		name string
		fn   func()
		want string
	}{
		{"SubscribeAdvertised nil", func() { SubscribeAdvertised(nil) }, "nil callback"},
		{"Validate nil", func() { Validate("broker.public", nil) }, "nil callback"},
		{"Subscribe nil", func() { Subscribe("broker.public", nil) }, "nil callback"},
		{"Validate unknown key", func() { Validate("no.such.key", func(Value, Value) error { return nil }) }, "unknown key"},
		{"Subscribe unknown key", func() { Subscribe("no.such.key", func(Value, Value) {}) }, "unknown key"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := panicText(t, tc.fn); !strings.Contains(got, tc.want) {
				t.Fatalf("panic = %q, want it to contain %q", got, tc.want)
			}
		})
	}
}

// closeErrorListener stands in for a bound listener whose Close fails, so the
// probe's close-error branch is reachable without a real socket.
type closeErrorListener struct{ err error }

func (l *closeErrorListener) Accept() (net.Conn, error) { return nil, l.err }
func (l *closeErrorListener) Close() error              { return l.err }
func (l *closeErrorListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
}

// TestProbeListenReportsCloseFailure checks that ProbeListen reports a failed
// close separately from a failed bind and keeps the close error reachable with
// errors.Is.
func TestProbeListenReportsCloseFailure(t *testing.T) {
	boom := errors.New("close failed")
	prev := probeListenNet
	probeListenNet = func(string, string) (net.Listener, error) {
		return &closeErrorListener{err: boom}, nil
	}
	t.Cleanup(func() { probeListenNet = prev })

	err := ProbeListen("127.0.0.1:17443")
	if err == nil || !strings.Contains(err.Error(), "cannot close the probe listener") {
		t.Fatalf("ProbeListen() error = %v, want the close-failure message", err)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("ProbeListen() error = %v, want it to wrap the close error", err)
	}
}

// TestListenFromFileReportsReadAndTypeErrors checks that ListenFromFile
// reports an unreadable file and a wrongly typed broker.listen as its own
// errors.
func TestListenFromFileReportsReadAndTypeErrors(t *testing.T) {
	if _, err := ListenFromFile(t.TempDir()); err == nil || !strings.Contains(err.Error(), "cannot read") {
		t.Fatalf("ListenFromFile(directory) error = %v, want a read error", err)
	}

	path := filepath.Join(t.TempDir(), "config.yaml")
	body := "broker:\n  listen:\n    - a\n    - b\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := ListenFromFile(path)
	if !errors.Is(err, ErrInvalidValue) || !strings.Contains(err.Error(), "broker.listen") {
		t.Fatalf("ListenFromFile(wrong type) error = %v, want an ErrInvalidValue naming broker.listen", err)
	}
}

// TestAuditWritesThroughLoadedCore checks that Audit writes through the loaded
// core's log and ReadAudit reads the record back.
func TestAuditWritesThroughLoadedCore(t *testing.T) {
	c, _ := newValueCore(t, "", "")
	installCoreForTest(t, c)

	Audit(Event{Event: EventConfigSet, Key: "broker.public", Actor: "tester", Account: "tester"})

	events, err := ReadAudit(time.Time{})
	if err != nil {
		t.Fatalf("ReadAudit: %v", err)
	}
	for _, e := range events {
		if e.Event == EventConfigSet && e.Key == "broker.public" && e.Actor == "tester" {
			return
		}
	}
	t.Fatalf("ReadAudit returned %d events without the config_set record just audited", len(events))
}

// TestAppendAuditReportsOpenFailure checks that AppendAudit reports a log it
// cannot open.
func TestAppendAuditReportsOpenFailure(t *testing.T) {
	if err := AppendAudit("", Event{Event: EventLinkUp}); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("AppendAudit(empty path) error = %v, want the empty-path error", err)
	}
	if err := AppendAudit(t.TempDir(), Event{Event: EventLinkUp}); err == nil || !strings.Contains(err.Error(), "cannot open the audit log") {
		t.Fatalf("AppendAudit(directory) error = %v, want the open-failure error", err)
	}
}
