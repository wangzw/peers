package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
)

// TestSetLowPortRefusalDoesNotAskToOpenTheFile covers the system Broker's Set
// refusal for a privileged port. The remedy must not tell the operator to widen
// the file so every account can read it: the startup path already asks for
// `chmod g-w`, and the Set path must give the same advice.
func TestSetLowPortRefusalDoesNotAskToOpenTheFile(t *testing.T) {
	c, path := systemWatchCore(t, "broker:\n  listen: 127.0.0.1:7777\n", false)

	c.mu.Lock()
	_, err := c.setLocked(Actor{UIDKnown: true, UID: 0}, "broker.listen", stringValue(":80"))
	c.mu.Unlock()

	if !errors.Is(err, ErrLowPortDenied) {
		t.Fatalf("setLocked error = %v, want ErrLowPortDenied", err)
	}
	want := fmt.Sprintf("%s: cannot set broker.listen to :80 while %s is writable by an account other than root; run `chmod g-w %s` (and remove any ACL that grants write), or use a port at or above 1024", ErrLowPortDenied, path, path)
	if got := err.Error(); got != want {
		t.Errorf("Set refusal = %q, want %q", got, want)
	}
}

// TestSetSystemCreatesConfigFileWith0600 covers the mode of a configuration
// file the system Broker creates itself. It matches the installation's
// conservative mode instead of leaving a statically keyed file readable by
// every account on the host.
func TestSetSystemCreatesConfigFileWith0600(t *testing.T) {
	c, path := newValueCore(t, "", "")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	c.mode = ModeSystem
	installCoreForTest(t, c)

	if _, err := Set(Actor{}, "broker.public", boolValue(true)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("system Broker created the config file with mode %04o, want 0600", perm)
	}
}

// TestStartupLowPortRefusalNamesTheConfiguredValue covers the startup refusal:
// it shows the broker.listen value found in the file, so root can check what an
// account able to write the file may have put there before tightening it.
func TestStartupLowPortRefusalNamesTheConfiguredValue(t *testing.T) {
	home := t.TempDir()
	c, path := systemLoadCore(t, home, false)
	if err := os.WriteFile(path, []byte("broker:\n  listen: 127.0.0.1:80\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := c.load(ModeSystem, ""); err != nil {
		t.Fatalf("load: %v", err)
	}

	err := c.checkLowPortAtStartup()
	if !errors.Is(err, ErrLowPortDenied) {
		t.Fatalf("checkLowPortAtStartup error = %v, want ErrLowPortDenied", err)
	}
	want := fmt.Sprintf("%s: broker.listen in %s is 127.0.0.1:80, a port below 1024, and the file is writable by an account other than root, which may have written this value; check that it is the one you want, then run `chmod g-w %s` (and remove any ACL that grants write) or use a port at or above 1024", ErrLowPortDenied, path, path)
	if got := err.Error(); got != want {
		t.Errorf("startup refusal = %q, want %q", got, want)
	}
}

// TestReloadLowPortWarningNamesTheConfiguredValue covers the hot-reload
// rejection: the warning shows the value the file now holds, like the startup
// refusal, so an operator reading the log sees what was written.
func TestReloadLowPortWarningNamesTheConfiguredValue(t *testing.T) {
	c, path := systemWatchCore(t, "broker:\n  listen: 127.0.0.1:7777\n", false)
	var logs strings.Builder
	c.mu.Lock()
	c.logger = slog.New(slog.NewTextHandler(&logs, nil))
	c.mu.Unlock()

	writeConfigBody(t, path, "broker:\n  listen: 127.0.0.1:80\n")
	c.rescanOnce()

	if got := c.values["broker.listen"].String; got != "127.0.0.1:7777" {
		t.Fatalf("broker.listen = %q after the rejected reload, want 127.0.0.1:7777", got)
	}
	want := fmt.Sprintf("config: keeping the current listen address; broker.listen in %s is 127.0.0.1:80, a port below 1024, and the file is writable by an account other than root, which may have written this value; check that it is the one you want, then run `chmod g-w %s` (and remove any ACL that grants write) or use a port at or above 1024", path, path)
	if text := logs.String(); !strings.Contains(text, want) {
		t.Errorf("reload warning %q does not contain %q", text, want)
	}
}
