package config

import (
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// TestLowPrivilegedListenPort covers the kinds of value that are not a
// privileged listen port: a non-string value, an empty value, an unparseable
// value, and a port at the boundary.
func TestLowPrivilegedListenPort(t *testing.T) {
	cases := []struct {
		name string
		v    Value
		want bool
	}{
		{"not a string", intValue(80), false},
		{"empty", stringValue(""), false},
		{"unparseable", stringValue("not-a-host-port"), false},
		{"privileged", stringValue("127.0.0.1:80"), true},
		{"boundary", stringValue(":1024"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := lowPrivilegedListenPort(tc.v); got != tc.want {
				t.Fatalf("lowPrivilegedListenPort(%+v) = %v, want %v", tc.v, got, tc.want)
			}
		})
	}
}

// TestCheckLowPortAllowedReportsVerificationFailure covers a Set of a
// privileged port by root while the file permission cannot be read: the Set is
// refused and the error says the permission could not be verified.
func TestCheckLowPortAllowedReportsVerificationFailure(t *testing.T) {
	c, _ := newWatchCore(t, "broker:\n  listen: 127.0.0.1:7777\n", false, nil)
	c.mu.Lock()
	c.mode = ModeSystem
	c.configFileRootOnly = func(string) (bool, error) { return false, errors.New("stat failed") }
	c.mu.Unlock()

	_, err := c.setLocked(Actor{UIDKnown: true, UID: 0}, "broker.listen", stringValue(":80"))
	if !errors.Is(err, ErrLowPortDenied) {
		t.Fatalf("setLocked error = %v, want ErrLowPortDenied", err)
	}
	if !strings.Contains(err.Error(), "cannot verify") {
		t.Fatalf("setLocked error = %v, want it to explain the failed verification", err)
	}
}

// TestRejectReloadedLowPortReportsVerificationFailure covers a reload that
// lowers broker.listen into the privileged range while the file permission
// cannot be read: the old value is kept and the rejection is audited with the
// reason.
func TestRejectReloadedLowPortReportsVerificationFailure(t *testing.T) {
	c, path := newWatchCore(t, "broker:\n  listen: 127.0.0.1:7777\n", false, nil)
	var logs strings.Builder
	c.mu.Lock()
	c.mode = ModeSystem
	c.logger = slog.New(slog.NewTextHandler(&logs, nil))
	c.configFileRootOnly = func(string) (bool, error) { return false, errors.New("stat failed") }
	c.mu.Unlock()

	writeConfigBody(t, path, "broker:\n  listen: 127.0.0.1:80\n")
	c.rescanOnce()

	if got := c.values["broker.listen"].String; got != "127.0.0.1:7777" {
		t.Fatalf("broker.listen = %q after the rejected reload, want 127.0.0.1:7777", got)
	}
	events, err := c.readAudit(time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range events {
		if e.Event != EventConfigRejected || e.Key != "broker.listen" {
			continue
		}
		found = true
		if want := "the configuration file permission could not be verified"; e.Detail != want {
			t.Fatalf("config_rejected detail = %q, want %q", e.Detail, want)
		}
	}
	if !found {
		t.Fatal("the rejected reload was not audited")
	}
	if !strings.Contains(logs.String(), "cannot verify that the configuration file") {
		t.Fatalf("log = %q, want the verification failure", logs.String())
	}
}

// TestCheckLowPortAtStartupSkipsWhenTheRuleDoesNotApply covers the three
// exempt cases: a user Broker, a command-line listen override, and a port at or
// above 1024.
func TestCheckLowPortAtStartupSkipsWhenTheRuleDoesNotApply(t *testing.T) {
	t.Run("user mode", func(t *testing.T) {
		c, _ := newWatchCore(t, "broker:\n  listen: 127.0.0.1:80\n", false, nil)
		c.mu.Lock()
		c.mode = ModeUser
		c.mu.Unlock()
		if err := c.checkLowPortAtStartup(); err != nil {
			t.Fatalf("checkLowPortAtStartup in user mode = %v, want nil", err)
		}
	})

	t.Run("listen override", func(t *testing.T) {
		c, _ := newWatchCore(t, "broker:\n  listen: 127.0.0.1:80\n", false, nil)
		c.mu.Lock()
		c.mode = ModeSystem
		c.listenOverride = true
		c.mu.Unlock()
		if err := c.checkLowPortAtStartup(); err != nil {
			t.Fatalf("checkLowPortAtStartup with an override = %v, want nil", err)
		}
	})

	t.Run("port at or above 1024", func(t *testing.T) {
		c, _ := newWatchCore(t, "broker:\n  listen: 127.0.0.1:8080\n", false, nil)
		c.mu.Lock()
		c.mode = ModeSystem
		c.mu.Unlock()
		if err := c.checkLowPortAtStartup(); err != nil {
			t.Fatalf("checkLowPortAtStartup on :8080 = %v, want nil", err)
		}
	})
}

// TestCheckLowPortAtStartupReportsVerificationFailure covers a privileged port
// in system mode while the file permission cannot be read: the start check is
// refused as ErrLowPortDenied.
func TestCheckLowPortAtStartupReportsVerificationFailure(t *testing.T) {
	c, _ := newWatchCore(t, "broker:\n  listen: 127.0.0.1:80\n", false, nil)
	c.mu.Lock()
	c.mode = ModeSystem
	c.configFileRootOnly = func(string) (bool, error) { return false, errors.New("stat failed") }
	c.mu.Unlock()

	err := c.checkLowPortAtStartup()
	if !errors.Is(err, ErrLowPortDenied) {
		t.Fatalf("checkLowPortAtStartup error = %v, want ErrLowPortDenied", err)
	}
	if !strings.Contains(err.Error(), "cannot verify") {
		t.Fatalf("checkLowPortAtStartup error = %v, want it to explain the failed verification", err)
	}
}
