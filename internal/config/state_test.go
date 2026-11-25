package config

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
)

// TestSetListenKeepsDerivedAlias checks that a listen change is accepted
// without consulting the broker.alias validators and leaves the derived alias
// alone.
func TestSetListenKeepsDerivedAlias(t *testing.T) {
	c, path := newValueCore(t, "broker:\n  public: false\n", "")
	installCoreForTest(t, c)

	rejected := errors.New("alias equals a peer's effective alias")
	checks := 0
	Validate("broker.alias", func(old, new Value) error {
		checks++
		return rejected
	})

	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Set(Actor{}, "broker.listen", stringValue("127.0.0.1:27443")); err != nil {
		t.Fatalf("Set(broker.listen) error = %v, want nil", err)
	}
	if checks != 0 {
		t.Fatalf("the broker.alias validator ran %d times on a listen change, want 0", checks)
	}
	if v, _ := Get("broker.listen"); v.String != "127.0.0.1:27443" {
		t.Fatalf("broker.listen = %q after Set, want the new value", v.String)
	}
	if v, _ := Get("broker.alias"); v.String != "hostA.tester" {
		t.Fatalf("broker.alias = %q after Set, want the unchanged derived value", v.String)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) == string(after) {
		t.Fatal("the configuration file did not change after an accepted Set")
	}
}

// TestReloadListenKeepsDerivedAlias checks that a reloaded listen address leaves
// the derived alias alone without consulting its validators.
func TestReloadListenKeepsDerivedAlias(t *testing.T) {
	c, path := newValueCore(t, "broker:\n  listen: \":17443\"\n", "")
	installCoreForTest(t, c)
	var logs bytes.Buffer
	c.logger = slog.New(slog.NewTextHandler(&logs, nil))

	rejected := errors.New("alias equals a peer's effective alias")
	checks := 0
	Validate("broker.alias", func(old, new Value) error {
		checks++
		return rejected
	})

	writeConfigBody(t, path, "broker:\n  listen: \"127.0.0.1:27443\"\n")
	c.rescanOnce()

	if got := aliasOf(t, c); got != "hostA.tester" {
		t.Fatalf("broker.alias = %q after a listen reload, want the unchanged derived value hostA.tester", got)
	}
	if v, _ := Get("broker.listen"); v.String != "127.0.0.1:27443" {
		t.Fatalf("broker.listen = %q after the reload, want the new value", v.String)
	}
	if checks != 0 {
		t.Fatalf("the broker.alias validator ran %d times on a listen reload, want 0", checks)
	}
	if n := countEvent(t, c, EventConfigFileChanged, "broker.alias"); n != 0 {
		t.Fatalf("config_file_changed records for broker.alias = %d, want 0", n)
	}
}

// TestReloadRejectedDerivedAliasKeepsExplicitAlias checks that a rejected
// derived alias keeps the old explicit alias and the explicit flag.
func TestReloadRejectedDerivedAliasKeepsExplicitAlias(t *testing.T) {
	c, path := newValueCore(t, "broker:\n  listen: \":17443\"\n  alias: \"hostB\"\n", "")
	installCoreForTest(t, c)
	var logs bytes.Buffer
	c.logger = slog.New(slog.NewTextHandler(&logs, nil))

	rejected := errors.New("alias equals a peer's effective alias")
	Validate("broker.alias", func(old, new Value) error { return rejected })

	writeConfigBody(t, path, "broker:\n  listen: \"127.0.0.1:27443\"\n")
	c.rescanOnce()

	if got := aliasOf(t, c); got != "hostB" {
		t.Fatalf("broker.alias = %q after a rejected reload, want the old explicit value hostB", got)
	}
	if v, _ := Get("broker.listen"); v.String != ":17443" {
		t.Fatalf("broker.listen = %q after a rejected reload, want the old value", v.String)
	}
	if !c.explicitAlias {
		t.Fatal("explicitAlias = false after a rejected derivation, want the old explicit alias to stay in effect")
	}
	if !strings.Contains(logs.String(), "validator rejected the derived alias") {
		t.Fatalf("the rejected derived alias was not logged; log = %q", logs.String())
	}
	if n := countEvent(t, c, EventConfigFileChanged, "broker.alias"); n != 0 {
		t.Fatalf("config_file_changed records for broker.alias = %d, want 0", n)
	}
}

// TestReloadExplicitAliasValidatesConfiguredValue checks that a configured alias
// is validated and a rejection rolls back the alias and the listen group.
func TestReloadExplicitAliasValidatesConfiguredValue(t *testing.T) {
	c, path := newValueCore(t, "broker:\n  listen: \":17443\"\n", "")
	installCoreForTest(t, c)
	var logs bytes.Buffer
	c.logger = slog.New(slog.NewTextHandler(&logs, nil))

	rejected := errors.New("alias equals a peer's effective alias")
	var checkedOld, checkedNew string
	Validate("broker.alias", func(old, new Value) error {
		checkedOld, checkedNew = old.String, new.String
		if new.String == "hostB" {
			return rejected
		}
		return nil
	})

	writeConfigBody(t, path, "broker:\n  listen: \"127.0.0.1:27443\"\n  alias: \"hostB\"\n")
	c.rescanOnce()

	if checkedOld != "hostA.tester" || checkedNew != "hostB" {
		t.Fatalf("validator saw old = %q, new = %q; want the configured change hostA.tester -> hostB",
			checkedOld, checkedNew)
	}
	if got := aliasOf(t, c); got != "hostA.tester" {
		t.Fatalf("broker.alias = %q after a rejected reload, want the old derived value hostA.tester", got)
	}
	// Keeping the old alias rolls the listen address group back with it.
	if v, _ := Get("broker.listen"); v.String != ":17443" {
		t.Fatalf("broker.listen = %q after a rejected reload, want the old value", v.String)
	}
	if !strings.Contains(logs.String(), "validator rejected the reloaded alias") {
		t.Fatalf("the rejected configured alias was not logged; log = %q", logs.String())
	}
	if n := countEvent(t, c, EventConfigFileChanged, "broker.alias"); n != 0 {
		t.Fatalf("config_file_changed records for broker.alias = %d, want 0", n)
	}
}
