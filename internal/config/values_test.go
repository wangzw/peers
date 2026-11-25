package config

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

var apiTestMu sync.Mutex

func installCoreForTest(t *testing.T, c *core) {
	t.Helper()
	apiTestMu.Lock()
	global.mu.Lock()
	old := global.core
	global.core = c
	global.mu.Unlock()
	apiTestMu.Unlock()
	t.Cleanup(func() {
		apiTestMu.Lock()
		global.mu.Lock()
		global.core = old
		global.mu.Unlock()
		apiTestMu.Unlock()
	})
}

func newValueCore(t *testing.T, body, override string) (*core, string) {
	t.Helper()
	home := t.TempDir()
	c := newCore()
	c.skipDataCheck = true
	c.noBackground = true
	c.checkRuntime = func() (string, error) { return filepath.Join(home, "run"), nil }
	c.homeDir = func() (string, error) { return home, nil }
	c.hostname = func() (string, error) { return "hostA", nil }
	c.accountName = func() (string, error) { return "tester", nil }
	c.now = func() time.Time { return time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC) }
	c.routesSrc = watchTestRoutes{}
	dir := filepath.Join(home, ".peers")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := c.load(ModeUser, override); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.close)
	return c, path
}

// TestREQBrokerConfig001_GetAllEffectiveValues checks that All returns one
// sorted entry per key table entry and Get refuses an unknown key.
func TestREQBrokerConfig001_GetAllEffectiveValues(t *testing.T) {
	c, _ := newValueCore(t, "broker:\n  alias: configured\n  public: true\n", "")
	installCoreForTest(t, c)

	if v, err := Get("broker.alias"); err != nil || v.String != "configured" {
		t.Fatalf("Get(alias) = %+v, %v", v, err)
	}
	all := All()
	if len(all) != len(keyTable) {
		t.Fatalf("All returned %d entries, want %d", len(all), len(keyTable))
	}
	for i := 1; i < len(all); i++ {
		if all[i-1].Key >= all[i].Key {
			t.Fatalf("All is not sorted: %q before %q", all[i-1].Key, all[i].Key)
		}
	}
	if _, err := Get("no.such.key"); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("unknown key error = %v", err)
	}
}

// TestREQBrokerConfig002_SetPersistsAndUpdates checks that Set updates memory,
// the file and the config_set audit log.
func TestREQBrokerConfig002_SetPersistsAndUpdates(t *testing.T) {
	c, path := newValueCore(t, "", "")
	installCoreForTest(t, c)

	if _, err := Set(Actor{Account: "ana", Agent: "hostA/ana"}, "broker.public", boolValue(true)); err != nil {
		t.Fatal(err)
	}
	if v, err := Get("broker.public"); err != nil || !v.Bool {
		t.Fatalf("Get(public) = %+v, %v", v, err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := parseConfigFile(data)
	if err != nil {
		t.Fatal(err)
	}
	if !doc.values["broker.public"].Bool {
		t.Fatalf("file does not contain the new value:\n%s", data)
	}
	if n := countEvent(t, c, EventConfigSet, "broker.public"); n != 1 {
		t.Fatalf("config_set records = %d, want 1", n)
	}
}

// TestREQBrokerConfig029_SetListenRejectedUnderOverride checks that Set refuses
// broker.listen while a startup override is in effect and changes neither the
// file nor the override.
func TestREQBrokerConfig029_SetListenRejectedUnderOverride(t *testing.T) {
	c, path := newValueCore(t, "broker:\n  listen: \":17443\"\n", ":17444")
	installCoreForTest(t, c)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Set(Actor{}, "broker.listen", stringValue(":17445")); !errors.Is(err, ErrListenOverridden) {
		t.Fatalf("Set(listen) error = %v, want ErrListenOverridden", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatalf("the file changed:\nbefore:\n%s\nafter:\n%s", before, after)
	}
	if v, _ := Get("broker.listen"); v.String != ":17444" {
		t.Fatalf("in-memory listen = %q, want the override", v.String)
	}
}

// TestREQBrokerConfig034_ValidatorFailureKeepsOld checks that a refused
// validation keeps the old value, the file and the audit log unchanged.
func TestREQBrokerConfig034_ValidatorFailureKeepsOld(t *testing.T) {
	c, path := newValueCore(t, "broker:\n  alias: old\n", "")
	installCoreForTest(t, c)
	rejected := errors.New("validator says no")
	Validate("broker.alias", func(old, new Value) error { return rejected })
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Set(Actor{}, "broker.alias", stringValue("new")); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("Set error = %v, want ErrInvalidValue", err)
	}
	if v, _ := Get("broker.alias"); v.String != "old" {
		t.Fatalf("alias = %q, want old", v.String)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("the file changed after a rejected Set")
	}
	if n := countEvent(t, c, EventConfigSet, ""); n != 0 {
		t.Fatalf("config_set records = %d, want 0", n)
	}
}

// TestREQBrokerPairing030_SecretNotReturned checks that Get and All report only
// whether a secret is set and Secret returns its value.
func TestREQBrokerPairing030_SecretNotReturned(t *testing.T) {
	c, _ := newValueCore(t, "pairing:\n  static_key: \"0123456789abcdef\"\n", "")
	installCoreForTest(t, c)

	v, err := Get("pairing.static_key")
	if err != nil {
		t.Fatal(err)
	}
	if v.Kind != KindSecret || !v.SecretIsSet || v.String != "" {
		t.Fatalf("Get(secret) = %+v, want only SecretIsSet", v)
	}
	for _, entry := range All() {
		if entry.Key == "pairing.static_key" && entry.Value.String != "" {
			t.Fatalf("All leaked the secret: %+v", entry.Value)
		}
	}
	if got := Secret("pairing.static_key"); got != "0123456789abcdef" {
		t.Fatalf("Secret = %q", got)
	}
	if got := Secret("broker.alias"); got != "" {
		t.Fatalf("Secret(non-secret) = %q, want empty", got)
	}
}

// TestConfigSubscribeReceivesSetNotification checks that Set notifies a
// subscriber of the key with the new value.
func TestConfigSubscribeReceivesSetNotification(t *testing.T) {
	c, _ := newValueCore(t, "", "")
	installCoreForTest(t, c)
	got := make(chan Value, 1)
	Subscribe("broker.public", func(old, new Value) { got <- new })
	if _, err := Set(Actor{}, "broker.public", boolValue(true)); err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-got:
		if !v.Bool {
			t.Fatalf("notification value = %+v", v)
		}
	case <-time.After(eventTimeout):
		t.Fatal("no notification arrived")
	}
}

// TestConfigCrossKeySetRejected checks that Set refuses link.reconnect_max
// below link.reconnect_min and keeps the old value.
func TestConfigCrossKeySetRejected(t *testing.T) {
	c, _ := newValueCore(t, "link:\n  reconnect_min: 1s\n  reconnect_max: 60s\n", "")
	installCoreForTest(t, c)
	if _, err := Set(Actor{}, "link.reconnect_max", durationValue(500*time.Millisecond)); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("Set error = %v, want ErrInvalidValue", err)
	}
	if v, _ := Get("link.reconnect_max"); v.Duration != time.Minute {
		t.Fatalf("reconnect_max = %s, want 1m", v.Duration)
	}
}

// TestConfigLoadOnlyOnce checks that a second Load returns ErrAlreadyLoaded.
func TestConfigLoadOnlyOnce(t *testing.T) {
	c, _ := newValueCore(t, "", "")
	installCoreForTest(t, c)
	if err := Load(ModeUser, ""); !errors.Is(err, ErrAlreadyLoaded) {
		t.Fatalf("second Load error = %v, want ErrAlreadyLoaded", err)
	}
}

// TestConfigSetFailsWhenFileCannotBeParsed checks that Set fails and does not
// overwrite an unparseable file.
func TestConfigSetFailsWhenFileCannotBeParsed(t *testing.T) {
	c, path := newValueCore(t, "", "")
	installCoreForTest(t, c)
	if err := os.WriteFile(path, []byte("broker: [\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Set(Actor{}, "broker.public", boolValue(true)); err == nil {
		t.Fatal("Set succeeded over an unparseable file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "broker: [\n" {
		t.Fatalf("Set overwrote the broken file:\n%s", data)
	}
}

// TestBrokerOfflineKey checks that broker.offline defaults to false and can be
// set through Set and through the configuration file.
func TestBrokerOfflineKey(t *testing.T) {
	t.Run("default and Set", func(t *testing.T) {
		c, path := newValueCore(t, "", "")
		installCoreForTest(t, c)

		v, err := Get("broker.offline")
		if err != nil {
			t.Fatalf("Get(broker.offline): %v", err)
		}
		if v.Kind != KindBool || v.Bool {
			t.Fatalf("default broker.offline = %+v, want a false bool", v)
		}

		if _, err := Set(Actor{Account: "ana", Agent: "hostA/ana"}, "broker.offline", boolValue(true)); err != nil {
			t.Fatalf("Set(broker.offline): %v", err)
		}
		if v, err := Get("broker.offline"); err != nil || v.Kind != KindBool || !v.Bool {
			t.Fatalf("Get(broker.offline) after Set = %+v, %v, want true", v, err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		doc, err := parseConfigFile(data)
		if err != nil {
			t.Fatal(err)
		}
		if got, ok := doc.values["broker.offline"]; !ok || !got.Bool {
			t.Fatalf("file does not hold broker.offline: true:\n%s", data)
		}
		if n := countEvent(t, c, EventConfigSet, "broker.offline"); n != 1 {
			t.Fatalf("config_set records for broker.offline = %d, want 1", n)
		}
	})

	t.Run("from the configuration file", func(t *testing.T) {
		c, _ := newValueCore(t, "broker:\n  offline: true\n", "")
		installCoreForTest(t, c)

		if v, err := Get("broker.offline"); err != nil || v.Kind != KindBool || !v.Bool {
			t.Fatalf("Get(broker.offline) = %+v, %v, want true from the file", v, err)
		}
	})
}
