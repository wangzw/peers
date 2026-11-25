package config

import (
	"crypto/sha256"
	"errors"
	"os"
	"testing"
)

// TestSetUsesTheBytesReloadRead checks that Set validates and writes back the
// bytes its reload read, not a second read of the file.
func TestSetUsesTheBytesReloadRead(t *testing.T) {
	c, path := newWatchCore(t, "broker:\n  public: false\n  alias: old\n", false, nil)

	external := "broker:\n  public: true\n  alias: old\n"
	realRead := c.readFile
	injected := false
	c.readFile = func(name string) ([]byte, error) {
		data, err := realRead(name)
		if err != nil || injected {
			return data, err
		}
		injected = true
		return data, os.WriteFile(name, []byte(external), 0o600)
	}

	before := countEvent(t, c, EventConfigFileChanged, "")
	c.mu.Lock()
	_, err := c.setLocked(Actor{}, "broker.alias", stringValue("new"))
	c.mu.Unlock()
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	if !injected {
		t.Fatal("the injected external write did not run; the read seam was not used")
	}

	disk, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	doc, parseErr := parseConfigFile(disk)
	if parseErr != nil {
		t.Fatalf("the written file does not parse: %v", parseErr)
	}

	c.mu.Lock()
	memPublic := c.values["broker.public"].Bool
	memAlias := c.values["broker.alias"].String
	hash := c.lastFileHash
	c.mu.Unlock()

	if memPublic != doc.values["broker.public"].Bool {
		t.Fatalf("broker.public = %v in memory but %v in the file; the file and memory diverged",
			memPublic, doc.values["broker.public"].Bool)
	}
	if memAlias != "new" || doc.values["broker.alias"].String != "new" {
		t.Fatalf("broker.alias = %q in memory and %q in the file, want new in both",
			memAlias, doc.values["broker.alias"].String)
	}
	if hash != sha256.Sum256(disk) {
		t.Error("lastFileHash does not describe the file on disk after Set")
	}

	c.rescanOnce()
	if after := countEvent(t, c, EventConfigFileChanged, ""); after != before {
		t.Errorf("a later reload adopted %d externally changed keys", after-before)
	}
	c.mu.Lock()
	after := c.values["broker.public"].Bool
	c.mu.Unlock()
	if after != memPublic {
		t.Errorf("broker.public = %v after a later reload, want %v", after, memPublic)
	}
}

// TestSetInitialReadFailureIsInternal checks that a configuration read failure
// at the start of Set is an internal error, not an invalid value.
func TestSetInitialReadFailureIsInternal(t *testing.T) {
	c, _ := newWatchCore(t, "broker:\n  public: false\n  alias: old\n", false, nil)

	injected := errors.New("injected read failure")
	c.readFile = func(string) ([]byte, error) { return nil, injected }

	c.mu.Lock()
	_, err := c.setLocked(Actor{}, "broker.public", boolValue(true))
	public := c.values["broker.public"].Bool
	c.mu.Unlock()

	if err == nil {
		t.Fatal("Set succeeded although the configuration file could not be read")
	}
	if errors.Is(err, ErrInvalidValue) {
		t.Fatalf("Set error = %v; a read failure must be internal, not invalid_argument", err)
	}
	if !errors.Is(err, injected) {
		t.Fatalf("Set error = %v; it does not carry the read failure", err)
	}
	if public {
		t.Error("broker.public = true in memory after a failed read")
	}
	if n := countEvent(t, c, EventConfigSet, ""); n != 0 {
		t.Errorf("config_set records after a failed read = %d, want 0", n)
	}
}
