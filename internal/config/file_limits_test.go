package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// newLimitTestCore builds a core that resolves its configuration file under a
// temporary home but does not load it, so a test can call load itself and
// observe the error.
func newLimitTestCore(t *testing.T) (*core, string) {
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
	t.Cleanup(c.close)
	return c, filepath.Join(dir, "config.yaml")
}

func writeLimitFile(t *testing.T, path string, body []byte) {
	t.Helper()
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestConfigFileRejectsAnchorsAndAliases checks that a configuration file
// which defines an anchor or uses an alias is refused at load. Anchors let one
// node stand for another, so a small file can describe a cycle or an
// exponentially large tree; refusing them keeps the parse bounded by the bytes
// on disk.
func TestConfigFileRejectsAnchorsAndAliases(t *testing.T) {
	cases := []struct{ name, body string }{
		{"alias", "x: &a [1]\ny: *a\n"},
		{"merge key", "x: &a {b: 1}\ny:\n  <<: *a\n"},
		{"plain anchor", "x: &a 1\n"},
		{"recursive", "x: &a [*a]\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, path := newLimitTestCore(t)
			writeLimitFile(t, path, []byte(tc.body))
			err := c.load(ModeUser, "")
			if err == nil {
				t.Fatal("load accepted a configuration file with an anchor or alias")
			}
			if !strings.Contains(err.Error(), "anchors and aliases are not supported") {
				t.Fatalf("load error = %v, want it to name anchors and aliases", err)
			}
		})
	}
}

// TestConfigFileRejectsRecursiveAnchorOnReload checks the hot-reload path. A
// reload that followed a self-referential alias would recurse without end, so
// the file must be refused and the running values kept.
func TestConfigFileRejectsRecursiveAnchorOnReload(t *testing.T) {
	c, path := newWatchCore(t, "broker:\n  alias: old\n", false, nil)
	writeLimitFile(t, path, []byte("x: &a [*a]\n"))

	c.rescanOnce()

	if got := aliasOf(t, c); got != "old" {
		t.Fatalf("alias = %q after reloading a file with a recursive anchor, want old", got)
	}
}

// doublingAliasFile builds a document in which each alias refers twice to the
// previous one. Following the aliases while walking the tree takes time
// exponential in levels.
func doublingAliasFile(levels int) string {
	var b strings.Builder
	b.WriteString("a0: &a0 [0]\n")
	for i := 1; i < levels; i++ {
		fmt.Fprintf(&b, "a%d: &a%d [*a%d, *a%d]\n", i, i, i-1, i-1)
	}
	return b.String()
}

// TestConfigFileRejectsExponentialAliases checks that a small file whose
// aliases expand to an exponential number of nodes is refused quickly instead
// of holding the parser for hours. The parse runs in its own goroutine under a
// deadline, so a hang shows up as a test failure rather than a stuck suite.
func TestConfigFileRejectsExponentialAliases(t *testing.T) {
	body := doublingAliasFile(30)
	done := make(chan error, 1)
	go func() {
		_, err := parseConfigFile([]byte(body))
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("parseConfigFile accepted a file with aliases")
		}
		if !strings.Contains(err.Error(), "anchors and aliases are not supported") {
			t.Fatalf("parseConfigFile error = %v, want it to name anchors and aliases", err)
		}
	case <-time.After(eventTimeout):
		t.Fatalf("parseConfigFile did not return within %s for a file with 30 levels of doubling aliases", eventTimeout)
	}
}

// oversizedConfigBody returns a valid configuration file of 1 MiB + 1 byte
// that would change pairing.max_failures if it were read.
func oversizedConfigBody() []byte {
	body := []byte("pairing:\n  max_failures: 7\n")
	pad := (1 << 20) + 1 - len(body)
	return append(body, bytes.Repeat([]byte("#"), pad)...)
}

// TestConfigFileRejectsOversizedFile checks that a file above the 1 MiB limit
// is refused as a read error instead of being loaded.
func TestConfigFileRejectsOversizedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	writeLimitFile(t, path, oversizedConfigBody())

	if _, err := readConfigFile(path); err == nil {
		t.Fatal("readConfigFile accepted a file of 1 MiB + 1 byte")
	} else if !strings.Contains(err.Error(), "larger than 1 MiB") {
		t.Fatalf("readConfigFile error = %v, want it to name the 1 MiB limit", err)
	}

	c, loadPath := newLimitTestCore(t)
	writeLimitFile(t, loadPath, oversizedConfigBody())
	if err := c.load(ModeUser, ""); err == nil {
		t.Fatal("load accepted a file of 1 MiB + 1 byte")
	} else if !strings.Contains(err.Error(), "larger than 1 MiB") {
		t.Fatalf("load error = %v, want it to name the 1 MiB limit", err)
	}
}

// TestReadConfigFileFromEnforcesTheLimitForAnyReader covers the check after the
// read as well as the reader's own limit. A reader that returns too much must
// not hand the bytes to the parser, so the limit holds even when the reader is
// replaced or its limit is bypassed.
func TestReadConfigFileFromEnforcesTheLimitForAnyReader(t *testing.T) {
	body := []byte("broker:\n  alias: " + strings.Repeat("x", 1<<20))
	if len(body) <= maxConfigFileBytes {
		t.Fatalf("the test body is %d bytes, want more than the %d byte limit", len(body), maxConfigFileBytes)
	}
	readFile := func(string) ([]byte, error) { return body, nil }

	if _, err := readConfigFileFrom("/tmp/config.yaml", readFile); err == nil {
		t.Fatal("readConfigFileFrom accepted an oversized body from its reader")
	} else if !strings.Contains(err.Error(), "larger than 1 MiB") {
		t.Fatalf("readConfigFileFrom error = %v, want it to name the 1 MiB limit", err)
	}
}

// TestConfigFileAcceptsFileAtTheLimit pins the boundary: exactly 1 MiB is
// still read.
func TestConfigFileAcceptsFileAtTheLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	writeLimitFile(t, path, bytes.Repeat([]byte("#"), 1<<20))

	if _, err := readConfigFile(path); err != nil {
		t.Fatalf("readConfigFile rejected a file of exactly 1 MiB: %v", err)
	}
}

// TestConfigFileReloadKeepsValuesForOversizedFile checks the hot-reload path:
// an oversized file is a read error, so the running values stay in place
// instead of the new value being adopted.
func TestConfigFileReloadKeepsValuesForOversizedFile(t *testing.T) {
	c, path := newWatchCore(t, "pairing:\n  max_failures: 5\n", false, nil)
	writeLimitFile(t, path, oversizedConfigBody())

	c.rescanOnce()

	c.mu.Lock()
	got := c.values["pairing.max_failures"].Int
	c.mu.Unlock()
	if got != 5 {
		t.Fatalf("pairing.max_failures = %d after reloading an oversized file, want 5", got)
	}
}

// TestConfigFileSetRefusesOversizedFile checks that Set fails and leaves the
// file untouched when the file on disk is above the limit.
func TestConfigFileSetRefusesOversizedFile(t *testing.T) {
	c, path := newWatchCore(t, "pairing:\n  max_failures: 5\n", false, nil)
	before := oversizedConfigBody()
	writeLimitFile(t, path, before)

	c.mu.Lock()
	_, err := c.setLocked(Actor{}, "pairing.max_failures", intValue(9))
	c.mu.Unlock()
	if err == nil {
		t.Fatal("Set accepted an oversized configuration file")
	}
	if !strings.Contains(err.Error(), "larger than 1 MiB") {
		t.Fatalf("Set error = %v, want it to name the 1 MiB limit", err)
	}
	after, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("Set rewrote the configuration file after refusing it")
	}
}

// nestedMappingFile builds a document whose mappings are nested depth levels
// deep.
func nestedMappingFile(depth int) string {
	var b strings.Builder
	for i := 0; i < depth-1; i++ {
		b.WriteString(strings.Repeat("  ", i))
		b.WriteString("a:\n")
	}
	b.WriteString(strings.Repeat("  ", depth-1))
	b.WriteString("a: 1\n")
	return b.String()
}

// TestConfigFileRespectsNestingLimit checks that 32 levels of nesting parse and
// 33 are refused. The generated templates, which use plain key-value pairs,
// still parse.
func TestConfigFileRespectsNestingLimit(t *testing.T) {
	if _, err := parseConfigFile([]byte(nestedMappingFile(32))); err != nil {
		t.Fatalf("32 levels of nesting did not parse: %v", err)
	}
	if _, err := parseConfigFile([]byte(nestedMappingFile(33))); err == nil {
		t.Fatal("parseConfigFile accepted 33 levels of nesting")
	} else if !strings.Contains(err.Error(), "nesting is deeper than 32 levels") {
		t.Fatalf("parseConfigFile error = %v, want it to name the nesting limit", err)
	}

	for _, mode := range []Mode{ModeSystem, ModeUser} {
		if _, err := parseConfigFile(Template(mode)); err != nil {
			t.Errorf("Template(%v) no longer parses: %v", mode, err)
		}
	}
}
