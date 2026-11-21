package config

import (
	"crypto/sha256"
	"net/netip"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// TestReadConfigFileHashesParsedBytesWithoutSecondRead pins the reload
// invariant: the stored hash comes from the same bytes that were parsed. The
// injected reader would return different content on a second call, so a
// separate read for hashing shows up as a wrong hash or an extra call.
func TestReadConfigFileHashesParsedBytesWithoutSecondRead(t *testing.T) {
	first := []byte("broker:\n  alias: first\n")
	second := []byte("broker:\n  alias: second\n")
	calls := 0
	readFile := func(string) ([]byte, error) {
		calls++
		if calls == 1 {
			return first, nil
		}
		return second, nil
	}

	doc, err := readConfigFileFrom("/tmp/config.yaml", readFile)
	if err != nil {
		t.Fatalf("readConfigFileFrom: %v", err)
	}
	if calls != 1 {
		t.Fatalf("read the file %d times, want exactly 1", calls)
	}
	if got, want := doc.contentHash, sha256.Sum256(first); got != want {
		t.Fatalf("contentHash = %x, want the hash of the parsed bytes %x", got, want)
	}
	if got := doc.values["broker.alias"].String; got != "first" {
		t.Fatalf("broker.alias = %q, want first", got)
	}
}

// hashTestRoutes supplies a fixed default-route address so the test never
// enumerates the host's route tables.
type hashTestRoutes struct{}

func (hashTestRoutes) DefaultRouteAddresses() ([]netip.Addr, error) {
	return []netip.Addr{netip.MustParseAddr("192.0.2.20")}, nil
}

// TestLoadStoresHashOfTheBytesItParsed checks that load publishes the same
// hash the parsed document carries.
func TestLoadStoresHashOfTheBytesItParsed(t *testing.T) {
	home := t.TempDir()
	body := "broker:\n  alias: hashed\n"
	dir := filepath.Join(home, ".peers")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	c := newCore()
	c.skipDataCheck = true
	c.noBackground = true
	c.checkRuntime = func() (string, error) { return home, nil }
	c.homeDir = func() (string, error) { return home, nil }
	c.hostname = func() (string, error) { return "hostA", nil }
	c.accountName = func() (string, error) { return "tester", nil }
	c.now = func() time.Time { return time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC) }
	c.routesSrc = hashTestRoutes{}
	t.Cleanup(c.close)

	if err := c.load(ModeUser, ""); err != nil {
		t.Fatalf("load() error = %v", err)
	}
	if c.file == nil {
		t.Fatal("load did not retain the parsed document")
	}
	if got, want := c.lastFileHash, sha256.Sum256([]byte(body)); got != want {
		t.Fatalf("lastFileHash = %x, want %x", got, want)
	}
	if c.lastFileHash != c.file.contentHash {
		t.Fatalf("lastFileHash = %x, document hash = %x", c.lastFileHash, c.file.contentHash)
	}
}

// TestWriteConfigFileCreatesParentDirWithDeploymentMode covers the case where
// the config directory disappeared after Load: creating it must use the mode
// the deployment mode requires, not a user-mode 0700 in system mode. The file
// mode is 0600 in both modes, so it cannot be used to infer the directory mode.
func TestWriteConfigFileCreatesParentDirWithDeploymentMode(t *testing.T) {
	old := syscall.Umask(0o022)
	t.Cleanup(func() { syscall.Umask(old) })

	cases := []struct {
		name    string
		mode    Mode
		dirMode os.FileMode
	}{
		{"user", ModeUser, 0o700},
		{"system", ModeSystem, 0o755},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "missing", "peers")
			path := filepath.Join(dir, "config.yaml")
			if err := writeConfigFile(path, []byte("broker:\n  alias: a\n"), tc.mode); err != nil {
				t.Fatalf("writeConfigFile: %v", err)
			}
			info, err := os.Stat(dir)
			if err != nil {
				t.Fatalf("stat directory: %v", err)
			}
			if got := info.Mode().Perm(); got != tc.dirMode {
				t.Fatalf("directory mode = %04o, want %04o", got, tc.dirMode)
			}
			fileInfo, err := os.Stat(path)
			if err != nil {
				t.Fatalf("stat file: %v", err)
			}
			if got := fileInfo.Mode().Perm(); got != 0o600 {
				t.Fatalf("file mode = %04o, want 0600", got)
			}
		})
	}
}
