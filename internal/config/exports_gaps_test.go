package config

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// TestREQBrokerPairing028_HostnameExportedEntry checks that Hostname uses the
// loaded instance's source, falls back to the process hostname and reports a
// read failure as an empty string with a log entry.
func TestREQBrokerPairing028_HostnameExportedEntry(t *testing.T) {
	t.Run("loaded instance uses its own source", func(t *testing.T) {
		c := newCore()
		c.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
		c.hostname = func() (string, error) { return "work station (lab)", nil }
		installCoreForTest(t, c)

		if got, want := Hostname(), "work-station--lab-"; got != want {
			t.Fatalf("Hostname() = %q, want %q", got, want)
		}
	})

	t.Run("loaded instance logs a read failure", func(t *testing.T) {
		var buf bytes.Buffer
		c := newCore()
		c.logger = slog.New(slog.NewTextHandler(&buf, nil))
		c.hostname = func() (string, error) { return "", errors.New("hostname unavailable") }
		installCoreForTest(t, c)

		if got := Hostname(); got != "" {
			t.Fatalf("Hostname() = %q, want an empty string", got)
		}
		if logged := buf.String(); !strings.Contains(logged, "hostname unavailable") {
			t.Fatalf("log = %q, want it to name the hostname failure", logged)
		}
	})

	t.Run("no loaded instance uses the process hostname", func(t *testing.T) {
		installCoreForTest(t, nil)

		name, err := os.Hostname()
		got := Hostname()
		if err != nil {
			if got != "" {
				t.Fatalf("Hostname() = %q after os.Hostname failed, want an empty string", got)
			}
			return
		}
		if want := sanitizeHostname(name); got != want {
			t.Fatalf("Hostname() = %q, want the sanitized process hostname %q", got, want)
		}
	})
}

// TestREQBrokerConfig013_RuntimeDirExportedEntries checks that CheckRuntimeDir
// creates the current account's runtime directory with a compliant owner and
// permission and that AccountLock returns the lock path beneath it.
func TestREQBrokerConfig013_RuntimeDirExportedEntries(t *testing.T) {
	euid := os.Geteuid()
	runtimeDir := runtimeDirPath(euid)

	// The runtime directory is shared per account; do not remove it after the
	// test.
	if err := CheckRuntimeDir(); err != nil {
		t.Fatalf("CheckRuntimeDir() error = %v", err)
	}
	info, err := os.Lstat(runtimeDir)
	if err != nil {
		t.Fatalf("lstat %s: %v", runtimeDir, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		t.Fatalf("%s is not a plain directory (mode %v)", runtimeDir, info.Mode())
	}
	if st, ok := info.Sys().(*syscall.Stat_t); ok && int(st.Uid) != euid {
		t.Fatalf("%s is owned by uid %d, want %d", runtimeDir, int(st.Uid), euid)
	}
	if err := checkDirPermission(info.Mode().Perm(), euid, permRuntime); err != nil {
		t.Fatalf("%s has mode %#o: %v", runtimeDir, info.Mode().Perm(), err)
	}

	if got, want := AccountLock(), filepath.Join(runtimeDir, "broker.lock"); got != want {
		t.Fatalf("AccountLock() = %q, want %q", got, want)
	}
	if err := takeAccountInitErr(); err != nil {
		t.Fatalf("AccountLock recorded a startup error: %v", err)
	}
}

// TestPrepareDataDirsUnderTemporaryRoots checks that prepareDataDirs creates a
// private data directory and refuses the system form for a non-root account.
func TestPrepareDataDirsUnderTemporaryRoots(t *testing.T) {
	euid := os.Geteuid()
	base := t.TempDir()
	dataDir := filepath.Join(base, "data")
	c := &core{mode: ModeUser, euid: func() int { return euid }}

	if err := c.prepareDataDirs(PathSet{DataDir: dataDir}); err != nil {
		t.Fatalf("prepareDataDirs() error = %v", err)
	}
	info, err := os.Lstat(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Fatalf("%s is not a directory", dataDir)
	}
	if info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("%s has mode %#o, want no group or other access", dataDir, info.Mode().Perm())
	}
	if err := c.prepareDataDirs(PathSet{DataDir: dataDir}); err != nil {
		t.Fatalf("prepareDataDirs() on an existing directory error = %v", err)
	}

	if euid != 0 {
		system := &core{mode: ModeSystem, euid: func() int { return euid }}
		err := system.prepareDataDirs(PathSet{DataDir: filepath.Join(base, "system")})
		if err == nil || !strings.Contains(err.Error(), "must run as root") {
			t.Fatalf("system prepareDataDirs as uid %d = %v, want a root requirement", euid, err)
		}
	}
}

// TestConfigValueKindsAreConcrete checks that every key in the table maps to a
// concrete kind, never KindUnset.
func TestConfigValueKindsAreConcrete(t *testing.T) {
	concrete := map[ValueKind]bool{
		KindString:     true,
		KindBool:       true,
		KindInt:        true,
		KindDuration:   true,
		KindStringList: true,
		KindSecret:     true,
	}
	for _, name := range allKeyNames() {
		spec, ok := lookupKey(name)
		if !ok {
			t.Fatalf("%s is not in the key table", name)
		}
		kind := valueKindForSpec(spec)
		if kind == KindUnset {
			t.Errorf("%s maps to KindUnset", name)
		}
		if !concrete[kind] {
			t.Errorf("%s maps to unknown kind %d", name, kind)
		}
	}
}
