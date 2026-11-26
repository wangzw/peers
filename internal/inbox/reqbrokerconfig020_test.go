package inbox

import (
	"path/filepath"
	"testing"
)

// TestREQBrokerConfig020_LookupSocketPathForOwnerCheck checks that Lookup returns the record's socket path verbatim and the resolved configuration directory.
func TestREQBrokerConfig020_LookupSocketPathForOwnerCheck(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "inbox.sock")
	aWriteRecord(t, dir, 7101, map[string]any{
		"name":                "api-worker",
		"status":              "idle",
		"messagingSocketPath": socket,
	})

	s, err := Lookup(dir, socket)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if s.SocketPath != socket {
		t.Fatalf("SocketPath = %q, want the record's value %q", s.SocketPath, socket)
	}
	if s.ConfigDir != dir {
		t.Fatalf("ConfigDir = %q, want the resolved %q", s.ConfigDir, dir)
	}
}
