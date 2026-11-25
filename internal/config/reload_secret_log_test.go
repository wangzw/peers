package config

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// TestConfigReloadInvalidStaticKeyDoesNotLeakToLog checks that a rejected static
// key keeps the old value, records no change and never appears in the log.
func TestConfigReloadInvalidStaticKeyDoesNotLeakToLog(t *testing.T) {
	c, path := newNCCore(t, "pairing:\n  static_key: \"0123456789abcdef\"\n", nil)
	var logs bytes.Buffer
	c.logger = slog.New(slog.NewTextHandler(&logs, nil))

	const badKey = "s3cr3t" // six characters: shorter than the 16-character minimum
	ncWriteConfig(t, path, "pairing:\n  static_key: \""+badKey+"\"\n")
	c.rescanOnce()

	logged := logs.String()
	if !strings.Contains(logged, "pairing.static_key") {
		t.Fatalf("the rejected reload was not logged; log = %q", logged)
	}
	if strings.Contains(logged, badKey) {
		t.Fatalf("the log leaked the static key value: %q", logged)
	}

	c.mu.Lock()
	kept := c.values["pairing.static_key"].String
	c.mu.Unlock()
	if kept != "0123456789abcdef" {
		t.Fatalf("pairing.static_key = %q after an invalid reload, want the previous key", kept)
	}

	events, err := c.readAudit(time.Time{})
	if err != nil {
		t.Fatalf("read audit: %v", err)
	}
	for _, event := range events {
		if event.Event == EventConfigFileChanged && event.Key == "pairing.static_key" {
			t.Fatalf("an invalid static key produced a config_file_changed record: %+v", event)
		}
	}
}
