package inbox

import (
	"os"
	"path/filepath"
	"testing"
)

// TestWatch_ConfigDirectoryAbsentAtStart checks that a configuration directory
// absent at startup is reported as 0 and that a record written after it appears
// is reported.
func TestWatch_ConfigDirectoryAbsentAtStart(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "config")

	pids := make(chan int, 16)
	stop, err := Watch(dir, func(pid int) { pids <- pid })
	if err != nil {
		t.Fatalf("Watch on a configuration directory that does not exist yet: %v", err)
	}
	defer stop()

	aWaitPID(t, pids, 0)

	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("creating configuration directory: %v", err)
	}

	const pid = 7601
	aWriteRecord(t, dir, pid, map[string]any{
		"name":                "agent",
		"messagingSocketPath": "/tmp/watch-startup-fallback.sock",
	})
	aWaitPID(t, pids, pid)
}

// TestWatch_SessionsDirectoryAppearsLater checks that a record written while
// the registry directory appears is reported.
func TestWatch_SessionsDirectoryAppearsLater(t *testing.T) {
	dir := t.TempDir()

	pids := make(chan int, 16)
	stop, err := Watch(dir, func(pid int) { pids <- pid })
	if err != nil {
		t.Fatalf("Watch on a configuration directory without a registry directory: %v", err)
	}
	defer stop()

	const pid = 7602
	aWriteRecord(t, dir, pid, map[string]any{
		"name":                "agent",
		"messagingSocketPath": "/tmp/watch-sessions-later.sock",
	})
	aWaitPID(t, pids, pid)
}
