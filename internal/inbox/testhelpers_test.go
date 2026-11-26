package inbox

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// aWaitTimeout bounds a wait that is expected to succeed; it only decides how long a wait that never completes takes to fail.
const aWaitTimeout = 30 * time.Second

func aSessionsDir(configDir string) string {
	return filepath.Join(configDir, "sessions")
}

// bShortTempDir returns a temporary directory rooted at /tmp, because t.TempDir() sits under /var/folders on macOS and overflows the roughly 104-byte Unix socket path limit.
func bShortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "inbox-test-")
	if err != nil {
		t.Fatalf("creating short temporary directory: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func aWriteRecord(t *testing.T, configDir string, pid int, fields map[string]any) string {
	t.Helper()
	record := map[string]any{
		"pid":                 pid,
		"name":                "agent",
		"status":              "idle",
		"messagingSocketPath": "",
		"sessionId":           "00000000-0000-4000-8000-000000000000",
		"nameSource":          "user",
		"kind":                "interactive",
		"entrypoint":          "cli",
		"peerProtocol":        1,
	}
	for key, value := range fields {
		record[key] = value
	}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("encoding record: %v", err)
	}
	return aWriteRaw(t, configDir, pid, data)
}

func aWriteRaw(t *testing.T, configDir string, pid int, data []byte) string {
	t.Helper()
	dir := aSessionsDir(configDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("creating sessions directory: %v", err)
	}
	path := filepath.Join(dir, strconv.Itoa(pid)+".json")
	aWriteFile(t, path, data)
	return path
}

func aWriteFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
}

func aSetModTime(t *testing.T, path string, modTime time.Time) {
	t.Helper()
	if err := os.Chtimes(path, modTime, modTime); err != nil {
		t.Fatalf("setting modification time of %s: %v", path, err)
	}
}

func aWaitPID(t *testing.T, pids <-chan int, want int) {
	t.Helper()
	aWaitPIDs(t, pids, want)
}

func aWaitPIDs(t *testing.T, pids <-chan int, want ...int) {
	t.Helper()
	remaining := make(map[int]bool, len(want))
	for _, pid := range want {
		remaining[pid] = true
	}
	timer := time.NewTimer(aWaitTimeout)
	defer timer.Stop()
	for len(remaining) > 0 {
		select {
		case got := <-pids:
			delete(remaining, got)
		case <-timer.C:
			t.Fatalf("timed out; still waiting for callbacks reporting %v", remaining)
		}
	}
}

func aDrain(pids <-chan int) {
	for {
		select {
		case <-pids:
		default:
			return
		}
	}
}
