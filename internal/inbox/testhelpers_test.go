package inbox

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// aWaitTimeout is the package-wide fallback for a wait on an event that is
// expected to arrive: a file-system notification, a stat, a Unix socket
// callback or a Stop returning. It only decides how long a wait that will
// never complete takes to fail: the success path selects on the event itself
// and never spends this time, so a larger value costs nothing. Thirty seconds
// is far above the worst normal latency under load; it is not a claim that an
// event should take that long.
const aWaitTimeout = 30 * time.Second

func aSessionsDir(configDir string) string {
	return filepath.Join(configDir, "sessions")
}

// bShortTempDir returns a temporary directory rooted at /tmp. t.TempDir() sits
// under /var/folders on macOS, where a path such as
// <dir>/sessions/<pid>.json overflows the roughly 104-byte Unix socket path
// limit. The directory is removed when the test ends.
func bShortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "inbox-test-")
	if err != nil {
		t.Fatalf("creating short temporary directory: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// aWriteRecord writes a canonical <pid>.json record with the fields every
// fixture starts from. Entries in fields override or extend them.
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

// aWriteRaw writes the exact bytes of a <pid>.json record.
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

// aWaitPID waits for one callback reporting want. Reports for other process
// ids are ignored.
func aWaitPID(t *testing.T, pids <-chan int, want int) {
	t.Helper()
	aWaitPIDs(t, pids, want)
}

// aWaitPIDs waits until every wanted process id has been reported once.
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

// aDrain discards callbacks that are already queued.
func aDrain(pids <-chan int) {
	for {
		select {
		case <-pids:
		default:
			return
		}
	}
}
