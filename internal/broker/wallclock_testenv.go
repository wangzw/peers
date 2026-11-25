//go:build peers_testenv

package broker

import (
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
)

const testClockFileEnv = "PEERS_TEST_CLOCK_FILE"

func init() {
	path := os.Getenv(testClockFileEnv)
	if path == "" {
		return
	}
	wallClock = offsetClock(path)
}

// offsetClock returns a clock that adds the duration in path to the current
// time, re-reading the file when its size or modification time changes. A
// missing or unparsable file yields an offset of zero.
func offsetClock(path string) func() time.Time {
	var (
		mu       sync.Mutex
		modTime  time.Time
		size     int64
		offset   time.Duration
		loaded   bool
		unusable bool
	)
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()

		info, err := os.Stat(path)
		if err != nil {
			offset = 0
			loaded = false
			if !unusable {
				slog.Default().Warn("test_clock_file_unusable", "path", path, "error", err)
				unusable = true
			}
			return time.Now()
		}
		if !loaded || !info.ModTime().Equal(modTime) || info.Size() != size {
			d, err := readClockOffset(path)
			modTime, size, loaded = info.ModTime(), info.Size(), true
			if err != nil {
				offset = 0
				if !unusable {
					slog.Default().Warn("test_clock_file_unusable", "path", path, "error", err)
					unusable = true
				}
			} else {
				offset = d
				unusable = false
			}
		}
		return time.Now().Add(offset)
	}
}

func readClockOffset(path string) (time.Duration, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return time.ParseDuration(strings.TrimSpace(string(data)))
}
