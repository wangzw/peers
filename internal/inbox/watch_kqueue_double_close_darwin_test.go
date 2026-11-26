//go:build darwin

package inbox

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

const (
	kqueueDoubleCloseRounds    = 500
	kqueueDoubleCloseFiles     = 8
	kqueueDoubleCloseSentinels = 64
	kqueueDoubleCloseRoundWait = aWaitTimeout
)

type kqueueSentinel struct {
	file *os.File
	fd   int
	ino  uint64
	name string
}

// TestWatcher_KqueueDoubleCloseLeavesSentinelsIntact checks that fsnotify
// v1.10.1's kqueue backend does not close a descriptor a second time when Close
// races a Remove event.
func TestWatcher_KqueueDoubleCloseLeavesSentinelsIntact(t *testing.T) {
	sentinelDir := t.TempDir()
	watchRoot := t.TempDir()
	overallDeadline := time.Now().Add(5 * time.Minute)

	for round := 0; round < kqueueDoubleCloseRounds; round++ {
		func() {
			if deadline, ok := t.Deadline(); ok && time.Now().After(deadline) {
				t.Fatalf("round %d: test deadline reached before the round started", round)
			}
			if time.Now().After(overallDeadline) {
				t.Fatalf("round %d: overall stress deadline reached", round)
			}

			sentinels := openKqueueSentinels(t, sentinelDir, round)
			defer closeKqueueSentinels(sentinels)

			dir := filepath.Join(watchRoot, fmt.Sprintf("round-%03d", round))
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatalf("round %d: creating watched directory: %v", round, err)
			}
			for i := 0; i < kqueueDoubleCloseFiles; i++ {
				path := filepath.Join(dir, fmt.Sprintf("watched-%02d", i))
				if err := os.WriteFile(path, []byte("watched"), 0o600); err != nil {
					t.Fatalf("round %d: creating %s: %v", round, path, err)
				}
			}

			watcher, err := fsnotify.NewWatcher()
			if err != nil {
				t.Fatalf("round %d: creating watcher: %v", round, err)
			}
			if err := watcher.Add(dir); err != nil {
				_ = watcher.Close()
				t.Fatalf("round %d: adding %s: %v", round, dir, err)
			}

			start := make(chan struct{})
			var actions sync.WaitGroup
			actions.Add(2)
			go func() {
				defer actions.Done()
				<-start
				_ = os.RemoveAll(dir)
			}()
			go func() {
				defer actions.Done()
				<-start
				_ = watcher.Close()
			}()
			close(start)

			done := make(chan struct{})
			go func() {
				actions.Wait()
				close(done)
			}()
			timer := time.NewTimer(kqueueDoubleCloseRoundWait)
			select {
			case <-done:
				timer.Stop()
			case <-timer.C:
				t.Fatalf("round %d: RemoveAll and Close did not both return within %v", round, kqueueDoubleCloseRoundWait)
			}

			for _, sentinel := range sentinels {
				var stat syscall.Stat_t
				if err := syscall.Fstat(sentinel.fd, &stat); err != nil {
					t.Fatalf("round %d: sentinel fd %d (%s): fstat: %v; expected inode %d, EBADF means a double close reached a recycled descriptor",
						round, sentinel.fd, sentinel.name, err, sentinel.ino)
				}
				if stat.Ino != sentinel.ino {
					t.Fatalf("round %d: sentinel fd %d (%s): inode changed: expected %d, got %d",
						round, sentinel.fd, sentinel.name, sentinel.ino, stat.Ino)
				}
			}
		}()
	}
}

func openKqueueSentinels(t *testing.T, dir string, round int) []kqueueSentinel {
	t.Helper()

	sentinels := make([]kqueueSentinel, 0, kqueueDoubleCloseSentinels)
	for i := 0; i < kqueueDoubleCloseSentinels; i++ {
		path := filepath.Join(dir, fmt.Sprintf("sentinel-%02d", i))
		file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
		if err != nil {
			closeKqueueSentinels(sentinels)
			t.Fatalf("round %d: opening sentinel %s: %v", round, path, err)
		}
		info, err := file.Stat()
		if err != nil {
			_ = file.Close()
			closeKqueueSentinels(sentinels)
			t.Fatalf("round %d: stat sentinel %s: %v", round, path, err)
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			_ = file.Close()
			closeKqueueSentinels(sentinels)
			t.Fatalf("round %d: sentinel %s: unexpected Stat_t type %T", round, path, info.Sys())
		}
		sentinels = append(sentinels, kqueueSentinel{
			file: file,
			fd:   int(file.Fd()),
			ino:  stat.Ino,
			name: path,
		})
	}
	return sentinels
}

func closeKqueueSentinels(sentinels []kqueueSentinel) {
	for _, sentinel := range sentinels {
		_ = sentinel.file.Close()
	}
}
