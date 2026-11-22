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
	// kqueueDoubleCloseRounds is the stress-loop bound. An explicit round
	// count keeps the test finite even when the overall test deadline is
	// unset.
	kqueueDoubleCloseRounds = 500
	// kqueueDoubleCloseFiles is the number of files placed in each round's
	// watched directory, so kqueue opens one descriptor per file.
	kqueueDoubleCloseFiles = 8
	// kqueueDoubleCloseSentinels is the number of unrelated descriptors held
	// open across each round. A descriptor closed a second time can be
	// recycled by a later open, and the sentinel check catches that.
	kqueueDoubleCloseSentinels = 64
	// kqueueDoubleCloseRoundWait bounds one round's RemoveAll + Close wait. The
	// success path selects on the goroutines finishing, so this only decides
	// how long a stalled round takes to fail; it shares the package-wide
	// fallback rather than a shorter value that load could exhaust.
	kqueueDoubleCloseRoundWait = aWaitTimeout
)

// kqueueSentinel is one open file whose descriptor must survive a round.
type kqueueSentinel struct {
	file *os.File
	fd   int
	ino  uint64
	name string
}

// TestWatcher_KqueueDoubleCloseLeavesSentinelsIntact reproduces the fsnotify
// v1.10.1 kqueue double-close defect: Watcher.Close and the readEvents
// goroutine's removal path can both close the same kqueue watch descriptor.
// Close snapshots the watches, closes each descriptor, and then signals
// readEvents; readEvents may already be inside remove for a Remove event and
// close the same descriptor again. The second close can hit whichever
// descriptor the kernel reused after the first close, including a file that
// has nothing to do with the watcher.
//
// Each round watches a freshly created directory and races RemoveAll on it
// against Close. Unrelated sentinel files stay open across the round; after
// the round every sentinel descriptor must still refer to its original inode.
// A closed or recycled descriptor fails the test with the round, descriptor
// number and both inode values.
func TestWatcher_KqueueDoubleCloseLeavesSentinelsIntact(t *testing.T) {
	sentinelDir := t.TempDir()
	watchRoot := t.TempDir()
	// An explicit overall bound keeps a slow host from turning the stress
	// loop into an unbounded run; t.Deadline is honored when the caller sets
	// one.
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

			// A shared start channel makes RemoveAll and Close overlap as
			// tightly as possible; the defect needs both paths inside their
			// close call at the same time.
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

// openKqueueSentinels opens the round's sentinel files and records the inode
// each descriptor refers to.
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

// closeKqueueSentinels releases the round's sentinel descriptors. It runs even
// when the round failed, so the failure report is not obscured by later rounds
// exhausting the descriptor table.
func closeKqueueSentinels(sentinels []kqueueSentinel) {
	for _, sentinel := range sentinels {
		_ = sentinel.file.Close()
	}
}
