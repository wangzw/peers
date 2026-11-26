package inbox

import (
	"crypto/sha256"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"
)

// testStampBeforeRead, when set by a test, runs after stampRecord has decided
// that a file looks readable and before it reads the content.
var testStampBeforeRead atomic.Pointer[func()]

// fileStamp is the watcher's deduplication key for one registry file. The
// content digest is needed because mtime and size alone miss an in-place
// rewrite of the same size in the same file system timestamp tick.
type fileStamp struct {
	mtime  time.Time
	size   int64
	mode   fs.FileMode
	digest [sha256.Size]byte
}

// scanSessions snapshots every canonical <pid>.json entry in dir; a missing or
// unreadable directory gives an empty snapshot.
func scanSessions(dir string) map[int]fileStamp {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	stamps := make(map[int]fileStamp, len(entries))
	for _, entry := range entries {
		pid, ok := parseRecordName(entry.Name())
		if !ok {
			continue
		}
		if stamp, ok := stampRecord(filepath.Join(dir, entry.Name())); ok {
			stamps[pid] = stamp
		}
	}
	return stamps
}

func stampRecord(path string) (fileStamp, bool) {
	info, err := os.Lstat(path)
	if err != nil {
		return fileStamp{}, false
	}
	stamp := fileStamp{
		mtime: info.ModTime(),
		size:  info.Size(),
		mode:  info.Mode(),
	}
	// Only regular, bounded files are hashed; a symbolic link or an oversized
	// file still gets a stat-based stamp so a change in it is reported.
	if !info.Mode().IsRegular() || info.Size() > maxRecordSize {
		return stamp, true
	}
	if hook := testStampBeforeRead.Load(); hook != nil {
		(*hook)()
	}
	// readRecordData opens with O_NOFOLLOW and O_NONBLOCK and rechecks the file,
	// so a path replaced by a symbolic link or FIFO after the Lstat cannot
	// redirect the read or block this goroutine.
	data, err := readRecordData(path)
	if err != nil {
		// A transient read failure must not look like a deletion, so keep the
		// stat-based stamp; the next scan settles it.
		return stamp, true
	}
	stamp.digest = sha256.Sum256(data)
	return stamp, true
}

// changedPIDs returns the process ids whose stamp appeared, disappeared or
// changed between two snapshots, in ascending order.
func changedPIDs(previous, current map[int]fileStamp) []int {
	pids := make([]int, 0)
	for pid, stamp := range current {
		if old, ok := previous[pid]; !ok || old != stamp {
			pids = append(pids, pid)
		}
	}
	for pid := range previous {
		if _, ok := current[pid]; !ok {
			pids = append(pids, pid)
		}
	}
	sort.Ints(pids)
	return pids
}
