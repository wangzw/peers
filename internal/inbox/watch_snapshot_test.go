package inbox

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestScanSessions_ReplacedByFIFODoesNotBlock checks that a record replaced by
// a FIFO between its Lstat and its read does not block the snapshot.
func TestScanSessions_ReplacedByFIFODoesNotBlock(t *testing.T) {
	const pid = 7101
	dir := t.TempDir()
	path := aWriteRecord(t, dir, pid, map[string]any{
		"name":                "agent",
		"messagingSocketPath": "/tmp/watch-fifo-race.sock",
	})

	var once sync.Once
	replace := func() {
		once.Do(func() {
			if err := os.Remove(path); err != nil {
				t.Errorf("removing the record: %v", err)
				return
			}
			aCreateFIFO(t, path)
		})
	}
	testStampBeforeRead.Store(&replace)
	t.Cleanup(func() { testStampBeforeRead.Store(nil) })

	done := make(chan map[int]fileStamp, 1)
	go func() {
		done <- scanSessions(aSessionsDir(dir))
	}()

	select {
	case stamps := <-done:
		if _, ok := stamps[pid]; !ok {
			t.Fatal("the record replaced by a FIFO is missing from the snapshot")
		}
	case <-time.After(aWaitTimeout):
		t.Fatal("scanSessions blocked on a FIFO that replaced a record after its Lstat")
	}
}

func aStampWithoutBlocking(t *testing.T, path string) fileStamp {
	t.Helper()
	type result struct {
		stamp fileStamp
		ok    bool
	}
	done := make(chan result, 1)
	go func() {
		stamp, ok := stampRecord(path)
		done <- result{stamp: stamp, ok: ok}
	}()
	select {
	case got := <-done:
		if !got.ok {
			t.Fatalf("stampRecord(%s) reported no stamp", path)
		}
		return got.stamp
	case <-time.After(aWaitTimeout):
		t.Fatalf("stampRecord(%s) blocked", path)
		return fileStamp{}
	}
}

func aScanWithoutBlocking(t *testing.T, dir string) map[int]fileStamp {
	t.Helper()
	done := make(chan map[int]fileStamp, 1)
	go func() { done <- scanSessions(dir) }()
	select {
	case stamps := <-done:
		return stamps
	case <-time.After(aWaitTimeout):
		t.Fatalf("scanSessions(%s) blocked", dir)
		return nil
	}
}

// TestStampRecord_NonRegularAndOversizedEntriesAreNotRead checks that a
// symbolic link, a directory and an oversized file each get a stat-based stamp
// without being read.
func TestStampRecord_NonRegularAndOversizedEntriesAreNotRead(t *testing.T) {
	dir := t.TempDir()
	sessions := aSessionsDir(dir)
	if err := os.MkdirAll(sessions, 0o700); err != nil {
		t.Fatalf("creating sessions directory: %v", err)
	}

	fifo := filepath.Join(dir, "blocking-target")
	aCreateFIFO(t, fifo)
	link := filepath.Join(sessions, "7201.json")
	if err := os.Symlink(fifo, link); err != nil {
		t.Fatalf("creating symbolic link record: %v", err)
	}
	linkStamp := aStampWithoutBlocking(t, link)
	if linkStamp.mode&os.ModeSymlink == 0 {
		t.Fatalf("symbolic link record has mode %v, want a symbolic link", linkStamp.mode)
	}
	if linkStamp.digest != (fileStamp{}).digest {
		t.Fatal("stampRecord followed a symbolic link and hashed its target")
	}

	directory := filepath.Join(sessions, "7202.json")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatalf("creating directory record: %v", err)
	}
	aCreateFIFO(t, filepath.Join(directory, "inner"))
	directoryStamp := aStampWithoutBlocking(t, directory)
	if !directoryStamp.mode.IsDir() {
		t.Fatalf("directory record has mode %v, want a directory", directoryStamp.mode)
	}
	if directoryStamp.digest != (fileStamp{}).digest {
		t.Fatal("stampRecord descended into a directory record and hashed its contents")
	}

	oversized := filepath.Join(sessions, "7203.json")
	aWriteFile(t, oversized, []byte(strings.Repeat("x", maxRecordSize+1)))
	oversizedStamp := aStampWithoutBlocking(t, oversized)
	if !oversizedStamp.mode.IsRegular() || oversizedStamp.size <= maxRecordSize {
		t.Fatalf("oversized record has mode %v and size %d, want a regular file above maxRecordSize",
			oversizedStamp.mode, oversizedStamp.size)
	}
	if oversizedStamp.digest != (fileStamp{}).digest {
		t.Fatal("stampRecord read an oversized record and hashed it")
	}
}

// TestScanSessions_NonRegularAndOversizedRewritesAreNotChanges checks that
// rewriting the content behind a symbolic link, a directory or an oversized
// file leaves its stamp unchanged.
func TestScanSessions_NonRegularAndOversizedRewritesAreNotChanges(t *testing.T) {
	const (
		linkPID      = 7201
		directoryPID = 7202
		oversizedPID = 7203
	)
	dir := t.TempDir()
	sessions := aSessionsDir(dir)
	if err := os.MkdirAll(sessions, 0o700); err != nil {
		t.Fatalf("creating sessions directory: %v", err)
	}

	target := filepath.Join(dir, "link-target")
	aWriteFile(t, target, []byte("first"))
	link := filepath.Join(sessions, "7201.json")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("creating symbolic link record: %v", err)
	}

	directory := filepath.Join(sessions, "7202.json")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatalf("creating directory record: %v", err)
	}
	inner := filepath.Join(directory, "inner")
	aWriteFile(t, inner, []byte("first"))

	oversized := filepath.Join(sessions, "7203.json")
	fixed := time.Unix(1_700_000_000, 0)
	aWriteFile(t, oversized, []byte(strings.Repeat("a", maxRecordSize+1)))
	aSetModTime(t, oversized, fixed)

	before := aScanWithoutBlocking(t, sessions)
	for _, pid := range []int{linkPID, directoryPID, oversizedPID} {
		if _, ok := before[pid]; !ok {
			t.Fatalf("the snapshot is missing the entry for pid %d", pid)
		}
	}

	aWriteFile(t, target, []byte("other"))
	aWriteFile(t, inner, []byte("other"))
	aWriteFile(t, oversized, []byte(strings.Repeat("b", maxRecordSize+1)))
	aSetModTime(t, oversized, fixed)

	after := aScanWithoutBlocking(t, sessions)
	if changed := changedPIDs(before, after); len(changed) != 0 {
		t.Fatalf("changedPIDs = %v, want none: a symbolic link, a directory and an oversized file are not read, so their hidden content is not a record change", changed)
	}
}
