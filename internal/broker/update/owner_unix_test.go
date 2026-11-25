//go:build unix

package update

import (
	"os"
	"syscall"
	"testing"
)

func assertSameOwner(t *testing.T, path string, want os.FileInfo) {
	t.Helper()
	got, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	gotStat, ok := got.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("stat %s: unexpected system information %T", path, got.Sys())
	}
	wantStat, ok := want.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("want: unexpected system information %T", want.Sys())
	}
	if gotStat.Uid != wantStat.Uid || gotStat.Gid != wantStat.Gid {
		t.Errorf("owner = %d:%d, want %d:%d", gotStat.Uid, gotStat.Gid, wantStat.Uid, wantStat.Gid)
	}
}
