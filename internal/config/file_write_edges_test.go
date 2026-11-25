package config

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

// assertOpenFlags checks the O_NOFOLLOW requirement on one recorded open call
// and, when wantCreate is set, that the call creates a new file exclusively.
func assertOpenFlags(t *testing.T, call int, flags []int, wantCreate bool) {
	t.Helper()
	if call >= len(flags) {
		t.Fatalf("open calls = %d, want call %d", len(flags), call+1)
	}
	got := flags[call]
	if got&unix.O_NOFOLLOW == 0 {
		t.Errorf("open call %d flags = %#x, want O_NOFOLLOW", call+1, got)
	}
	if wantCreate {
		if got&os.O_CREATE == 0 || got&os.O_EXCL == 0 {
			t.Errorf("open call %d flags = %#x, want O_CREATE and O_EXCL", call+1, got)
		}
	} else if got&os.O_CREATE != 0 {
		t.Errorf("open call %d flags = %#x, want no O_CREATE", call+1, got)
	}
}

// TestWriteConfigFileReportsDirectoryCreationFailure checks that a failed
// parent-directory creation is reported as ErrFileWrite.
func TestWriteConfigFileReportsDirectoryCreationFailure(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "missing-target")
	link := filepath.Join(dir, "config-dir")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}

	err := writeConfigFile(filepath.Join(link, "config.yaml"), []byte("broker:\n  alias: a\n"), ModeUser)
	if err == nil {
		t.Fatal("writeConfigFile wrote through a dangling directory symlink")
	}
	if !errors.Is(err, ErrFileWrite) {
		t.Fatalf("writeConfigFile error = %v, want ErrFileWrite", err)
	}
	if !strings.Contains(err.Error(), "mkdir") {
		t.Fatalf("writeConfigFile error = %v, want the directory-creation failure", err)
	}
}

// TestWriteConfigFileReportsTruncateFailure checks that a file that opens but
// cannot be truncated is reported as ErrFileWrite.
func TestWriteConfigFileReportsTruncateFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.fifo")
	if err := unix.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}

	err := writeConfigFile(path, []byte("broker:\n  alias: a\n"), ModeUser)
	if err == nil {
		t.Fatal("writeConfigFile accepted a FIFO")
	}
	if !errors.Is(err, ErrFileWrite) {
		t.Fatalf("writeConfigFile error = %v, want ErrFileWrite", err)
	}
	if !strings.Contains(err.Error(), "truncate") {
		t.Fatalf("writeConfigFile error = %v, want the truncate failure", err)
	}
	info, statErr := os.Stat(path)
	if statErr != nil {
		t.Fatal(statErr)
	}
	if info.Mode()&os.ModeNamedPipe == 0 {
		t.Fatalf("the FIFO was replaced by a %v", info.Mode())
	}
}

// TestWriteConfigFileReopensAFileCreatedConcurrently checks that a file
// created between the first open and the exclusive create is reopened and
// written in place.
func TestWriteConfigFileReopensAFileCreatedConcurrently(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	old := openConfigFileForWrite
	t.Cleanup(func() { openConfigFileForWrite = old })
	var flags []int
	openConfigFileForWrite = func(name string, flag int, perm os.FileMode) (*os.File, error) {
		flags = append(flags, flag)
		if flag&os.O_CREATE != 0 {
			created, err := os.OpenFile(name, flag, perm)
			if err != nil {
				return nil, err
			}
			_ = created.Close()
			return nil, &os.PathError{Op: "open", Path: name, Err: syscall.EEXIST}
		}
		return os.OpenFile(name, flag, perm)
	}

	data := []byte("broker:\n  alias: a\n")
	if err := writeConfigFile(path, data, ModeUser); err != nil {
		t.Fatalf("writeConfigFile: %v", err)
	}
	if len(flags) != 3 {
		t.Fatalf("open calls = %d, want 3", len(flags))
	}
	assertOpenFlags(t, 0, flags, false)
	assertOpenFlags(t, 1, flags, true)
	assertOpenFlags(t, 2, flags, false)
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("file content = %q, want %q", got, data)
	}
}

// TestWriteConfigFileReportsFailedReopen checks that a file that disappears
// before it can be reopened is reported as ErrFileWrite.
func TestWriteConfigFileReportsFailedReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	old := openConfigFileForWrite
	t.Cleanup(func() { openConfigFileForWrite = old })
	var flags []int
	openConfigFileForWrite = func(name string, flag int, perm os.FileMode) (*os.File, error) {
		flags = append(flags, flag)
		if flag&os.O_CREATE != 0 {
			created, err := os.OpenFile(name, flag, perm)
			if err == nil {
				_ = created.Close()
			}
			_ = os.Remove(name)
			return nil, &os.PathError{Op: "open", Path: name, Err: syscall.EEXIST}
		}
		return os.OpenFile(name, flag, perm)
	}

	err := writeConfigFile(path, []byte("broker:\n  alias: a\n"), ModeUser)
	if !errors.Is(err, ErrFileWrite) {
		t.Fatalf("writeConfigFile error = %v, want ErrFileWrite", err)
	}
	if len(flags) != 3 {
		t.Fatalf("open calls = %d, want 3", len(flags))
	}
	assertOpenFlags(t, 1, flags, true)
	assertOpenFlags(t, 2, flags, false)
}

// TestWriteConfigFileReportsModeFailure checks that a failed chmod on the file
// created by the exclusive open is reported as ErrFileWrite.
func TestWriteConfigFileReportsModeFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	created, err := os.CreateTemp(dir, "created-")
	if err != nil {
		t.Fatal(err)
	}
	if err := created.Close(); err != nil {
		t.Fatal(err)
	}

	old := openConfigFileForWrite
	t.Cleanup(func() { openConfigFileForWrite = old })
	var flags []int
	openConfigFileForWrite = func(name string, flag int, perm os.FileMode) (*os.File, error) {
		flags = append(flags, flag)
		if flag&os.O_CREATE != 0 {
			return created, nil
		}
		return os.OpenFile(name, flag, perm)
	}

	err = writeConfigFile(path, []byte("broker:\n  alias: a\n"), ModeUser)
	if !errors.Is(err, ErrFileWrite) {
		t.Fatalf("writeConfigFile error = %v, want ErrFileWrite", err)
	}
	if !strings.Contains(err.Error(), "chmod") {
		t.Fatalf("writeConfigFile error = %v, want the chmod failure", err)
	}
	if len(flags) != 2 {
		t.Fatalf("open calls = %d, want 2", len(flags))
	}
}
