package pairing

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

type fileOps interface {
	WriteFile(path string, data []byte, mode os.FileMode) error
	Rename(oldpath, newpath string) error
	SyncDir(dir string) error
	ReadFile(path string) ([]byte, error)
	Remove(path string) error
}

type realFileOps struct{}

func (realFileOps) WriteFile(path string, data []byte, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func (realFileOps) Rename(oldpath, newpath string) error { return os.Rename(oldpath, newpath) }

func (realFileOps) SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

func (realFileOps) ReadFile(path string) ([]byte, error) { return os.ReadFile(path) }

func (realFileOps) Remove(path string) error { return os.Remove(path) }

// A directory-sync failure after a successful rename is logged, not returned:
// the rename already replaced path, so reporting failure would leave memory on
// the old table while disk holds the new one.
func atomicWrite(ops fileOps, path string, data []byte, mode os.FileMode) error {
	if ops == nil {
		ops = realFileOps{}
	}
	dir := filepath.Dir(path)
	tmp := filepath.Join(dir, fmt.Sprintf(".%s.tmp-%s", filepath.Base(path), tempSuffix()))
	if err := ops.WriteFile(tmp, data, mode); err != nil {
		_ = ops.Remove(tmp)
		return err
	}
	if err := ops.Rename(tmp, path); err != nil {
		_ = ops.Remove(tmp)
		return err
	}
	if err := ops.SyncDir(dir); err != nil {
		slog.Error("pairing: sync directory after rename", "dir", dir, "path", path, "error", err)
	}
	return nil
}

func tempSuffix() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err == nil {
		return hex.EncodeToString(b[:])
	}
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func readFileOrNotExist(ops fileOps, path string) (data []byte, missing bool, err error) {
	if ops == nil {
		ops = realFileOps{}
	}
	data, err = ops.ReadFile(path)
	if err == nil {
		return data, false, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return nil, true, nil
	}
	return nil, false, err
}
