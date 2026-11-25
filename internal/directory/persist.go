package directory

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"time"
)

const fileName = "directory.json"

const fileFormat = 1

// errUnreadable reports that directory.json cannot be used: its content does
// not parse or its format is unknown. A read failure is returned as itself so
// that startup fails instead of silently replacing the file.
var errUnreadable = errors.New("directory.json is unreadable")

type directoryFile struct {
	Format        int          `json:"format"`
	HighestIssued uint64       `json:"highest_issued"`
	Records       []fileRecord `json:"records"`
}

type fileRecord struct {
	Raw       []byte `json:"raw"`
	Signature []byte `json:"signature"`
	Source    string `json:"source"`
}

// saveFile writes directory.json; it does not sort, so the caller must pass
// records in a stable order for identical content to produce identical bytes.
func saveFile(ops fileOps, dir string, highest uint64, recs []fileRecord) error {
	if ops == nil {
		ops = realFileOps{}
	}
	data, err := json.Marshal(directoryFile{
		Format:        fileFormat,
		HighestIssued: highest,
		Records:       recs,
	})
	if err != nil {
		return fmt.Errorf("cannot encode the directory file: %w", err)
	}
	return atomicWrite(ops, filepath.Join(dir, fileName), data)
}

// loadFile reads directory.json without verifying signatures or Source.
func loadFile(ops fileOps, dir string) (recs []fileRecord, highest uint64, err error) {
	if ops == nil {
		ops = realFileOps{}
	}
	path := filepath.Join(dir, fileName)
	data, err := ops.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("cannot read %s: %w", path, err)
	}
	var f directoryFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, 0, errUnreadable
	}
	if f.Format != fileFormat {
		return nil, 0, errUnreadable
	}
	return f.Records, f.HighestIssued, nil
}

// atomicWrite writes to a temporary file in the same directory, fsyncs it,
// renames it over path and fsyncs the directory. A failed write or rename
// leaves path untouched; a failed directory sync happens after the rename, so
// path may already hold the new content.
func atomicWrite(ops fileOps, path string, data []byte) error {
	if ops == nil {
		ops = realFileOps{}
	}
	tmp := path + ".tmp-" + randomSuffix()
	if err := ops.WriteFile(tmp, data, 0o600); err != nil {
		_ = ops.Remove(tmp)
		return fmt.Errorf("cannot write %s: %w", tmp, err)
	}
	if err := ops.Rename(tmp, path); err != nil {
		_ = ops.Remove(tmp)
		return fmt.Errorf("cannot rename %s to %s: %w", tmp, path, err)
	}
	if err := ops.SyncDir(filepath.Dir(path)); err != nil {
		_ = ops.Remove(tmp)
		return fmt.Errorf("cannot sync %s: %w", filepath.Dir(path), err)
	}
	return nil
}

func randomSuffix() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
