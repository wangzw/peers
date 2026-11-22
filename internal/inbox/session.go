package inbox

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// Lookup finds the session record whose messagingSocketPath equals socketPath
// under the given configuration directory. An empty socketPath never matches:
// a session that has not bound its inbox cannot be the intended target.
//
// When several records carry the same path, the newest modification time wins;
// equal times are resolved in favour of the larger process id so that the
// result is deterministic.
func Lookup(configDir, socketPath string) (Session, error) {
	dir, err := resolveConfigDir(configDir)
	if err != nil {
		return Session{}, err
	}
	if socketPath == "" {
		return Session{}, ErrNoRecord
	}
	sessions := sessionsDir(dir)
	entries, err := os.ReadDir(sessions)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
			return Session{}, ErrNoRecord
		}
		return Session{}, fmt.Errorf("cannot read the session registry %q: %w", sessions, err)
	}

	var (
		best      Session
		bestMod   time.Time
		bestFound bool
	)
	for _, entry := range entries {
		pid, ok := parseRecordName(entry.Name())
		if !ok {
			continue
		}
		path := filepath.Join(sessions, entry.Name())
		s, err := readRecord(path, pid)
		if err != nil {
			// A half-written, replaced or vanished record is skipped; one
			// unreadable record must not hide the rest. Other errors, such as
			// a permission failure, are reported to the caller.
			if errors.Is(err, ErrPartial) || errors.Is(err, ErrNoRecord) {
				continue
			}
			return Session{}, err
		}
		if s.SocketPath != socketPath {
			continue
		}
		info, err := os.Lstat(path)
		if err != nil {
			continue
		}
		mod := info.ModTime()
		if !bestFound || mod.After(bestMod) || (mod.Equal(bestMod) && s.PID > best.PID) {
			s.ConfigDir = dir
			best = s
			bestMod = mod
			bestFound = true
		}
	}
	if !bestFound {
		return Session{}, ErrNoRecord
	}
	return best, nil
}

// Reread re-reads the record named by s.PID in s.ConfigDir. It does not rescan
// the directory, compare the inbox socket path with the previous value, or
// judge whether the session is still alive. A record whose socket path became
// empty or changed is returned verbatim; the broker decides what that means. A
// record that is missing, no longer a regular file, or larger than the size
// limit is ErrNoRecord; only an unparseable regular file is ErrPartial.
func Reread(s Session) (Session, error) {
	dir, err := resolveConfigDir(s.ConfigDir)
	if err != nil {
		return Session{}, err
	}
	if s.PID <= 0 {
		return Session{}, ErrNoRecord
	}
	next, err := readRecord(recordFile(dir, s.PID), s.PID)
	if err != nil {
		return Session{}, err
	}
	next.ConfigDir = dir
	return next, nil
}

// parseRecord reads the four string fields the package depends on, plus
// peerProtocol, which is carried to the caller but not judged here. Any
// syntactically valid JSON is accepted; missing fields and fields of the wrong
// type become empty strings, and an invalid peerProtocol becomes 0 without
// making the record ErrPartial. Only malformed JSON is ErrPartial.
func parseRecord(data []byte, pid int) (Session, error) {
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		return Session{}, ErrPartial
	}
	return Session{
		PID:          pid,
		Name:         stringField(fields, "name"),
		Status:       stringField(fields, "status"),
		SocketPath:   stringField(fields, "messagingSocketPath"),
		PeerProtocol: protocolField(fields, "peerProtocol"),
	}, nil
}

func stringField(fields map[string]any, key string) string {
	value, ok := fields[key].(string)
	if !ok {
		return ""
	}
	return value
}

// protocolField returns fields[key] as a protocol version: a JSON number that
// is an integer between 1 and math.MaxInt32. Anything else yields 0.
func protocolField(fields map[string]any, key string) int {
	value, ok := fields[key].(float64)
	if !ok || value != math.Trunc(value) || value < 1 || value > math.MaxInt32 {
		return 0
	}
	return int(value)
}

// readRecord reads and parses <pid>.json without following symlinks.
func readRecord(path string, pid int) (Session, error) {
	data, err := readRecordData(path)
	if err != nil {
		return Session{}, err
	}
	return parseRecord(data, pid)
}

// readRecordData returns the bytes of a regular record file.
//
// It inspects the path before opening it: opening a FIFO would block until a
// writer appears, and a Unix socket file cannot be read at all. A symbolic
// link, FIFO, socket, directory, device or a file larger than maxRecordSize is
// ErrNoRecord, the same as a record that has been deleted: such an entry is no
// longer a record this package can read, and a caller must not keep using the
// previous value. The read itself uses O_NOFOLLOW and O_NONBLOCK so that
// replacing the path after the inspection cannot make it follow a link or
// block on a FIFO.
func readRecordData(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
			return nil, ErrNoRecord
		}
		return nil, fmt.Errorf("cannot inspect session record %q: %w", path, err)
	}
	if !info.Mode().IsRegular() || info.Size() > maxRecordSize {
		return nil, ErrNoRecord
	}

	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		switch {
		case errors.Is(err, fs.ErrNotExist), errors.Is(err, syscall.ENOTDIR):
			return nil, ErrNoRecord
		case errors.Is(err, syscall.ELOOP):
			return nil, ErrNoRecord
		default:
			return nil, fmt.Errorf("cannot open session record %q: %w", path, err)
		}
	}
	defer func() { _ = file.Close() }()

	info, err = file.Stat()
	if err != nil {
		return nil, fmt.Errorf("cannot inspect session record %q: %w", path, err)
	}
	if !info.Mode().IsRegular() || info.Size() > maxRecordSize {
		return nil, ErrNoRecord
	}
	data, err := io.ReadAll(io.LimitReader(file, maxRecordSize+1))
	if err != nil {
		return nil, fmt.Errorf("cannot read session record %q: %w", path, err)
	}
	if len(data) > maxRecordSize {
		return nil, ErrNoRecord
	}
	return data, nil
}
