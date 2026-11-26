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
// under the given configuration directory, and returns ErrNoRecord when none
// matches. An empty configDir means this account's default, $HOME/.claude. An
// empty socketPath never matches. When several records carry the same path, the
// newest modification time wins, and equal times favour the larger process id.
// A home directory or registry that cannot be read returns that error as is.
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

// Reread re-reads the record named by s.PID in s.ConfigDir, where an empty
// configDir means this account's default, $HOME/.claude. It does not rescan the
// directory, compare the socket path with the previous value or judge whether the
// session is alive. A missing or non-regular record, or one larger than the size
// limit, is ErrNoRecord; only an unparseable regular file is ErrPartial. A home
// directory or record that cannot be read returns that error as is.
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

// parseRecord accepts a JSON object: missing or mistyped fields become empty, and
// an invalid peerProtocol becomes 0. A syntactically valid array, string or
// number is ErrPartial; null parses to an empty record.
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

func protocolField(fields map[string]any, key string) int {
	value, ok := fields[key].(float64)
	if !ok || value != math.Trunc(value) || value < 1 || value > math.MaxInt32 {
		return 0
	}
	return int(value)
}

func readRecord(path string, pid int) (Session, error) {
	data, err := readRecordData(path)
	if err != nil {
		return Session{}, err
	}
	return parseRecord(data, pid)
}

// readRecordData returns the bytes of a regular record file. It inspects the
// path before opening it, because opening a FIFO would block until a writer
// appears and a Unix socket file cannot be read; a symlink, FIFO, socket,
// directory, device or oversized file is ErrNoRecord like a deleted record. The
// open uses O_NOFOLLOW and O_NONBLOCK so a path replaced after the inspection
// cannot turn into a symlink or a FIFO.
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
