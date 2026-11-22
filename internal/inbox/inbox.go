package inbox

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// maxRecordSize is the largest session registry record the package reads. It
// matches the limit Claude Code applies when it reads the registry; a larger
// file is treated as one that cannot be parsed.
const maxRecordSize = 256 << 10

// Session is one session registry record.
type Session struct {
	ConfigDir    string // the configuration directory the record lives under
	PID          int    // session process id, the record file name
	Name         string // session name, verbatim from the record
	Status       string // verbatim from the record, may be empty
	SocketPath   string // verbatim from the record; empty until the session binds its inbox
	PeerProtocol int    // peerProtocol from the record; 0 when missing or not a positive integer
}

var (
	// ErrNoRecord reports that no matching session registry record exists.
	ErrNoRecord = errors.New("no session registry record")
	// ErrPartial reports that a regular record file exists but its content
	// cannot be parsed, including an empty file.
	ErrPartial = errors.New("the session registry record is incomplete")
	// ErrBadSignature reports that sig is empty or the receiver would change
	// it.
	ErrBadSignature = errors.New("the receiver would change the signature")
	// ErrNotWritten reports that the message was certainly not written.
	ErrNotWritten = errors.New("the message was not written to the inbox socket")
	// ErrUncertain reports that the message may have been written.
	ErrUncertain = errors.New("the message may have been written to the inbox socket")
)

// resolveConfigDir returns the configuration directory to read.
//
// An empty configDir means the current account's default, $HOME/.claude. A
// non-empty value is used as given after a lexical filepath.Clean: a relative
// path stays relative and is resolved by the operating system against the
// caller's working directory. CLAUDE_CONFIG_DIR is deliberately not consulted:
// the design says the default is the account's home directory, and a caller
// that knows a different directory passes it in.
func resolveConfigDir(configDir string) (string, error) {
	if configDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("cannot resolve the home directory: %w", err)
		}
		return filepath.Join(home, ".claude"), nil
	}
	return filepath.Clean(configDir), nil
}

// sessionsDir returns the session registry directory of a resolved
// configuration directory.
func sessionsDir(configDir string) string {
	return filepath.Join(configDir, "sessions")
}

// recordFile returns the registry file of one process.
func recordFile(configDir string, pid int) string {
	return filepath.Join(sessionsDir(configDir), strconv.Itoa(pid)+".json")
}

// parseRecordName accepts the canonical "<pid>.json" spelling only. It rejects
// names such as "007.json" because strconv.Itoa would not reproduce them.
func parseRecordName(name string) (int, bool) {
	digits, ok := strings.CutSuffix(name, ".json")
	if !ok || digits == "" {
		return 0, false
	}
	// Atoi rejects non-digits but accepts a sign and leading zeros, so pid <= 0
	// rejects a negative value and the round trip through Itoa rejects "+7" and
	// other spellings such as "007".
	pid, err := strconv.Atoi(digits)
	if err != nil || pid <= 0 || strconv.Itoa(pid) != digits {
		return 0, false
	}
	return pid, true
}
