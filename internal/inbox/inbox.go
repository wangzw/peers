package inbox

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// maxRecordSize matches the limit Claude Code applies when it reads the
// registry; a larger record is treated like a deleted one and is ErrNoRecord.
const maxRecordSize = 256 << 10

// Session is one session registry record.
type Session struct {
	ConfigDir    string // the configuration directory the record lives under
	PID          int    // session process id, the record file name
	Name         string // session name, verbatim from the record
	Status       string // verbatim from the record, may be empty
	SocketPath   string // verbatim from the record; empty until the session binds its inbox
	PeerProtocol int    // peerProtocol from the record; 0 unless it is an integer from 1 to MaxInt32
}

// Errors returned by Lookup, Reread and Deliver.
var (
	ErrNoRecord = errors.New("no session registry record")
	// ErrPartial reports a regular record file whose content cannot be parsed,
	// including an empty file.
	ErrPartial = errors.New("the session registry record is incomplete")
	// ErrBadSignature reports that sig is empty or the receiver would change it.
	ErrBadSignature = errors.New("the receiver would change the signature")
	// ErrNotWritten reports that the message was certainly not written.
	ErrNotWritten = errors.New("the message was not written to the inbox socket")
	// ErrUncertain reports that the message may have been written.
	ErrUncertain = errors.New("the message may have been written to the inbox socket")
)

// resolveConfigDir returns the configuration directory to read. An empty
// configDir means this account's default, $HOME/.claude; a non-empty value is
// cleaned lexically and otherwise used as given, so a relative path stays
// relative. CLAUDE_CONFIG_DIR is deliberately not consulted.
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

func sessionsDir(configDir string) string {
	return filepath.Join(configDir, "sessions")
}

func recordFile(configDir string, pid int) string {
	return filepath.Join(sessionsDir(configDir), strconv.Itoa(pid)+".json")
}

// parseRecordName accepts the canonical "<pid>.json" spelling only: a name such
// as "007.json" is rejected because strconv.Itoa would not reproduce it.
func parseRecordName(name string) (int, bool) {
	digits, ok := strings.CutSuffix(name, ".json")
	if !ok || digits == "" {
		return 0, false
	}
	pid, err := strconv.Atoi(digits)
	if err != nil || pid <= 0 || strconv.Itoa(pid) != digits {
		return 0, false
	}
	return pid, true
}
