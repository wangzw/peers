package harness

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/test/e2e/node"
)

// artifactsEnv names the directory a failing test writes its diagnostic dump
// to. Both the local and the container backends use it, and CI uploads it as
// an artifact. An unset value keeps the dump in the test log instead.
const artifactsEnv = "E2E_ARTIFACTS"

// pairingKeyPattern matches a one-time pairing key: four groups of four
// Crockford Base32 characters. Broker logs must not carry one, and this
// scrubbing is the second line of defence before the dump is uploaded.
var pairingKeyPattern = regexp.MustCompile(`\b[0-9A-HJKMNP-TV-Z]{4}(?:-[0-9A-HJKMNP-TV-Z]{4}){3}\b`)

// secretEnvPattern matches a line that assigns a secret-bearing environment
// variable, which must never leave the test host.
var secretEnvPattern = regexp.MustCompile(`(?m)^([A-Z0-9_]*(?:KEY|TOKEN|SECRET)[A-Z0-9_]*)=.*$`)

// redactSecrets removes pairing keys and secret environment assignments from
// text before it is written to an artifact.
func redactSecrets(data []byte) []byte {
	text := secretEnvPattern.ReplaceAllString(string(data), "$1=[redacted]")
	text = pairingKeyPattern.ReplaceAllString(text, "[redacted-pairing-key]")
	return []byte(text)
}

// artifactSubdir returns the directory one test's dump goes to, below the
// configured root. A test name contains slashes for subtests, so they are
// flattened into one directory name.
func artifactSubdir(root string, t testing.TB) string {
	name := t.Name()
	for _, separator := range []string{"/", "\\", ":", " "} {
		name = strings.ReplaceAll(name, separator, "_")
	}
	return filepath.Join(root, name)
}

// writeArtifact writes data to dir/name with secrets redacted.
func writeArtifact(t testing.TB, dir, name string, data []byte) {
	t.Helper()
	data = redactSecrets(data)
	if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
		t.Errorf("harness: writing artifact %s: %v", filepath.Join(dir, name), err)
	}
}

// collectArtifacts copies the node's Broker and MCP server logs to
// E2E_ARTIFACTS when the test failed. Without the variable it logs their tails
// instead. Only log files are written: no environment, no configuration file
// and no session record, and the logs pass through redactSecrets.
func (l *localNode) collectArtifacts() {
	files := l.artifactFiles()
	root := os.Getenv(artifactsEnv)
	if root == "" {
		for _, file := range files {
			data, err := os.ReadFile(file.source)
			if err != nil {
				continue
			}
			l.t.Logf("harness: node %s, %s (tail):\n%s", l.name, file.name, tail(string(data), 50))
		}
		return
	}
	dir := filepath.Join(artifactSubdir(root, l.t), l.name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		l.t.Errorf("harness: creating artifact directory %s: %v", dir, err)
		return
	}
	for _, file := range files {
		data, err := os.ReadFile(file.source)
		if err != nil {
			continue
		}
		writeArtifact(l.t, dir, file.name, data)
	}
}

// artifactFile is one source log and the name it takes in the dump.
type artifactFile struct {
	source string
	name   string
}

// artifactFiles lists the Broker logs and the MCP server logs this node wrote.
func (l *localNode) artifactFiles() []artifactFile {
	l.mu.Lock()
	root := l.root
	home := l.home
	l.mu.Unlock()
	var files []artifactFile
	for _, pattern := range []struct{ dir, glob string }{
		{root, "broker-*.log"},
		{home, "mcp-*.log"},
	} {
		matches, err := filepath.Glob(filepath.Join(pattern.dir, pattern.glob))
		if err != nil {
			continue
		}
		for _, path := range matches {
			files = append(files, artifactFile{source: path, name: filepath.Base(path)})
		}
	}
	return files
}

// quitDump returns the goroutine dump a Go runtime writes on SIGQUIT: the
// text from the last "SIGQUIT: quit" marker to the end. The dump begins with
// the goroutine stacks and ends with a register dump, so the tail of the log
// alone can miss the stacks when the process had many goroutines. A log with
// no marker falls back to its tail.
func quitDump(data []byte) string {
	text := string(data)
	if i := strings.LastIndex(text, "SIGQUIT: quit"); i >= 0 {
		return text[i:]
	}
	return tail(text, 200)
}

// brokerStack sends SIGQUIT to the node's Broker and returns the stack the Go
// runtime writes to the Broker log. The signal goes through the Broker handle,
// which holds its lifecycle lock across the reaped check and the send, so a
// Broker this node already waited for is never signalled through a pid that
// may name another process. SIGQUIT terminates the Broker after the dump, so
// this is evidence collection for a failing or stalled run only.
func (l *localNode) brokerStack(t testing.TB) string {
	t.Helper()
	l.mu.Lock()
	broker := l.broker
	logPath := l.brokerLogPath
	l.mu.Unlock()
	if broker == nil {
		return fmt.Sprintf("no Broker is running on node %s", l.name)
	}
	if err := broker.SendSignal(syscall.SIGQUIT); err != nil {
		if errors.Is(err, node.ErrBrokerReaped) {
			return fmt.Sprintf("Broker on node %s has already been reaped; no signal was sent", l.name)
		}
		return fmt.Sprintf("sending SIGQUIT to the Broker on node %s: %v", l.name, err)
	}
	// The runtime writes the dump before exiting; wait for the exit so the log
	// is complete, then read it.
	_, _ = broker.Wait(3 * time.Second)
	data, err := os.ReadFile(logPath)
	if err != nil {
		return fmt.Sprintf("reading Broker log %s: %v", logPath, err)
	}
	return quitDump(data)
}
