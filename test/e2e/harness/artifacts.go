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

const artifactsEnv = "E2E_ARTIFACTS"

// pairingKeyPattern matches a one-time pairing key: four groups of four Crockford Base32 characters.
var pairingKeyPattern = regexp.MustCompile(`\b[0-9A-HJKMNP-TV-Z]{4}(?:-[0-9A-HJKMNP-TV-Z]{4}){3}\b`)

// secretEnvPattern matches a line that assigns a secret-bearing environment variable.
var secretEnvPattern = regexp.MustCompile(`(?m)^([A-Z0-9_]*(?:KEY|TOKEN|SECRET)[A-Z0-9_]*)=.*$`)

func redactSecrets(data []byte) []byte {
	text := secretEnvPattern.ReplaceAllString(string(data), "$1=[redacted]")
	text = pairingKeyPattern.ReplaceAllString(text, "[redacted-pairing-key]")
	return []byte(text)
}

func artifactSubdir(root string, t testing.TB) string {
	name := t.Name()
	for _, separator := range []string{"/", "\\", ":", " "} {
		name = strings.ReplaceAll(name, separator, "_")
	}
	return filepath.Join(root, name)
}

func writeArtifact(t testing.TB, dir, name string, data []byte) {
	t.Helper()
	data = redactSecrets(data)
	if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
		t.Errorf("harness: writing artifact %s: %v", filepath.Join(dir, name), err)
	}
}

// collectArtifacts, when E2E_ARTIFACTS is set, writes only the Broker and MCP
// server logs to that directory through redactSecrets, adding no environment,
// configuration file or session record, because the dump leaves the test host
// and those could carry secrets; with E2E_ARTIFACTS unset it only logs a tail of
// each file to the test output.
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

type artifactFile struct {
	source string
	name   string
}

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

// quitDump returns the goroutine dump a Go runtime writes on SIGQUIT, from the
// last "SIGQUIT: quit" marker to the end. The stacks come first and the
// register dump last, so a tail alone can miss them; a log with no marker falls
// back to its tail.
func quitDump(data []byte) string {
	text := string(data)
	if i := strings.LastIndex(text, "SIGQUIT: quit"); i >= 0 {
		return text[i:]
	}
	return tail(text, 200)
}

// brokerStack sends SIGQUIT to the node's Broker and returns the goroutine dump
// from its log; because SIGQUIT terminates the Broker, this is evidence
// collection for a failing or stalled run only. The Broker handle's lifecycle
// lock spans the reaped check and the send, so a Broker this node already
// waited for is never signalled through a reused pid.
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
	// The runtime writes the dump before exiting, so wait for the exit before reading.
	_, _ = broker.Wait(3 * time.Second)
	data, err := os.ReadFile(logPath)
	if err != nil {
		return fmt.Sprintf("reading Broker log %s: %v", logPath, err)
	}
	return quitDump(data)
}
