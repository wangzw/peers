package inbox

import (
	"os"
	"strings"
)

const sessionEnvVar = "CLAUDE_CODE_MESSAGING_SOCKET"

// maxAncestorDepth bounds the walk up the parent chain, so a malformed or
// cyclic process table cannot loop forever.
const maxAncestorDepth = 256

// Tests replace readParentPID and readEnviron to model a process tree without
// depending on the real one.
var (
	readParentPID = platformParentPID
	readEnviron   = platformEnviron
)

// InSession reports whether the current process belongs to the process tree of
// a Claude Code session. The process's own environment is consulted first; then
// each ancestor counts if its environment carries the session's inbox socket
// path or its process id has a record in this account's session registry. An
// ancestor whose environment cannot be read is skipped, and the walk stops as
// not in a session when an ancestor's parent cannot be read.
//
// A peers_testenv build may turn off the ancestor walk; a release build never
// does.
func InSession() bool {
	if os.Getenv(sessionEnvVar) != "" {
		return true
	}
	if skipAncestorSessionCheck() {
		return false
	}
	configDir, dirErr := sessionConfigDir()
	pid := os.Getppid()
	for depth := 0; depth < maxAncestorDepth && pid > 1; depth++ {
		if dirErr == nil && sessionRecordExists(configDir, pid) {
			return true
		}
		if env, err := readEnviron(pid); err == nil && environmentHasSession(env) {
			return true
		}
		next, err := readParentPID(pid)
		if err != nil || next <= 1 || next == pid {
			return false
		}
		pid = next
	}
	return false
}

// sessionConfigDir returns the configuration directory whose session registry
// belongs to this account. CLAUDE_CONFIG_DIR wins when set, as it does for
// Claude Code.
func sessionConfigDir() (string, error) {
	if dir := os.Getenv("CLAUDE_CONFIG_DIR"); dir != "" {
		return resolveConfigDir(dir)
	}
	return resolveConfigDir("")
}

func sessionRecordExists(configDir string, pid int) bool {
	info, err := os.Lstat(recordFile(configDir, pid))
	if err != nil {
		return false
	}
	return info.Mode().IsRegular()
}

func environmentHasSession(env []string) bool {
	prefix := sessionEnvVar + "="
	for _, entry := range env {
		if strings.HasPrefix(entry, prefix) && len(entry) > len(prefix) {
			return true
		}
	}
	return false
}

// parseNulList splits a NUL-separated block into its non-empty entries, dropping
// the padding zeros the kernel places between the argument and environment
// areas.
func parseNulList(data []byte) []string {
	return strings.FieldsFunc(string(data), func(r rune) bool { return r == 0 })
}

// parseProcargs2 returns the strings packed by the macOS kern.procargs2 sysctl:
// a 32-bit argument count, then the NUL-terminated argument strings, zero
// padding, and the environment. Only an exact KEY=value match is looked for, so
// splitting the whole region is enough.
func parseProcargs2(raw []byte) []string {
	if len(raw) < 4 {
		return nil
	}
	return parseNulList(raw[4:])
}
