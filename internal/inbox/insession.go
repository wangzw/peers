package inbox

import (
	"bytes"
	"os"
)

// sessionEnvVar is the environment variable with which Claude Code marks the
// commands and MCP servers it starts inside a session.
const sessionEnvVar = "CLAUDE_CODE_MESSAGING_SOCKET"

// maxAncestorDepth bounds the walk up the parent chain. A real process tree is
// far shallower; the bound only stops a malformed or cyclic process table from
// looping forever.
const maxAncestorDepth = 256

// readParentPID and readEnviron read one process's parent and environment
// without sending it a signal. Tests replace them to model a process tree
// without depending on the real one.
var (
	readParentPID = platformParentPID
	readEnviron   = platformEnviron
)

// InSession reports whether the current process belongs to the process tree of
// a Claude Code session, which Claude Code marks by exporting the session's
// inbox socket path.
//
// The current process's own environment is consulted first. If it is clean, the
// walk continues up the parent chain: an ancestor that carries the variable, or
// whose process id has a record in this account's session registry, proves the
// session. An ancestor whose environment cannot be read, for example because of
// permissions, is skipped and the walk continues. When the parent process id of
// the current pid cannot be read, the walk stops and the process is treated as
// not in a session: without a parent there is no further ancestor to check, and
// guessing would risk refusing to start outside a session. A circular or very
// long chain stops at maxAncestorDepth.
//
// A build carrying the peers_testenv tag may turn off the parent-chain walk, so
// a test sandbox started from a process that itself lives inside a session can
// run the Broker. The own-environment check above stays in force, because it is
// what the refusal is about; a release build never turns the walk off.
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
// records every session of this account. CLAUDE_CONFIG_DIR wins when it is set,
// as it does for Claude Code; otherwise the account's default ~/.claude is
// used, the same directory Lookup reads through its default.
func sessionConfigDir() (string, error) {
	if dir := os.Getenv("CLAUDE_CONFIG_DIR"); dir != "" {
		return resolveConfigDir(dir)
	}
	return resolveConfigDir("")
}

// sessionRecordExists reports whether the registry holds a record for pid. The
// record data is not read: the presence of a regular file named <pid>.json is
// what marks the process as a session, the same file Lookup would parse.
func sessionRecordExists(configDir string, pid int) bool {
	info, err := os.Lstat(recordFile(configDir, pid))
	if err != nil {
		return false
	}
	return info.Mode().IsRegular()
}

// environmentHasSession reports whether env carries a non-empty
// CLAUDE_CODE_MESSAGING_SOCKET.
func environmentHasSession(env []string) bool {
	prefix := sessionEnvVar + "="
	for _, entry := range env {
		if len(entry) > len(prefix) && entry[:len(prefix)] == prefix {
			return true
		}
	}
	return false
}

// parseNulList splits a NUL-separated block into its non-empty entries. Empty
// entries, such as the padding zeros the kernel places between the argument and
// environment areas, are dropped.
func parseNulList(data []byte) []string {
	var entries []string
	for len(data) > 0 {
		end := bytes.IndexByte(data, 0)
		if end < 0 {
			entries = append(entries, string(data))
			break
		}
		if end > 0 {
			entries = append(entries, string(data[:end]))
		}
		data = data[end+1:]
	}
	return entries
}

// parseProcargs2 returns the strings packed by the macOS kern.procargs2 sysctl.
// The buffer starts with a 32-bit argument count, then holds the NUL-terminated
// argument strings and the environment, with zero padding between the two. Only
// an exact KEY=value match is looked for, so splitting the whole region is
// enough and the padding is harmless.
func parseProcargs2(raw []byte) []string {
	if len(raw) < 4 {
		return nil
	}
	return parseNulList(raw[4:])
}
