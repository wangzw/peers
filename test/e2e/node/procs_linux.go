//go:build linux

package node

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

// ListProcs returns the processes match accepts, ordered by PID. A process
// that exits during the scan is skipped. Reading the command line and the
// environment is best effort: the environment of another user's process is
// left nil.
func ListProcs(match func(Proc) bool) ([]Proc, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, fmt.Errorf("node: listing processes: %w", err)
	}
	procs := make([]Proc, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		proc, err := readProc(pid)
		if err != nil {
			continue
		}
		if match == nil || match(proc) {
			procs = append(procs, proc)
		}
	}
	sort.Slice(procs, func(i, j int) bool { return procs[i].PID < procs[j].PID })
	return procs, nil
}

func readProc(pid int) (Proc, error) {
	ppid, sid, err := readProcStat(pid)
	if err != nil {
		return Proc{}, err
	}
	proc := Proc{PID: pid, PPID: ppid, SID: sid}
	proc.UID = readProcUID(pid)
	proc.Args = readProcArgs(pid)
	proc.Env = readProcEnv(pid)
	return proc, nil
}

func readProcStat(pid int) (ppid, sid int, err error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, 0, err
	}
	text := string(data)
	// The command name in field 2 is parenthesised and may itself contain
	// spaces and parentheses, so the fields after the last ')' are the only
	// ones that can be split.
	close := strings.LastIndexByte(text, ')')
	if close < 0 || close+1 >= len(text) {
		return 0, 0, fmt.Errorf("node: malformed stat for process %d", pid)
	}
	fields := strings.Fields(text[close+1:])
	// fields[0] is state, fields[1] ppid, fields[2] pgrp, fields[3] session.
	if len(fields) < 4 {
		return 0, 0, fmt.Errorf("node: malformed stat for process %d", pid)
	}
	ppid, err = strconv.Atoi(fields[1])
	if err != nil {
		return 0, 0, fmt.Errorf("node: malformed parent PID for process %d: %w", pid, err)
	}
	sid, err = strconv.Atoi(fields[3])
	if err != nil {
		return 0, 0, fmt.Errorf("node: malformed session ID for process %d: %w", pid, err)
	}
	return ppid, sid, nil
}

func readProcUID(pid int) int {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return -1
	}
	for _, line := range strings.Split(string(data), "\n") {
		rest, ok := strings.CutPrefix(line, "Uid:")
		if !ok {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			return -1
		}
		uid, err := strconv.Atoi(fields[0])
		if err != nil {
			return -1
		}
		return uid
	}
	return -1
}

func readProcArgs(pid int) []string {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
	if err != nil || len(data) == 0 {
		return nil
	}
	return splitNUL(data)
}

func readProcEnv(pid int) map[string]string {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", pid))
	if err != nil {
		return nil
	}
	entries := splitNUL(data)
	env := make(map[string]string, len(entries))
	for _, entry := range entries {
		name, value, ok := strings.Cut(entry, "=")
		if ok {
			env[name] = value
		}
	}
	return env
}

func splitNUL(data []byte) []string {
	parts := strings.Split(string(data), "\x00")
	if n := len(parts); n > 0 && parts[n-1] == "" {
		parts = parts[:n-1]
	}
	return parts
}

// Ancestors returns the PIDs from pid's parent up to and including 1. A
// process that disappears during the walk is an error.
func Ancestors(pid int) ([]int, error) {
	ancestors := make([]int, 0, 8)
	for {
		if len(ancestors) > 4096 {
			return nil, fmt.Errorf("node: ancestor chain of process %d does not reach 1", pid)
		}
		ppid, _, err := readProcStat(pid)
		if err != nil {
			return nil, fmt.Errorf("node: reading ancestors of process %d: %w", pid, err)
		}
		ancestors = append(ancestors, ppid)
		if ppid <= 1 {
			return ancestors, nil
		}
		pid = ppid
	}
}
