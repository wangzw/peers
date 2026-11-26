//go:build linux

package harness

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// processStartTime returns field 22 of /proc/<pid>/stat, the process start time
// in clock ticks since boot.
func processStartTime(pid int) (uint64, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, err
	}
	text := string(data)
	// The command name is parenthesised and may contain spaces and parentheses, so
	// only the fields after the last ')' are split; they start at state (field 3)
	// and starttime is field 22.
	close := strings.LastIndexByte(text, ')')
	if close < 0 || close+1 >= len(text) {
		return 0, fmt.Errorf("harness: malformed stat for process %d", pid)
	}
	fields := strings.Fields(text[close+1:])
	if len(fields) < 20 {
		return 0, fmt.Errorf("harness: malformed stat for process %d", pid)
	}
	return strconv.ParseUint(fields[19], 10, 64)
}

func processNamespace(pid int) (uint64, error) {
	link, err := os.Readlink(fmt.Sprintf("/proc/%d/ns/pid", pid))
	if err != nil {
		return 0, err
	}
	open := strings.IndexByte(link, '[')
	close := strings.IndexByte(link, ']')
	if open < 0 || close <= open+1 {
		return 0, fmt.Errorf("harness: malformed pid namespace link %q", link)
	}
	return strconv.ParseUint(link[open+1:close], 10, 64)
}
