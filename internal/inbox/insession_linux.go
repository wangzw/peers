//go:build linux

package inbox

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// platformParentPID returns the parent process id of pid, read from
// /proc/<pid>/stat.
func platformParentPID(pid int) (int, error) {
	data, err := os.ReadFile(procPath(pid, "stat"))
	if err != nil {
		return 0, err
	}
	return parseProcStatParent(data)
}

// parseProcStatParent extracts the parent process id from /proc/<pid>/stat. The
// second field is the executable name in parentheses and may itself contain
// spaces and parentheses, so the parse starts after the last ')'.
func parseProcStatParent(data []byte) (int, error) {
	close := bytes.LastIndexByte(data, ')')
	if close < 0 {
		return 0, fmt.Errorf("the process stat has no command field")
	}
	fields := strings.Fields(string(data[close+1:]))
	if len(fields) < 2 {
		return 0, fmt.Errorf("the process stat has no parent field")
	}
	ppid, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0, fmt.Errorf("the process stat parent field %q is not a number", fields[1])
	}
	return ppid, nil
}

// platformEnviron returns the environment of pid, read from
// /proc/<pid>/environ. The kernel exposes the environment the process had at
// exec time, and only to a caller that may inspect the process.
func platformEnviron(pid int) ([]string, error) {
	file, err := os.Open(procPath(pid, "environ"))
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	var entries []string
	reader := bufio.NewReader(file)
	for {
		entry, readErr := reader.ReadString(0)
		if entry != "" && entry != "\x00" {
			entries = append(entries, strings.TrimSuffix(entry, "\x00"))
		}
		if readErr == io.EOF {
			return entries, nil
		}
		if readErr != nil {
			return nil, readErr
		}
	}
}

func procPath(pid int, name string) string {
	return "/proc/" + strconv.Itoa(pid) + "/" + name
}
