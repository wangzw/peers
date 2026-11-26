//go:build linux

package node

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// unixAcceptingFlag is the kernel's SO_ACCEPTCON flag in /proc/net/unix.
const unixAcceptingFlag = 0x00010000

// InboxConnections returns how many peers are connected to the session's inbox
// socket, read from the /proc/net/unix entries whose path is the socket path and
// whose state is 03 (connected).
func (s *Session) InboxConnections() (int, error) {
	path := s.fake.SocketPath()
	if path == "" {
		return 0, nil
	}

	file, err := os.Open("/proc/net/unix")
	if err != nil {
		return 0, err
	}
	defer func() { _ = file.Close() }()

	connected := 0
	listening := 0
	scanner := bufio.NewScanner(file)
	first := true
	for scanner.Scan() {
		if first {
			first = false
			continue
		}
		fields := strings.Fields(scanner.Text())
		// Num RefCount Protocol Flags Type St Inode Path
		if len(fields) < 8 {
			continue
		}
		if strings.Join(fields[7:], " ") != path {
			continue
		}
		connectedEntry := fields[5] == "03"
		if connectedEntry {
			connected++
			// A listening socket is normally state 01 and is not counted above;
			// subtract it anyway when the kernel reports an accepting socket as
			// connected.
			if flags, err := strconv.ParseUint(fields[3], 16, 32); err == nil && flags&unixAcceptingFlag != 0 {
				listening++
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}

	n := connected - listening
	if n < 0 {
		n = 0
	}
	return n, nil
}
