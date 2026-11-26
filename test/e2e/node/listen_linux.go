//go:build linux

package node

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// ListeningAddrs returns the TCP addresses pid listens on, as host:port
// strings. It matches the process's socket file descriptors against the
// listening rows of /proc/net/tcp and /proc/net/tcp6. The result is sorted and
// free of duplicates.
func ListeningAddrs(pid int) ([]string, error) {
	inodes, err := processSocketInodes(pid)
	if err != nil {
		return nil, err
	}
	addrs := make(map[string]struct{})
	for _, table := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		if err := collectListening(table, inodes, addrs); err != nil {
			return nil, err
		}
	}
	list := make([]string, 0, len(addrs))
	for addr := range addrs {
		list = append(list, addr)
	}
	sort.Strings(list)
	return list, nil
}

func processSocketInodes(pid int) (map[uint64]struct{}, error) {
	dir := fmt.Sprintf("/proc/%d/fd", pid)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("node: listing sockets of process %d: %w", pid, err)
	}
	inodes := make(map[uint64]struct{}, len(entries))
	for _, entry := range entries {
		target, err := os.Readlink(filepath.Join(dir, entry.Name()))
		if err != nil {
			continue
		}
		if inode, ok := socketInode(target); ok {
			inodes[inode] = struct{}{}
		}
	}
	return inodes, nil
}

func socketInode(target string) (uint64, bool) {
	const prefix, suffix = "socket:[", "]"
	if !strings.HasPrefix(target, prefix) || !strings.HasSuffix(target, suffix) {
		return 0, false
	}
	inode, err := strconv.ParseUint(target[len(prefix):len(target)-len(suffix)], 10, 64)
	if err != nil {
		return 0, false
	}
	return inode, true
}

func collectListening(path string, inodes map[uint64]struct{}, addrs map[string]struct{}) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("node: reading %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		// sl local_address rem_address st tx_queue:rx_queue ... inode ...
		if len(fields) < 10 || fields[3] != "0A" {
			continue
		}
		inode, err := strconv.ParseUint(fields[9], 10, 64)
		if err != nil {
			continue
		}
		if _, ok := inodes[inode]; !ok {
			continue
		}
		addr, err := decodeLocalAddr(fields[1])
		if err != nil {
			continue
		}
		addrs[addr] = struct{}{}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("node: reading %s: %w", path, err)
	}
	return nil
}

// decodeLocalAddr turns the local_address field of /proc/net/tcp, hex
// address:hex port, into a host:port string. IPv4 addresses and every 32-bit
// word of an IPv6 address are stored little-endian.
func decodeLocalAddr(field string) (string, error) {
	hexAddr, hexPort, ok := strings.Cut(field, ":")
	if !ok {
		return "", fmt.Errorf("node: malformed local address %q", field)
	}
	port, err := strconv.ParseUint(hexPort, 16, 16)
	if err != nil {
		return "", fmt.Errorf("node: malformed port in %q: %w", field, err)
	}

	var ip net.IP
	switch len(hexAddr) {
	case 8:
		value, err := strconv.ParseUint(hexAddr, 16, 32)
		if err != nil {
			return "", fmt.Errorf("node: malformed IPv4 address in %q: %w", field, err)
		}
		ip = net.IPv4(byte(value), byte(value>>8), byte(value>>16), byte(value>>24))
	case 32:
		ip = make(net.IP, net.IPv6len)
		for i := 0; i < 4; i++ {
			value, err := strconv.ParseUint(hexAddr[i*8:(i+1)*8], 16, 32)
			if err != nil {
				return "", fmt.Errorf("node: malformed IPv6 address in %q: %w", field, err)
			}
			ip[i*4+0] = byte(value)
			ip[i*4+1] = byte(value >> 8)
			ip[i*4+2] = byte(value >> 16)
			ip[i*4+3] = byte(value >> 24)
		}
	default:
		return "", fmt.Errorf("node: unexpected address length in %q", field)
	}
	return net.JoinHostPort(ip.String(), strconv.FormatUint(port, 10)), nil
}
