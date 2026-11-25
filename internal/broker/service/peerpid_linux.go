//go:build linux

package service

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

func socketPeerPID(conn *net.UnixConn) (int, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, err
	}
	var (
		pid     int
		credErr error
	)
	if err := raw.Control(func(fd uintptr) {
		cred, err := unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
		if err != nil {
			credErr = err
			return
		}
		pid = int(cred.Pid)
	}); err != nil {
		return 0, err
	}
	if credErr != nil {
		return 0, credErr
	}
	if pid == 0 {
		return 0, fmt.Errorf("the socket peer has no process id")
	}
	return pid, nil
}
