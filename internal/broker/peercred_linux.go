//go:build linux

package broker

import (
	"net"

	"golang.org/x/sys/unix"
)

func readPeerCred(conn *net.UnixConn) (Caller, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return Caller{}, err
	}
	var (
		ucred   *unix.Ucred
		credErr error
	)
	if err := raw.Control(func(fd uintptr) {
		ucred, credErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return Caller{}, err
	}
	if credErr != nil {
		return Caller{}, credErr
	}
	return Caller{UID: ucred.Uid, GID: ucred.Gid, PID: ucred.Pid}, nil
}
