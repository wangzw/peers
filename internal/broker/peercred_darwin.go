//go:build darwin

package broker

import (
	"net"

	"golang.org/x/sys/unix"
)

// readPeerCred reads the peer's credentials with LOCAL_PEERCRED and
// LOCAL_PEERPID.
func readPeerCred(conn *net.UnixConn) (Caller, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return Caller{}, err
	}
	var (
		xucred  *unix.Xucred
		pid     int
		credErr error
		pidErr  error
	)
	if err := raw.Control(func(fd uintptr) {
		xucred, credErr = unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
		if credErr != nil {
			return
		}
		pid, pidErr = unix.GetsockoptInt(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERPID)
	}); err != nil {
		return Caller{}, err
	}
	if credErr != nil {
		return Caller{}, credErr
	}
	if pidErr != nil {
		return Caller{}, pidErr
	}
	c := Caller{UID: xucred.Uid, PID: int32(pid)}
	if xucred.Ngroups > 0 {
		c.GID = xucred.Groups[0]
	}
	return c, nil
}
