package mcpserver

import (
	"fmt"
	"net"
)

func systemPeerUID(conn net.Conn) (uint32, error) {
	uc, ok := conn.(*net.UnixConn)
	if !ok {
		return 0, fmt.Errorf("the connection is not a Unix socket")
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		return 0, err
	}
	var (
		uid   uint32
		opErr error
	)
	if err := raw.Control(func(fd uintptr) {
		uid, opErr = peerUIDFromFD(int(fd))
	}); err != nil {
		return 0, err
	}
	return uid, opErr
}
