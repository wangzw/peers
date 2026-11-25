//go:build !linux && !darwin

package service

import (
	"fmt"
	"net"
)

func socketPeerPID(*net.UnixConn) (int, error) {
	return 0, fmt.Errorf("socket peer credentials are not supported on this platform")
}
