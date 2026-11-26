//go:build e2e && !linux

package e2e

import (
	"errors"
	"net"
)

func unixPeerUID(*net.UnixConn) (uint32, error) {
	return 0, errors.New("SO_PEERCRED is available only on linux")
}
