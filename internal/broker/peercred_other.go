//go:build !linux && !darwin

package broker

import (
	"errors"
	"net"
)

func readPeerCred(*net.UnixConn) (Caller, error) {
	return Caller{}, errors.New("peer credentials are not supported on this platform")
}
