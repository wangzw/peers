//go:build !linux && !darwin

package service

import "errors"

func peerUIDFromFD(int) (uint32, error) {
	return 0, errors.New("peer credentials are not supported on this platform")
}
