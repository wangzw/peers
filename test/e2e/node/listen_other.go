//go:build !linux

package node

import "errors"

// ListeningAddrs returns errors.ErrUnsupported; it is only implemented on
// Linux.
func ListeningAddrs(pid int) ([]string, error) {
	return nil, errors.ErrUnsupported
}
