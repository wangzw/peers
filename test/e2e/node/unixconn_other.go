//go:build !linux

package node

import "errors"

// InboxConnections returns errors.ErrUnsupported; it is only implemented on
// Linux.
func (s *Session) InboxConnections() (int, error) {
	return 0, errors.ErrUnsupported
}
