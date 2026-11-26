//go:build !peers_testenv

package mcpserver

import "context"

// findNamedSocket is a no-op: a release build has no environment variable that
// names a socket, so selection depends only on the calling account.
func (*selector) findNamedSocket(context.Context) (target, bool, error) {
	return target{}, false, nil
}
