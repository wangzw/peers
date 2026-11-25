package directory

import "github.com/wangzw/peers/internal/link"

// NewLinkTransport adapts a *link.Link to the directory's Links interface. The
// returned Links forwards every call to that link.
func NewLinkTransport(l *link.Link) Links { return newLinkTransport(l) }
