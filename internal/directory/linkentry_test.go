package directory

import (
	"testing"

	"github.com/wangzw/peers/internal/link"
)

// The link package must keep the method set the adapter relies on; a change to
// any signature fails to compile here.
var _ linkAPI = (*link.Link)(nil)

// TestNewLinkTransportNotNil checks that NewLinkTransport(nil) returns a non-nil
// Links.
func TestNewLinkTransportNotNil(t *testing.T) {
	links := NewLinkTransport(nil)
	if links == nil {
		t.Fatal("NewLinkTransport(nil) returned nil, want a non-nil Links")
	}
}
