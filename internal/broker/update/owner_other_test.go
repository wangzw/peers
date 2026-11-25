//go:build !unix

package update

import (
	"os"
	"testing"
)

func assertSameOwner(t *testing.T, _ string, _ os.FileInfo) {
	t.Helper()
}
