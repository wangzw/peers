//go:build e2e

package e2e

import (
	"flag"
	"log"
	"os"
	"testing"

	"github.com/wangzw/peers/test/e2e/harness"
)

// TestMain runs the tests in a child process and cleans residue before and
// after, so a go test timeout still leaves no residue.
func TestMain(m *testing.M) {
	flag.Parse()
	if harness.IsSupervisorChild() {
		os.Exit(m.Run())
	}

	harness.CleanResidue(log.Printf)
	code := harness.RunTestProcess(log.Printf, os.Args[1:])
	harness.CleanResidue(log.Printf)
	os.Exit(code)
}
