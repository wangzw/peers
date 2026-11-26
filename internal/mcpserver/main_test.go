package mcpserver

import (
	"os"
	"testing"
)

var testRoles = map[string]func() int{}

// TestMain runs the test role PEERS_TEST_ROLE names in this process, and the
// tests otherwise.
func TestMain(m *testing.M) {
	if role := os.Getenv("PEERS_TEST_ROLE"); role != "" {
		run, ok := testRoles[role]
		if !ok {
			os.Exit(2)
		}
		os.Exit(run())
	}
	os.Exit(m.Run())
}
