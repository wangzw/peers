//go:build linux

package inbox

import (
	"os"
	"testing"
)

func TestPlatformParentPID(t *testing.T) {
	got, err := platformParentPID(os.Getpid())
	if err != nil {
		t.Fatalf("platformParentPID(%d): %v", os.Getpid(), err)
	}
	if want := os.Getppid(); got != want {
		t.Fatalf("platformParentPID(%d) = %d, want %d", os.Getpid(), got, want)
	}
}

func TestParseProcStatParent(t *testing.T) {
	// The command name contains spaces and parentheses, which must not shift the
	// parent field.
	data := []byte("1234 (a (weird) name) S 42 1234 1234 0 -1 4194304 0 0\n")
	got, err := parseProcStatParent(data)
	if err != nil {
		t.Fatalf("parseProcStatParent: %v", err)
	}
	if got != 42 {
		t.Fatalf("parseProcStatParent = %d, want 42", got)
	}
}

func TestParseProcStatParentRejectsMalformed(t *testing.T) {
	for _, data := range []string{"", "1234 no parenthesised name", "1234 (name) S"} {
		if _, err := parseProcStatParent([]byte(data)); err == nil {
			t.Fatalf("parseProcStatParent(%q) accepted malformed data", data)
		}
	}
}
