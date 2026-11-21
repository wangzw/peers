package inbox

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestREQAgentMessaging003_NormalizeName(t *testing.T) {
	// REQ-agent-messaging-003
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"trim-and-lower", " API Worker ", "api-worker"},
		{"single-space", "A B", "a-b"},
		{"consecutive-spaces-kept", "a  b", "a--b"},
		{"tab-is-a-control-char", "A\tB", "ab"},
		{"newline-is-a-control-char", "a\nb", "ab"},
		{"zero-width-format-char", "a" + string(rune(0x200b)) + "b", "ab"},
		{"leading-format-char", string(rune(0x200f)) + "Worker", "worker"},
		{"non-ascii-space", "a" + string(rune(0x00a0)) + "b", "a-b"},
		{"uppercase", "API", "api"},
		{"empty", "", ""},
		{"invalid-utf8", string([]byte{'a', 0xff}), "a" + string(rune(0xfffd))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := NormalizeName(tc.in); got != tc.want {
				t.Fatalf("NormalizeName(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestNormalizeNameTruncatesLongName checks that a name far longer than the
// limit comes back within it.
func TestNormalizeNameTruncatesLongName(t *testing.T) {
	got := NormalizeName(strings.Repeat("a", 300))
	if len(got) > MaxNameBytes {
		t.Fatalf("NormalizeName(300 a's) is %d bytes, want at most %d", len(got), MaxNameBytes)
	}
}

// TestNormalizeNameTruncationKeepsUTF8Boundary checks that the cut lands on a
// rune boundary, so the result stays valid UTF-8, and that a cut that leaves a
// hyphen at the end drops it.
func TestNormalizeNameTruncationKeepsUTF8Boundary(t *testing.T) {
	cases := []struct {
		name string
		in   string
	}{
		{
			// The second byte of the 2-byte "é" falls at byte 128.
			name: "cut inside a multibyte rune",
			in:   strings.Repeat("a", 127) + "é" + strings.Repeat("b", 10),
		},
		{
			// The space becomes a hyphen at byte 128 and is then trimmed.
			name: "cut leaves a trailing hyphen",
			in:   strings.Repeat("a", 127) + " " + strings.Repeat("b", 10),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := NormalizeName(tc.in)
			if len(got) > MaxNameBytes {
				t.Fatalf("result is %d bytes, want at most %d", len(got), MaxNameBytes)
			}
			if !utf8.ValidString(got) {
				t.Fatalf("result is not valid UTF-8: %q", got)
			}
			if strings.HasSuffix(got, "-") {
				t.Fatalf("result ends with a hyphen: %q", got)
			}
		})
	}
}
