package inbox

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// TestREQAgentMessaging003_NormalizeName checks the name normalization rule across whitespace, case, format characters and invalid UTF-8.
func TestREQAgentMessaging003_NormalizeName(t *testing.T) {
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

const wantMaxNameBytes = 128

// TestMaxNameBytes checks that MaxNameBytes is 128.
func TestMaxNameBytes(t *testing.T) {
	if MaxNameBytes != wantMaxNameBytes {
		t.Fatalf("MaxNameBytes = %d, want %d", MaxNameBytes, wantMaxNameBytes)
	}
}

// TestNormalizeNameCapsLength checks that a 256 KiB name is normalized to at most MaxNameBytes bytes.
func TestNormalizeNameCapsLength(t *testing.T) {
	got := NormalizeName(strings.Repeat("a", 256*1024))
	if len(got) > wantMaxNameBytes {
		t.Fatalf("NormalizeName of 256 KiB is %d bytes, want at most %d", len(got), wantMaxNameBytes)
	}
}

// TestNormalizeNameTruncatesLongName checks that a name far longer than the limit comes back within it.
func TestNormalizeNameTruncatesLongName(t *testing.T) {
	got := NormalizeName(strings.Repeat("a", 300))
	if len(got) > MaxNameBytes {
		t.Fatalf("NormalizeName(300 a's) is %d bytes, want at most %d", len(got), MaxNameBytes)
	}
}

// TestNormalizeNameTruncationKeepsUTF8Boundary checks that truncation lands on a rune boundary and drops a trailing hyphen.
func TestNormalizeNameTruncationKeepsUTF8Boundary(t *testing.T) {
	cases := []struct {
		name string
		in   string
	}{
		{
			name: "cut inside a multibyte rune",
			in:   strings.Repeat("a", 127) + "é" + strings.Repeat("b", 10),
		},
		{
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
