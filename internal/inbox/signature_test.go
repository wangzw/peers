package inbox

import (
	"strconv"
	"strings"
	"testing"
)

func cSignatureInputs() []string {
	return []string{
		`a"b<c>d`,
		"  x  ",
		"a\nb\tc\x00d",
		"a" + string(rune(0x200b)) + "b",
		string(rune(0x200f)) + "worker",
		"",
		`a\b`,
		"a" + string(rune(0x2028)) + "b" + string(rune(0x2029)) + "c",
		"a" + string(rune(0xe000)) + "b",
		"a" + string(rune(0x0378)) + "b",
		"\xed\xa0\x80",
		strings.Repeat("a", 64),
		strings.Repeat("a", 65),
		strings.Repeat("é", 65),
		" " + strings.Repeat("a", 65),
	}
}

// TestREQAgentMessaging029_CleanSignature checks that forbidden characters are dropped, whitespace trimmed and empty input stays empty.
func TestREQAgentMessaging029_CleanSignature(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"forbidden-characters", `a"b<c>d`, "abcd"},
		{"trim", "  x  ", "x"},
		{"control-characters", "a\nb\tc\x00d", "abcd"},
		{"zero-width-format-char", "a" + string(rune(0x200b)) + "b", "ab"},
		{"leading-format-char", string(rune(0x200f)) + "worker", "worker"},
		{"empty", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := CleanSignature(tc.in); got != tc.want {
				t.Fatalf("CleanSignature(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestCleanSignatureDropsCharactersTheReceiverCannotRead checks that a backslash, separators, private-use and unassigned code points are dropped.
func TestCleanSignatureDropsCharactersTheReceiverCannotRead(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"backslash", `a\b`, "ab"},
		{"line-separator", "a" + string(rune(0x2028)) + "b", "ab"},
		{"paragraph-separator", "a" + string(rune(0x2029)) + "b", "ab"},
		{"private-use", "a" + string(rune(0xe000)) + "b", "ab"},
		{"unassigned", "a" + string(rune(0x0378)) + "b", "ab"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := CleanSignature(tc.in); got != tc.want {
				t.Fatalf("CleanSignature(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestCleanSignatureRejectsInvalidUTF8 checks that invalid UTF-8 is changed rather than passed through.
func TestCleanSignatureRejectsInvalidUTF8(t *testing.T) {
	const invalid = "\xed\xa0\x80" // UTF-8 encoding of a lone surrogate
	if got := CleanSignature(invalid); got == invalid {
		t.Fatalf("CleanSignature(%q) left invalid UTF-8 unchanged", invalid)
	}
}

// TestCleanSignatureSurvivesQuoteAndRecleaning checks that a cleaned signature needs no JSON escaping and cleaning it again changes nothing.
func TestCleanSignatureSurvivesQuoteAndRecleaning(t *testing.T) {
	for _, in := range cSignatureInputs() {
		got := CleanSignature(in)
		if want := `"` + got + `"`; strconv.Quote(got) != want {
			t.Errorf("strconv.Quote(CleanSignature(%q)) = %s, want %s: the escaped name would reach the receiver", in, strconv.Quote(got), want)
		}
		if again := CleanSignature(got); again != got {
			t.Errorf("CleanSignature(%q) = %q, then CleanSignature(%q) = %q, want idempotence", in, got, got, again)
		}
	}
}

// TestREQAgentMessaging037_TruncatesAt64Runes checks that cleaning keeps 64 runes and cuts more on a rune boundary.
func TestREQAgentMessaging037_TruncatesAt64Runes(t *testing.T) {
	keep := strings.Repeat("a", 64)
	if got := CleanSignature(keep); got != keep {
		t.Fatalf("CleanSignature of 64 runes changed it to %q", got)
	}
	cut := strings.Repeat("a", 65)
	if got, want := CleanSignature(cut), strings.Repeat("a", 64); got != want {
		t.Fatalf("CleanSignature of 65 runes = %q, want %q", got, want)
	}
	multibyte := strings.Repeat("é", 65)
	if got, want := CleanSignature(multibyte), strings.Repeat("é", 64); got != want {
		t.Fatalf("CleanSignature of 65 multi-byte runes = %q, want %q", got, want)
	}
	padded := " " + cut
	if got, want := CleanSignature(padded), strings.Repeat("a", 64); got != want {
		t.Fatalf("CleanSignature of padded 65 runes = %q, want %q", got, want)
	}
}
