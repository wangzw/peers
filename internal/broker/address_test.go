package broker

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/wangzw/peers/internal/inbox"
)

// TestAgentIDForMatchesClaudeCodeAlgorithm checks that the agent ID is the
// hashed socket path under the fixed prefix and is unchanged by path cleaning.
func TestAgentIDForMatchesClaudeCodeAlgorithm(t *testing.T) {
	const socketPath = "/tmp/cc/s.sock"
	sum := sha256.Sum256([]byte("session:" + socketPath))
	want := AgentID(hex.EncodeToString(sum[:]))

	id, path, err := AgentIDFor(socketPath)
	if err != nil {
		t.Fatalf("AgentIDFor(%q): %v", socketPath, err)
	}
	if id != want {
		t.Errorf("AgentIDFor(%q) = %q, want %q", socketPath, id, want)
	}
	if path != socketPath {
		t.Errorf("cleaned path = %q, want %q", path, socketPath)
	}
	if len(id) != 64 {
		t.Errorf("ID length = %d, want 64", len(id))
	}

	for _, variant := range []string{"/tmp/cc//s.sock", "/tmp/cc/./s.sock"} {
		got, _, err := AgentIDFor(variant)
		if err != nil {
			t.Fatalf("AgentIDFor(%q): %v", variant, err)
		}
		if got != want {
			t.Errorf("AgentIDFor(%q) = %q, want the same ID as %q", variant, got, socketPath)
		}
	}
}

// TestAgentIDForRejectsRelativeAndDotDot checks that an empty, relative or
// dot-dot socket path yields ErrBadSocketPath.
func TestAgentIDForRejectsRelativeAndDotDot(t *testing.T) {
	for _, socketPath := range []string{"s.sock", "./s.sock", "/tmp/../s.sock", ""} {
		id, path, err := AgentIDFor(socketPath)
		if !errors.Is(err, ErrBadSocketPath) {
			t.Errorf("AgentIDFor(%q) error = %v, want ErrBadSocketPath", socketPath, err)
		}
		if id != "" || path != "" {
			t.Errorf("AgentIDFor(%q) = (%q, %q), want empty values on error", socketPath, id, path)
		}
	}
}

func makeTestID(prefix string) AgentID {
	if len(prefix) > 64 {
		panic("prefix longer than an agent ID")
	}
	return AgentID(prefix + strings.Repeat("0", 64-len(prefix)))
}

// TestComputeRefsShortestUniquePrefix checks that refs are the shortest unique
// prefix of 6 to 12 characters.
func TestComputeRefsShortestUniquePrefix(t *testing.T) {
	a0 := makeTestID("aaaaaa0")
	a1 := makeTestID("aaaaaa1")
	ab := makeTestID("aaaaab0")
	b := makeTestID("bbbbbbbbbbbb")

	if n := utf8.RuneCountInString(string(a0)); n != 64 {
		t.Fatalf("test ID length = %d, want 64", n)
	}

	refs := ComputeRefs([]AgentID{a0, ab, b})
	for id, want := range map[AgentID]string{
		a0: "aaaaaa",
		ab: "aaaaab",
		b:  "bbbbbb",
	} {
		if got := refs[id]; got != want {
			t.Errorf("ref(%q) = %q, want %q", id, got, want)
		}
	}

	refs = ComputeRefs([]AgentID{a0, a1, ab, b})
	if got, want := refs[a0], "aaaaaa0"; got != want {
		t.Errorf("ref(a0) after collision = %q, want %q", got, want)
	}
	if got, want := refs[a1], "aaaaaa1"; got != want {
		t.Errorf("ref(a1) after collision = %q, want %q", got, want)
	}
	if got, want := refs[ab], "aaaaab"; got != want {
		t.Errorf("ref(ab) after collision = %q, want %q", got, want)
	}
	if got, want := refs[b], "bbbbbb"; got != want {
		t.Errorf("ref(b) after collision = %q, want %q", got, want)
	}

	same12a := makeTestID("abcdef012345")
	same12b := AgentID("abcdef012345" + "f" + strings.Repeat("0", 51))
	refs = ComputeRefs([]AgentID{same12a, same12b})
	for _, id := range []AgentID{same12a, same12b} {
		if got, want := refs[id], "abcdef012345"; got != want {
			t.Errorf("ref(%q) with a 12-character collision = %q, want %q", id, got, want)
		}
	}

	refs = ComputeRefs([]AgentID{a0})
	if got, want := refs[a0], "aaaaaa"; got != want {
		t.Errorf("ref(single) = %q, want %q", got, want)
	}

	refs = ComputeRefs([]AgentID{a0, a0})
	if len(refs) != 1 {
		t.Errorf("duplicate ID produced %d refs, want 1", len(refs))
	}
	if got, want := refs[a0], "aaaaaa"; got != want {
		t.Errorf("ref(duplicate) = %q, want %q", got, want)
	}
}

// TestNameUsable checks that names are normalized before the usability test and
// that empty, @, * or unreadable names stay unusable.
func TestNameUsable(t *testing.T) {
	cases := []struct {
		raw    string
		want   string
		usable bool
	}{
		{" API Worker ", "api-worker", true},
		{"a@b", "a@b", false},
		{"*x", "*x", false},
		{"", "", false},
		{"\u200b", "", false},
		{"\"<>\"", "\"<>\"", false},
	}
	for _, c := range cases {
		got, usable := NameUsable(c.raw)
		if got != c.want || usable != c.usable {
			t.Errorf("NameUsable(%q) = (%q, %v), want (%q, %v)", c.raw, got, usable, c.want, c.usable)
		}
	}
}

// TestValidAlias checks that an alias is 1 to 24 bytes of letters, digits, dot,
// dash and underscore.
func TestValidAlias(t *testing.T) {
	valid := []string{
		"a",
		"hostB",
		"host.domain_1",
		"ABCDEFGHIJKLMNOPQRSTUVWX",
		"..__--",
	}
	for _, alias := range valid {
		if !ValidAlias(alias) {
			t.Errorf("ValidAlias(%q) = false, want true", alias)
		}
	}

	invalid := []string{
		"",
		"ABCDEFGHIJKLMNOPQRSTUVWXY",
		"a b",
		"a@b",
		"a/b",
		"ä",
	}
	for _, alias := range invalid {
		if ValidAlias(alias) {
			t.Errorf("ValidAlias(%q) = true, want false", alias)
		}
	}
}

// TestFormatAddressUniqueName checks that an address is name@alias and carries a
// bracketed ref only when asked.
func TestFormatAddressUniqueName(t *testing.T) {
	if got, want := FormatAddress("api-worker", "1a2b3c", "hostB", false), "api-worker@hostB"; got != want {
		t.Errorf("FormatAddress without ref = %q, want %q", got, want)
	}
	if got, want := FormatAddress("api-worker", "1a2b3c", "hostB", true), "api-worker [1a2b3c]@hostB"; got != want {
		t.Errorf("FormatAddress with ref = %q, want %q", got, want)
	}
}

// TestFormatAddressPanicsOnInvalidArguments checks that an invalid ref or alias
// panics.
func TestFormatAddressPanicsOnInvalidArguments(t *testing.T) {
	cases := []struct {
		what  string
		ref   string
		alias string
	}{
		{"short ref", "abcde", "hostB"},
		{"uppercase ref", "ABCDEF", "hostB"},
		{"non-hex ref", "abcdefg", "hostB"},
		{"invalid alias", "abcdef", "bad alias"},
	}
	for _, c := range cases {
		t.Run(c.what, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("FormatAddress(ref %q, alias %q) did not panic", c.ref, c.alias)
				}
			}()
			FormatAddress("api-worker", c.ref, c.alias, false)
		})
	}
}

// TestFormatAddressCleanedNameGetsRef checks that a name changed by
// CleanSignature gets a ref.
func TestFormatAddressCleanedNameGetsRef(t *testing.T) {
	got := FormatAddress("a<b", "1a2b3c", "hostB", false)
	if want := "ab [1a2b3c]@hostB"; got != want {
		t.Errorf("FormatAddress for a cleaned name = %q, want %q", got, want)
	}
}

// TestFormatAddressTruncatesLongName checks that a long name is shortened until
// the address fits both limits, with the ref and alias kept whole.
func TestFormatAddressTruncatesLongName(t *testing.T) {
	const ref = "abcdef012345"
	alias := strings.Repeat("a", 24)
	check := func(t *testing.T, addr, rawName string) {
		t.Helper()
		if n := utf8.RuneCountInString(addr); n != MaxAddressRunes {
			t.Errorf("address %q has %d runes, want %d", addr, n, MaxAddressRunes)
		}
		if n := len(addr); n > MaxAddressBytes {
			t.Errorf("address %q has %d bytes, want at most %d", addr, n, MaxAddressBytes)
		}
		if !strings.Contains(addr, " ["+ref+"]@") {
			t.Errorf("address %q is missing the full ref and alias", addr)
		}
		if !strings.HasSuffix(addr, "]@"+alias) {
			t.Errorf("address %q does not end with the full alias", addr)
		}
		if clean := inbox.CleanSignature(addr); clean != addr {
			t.Errorf("CleanSignature(%q) = %q, want unchanged", addr, clean)
		}
		name, gotRef, gotAlias, ok := ParseAddress(addr)
		if !ok {
			t.Fatalf("ParseAddress(%q) failed", addr)
		}
		if gotRef != ref {
			t.Errorf("parsed ref = %q, want %q", gotRef, ref)
		}
		if gotAlias != alias {
			t.Errorf("parsed alias = %q, want %q", gotAlias, alias)
		}
		if !strings.HasPrefix(rawName, name) {
			t.Errorf("parsed name %q is not a prefix of %q", name, rawName)
		}
	}

	asciiName := strings.Repeat("x", 70)
	check(t, FormatAddress(asciiName, ref, alias, false), asciiName)

	shortName := strings.Repeat("x", 50)
	check(t, FormatAddress(shortName, ref, alias, false), shortName)

	fourByteName := strings.Repeat("\U0001F600", 70)
	check(t, FormatAddress(fourByteName, ref, alias, false), fourByteName)
}

// TestFormatAddressTruncatesForByteLimit checks that the byte limit alone
// shortens a name built of four-byte characters while the rune count still fits.
func TestFormatAddressTruncatesForByteLimit(t *testing.T) {
	const (
		ref   = "abcdef"
		alias = "h"
		char  = "\U0001F600"
	)
	name := strings.Repeat(char, 50)
	untruncated := name + " [" + ref + "]@" + alias
	if n := utf8.RuneCountInString(untruncated); n > MaxAddressRunes {
		t.Fatalf("test setup: %d runes, want at most %d so only the byte limit binds", n, MaxAddressRunes)
	}
	if n := len(untruncated); n <= MaxAddressBytes {
		t.Fatalf("test setup: %d bytes, want more than %d", n, MaxAddressBytes)
	}

	addr := FormatAddress(name, ref, alias, false)
	if n := len(addr); n > MaxAddressBytes {
		t.Errorf("address %q has %d bytes, want at most %d", addr, n, MaxAddressBytes)
	}
	if n := utf8.RuneCountInString(addr); n >= MaxAddressRunes {
		t.Errorf("address %q has %d runes, want fewer than %d so the byte limit caused the truncation", addr, n, MaxAddressRunes)
	}
	if !strings.HasSuffix(addr, " ["+ref+"]@"+alias) {
		t.Errorf("address %q is missing the full ref and alias", addr)
	}

	namePart, gotRef, gotAlias, ok := ParseAddress(addr)
	if !ok {
		t.Fatalf("ParseAddress(%q) failed", addr)
	}
	if gotRef != ref || gotAlias != alias {
		t.Errorf("parsed ref, alias = %q, %q, want %q, %q", gotRef, gotAlias, ref, alias)
	}
	if !strings.HasPrefix(name, namePart) {
		t.Errorf("parsed name %q is not a prefix of %q", namePart, name)
	}
}

// TestParseAddressRoundTrip checks that ParseAddress recovers the parts of a
// formatted address and rejects malformed input.
func TestParseAddressRoundTrip(t *testing.T) {
	const alias24 = "aaaaaaaaaaaaaaaaaaaaaaaa"

	cases := []struct {
		addr  string
		name  string
		ref   string
		alias string
		ok    bool
	}{
		{FormatAddress("api-worker", "1a2b3c", "hostB", false), "api-worker", "", "hostB", true},
		{FormatAddress("api-worker", "1a2b3c", "hostB", true), "api-worker", "1a2b3c", "hostB", true},
		{FormatAddress("a<b", "1a2b3c", "hostB", false), "ab", "1a2b3c", "hostB", true},
		{FormatAddress(strings.Repeat("x", 70), "abcdef012345", alias24, false), strings.Repeat("x", 24), "abcdef012345", alias24, true},
		{"a@b@hostB", "a@b", "", "hostB", true},
		{"api [xyz]@h", "api [xyz]", "", "h", true},
		{"a [ABCDEF]@h", "a [ABCDEF]", "", "h", true},
		{"a [abcdef0123456]@h", "a [abcdef0123456]", "", "h", true},
		{"api [12345]@h", "api [12345]", "", "h", true},
		{"a [b [abcdef]@h", "a [b", "abcdef", "h", true},
		{"x [abcdef] [012345]@h", "x [abcdef]", "012345", "h", true},
		{" [abcdef]@h", "", "", "", false},
		{"@h", "", "", "", false},
		{"a@", "", "", "", false},
		{"a@bad alias", "", "", "", false},
		{"a@" + strings.Repeat("a", 25), "", "", "", false},
		{"no-at-sign", "", "", "", false},
	}
	for _, c := range cases {
		name, ref, alias, ok := ParseAddress(c.addr)
		if ok != c.ok || name != c.name || ref != c.ref || alias != c.alias {
			t.Errorf("ParseAddress(%q) = (%q, %q, %q, %v), want (%q, %q, %q, %v)",
				c.addr, name, ref, alias, ok, c.name, c.ref, c.alias, c.ok)
		}
	}
}
