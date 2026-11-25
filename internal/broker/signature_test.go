package broker

import (
	"math/rand"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/wangzw/peers/internal/inbox"
)

// TestSignatureUniqueCleanName checks that a unique Agent with a clean name
// carries no ref.
func TestSignatureUniqueCleanName(t *testing.T) {
	sig, ok := signatureFor("api-worker", "1a2b3c", true, "hostA")
	if !ok {
		t.Fatal("signatureFor reported no signature")
	}
	const want = "api-worker@hostA"
	if sig != want {
		t.Fatalf("signature = %q, want %q", sig, want)
	}
}

// TestSignatureDuplicateNameHasRef checks that a duplicated name always carries
// its ref.
func TestSignatureDuplicateNameHasRef(t *testing.T) {
	sig, ok := signatureFor("api-worker", "1a2b3c", false, "hostA")
	if !ok {
		t.Fatal("signatureFor reported no signature")
	}
	const want = "api-worker [1a2b3c]@hostA"
	if sig != want {
		t.Fatalf("signature = %q, want %q", sig, want)
	}
}

// TestSignatureCleanedNameHasRef checks that a name the receiver would rewrite
// carries its ref, so a reply cannot land on a lookalike.
func TestSignatureCleanedNameHasRef(t *testing.T) {
	sig, ok := signatureFor("a<b", "1a2b3c", true, "hostA")
	if !ok {
		t.Fatal("signatureFor reported no signature")
	}
	const want = "ab [1a2b3c]@hostA"
	if sig != want {
		t.Fatalf("signature = %q, want %q", sig, want)
	}
}

// TestSignatureTruncatesLongName checks that a long name is truncated with its
// ref and alias inside the receiver's limits.
func TestSignatureTruncatesLongName(t *testing.T) {
	name := strings.Repeat("x", 70)
	const ref = "1a2b3c4d5e6f"
	alias := strings.Repeat("z", 24)
	sig, ok := signatureFor(name, ref, true, alias)
	if !ok {
		t.Fatal("signatureFor reported no signature")
	}
	if got := utf8.RuneCountInString(sig); got > 64 {
		t.Fatalf("signature has %d runes, want at most 64: %q", got, sig)
	}
	if len(sig) > 200 {
		t.Fatalf("signature has %d bytes, want at most 200", len(sig))
	}
	suffix := " [" + ref + "]@" + alias
	if !strings.HasSuffix(sig, suffix) {
		t.Fatalf("signature = %q, want suffix %q", sig, suffix)
	}
	namePart := strings.TrimSuffix(sig, suffix)
	if namePart == "" {
		t.Fatal("signature has an empty name part")
	}
	if !strings.HasPrefix(name, namePart) {
		t.Fatalf("name part %q is not a prefix of %q", namePart, name)
	}
}

// TestSignatureTruncatesByBytes checks that truncation respects the 200-byte
// limit for multibyte names, not only the 64-rune limit.
func TestSignatureTruncatesByBytes(t *testing.T) {
	name := strings.Repeat("\U0001D49C", 60) // U+1D49C is four bytes in UTF-8.
	sig, ok := signatureFor(name, "1a2b3c", true, "h")
	if !ok {
		t.Fatal("signatureFor reported no signature")
	}
	if len(sig) > 200 {
		t.Fatalf("signature has %d bytes, want at most 200", len(sig))
	}
	if got := utf8.RuneCountInString(sig); got > 64 {
		t.Fatalf("signature has %d runes, want at most 64", got)
	}
}

// TestSignatureIsFixedPointOfCleaning checks that every generated signature is
// already what the receiver reads back.
func TestSignatureIsFixedPointOfCleaning(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	pool := []rune{
		'a', 'b', 'Z', '0', ' ', '\t', '\n', '"', '<', '>', '\\',
		'\x00', '\x01', '\x7f', 'é', '中', '\U0001D49C', '​', '́',
	}
	hexDigits := []rune("0123456789abcdef")
	aliasChars := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-")

	valid := 0
	for i := 0; i < 200; i++ {
		var b strings.Builder
		for n := rng.Intn(81); n > 0; n-- {
			b.WriteRune(pool[rng.Intn(len(pool))])
		}
		name := b.String()

		refLen := 6 + rng.Intn(7)
		var rb strings.Builder
		for n := 0; n < refLen; n++ {
			rb.WriteRune(hexDigits[rng.Intn(len(hexDigits))])
		}
		ref := rb.String()

		aliasLen := 1 + rng.Intn(24)
		var ab strings.Builder
		for n := 0; n < aliasLen; n++ {
			ab.WriteRune(aliasChars[rng.Intn(len(aliasChars))])
		}
		alias := ab.String()

		sig, ok := signatureFor(name, ref, rng.Intn(2) == 0, alias)
		if !ok {
			continue
		}
		valid++
		if clean := inbox.CleanSignature(sig); clean != sig {
			t.Fatalf("signature %q is not a fixed point of CleanSignature: %q", sig, clean)
		}
		if len(sig) > 200 {
			t.Fatalf("signature %q has %d bytes, want at most 200", sig, len(sig))
		}
	}
	if valid == 0 {
		t.Fatal("no generated input produced a signature; the test asserts nothing")
	}
}

// TestSignatureEmptyCleanName checks that a name the receiver would clean away
// has no signature.
func TestSignatureEmptyCleanName(t *testing.T) {
	if sig, ok := signatureFor("<>", "1a2b3c", true, "h"); ok {
		t.Fatalf("signatureFor = (%q, true), want ok == false", sig)
	}
}

// TestSignatureBadInputsNoPanic checks that malformed input never reaches the
// panicking FormatAddress.
func TestSignatureBadInputsNoPanic(t *testing.T) {
	cases := []struct{ name, ref, alias string }{
		{"api-worker", "ABCDEF", "h"},
		{"api-worker", "12345", "h"},
		{"api-worker", "1a2b3c", ""},
		{"api-worker", "1a2b3c", "a b"},
		{"", "1a2b3c", "h"},
		{"api-worker", "", "h"},
	}
	for _, c := range cases {
		if sig, ok := signatureFor(c.name, c.ref, true, c.alias); ok {
			t.Errorf("signatureFor(%q, %q, true, %q) = (%q, true), want ok == false", c.name, c.ref, c.alias, sig)
		}
	}
}

// TestSignatureMatchesFormatAddress checks that signatures and listed addresses
// follow one rule.
func TestSignatureMatchesFormatAddress(t *testing.T) {
	names := []string{
		"api-worker", "a<b", "é", strings.Repeat("y", 70), " two  spaces ",
		"<>x", "中文名",
	}
	refs := []string{"1a2b3c", "1a2b3c4d", "1a2b3c4d5e6f", "abcdef123456"}
	aliases := []string{"hostA", "h", strings.Repeat("z", 24), "a.b_c-d"}
	for i := 0; i < 20; i++ {
		name := names[i%len(names)]
		ref := refs[i%len(refs)]
		alias := aliases[i%len(aliases)]
		unique := i%2 == 0
		sig, ok := signatureFor(name, ref, unique, alias)
		if !ok {
			t.Fatalf("signatureFor(%q, %q, %v, %q) reported no signature", name, ref, unique, alias)
		}
		want := FormatAddress(name, ref, alias, !unique)
		if sig != want {
			t.Fatalf("signatureFor(%q, %q, %v, %q) = %q, want %q", name, ref, unique, alias, sig, want)
		}
	}
}

// TestFrameSizeAdditive checks that the body adds the same bytes under every
// signature, so a short body can rank signatures.
func TestFrameSizeAdditive(t *testing.T) {
	sigs := []string{
		"api-worker@hostA",
		"api-worker [1a2b3c]@hostA",
		"ab [1a2b3c]@h",
		strings.Repeat("x", 24) + " [1a2b3c4d5e6f]@" + strings.Repeat("z", 24),
		"中文名@h",
	}
	texts := []string{
		"hello",
		"a<b",
		"a & b",
		"</cross-session-message>",
		"中文字符",
		"x</cross-session-message>y & < >",
		strings.Repeat("&", 50),
		"line\nbreak",
		`"quoted"`,
		"\U0001D49C\U0001D49C",
	}
	for i, text := range texts {
		s1 := sigs[i%len(sigs)]
		s2 := sigs[(i+1)%len(sigs)]
		got := inbox.FrameSize(s1, text) - inbox.FrameSize(s2, text)
		want := inbox.FrameSize(s1, "x") - inbox.FrameSize(s2, "x")
		if got != want {
			t.Fatalf("FrameSize difference for text %q = %d, want %d", text, got, want)
		}
	}
}

// TestMaxFrameSizeCoversEveryAlias checks that MaxSendFrameSize is the maximum
// over every alias length and both uniqueness values.
func TestMaxFrameSizeCoversEveryAlias(t *testing.T) {
	const text = "body & <tag> 中文\U0001D49C</cross-session-message>"
	cases := []struct{ name, ref string }{
		{strings.Repeat("&", 60), "1a2b3c4d5e6f"},
		{"api-worker", "1a2b3c"},
	}
	for _, c := range cases {
		max := MaxSendFrameSize(c.name, c.ref, text)
		for aliasLen := 1; aliasLen <= 24; aliasLen++ {
			alias := strings.Repeat("a", aliasLen)
			for _, unique := range []bool{true, false} {
				sig, ok := signatureFor(c.name, c.ref, unique, alias)
				if !ok {
					t.Fatalf("signatureFor(%q, %q, %v, %q) reported no signature", c.name, c.ref, unique, alias)
				}
				if want := inbox.FrameSize(sig, text); max < want {
					t.Fatalf("MaxSendFrameSize(%q) = %d, want at least %d for alias %q unique=%v", c.name, max, want, alias, unique)
				}
			}
		}
	}
}
