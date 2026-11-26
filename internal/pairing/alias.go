package pairing

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/wangzw/peers/internal/config"
)

func isAliasByte(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z':
	case c >= 'A' && c <= 'Z':
	case c >= '0' && c <= '9':
	case c == '.', c == '-', c == '_':
	default:
		return false
	}
	return true
}

func validAlias(s string) bool {
	if len(s) == 0 || len(s) > config.AliasMaxLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !isAliasByte(s[i]) {
			return false
		}
	}
	return true
}

// normalizeAlias returns the stored form of a self-reported alias; usability is
// still decided from the original value, not this form.
func normalizeAlias(alias string) string {
	if validAlias(alias) {
		return alias
	}
	return truncatePart(sanitizeAliasPart(alias), config.AliasMaxLen)
}

func sanitizeAliasPart(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if r < utf8.RuneSelf && isAliasByte(byte(r)) {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('-')
	}
	return b.String()
}

// assignLocalAlias builds a local alias from the sanitized self-reported alias
// and a suffix: increasing counts of the fingerprint's hex digits, then a
// two-digit sequence, then "-xx". A candidate is skipped when it is taken or
// equals ownAlias, and the result is "" when every candidate is taken. taken
// must already hold the Broker's own alias and every other peer's effective alias.
func assignLocalAlias(reported string, fp Fingerprint, ownAlias string, taken map[string]struct{}) string {
	base := sanitizeAliasPart(reported)
	if base == "" {
		base = "peer"
	}

	hex := fingerprintHex(fp)

	// The suffix '-' plus 23 hex digits already fills the whole alias, so the
	// loop stops there.
	for digits := 4; digits <= len(hex) && digits+1 <= config.AliasMaxLen; digits++ {
		suffix := "-" + hex[:digits]
		candidate := truncatePart(base, config.AliasMaxLen-len(suffix)) + suffix
		if aliasAvailable(candidate, ownAlias, taken) {
			return candidate
		}
	}

	// All fingerprint-based names collided; fall back to a two-digit sequence.
	for seq := 0; seq <= 99; seq++ {
		suffix := fmt.Sprintf("-%02d", seq)
		candidate := truncatePart(base, config.AliasMaxLen-len(suffix)) + suffix
		if aliasAvailable(candidate, ownAlias, taken) {
			return candidate
		}
	}

	// Fall back to '-xx' rather than loop forever after a hundred collisions.
	candidate := truncatePart(base, config.AliasMaxLen-3) + "-xx"
	if aliasAvailable(candidate, ownAlias, taken) {
		return candidate
	}
	return ""
}

func aliasAvailable(candidate, ownAlias string, taken map[string]struct{}) bool {
	if !validAlias(candidate) || candidate == ownAlias {
		return false
	}
	_, used := taken[candidate]
	return !used
}

// truncatePart shortens s to at most n bytes without splitting a rune.
func truncatePart(s string, n int) string {
	if n <= 0 {
		return ""
	}
	if len(s) <= n {
		return s
	}
	for n > 0 && !isRuneStart(s[n]) {
		n--
	}
	return s[:n]
}

func isRuneStart(b byte) bool { return b&0xc0 != 0x80 }
