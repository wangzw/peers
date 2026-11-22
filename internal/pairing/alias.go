package pairing

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/wangzw/peers/internal/config"
)

// isAliasByte reports whether c is one of the bytes an alias may contain:
// ASCII letters, digits, '.', '-' and '_'.
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

// validAlias reports whether s can be used as an alias. Aliases are 1 to
// config.AliasMaxLen bytes long and consist of ASCII letters, digits, '.', '-'
// and '_'. Matching is case-sensitive; no normalization is applied anywhere.
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

// normalizeAlias returns the form of a self-reported alias that the peer table
// stores. A value that satisfies validAlias is returned unchanged. Any other
// value is reduced to the alias character set and truncated to
// config.AliasMaxLen, so an oversized or malformed value is never persisted as
// it was reported; the result may be empty. Whether the reported value counts as
// usable is still decided by validAlias on the original, not on this stored
// form.
func normalizeAlias(alias string) string {
	if validAlias(alias) {
		return alias
	}
	return truncatePart(sanitizeAliasPart(alias), config.AliasMaxLen)
}

// sanitizeAliasPart turns s into a string that satisfies the alias character
// set. Every rune outside [A-Za-z0-9._-] becomes '-'. The result may be longer
// than an alias; callers truncate it.
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

// assignLocalAlias builds a local alias that is unique against taken. The
// candidate is the sanitized self-reported alias, '-', and leading hex digits
// of fp. If that collides, more hex digits are added, and after the hex space
// is exhausted a two-digit sequence number is appended. The result always
// satisfies validAlias.
//
// taken must contain every alias that must not be reused: the Broker's own
// alias and the effective alias of every other peer. ownAlias is included for
// the same reason but is accepted separately so callers cannot forget it.
func assignLocalAlias(reported string, fp Fingerprint, ownAlias string, taken map[string]struct{}) string {
	base := sanitizeAliasPart(reported)
	if base == "" {
		base = "peer"
	}

	hex := fingerprintHex(fp)

	// Try the self-reported base with an increasing number of fingerprint
	// digits. A suffix of "-" plus 23 hex digits already fills the whole alias,
	// so longer prefixes cannot fit.
	for digits := 4; digits <= len(hex) && digits+1 <= config.AliasMaxLen; digits++ {
		suffix := "-" + hex[:digits]
		candidate := truncatePart(base, config.AliasMaxLen-len(suffix)) + suffix
		if aliasAvailable(candidate, ownAlias, taken) {
			return candidate
		}
	}

	// The fingerprint-based names were all taken. Keep the base and append a
	// two-digit sequence number instead, making room by truncating the base.
	for seq := 0; seq <= 99; seq++ {
		suffix := fmt.Sprintf("-%02d", seq)
		candidate := truncatePart(base, config.AliasMaxLen-len(suffix)) + suffix
		if aliasAvailable(candidate, ownAlias, taken) {
			return candidate
		}
	}

	// A hundred collisions on one prefix is outside anything a real peer table
	// produces. Return a value that is still valid and different from the last
	// attempt rather than looping forever.
	candidate := truncatePart(base, config.AliasMaxLen-3) + "-xx"
	if aliasAvailable(candidate, ownAlias, taken) {
		return candidate
	}
	return ""
}

// aliasAvailable reports whether candidate is a valid alias not in taken and
// not equal to ownAlias.
func aliasAvailable(candidate, ownAlias string, taken map[string]struct{}) bool {
	if !validAlias(candidate) || candidate == ownAlias {
		return false
	}
	_, used := taken[candidate]
	return !used
}

// truncatePart shortens s to at most n bytes without splitting a rune. The
// sanitized inputs are ASCII, but keeping it rune-safe avoids a broken alias if
// the function is reused with arbitrary text.
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

// isRuneStart reports whether b begins a UTF-8 rune.
func isRuneStart(b byte) bool { return b&0xc0 != 0x80 }
