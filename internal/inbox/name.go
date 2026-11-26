package inbox

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// MaxNameBytes is the longest agent name, in bytes, that a Broker registers,
// publishes in a roster or accepts from a peer.
const MaxNameBytes = 128

// NormalizeName returns the name as Claude Code matches session names: control
// and format characters are dropped, surrounding whitespace is trimmed, the
// result is lowercased, and every remaining whitespace character becomes a
// hyphen. A result longer than MaxNameBytes is cut on a rune boundary, and a
// hyphen left at the end by the cut is dropped.
func NormalizeName(name string) string {
	name = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return -1
		}
		return r
	}, name)
	name = strings.TrimSpace(name)
	name = strings.ToLower(name)
	name = strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return '-'
		}
		return r
	}, name)
	if len(name) <= MaxNameBytes {
		return name
	}
	end := MaxNameBytes
	for end > 0 && !utf8.RuneStart(name[end]) {
		end--
	}
	return strings.TrimRight(name[:end], "-")
}
