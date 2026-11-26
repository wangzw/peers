package inbox

import (
	"strconv"
	"strings"
)

const maxSignatureRunes = 64

// CleanSignature returns sig as the receiver sees it: double quotes, angle
// brackets, backslashes and every code point strconv.IsPrint reports as
// non-printable are dropped, surrounding whitespace is trimmed, and the result is
// truncated to 64 runes.
func CleanSignature(sig string) string {
	sig = strings.Map(func(r rune) rune {
		switch r {
		case '"', '<', '>', '\\':
			return -1
		}
		if !strconv.IsPrint(r) {
			return -1
		}
		return r
	}, sig)
	sig = strings.TrimSpace(sig)
	if runes := []rune(sig); len(runes) > maxSignatureRunes {
		sig = string(runes[:maxSignatureRunes])
	}
	return sig
}
