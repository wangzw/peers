package broker

import (
	"strings"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/inbox"
)

const maxInboxFrameBytes = 1_000_000

// signatureFor returns the Agent's address as seen through alias; ok is false
// when the cleaned name is empty, ref is not 6 to 12 lowercase hex, or alias is
// malformed. Its spelling is FormatAddress's, so a signature and a listed
// address follow one rule.
func signatureFor(name, ref string, unique bool, alias string) (sig string, ok bool) {
	if inbox.CleanSignature(name) == "" {
		return "", false
	}
	if !ValidAlias(alias) {
		return "", false
	}
	if !validRef(ref) {
		return "", false
	}
	s := FormatAddress(name, ref, alias, !unique)
	// FormatAddress trims and truncates, but a signature that the receiver would
	// rewrite is not a signature: the receiver would show a different sender.
	if inbox.CleanSignature(s) != s {
		return "", false
	}
	return s, true
}

// MaxSendFrameSize returns the encoded frame size of text under the longest
// signature a sender named name with ref could get on any receiver.
func MaxSendFrameSize(name, ref, text string) int {
	// The alias the receiver uses is unknown, so every alias length from 1 to
	// config.AliasMaxLen is tried with and without the ref; frame sizes with a
	// short body rank them because JSON escaping is per character.
	best := ""
	bestSize := -1
	for aliasLen := 1; aliasLen <= config.AliasMaxLen; aliasLen++ {
		alias := strings.Repeat("a", aliasLen)
		for _, unique := range []bool{true, false} {
			sig, ok := signatureFor(name, ref, unique, alias)
			if !ok {
				continue
			}
			if n := inbox.FrameSize(sig, "x"); n > bestSize {
				bestSize = n
				best = sig
			}
		}
	}
	if bestSize < 0 {
		return inbox.FrameSize("", text)
	}
	return inbox.FrameSize(best, text)
}
