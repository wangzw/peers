package topic

import (
	"crypto/sha256"
	"unicode"
	"unicode/utf8"

	"github.com/wangzw/peers/internal/link"
)

// ValidateName reports whether name is a legal topic name: nil or ErrInvalidTopic.
// A name is 1 to 256 bytes long, valid UTF-8, and free of control characters.
func ValidateName(name string) error {
	if len(name) == 0 || len(name) > 256 {
		return ErrInvalidTopic
	}
	if !utf8.ValidString(name) {
		return ErrInvalidTopic
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return ErrInvalidTopic
		}
	}
	return nil
}

// digest returns SHA-256 of the salt, then sender's fingerprint, then name.
// Binding the sender makes one direction's digest unusable in the other, so a
// peer cannot echo a Broker's own digest set back to match it. Salt is always
// 64 bytes and a fingerprint 32, so the concatenation is unambiguous. It never
// writes to the caller's salt slice.
func digest(salt []byte, sender link.Fingerprint, name string) [32]byte {
	buf := make([]byte, 0, len(salt)+len(sender)+len(name))
	buf = append(buf, salt...)
	buf = append(buf, sender[:]...)
	buf = append(buf, name...)
	return sha256.Sum256(buf)
}
