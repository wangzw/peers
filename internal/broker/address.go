package broker

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/inbox"
)

// MaxAddressRunes is the rune limit of a formatted agent address.
const MaxAddressRunes = 64

// MaxAddressBytes is the byte limit of a formatted agent address.
const MaxAddressBytes = 200

// AgentID identifies a local agent: 64 lowercase hexadecimal characters derived
// from the agent's inbox socket path.
type AgentID string

// ErrBadSocketPath is returned by AgentIDFor for a socket path that cannot
// identify an agent: an empty path, a relative path, or a path containing a
// ".." element.
var ErrBadSocketPath = errors.New("inbox socket path must be absolute and must not contain a dot-dot element")

// AgentIDFor derives the agent ID for an inbox socket path. It returns the ID
// and the cleaned path used to derive it. The ".." check runs before cleaning,
// because Clean would resolve a traversal and hide it.
func AgentIDFor(socketPath string) (AgentID, string, error) {
	if socketPath == "" || !filepath.IsAbs(socketPath) {
		return "", "", ErrBadSocketPath
	}
	for _, elem := range strings.Split(socketPath, "/") {
		if elem == ".." {
			return "", "", ErrBadSocketPath
		}
	}
	p := filepath.Clean(socketPath)
	sum := sha256.Sum256([]byte("session:" + p))
	return AgentID(hex.EncodeToString(sum[:])), p, nil
}

// ComputeRefs returns a disambiguation ref for every unique ID in ids. The ref
// is the shortest prefix of 6 to 12 characters that no other ID shares. An ID
// whose first 12 characters collide with another ID's gets 12 characters.
func ComputeRefs(ids []AgentID) map[AgentID]string {
	unique := make([]AgentID, 0, len(ids))
	seen := make(map[AgentID]bool, len(ids))
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			unique = append(unique, id)
		}
	}
	refs := make(map[AgentID]string, len(unique))
	for _, id := range unique {
		length := 12
		for l := 6; l <= 12; l++ {
			if l > len(id) {
				break
			}
			collides := false
			for _, other := range unique {
				if other == id || l > len(other) {
					continue
				}
				if id[:l] == other[:l] {
					collides = true
					break
				}
			}
			if !collides {
				length = l
				break
			}
		}
		if length > len(id) {
			length = len(id)
		}
		refs[id] = string(id[:length])
	}
	return refs
}

// NameUsable normalizes a raw session name and reports whether the broker may
// register it. A name is unusable when it is empty after normalization, its
// cleaned signature is empty, or it contains "@" or "*".
func NameUsable(raw string) (string, bool) {
	n, _, usable := NameUsableDetail(raw)
	return n, usable
}

// NameUsableDetail is NameUsable with an explanation. It applies the same
// rules in the same order and returns the normalized name, the detail and
// whether the name is usable. detail is empty when the name is usable; when it
// is not, detail names the first failing rule for the model.
func NameUsableDetail(raw string) (string, string, bool) {
	n := inbox.NormalizeName(raw)
	switch {
	case n == "":
		return n, "the session name is empty", false
	case inbox.CleanSignature(n) == "":
		return n, "the session name has no characters a receiver can read", false
	case strings.Contains(n, "@"):
		return n, `the session name contains "@"`, false
	case strings.Contains(n, "*"):
		return n, `the session name contains "*"`, false
	}
	return n, "", true
}

// ValidAlias reports whether alias is a valid Broker alias: 1 to
// config.AliasMaxLen bytes drawn from [A-Za-z0-9._-].
func ValidAlias(alias string) bool {
	if len(alias) < 1 || len(alias) > config.AliasMaxLen {
		return false
	}
	return aliasCharsValid(alias)
}

func aliasCharsValid(alias string) bool {
	for i := 0; i < len(alias); i++ {
		c := alias[i]
		switch {
		case c >= 'a' && c <= 'z':
		case c >= 'A' && c <= 'Z':
		case c >= '0' && c <= '9':
		case c == '.', c == '_', c == '-':
		default:
			return false
		}
	}
	return true
}

// FormatAddress renders name@alias, adding " [ref]" after the name when needRef
// is set or the name changes under CleanSignature. A result over MaxAddressRunes
// or MaxAddressBytes carries the ref and shortens the name from the end until it
// fits.
//
// A caller that may render a shortened name must pass the full twelve-character
// ref: a shortened address resolves only with that full ref.
//
// name must already be normalized with inbox.NormalizeName; FormatAddress
// applies inbox.CleanSignature to it but does not normalize it. FormatAddress
// panics on a ref that is not 6 to 12 lowercase hexadecimal characters or an
// alias that fails ValidAlias.
func FormatAddress(name, ref, alias string, needRef bool) string {
	out, _ := formatAddress(name, ref, alias, needRef)
	return out
}

func addressNameTruncates(name, ref, alias string, needRef bool) bool {
	_, truncated := formatAddress(name, ref, alias, needRef)
	return truncated
}

// maxAddressAlias is the longest alias any viewer can use, so a name that fits
// at it fits at every shorter alias.
func maxAddressAlias() string {
	return strings.Repeat("a", config.AliasMaxLen)
}

func formatAddress(name, ref, alias string, needRef bool) (string, bool) {
	if !validRef(ref) {
		panic("broker: FormatAddress: ref must be 6 to 12 lowercase hex characters")
	}
	if !ValidAlias(alias) {
		panic("broker: FormatAddress: alias is not valid")
	}
	clean := inbox.CleanSignature(name)
	if clean != name {
		needRef = true
	}
	render := func(n string, withRef bool) string {
		if withRef {
			return n + " [" + ref + "]@" + alias
		}
		return n + "@" + alias
	}
	fits := func(s string) bool {
		return utf8.RuneCountInString(s) <= MaxAddressRunes && len(s) <= MaxAddressBytes
	}
	out := render(clean, needRef)
	if fits(out) {
		return out, false
	}
	runes := []rune(clean)
	for len(runes) > 0 {
		runes = runes[:len(runes)-1]
		out = render(string(runes), true)
		if fits(out) {
			return out, true
		}
	}
	return render("", true), true
}

// ParseAddress splits an address into its name, ref and alias. It splits at the
// last "@", requires a valid alias and a non-empty name, and recognizes a
// trailing " [hex]" suffix of 6 to 12 lowercase hexadecimal characters as a
// ref. It performs no normalization; the caller normalizes the name.
func ParseAddress(addr string) (name, ref, alias string, ok bool) {
	at := strings.LastIndexByte(addr, '@')
	if at < 0 {
		return "", "", "", false
	}
	name, alias = addr[:at], addr[at+1:]
	if !ValidAlias(alias) {
		return "", "", "", false
	}
	if strings.HasSuffix(name, "]") {
		if j := strings.LastIndex(name, " ["); j >= 0 {
			if candidate := name[j+2 : len(name)-1]; validRef(candidate) {
				name = name[:j]
				ref = candidate
			}
		}
	}
	if name == "" {
		return "", "", "", false
	}
	return name, ref, alias, true
}

func validRef(ref string) bool {
	if len(ref) < 6 || len(ref) > 12 {
		return false
	}
	for i := 0; i < len(ref); i++ {
		c := ref[i]
		if c >= '0' && c <= '9' {
			continue
		}
		if c >= 'a' && c <= 'f' {
			continue
		}
		return false
	}
	return true
}
