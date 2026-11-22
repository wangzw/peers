package topic

import (
	"bytes"
	"cmp"
	"errors"
	"slices"
	"unicode"

	"github.com/wangzw/peers/internal/topic/topicpb"
)

// statusToWire maps a Member.Status to its wire value. Only the four known
// lowercase values are recognized; everything else means unknown.
func statusToWire(s string) topicpb.AgentStatus {
	switch s {
	case "idle":
		return topicpb.AgentStatus_AGENT_STATUS_IDLE
	case "busy":
		return topicpb.AgentStatus_AGENT_STATUS_BUSY
	case "waiting":
		return topicpb.AgentStatus_AGENT_STATUS_WAITING
	case "shell":
		return topicpb.AgentStatus_AGENT_STATUS_SHELL
	default:
		return topicpb.AgentStatus_AGENT_STATUS_UNSPECIFIED
	}
}

// statusFromWire maps a wire status to Member.Status. UNSPECIFIED and any
// unknown enum value mean unknown, which is the empty string.
func statusFromWire(s topicpb.AgentStatus) string {
	switch s {
	case topicpb.AgentStatus_AGENT_STATUS_IDLE:
		return "idle"
	case topicpb.AgentStatus_AGENT_STATUS_BUSY:
		return "busy"
	case topicpb.AgentStatus_AGENT_STATUS_WAITING:
		return "waiting"
	case topicpb.AgentStatus_AGENT_STATUS_SHELL:
		return "shell"
	default:
		return ""
	}
}

// memberToWire converts a Member to its wire form.
func memberToWire(m Member) *topicpb.Member {
	return &topicpb.Member{
		Name:         m.Name,
		Ref:          m.Ref,
		Status:       statusToWire(m.Status),
		PreviousName: m.PrevName,
		Account:      m.Account,
		RefRequired:  m.RefRequired,
	}
}

// memberFromWire converts a wire member to a Member. A nil message is the zero
// Member.
func memberFromWire(p *topicpb.Member) Member {
	if p == nil {
		return Member{}
	}
	return Member{
		Name:        p.Name,
		Ref:         p.Ref,
		Status:      statusFromWire(p.Status),
		PrevName:    p.PreviousName,
		Account:     p.Account,
		RefRequired: p.RefRequired,
	}
}

// MaxMemberNameBytes is the longest member name or previous name, in bytes,
// that a roster entry may carry. It matches inbox.MaxNameBytes, which topic
// cannot import; a broker test keeps the two equal.
const MaxMemberNameBytes = 128

// validMemberEntry reports whether an inbound roster entry follows the broker's
// rules for names, disambiguators and previous names: a Ref of 6 to 12 lowercase
// hex digits, a non-empty name without whitespace, '@', '*' or control
// characters, and a previous name that is either empty or follows the same name
// rule. The account is display-only and is not part of this check: an illegal
// account is cleared by SanitizeAccountName, never allowed to drop the entry.
func validMemberEntry(m Member) bool {
	if len(m.Ref) < 6 || len(m.Ref) > 12 {
		return false
	}
	for i := 0; i < len(m.Ref); i++ {
		if c := m.Ref[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	if !validMemberName(m.Name) {
		return false
	}
	if m.PrevName != "" && !validMemberName(m.PrevName) {
		return false
	}
	return true
}

// SanitizeAccountName returns account when it is usable as the display-only
// account name on a roster entry, or "" when it is not. An illegal account must
// not cost the whole member entry, and the publisher may have sent one because
// its own account database allows characters a member name does not.
func SanitizeAccountName(account string) string {
	if account == "" {
		return ""
	}
	if len(account) > MaxMemberNameBytes || !validMemberName(account) {
		return ""
	}
	return account
}

// validMemberName reports whether name is a non-empty member name of at most
// MaxMemberNameBytes bytes with no whitespace, '@', '*' or control characters.
func validMemberName(name string) bool {
	if name == "" || len(name) > MaxMemberNameBytes {
		return false
	}
	for _, r := range name {
		if unicode.IsSpace(r) || unicode.IsControl(r) || r == '@' || r == '*' {
			return false
		}
	}
	return true
}

// sortByBytes sorts s by the bytes of each key, ascending. Digests and
// fingerprints both use this order so frames leave in a deterministic order.
func sortByBytes[T ~[32]byte](s []T) {
	slices.SortFunc(s, func(a, b T) int {
		return bytes.Compare(a[:], b[:])
	})
}

// digestsMsg builds a TopicDigests with the digests in ascending byte order.
func digestsMsg(ds [][32]byte) *topicpb.TopicDigests {
	sorted := slices.Clone(ds)
	sortByBytes(sorted)
	out := &topicpb.TopicDigests{Digests: make([][]byte, len(sorted))}
	for i, d := range sorted {
		b := make([]byte, len(d))
		copy(b, d[:])
		out.Digests[i] = b
	}
	return out
}

// rosterMsg builds a Roster whose members are in ascending Ref order.
func rosterMsg(d [32]byte, ms []Member) *topicpb.Roster {
	sorted := sortMembers(slices.Clone(ms))
	out := &topicpb.Roster{
		Digest:  d[:],
		Members: make([]*topicpb.Member, len(sorted)),
	}
	for i, m := range sorted {
		out.Members[i] = memberToWire(m)
	}
	return out
}

// deltaMsg builds a RosterDelta whose three lists are each in ascending Ref
// order.
func deltaMsg(d [32]byte, added []Member, removed []memberKey, changed []Member) *topicpb.RosterDelta {
	sortedAdded := sortMembers(slices.Clone(added))
	sortedRemoved := slices.Clone(removed)
	slices.SortFunc(sortedRemoved, func(a, b memberKey) int {
		return cmp.Compare(a.Ref, b.Ref)
	})
	sortedChanged := sortMembers(slices.Clone(changed))

	out := &topicpb.RosterDelta{
		Digest:  d[:],
		Added:   make([]*topicpb.Member, len(sortedAdded)),
		Removed: make([]*topicpb.MemberKey, len(sortedRemoved)),
		Changed: make([]*topicpb.Member, len(sortedChanged)),
	}
	for i, m := range sortedAdded {
		out.Added[i] = memberToWire(m)
	}
	for i, k := range sortedRemoved {
		out.Removed[i] = &topicpb.MemberKey{Name: k.Name, Ref: k.Ref}
	}
	for i, m := range sortedChanged {
		out.Changed[i] = memberToWire(m)
	}
	return out
}

// sortMembers returns ms sorted by Ref ascending.
func sortMembers(ms []Member) []Member {
	slices.SortFunc(ms, func(a, b Member) int {
		return cmp.Compare(a.Ref, b.Ref)
	})
	return ms
}

// errBadDigest reports a digest that is not exactly 32 bytes.
var errBadDigest = errors.New("digest is not 32 bytes")

// parseDigest checks that b is a 32-byte digest and copies it.
func parseDigest(b []byte) ([32]byte, error) {
	var d [32]byte
	if len(b) != len(d) {
		return d, errBadDigest
	}
	copy(d[:], b)
	return d, nil
}

// parseDigests returns the set of digests in m. Any item of the wrong length
// rejects the whole frame.
func parseDigests(m *topicpb.TopicDigests) (map[[32]byte]struct{}, error) {
	out := map[[32]byte]struct{}{}
	if m == nil {
		return out, nil
	}
	for _, b := range m.Digests {
		d, err := parseDigest(b)
		if err != nil {
			return nil, err
		}
		out[d] = struct{}{}
	}
	return out, nil
}
