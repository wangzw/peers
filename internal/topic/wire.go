package topic

import (
	"bytes"
	"cmp"
	"errors"
	"slices"
	"unicode"

	"github.com/wangzw/peers/internal/topic/topicpb"
)

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

// A nil message becomes the zero Member.
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

// MaxMemberNameBytes is the longest member name or previous name, in bytes, that
// a roster entry may carry. It equals inbox.MaxNameBytes, which topic cannot
// import; a broker test keeps them equal.
const MaxMemberNameBytes = 128

// The account is display-only and not checked here: an illegal account is
// cleared by SanitizeAccountName and must not drop the entry.
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
// account name on a roster entry, or the empty string when it is not.
func SanitizeAccountName(account string) string {
	if account == "" {
		return ""
	}
	if len(account) > MaxMemberNameBytes || !validMemberName(account) {
		return ""
	}
	return account
}

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

func sortByBytes[T ~[32]byte](s []T) {
	slices.SortFunc(s, func(a, b T) int {
		return bytes.Compare(a[:], b[:])
	})
}

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

func sortMembers(ms []Member) []Member {
	slices.SortFunc(ms, func(a, b Member) int {
		return cmp.Compare(a.Ref, b.Ref)
	})
	return ms
}

var errBadDigest = errors.New("digest is not 32 bytes")

func parseDigest(b []byte) ([32]byte, error) {
	var d [32]byte
	if len(b) != len(d) {
		return d, errBadDigest
	}
	copy(d[:], b)
	return d, nil
}

// Any item of the wrong length rejects the whole frame.
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
