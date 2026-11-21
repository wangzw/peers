package broker

import (
	"sort"
	"strings"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/inbox"
)

// parsedAddress is a target address split into its parts. Name is normalized;
// Ref is empty when the address carries no disambiguation ref.
type parsedAddress struct {
	Name  string // after inbox.NormalizeName
	Ref   string // "" when absent
	Alias string
}

// parseTarget trims s, splits it with ParseAddress and normalizes the name. ok
// is false when ParseAddress rejects s or the normalized name is empty.
func parseTarget(s string) (parsedAddress, bool) {
	name, ref, alias, ok := ParseAddress(strings.TrimSpace(s))
	if !ok {
		return parsedAddress{}, false
	}
	name = inbox.NormalizeName(name)
	if name == "" {
		return parsedAddress{}, false
	}
	return parsedAddress{Name: name, Ref: ref, Alias: alias}, true
}

// candidate is one Agent a target address could name. Address is spelled as the
// caller sees it; RefAddress is the same spelling with the ref forced, used to
// disambiguate duplicate names.
type candidate struct {
	Name, Ref, PrevName string
	ID                  string // local only; "" for remote
	Visible             bool
	// RefPinned reports that this Agent's name belongs to another account, so
	// its ref is the full 12 characters and only a full-length ref may select
	// it. It is always false for remote candidates.
	RefPinned bool
	// UID is the account that registered a local Agent. NameOwnerUID is the
	// account that owns Name, and NameOwned reports that the table has an
	// entry for it. The three are zero for remote candidates.
	UID          uint32
	NameOwnerUID uint32
	NameOwned    bool
	// PrevNameOwnerUID and PrevNameOwned do the same for PrevName.
	PrevNameOwnerUID uint32
	PrevNameOwned    bool
	// RefRequired reports that the publisher of a remote member requires a
	// ref for it: a bare name must not select that member.
	RefRequired bool
	Address     string // as the caller sees it
	RefAddress  string // same, with the ref forced (unique=false)
}

// nameOwnerRefusal carries the details of a by-name resolution refused because
// the name belongs to another account. The caller records them; they never
// enter an error message.
type nameOwnerRefusal struct {
	Name        string
	OwnerUID    uint32
	OwnerKnown  bool
	MatchedUIDs []uint32 // visible same-name Agents of other accounts, ascending
}

// addrFunc spells the address of an Agent for the caller; callers pass a
// closure over signatureFor with the alias they see.
type addrFunc func(name, ref string, unique bool) string

// pickTarget returns the index of the unique target or a Not-sent *Error.
// where is appended to messages, e.g. "on hostB" or "on this Broker".
func pickTarget(addr parsedAddress, cands []candidate, refMatch func(c candidate, ref string) bool, where string) (int, *Error) {
	return pickTargetRecording(addr, cands, refMatch, where, nil)
}

// pickTargetRecording is pickTarget, filling refusal when a bare name was
// refused because it belongs to another account. refusal may be nil.
func pickTargetRecording(addr parsedAddress, cands []candidate, refMatch func(c candidate, ref string) bool, where string, refusal *nameOwnerRefusal) (int, *Error) {
	if addr.Ref != "" {
		// A ref selects its Agent directly; ownership never enters the match.
		return pickTargetByRef(addr, cands, refMatch, where)
	}
	return pickTargetByName(addr, cands, where, refusal)
}

// pickTargetByRef locates the target by its ref. A full 12-character ref matches
// a name prefix; a shorter ref may be one the owner's callers hold, so it
// selects only an Agent whose name matches exactly and, when the name has a
// recorded owner, belongs to that owner's account.
func pickTargetByRef(addr parsedAddress, cands []candidate, refMatch func(c candidate, ref string) bool, where string) (int, *Error) {
	// Clean the name the way the Agent's own name is cleaned before comparing: a
	// pinned roster name may exceed the cleaner's codepoint limit, and a name the
	// cleaner changes must still match the Agent whose address carried a ref for
	// that reason.
	name := inbox.CleanSignature(addr.Name)
	if name == "" {
		// An empty prefix matches every cleaned name, so a name that cleans to
		// nothing names no Agent.
		return -1, notSent(localpb.ErrorCode_ERROR_CODE_NOT_FOUND, "no agent is known by the ref %s %s", addr.Ref, where)
	}
	owner, owned := nameOwnerOfCandidates(cands, addr.Name)

	var matched, invisible []int
	for i, c := range cands {
		if !refMatch(c, addr.Ref) {
			continue
		}
		if c.Visible {
			matched = append(matched, i)
		} else {
			invisible = append(invisible, i)
		}
	}

	switch len(matched) {
	case 0:
		if len(invisible) > 0 {
			return -1, notSent(localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE, "%s is not visible %s", addr.Name, where)
		}
		return -1, notSent(localpb.ErrorCode_ERROR_CODE_NOT_FOUND, "no agent is known by the ref %s %s", addr.Ref, where)
	case 1:
		c := cands[matched[0]]
		if refNameMatches(c, addr.Ref, name, owner, owned) {
			return matched[0], nil
		}
		if c.PrevName != "" && refNameMatches(candidate{Name: c.PrevName, UID: c.UID}, addr.Ref, name, c.PrevNameOwnerUID, c.PrevNameOwned) {
			return -1, notSent(localpb.ErrorCode_ERROR_CODE_NOT_FOUND, "%s is now %s %s", addr.Name, c.Address, where)
		}
		return -1, notSent(localpb.ErrorCode_ERROR_CODE_NOT_FOUND, "%s does not name the agent with ref %s %s", addr.Name, addr.Ref, where)
	default:
		return -1, ambiguousTarget(addr.Name, matched, cands, where)
	}
}

// refNameMatches reports whether c may be selected by a ref that carries the
// cleaned address name. A name too long for an address is rendered truncated,
// so a caller who copied such an address holds only a prefix of the real name.
// That address always carries the full twelve-character ref, and the prefix is
// accepted only for a full ref and only when the name written is exactly a
// shortened rendering of the candidate's name (the shortened name, not any
// shorter prefix).
// A short ref is one the owner's callers may hold, so it selects only an exact
// name or the name's owner account. Any other candidate needs its name spelled
// exactly, so a prefix-named Agent of another account, such as worker2
// answering worker, or one that registered worker plus filler, is never
// selected by a short ref.
func refNameMatches(c candidate, ref, cleanedName string, owner uint32, owned bool) bool {
	cleaned := inbox.CleanSignature(c.Name)
	if cleaned == cleanedName {
		return true
	}
	if len(ref) >= 12 && truncatedNameAtSomeAlias(c, cleaned, cleanedName) {
		return true
	}
	// The owner account's Agent may still be selected by a short ref the caller
	// kept from an earlier, shorter rendering.
	if owned && c.UID == owner {
		return strings.HasPrefix(cleaned, cleanedName)
	}
	return false
}

// truncatedNameAtSomeAlias reports whether cleanedName is the name part of the
// address FormatAddress renders for c, shortened to fit, at some legal alias.
// The address the caller copied may have been rendered by another Broker, whose
// alias for this one has a different length, so the truncation point need not
// equal this Broker's own rendering: any alias length from 1 to
// config.AliasMaxLen is possible.
//
// Only the ref-forcing rendering is tried. A shortened address always carries
// the ref, and once the name does not fit, FormatAddress shortens it with the
// ref appended, so a rendering made without a ref shortens to the same name.
// The no-ref rendering only adds the case where the whole name fits and is
// therefore not shortened.
func truncatedNameAtSomeAlias(c candidate, cleaned, cleanedName string) bool {
	if len(cleanedName) >= len(cleaned) || !strings.HasPrefix(cleaned, cleanedName) {
		return false
	}
	if !validRef(c.Ref) {
		return false
	}
	for aliasLen := 1; aliasLen <= config.AliasMaxLen; aliasLen++ {
		addr, truncated := formatAddress(c.Name, c.Ref, strings.Repeat("a", aliasLen), true)
		if truncated && renderedAddressName(addr) == cleanedName {
			return true
		}
	}
	return false
}

// renderedAddressName returns the cleaned name part of a rendered address. The
// candidate's Address may or may not carry the "@alias" suffix, so the suffix
// and a trailing " [ref]" are stripped textually rather than through
// ParseAddress.
func renderedAddressName(address string) string {
	s := address
	if at := strings.LastIndexByte(s, '@'); at >= 0 {
		s = s[:at]
	}
	s = strings.TrimSpace(s)
	if open := strings.LastIndexByte(s, '['); open > 0 && s[open-1] == ' ' && strings.HasSuffix(s, "]") {
		if validRef(strings.TrimSpace(s[open+1 : len(s)-1])) {
			s = strings.TrimSpace(s[:open])
		}
	}
	return inbox.CleanSignature(s)
}

// pickTargetByName locates the target by an exact name. A name recorded in the
// name owner table resolves only among the owner account's Agents: an Agent of
// another account is reached only by its ref. A remote member whose publisher
// requires a ref never takes part in a by-name match. The rename hint follows
// the same rule, so it never points a caller at another account's Agent.
func pickTargetByName(addr parsedAddress, cands []candidate, where string, refusal *nameOwnerRefusal) (int, *Error) {
	owner, owned := nameOwnerOfCandidates(cands, addr.Name)

	var matched []int
	var refused []uint32
	remoteRefused := false
	for i, c := range cands {
		if c.Name != addr.Name {
			continue
		}
		switch {
		case c.RefRequired:
			if c.Visible {
				remoteRefused = true
			}
		case owned && c.UID != owner:
			if c.Visible {
				refused = append(refused, c.UID)
			}
		case c.Visible:
			matched = append(matched, i)
		}
	}

	switch len(matched) {
	case 1:
		return matched[0], nil
	case 0:
		// The owner's own Agent still reports not_visible, before any other
		// account's Agent is considered.
		if owned {
			for _, c := range cands {
				if c.Name == addr.Name && c.UID == owner && !c.RefRequired && !c.Visible {
					return -1, notSent(localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE, "%s is not visible %s", addr.Name, where)
				}
			}
		}
		if len(refused) > 0 || remoteRefused {
			if refusal != nil {
				refusal.Name = addr.Name
				refusal.OwnerUID = owner
				refusal.OwnerKnown = owned
				refusal.MatchedUIDs = uniqueSortedUIDs(refused)
			}
			return -1, notSent(localpb.ErrorCode_ERROR_CODE_NOT_FOUND,
				"no agent named \"%s\" %s: the name belongs to another account; an agent of a different account is reached only by its full address with a disambiguator",
				addr.Name, where)
		}
		// A name nobody owns keeps the pre-ownership rule: any invisible Agent
		// with that name reports not_visible.
		for _, c := range cands {
			if !c.Visible && c.Name == addr.Name && !c.RefRequired {
				return -1, notSent(localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE, "%s is not visible %s", addr.Name, where)
			}
		}
		var prev []int
		for i, c := range cands {
			if !c.Visible || c.RefRequired || c.PrevName != addr.Name {
				continue
			}
			switch {
			case c.PrevNameOwned:
				// The previous name records its own owner, which is what a
				// caller who never saw the current name needs to respect.
				if c.UID != c.PrevNameOwnerUID {
					continue
				}
			case owned:
				if c.UID != owner {
					continue
				}
			}
			prev = append(prev, i)
		}
		if len(prev) > 0 {
			addrs := make([]string, 0, len(prev))
			for _, i := range prev {
				addrs = append(addrs, cands[i].Address)
			}
			sort.Strings(addrs)
			return -1, notSent(localpb.ErrorCode_ERROR_CODE_NOT_FOUND, "%s is now %s %s", addr.Name, strings.Join(addrs, ", "), where)
		}
		return -1, notSent(localpb.ErrorCode_ERROR_CODE_NOT_FOUND, "no agent named %q %s", addr.Name, where)
	default:
		return -1, ambiguousTarget(addr.Name, matched, cands, where)
	}
}

// nameOwnerOfCandidates returns the owner of name recorded on its candidates.
// Every local candidate for a name carries the same owner; remote candidates
// carry none.
func nameOwnerOfCandidates(cands []candidate, name string) (uint32, bool) {
	for _, c := range cands {
		if c.Name == name && c.NameOwned {
			return c.NameOwnerUID, true
		}
	}
	return 0, false
}

// uniqueSortedUIDs returns the distinct uids in ascending order.
func uniqueSortedUIDs(uids []uint32) []uint32 {
	if len(uids) == 0 {
		return nil
	}
	out := append([]uint32(nil), uids...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	n := 1
	for i := 1; i < len(out); i++ {
		if out[i] != out[n-1] {
			out[n] = out[i]
			n++
		}
	}
	return out[:n]
}

// ambiguousTarget lists the ref-forced addresses of the matched Agents in
// lexicographic order.
func ambiguousTarget(name string, matched []int, cands []candidate, where string) *Error {
	addrs := make([]string, 0, len(matched))
	for _, i := range matched {
		addrs = append(addrs, cands[i].RefAddress)
	}
	sort.Strings(addrs)
	return notSent(localpb.ErrorCode_ERROR_CODE_AMBIGUOUS, "%s is ambiguous %s: %s", name, where, strings.Join(addrs, ", "))
}

// localRefMatch matches a local Agent whose ID starts with the address ref. An
// Agent whose name belongs to another account is matched only by a full
// 12-character ref: a shorter ref may be one the owner's callers hold, and it
// must not hand their messages to this account.
func localRefMatch(c candidate, ref string) bool {
	if c.RefPinned && len(ref) < 12 {
		return false
	}
	return strings.HasPrefix(c.ID, ref)
}

// remoteRefMatch matches a remote member whose ref is a prefix of the address
// ref or vice versa: a roster's ref length changes as Agents come and go.
func remoteRefMatch(c candidate, ref string) bool {
	return strings.HasPrefix(c.Ref, ref) || strings.HasPrefix(ref, c.Ref)
}

// localCandidates builds candidates from all local Agents. The ref appears in
// an Agent's address when the Agent must show one because of how its name is
// held (RefMandatory); whether a duplicate sits outside the caller's view does
// not change that decision, which the Broker made over the whole table.
func localCandidates(all []agentView, visible func(agentView) bool, addr addrFunc) []candidate {
	cands := make([]candidate, 0, len(all))
	for _, a := range all {
		cands = append(cands, candidate{
			Name:             a.Name,
			Ref:              a.Ref,
			PrevName:         a.PrevName,
			ID:               a.ID,
			Visible:          visible(a),
			RefPinned:        a.RefPinned,
			UID:              a.UID,
			NameOwnerUID:     a.NameOwnerUID,
			NameOwned:        a.NameOwned,
			PrevNameOwnerUID: a.PrevNameOwnerUID,
			PrevNameOwned:    a.PrevNameOwned,
			Address:          addr(a.Name, a.Ref, !a.RefMandatory),
			RefAddress:       addr(a.Name, a.Ref, false),
		})
	}
	return cands
}

// remoteCandidates builds candidates from a peer's roster entries,
// de-duplicated by (Name, Ref). A name is unique when it appears with a single
// distinct Ref. A member whose publisher requires a ref always gets one in its
// address, even when its name is unique.
func remoteCandidates(members []remoteMember, visible func(remoteMember) bool, addr addrFunc) []candidate {
	seen := make(map[string]bool, len(members))
	// plainRefsByName counts only members whose publisher does not require a
	// ref. A ref-required member shares the name but must not add a ref to the
	// owner's address: the owner's entry stays ref-free next to it.
	plainRefsByName := make(map[string]map[string]bool)
	unique := make([]remoteMember, 0, len(members))
	for _, m := range members {
		key := m.Name + "\x00" + m.Ref
		if seen[key] {
			continue
		}
		seen[key] = true
		unique = append(unique, m)
		if m.RefRequired {
			continue
		}
		if plainRefsByName[m.Name] == nil {
			plainRefsByName[m.Name] = make(map[string]bool)
		}
		plainRefsByName[m.Name][m.Ref] = true
	}

	cands := make([]candidate, 0, len(unique))
	for _, m := range unique {
		isUnique := len(plainRefsByName[m.Name]) == 1 && !m.RefRequired
		cands = append(cands, candidate{
			Name:        m.Name,
			Ref:         m.Ref,
			PrevName:    m.PrevName,
			Visible:     visible(m),
			RefRequired: m.RefRequired,
			Address:     addr(m.Name, m.Ref, isUnique),
			RefAddress:  addr(m.Name, m.Ref, false),
		})
	}
	return cands
}
