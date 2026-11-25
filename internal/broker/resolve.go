package broker

import (
	"sort"
	"strings"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/inbox"
)

type parsedAddress struct {
	Name  string
	Ref   string
	Alias string
}

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

type candidate struct {
	Name, Ref, PrevName string
	ID                  string
	Visible             bool
	RefPinned           bool
	UID                 uint32
	NameOwnerUID        uint32
	NameOwned           bool
	PrevNameOwnerUID    uint32
	PrevNameOwned       bool
	RefRequired         bool
	Address             string
	RefAddress          string
}

// nameOwnerRefusal carries details of a by-name resolution refused because the
// name belongs to another account; they never enter an error message.
type nameOwnerRefusal struct {
	Name        string
	OwnerUID    uint32
	OwnerKnown  bool
	MatchedUIDs []uint32
}

type addrFunc func(name, ref string, unique bool) string

func pickTarget(addr parsedAddress, cands []candidate, refMatch func(c candidate, ref string) bool, where string) (int, *Error) {
	return pickTargetRecording(addr, cands, refMatch, where, nil)
}

func pickTargetRecording(addr parsedAddress, cands []candidate, refMatch func(c candidate, ref string) bool, where string, refusal *nameOwnerRefusal) (int, *Error) {
	if addr.Ref != "" {
		// A ref selects its Agent directly; ownership never enters the match.
		return pickTargetByRef(addr, cands, refMatch, where)
	}
	return pickTargetByName(addr, cands, where, refusal)
}

func pickTargetByRef(addr parsedAddress, cands []candidate, refMatch func(c candidate, ref string) bool, where string) (int, *Error) {
	name := inbox.CleanSignature(addr.Name)
	if name == "" {
		// An empty cleaned name would prefix-match every Agent, so it names none.
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

// truncatedNameAtSomeAlias reports whether cleanedName is c's address name
// shortened to fit at some legal alias length. Only the ref-forcing rendering is
// tried, because a shortened address always carries the ref.
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

// renderedAddressName strips the alias and a trailing " [ref]" textually,
// because a candidate's Address may lack the "@alias" suffix.
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

// nameOwnerOfCandidates returns the owner recorded on the candidates; every
// local candidate for a name carries the same owner and remote candidates none.
func nameOwnerOfCandidates(cands []candidate, name string) (uint32, bool) {
	for _, c := range cands {
		if c.Name == name && c.NameOwned {
			return c.NameOwnerUID, true
		}
	}
	return 0, false
}

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

func ambiguousTarget(name string, matched []int, cands []candidate, where string) *Error {
	addrs := make([]string, 0, len(matched))
	for _, i := range matched {
		addrs = append(addrs, cands[i].RefAddress)
	}
	sort.Strings(addrs)
	return notSent(localpb.ErrorCode_ERROR_CODE_AMBIGUOUS, "%s is ambiguous %s: %s", name, where, strings.Join(addrs, ", "))
}

func localRefMatch(c candidate, ref string) bool {
	if c.RefPinned && len(ref) < 12 {
		return false
	}
	return strings.HasPrefix(c.ID, ref)
}

func remoteRefMatch(c candidate, ref string) bool {
	return strings.HasPrefix(c.Ref, ref) || strings.HasPrefix(ref, c.Ref)
}

// The ref appears when RefMandatory is set, a decision the Broker made over the
// whole table, so a duplicate outside the caller's view does not change it.
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

func remoteCandidates(members []remoteMember, visible func(remoteMember) bool, addr addrFunc) []candidate {
	seen := make(map[string]bool, len(members))
	// Count only members that do not require a ref, so a ref-required collider
	// does not add a ref to the owner's address.
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
