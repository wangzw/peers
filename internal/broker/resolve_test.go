package broker

import (
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
)

// resolveAgent describes one Agent independently of whether it is local or
// remote, so every pickTarget case runs against both candidate sets.
type resolveAgent struct {
	name, ref, prev string
	visible         bool
}

// resolveTestAddr spells addresses the way a send test does, without depending
// on the signature work item.
func resolveTestAddr(name, ref string, unique bool) string {
	return FormatAddress(name, ref, "h", !unique)
}

// resolveLocalCands builds local candidates. The ID starts with the ref so the
// local prefix rule sees the same disambiguation the remote ref describes.
func resolveLocalCands(agents []resolveAgent) []candidate {
	views := make([]agentView, len(agents))
	visible := make(map[string]bool, len(agents))
	for i, a := range agents {
		id := a.ref + strings.Repeat("0", 64-len(a.ref))
		views[i] = agentView{ID: id, Name: a.name, Ref: a.ref, PrevName: a.prev}
		visible[id] = a.visible
	}
	return localCandidates(views, func(v agentView) bool { return visible[v.ID] }, resolveTestAddr)
}

// resolveRemoteCands builds remote candidates.
func resolveRemoteCands(agents []resolveAgent) []candidate {
	members := make([]remoteMember, len(agents))
	visible := make(map[string]bool, len(agents))
	for i, a := range agents {
		members[i] = remoteMember{Name: a.name, Ref: a.ref, PrevName: a.prev}
		visible[a.name+"\x00"+a.ref] = a.visible
	}
	return remoteCandidates(members, func(m remoteMember) bool {
		return visible[m.Name+"\x00"+m.Ref]
	}, resolveTestAddr)
}

// resolveBothKinds runs the same case against local and remote candidates.
func resolveBothKinds(t *testing.T, agents []resolveAgent, run func(t *testing.T, cands []candidate, refMatch func(candidate, string) bool, where string)) {
	t.Helper()
	t.Run("local", func(t *testing.T) {
		run(t, resolveLocalCands(agents), localRefMatch, "on this Broker")
	})
	t.Run("remote", func(t *testing.T) {
		run(t, resolveRemoteCands(agents), remoteRefMatch, "on hostB")
	})
}

func resolveWantCode(t *testing.T, err *Error, want localpb.ErrorCode) {
	t.Helper()
	if err.Code != want {
		t.Fatalf("Code = %v, want %v (message %q)", err.Code, want, err.Message)
	}
}

func resolveWantContains(t *testing.T, s, sub string) {
	t.Helper()
	if !strings.Contains(s, sub) {
		t.Fatalf("%q does not contain %q", s, sub)
	}
}

func resolveWantNotContains(t *testing.T, s, sub string) {
	t.Helper()
	if strings.Contains(s, sub) {
		t.Fatalf("%q unexpectedly contains %q", s, sub)
	}
}

// TestParseTargetPlain checks the ordinary name@alias form. Splitting at the
// first "@" instead of the last would break this.
func TestParseTargetPlain(t *testing.T) {
	got, ok := parseTarget("api-worker@hostB")
	if !ok {
		t.Fatal("parseTarget rejected a valid address")
	}
	want := parsedAddress{Name: "api-worker", Alias: "hostB"}
	if got != want {
		t.Fatalf("parseTarget = %+v, want %+v", got, want)
	}
}

// TestParseTargetWithRef checks that the trailing ref is split off the name
// (REQ-agent-messaging-029).
func TestParseTargetWithRef(t *testing.T) {
	got, ok := parseTarget("ab [1a2b3c]@hostA")
	if !ok {
		t.Fatal("parseTarget rejected a valid address")
	}
	want := parsedAddress{Name: "ab", Ref: "1a2b3c", Alias: "hostA"}
	if got != want {
		t.Fatalf("parseTarget = %+v, want %+v", got, want)
	}
}

// TestParseTargetNormalizesName checks that surrounding whitespace is trimmed
// and the name is normalized like a session name.
func TestParseTargetNormalizesName(t *testing.T) {
	got, ok := parseTarget(" API Worker @h")
	if !ok {
		t.Fatal("parseTarget rejected a valid address")
	}
	if got.Name != "api-worker" {
		t.Fatalf("Name = %q, want %q", got.Name, "api-worker")
	}
}

// TestParseTargetRejects checks that malformed addresses are rejected before
// they can produce a misleading not_found.
func TestParseTargetRejects(t *testing.T) {
	cases := []string{
		"no-at",
		"a@",
		"@h",
		"a@bad alias",
		"a@" + strings.Repeat("a", 25),
	}
	for _, in := range cases {
		if _, ok := parseTarget(in); ok {
			t.Errorf("parseTarget(%q) = ok, want rejected", in)
		}
	}
}

// TestPickUniqueByName checks that the only visible Agent of that name is
// selected.
func TestPickUniqueByName(t *testing.T) {
	agents := []resolveAgent{{name: "api-worker", ref: "1a2b3c", visible: true}}
	resolveBothKinds(t, agents, func(t *testing.T, cands []candidate, refMatch func(candidate, string) bool, where string) {
		idx, err := pickTarget(parsedAddress{Name: "api-worker", Alias: "h"}, cands, refMatch, where)
		if err != nil {
			t.Fatalf("pickTarget: %v", err)
		}
		if idx != 0 {
			t.Fatalf("idx = %d, want 0", idx)
		}
	})
}

// TestPickAmbiguousListsVisibleOnly checks that ambiguity lists the visible
// Agents only and leaks no address of an invisible one
// (REQ-agent-messaging-007).
func TestPickAmbiguousListsVisibleOnly(t *testing.T) {
	agents := []resolveAgent{
		{name: "api-worker", ref: "1a2b3c", visible: true},
		{name: "api-worker", ref: "d4e5f6", visible: true},
		{name: "api-worker", ref: "9a9a9a", visible: false},
	}
	resolveBothKinds(t, agents, func(t *testing.T, cands []candidate, refMatch func(candidate, string) bool, where string) {
		_, err := pickTarget(parsedAddress{Name: "api-worker", Alias: "h"}, cands, refMatch, where)
		if err == nil {
			t.Fatal("pickTarget selected a target, want ambiguous")
		}
		resolveWantCode(t, err, localpb.ErrorCode_ERROR_CODE_AMBIGUOUS)
		resolveWantContains(t, err.Message, FormatAddress("api-worker", "1a2b3c", "h", true))
		resolveWantContains(t, err.Message, FormatAddress("api-worker", "d4e5f6", "h", true))
		resolveWantNotContains(t, err.Message, FormatAddress("api-worker", "9a9a9a", "h", true))
		resolveWantNotContains(t, err.Message, "9a9a9a")
	})
}

// TestPickByRefAmongDuplicates checks that a ref selects one of two Agents
// sharing a name (REQ-agent-messaging-007).
func TestPickByRefAmongDuplicates(t *testing.T) {
	agents := []resolveAgent{
		{name: "api-worker", ref: "1a2b3c", visible: true},
		{name: "api-worker", ref: "d4e5f6", visible: true},
	}
	resolveBothKinds(t, agents, func(t *testing.T, cands []candidate, refMatch func(candidate, string) bool, where string) {
		idx, err := pickTarget(parsedAddress{Name: "api-worker", Ref: "1a2b3c", Alias: "h"}, cands, refMatch, where)
		if err != nil {
			t.Fatalf("pickTarget: %v", err)
		}
		if idx != 0 {
			t.Fatalf("idx = %d, want 0", idx)
		}
	})
}

// TestPickRefNameMismatchUsesPrevName checks that a ref plus a previous name
// reports the current address instead of delivering (REQ-agent-messaging-031).
func TestPickRefNameMismatchUsesPrevName(t *testing.T) {
	agents := []resolveAgent{
		{name: "api-v2", ref: "1a2b3c", prev: "api-worker", visible: true},
	}
	resolveBothKinds(t, agents, func(t *testing.T, cands []candidate, refMatch func(candidate, string) bool, where string) {
		_, err := pickTarget(parsedAddress{Name: "api-worker", Ref: "1a2b3c", Alias: "h"}, cands, refMatch, where)
		if err == nil {
			t.Fatal("pickTarget selected a target, want not_found")
		}
		resolveWantCode(t, err, localpb.ErrorCode_ERROR_CODE_NOT_FOUND)
		resolveWantContains(t, err.Message, "is now "+cands[0].Address)
	})
}

// TestPickPrevNameWithoutRef checks the rename hint for an address without a
// ref (REQ-agent-messaging-031, REQ-agent-messaging-005).
func TestPickPrevNameWithoutRef(t *testing.T) {
	agents := []resolveAgent{
		{name: "api-v2", ref: "1a2b3c", prev: "api-worker", visible: true},
	}
	resolveBothKinds(t, agents, func(t *testing.T, cands []candidate, refMatch func(candidate, string) bool, where string) {
		_, err := pickTarget(parsedAddress{Name: "api-worker", Alias: "h"}, cands, refMatch, where)
		if err == nil {
			t.Fatal("pickTarget selected a target, want not_found")
		}
		resolveWantCode(t, err, localpb.ErrorCode_ERROR_CODE_NOT_FOUND)
		resolveWantContains(t, err.Message, "api-v2")
	})
}

// TestPickInvisibleIsNotVisible checks that an Agent the caller cannot see
// yields not_visible and no address (REQ-agent-messaging-014).
func TestPickInvisibleIsNotVisible(t *testing.T) {
	agents := []resolveAgent{
		{name: "api-worker", ref: "1a2b3c", visible: false},
	}
	resolveBothKinds(t, agents, func(t *testing.T, cands []candidate, refMatch func(candidate, string) bool, where string) {
		_, err := pickTarget(parsedAddress{Name: "api-worker", Alias: "h"}, cands, refMatch, where)
		if err == nil {
			t.Fatal("pickTarget selected a target, want not_visible")
		}
		resolveWantCode(t, err, localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE)
		resolveWantNotContains(t, err.Message, cands[0].Address)
		resolveWantNotContains(t, err.Message, cands[0].RefAddress)
	})
}

// TestPickTruncatedNamePrefix checks that the truncated address an Agent
// renders selects it in a reply (REQ-agent-messaging-037, :025). The address
// carries the full twelve-character ref. The name written may be the shortened
// name this Broker renders or the shortened name another alias length renders,
// because the address may have been made by another Broker; a prefix that no
// legal alias length produces is not accepted.
func TestPickTruncatedNamePrefix(t *testing.T) {
	long := strings.Repeat("x", 70)
	agents := []resolveAgent{{name: long, ref: "1a2b3c4d5e6f", visible: true}}
	resolveBothKinds(t, agents, func(t *testing.T, cands []candidate, refMatch func(candidate, string) bool, where string) {
		name, ref, alias, ok := ParseAddress(cands[0].Address)
		if !ok {
			t.Fatalf("candidate address %q does not parse", cands[0].Address)
		}
		if len(ref) != 12 {
			t.Fatalf("candidate address %q carries a %d-character ref, want 12", cands[0].Address, len(ref))
		}
		idx, err := pickTarget(parsedAddress{Name: name, Ref: ref, Alias: alias}, cands, refMatch, where)
		if err != nil {
			t.Fatalf("pickTarget: %v", err)
		}
		if idx != 0 {
			t.Fatalf("idx = %d, want 0", idx)
		}

		// One rune shorter is the shortened name at the next alias length, which
		// another Broker would render, so the full ref still selects it.
		runes := []rune(name)
		fromLongerAlias := parsedAddress{Name: string(runes[:len(runes)-1]), Ref: ref, Alias: alias}
		idx, err = pickTarget(fromLongerAlias, cands, refMatch, where)
		if err != nil {
			t.Fatalf("pickTarget for a name shortened by another alias: %v", err)
		}
		if idx != 0 {
			t.Fatalf("idx = %d, want 0 for a name shortened by another alias", idx)
		}

		// A prefix that no legal alias length produces is not the rendered name
		// at any alias, so the full ref does not select it.
		unreachable := parsedAddress{Name: string(runes[:len(runes)-config.AliasMaxLen-1]), Ref: ref, Alias: alias}
		if _, err := pickTarget(unreachable, cands, refMatch, where); err == nil {
			t.Fatal("a prefix no legal alias length renders selected the Agent")
		}
	})
}

// TestPickCleanedNameWithRef checks that a cleaned name plus ref selects the
// Agent whose raw name cleans to it, not the Agent actually named that
// (REQ-agent-messaging-029).
func TestPickCleanedNameWithRef(t *testing.T) {
	agents := []resolveAgent{
		{name: "a<b", ref: "aaaaaa", visible: true},
		{name: "ab", ref: "bbbbbb", visible: true},
	}
	resolveBothKinds(t, agents, func(t *testing.T, cands []candidate, refMatch func(candidate, string) bool, where string) {
		addr := parsedAddress{Name: "ab", Ref: "aaaaaa", Alias: "h"}
		idx, err := pickTarget(addr, cands, refMatch, where)
		if err != nil {
			t.Fatalf("pickTarget: %v", err)
		}
		if idx != 0 {
			t.Fatalf("idx = %d, want 0", idx)
		}
	})
}

// TestPickRefEmptyCleanedName checks that a name that cleans to nothing cannot
// select an Agent: an empty prefix matches every cleaned name, so it must be
// treated as no match rather than a wildcard.
func TestPickRefEmptyCleanedName(t *testing.T) {
	agents := []resolveAgent{{name: "api-worker", ref: "1a2b3c", visible: true}}
	resolveBothKinds(t, agents, func(t *testing.T, cands []candidate, refMatch func(candidate, string) bool, where string) {
		_, err := pickTarget(parsedAddress{Name: "<>", Ref: "1a2b3c", Alias: "h"}, cands, refMatch, where)
		if err == nil {
			t.Fatal("pickTarget selected a target for a name that cleans to nothing, want not_found")
		}
		resolveWantCode(t, err, localpb.ErrorCode_ERROR_CODE_NOT_FOUND)
	})
}

// TestRemoteRefMatchMutualPrefix checks that a roster ref and an address ref
// match when either is a prefix of the other, and not otherwise.
func TestRemoteRefMatchMutualPrefix(t *testing.T) {
	if !remoteRefMatch(candidate{Ref: "1a2b3c"}, "1a2b3c4d") {
		t.Fatal("a longer address ref did not match the roster ref")
	}
	if !remoteRefMatch(candidate{Ref: "1a2b3c4d5e6f"}, "1a2b3c") {
		t.Fatal("a shorter address ref did not match the roster ref")
	}
	if remoteRefMatch(candidate{Ref: "1a2b3c"}, "1a2b3d") {
		t.Fatal("a different ref matched")
	}

	// The same rule must hold through pickTarget.
	cands := resolveRemoteCands([]resolveAgent{{name: "api-worker", ref: "1a2b3c", visible: true}})
	idx, err := pickTarget(parsedAddress{Name: "api-worker", Ref: "1a2b3c4d", Alias: "h"}, cands, remoteRefMatch, "on hostB")
	if err != nil {
		t.Fatalf("pickTarget: %v", err)
	}
	if idx != 0 {
		t.Fatalf("idx = %d, want 0", idx)
	}
}

// TestPickNotFound checks the ordinary miss, with the location in the message.
func TestPickNotFound(t *testing.T) {
	agents := []resolveAgent{{name: "api-worker", ref: "1a2b3c", visible: true}}
	resolveBothKinds(t, agents, func(t *testing.T, cands []candidate, refMatch func(candidate, string) bool, where string) {
		_, err := pickTarget(parsedAddress{Name: "nobody", Alias: "h"}, cands, refMatch, where)
		if err == nil {
			t.Fatal("pickTarget selected a target, want not_found")
		}
		resolveWantCode(t, err, localpb.ErrorCode_ERROR_CODE_NOT_FOUND)
		resolveWantContains(t, err.Message, where)
	})
}

// TestRemoteCandidatesRefRequired checks that a remote member whose publisher
// requires a ref always gets a ref in its address, even when its name is unique
// in the roster. A member without the flag keeps the plain address when unique.
func TestRemoteCandidatesRefRequired(t *testing.T) {
	members := []remoteMember{
		{Name: "worker", Ref: "abcdef", RefRequired: true},
		{Name: "steve", Ref: "123456"},
	}
	cands := remoteCandidates(members, func(remoteMember) bool { return true }, resolveTestAddr)
	byName := make(map[string]candidate, len(cands))
	for _, c := range cands {
		byName[c.Name] = c
	}
	if got, want := byName["worker"].Address, resolveTestAddr("worker", "abcdef", false); got != want {
		t.Fatalf("RefRequired member Address = %q, want the ref-forced %q", got, want)
	}
	if got, want := byName["steve"].Address, resolveTestAddr("steve", "123456", true); got != want {
		t.Fatalf("ordinary member Address = %q, want the plain %q", got, want)
	}
}
