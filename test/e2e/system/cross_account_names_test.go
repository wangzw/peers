//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/testkit/fakesession"
	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/syscontainer"
)

const (
	caTopic = "cross-account-names"

	caSystemConfig = "broker:\n  alias: sys\n  advertise: \"hostA:17443\"\n"

	caPeerListen = ":17444"

	caPairingFile   = "ca-pairing"
	caConnectedFile = "ca-connected"
	caImpObserved   = "ca-impersonation-observed"
	caImpDone       = "ca-impersonation-done"
	caWorkersSeen   = "ca-workers-seen"
	caOwnerLeft     = "ca-owner-left"
	caSentFile      = "ca-sent"
	caDoneFile      = "ca-done"

	caLocalBare  = "ca-local-bare-probe"
	caLocalRef   = "ca-local-ref-probe"
	caRemoteBare = "ca-remote-bare-probe"
	caRemoteRef  = "ca-remote-ref-probe"
)

const (
	caPairingBound = 90 * time.Second

	caLinkBound = 30 * time.Second

	caMarkerBound = 60 * time.Second

	caRemoveBound = 15 * time.Second
)

// caAgentEntry is a local shape so the test also builds against a Broker that
// does not report the account field.
type caAgentEntry struct {
	Address string `json:"address"`
	Account string `json:"account"`
}

type caListOut struct {
	Self   string         `json:"self_address"`
	Agents []caAgentEntry `json:"agents"`
}

// TestREQAgentMessaging047_CrossAccountImpersonation checks that a second
// account cannot impersonate the account that registered a name first.
func TestREQAgentMessaging047_CrossAccountImpersonation(t *testing.T) {
	if !syscontainer.InContainer() {
		t.Parallel()
		syscontainer.RunPairTest(t, e2ePairOptions)
		return
	}
	switch syscontainer.PairRole() {
	case syscontainer.RoleA:
		defer syscontainer.PairGuard(t)()
		caImpersonationOnHostA(t, caHostASetup(t))
	case syscontainer.RoleB:
		defer syscontainer.PairGuard(t)()
		caImpersonationOnHostB(t, caHostBSetup(t))
	default:
		t.Fatalf("the pair test runs in a pair container as role A or B")
	}
}

// TestREQAgentMessaging048_CrossAccountInterception checks that after the name's
// owner leaves, a by-name send to the bare name is refused with not_found, and
// the later account receives only the full-ref sends.
func TestREQAgentMessaging048_CrossAccountInterception(t *testing.T) {
	if !syscontainer.InContainer() {
		t.Parallel()
		syscontainer.RunPairTest(t, e2ePairOptions)
		return
	}
	switch syscontainer.PairRole() {
	case syscontainer.RoleA:
		defer syscontainer.PairGuard(t)()
		caInterceptionOnHostA(t, caHostASetup(t))
	case syscontainer.RoleB:
		defer syscontainer.PairGuard(t)()
		caInterceptionOnHostB(t, caHostBSetup(t))
	default:
		t.Fatalf("the pair test runs in a pair container as role A or B")
	}
}

func caHostASetup(t *testing.T) *syscontainer.Session {
	t.Helper()

	syscontainer.StartSystemBroker(t, caSystemConfig)

	root := syscontainer.NewSession(t, "root", "ca-observer")
	caWaitRegistered(t, root)
	caJoin(t, root, caTopic)
	caPublishPairing(t, root)
	syscontainer.PairAwait(t, caConnectedFile, caPairingBound)
	return root
}

func caHostBSetup(t *testing.T) *syscontainer.Session {
	t.Helper()

	syscontainer.StartUserBroker(t, "peer", "--listen", caPeerListen)
	sender := syscontainer.NewSession(t, "peer", "ca-sender")
	caWaitRegistered(t, sender)
	caJoin(t, sender, caTopic)

	address, key := caReadPairing(t, syscontainer.PairAwait(t, caPairingFile, caPairingBound))
	if text, isError := sender.Call(t, "connect_broker", map[string]any{"address": address, "key": key}); isError {
		t.Fatalf("connect_broker on host B: %s", text)
	}
	syscontainer.PairWrite(t, caConnectedFile, []byte("connected\n"))
	return sender
}

func caImpersonationOnHostA(t *testing.T, root *syscontainer.Session) {
	t.Helper()

	peer := syscontainer.NewSession(t, "peer", "twin")
	caWaitRegistered(t, peer)
	caJoin(t, peer, caTopic)
	other := syscontainer.NewSession(t, "other", "twin")
	caWaitRegistered(t, other)
	caJoin(t, other, caTopic)

	caWaitNamed(t, root, "twin", 2)

	harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "the owner's address carries no ref", func(deadline time.Time) (bool, string) {
		out, state := sysPollDecode[caListOut](t, deadline, peer, "list_agents", map[string]any{})
		if state != "" {
			return false, state
		}
		name, ref, _ := caSplitAddress(out.Self)
		if name != "twin" {
			return false, "self_address is " + out.Self
		}
		if ref != "" {
			return false, fmt.Sprintf("the owner's self_address %q carries the ref %q", out.Self, ref)
		}
		return true, ""
	})

	harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "the later account's address carries a twelve-character ref", func(deadline time.Time) (bool, string) {
		out, state := sysPollDecode[caListOut](t, deadline, other, "list_agents", map[string]any{})
		if state != "" {
			return false, state
		}
		name, ref, _ := caSplitAddress(out.Self)
		if name != "twin" {
			return false, "self_address is " + out.Self
		}
		if len(ref) != 12 || !caHex(ref) {
			return false, fmt.Sprintf("the later account's self_address %q carries ref %q, want twelve hexadecimal characters", out.Self, ref)
		}
		return true, ""
	})

	caRequireLocalRoster(t, caWaitNamed(t, root, "twin", 2), "twin", "peer", "other")

	syscontainer.PairAwait(t, caImpObserved, caMarkerBound)
	syscontainer.PairWrite(t, caImpDone, []byte("done\n"))
}

func caImpersonationOnHostB(t *testing.T, sender *syscontainer.Session) {
	t.Helper()

	caRequirePeerRoster(t, caWaitNamed(t, sender, "twin", 2), "twin", "peer", "other")
	syscontainer.PairWrite(t, caImpObserved, []byte("observed\n"))
	syscontainer.PairAwait(t, caImpDone, caMarkerBound)
}

func caInterceptionOnHostA(t *testing.T, root *syscontainer.Session) {
	t.Helper()

	peer := syscontainer.NewSession(t, "peer", "worker")
	caWaitRegistered(t, peer)
	caJoin(t, peer, caTopic)
	other := syscontainer.NewSession(t, "other", "worker")
	caWaitRegistered(t, other)
	caJoin(t, other, caTopic)

	caWaitNamed(t, root, "worker", 2)
	// Wait for the peer to observe both entries before the owner leaves, so its
	// assertion does not race the removal.
	syscontainer.PairAwait(t, caWorkersSeen, caMarkerBound)

	closeSession(t, peer)
	remaining := caWaitSingle(t, root, "worker")
	syscontainer.PairWrite(t, caOwnerLeft, []byte(remaining.Address+"\n"))

	// The bare name belongs to the departed account, so the send must be
	// refused.
	if text, isError := root.Call(t, "send_message", map[string]any{"to": "worker@sys", "message": caLocalBare}); !isError {
		t.Fatalf("a by-name send reached the account that does not own the name: %s", text)
	} else {
		caRequireOwnerRefusal(t, text)
	}

	_, ref, _ := caSplitAddress(remaining.Address)
	if len(ref) != 12 || !caHex(ref) {
		t.Fatalf("the account holding worker carries ref %q, want twelve hexadecimal characters: %q", ref, remaining.Address)
	}
	if text, isError := root.Call(t, "send_message", map[string]any{"to": remaining.Address, "message": caLocalRef}); isError {
		t.Fatalf("a send to the holding account's full ref failed: %s", text)
	}

	syscontainer.PairAwait(t, caSentFile, caMarkerBound)
	caRequireProbes(t, other, caLocalRef, caRemoteRef, caLocalBare, caRemoteBare)
	syscontainer.PairWrite(t, caDoneFile, []byte("done\n"))
}

func caInterceptionOnHostB(t *testing.T, sender *syscontainer.Session) {
	t.Helper()

	caWaitNamed(t, sender, "worker", 2)
	syscontainer.PairWrite(t, caWorkersSeen, []byte("seen\n"))
	ownerAddress := string(syscontainer.PairAwait(t, caOwnerLeft, caMarkerBound))
	_, ref, _ := caSplitAddress(strings.TrimSpace(ownerAddress))
	if len(ref) != 12 || !caHex(ref) {
		t.Fatalf("host A announced the holding account as %q, want its full twelve-character ref line", ownerAddress)
	}

	remaining := caWaitSingle(t, sender, "worker")
	_, _, alias := caSplitAddress(remaining.Address)
	if alias == "" {
		t.Fatalf("the remaining address %q carries no alias", remaining.Address)
	}
	if remaining.Account != "other" {
		t.Fatalf("the remaining entry names account %q, want other: %q", remaining.Account, remaining.Address)
	}

	if text, isError := sender.Call(t, "send_message", map[string]any{"to": "worker@" + alias, "message": caRemoteBare}); !isError {
		t.Fatalf("a by-name send over the link reached the account that does not own the name: %s", text)
	} else {
		caRequireOwnerRefusal(t, text)
	}

	// The holder's own address is the full ref host A announced; the peer's
	// list may render a sole member without its ref, but resolving the bare
	// name is still refused above.
	refAddress := "worker [" + ref + "]@" + alias
	if text, isError := sender.Call(t, "send_message", map[string]any{"to": refAddress, "message": caRemoteRef}); isError {
		t.Fatalf("a send to the holding account's full ref over the link failed: %s", text)
	}

	syscontainer.PairWrite(t, caSentFile, []byte("sent\n"))
	syscontainer.PairAwait(t, caDoneFile, caMarkerBound)
}

func caPublishPairing(t *testing.T, session *syscontainer.Session) {
	t.Helper()

	text, isError := session.Call(t, "create_pairing_key", map[string]any{})
	if isError {
		t.Fatalf("create_pairing_key on host A: %s", text)
	}
	var out struct {
		Key       string   `json:"key"`
		Addresses []string `json:"shown_addresses"`
	}
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		t.Fatalf("decoding create_pairing_key %q: %v", text, err)
	}
	if out.Key == "" || len(out.Addresses) == 0 {
		t.Fatalf("create_pairing_key returned no key or address: %s", text)
	}
	address := out.Addresses[0]
	for _, shown := range out.Addresses {
		if strings.Contains(shown, "hostA:17443") {
			address = shown
			break
		}
	}
	syscontainer.PairWrite(t, caPairingFile, []byte(address+"\n"+out.Key+"\n"))
}

func caReadPairing(t *testing.T, data []byte) (address, key string) {
	t.Helper()
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) != 2 || lines[0] == "" || lines[1] == "" {
		t.Fatalf("the shared pairing file is %q, want an address line and a key line", data)
	}
	return lines[0], lines[1]
}

func caWaitRegistered(t *testing.T, s *syscontainer.Session) {
	t.Helper()
	harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "session "+s.Account+" to register", func(deadline time.Time) (bool, string) {
		_, state := sysPollDecode[caListOut](t, deadline, s, "list_agents", map[string]any{})
		return state == "", state
	})
}

func caJoin(t *testing.T, s *syscontainer.Session, topic string) {
	t.Helper()
	harness.EventuallyPoll(t, caLinkBound, "join_topic "+topic+" as "+s.Account, func(deadline time.Time) (bool, string) {
		text, isError, err := s.PollCall(t, deadline, "join_topic", map[string]any{"name": topic})
		if err != nil {
			return false, err.Error()
		}
		if isError {
			return false, text
		}
		return true, ""
	})
}

func caWaitNamed(t *testing.T, s *syscontainer.Session, name string, want int) caListOut {
	t.Helper()
	var last caListOut
	harness.EventuallyPoll(t, harness.ListAgentsWaitBound, fmt.Sprintf("%d agents named %q are listed", want, name), func(deadline time.Time) (bool, string) {
		out, state := sysPollDecode[caListOut](t, deadline, s, "list_agents", map[string]any{})
		if state != "" {
			return false, state
		}
		last = out
		if got := len(caNamed(out, name)); got != want {
			return false, fmt.Sprintf("found %d agents named %q", got, name)
		}
		return true, ""
	})
	return last
}

func caWaitSingle(t *testing.T, s *syscontainer.Session, name string) caAgentEntry {
	t.Helper()
	out := caWaitNamed(t, s, name, 1)
	return caNamed(out, name)[0]
}

func caRequireLocalRoster(t *testing.T, out caListOut, name, owner, later string) {
	t.Helper()
	entries := caRequireBothAccounts(t, out, name, owner, later)
	for _, entry := range entries {
		_, ref, _ := caSplitAddress(entry.Address)
		switch entry.Account {
		case owner:
			if ref != "" {
				t.Fatalf("the owner account %q carries a ref: %q", owner, entry.Address)
			}
		case later:
			if len(ref) != 12 || !caHex(ref) {
				t.Fatalf("the later account %q carries ref %q, want twelve hexadecimal characters", later, ref)
			}
		}
	}
}

func caRequirePeerRoster(t *testing.T, out caListOut, name, owner, later string) {
	t.Helper()
	entries := caRequireBothAccounts(t, out, name, owner, later)
	for _, entry := range entries {
		_, ref, _ := caSplitAddress(entry.Address)
		switch entry.Account {
		case owner:
			if ref != "" {
				t.Fatalf("the owner account %q carries ref %q on the peer, want a ref-free address", owner, ref)
			}
		case later:
			if len(ref) != 12 || !caHex(ref) {
				t.Fatalf("the later account %q carries ref %q, want twelve hexadecimal characters", later, ref)
			}
		}
	}
}

func caRequireBothAccounts(t *testing.T, out caListOut, name, owner, later string) []caAgentEntry {
	t.Helper()
	entries := caNamed(out, name)
	if len(entries) != 2 {
		t.Fatalf("the roster lists %d agents named %q, want two: %v", len(entries), name, caAddresses(entries))
	}
	found := map[string]bool{}
	for _, entry := range entries {
		switch entry.Account {
		case owner, later:
			found[entry.Account] = true
		default:
			t.Errorf("an entry names account %q, want %q or %q: %q", entry.Account, owner, later, entry.Address)
		}
	}
	if !found[owner] || !found[later] {
		t.Fatalf("the roster does not carry both accounts: %v", caAddresses(entries))
	}
	return entries
}

func caRequireOwnerRefusal(t *testing.T, text string) {
	t.Helper()
	if !strings.Contains(text, "(code: not_found)") {
		t.Fatalf("the refusal code is not not_found: %s", text)
	}
	if !strings.Contains(text, "the name belongs to another account") {
		t.Fatalf("the refusal does not say the name belongs to another account: %s", text)
	}
}

func caRequireProbes(t *testing.T, holder *syscontainer.Session, wantFirst, wantSecond, refuseFirst, refuseSecond string) {
	t.Helper()
	harness.Eventually(t, caRemoveBound, "the holding account receives both ref-addressed probes and no bare-name one", func() (bool, string) {
		frames := holder.Fake.Frames()
		for _, probe := range []string{wantFirst, wantSecond} {
			if got := caCountFrames(frames, probe); got != 1 {
				return false, fmt.Sprintf("the holder received %q %d times, want once", probe, got)
			}
		}
		for _, probe := range []string{refuseFirst, refuseSecond} {
			if got := caCountFrames(frames, probe); got != 0 {
				return false, fmt.Sprintf("the holder received the bare-name probe %q %d times, want none", probe, got)
			}
		}
		return true, ""
	})
}

func caCountFrames(frames []fakesession.Frame, probe string) int {
	count := 0
	for _, frame := range frames {
		if strings.Contains(frame.Content, probe) {
			count++
		}
	}
	return count
}

func caNamed(out caListOut, name string) []caAgentEntry {
	var entries []caAgentEntry
	for _, entry := range out.Agents {
		if got, _, _ := caSplitAddress(entry.Address); got == name {
			entries = append(entries, entry)
		}
	}
	return entries
}

func caAddresses(entries []caAgentEntry) []string {
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.Address+" (account "+entry.Account+")")
	}
	return out
}

// caSplitAddress splits "name [ref]@alias" into its three parts.
func caSplitAddress(address string) (name, ref, alias string) {
	at := strings.LastIndexByte(address, '@')
	if at < 0 {
		return strings.TrimSpace(address), "", ""
	}
	name, alias = address[:at], address[at+1:]
	if open := strings.LastIndexByte(name, '['); open >= 0 && strings.HasSuffix(name, "]") {
		ref = strings.TrimSpace(name[open+1 : len(name)-1])
		name = name[:open]
	}
	return strings.TrimSpace(name), ref, strings.TrimSpace(alias)
}

func caHex(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}
