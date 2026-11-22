package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakesession"
)

// listCall performs one ListAgents call with a bounded context.
func listCall(t *testing.T, client localpb.LocalClient, req *localpb.ListAgentsRequest) (*localpb.ListAgentsResponse, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	return client.ListAgents(ctx, req)
}

// mustListAgents lists and fails the test on an RPC error.
func mustListAgents(t *testing.T, tb *testBroker, req *localpb.ListAgentsRequest) *localpb.ListAgentsResponse {
	t.Helper()
	resp, err := listCall(t, tb.client, req)
	if err != nil {
		t.Fatalf("ListAgents(%v): %v", req, err)
	}
	return resp
}

// registerListAgent registers fake on tb and returns its agent ID.
func registerListAgent(t *testing.T, tb *testBroker, fake *fakesession.Fake) AgentID {
	t.Helper()
	rs := startRegistration(t, tb, fake)
	return AgentID(rs.complete(eventTimeout).GetAgentId())
}

// joinListTopics joins the agent to each topic on the fake topic table.
func joinListTopics(t *testing.T, tb *testBroker, id AgentID, names ...string) {
	t.Helper()
	for _, name := range names {
		if err := tb.topics.Join(id, name); err != nil {
			t.Fatalf("Join(%s, %q): %v", id, name, err)
		}
	}
}

// listAddresses returns the entry addresses in the order the Broker sent them.
func listAddresses(resp *localpb.ListAgentsResponse) []string {
	out := make([]string, 0, len(resp.GetAgents()))
	for _, entry := range resp.GetAgents() {
		out = append(out, entry.GetAddress())
	}
	return out
}

// listEntryByAddress returns the entry whose address is addr.
func listEntryByAddress(t *testing.T, resp *localpb.ListAgentsResponse, addr string) *localpb.AgentEntry {
	t.Helper()
	for _, entry := range resp.GetAgents() {
		if entry.GetAddress() == addr {
			return entry
		}
	}
	t.Fatalf("no entry with address %q in %v", addr, listAddresses(resp))
	return nil
}

func TestListAgentsIncludesSelf(t *testing.T) {
	// REQ-agent-messaging-001, REQ-agent-messaging-011: a registered session
	// gets its own address and is not listed as another agent.
	tb := newTestBroker(t)
	self := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: "api-worker"}))

	resp := mustListAgents(t, tb, &localpb.ListAgentsRequest{AgentId: string(self)})
	if got := resp.GetSelfAddress(); got != "api-worker@hostA" {
		t.Fatalf("self_address = %q, want api-worker@hostA", got)
	}
	if got := listAddresses(resp); len(got) != 0 {
		t.Fatalf("agents = %v, want none besides the caller", got)
	}
}

func TestUnregisteredSessionNotListed(t *testing.T) {
	// REQ-agent-messaging-002: a session that has a registry record but never
	// registered a connection is not listed.
	tb := newTestBroker(t)
	self := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: "api-worker"}))
	joinListTopics(t, tb, self, "t1")

	_ = fakesession.New(t, fakesession.Options{Name: "ghost"})

	resp := mustListAgents(t, tb, &localpb.ListAgentsRequest{AgentId: string(self)})
	if got := listAddresses(resp); len(got) != 0 {
		t.Fatalf("agents = %v, want the unregistered ghost to be absent", got)
	}
}

func TestListAgentsSharedTopics(t *testing.T) {
	// REQ-agent-messaging-009: each entry carries the topics shared with the
	// caller, and only those.
	tb := newTestBroker(t)
	self := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: "caller"}))
	peer := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: "peer"}))

	joinListTopics(t, tb, self, "t1", "t2", "t3")
	joinListTopics(t, tb, peer, "t1", "t2", "t4")

	resp := mustListAgents(t, tb, &localpb.ListAgentsRequest{AgentId: string(self)})
	entry := listEntryByAddress(t, resp, "peer@hostA")
	if got := entry.GetSharedTopics(); !slices.Equal(got, []string{"t1", "t2"}) {
		t.Fatalf("shared_topics = %v, want [t1 t2]", got)
	}
}

func TestListAgentsLocalDuplicateNamesGetRefs(t *testing.T) {
	// REQ-agent-messaging-004: two local agents with the same name both carry
	// the disambiguation ref in their address.
	tb := newTestBroker(t)
	self := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: "caller"}))
	first := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: "w"}))
	second := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: "w"}))
	joinListTopics(t, tb, self, "t1")
	joinListTopics(t, tb, first, "t1")
	joinListTopics(t, tb, second, "t1")

	firstSnap, ok := tb.b.lookupAgent(first)
	if !ok {
		t.Fatal("the first duplicate agent is not in the table")
	}
	secondSnap, ok := tb.b.lookupAgent(second)
	if !ok {
		t.Fatal("the second duplicate agent is not in the table")
	}
	for _, snap := range []agentSnapshot{firstSnap, secondSnap} {
		if len(snap.Ref) < 6 || len(snap.Ref) > 12 {
			t.Fatalf("ref %q is not 6 to 12 characters", snap.Ref)
		}
		if !strings.HasPrefix(string(snap.ID), snap.Ref) {
			t.Fatalf("ref %q is not a prefix of %s", snap.Ref, snap.ID)
		}
	}

	resp := mustListAgents(t, tb, &localpb.ListAgentsRequest{AgentId: string(self)})
	want := []string{
		fmt.Sprintf("w [%s]@hostA", firstSnap.Ref),
		fmt.Sprintf("w [%s]@hostA", secondSnap.Ref),
	}
	sort.Strings(want)
	if got := listAddresses(resp); !slices.Equal(got, want) {
		t.Fatalf("agents = %v, want %v", got, want)
	}
}

func TestListAgentsRemote(t *testing.T) {
	// REQ-agent-messaging-004, REQ-agent-messaging-008: remote agents are listed
	// with the peer's effective alias, get a ref when their name is not unique
	// on the peer, and are skipped when the alias is unknown.
	t.Run("alias and shared topics", func(t *testing.T) {
		tb := newTestBroker(t)
		self := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: "caller"}))
		joinListTopics(t, tb, self, "t1")

		var fp [32]byte
		fp[0] = 0xb1
		tb.topics.SetRemote(fp,
			[]Member{{Name: "api-worker", Ref: "abcdef", Status: "idle"}},
			map[string][]string{"api-worker": {"t1"}})
		tb.peers.aliases[fp] = "hostB"

		resp := mustListAgents(t, tb, &localpb.ListAgentsRequest{AgentId: string(self)})
		if got := listAddresses(resp); !slices.Equal(got, []string{"api-worker@hostB"}) {
			t.Fatalf("agents = %v, want [api-worker@hostB]", got)
		}
		entry := resp.GetAgents()[0]
		if entry.GetStatus() != localpb.AgentStatus_AGENT_STATUS_IDLE {
			t.Fatalf("status = %v, want AGENT_STATUS_IDLE", entry.GetStatus())
		}
		if got := entry.GetSharedTopics(); !slices.Equal(got, []string{"t1"}) {
			t.Fatalf("shared_topics = %v, want [t1]", got)
		}
	})

	t.Run("duplicate names carry refs", func(t *testing.T) {
		tb := newTestBroker(t)
		self := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: "caller"}))
		joinListTopics(t, tb, self, "t1")

		var fp [32]byte
		fp[0] = 0xb2
		tb.topics.SetRemote(fp, []Member{
			{Name: "dup", Ref: "aaaaaa", Status: "busy"},
			{Name: "dup", Ref: "bbbbbb"},
		}, map[string][]string{"dup": {"t1"}})
		tb.peers.aliases[fp] = "hostB"

		resp := mustListAgents(t, tb, &localpb.ListAgentsRequest{AgentId: string(self)})
		want := []string{"dup [aaaaaa]@hostB", "dup [bbbbbb]@hostB"}
		if got := listAddresses(resp); !slices.Equal(got, want) {
			t.Fatalf("agents = %v, want %v", got, want)
		}
		if got := resp.GetAgents()[0].GetStatus(); got != localpb.AgentStatus_AGENT_STATUS_BUSY {
			t.Fatalf("first duplicate status = %v, want AGENT_STATUS_BUSY", got)
		}
		if got := resp.GetAgents()[1].GetStatus(); got != localpb.AgentStatus_AGENT_STATUS_UNSPECIFIED {
			t.Fatalf("second duplicate status = %v, want AGENT_STATUS_UNSPECIFIED", got)
		}
	})

	t.Run("ref-required collider leaves the owner ref-free", func(t *testing.T) {
		tb := newTestBroker(t)
		self := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: "caller"}))
		joinListTopics(t, tb, self, "t1")

		var fp [32]byte
		fp[0] = 0xb5
		tb.topics.SetRemote(fp, []Member{
			{Name: "dup", Ref: "aaaaaa"},
			{Name: "dup", Ref: "bbbbbb", RefRequired: true},
		}, map[string][]string{"dup": {"t1"}})
		tb.peers.aliases[fp] = "hostB"

		resp := mustListAgents(t, tb, &localpb.ListAgentsRequest{AgentId: string(self)})
		want := []string{"dup [bbbbbb]@hostB", "dup@hostB"}
		if got := listAddresses(resp); !slices.Equal(got, want) {
			t.Fatalf("agents = %v, want %v: the owner must stay ref-free next to a ref-required collider", got, want)
		}
	})

	t.Run("peer without alias is skipped", func(t *testing.T) {
		tb := newTestBroker(t)
		self := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: "caller"}))
		joinListTopics(t, tb, self, "t1")

		var fp [32]byte
		fp[0] = 0xb3
		tb.topics.SetRemote(fp,
			[]Member{{Name: "stranger", Ref: "abcdef"}},
			map[string][]string{"stranger": {"t1"}})

		resp := mustListAgents(t, tb, &localpb.ListAgentsRequest{AgentId: string(self)})
		if got := listAddresses(resp); len(got) != 0 {
			t.Fatalf("agents = %v, want the peer without a known alias to be skipped", got)
		}
	})
}

func TestListAgentsEntriesCarryAccount(t *testing.T) {
	// REQ-agent-messaging-008: every entry names the account that registered the
	// agent. A local entry takes the registration account; a remote entry takes
	// the account its publisher put on the roster. The field is display only.
	tb := newTestBroker(t)
	b := tb.b
	// The fixture inserts agents directly, so the session reread would revoke
	// them; keep it from touching the table.
	setRefreshOne(b, func(*agent) {})

	caller := testAgentID("cccccc1")
	owner := testAgentID("aaaaaa1")
	other := testAgentID("bbbbbb1")
	anonymous := testAgentID("dddddd1")

	b.mu.Lock()
	insertCrossAccountAgent(b, caller, "caller", 1003, 3)
	insertCrossAccountAgent(b, owner, "worker", 1001, 1)
	insertCrossAccountAgent(b, other, "other", 1002, 2)
	// An agent whose account name was never looked up falls back to the
	// decimal uid, the same fallback AgentStatusEntry.account uses.
	insertCrossAccountAgent(b, anonymous, "anon", 1004, 4)
	b.agents[anonymous].caller.Username = ""
	b.recomputeRefsLocked()
	b.mu.Unlock()

	for _, id := range []AgentID{caller, owner, other, anonymous} {
		joinListTopics(t, tb, id, "t1")
	}

	var fp [32]byte
	fp[0] = 0xc1
	tb.topics.SetRemote(fp,
		[]Member{{Name: "remote-worker", Ref: "abcdef", Account: "peeracct"}},
		map[string][]string{"remote-worker": {"t1"}})
	tb.peers.aliases[fp] = "hostB"

	resp, err := b.ListAgents(accessCallerContext(1003, "acct1003"), &localpb.ListAgentsRequest{AgentId: string(caller)})
	if err != nil {
		t.Fatalf("ListAgents: %v", err)
	}

	// Marshal the response instead of reading the Go field: the assertion then
	// fails on a missing account rather than the test failing to compile against
	// code that predates the field.
	raw, err := protojson.Marshal(resp)
	if err != nil {
		t.Fatalf("protojson.Marshal: %v", err)
	}
	var decoded struct {
		Agents []struct {
			Address string `json:"address"`
			Account string `json:"account"`
		} `json:"agents"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	seen := make(map[string]string, len(decoded.Agents))
	for _, a := range decoded.Agents {
		seen[a.Address] = a.Account
	}
	want := map[string]string{
		"worker@hostA":        "acct1001",
		"other@hostA":         "acct1002",
		"anon@hostA":          "1004",
		"remote-worker@hostB": "peeracct",
	}
	for addr, acct := range want {
		got, ok := seen[addr]
		if !ok {
			t.Errorf("no entry for %s in %v", addr, raw)
			continue
		}
		if got != acct {
			t.Errorf("account for %s = %q, want %q", addr, got, acct)
		}
	}
}

func TestListAgentsSkipsRemoteMemberWithInvalidRef(t *testing.T) {
	// A remote member arrives from the peer, so its ref is untrusted: an
	// invalid ref must skip that entry with a warning instead of panicking the
	// process, and the warning must not carry the member's name.
	tb := newTestBroker(t)
	self := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: "caller"}))
	joinListTopics(t, tb, self, "t1")

	var fp [32]byte
	fp[0] = 0xb4
	tb.topics.SetRemote(fp, []Member{
		{Name: "x", Ref: ""},
		{Name: "good", Ref: "abcdef", Status: "idle"},
	}, map[string][]string{"x": {"t1"}, "good": {"t1"}})
	tb.peers.aliases[fp] = "hostB"

	resp := mustListAgents(t, tb, &localpb.ListAgentsRequest{AgentId: string(self)})
	if got := listAddresses(resp); !slices.Equal(got, []string{"good@hostB"}) {
		t.Fatalf("agents = %v, want only the member with a valid ref", got)
	}

	// The literal event name, not the production constant, so this test also
	// compiles and fails against the code that panics.
	const event = "remote_member_invalid"
	var warnings []map[string]any
	for _, line := range tb.logLines(t) {
		if line["event"] != event {
			continue
		}
		warnings = append(warnings, line)
	}
	if len(warnings) != 1 {
		t.Fatalf("got %d %s records, want one", len(warnings), event)
	}
	warn := warnings[0]
	if got := warn["level"]; got != "WARN" {
		t.Fatalf("level = %v, want WARN", got)
	}
	for key, value := range warn {
		if s, ok := value.(string); ok && s == "x" {
			t.Fatalf("log field %q carries the member name: %v", key, warn)
		}
	}
}

func TestListAgentsStatus(t *testing.T) {
	// REQ-agent-messaging-010: the status is the registry value mapped to the
	// local interface enum, unchanged by the listing.
	tb := newTestBroker(t)
	self := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: "caller"}))
	alpha := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: "alpha"}))
	bravo := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: "bravo"}))
	charlie := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: "charlie"}))
	joinListTopics(t, tb, self, "t1")
	joinListTopics(t, tb, alpha, "t1")
	joinListTopics(t, tb, bravo, "t1")
	joinListTopics(t, tb, charlie, "t1")

	// This test is about the status mapping, not the reread: keep the state set
	// below from being overwritten by the session record.
	setRefreshOne(tb.b, func(*agent) {})
	setAgentState(t, tb.b, alpha, "alpha", "busy", "")
	setAgentState(t, tb.b, bravo, "bravo", "idle", "")
	setAgentState(t, tb.b, charlie, "charlie", "", "")

	resp := mustListAgents(t, tb, &localpb.ListAgentsRequest{AgentId: string(self)})
	want := []struct {
		address string
		status  localpb.AgentStatus
	}{
		{"alpha@hostA", localpb.AgentStatus_AGENT_STATUS_BUSY},
		{"bravo@hostA", localpb.AgentStatus_AGENT_STATUS_IDLE},
		{"charlie@hostA", localpb.AgentStatus_AGENT_STATUS_UNSPECIFIED},
	}
	if got := listAddresses(resp); len(got) != len(want) {
		t.Fatalf("agents = %v, want %d entries", got, len(want))
	}
	for _, w := range want {
		if got := listEntryByAddress(t, resp, w.address).GetStatus(); got != w.status {
			t.Fatalf("%s status = %v, want %v", w.address, got, w.status)
		}
	}
}

func TestListAgentsRereadsBeforeListing(t *testing.T) {
	// Listing rereads the caller and the local visible agents before building
	// the entries, so a rename shows in the same response.
	tb := newTestBroker(t)
	self := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: "caller"}))
	peer := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: "bravo"}))
	joinListTopics(t, tb, self, "t1")
	joinListTopics(t, tb, peer, "t1")

	var mu sync.Mutex
	var order []AgentID
	setRefreshOne(tb.b, func(a *agent) {
		mu.Lock()
		order = append(order, a.id)
		mu.Unlock()
		if a.id != peer {
			return
		}
		tb.b.mu.Lock()
		if ag, ok := tb.b.agents[peer]; ok {
			ag.name = "renamed"
		}
		tb.b.mu.Unlock()
	})

	resp := mustListAgents(t, tb, &localpb.ListAgentsRequest{AgentId: string(self)})

	mu.Lock()
	got := append([]AgentID(nil), order...)
	mu.Unlock()
	if len(got) < 2 || got[0] != self {
		t.Fatalf("reread order = %v, want the caller %s first", got, self)
	}
	if !slices.Contains(got, peer) {
		t.Fatalf("reread order = %v, want it to include %s", got, peer)
	}
	if got := listAddresses(resp); !slices.Equal(got, []string{"renamed@hostA"}) {
		t.Fatalf("agents = %v, want the renamed address", got)
	}
}

func TestListAgentsPrevName(t *testing.T) {
	// REQ-agent-messaging-032: a previous name within its retention period is
	// reported, and an expired (cleared) one is not.
	tb := newTestBroker(t)
	self := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: "caller"}))
	peer := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: "bravo"}))
	joinListTopics(t, tb, self, "t1")
	joinListTopics(t, tb, peer, "t1")

	setAgentState(t, tb.b, peer, "bravo", "idle", "api-worker")
	resp := mustListAgents(t, tb, &localpb.ListAgentsRequest{AgentId: string(self)})
	if got := listEntryByAddress(t, resp, "bravo@hostA").GetPreviousName(); got != "api-worker" {
		t.Fatalf("previous_name = %q, want api-worker", got)
	}

	setAgentState(t, tb.b, peer, "bravo", "idle", "")
	resp = mustListAgents(t, tb, &localpb.ListAgentsRequest{AgentId: string(self)})
	if got := listEntryByAddress(t, resp, "bravo@hostA").GetPreviousName(); got != "" {
		t.Fatalf("previous_name = %q, want empty after expiration", got)
	}
}

func TestListAgentsFilters(t *testing.T) {
	tb := newTestBroker(t)
	self := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: "caller"}))
	api := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: "api-worker"}))
	other := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: "other"}))
	joinListTopics(t, tb, self, "t1", "t2")
	joinListTopics(t, tb, api, "t1", "t2")
	joinListTopics(t, tb, other, "t2")

	cases := []struct {
		name string
		req  *localpb.ListAgentsRequest
		want []string
	}{
		{
			name: "topic",
			req:  &localpb.ListAgentsRequest{AgentId: string(self), Topic: "t1"},
			want: []string{"api-worker@hostA"},
		},
		{
			name: "broker",
			req:  &localpb.ListAgentsRequest{AgentId: string(self), Broker: "hostA"},
			want: []string{"api-worker@hostA", "other@hostA"},
		},
		{
			name: "unknown broker",
			req:  &localpb.ListAgentsRequest{AgentId: string(self), Broker: "hostB"},
			want: nil,
		},
		{
			name: "name is normalized",
			req:  &localpb.ListAgentsRequest{AgentId: string(self), Name: " API Worker "},
			want: []string{"api-worker@hostA"},
		},
		{
			name: "blank name matches nothing",
			req:  &localpb.ListAgentsRequest{AgentId: string(self), Name: "   "},
			want: nil,
		},
		{
			name: "combined",
			req:  &localpb.ListAgentsRequest{AgentId: string(self), Topic: "t2", Broker: "hostA", Name: "api-worker"},
			want: []string{"api-worker@hostA"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := mustListAgents(t, tb, tc.req)
			if got := listAddresses(resp); !slices.Equal(got, tc.want) {
				t.Fatalf("agents = %v, want %v", got, tc.want)
			}
		})
	}

	// Filtering decides which entries are returned; it does not trim the
	// shared topics of the entries that remain.
	resp := mustListAgents(t, tb, &localpb.ListAgentsRequest{AgentId: string(self), Topic: "t1"})
	if got := listEntryByAddress(t, resp, "api-worker@hostA").GetSharedTopics(); !slices.Equal(got, []string{"t1", "t2"}) {
		t.Fatalf("shared_topics = %v, want [t1 t2] after filtering", got)
	}
}

func TestListAgentsLongName(t *testing.T) {
	// REQ-agent-messaging-037: an over-long name is truncated in the address
	// and the ref is kept.
	tb := newTestBroker(t)
	self := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: "caller"}))
	long := strings.Repeat("a", 70)
	longID := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: long}))
	joinListTopics(t, tb, self, "t1")
	joinListTopics(t, tb, longID, "t1")

	resp := mustListAgents(t, tb, &localpb.ListAgentsRequest{AgentId: string(self)})
	if len(resp.GetAgents()) != 1 {
		t.Fatalf("agents = %v, want one entry", listAddresses(resp))
	}
	address := resp.GetAgents()[0].GetAddress()
	if utf8.RuneCountInString(address) > MaxAddressRunes {
		t.Fatalf("address %q has more than %d runes", address, MaxAddressRunes)
	}
	if len(address) > MaxAddressBytes {
		t.Fatalf("address %q has more than %d bytes", address, MaxAddressBytes)
	}
	name, ref, alias, ok := ParseAddress(address)
	if !ok {
		t.Fatalf("address %q does not parse", address)
	}
	if ref == "" {
		t.Fatalf("address %q carries no ref after truncation", address)
	}
	if alias != "hostA" {
		t.Fatalf("address %q alias = %q, want hostA", address, alias)
	}
	if name == "" || !strings.HasPrefix(long, name) {
		t.Fatalf("address %q name = %q, want a prefix of the session name", address, name)
	}
}

// TestListAgentsLongNameAddressCarriesFullRef checks that the truncated address
// list_agents shows always carries the full twelve-character ref, and that a
// caller can select the Agent with exactly that address.
func TestListAgentsLongNameAddressCarriesFullRef(t *testing.T) {
	tb := newTestBroker(t)
	caller := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: "caller"}))
	longName := strings.Repeat("x", 70)
	longID := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: longName}))
	joinListTopics(t, tb, caller, "t1")
	joinListTopics(t, tb, longID, "t1")

	resp := mustListAgents(t, tb, &localpb.ListAgentsRequest{AgentId: string(caller)})
	if len(resp.GetAgents()) != 1 {
		t.Fatalf("agents = %v, want one entry", listAddresses(resp))
	}
	address := resp.GetAgents()[0].GetAddress()
	name, ref, alias, ok := ParseAddress(address)
	if !ok {
		t.Fatalf("address %q does not parse", address)
	}
	if alias != "hostA" {
		t.Fatalf("address %q alias = %q, want hostA", address, alias)
	}
	if len(ref) != 12 {
		t.Fatalf("address %q carries a %d-character ref, want the full 12", address, len(ref))
	}
	if !strings.HasPrefix(longName, name) {
		t.Fatalf("address %q name = %q, want a prefix of the long name", address, name)
	}

	agents := newCrossAccountAgents()
	inbox := newRecordingInbox()
	agents.refresh(tb.b, map[string]*recordingInbox{string(longID): inbox})
	topics := &deliveryTopics{
		local: map[string][]string{
			string(caller): {"t1"},
			string(longID): {"t1"},
		},
		remote: make(map[peerID][]deliveryRemoteEntry),
	}
	d := newCollisionDelivery(agents, topics, &syncBuffer{})
	if err := d.Send(context.Background(), agents.views[string(caller)], address, "hello"); err != nil {
		t.Fatalf("Send to the listed truncated address: %v", err)
	}
	calls := inbox.Calls()
	if len(calls) != 1 {
		t.Fatalf("the long-named Agent received %d messages, want 1", len(calls))
	}
	if calls[0].Sig != "caller@hostA" {
		t.Fatalf("signature = %q, want caller@hostA", calls[0].Sig)
	}
}

func TestListAgentsNotRegistered(t *testing.T) {
	t.Run("unknown agent", func(t *testing.T) {
		tb := newTestBroker(t)
		_, err := listCall(t, tb.client, &localpb.ListAgentsRequest{AgentId: string(testAgentID("dddddd0"))})
		wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_NOT_REGISTERED)
		st, _ := status.FromError(err)
		if st.Code() != codes.FailedPrecondition {
			t.Fatalf("gRPC code = %v, want FailedPrecondition", st.Code())
		}
	})

	t.Run("caller revoked during reread", func(t *testing.T) {
		tb := newTestBroker(t)
		self := registerListAgent(t, tb, fakesession.New(t, fakesession.Options{Name: "caller"}))

		setRefreshOne(tb.b, func(a *agent) {
			if a.id != self {
				return
			}
			tb.b.mu.Lock()
			ag, ok := tb.b.agents[self]
			if !ok {
				tb.b.mu.Unlock()
				return
			}
			stops := tb.b.unregisterLocked(ag)
			tb.b.unlockAndNotify(stops)
		})

		_, err := listCall(t, tb.client, &localpb.ListAgentsRequest{AgentId: string(self)})
		wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_NOT_REGISTERED)
	})
}
