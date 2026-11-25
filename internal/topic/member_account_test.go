package topic

import (
	"slices"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/topic/topicpb"
)

// TestMemberAccountAndRefRequiredAcrossBrokers checks that a roster entry's
// account and ref_required fields cross the wire to another Broker.
func TestMemberAccountAndRefRequiredAcrossBrokers(t *testing.T) {
	const name = "t1"

	a := wlNewEndpoint(t, fakeLinkFingerprint(1))
	b := wlNewEndpoint(t, fakeLinkFingerprint(2))

	alpha := AgentID("alpha")
	charlie := AgentID("charlie")

	a.r.AgentChanged(alpha, Member{
		Name:        "worker",
		Ref:         "aa0001",
		Status:      "idle",
		Account:     "bob",
		RefRequired: true,
	})
	a.join(t, alpha, name)
	b.introduce(t, charlie, "charlie", "c10002")
	b.join(t, charlie, name)

	connect(a.l, b.l, []byte("salt"))
	flushAll(t, a.l, b.l)

	wlCheckMembers(t, "B.RemoteMembers(A)", b.r.RemoteMembers(a.l.self), []Member{{
		Name:        "worker",
		Ref:         "aa0001",
		Status:      "idle",
		Account:     "bob",
		RefRequired: true,
	}})

	a.l.clearSent(b.l.self)
	a.r.AgentChanged(alpha, Member{
		Name:    "worker",
		Ref:     "aa0001",
		Status:  "idle",
		Account: "bob",
	})
	flushAll(t, a.l, b.l)

	frames := a.l.sentFrames(b.l.self)
	if len(frames) != 1 || frames[0].Kind != topicpb.KindRosterDelta {
		t.Fatalf("A sent %d frames to B after the RefRequired flip, want one RosterDelta", len(frames))
	}
	wlCheckMembers(t, "B.RemoteMembers(A) after the flip", b.r.RemoteMembers(a.l.self), []Member{{
		Name:    "worker",
		Ref:     "aa0001",
		Status:  "idle",
		Account: "bob",
	}})
}

// TestAccountValidated checks that the account field is kept when legal and
// cleared otherwise, while the member entry survives.
func TestAccountValidated(t *testing.T) {
	p := fakeLinkFingerprint(2)
	salt := []byte("salt")
	fl := newFakeLink(t, fakeLinkFingerprint(1))
	r, buf := newLinkRegistry(t, fl)
	seedLocal(t, r, "a", Member{Name: "api", Ref: "aaaaaa"}, "t1")

	fl.up(p, salt)
	flushAll(t, fl)
	d1 := digest(salt, p, "t1")
	fl.inject(p, topicpb.KindTopicDigests, digestsMsg([][32]byte{d1}))
	flushAll(t, fl)

	atLimit := strings.Repeat("b", MaxMemberNameBytes)
	tooLong := strings.Repeat("a", MaxMemberNameBytes+1)
	fl.inject(p, topicpb.KindRoster, &topicpb.Roster{Digest: d1[:], Members: []*topicpb.Member{
		{Name: "with-account", Ref: "bbbbbb", Account: "bob"},
		{Name: "at-limit", Ref: "cccccc", Account: atLimit},
		{Name: "empty-account", Ref: "dddddd"},
		{Name: "too-long", Ref: "eeeeee", Account: tooLong},
		{Name: "with-control", Ref: "ffffff", Account: "bo\nb"},
		{Name: "with-space", Ref: "111111", Account: "bo b"},
		{Name: "with-at", Ref: "222222", Account: "a@b"},
	}})
	flushAll(t, fl)

	got := memberNames(r.RemoteMembers(p))
	slices.Sort(got)
	want := []string{"at-limit", "empty-account", "too-long", "with-account", "with-at", "with-control", "with-space"}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("RemoteMembers names = %v, want %v", got, want)
	}
	for _, m := range r.RemoteMembers(p) {
		switch m.Name {
		case "with-account":
			if m.Account != "bob" {
				t.Fatalf("with-account.Account = %q, want bob", m.Account)
			}
		case "at-limit":
			if m.Account != atLimit {
				t.Fatalf("at-limit.Account = %q, want the %d-byte account", m.Account, MaxMemberNameBytes)
			}
		case "too-long", "with-control", "with-space", "with-at":
			if m.Account != "" {
				t.Fatalf("%s.Account = %q, want the illegal account cleared", m.Name, m.Account)
			}
		}
	}

	logs := buf.String()
	if strings.Contains(logs, "topic.member_dropped") {
		t.Fatalf("an illegal account dropped a member entry: %s", logs)
	}
	if strings.Contains(logs, tooLong) || strings.Contains(logs, atLimit) {
		t.Errorf("log leaks an account name: %s", logs)
	}
}
