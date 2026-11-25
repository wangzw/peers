package topic

import (
	"errors"
	"slices"
	"testing"
)

// TestJoinRequiresInfo checks that joining a topic for an agent that setInfo
// never introduced fails without touching the table.
func TestJoinRequiresInfo(t *testing.T) {
	lt := newLocalTable()

	joined, created, err := lt.join("a", "t1")
	if !errors.Is(err, ErrUnknownAgent) {
		t.Fatalf("join for an unknown agent: err = %v, want ErrUnknownAgent", err)
	}
	if joined || created {
		t.Errorf("join for an unknown agent = (%v, %v), want (false, false)", joined, created)
	}
	if names := lt.topicNames(); len(names) != 0 {
		t.Errorf("topicNames after a rejected join = %v, want none", names)
	}
	if _, ok := lt.info("a"); ok {
		t.Error("info found an agent that setInfo never introduced")
	}
	if lt.inTopic("a", "t1") {
		t.Error("the rejected agent is a member of t1")
	}
}

// TestJoinLeaveFlags checks the joined, created, left and emptied flags.
func TestJoinLeaveFlags(t *testing.T) {
	lt := newLocalTable()
	lt.setInfo("a", Member{Name: "api-worker", Ref: "aaaaaa"})
	lt.setInfo("b", Member{Name: "frontend", Ref: "bbbbbb"})

	joined, created, err := lt.join("a", "t1")
	if err != nil || !joined || !created {
		t.Fatalf("first join = (%v, %v, %v), want (true, true, nil)", joined, created, err)
	}
	if !lt.inTopic("a", "t1") {
		t.Error("a is not a member of t1 after joining it")
	}

	joined, created, err = lt.join("a", "t1")
	if err != nil || joined || created {
		t.Errorf("repeated join = (%v, %v, %v), want (false, false, nil)", joined, created, err)
	}

	joined, created, err = lt.join("b", "t1")
	if err != nil || !joined || created {
		t.Errorf("second agent joining a non-empty topic = (%v, %v, %v), want (true, false, nil)", joined, created, err)
	}

	left, emptied := lt.leave("a", "t1")
	if !left || emptied {
		t.Errorf("first leave = (%v, %v), want (true, false)", left, emptied)
	}
	if lt.inTopic("a", "t1") {
		t.Error("a is still a member of t1 after leaving it")
	}

	left, emptied = lt.leave("b", "t1")
	if !left || !emptied {
		t.Errorf("last leave = (%v, %v), want (true, true)", left, emptied)
	}
	if names := lt.topicNames(); slices.Contains(names, "t1") {
		t.Errorf("topicNames still contains an emptied topic: %v", names)
	}

	left, emptied = lt.leave("a", "t1")
	if left || emptied {
		t.Errorf("leave for a non-member = (%v, %v), want (false, false)", left, emptied)
	}

	left, emptied = lt.leave("nobody", "t1")
	if left || emptied {
		t.Errorf("leave for an unknown agent = (%v, %v), want (false, false)", left, emptied)
	}
}

// TestMultipleTopics checks that one agent can belong to several topics.
func TestMultipleTopics(t *testing.T) {
	lt := newLocalTable()
	lt.setInfo("a", Member{Name: "api-worker", Ref: "aaaaaa"})
	if _, _, err := lt.join("a", "t2"); err != nil {
		t.Fatalf("join t2: %v", err)
	}
	if _, _, err := lt.join("a", "t1"); err != nil {
		t.Fatalf("join t1: %v", err)
	}

	if got, want := lt.topicsOf("a"), []string{"t1", "t2"}; !slices.Equal(got, want) {
		t.Errorf("topicsOf(a) = %v, want %v", got, want)
	}
}

// TestLeaveAllRemovesAgent checks that leaveAll drops every membership and the
// agent's information itself.
func TestLeaveAllRemovesAgent(t *testing.T) {
	lt := newLocalTable()
	lt.setInfo("a", Member{Name: "api-worker", Ref: "aaaaaa"})
	lt.setInfo("b", Member{Name: "frontend", Ref: "bbbbbb"})
	_, _, _ = lt.join("a", "t1")
	_, _, _ = lt.join("a", "t2")
	_, _, _ = lt.join("b", "t2")

	info, emptied, remaining, existed := lt.leaveAll("a")
	if !existed {
		t.Fatal("leaveAll reported an existing agent as absent")
	}
	if info.Name != "api-worker" || info.Ref != "aaaaaa" {
		t.Errorf("leaveAll returned info %+v, want the agent's information", info)
	}
	if want := []string{"t1"}; !slices.Equal(emptied, want) {
		t.Errorf("emptied = %v, want %v", emptied, want)
	}
	if want := []string{"t2"}; !slices.Equal(remaining, want) {
		t.Errorf("remaining = %v, want %v", remaining, want)
	}
	if _, ok := lt.info("a"); ok {
		t.Error("info still finds the agent after leaveAll")
	}
	if got := lt.topicsOf("a"); got != nil {
		t.Errorf("topicsOf after leaveAll = %v, want nil", got)
	}
	if _, _, err := lt.join("a", "t1"); !errors.Is(err, ErrUnknownAgent) {
		t.Errorf("join after leaveAll: err = %v, want ErrUnknownAgent", err)
	}
	if members := lt.members("t2"); len(members) != 1 || members[0].Ref != "bbbbbb" {
		t.Errorf("members(t2) after leaveAll = %+v, want only b", members)
	}
	if names := lt.topicNames(); !slices.Equal(names, []string{"t2"}) {
		t.Errorf("topicNames after leaveAll = %v, want [t2]", names)
	}

	if _, emptied, remaining, existed := lt.leaveAll("a"); existed || emptied != nil || remaining != nil {
		t.Errorf("second leaveAll = (%v, %v, %v), want (false, nil, nil)", existed, emptied, remaining)
	}
}

// TestMembersReflectInfo checks that members reads the current information and
// orders entries by Ref.
func TestMembersReflectInfo(t *testing.T) {
	lt := newLocalTable()
	lt.setInfo("a", Member{Name: "api-worker", Ref: "rb"})
	lt.setInfo("b", Member{Name: "frontend", Ref: "ra"})
	_, _, _ = lt.join("a", "t1")
	_, _, _ = lt.join("b", "t1")

	got := lt.members("t1")
	want := []Member{
		{Name: "frontend", Ref: "ra"},
		{Name: "api-worker", Ref: "rb"},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("members(t1) = %+v, want %+v (ascending by Ref)", got, want)
	}

	old, existed := lt.setInfo("a", Member{Name: "api-v2", Ref: "rb", Status: "busy", PrevName: "api-worker"})
	if !existed {
		t.Fatal("setInfo reported a known agent as new")
	}
	if old.Name != "api-worker" {
		t.Errorf("setInfo returned old info %+v, want the previous name", old)
	}

	got = lt.members("t1")
	want = []Member{
		{Name: "frontend", Ref: "ra"},
		{Name: "api-v2", Ref: "rb", Status: "busy", PrevName: "api-worker"},
	}
	if !slices.Equal(got, want) {
		t.Errorf("members(t1) after setInfo = %+v, want %+v", got, want)
	}

	if info, ok := lt.info("a"); !ok || info.Name != "api-v2" {
		t.Errorf("info(a) = (%+v, %v), want the updated information", info, ok)
	}

	if got := lt.members("no-such-topic"); got != nil {
		t.Errorf("members of an unknown topic = %+v, want nil", got)
	}
}
