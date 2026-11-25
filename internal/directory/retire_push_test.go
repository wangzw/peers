package directory

import (
	"testing"

	"github.com/wangzw/peers/internal/directory/directorypb"
)

// TestRetirePublicTopicPushesRevocationImmediately checks that retiring a declaration broadcasts the revocation at once.
func TestRetirePublicTopicPushesRevocationImmediately(t *testing.T) {
	net := newFakeNet()
	clock := newFakeClock(dirTestNow())
	a := newScenarioNode(t, net, clock, 1)
	b := newScenarioNode(t, net, clock, 2)
	a.set.SetAlias("hostA")
	a.set.SetPublic(true)
	for _, n := range []*scenarioNode{a, b} {
		bringUp(n.d)
	}
	net.Connect(a.fp, b.fp)
	nodes := []*scenarioNode{a, b}
	scenarioWait(t, net, nodes, "the links to settle", func() bool { return true })

	if err := a.d.DeclarePublicTopic("infra-help", "ask here"); err != nil {
		t.Fatalf("DeclarePublicTopic: %v", err)
	}
	scenarioWait(t, net, nodes, "B to list infra-help", func() bool {
		return scenarioTopicFound(b.d, "infra-help")
	})

	id := scenarioTopicID(a.fp, "infra-help")
	announcement, ok := scenarioHeld(a.d, id)
	if !ok || announcement.isRevocation() {
		t.Fatalf("A holds no live announcement for %v before retiring it", id)
	}

	before := len(a.ep.Sent())
	a.d.RetirePublicTopic("infra-help")

	revocation, ok := scenarioHeld(a.d, id)
	if !ok || !revocation.isRevocation() {
		t.Fatalf("A holds no revocation for %v after retiring it", id)
	}
	if got := revocation.body.GetRevocation().GetRevokedKind(); got != directorypb.RecordKind_RECORD_KIND_TOPIC_ANNOUNCEMENT {
		t.Fatalf("revocation for %v revokes kind %v, want the topic announcement", id, got)
	}
	if revocation.version <= announcement.version {
		t.Fatalf("revocation version %d is not above the announcement's %d", revocation.version, announcement.version)
	}

	var pushed []*directorypb.RecordVersion
	for _, f := range a.ep.Sent()[before:] {
		if f.to != b.fp || f.kind != directorypb.KindRecordList {
			continue
		}
		list := decodeRecordList(t, f.payload)
		if len(list.GetRecords()) != 1 {
			continue
		}
		got, ok := idFromProto(list.GetRecords()[0].GetId())
		if ok && got == id {
			pushed = append(pushed, list.GetRecords()[0])
		}
	}
	if len(pushed) != 1 {
		t.Fatalf("A pushed %d incremental RecordLists holding %v to B after retiring it, want exactly 1", len(pushed), id)
	}
	if got := pushed[0].GetVersion(); got != revocation.version {
		t.Fatalf("A pushed version %d for %v, want the revocation's version %d", got, id, revocation.version)
	}
}
