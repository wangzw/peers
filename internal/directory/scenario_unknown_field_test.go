package directory

import (
	"bytes"
	"crypto/ed25519"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/wangzw/peers/internal/directory/directorypb"
)

func scenarioUnknownFieldCard(t *testing.T, seed byte) *storedRecord {
	t.Helper()
	pub, priv := recTestKey(seed)
	body := recTestBody(pub, recKindCard, "", 1, dirTestNow().Add(recordTTL), recTestCard("hostX",
		&directorypb.AdvertisedAddress{Kind: "direct", Addr: "198.51.100.4:17443"}))
	raw := recTestMarshal(t, body)
	raw = protowire.AppendTag(raw, 100, protowire.VarintType)
	raw = protowire.AppendVarint(raw, 1)

	rec, err := parseRecord(&directorypb.Record{Raw: raw, Signature: ed25519.Sign(priv, raw)})
	if err != nil {
		t.Fatalf("parse a signed card with an unknown field: %v", err)
	}
	return rec
}

// TestScenarioForwardsUnknownFieldsByteForByte checks that forwarding preserves a
// card's raw bytes, including an unknown field, byte for byte.
func TestScenarioForwardsUnknownFieldsByteForByte(t *testing.T) {
	net := newFakeNet()
	clock := newFakeClock(dirTestNow())
	p := newScenarioNode(t, net, clock, 1)
	b := newScenarioNode(t, net, clock, 2)
	c := newScenarioNode(t, net, clock, 3)

	rec := scenarioUnknownFieldCard(t, 200)
	scenarioInject(t, p, rec)

	bringUp(p.d)
	bringUp(b.d)
	bringUp(c.d)

	net.Connect(p.fp, b.fp)
	net.Connect(b.fp, c.fp)

	scenarioWait(t, net, []*scenarioNode{p, b, c}, "the unknown-field card to reach C", func() bool {
		_, ok := scenarioHeld(c.d, rec.id)
		return ok
	})
	held, ok := scenarioHeld(c.d, rec.id)
	if !ok {
		t.Fatal("C does not hold the card")
	}
	if !bytes.Equal(held.raw, rec.raw) {
		t.Fatalf("C's raw bytes differ from the published ones:\n got %x\nwant %x", held.raw, rec.raw)
	}
}
