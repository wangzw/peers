package broker

import (
	"context"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/pairing"
)

// newAssemblerLink returns a link that is built but never started, which is
// enough for the topic assembler to register its frame handlers on it.
func newAssemblerLink(t *testing.T) *link.Link {
	t.Helper()
	lk, err := link.New(link.Options{
		Peers:    new(pairing.Pairing),
		Settings: link.ConfigSettings(),
		Clock:    link.RealClock(),
	})
	if err != nil {
		t.Fatalf("link.New: %v", err)
	}
	t.Cleanup(lk.Close)
	return lk
}

func TestTopicAssemblerNeedsLink(t *testing.T) {
	deps := &Deps{}
	_, err := (topicAssembler{}).Assemble(context.Background(), nil, deps, &Assembly{})
	if err == nil || !strings.Contains(err.Error(), "pairing and link") {
		t.Fatalf("Assemble without a link = %v, want an error naming the pairing and link assembler", err)
	}
	if deps.Topics != nil {
		t.Fatal("Assemble without a link installed Deps.Topics")
	}
}

func TestTopicAssemblerNeedsPairing(t *testing.T) {
	lk := newAssemblerLink(t)
	cfg := newFakeConfig(t.TempDir())
	deps := &Deps{Config: cfg}

	_, err := (topicAssembler{}).Assemble(context.Background(), cfg, deps, &Assembly{Link: lk})
	if err == nil || !strings.Contains(err.Error(), "pairing and link") {
		t.Fatalf("Assemble without a pairing = %v, want an error naming the pairing and link assembler", err)
	}
	if deps.Topics != nil {
		t.Fatal("Assemble without a pairing installed Deps.Topics")
	}
}

func TestTopicAssemblerFillsTopics(t *testing.T) {
	lk := newAssemblerLink(t)
	cfg := newFakeConfig(t.TempDir())
	deps := &Deps{Config: cfg}
	asm := &Assembly{Link: lk}

	a := topicAssembler{self: link.Fingerprint{1}}
	if _, err := a.Assemble(context.Background(), cfg, deps, asm); err != nil {
		t.Fatalf("Assemble: %v", err)
	}
	if deps.Topics == nil {
		t.Fatal("Assemble left Deps.Topics nil")
	}

	deps.Topics.AgentChanged("ag1", Member{Name: "worker", Ref: "ref1", Status: "idle"})
	if err := deps.Topics.Join("ag1", "t1"); err != nil {
		t.Fatalf("Join through the assembled table: %v", err)
	}
	if got := deps.Topics.TopicsOf("ag1"); len(got) != 1 || got[0] != "t1" {
		t.Fatalf("TopicsOf(ag1) = %v, want [t1]", got)
	}
}

// TestTopicAssemblerSelfIsPairingFingerprint checks that the assembler labels
// its digests with asm.Pairing.Fingerprint() when the test override is unset.
// Building a real pairing identity needs the real configuration, so the test
// reads the fingerprint through the seam production leaves nil, then checks
// through Assemble that a non-zero pairing fingerprint reaches topic.New as
// Options.Self.
func TestTopicAssemblerSelfIsPairingFingerprint(t *testing.T) {
	asm := &Assembly{Pairing: new(pairing.Pairing)}

	// The default path returns exactly the pairing's fingerprint.
	got, err := (topicAssembler{}).selfFingerprint(asm)
	if err != nil {
		t.Fatalf("selfFingerprint: %v", err)
	}
	if want := asm.Pairing.Fingerprint(); got != want {
		t.Fatalf("selfFingerprint = %x, want the pairing fingerprint %x", got, want)
	}

	// A non-zero fingerprint standing in for the pairing's is returned as is
	// and reaches topic.New: the registry panics on a zero Self, so a
	// successful Assemble proves the value was used.
	want := link.Fingerprint{0x5a}
	a := topicAssembler{pairingFingerprint: func(*pairing.Pairing) link.Fingerprint { return want }}
	if resolved, err := a.selfFingerprint(asm); err != nil || resolved != want {
		t.Fatalf("selfFingerprint = %x, %v, want %x", resolved, err, want)
	}

	lk := newAssemblerLink(t)
	cfg := newFakeConfig(t.TempDir())
	deps := &Deps{Config: cfg}
	if _, err := a.Assemble(context.Background(), cfg, deps, &Assembly{Pairing: asm.Pairing, Link: lk}); err != nil {
		t.Fatalf("Assemble with a pairing fingerprint: %v", err)
	}
	if deps.Topics == nil {
		t.Fatal("Assemble left Deps.Topics nil")
	}
}

func TestDefaultAssemblersOrder(t *testing.T) {
	as := DefaultAssemblers()
	if len(as) < 2 {
		t.Fatalf("DefaultAssemblers has %d entries, want at least 2", len(as))
	}
	if got := as[0].Name(); got != "pairing and link" {
		t.Fatalf("first assembler = %q, want %q", got, "pairing and link")
	}
	if got := as[1].Name(); got != "topic" {
		t.Fatalf("second assembler = %q, want %q", got, "topic")
	}
}
