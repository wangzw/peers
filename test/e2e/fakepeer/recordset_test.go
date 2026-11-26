//go:build e2e

package main

import (
	"bytes"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/directory/directorypb"
	"github.com/wangzw/peers/test/e2e/fakepeer/fakerec"
)

var testNow = time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)

func buildRecord(t *testing.T, spec fakerec.Spec) *directorypb.Record {
	t.Helper()
	rec, err := fakerec.Build(spec, testNow)
	if err != nil {
		t.Fatalf("build record: %v", err)
	}
	return rec
}

func recordBody(t *testing.T, rec *directorypb.Record) *directorypb.RecordBody {
	t.Helper()
	body := new(directorypb.RecordBody)
	if err := proto.Unmarshal(rec.GetRaw(), body); err != nil {
		t.Fatalf("unmarshal record body: %v", err)
	}
	return body
}

// TestRecordSetList checks that list reports every parsable record's identifier
// and version.
func TestRecordSetList(t *testing.T) {
	card := buildRecord(t, fakerec.Spec{
		Seed:      "card-seed",
		Kind:      "card",
		Alias:     "peer",
		Addresses: []fakerec.Addr{{Kind: "direct", Addr: "127.0.0.1:17443"}},
		Version:   3,
		ExpiresIn: 24 * time.Hour,
	})
	topic := buildRecord(t, fakerec.Spec{
		Seed:        "topic-seed",
		Kind:        "topic",
		Topic:       "infra-help",
		Description: "ask infra",
		Version:     7,
		ExpiresIn:   24 * time.Hour,
	})
	broken := &directorypb.Record{Raw: []byte{0xff, 0x00}}

	set := &recordSet{}
	set.set([]*directorypb.Record{card, broken, topic})

	list := set.list()
	if got := len(list.GetRecords()); got != 2 {
		t.Fatalf("list has %d records, want 2", got)
	}

	cardFP := fakerec.PublisherOf("card-seed")
	topicFP := fakerec.PublisherOf("topic-seed")
	want := []struct {
		publisher []byte
		kind      directorypb.RecordKind
		object    string
		version   uint64
	}{
		{cardFP[:], directorypb.RecordKind_RECORD_KIND_BROKER_CARD, "", 3},
		{topicFP[:], directorypb.RecordKind_RECORD_KIND_TOPIC_ANNOUNCEMENT, "infra-help", 7},
	}
	for i, w := range want {
		got := list.GetRecords()[i]
		if !bytes.Equal(got.GetId().GetPublisher(), w.publisher) {
			t.Errorf("record %d publisher = %x, want %x", i, got.GetId().GetPublisher(), w.publisher)
		}
		if got.GetId().GetKind() != w.kind {
			t.Errorf("record %d kind = %v, want %v", i, got.GetId().GetKind(), w.kind)
		}
		if got.GetId().GetObject() != w.object {
			t.Errorf("record %d object = %q, want %q", i, got.GetId().GetObject(), w.object)
		}
		if got.GetVersion() != w.version {
			t.Errorf("record %d version = %d, want %d", i, got.GetVersion(), w.version)
		}
	}
}

// TestRecordSetAnswer checks that answer returns the requested record with its
// raw bytes and signature unchanged.
func TestRecordSetAnswer(t *testing.T) {
	card := buildRecord(t, fakerec.Spec{
		Seed:      "card-seed",
		Kind:      "card",
		Alias:     "peer",
		Addresses: []fakerec.Addr{{Kind: "direct", Addr: "127.0.0.1:17443"}},
		Version:   1,
		ExpiresIn: 24 * time.Hour,
	})
	body := recordBody(t, card)

	set := &recordSet{}
	set.set([]*directorypb.Record{card})

	req := &directorypb.RecordRequest{Records: []*directorypb.RecordId{
		{
			Publisher: append([]byte(nil), body.GetPublisher()...),
			Kind:      body.GetKind(),
			Object:    body.GetObject(),
		},
		{
			Publisher: []byte("no-such-publisher"),
			Kind:      directorypb.RecordKind_RECORD_KIND_BROKER_CARD,
			Object:    "",
		},
	}}

	got := set.answer(req)
	if n := len(got.GetRecords()); n != 1 {
		t.Fatalf("answer has %d records, want 1", n)
	}
	rec := got.GetRecords()[0]
	if !bytes.Equal(rec.GetRaw(), card.GetRaw()) {
		t.Errorf("answer changed the raw bytes")
	}
	if !bytes.Equal(rec.GetSignature(), card.GetSignature()) {
		t.Errorf("answer changed the signature")
	}
}
