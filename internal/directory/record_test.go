package directory

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/directory/directorypb"
	"github.com/wangzw/peers/internal/pairing"
)

const (
	recKindNone  = directorypb.RecordKind_RECORD_KIND_UNSPECIFIED
	recKindCard  = directorypb.RecordKind_RECORD_KIND_BROKER_CARD
	recKindTopic = directorypb.RecordKind_RECORD_KIND_TOPIC_ANNOUNCEMENT
)

func recTestExpiry() time.Time {
	return time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
}

func recTestKey(seed byte) (ed25519.PublicKey, ed25519.PrivateKey) {
	priv := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{seed}, ed25519.SeedSize))
	return priv.Public().(ed25519.PublicKey), priv
}

func recTestBody(pub []byte, kind directorypb.RecordKind, object string, version uint64, expires time.Time, payload any) *directorypb.RecordBody {
	fp := pairing.FingerprintOf(pub)
	body := &directorypb.RecordBody{
		Publisher: fp[:],
		PublicKey: pub,
		Kind:      kind,
		Object:    object,
		Version:   version,
		Expires:   timestamppb.New(expires),
	}
	switch p := payload.(type) {
	case *directorypb.BrokerCard:
		body.Payload = &directorypb.RecordBody_BrokerCard{BrokerCard: p}
	case *directorypb.TopicAnnouncement:
		body.Payload = &directorypb.RecordBody_TopicAnnouncement{TopicAnnouncement: p}
	case *directorypb.Revocation:
		body.Payload = &directorypb.RecordBody_Revocation{Revocation: p}
	}
	return body
}

func recTestMarshal(t *testing.T, body *directorypb.RecordBody) []byte {
	t.Helper()
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(body)
	if err != nil {
		t.Fatalf("marshal record body: %v", err)
	}
	return raw
}

func recTestWire(t *testing.T, body *directorypb.RecordBody, priv ed25519.PrivateKey) *directorypb.Record {
	t.Helper()
	raw := recTestMarshal(t, body)
	return &directorypb.Record{Raw: raw, Signature: ed25519.Sign(priv, raw)}
}

func recTestCard(alias string, addrs ...*directorypb.AdvertisedAddress) *directorypb.BrokerCard {
	return &directorypb.BrokerCard{Alias: alias, Addresses: addrs}
}

func TestSignParseRoundTrip(t *testing.T) {
	expires := recTestExpiry()
	pub, priv := recTestKey(1)
	wantPublisher := pairing.FingerprintOf(pub)
	sign := func(data []byte) []byte { return ed25519.Sign(priv, data) }

	cases := []struct {
		name    string
		body    *directorypb.RecordBody
		wantID  recordID
		payload func(*testing.T, *storedRecord)
	}{
		{
			name: "broker card",
			body: recTestBody(pub, recKindCard, "", 42, expires, recTestCard("hostA",
				&directorypb.AdvertisedAddress{Kind: "direct", Addr: "198.51.100.1:17443"},
				&directorypb.AdvertisedAddress{Kind: "direct", Addr: "198.51.100.2:17443"},
			)),
			wantID: recordID{publisher: wantPublisher, kind: recKindCard, object: ""},
			payload: func(t *testing.T, rec *storedRecord) {
				card := rec.body.GetBrokerCard()
				if card == nil {
					t.Fatalf("parsed record has no broker card payload")
				}
				if card.GetAlias() != "hostA" {
					t.Errorf("alias = %q, want hostA", card.GetAlias())
				}
				if n := len(card.GetAddresses()); n != 2 {
					t.Errorf("address count = %d, want 2", n)
				}
			},
		},
		{
			name:   "topic announcement",
			body:   recTestBody(pub, recKindTopic, "news", 42, expires, &directorypb.TopicAnnouncement{Name: "news", Description: "updates"}),
			wantID: recordID{publisher: wantPublisher, kind: recKindTopic, object: "news"},
			payload: func(t *testing.T, rec *storedRecord) {
				ann := rec.body.GetTopicAnnouncement()
				if ann == nil || ann.GetName() != "news" || ann.GetDescription() != "updates" {
					t.Fatalf("parsed announcement = %v, want name news and description updates", ann)
				}
			},
		},
		{
			name:   "revocation",
			body:   recTestBody(pub, recKindTopic, "news", 42, expires, &directorypb.Revocation{RevokedKind: recKindTopic}),
			wantID: recordID{publisher: wantPublisher, kind: recKindTopic, object: "news"},
			payload: func(t *testing.T, rec *storedRecord) {
				if !rec.isRevocation() {
					t.Fatalf("parsed revocation does not report itself as a revocation")
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec, err := signRecord(tc.body, sign)
			if err != nil {
				t.Fatalf("signRecord: %v", err)
			}
			if rec.id != tc.wantID {
				t.Errorf("id = %+v, want %+v", rec.id, tc.wantID)
			}
			if rec.version != 42 {
				t.Errorf("version = %d, want 42", rec.version)
			}
			if !rec.expires.Equal(expires) {
				t.Errorf("expires = %v, want %v", rec.expires, expires)
			}
			wantRaw := recTestMarshal(t, tc.body)
			if !bytes.Equal(rec.raw, wantRaw) {
				t.Errorf("raw does not match the deterministic encoding of the body")
			}
			if !bytes.Equal(rec.sig, ed25519.Sign(priv, wantRaw)) {
				t.Errorf("signature does not cover raw")
			}
			rv := rec.recordVersion()
			if rv.GetVersion() != 42 || !bytes.Equal(rv.GetId().GetPublisher(), wantPublisher[:]) ||
				rv.GetId().GetKind() != tc.wantID.kind || rv.GetId().GetObject() != tc.wantID.object {
				t.Errorf("recordVersion = %v, want id %+v version 42", rv, tc.wantID)
			}
			if wire := rec.wire(); !bytes.Equal(wire.GetRaw(), rec.raw) || !bytes.Equal(wire.GetSignature(), rec.sig) {
				t.Errorf("wire form does not carry raw and signature unchanged")
			}
			tc.payload(t, rec)
		})
	}
}

// REQ-broker-discovery-006: a card whose bytes changed after signing is
// rejected.
func TestParseRejectsTamperedRaw(t *testing.T) {
	pub, priv := recTestKey(2)
	body := recTestBody(pub, recKindCard, "", 1, recTestExpiry(), recTestCard("hostA",
		&directorypb.AdvertisedAddress{Kind: "direct", Addr: "198.51.100.1:17443"}))
	wire := recTestWire(t, body, priv)

	i := bytes.Index(wire.Raw, []byte("198.51.100.1:17443"))
	if i < 0 {
		t.Fatalf("address not found in raw record")
	}
	wire.Raw[i] ^= 0x01

	if _, err := parseRecord(wire); !errors.Is(err, errBadSignature) {
		t.Fatalf("parseRecord(tampered raw) error = %v, want errBadSignature", err)
	}
}

// REQ-broker-discovery-006: a record that claims another publisher's
// fingerprint is rejected.
func TestParseRejectsForeignPublisher(t *testing.T) {
	pubA, _ := recTestKey(3)
	pubB, privB := recTestKey(4)
	card := recTestCard("hostA", &directorypb.AdvertisedAddress{Kind: "direct", Addr: "198.51.100.1:17443"})

	t.Run("publisher is not the key holder", func(t *testing.T) {
		body := recTestBody(pubB, recKindCard, "", 1, recTestExpiry(), card)
		fpA := pairing.FingerprintOf(pubA)
		body.Publisher = fpA[:]
		wire := recTestWire(t, body, privB)

		if _, err := parseRecord(wire); !errors.Is(err, errBadSignature) {
			t.Fatalf("parseRecord(foreign publisher) error = %v, want errBadSignature", err)
		}
	})

	t.Run("signature made by another key", func(t *testing.T) {
		body := recTestBody(pubA, recKindCard, "", 1, recTestExpiry(), card)
		wire := recTestWire(t, body, privB)

		if _, err := parseRecord(wire); !errors.Is(err, errBadSignature) {
			t.Fatalf("parseRecord(foreign signature) error = %v, want errBadSignature", err)
		}
	})
}

func TestParseRejectsBadCardAlias(t *testing.T) {
	// REQ-broker-pairing-037: a card whose alias breaks the alias rules is
	// dropped as malformed.
	pub, priv := recTestKey(5)

	cases := []struct {
		name  string
		alias string
		ok    bool
	}{
		{"25 bytes", strings.Repeat("a", 25), false},
		{"empty", "", false},
		{"space", "host A", false},
		{"at sign", "host@A", false},
		{"24 bytes", strings.Repeat("a", 24), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := recTestBody(pub, recKindCard, "", 1, recTestExpiry(), recTestCard(tc.alias))
			wire := recTestWire(t, body, priv)
			_, err := parseRecord(wire)
			if tc.ok {
				if err != nil {
					t.Fatalf("parseRecord(alias %q) error = %v, want nil", tc.alias, err)
				}
				return
			}
			if !errors.Is(err, errMalformed) {
				t.Fatalf("parseRecord(alias %q) error = %v, want errMalformed", tc.alias, err)
			}
		})
	}
}

func TestParseStructuralRules(t *testing.T) {
	pub, priv := recTestKey(6)
	expires := recTestExpiry()
	card := recTestCard("hostA", &directorypb.AdvertisedAddress{Kind: "direct", Addr: "198.51.100.1:17443"})

	cases := []struct {
		name  string
		build func() *directorypb.RecordBody
		ok    bool
	}{
		{
			name: "unspecified kind",
			build: func() *directorypb.RecordBody {
				return recTestBody(pub, recKindNone, "", 1, expires, card)
			},
		},
		{
			name: "card payload with topic kind",
			build: func() *directorypb.RecordBody {
				return recTestBody(pub, recKindTopic, "", 1, expires, card)
			},
		},
		{
			// An announcement payload paired with the card kind must not pass
			// structural validation even when object and name agree.
			name: "announcement payload with card kind",
			build: func() *directorypb.RecordBody {
				return recTestBody(pub, recKindCard, "news", 1, expires, &directorypb.TopicAnnouncement{Name: "news"})
			},
		},
		{
			name: "card with object",
			build: func() *directorypb.RecordBody {
				return recTestBody(pub, recKindCard, "something", 1, expires, card)
			},
		},
		{
			name: "announcement object differs from name",
			build: func() *directorypb.RecordBody {
				return recTestBody(pub, recKindTopic, "other", 1, expires, &directorypb.TopicAnnouncement{Name: "news"})
			},
		},
		{
			name: "announcement with empty name",
			build: func() *directorypb.RecordBody {
				return recTestBody(pub, recKindTopic, "", 1, expires, &directorypb.TopicAnnouncement{})
			},
		},
		{
			name: "revocation kind differs",
			build: func() *directorypb.RecordBody {
				return recTestBody(pub, recKindTopic, "news", 1, expires, &directorypb.Revocation{RevokedKind: recKindCard})
			},
		},
		{
			name: "missing expiry",
			build: func() *directorypb.RecordBody {
				body := recTestBody(pub, recKindCard, "", 1, expires, card)
				body.Expires = nil
				return body
			},
		},
		{
			name: "unknown kind without payload",
			build: func() *directorypb.RecordBody {
				return recTestBody(pub, directorypb.RecordKind(7), "", 1, expires, nil)
			},
			ok: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wire := recTestWire(t, tc.build(), priv)
			rec, err := parseRecord(wire)
			if tc.ok {
				if err != nil {
					t.Fatalf("parseRecord: %v", err)
				}
				if rec.id.kind != directorypb.RecordKind(7) || rec.isRevocation() {
					t.Errorf("unknown record parsed as kind %d revocation %v", rec.id.kind, rec.isRevocation())
				}
				return
			}
			if !errors.Is(err, errMalformed) {
				t.Fatalf("parseRecord error = %v, want errMalformed", err)
			}
		})
	}
}

func TestParseKeepsUnknownFields(t *testing.T) {
	pub, priv := recTestKey(7)
	body := recTestBody(pub, recKindCard, "", 3, recTestExpiry(), recTestCard("hostA",
		&directorypb.AdvertisedAddress{Kind: "direct", Addr: "198.51.100.1:17443"}))

	raw := recTestMarshal(t, body)
	raw = protowire.AppendTag(raw, 100, protowire.VarintType)
	raw = protowire.AppendVarint(raw, 1)
	wire := &directorypb.Record{Raw: raw, Signature: ed25519.Sign(priv, raw)}

	rec, err := parseRecord(wire)
	if err != nil {
		t.Fatalf("parseRecord with an unknown field: %v", err)
	}
	if !bytes.Equal(rec.raw, raw) {
		t.Fatalf("stored raw differs from the input bytes")
	}
}

func TestKnownAddrsSkipsUnknownKinds(t *testing.T) {
	card := &directorypb.BrokerCard{Addresses: []*directorypb.AdvertisedAddress{
		{Kind: "relay", Addr: "relay.example:17443"},
		{Kind: "direct", Addr: "198.51.100.4:17443"},
		{Kind: "direct", Addr: "198.51.100.5:17443"},
	}}

	got := knownAddrs(card, 8)
	want := []config.AdvertisedAddr{
		{Kind: "direct", Addr: "198.51.100.4:17443"},
		{Kind: "direct", Addr: "198.51.100.5:17443"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("knownAddrs = %v, want %v", got, want)
	}

	if got := knownAddrs(card, 1); !reflect.DeepEqual(got, want[:1]) {
		t.Errorf("knownAddrs with max 1 = %v, want %v", got, want[:1])
	}

	if got := knownAddrs(nil, 8); got != nil {
		t.Errorf("knownAddrs(nil) = %v, want nil", got)
	}
}

func TestParseCopiesInput(t *testing.T) {
	pub, priv := recTestKey(8)
	body := recTestBody(pub, recKindCard, "", 1, recTestExpiry(), recTestCard("hostA",
		&directorypb.AdvertisedAddress{Kind: "direct", Addr: "198.51.100.1:17443"}))
	wire := recTestWire(t, body, priv)

	rec, err := parseRecord(wire)
	if err != nil {
		t.Fatalf("parseRecord: %v", err)
	}
	rawBefore := append([]byte(nil), rec.raw...)
	sigBefore := append([]byte(nil), rec.sig...)

	for i := range wire.Raw {
		wire.Raw[i] = 0
	}
	for i := range wire.Signature {
		wire.Signature[i] = 0
	}

	if !bytes.Equal(rec.raw, rawBefore) {
		t.Errorf("raw changed after the input slice was modified")
	}
	if !bytes.Equal(rec.sig, sigBefore) {
		t.Errorf("signature changed after the input slice was modified")
	}
}

// TestIDFromProtoRejectsOversizedObject checks that a peer-chosen identifier
// object is bounded before it enters the pending and requested sets.
func TestIDFromProtoRejectsOversizedObject(t *testing.T) {
	pub := make([]byte, 32)
	tooLong := strings.Repeat("x", maxRecordObjectLen+1)
	if _, ok := idFromProto(&directorypb.RecordId{Publisher: pub, Object: tooLong}); ok {
		t.Fatalf("idFromProto accepted an object of %d bytes, want at most %d", len(tooLong), maxRecordObjectLen)
	}
	atLimit := strings.Repeat("x", maxRecordObjectLen)
	if _, ok := idFromProto(&directorypb.RecordId{Publisher: pub, Object: atLimit}); !ok {
		t.Fatalf("idFromProto rejected an object of exactly %d bytes", maxRecordObjectLen)
	}
}
