package directory

import (
	"bytes"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/directory/directorypb"
	"github.com/wangzw/peers/internal/pairing"
)

// Errors returned by record signing and parsing, matched with errors.Is.
var (
	errBadSignature   = errors.New("bad signature")
	errMalformed      = errors.New("malformed record")
	errRecordTooLarge = fmt.Errorf("%w: raw exceeds %d bytes", errMalformed, maxRecordRaw)
)

const maxRecordRaw = 64 << 10

const maxRecordObjectLen = 4 << 10

func signRecord(body *directorypb.RecordBody, sign func([]byte) []byte) (*storedRecord, error) {
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal the record body: %w", err)
	}
	if len(raw) > maxRecordRaw {
		return nil, errRecordTooLarge
	}
	sig := sign(raw)
	if len(sig) == 0 {
		return nil, ErrNoIdentity
	}
	rec, err := parseRecord(&directorypb.Record{Raw: raw, Signature: sig})
	if err != nil {
		return nil, err
	}
	return rec, nil
}

func parseRecord(r *directorypb.Record) (*storedRecord, error) {
	raw, sig := r.GetRaw(), r.GetSignature()
	if len(raw) == 0 || len(sig) == 0 {
		return nil, errMalformed
	}
	if len(raw) > maxRecordRaw {
		return nil, errRecordTooLarge
	}

	var body directorypb.RecordBody
	if err := proto.Unmarshal(raw, &body); err != nil {
		return nil, errMalformed
	}

	pub := body.GetPublicKey()
	if len(pub) != 32 {
		return nil, errMalformed
	}
	if !pairing.Verify(pub, raw, sig) {
		return nil, errBadSignature
	}

	publisher := body.GetPublisher()
	if len(publisher) != 32 {
		return nil, errBadSignature
	}
	fp := pairing.FingerprintOf(pub)
	if !bytes.Equal(publisher, fp[:]) {
		return nil, errBadSignature
	}

	kind := body.GetKind()
	if kind == directorypb.RecordKind_RECORD_KIND_UNSPECIFIED {
		return nil, errMalformed
	}
	switch p := body.Payload.(type) {
	case *directorypb.RecordBody_BrokerCard:
		if kind != directorypb.RecordKind_RECORD_KIND_BROKER_CARD ||
			body.GetObject() != "" || !validCardAlias(p.BrokerCard.GetAlias()) {
			return nil, errMalformed
		}
	case *directorypb.RecordBody_TopicAnnouncement:
		name := p.TopicAnnouncement.GetName()
		if kind != directorypb.RecordKind_RECORD_KIND_TOPIC_ANNOUNCEMENT ||
			name == "" || body.GetObject() != name {
			return nil, errMalformed
		}
	case *directorypb.RecordBody_Revocation:
		if p.Revocation.GetRevokedKind() != kind {
			return nil, errMalformed
		}
	case nil:
		// A nil payload is accepted so that a record this version does not
		// understand still propagates.
	default:
		return nil, errMalformed
	}

	if body.GetExpires() == nil {
		return nil, errMalformed
	}

	return &storedRecord{
		id: recordID{
			publisher: fp,
			kind:      kind,
			object:    body.GetObject(),
		},
		raw:     append([]byte(nil), raw...),
		sig:     append([]byte(nil), sig...),
		body:    &body,
		version: body.GetVersion(),
		expires: body.GetExpires().AsTime(),
	}, nil
}

func validCardAlias(s string) bool {
	if len(s) == 0 || len(s) > config.AliasMaxLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case 'A' <= c && c <= 'Z', 'a' <= c && c <= 'z', '0' <= c && c <= '9',
			c == '.', c == '-', c == '_':
		default:
			return false
		}
	}
	return true
}

func knownAddrs(card *directorypb.BrokerCard, max int) []config.AdvertisedAddr {
	if card == nil || max <= 0 {
		return nil
	}
	var addrs []config.AdvertisedAddr
	for _, a := range card.GetAddresses() {
		if a.GetKind() != "direct" || !validDirectAddr(a.GetAddr()) {
			continue
		}
		addrs = append(addrs, config.AdvertisedAddr{Kind: a.GetKind(), Addr: a.GetAddr()})
		if len(addrs) == max {
			break
		}
	}
	return addrs
}

func (r *storedRecord) recordVersion() *directorypb.RecordVersion {
	return &directorypb.RecordVersion{Id: r.id.toProto(), Version: r.version}
}

func (r *storedRecord) wire() *directorypb.Record {
	return &directorypb.Record{Raw: r.raw, Signature: r.sig}
}

func (r *storedRecord) isRevocation() bool {
	if r == nil || r.body == nil {
		return false
	}
	_, ok := r.body.Payload.(*directorypb.RecordBody_Revocation)
	return ok
}

func idFromProto(id *directorypb.RecordId) (recordID, bool) {
	pub := id.GetPublisher()
	if len(pub) != 32 || len(id.GetObject()) > maxRecordObjectLen {
		return recordID{}, false
	}
	var publisher Fingerprint
	copy(publisher[:], pub)
	return recordID{
		publisher: publisher,
		kind:      id.GetKind(),
		object:    id.GetObject(),
	}, true
}

func (id recordID) toProto() *directorypb.RecordId {
	return &directorypb.RecordId{
		Publisher: id.publisher[:],
		Kind:      id.kind,
		Object:    id.object,
	}
}
