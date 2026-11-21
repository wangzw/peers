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

// Errors returned by record signing and parsing. They are matched with
// errors.Is.
var (
	// errBadSignature means the record is unsigned, its signature does not
	// verify against the public key in its body, or the publisher is not the
	// holder of that key.
	errBadSignature = errors.New("bad signature")
	// errMalformed means the record's encoding or structure is invalid.
	errMalformed = errors.New("malformed record")
	// errRecordTooLarge is a malformed record whose raw bytes exceed
	// maxRecordRaw. It wraps errMalformed.
	errRecordTooLarge = fmt.Errorf("%w: raw exceeds %d bytes", errMalformed, maxRecordRaw)
)

// maxRecordRaw is the largest accepted encoded record body. The receiving path
// and the local signing path both enforce it, so no record larger than this can
// enter the directory or be issued, and a reconciliation reply stays bounded.
const maxRecordRaw = 64 << 10

// maxRecordObjectLen is the largest object name accepted in a record
// identifier. A public topic name is the only non-empty object this version
// issues, but a malformed one must still be accepted and forwarded, so the
// limit is generous. It keeps a peer-chosen RecordList from carrying unbounded
// object strings into the pending and requested sets.
const maxRecordObjectLen = 4 << 10

// signRecord marshals body deterministically, signs it and returns the stored
// form. The result is checked with parseRecord so that the local Broker never
// signs a record it would itself reject.
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

// parseRecord verifies and parses a record received from a link or loaded from
// disk. The raw bytes and the signature are copied into the result, so the
// caller may reuse its slices. The source field is left for the caller to fill.
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
		// A record whose payload this version does not know is still verified,
		// stored and forwarded so that newer records keep propagating.
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

// validCardAlias reports whether s is 1 to config.AliasMaxLen bytes long and
// contains only A-Z, a-z, 0-9, '.', '-' and '_'.
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

// knownAddrs returns the addresses of card whose kind is "direct", in order,
// at most max of them. Address kinds this version does not know and "direct"
// entries that are not well-formed (see validDirectAddr) are skipped; the card
// itself is still accepted and forwarded with them.
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

// recordVersion converts the record to its RecordList entry.
func (r *storedRecord) recordVersion() *directorypb.RecordVersion {
	return &directorypb.RecordVersion{Id: r.id.toProto(), Version: r.version}
}

// wire returns the record in its wire form, keeping raw and signature exactly
// as they were signed.
func (r *storedRecord) wire() *directorypb.Record {
	return &directorypb.Record{Raw: r.raw, Signature: r.sig}
}

// isRevocation reports whether the record's payload is a revocation.
func (r *storedRecord) isRevocation() bool {
	if r == nil || r.body == nil {
		return false
	}
	_, ok := r.body.Payload.(*directorypb.RecordBody_Revocation)
	return ok
}

// idFromProto converts a wire record identifier. It reports false when the
// publisher is not a 32-byte fingerprint or the object exceeds
// maxRecordObjectLen, so a peer cannot push unbounded identifier strings into
// the pending and requested sets.
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

// toProto converts the identifier to its wire form.
func (id recordID) toProto() *directorypb.RecordId {
	return &directorypb.RecordId{
		Publisher: id.publisher[:],
		Kind:      id.kind,
		Object:    id.object,
	}
}
