package directory

import (
	"sort"
	"time"
	"unicode"
	"unicode/utf8"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/wangzw/peers/internal/directory/directorypb"
)

// SetPublic turns publishing on or off. Making the Broker public issues a new
// card immediately; calling it again while public is a no-op. Making it private
// revokes every live locally issued record and clears the active public topic
// set, whether or not the Broker was public before. It returns ErrClosed after
// Close; a failed file write is not reported because the in-memory directory is
// authoritative and the next change or maintenance pass retries the write.
func (d *Directory) SetPublic(public bool) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return ErrClosed
	}
	now := d.now()
	var vs []*directorypb.RecordVersion
	if public {
		if d.public {
			d.mu.Unlock()
			return nil
		}
		d.public = true
		v, err := d.issueCardLocked(now)
		if err != nil {
			// The interface only reports ErrClosed, so a card that cannot be
			// signed leaves the Broker private instead of returning an error.
			d.public = false
			d.mu.Unlock()
			return nil
		}
		d.reissuePending = false
		vs = append(vs, v)
	} else {
		d.public = false
		d.reissuePending = false
		d.active = make(map[string]string)
		vs = d.revokeOwnLocked(now)
	}
	d.mu.Unlock()

	d.commitOwn(vs)
	return nil
}

// DeclarePublicTopic declares a public topic under the local Broker's name. It
// returns ErrInvalidTopic for an empty name, ErrInvalidDescription when the
// description is longer than 256 bytes, is not valid UTF-8 or contains control
// characters, ErrNotPublic when the Broker is not public, and ErrQuotaExceeded
// when the Broker already holds the maximum number of live declarations.
// Declaring the same name again issues a new version with the given
// description, and a name already declared by the Broker does not count against
// the quota.
func (d *Directory) DeclarePublicTopic(name, description string) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return ErrClosed
	}
	if name == "" {
		d.mu.Unlock()
		return ErrInvalidTopic
	}
	if !validDescription(description) {
		d.mu.Unlock()
		return ErrInvalidDescription
	}
	if !d.public {
		d.mu.Unlock()
		return ErrNotPublic
	}
	now := d.now()
	if d.st.countOwnAnnouncements(now, name) >= d.settings.MaxTopicsPerPublisher() {
		d.mu.Unlock()
		return ErrQuotaExceeded
	}
	v, err := d.issueAnnouncementLocked(name, description, now)
	if err != nil {
		d.mu.Unlock()
		return err
	}
	d.active[name] = description
	d.mu.Unlock()

	d.commitOwn([]*directorypb.RecordVersion{v})
	return nil
}

// RetirePublicTopic withdraws the local Broker's public declaration of name. It
// revokes the live declaration when one is held and does nothing otherwise.
func (d *Directory) RetirePublicTopic(name string) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	delete(d.active, name)
	now := d.now()
	var vs []*directorypb.RecordVersion
	id := recordID{
		publisher: d.st.self,
		kind:      directorypb.RecordKind_RECORD_KIND_TOPIC_ANNOUNCEMENT,
		object:    name,
	}
	if r, ok := d.st.get(id); ok && !r.isRevocation() && !isExpired(r.expires, now) {
		if v, err := d.issueRevocationLocked(id, now); err == nil {
			vs = append(vs, v)
		}
	}
	d.mu.Unlock()

	d.commitOwn(vs)
}

// RetireInactiveTopics revokes every live public topic declaration of the local
// Broker whose name is not in the active set. It exists for declarations loaded
// from disk: after a restart the Broker re-declares in the ways its callers
// choose, and the declarations loaded from earlier runs are withdrawn.
func (d *Directory) RetireInactiveTopics() {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	now := d.now()
	var vs []*directorypb.RecordVersion
	for _, r := range d.st.ownRecords() {
		if !liveRecord(r, now) {
			continue
		}
		if r.body.GetTopicAnnouncement() == nil {
			continue
		}
		if _, active := d.active[r.id.object]; active {
			continue
		}
		v, err := d.issueRevocationLocked(r.id, now)
		if err != nil {
			continue
		}
		vs = append(vs, v)
	}
	d.mu.Unlock()

	d.commitOwn(vs)
}

// Reissue signs and publishes a fresh card with the current alias and
// advertised addresses. It does nothing while the Broker is private. A signing
// failure leaves a pending marker that the maintenance loop retries.
func (d *Directory) Reissue() {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	if !d.public {
		d.mu.Unlock()
		return
	}
	d.reissuePending = true
	var vs []*directorypb.RecordVersion
	if v, err := d.issueCardLocked(d.now()); err == nil {
		d.reissuePending = false
		vs = append(vs, v)
	}
	d.mu.Unlock()

	d.commitOwn(vs)
}

// renewDue reissues the local card and every active public topic declaration
// whose renewal is due, retries a card whose Reissue could not be signed, and
// retries revocations whose signing failed earlier. Nothing is renewed while
// the Broker is private, but pending revocations are retried in either state
// until signing succeeds. The maintenance loop calls it once per wakeup.
func (d *Directory) renewDue(now time.Time) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	var vs []*directorypb.RecordVersion

	for _, r := range d.st.ownRecords() {
		if !r.revokePending || r.isRevocation() || isExpired(r.expires, now) {
			continue
		}
		if v, err := d.issueRevocationLocked(r.id, now); err == nil {
			vs = append(vs, v)
		}
	}

	if d.public {
		cardID := recordID{
			publisher: d.st.self,
			kind:      directorypb.RecordKind_RECORD_KIND_BROKER_CARD,
		}
		r, ok := d.st.get(cardID)
		due := ok && !r.isRevocation() && !isExpired(r.expires, now) && renewalDue(r.expires, now)
		if d.reissuePending || due {
			if v, err := d.issueCardLocked(now); err == nil {
				d.reissuePending = false
				vs = append(vs, v)
			}
		}

		names := make([]string, 0, len(d.active))
		for name := range d.active {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			id := recordID{
				publisher: d.st.self,
				kind:      directorypb.RecordKind_RECORD_KIND_TOPIC_ANNOUNCEMENT,
				object:    name,
			}
			r, ok := d.st.get(id)
			if !ok || r.isRevocation() || isExpired(r.expires, now) || !renewalDue(r.expires, now) {
				continue
			}
			if v, err := d.issueAnnouncementLocked(name, d.active[name], now); err == nil {
				vs = append(vs, v)
			}
		}
	}
	d.mu.Unlock()

	// A renewal is a local issuance and writes at once. When nothing was
	// renewed, a dirty flag left by records accepted from a link belongs to the
	// maintenance throttle, so this must not write it every pass.
	if len(vs) > 0 {
		d.persistIfDirty()
	}
	d.broadcast(vs, Fingerprint{})
}

// startupCheck applies the configured public flag at start: a public Broker
// signs a fresh card, and a private one revokes every live locally issued
// record. Start calls it once; the public flag changes only through SetPublic
// afterwards.
func (d *Directory) startupCheck(now time.Time) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	d.public = d.settings.Public()
	d.reissuePending = false
	var vs []*directorypb.RecordVersion
	if d.public {
		if v, err := d.issueCardLocked(now); err == nil {
			vs = append(vs, v)
		}
	} else {
		vs = d.revokeOwnLocked(now)
	}
	d.mu.Unlock()

	d.commitOwn(vs)
}

// reconcileOwn corrects records published by the local Broker that came back
// from the network, for example after a backup restore or a clock rollback.
// Accepting them has already raised the highest issued version; this signs
// replacements or revocations so the network converges on the local state.
func (d *Directory) reconcileOwn(ids []recordID) {
	if len(ids) == 0 {
		return
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	now := d.now()
	var vs []*directorypb.RecordVersion
	for _, id := range ids {
		r, ok := d.st.get(id)
		if !ok {
			continue
		}
		switch {
		case r.isRevocation():
			switch id.kind {
			case directorypb.RecordKind_RECORD_KIND_BROKER_CARD:
				if d.public {
					if v, err := d.issueCardLocked(now); err == nil {
						vs = append(vs, v)
					}
				}
			case directorypb.RecordKind_RECORD_KIND_TOPIC_ANNOUNCEMENT:
				if description, active := d.active[id.object]; active && d.public {
					if v, err := d.issueAnnouncementLocked(id.object, description, now); err == nil {
						vs = append(vs, v)
					}
				}
			}
		case r.body.GetBrokerCard() != nil:
			if d.public {
				if v, err := d.issueCardLocked(now); err == nil {
					vs = append(vs, v)
				}
			} else if v, err := d.issueRevocationLocked(id, now); err == nil {
				vs = append(vs, v)
			}
		case r.body.GetTopicAnnouncement() != nil:
			if description, active := d.active[id.object]; active && d.public {
				if v, err := d.issueAnnouncementLocked(id.object, description, now); err == nil {
					vs = append(vs, v)
				}
			} else if v, err := d.issueRevocationLocked(id, now); err == nil {
				vs = append(vs, v)
			}
		}
	}
	d.mu.Unlock()

	d.commitOwn(vs)
}

// validDescription reports whether s is acceptable as a public topic
// description: at most 256 bytes, valid UTF-8 and free of control characters.
// The empty string is valid.
func validDescription(s string) bool {
	if len(s) > 256 {
		return false
	}
	if !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// revokeOwnLocked revokes every live locally issued record and returns the
// versions of the revocations. The caller must hold the write lock and
// broadcast the returned versions itself.
func (d *Directory) revokeOwnLocked(now time.Time) []*directorypb.RecordVersion {
	var vs []*directorypb.RecordVersion
	for _, r := range d.st.ownRecords() {
		if isExpired(r.expires, now) || r.isRevocation() {
			continue
		}
		if v, err := d.issueRevocationLocked(r.id, now); err == nil {
			vs = append(vs, v)
		}
	}
	return vs
}

// issueCardLocked signs a new card from the current alias and advertised
// addresses and stores it. The caller must hold the write lock and broadcast
// the returned version itself.
func (d *Directory) issueCardLocked(now time.Time) (*directorypb.RecordVersion, error) {
	addrs := d.settings.Advertised()
	if len(addrs) > 8 {
		addrs = addrs[:8]
	}
	cardAddrs := make([]*directorypb.AdvertisedAddress, 0, len(addrs))
	for _, a := range addrs {
		cardAddrs = append(cardAddrs, &directorypb.AdvertisedAddress{Kind: a.Kind, Addr: a.Addr})
	}
	return d.signOwnLocked(&directorypb.RecordBody{
		Publisher: d.publisherBytes(),
		PublicKey: d.id.PublicKey(),
		Kind:      directorypb.RecordKind_RECORD_KIND_BROKER_CARD,
		Version:   nextVersion(now, d.st.highestIssued),
		Expires:   timestamppb.New(now.Add(recordTTL)),
		Payload: &directorypb.RecordBody_BrokerCard{BrokerCard: &directorypb.BrokerCard{
			Alias:     d.settings.Alias(),
			Addresses: cardAddrs,
		}},
	})
}

// issueAnnouncementLocked signs a new public topic declaration and stores it.
// The caller must hold the write lock and broadcast the returned version itself.
func (d *Directory) issueAnnouncementLocked(name, description string, now time.Time) (*directorypb.RecordVersion, error) {
	return d.signOwnLocked(&directorypb.RecordBody{
		Publisher: d.publisherBytes(),
		PublicKey: d.id.PublicKey(),
		Kind:      directorypb.RecordKind_RECORD_KIND_TOPIC_ANNOUNCEMENT,
		Object:    name,
		Version:   nextVersion(now, d.st.highestIssued),
		Expires:   timestamppb.New(now.Add(recordTTL)),
		Payload: &directorypb.RecordBody_TopicAnnouncement{TopicAnnouncement: &directorypb.TopicAnnouncement{
			Name:        name,
			Description: description,
		}},
	})
}

// issueRevocationLocked signs a revocation of id and stores it. The caller must
// hold the write lock and broadcast the returned version itself. The revocation
// expires with the record it revokes when that is later than a fresh ttl, so a
// clock rollback cannot make the revocation expire first and let the old record
// come back. A signing failure marks the record for the maintenance loop to
// retry.
func (d *Directory) issueRevocationLocked(id recordID, now time.Time) (*directorypb.RecordVersion, error) {
	expires := now.Add(recordTTL)
	if r, ok := d.st.get(id); ok && r.expires.After(expires) {
		expires = r.expires
	}
	v, err := d.signOwnLocked(&directorypb.RecordBody{
		Publisher: d.publisherBytes(),
		PublicKey: d.id.PublicKey(),
		Kind:      id.kind,
		Object:    id.object,
		Version:   nextVersion(now, d.st.highestIssued),
		Expires:   timestamppb.New(expires),
		Payload: &directorypb.RecordBody_Revocation{Revocation: &directorypb.Revocation{
			RevokedKind: id.kind,
		}},
	})
	if err != nil {
		if r, ok := d.st.get(id); ok {
			r.revokePending = true
		}
		return nil, err
	}
	return v, nil
}

// signOwnLocked signs body, stores the result as a locally issued record and
// returns its RecordVersion. A signing failure is logged as a warning and
// leaves the directory unchanged. The caller must hold the write lock.
func (d *Directory) signOwnLocked(body *directorypb.RecordBody) (*directorypb.RecordVersion, error) {
	r, err := signRecord(body, d.id.Sign)
	if err != nil {
		d.log.Warn("sign directory record", "kind", body.GetKind(), "object", body.GetObject(), "error", err)
		return nil, err
	}
	d.st.putOwn(r)
	d.dirty = true
	return r.recordVersion(), nil
}

// publisherBytes returns a copy of the local fingerprint for a record body.
func (d *Directory) publisherBytes() []byte {
	return append([]byte(nil), d.st.self[:]...)
}
