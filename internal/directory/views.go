package directory

import (
	"bytes"
	"sort"
	"time"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing"
)

// viewCardAddressMax is the number of advertised addresses a card contributes
// to the views. A received card may carry more; the extra entries are still
// accepted and forwarded but are not listed.
const viewCardAddressMax = 8

// viewCardRecord is an immutable snapshot of one BrokerCard record taken under
// the directory lock.
type viewCardRecord struct {
	publisher Fingerprint
	alias     string
	addresses []config.AdvertisedAddr
}

// viewTopicRecord is one well-formed public topic declaration considered while
// merging the topic list.
type viewTopicRecord struct {
	name        string
	description string
	version     uint64
	publisher   Fingerprint
	isPeer      bool
}

// Brokers returns every paired peer and every non-peer that holds a live card.
// The local Broker never appears. The record snapshot is taken under the read
// lock; the peer table and the link status are queried outside it.
func (d *Directory) Brokers() []KnownBroker {
	now := d.now()
	self := d.viewSelf()
	cards := d.viewCards(now)

	byPublisher := make(map[Fingerprint]viewCardRecord, len(cards))
	for _, c := range cards {
		byPublisher[c.publisher] = c
	}

	peers := d.peers.Peers()
	isPeer := make(map[Fingerprint]bool, len(peers))
	out := make([]KnownBroker, 0, len(peers)+len(cards))

	for _, p := range peers {
		isPeer[p.Fingerprint] = true
		card, hasCard := byPublisher[p.Fingerprint]
		addrs := viewPeerAddresses(p, card, hasCard)
		phase := d.links.Status(p.Fingerprint).Phase
		out = append(out, KnownBroker{
			Fingerprint: p.Fingerprint,
			Alias:       d.peers.EffectiveAlias(p.Fingerprint),
			Addresses:   addrs,
			IsPeer:      true,
			Role:        p.Role,
			Status:      viewStatus(phase, addrs),
			Link:        phase,
			Detail:      "",
		})
	}

	for _, c := range cards {
		if c.publisher == self || isPeer[c.publisher] {
			continue
		}
		addrs := c.addresses
		if addrs == nil {
			addrs = []config.AdvertisedAddr{}
		}
		out = append(out, KnownBroker{
			Fingerprint: c.publisher,
			Alias:       c.alias,
			Addresses:   addrs,
			IsPeer:      false,
			Role:        pairing.RoleUnknown,
			Status:      viewStatus(LinkNone, addrs),
			Link:        LinkNone,
			Detail:      "",
		})
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Alias != out[j].Alias {
			return out[i].Alias < out[j].Alias
		}
		return bytes.Compare(out[i].Fingerprint[:], out[j].Fingerprint[:]) < 0
	})
	return out
}

// PublicTopics returns the live public topic declarations of the whole
// directory, including the local Broker's own. Names are merged into one entry
// each. The description is chosen by publisher tier first: the local Broker's
// declaration, otherwise a peer's, otherwise the declarations of every other
// publisher. Within a tier the highest version wins, and a tie goes to the
// smaller publisher fingerprint. A publisher chooses its own version, so the
// tiering keeps a remote publisher from replacing the local or a peer
// declaration with a large version. A declaration whose name is not 1 to 256
// bytes of valid UTF-8 free of control characters, or whose description is not
// acceptable to validDescription, is left out; the record is still accepted and
// forwarded. The result is ordered by name.
func (d *Directory) PublicTopics() []PublicTopic {
	now := d.now()

	d.mu.RLock()
	self := d.st.self
	decls := make([]viewTopicRecord, 0, len(d.st.records))
	for _, r := range d.st.records {
		if isExpired(r.expires, now) || r.isRevocation() || r.body == nil {
			continue
		}
		ann := r.body.GetTopicAnnouncement()
		if ann == nil {
			continue
		}
		if !validPublicTopicName(ann.GetName()) || !validDescription(ann.GetDescription()) {
			continue
		}
		decls = append(decls, viewTopicRecord{
			name:        ann.GetName(),
			description: ann.GetDescription(),
			version:     r.version,
			publisher:   r.id.publisher,
		})
	}
	d.mu.RUnlock()

	// The peer table is queried outside the directory lock: a caller may
	// re-enter the directory from a peer callback.
	type mergedTopic struct {
		best  viewTopicRecord
		tier  int
		count int
	}
	byName := make(map[string]*mergedTopic, len(decls))
	for _, decl := range decls {
		tier := 2
		switch {
		case decl.publisher == self:
			tier = 0
		case d.peers.IsPeer(decl.publisher):
			tier = 1
		}
		decl.isPeer = tier == 1
		m, ok := byName[decl.name]
		if !ok {
			byName[decl.name] = &mergedTopic{best: decl, tier: tier, count: 1}
			continue
		}
		m.count++
		if tier < m.tier || (tier == m.tier && betterTopicDeclaration(decl, m.best)) {
			m.best = decl
			m.tier = tier
		}
	}

	cardAlias := make(map[Fingerprint]string)
	for _, c := range d.viewCards(now) {
		cardAlias[c.publisher] = c.alias
	}

	out := make([]PublicTopic, 0, len(byName))
	for _, m := range byName {
		alias := ""
		if m.best.isPeer {
			alias = d.peers.EffectiveAlias(m.best.publisher)
		} else {
			alias = cardAlias[m.best.publisher]
		}
		out = append(out, PublicTopic{
			Name:           m.best.name,
			Description:    m.best.description,
			Publisher:      m.best.publisher,
			PublisherAlias: alias,
			Declarers:      m.count,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// betterTopicDeclaration reports whether a should supply the description over b
// when both are in the same publisher tier. The higher version wins; a tie goes
// to the smaller publisher fingerprint.
func betterTopicDeclaration(a, b viewTopicRecord) bool {
	if a.version != b.version {
		return a.version > b.version
	}
	return bytes.Compare(a.publisher[:], b.publisher[:]) < 0
}

// CardsByAddress returns the fingerprints of every publisher whose live card
// advertises address, in ascending fingerprint byte order and without
// duplicates. It returns nil when no card matches. Addresses advertised with a
// kind this version does not know and the local Broker's own card are ignored.
// The lookup address and the advertised addresses are compared in canonical
// form, so host case, port leading zeros and IPv6 spelling do not hide a
// match.
//
// The version is chosen by each publisher, so a single winner must not be
// picked by version: any public publisher could set MaxUint64 and claim another
// Broker's address.
func (d *Directory) CardsByAddress(address string) []Fingerprint {
	now := d.now()
	self := d.viewSelf()
	cards := d.viewCards(now)
	address = pairing.CanonicalAddress(address)

	var out []Fingerprint
	seen := make(map[Fingerprint]bool, len(cards))
	for _, c := range cards {
		if c.publisher == self || seen[c.publisher] {
			continue
		}
		for _, a := range c.addresses {
			if pairing.CanonicalAddress(a.Addr) != address {
				continue
			}
			seen[c.publisher] = true
			out = append(out, c.publisher)
			break
		}
	}
	if len(out) == 0 {
		return nil
	}
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i][:], out[j][:]) < 0
	})
	return out
}

// viewCards snapshots every live, non-revoked BrokerCard record under the read
// lock, including the local Broker's own card.
func (d *Directory) viewCards(now time.Time) []viewCardRecord {
	d.mu.RLock()
	defer d.mu.RUnlock()

	out := make([]viewCardRecord, 0, len(d.st.records))
	for _, r := range d.st.records {
		if isExpired(r.expires, now) || r.isRevocation() || r.body == nil {
			continue
		}
		card := r.body.GetBrokerCard()
		if card == nil {
			continue
		}
		out = append(out, viewCardRecord{
			publisher: r.id.publisher,
			alias:     card.GetAlias(),
			addresses: knownAddrs(card, viewCardAddressMax),
		})
	}
	return out
}

// viewSelf returns the local Broker's fingerprint.
func (d *Directory) viewSelf() Fingerprint {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.st.self
}

// viewPeerAddresses builds the address list of a peer entry. When the local
// Broker is the dialer, the addresses the acceptor announced during pairing
// come first, in their original order, followed by the live card's addresses.
// SourceCard and SourcePairing candidates are skipped: the former may come
// from a revoked or expired card, and the latter is the pairing address. When
// the local Broker is the acceptor only the live card contributes. Entries are
// deduplicated by address.
func viewPeerAddresses(p pairing.Peer, card viewCardRecord, hasCard bool) []config.AdvertisedAddr {
	addrs := make([]config.AdvertisedAddr, 0, len(p.Candidates))
	seen := make(map[string]bool, len(p.Candidates))

	add := func(a config.AdvertisedAddr) {
		if seen[a.Addr] {
			return
		}
		seen[a.Addr] = true
		addrs = append(addrs, a)
	}

	if p.Role == pairing.RoleDialer {
		for _, c := range p.Candidates {
			if c.Source != pairing.SourceAcceptor {
				continue
			}
			add(config.AdvertisedAddr{Kind: "direct", Addr: c.Addr})
		}
	}
	if hasCard {
		for _, a := range card.addresses {
			add(a)
		}
	}
	return addrs
}

// viewStatus derives the broker status from the link phase and the address
// list.
func viewStatus(phase LinkPhase, addrs []config.AdvertisedAddr) BrokerStatus {
	if phase == LinkEstablished {
		return StatusConnected
	}
	if len(addrs) > 0 {
		return StatusConnectable
	}
	return StatusUnreachable
}
