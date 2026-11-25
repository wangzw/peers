package directory

import (
	"bytes"
	"sort"
	"time"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing"
)

// viewCardAddressMax is the advertised-address count a card contributes to the
// views; extra entries are still accepted and forwarded but not listed.
const viewCardAddressMax = 8

type viewCardRecord struct {
	publisher Fingerprint
	alias     string
	addresses []config.AdvertisedAddr
}

type viewTopicRecord struct {
	name        string
	description string
	version     uint64
	publisher   Fingerprint
	isPeer      bool
}

// Brokers returns every paired peer and every non-peer that holds a live card,
// sorted by alias and then fingerprint. The local Broker never appears. The
// record snapshot is taken under the read lock; the peer table and link status
// are queried outside it.
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
// directory, including the local Broker's own, merged one entry per name and
// ordered by name. The description is the one the tiering rule selects for the
// name, and a declaration with an invalid name or description is left out.
func (d *Directory) PublicTopics() []PublicTopic {
	now := d.now()

	d.mu.RLock()
	self := d.st.self
	decls := make([]viewTopicRecord, 0, len(d.st.records))
	for _, r := range d.st.records {
		if !liveRecord(r, now) {
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

	// Query the peer table outside the directory lock: a peer callback may
	// re-enter the directory.
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

func betterTopicDeclaration(a, b viewTopicRecord) bool {
	if a.version != b.version {
		return a.version > b.version
	}
	return bytes.Compare(a.publisher[:], b.publisher[:]) < 0
}

// CardsByAddress returns the fingerprints of every publisher whose live card
// advertises address, deduplicated and sorted ascending by fingerprint, or nil
// when no card matches. Addresses advertised with an unknown kind and the local
// Broker's own card are ignored, and both sides are compared in canonical form.
// A publisher chooses its own version, so do not pick a single winner by
// version.
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

func (d *Directory) viewCards(now time.Time) []viewCardRecord {
	d.mu.RLock()
	defer d.mu.RUnlock()

	out := make([]viewCardRecord, 0, len(d.st.records))
	for _, r := range d.st.records {
		if !liveRecord(r, now) {
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

func (d *Directory) viewSelf() Fingerprint {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.st.self
}

// viewPeerAddresses builds a peer's address list: the acceptor-announced
// candidates first when this side is the dialer, then the live card's
// addresses, deduplicated by address; an acceptor side takes only the card's.
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

func viewStatus(phase LinkPhase, addrs []config.AdvertisedAddr) BrokerStatus {
	if phase == LinkEstablished {
		return StatusConnected
	}
	if len(addrs) > 0 {
		return StatusConnectable
	}
	return StatusUnreachable
}
