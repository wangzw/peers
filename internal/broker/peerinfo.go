package broker

import (
	"bytes"
	"context"
	"sort"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing"
)

// peerAddresses lists the addresses a peer Broker is known to listen on.
//
// For a peer this Broker dials, the acceptor advertised those addresses and
// the directory card may carry more; the address dialed during pairing is not
// one of them, because it is not necessarily an advertised address, and
// SourceCard candidates are skipped because they may come from a withdrawn or
// expired card (a valid card's addresses arrive through cardAddrs). For a peer
// that dialed this Broker, only the card is used: this Broker never dials, so
// the acceptor's own list is irrelevant.
func peerAddresses(p pairing.Peer, cardAddrs []config.AdvertisedAddr) []*localpb.AdvertisedAddress {
	out := make([]*localpb.AdvertisedAddress, 0, len(p.Candidates)+len(cardAddrs))
	seen := make(map[string]bool, len(p.Candidates)+len(cardAddrs))
	add := func(addr string) {
		if addr == "" || seen[addr] {
			return
		}
		seen[addr] = true
		out = append(out, &localpb.AdvertisedAddress{Kind: config.AdvertisedKindDirect, Addr: addr})
	}
	if p.Role == pairing.RoleDialer {
		for _, c := range p.Candidates {
			if c.Source == pairing.SourceAcceptor {
				add(c.Addr)
			}
		}
	}
	for _, a := range cardAddrs {
		if a.Kind == config.AdvertisedKindDirect {
			add(a.Addr)
		}
	}
	return out
}

// linkStateToProto maps the broker's link phase onto the local API's state.
func linkStateToProto(ph LinkPhase) localpb.LinkState {
	switch ph {
	case LinkDialing:
		return localpb.LinkState_LINK_STATE_DIALING
	case LinkWaiting:
		return localpb.LinkState_LINK_STATE_WAITING
	case LinkHandshaking:
		return localpb.LinkState_LINK_STATE_HANDSHAKING
	case LinkEstablished:
		return localpb.LinkState_LINK_STATE_ESTABLISHED
	case LinkBackoff:
		return localpb.LinkState_LINK_STATE_BACKOFF
	case LinkRejected:
		return localpb.LinkState_LINK_STATE_REJECTED
	default:
		return localpb.LinkState_LINK_STATE_UNSPECIFIED
	}
}

// peerStatusDetail is the single source of a peer's status_detail text.
func peerStatusDetail(st PeerLinkStatus) string {
	var detail string
	switch st.Failure {
	case FailureNotAPeer:
		detail = "the peer no longer recognizes this Broker as its peer (it was unpaired on that side); redialing has stopped; pair again to reconnect"
	case FailureRoleMismatch:
		detail = "both Brokers consider themselves the dialer for this pair; pair again to fix the roles"
	case FailureVersionIncompatible:
		detail = "link protocol version incompatible with this peer; upgrade the older Broker"
	case FailureInboundLimit:
		detail = "the peer sent more state frames than this Broker's inbound limit allows on one connection; the link was closed and will reconnect after a backoff; check the audit log for the link_down record"
	default:
		return ""
	}
	if st.Detail != "" {
		detail += ": " + st.Detail
	}
	return detail
}

// peerBrokerInfo turns one peer and its link state into a ListBrokers entry.
func peerBrokerInfo(p pairing.Peer, st PeerLinkStatus, cardAddrs []config.AdvertisedAddr) *localpb.BrokerInfo {
	alias := p.LocalAlias
	if alias == "" {
		alias = p.Alias
	}
	addrs := peerAddresses(p, cardAddrs)

	status := localpb.BrokerStatus_BROKER_STATUS_NOT_DIRECTLY_REACHABLE
	switch {
	case st.Phase == LinkEstablished:
		status = localpb.BrokerStatus_BROKER_STATUS_CONNECTED
	case len(addrs) > 0:
		status = localpb.BrokerStatus_BROKER_STATUS_CONNECTABLE
	}

	var role localpb.PeerRole
	switch p.Role {
	case pairing.RoleDialer:
		role = localpb.PeerRole_PEER_ROLE_DIALER
	case pairing.RoleAcceptor:
		role = localpb.PeerRole_PEER_ROLE_ACCEPTOR
	}

	fp := make([]byte, len(p.Fingerprint))
	copy(fp, p.Fingerprint[:])

	return &localpb.BrokerInfo{
		Alias:               alias,
		Fingerprint:         fp,
		AdvertisedAddresses: addrs,
		IsPeer:              true,
		Status:              status,
		Role:                role,
		StatusDetail:        peerStatusDetail(st),
		LinkState:           linkStateToProto(st.Phase),
	}
}

// sortBrokerInfos orders brokers with peers first, then by alias, then by
// fingerprint bytes (equal to ascending hexadecimal order).
func sortBrokerInfos(infos []*localpb.BrokerInfo) {
	sort.SliceStable(infos, func(i, j int) bool {
		a, b := infos[i], infos[j]
		if a.GetIsPeer() != b.GetIsPeer() {
			return a.GetIsPeer()
		}
		if a.GetAlias() != b.GetAlias() {
			return a.GetAlias() < b.GetAlias()
		}
		return bytes.Compare(a.GetFingerprint(), b.GetFingerprint()) < 0
	})
}

// peerView implements PeerView over the pairing and link surfaces. The
// catalog is looked up on every call so an assembler can install it after the
// view was built.
type peerView struct {
	p       PairingOps
	l       PeerLinks
	catalog func() BrokerCatalog
}

var _ PeerView = peerView{}

// newPeerView builds the broker's peer view with a fixed catalog.
func newPeerView(p PairingOps, l PeerLinks, c BrokerCatalog) PeerView {
	return newPeerViewFunc(p, l, func() BrokerCatalog { return c })
}

// newPeerViewFunc builds the broker's peer view with a catalog looked up on
// each call; nil means no directory.
func newPeerViewFunc(p PairingOps, l PeerLinks, catalog func() BrokerCatalog) PeerView {
	return peerView{p: p, l: l, catalog: catalog}
}

// EffectiveAlias reports the peer's effective local alias; a fingerprint that
// is not a peer yields false.
func (v peerView) EffectiveAlias(f [32]byte) (string, bool) {
	if v.p == nil {
		return "", false
	}
	a := v.p.EffectiveAlias(pairing.Fingerprint(f))
	return a, a != ""
}

// Peers lists this Broker's peers for Status.
func (v peerView) Peers(context.Context) []*localpb.BrokerInfo {
	return v.infos()
}

// infos lists this Broker's peers as local API entries.
func (v peerView) infos() []*localpb.BrokerInfo {
	if v.p == nil || v.l == nil {
		return []*localpb.BrokerInfo{}
	}
	var catalog BrokerCatalog
	if v.catalog != nil {
		catalog = v.catalog()
	}
	peers := v.p.Peers()
	out := make([]*localpb.BrokerInfo, 0, len(peers))
	for _, peer := range peers {
		var card []config.AdvertisedAddr
		if catalog != nil {
			card = catalog.CardAddresses(peer.Fingerprint)
		}
		out = append(out, peerBrokerInfo(peer, v.l.Status(peer.Fingerprint), card))
	}
	sortBrokerInfos(out)
	return out
}
