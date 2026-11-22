package pairing

import (
	"log/slog"
	"net"
	"strconv"
	"strings"

	"github.com/wangzw/peers/internal/config"
)

// maxCandidatesPerSource is the cap on one acceptor or card address list. The
// cap keeps peers.json bounded no matter how large a list a peer advertises.
const maxCandidatesPerSource = 8

// maxAdvertisedAddrLen bounds one stored candidate address: a 253-byte domain
// name, ':' and a five-digit port. Longer input is rejected before the port is
// parsed so a port written with leading zeros cannot stretch the stored string.
// The bound is measured on the address without a fully qualified name's
// trailing dot, which config removes before applying its 253-byte domain limit.
const maxAdvertisedAddrLen = 259

// filterKnown keeps only the address entries this version understands and
// returns them as a new slice. Unknown kinds are skipped without disturbing the
// recognised entries around them. Empty addresses are dropped as well: they can
// only come from a malformed or hostile peer and cannot be dialed.
func filterKnown(addrs []config.AdvertisedAddr) []config.AdvertisedAddr {
	out := make([]config.AdvertisedAddr, 0, len(addrs))
	for _, a := range addrs {
		if a.Kind != config.AdvertisedKindDirect {
			continue
		}
		if a.Addr == "" {
			continue
		}
		out = append(out, config.AdvertisedAddr{Kind: a.Kind, Addr: a.Addr})
	}
	return out
}

// capKnown filters a list down to the entries that can be stored as candidates,
// keeps at most maxCandidatesPerSource of them and drops the rest from the end.
// Besides the unknown kinds filterKnown removes, an address that is not a
// dialable host:port is skipped in place so the valid entries around it are
// kept. A malformed entry is not logged: a hostile peer could repeat one to grow
// the log.
func capKnown(addrs []config.AdvertisedAddr) []config.AdvertisedAddr {
	out := make([]config.AdvertisedAddr, 0, len(addrs))
	for _, a := range filterKnown(addrs) {
		if !validAdvertisedAddr(a.Addr) {
			continue
		}
		out = append(out, a)
		if len(out) == maxCandidatesPerSource {
			break
		}
	}
	return out
}

// validDialPort reports whether port is a decimal number in 1 to 65535.
func validDialPort(port string) bool {
	if port == "" {
		return false
	}
	for i := 0; i < len(port); i++ {
		if port[i] < '0' || port[i] > '9' {
			return false
		}
	}
	number, err := strconv.Atoi(port)
	return err == nil && number >= 1 && number <= 65535
}

// validAdvertisedAddr reports whether addr is a host:port this version can dial.
// The host follows config.ValidDialHost and the port is a decimal number in 1 to
// 65535, the same rules connect_broker applies to its address. The "name(addr)" display
// form is not accepted: the table stores only the bare dialing address, so
// pairing keys can still be shown with the name without it reaching the table.
func validAdvertisedAddr(addr string) bool {
	// Reject an address that is far past the bound before splitting it. The
	// precise check below gives a fully qualified name one byte of slack for its
	// trailing dot, so anything longer than that is certainly invalid.
	if addr == "" || len(addr) > maxAdvertisedAddrLen+1 {
		return false
	}
	if strings.ContainsAny(addr, "()") {
		return false
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" || !validDialPort(port) || !config.ValidDialHost(host) {
		return false
	}
	// config.ValidDialHost strips one trailing dot from a fully qualified name
	// before applying the 253-byte domain limit, so the address form may be one
	// byte longer. Measure the stored address without that dot, or pairing would
	// drop a boundary address that configuration accepts.
	stored := len(addr)
	if strings.HasSuffix(host, ".") {
		stored--
	}
	return stored <= maxAdvertisedAddrLen
}

// mergeCandidateList merges the stored sources in the order acceptor, card,
// pairing, removes duplicate addresses keeping the first occurrence, and
// records the source each surviving address came from. Peers for which this
// Broker is the acceptor have no candidates.
func mergeCandidateList(e *peerEntry) []Candidate {
	if e == nil || e.role != RoleDialer {
		return nil
	}

	var out []Candidate
	seen := make(map[string]struct{})

	appendAddr := func(addr string, source AddressSource) {
		if addr == "" {
			return
		}
		if _, dup := seen[addr]; dup {
			return
		}
		seen[addr] = struct{}{}
		out = append(out, Candidate{Source: source, Addr: addr})
	}

	for _, a := range e.acceptor {
		if a.Kind != config.AdvertisedKindDirect {
			continue
		}
		appendAddr(a.Addr, SourceAcceptor)
	}
	for _, a := range e.card {
		if a.Kind != config.AdvertisedKindDirect {
			continue
		}
		appendAddr(a.Addr, SourceCard)
	}
	appendAddr(e.pairingAddr, SourcePairing)

	return out
}

// mergeCandidates returns the merged candidate addresses and the last connected
// address for a dialer peer. An acceptor peer yields no addresses and no last
// connected address. The last connected address is returned even when no
// candidate source is populated: link can still redial the address that last
// worked.
func mergeCandidates(e *peerEntry) ([]string, string) {
	if e == nil || e.role != RoleDialer {
		return nil, ""
	}
	list := mergeCandidateList(e)
	if len(list) == 0 {
		return nil, e.lastConnected
	}
	addrs := make([]string, 0, len(list))
	for _, c := range list {
		addrs = append(addrs, c.Addr)
	}
	return addrs, e.lastConnected
}

// ObserveAcceptorAddresses replaces the acceptor-sourced candidate list of a
// dialer peer. Unknown address kinds are dropped and the list is capped. Calls
// for unknown or acceptor peers are ignored; persistence failures keep the old
// list and are logged.
func (p *Pairing) ObserveAcceptorAddresses(fp Fingerprint, addresses []config.AdvertisedAddr) {
	if p == nil || p.peers == nil {
		slog.Error("pairing: ObserveAcceptorAddresses on an unopened instance", "fingerprint", fingerprintHex(fp))
		return
	}
	if err := p.peers.updateSource(fp, SourceAcceptor, addresses); err != nil {
		slog.Error("pairing: persist acceptor addresses", "fingerprint", fingerprintHex(fp), "error", err)
	}
}

// UpdateCardAddresses replaces the directory-card-sourced candidate list of a
// dialer peer. Unknown address kinds are dropped and the list is capped. Calls
// for unknown or acceptor peers are ignored; persistence failures keep the old
// list and are logged.
func (p *Pairing) UpdateCardAddresses(fp Fingerprint, addresses []config.AdvertisedAddr) {
	if p == nil || p.peers == nil {
		slog.Error("pairing: UpdateCardAddresses on an unopened instance", "fingerprint", fingerprintHex(fp))
		return
	}
	if err := p.peers.updateSource(fp, SourceCard, addresses); err != nil {
		slog.Error("pairing: persist card addresses", "fingerprint", fingerprintHex(fp), "error", err)
	}
}

// Candidates returns the candidate addresses of a dialer peer in dial order and
// the last connected address. Acceptor peers and unknown fingerprints have no
// candidates.
func (p *Pairing) Candidates(fp Fingerprint) (addresses []string, last string) {
	if p == nil || p.peers == nil {
		return nil, ""
	}
	entry, ok := p.peers.get(fp)
	if !ok {
		return nil, ""
	}
	return mergeCandidates(&entry)
}
