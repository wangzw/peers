package broker

import (
	"fmt"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/pairing"
)

// maxFingerprintWarnings bounds the warnings one Connect result carries. The
// directory lets every publisher name the same address, so without a bound a
// hostile directory could add one warning per card for every address the peer
// presented and grow the response without limit.
const maxFingerprintWarnings = 8

// cardLookup resolves an address to the fingerprints of every publisher whose
// live directory card advertises it.
type cardLookup interface {
	CardsByAddress(address string) []pairing.Fingerprint
}

// fingerprintWarnings compares the addresses presented during pairing with the
// directory cards at the same addresses. Every publisher found at an address
// that is not the peer itself yields one warning, because the directory lets
// each publisher choose its card version and one of them must not be allowed
// to shadow another. The compared set is every address the peer presented
// itself (the address dialed during pairing and the addresses the acceptor
// advertised) plus LastConnected. LastConnected is included unconditionally:
// re-pairing to an address the peer's card already advertises leaves it
// card-sourced in Candidates, and a card-sourced entry alone would hide the
// mismatch. Card-sourced addresses are otherwise left out because they come
// from the directory and cannot contradict it.
//
// Addresses are canonicalized before the lookup and the directory canonicalizes
// the advertised addresses it compares, so host case, port leading zeros and
// IPv6 spelling do not hide a match.
func fingerprintWarnings(peer pairing.Peer, cards cardLookup) []*localpb.Warning {
	var warnings []*localpb.Warning
	seen := make(map[string]bool)
	// warned deduplicates by card fingerprint: one card may name several of the
	// peer's addresses, but the mismatch is with the card, not with each
	// address. The cap then bounds the whole result.
	warned := make(map[pairing.Fingerprint]bool)

	check := func(addr string) {
		if addr == "" || len(warnings) >= maxFingerprintWarnings {
			return
		}
		addr = pairing.CanonicalAddress(addr)
		if seen[addr] {
			return
		}
		seen[addr] = true
		for _, cardFP := range cards.CardsByAddress(addr) {
			if cardFP == peer.Fingerprint || warned[cardFP] {
				continue
			}
			warned[cardFP] = true
			warnings = append(warnings, &localpb.Warning{
				Kind: localpb.WarningKind_WARNING_KIND_FINGERPRINT_MISMATCH,
				Message: fmt.Sprintf(
					"fingerprint mismatch: the directory card of %s lists %s, but the paired Broker presented %s; directory cards are self-reported, so confirm the fingerprint with the other side",
					cardFP.String(), addr, peer.Fingerprint.String(),
				),
			})
			if len(warnings) >= maxFingerprintWarnings {
				return
			}
		}
	}

	for _, c := range peer.Candidates {
		if c.Source != pairing.SourcePairing && c.Source != pairing.SourceAcceptor {
			continue
		}
		check(c.Addr)
	}
	check(peer.LastConnected)
	return warnings
}
