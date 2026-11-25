package broker

import (
	"fmt"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/pairing"
)

const maxFingerprintWarnings = 8

type cardLookup interface {
	CardsByAddress(address string) []pairing.Fingerprint
}

// fingerprintWarnings compares the addresses the peer presented and
// LastConnected against the directory cards there. LastConnected is checked
// even when it is card-sourced, because a re-pair can leave it so and a
// card-sourced entry alone would hide a mismatch.
func fingerprintWarnings(peer pairing.Peer, cards cardLookup) []*localpb.Warning {
	var warnings []*localpb.Warning
	seen := make(map[string]bool)
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
