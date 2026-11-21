package broker

import (
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/pairing"
)

// cardMap is the cardLookup test double. Like the directory it canonicalizes
// the address stored in the card before comparing; the caller passes an
// already canonical address.
type cardMap map[string][]pairing.Fingerprint

func (m cardMap) CardsByAddress(address string) []pairing.Fingerprint {
	var out []pairing.Fingerprint
	for raw, fps := range m {
		if canonicalTestAddress(raw) == address {
			out = append(out, fps...)
		}
	}
	return out
}

// canonicalTestAddress mirrors the address canonicalization the directory
// applies to an advertised address.
func canonicalTestAddress(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return strings.ToLower(addr)
	}
	host = strings.ToLower(host)
	if ip, err := netip.ParseAddr(host); err == nil {
		host = ip.String()
	}
	if n, err := strconv.Atoi(port); err == nil && n >= 0 && n <= 65535 {
		port = strconv.Itoa(n)
	}
	return net.JoinHostPort(host, port)
}

func fpcheckFP(b byte) pairing.Fingerprint {
	var f pairing.Fingerprint
	f[0] = b
	return f
}

// fpcheckMessage is the exact warning text a mismatch at addr produces.
func fpcheckMessage(addr string, cardFP, peerFP pairing.Fingerprint) string {
	return fmt.Sprintf(
		"fingerprint mismatch: the directory card of %s lists %s, but the paired Broker presented %s; directory cards are self-reported, so confirm the fingerprint with the other side",
		cardFP.String(), addr, peerFP.String(),
	)
}

func TestFingerprintWarningsMismatchOnDialAddress(t *testing.T) {
	const addr = "10.0.0.1:17443"
	dialed := fpcheckFP(1)
	cardOwner := fpcheckFP(2)
	peer := pairing.Peer{
		Fingerprint: dialed,
		Candidates: []pairing.Candidate{
			{Source: pairing.SourcePairing, Addr: addr},
		},
	}

	got := fingerprintWarnings(peer, cardMap{addr: {cardOwner}})
	if len(got) != 1 {
		t.Fatalf("got %d warnings, want 1: %v", len(got), got)
	}
	if got[0].Kind != localpb.WarningKind_WARNING_KIND_FINGERPRINT_MISMATCH {
		t.Errorf("warning kind = %v, want WARNING_KIND_FINGERPRINT_MISMATCH", got[0].Kind)
	}
	if want := fpcheckMessage(addr, cardOwner, dialed); got[0].Message != want {
		t.Errorf("message = %q, want %q", got[0].Message, want)
	}
}

func TestFingerprintWarningsMismatchOnAcceptorAddress(t *testing.T) {
	const dialAddr = "10.0.0.1:17443"
	const acceptorAddr = "10.0.0.2:17443"
	dialed := fpcheckFP(1)
	cardOwner := fpcheckFP(2)
	peer := pairing.Peer{
		Fingerprint: dialed,
		Candidates: []pairing.Candidate{
			{Source: pairing.SourcePairing, Addr: dialAddr},
			{Source: pairing.SourceAcceptor, Addr: acceptorAddr},
		},
	}
	cards := cardMap{
		dialAddr:     {dialed},
		acceptorAddr: {cardOwner},
	}

	got := fingerprintWarnings(peer, cards)
	if len(got) != 1 {
		t.Fatalf("got %d warnings, want 1: %v", len(got), got)
	}
	if want := fpcheckMessage(acceptorAddr, cardOwner, dialed); got[0].Message != want {
		t.Errorf("message = %q, want %q", got[0].Message, want)
	}
}

// TestFingerprintWarningsMismatchOnRepairedAddress reproduces a re-pairing: the
// peer table keeps the card source across a re-pair, so the dialed address is
// listed as SourceCard and the pairing source is deduplicated away. The
// warning must still cover LastConnected, or a mismatching card goes unnoticed.
func TestFingerprintWarningsMismatchOnRepairedAddress(t *testing.T) {
	const addr = "10.0.0.1:17443"
	dialed := fpcheckFP(1)
	cardOwner := fpcheckFP(2)
	peer := pairing.Peer{
		Fingerprint: dialed,
		Candidates: []pairing.Candidate{
			{Source: pairing.SourceCard, Addr: addr},
		},
		LastConnected: addr,
	}

	got := fingerprintWarnings(peer, cardMap{addr: {cardOwner}})
	if len(got) != 1 {
		t.Fatalf("got %d warnings, want 1 from LastConnected: %v", len(got), got)
	}
	if want := fpcheckMessage(addr, cardOwner, dialed); got[0].Message != want {
		t.Errorf("message = %q, want %q", got[0].Message, want)
	}
}

// TestFingerprintWarningsNormalizesAddress covers an address that differs only
// in host case and a leading-zero port from the card's form.
func TestFingerprintWarningsNormalizesAddress(t *testing.T) {
	const canonical = "hosta.example:17443"
	dialed := fpcheckFP(1)
	cardOwner := fpcheckFP(2)
	peer := pairing.Peer{
		Fingerprint: dialed,
		Candidates: []pairing.Candidate{
			{Source: pairing.SourcePairing, Addr: "HOSTA.Example:017443"},
		},
	}

	got := fingerprintWarnings(peer, cardMap{"hosta.example:17443": {cardOwner}})
	if len(got) != 1 {
		t.Fatalf("got %d warnings, want 1 after normalization: %v", len(got), got)
	}
	if want := fpcheckMessage(canonical, cardOwner, dialed); got[0].Message != want {
		t.Errorf("message = %q, want %q", got[0].Message, want)
	}
}

func TestFingerprintWarningsMatchNoWarning(t *testing.T) {
	fp := fpcheckFP(1)
	peer := pairing.Peer{
		Fingerprint: fp,
		Candidates: []pairing.Candidate{
			{Source: pairing.SourcePairing, Addr: "10.0.0.1:17443"},
			{Source: pairing.SourceAcceptor, Addr: "10.0.0.2:17443"},
		},
		LastConnected: "10.0.0.1:17443",
	}

	if got := fingerprintWarnings(peer, cardMap{"10.0.0.1:17443": {fp}, "10.0.0.2:17443": {fp}}); got != nil {
		t.Fatalf("matching cards returned %d warnings, want nil: %v", len(got), got)
	}
	if got := fingerprintWarnings(peer, cardMap{}); got != nil {
		t.Fatalf("missing cards returned %d warnings, want nil: %v", len(got), got)
	}
}

func TestFingerprintWarningsReportsEveryMismatchingPublisher(t *testing.T) {
	const addr = "10.0.0.1:17443"
	dialed := fpcheckFP(1)
	other1 := fpcheckFP(2)
	other2 := fpcheckFP(3)
	peer := pairing.Peer{
		Fingerprint: dialed,
		Candidates: []pairing.Candidate{
			{Source: pairing.SourcePairing, Addr: addr},
		},
	}
	cards := cardMap{addr: {other1, other2}}

	got := fingerprintWarnings(peer, cards)
	if len(got) != 2 {
		t.Fatalf("got %d warnings, want 2 for the other publishers: %v", len(got), got)
	}
	want := []string{
		fpcheckMessage(addr, other1, dialed),
		fpcheckMessage(addr, other2, dialed),
	}
	for i, message := range want {
		if got[i].Message != message {
			t.Errorf("warning %d = %q, want %q", i, got[i].Message, message)
		}
	}
}

func TestFingerprintWarningsIgnoresCardSource(t *testing.T) {
	const dialAddr = "10.0.0.1:17443"
	const cardAddr = "10.0.0.9:17443"
	dialed := fpcheckFP(1)
	cardOwner := fpcheckFP(2)
	peer := pairing.Peer{
		Fingerprint: dialed,
		Candidates: []pairing.Candidate{
			{Source: pairing.SourcePairing, Addr: dialAddr},
			{Source: pairing.SourcePairing, Addr: dialAddr},
			{Source: pairing.SourceCard, Addr: cardAddr},
		},
	}
	cards := cardMap{
		dialAddr: {cardOwner},
		cardAddr: {cardOwner},
	}

	got := fingerprintWarnings(peer, cards)
	if len(got) != 1 {
		t.Fatalf("got %d warnings, want 1 (card-sourced address ignored, duplicate deduplicated): %v", len(got), got)
	}
	if want := fpcheckMessage(dialAddr, cardOwner, dialed); got[0].Message != want {
		t.Errorf("message = %q, want %q", got[0].Message, want)
	}
}

// TestFingerprintWarningsDedupesCardAcrossAddresses checks that one mismatching
// card yields one warning even when it advertises several of the peer's
// addresses, so the result is bounded by the number of cards.
func TestFingerprintWarningsDedupesCardAcrossAddresses(t *testing.T) {
	const addr1 = "10.0.0.1:17443"
	const addr2 = "10.0.0.2:17443"
	dialed := fpcheckFP(1)
	cardOwner := fpcheckFP(2)
	peer := pairing.Peer{
		Fingerprint: dialed,
		Candidates: []pairing.Candidate{
			{Source: pairing.SourcePairing, Addr: addr1},
			{Source: pairing.SourceAcceptor, Addr: addr2},
		},
	}
	cards := cardMap{addr1: {cardOwner}, addr2: {cardOwner}}

	got := fingerprintWarnings(peer, cards)
	if len(got) != 1 {
		t.Fatalf("got %d warnings for one card at two addresses, want 1: %v", len(got), got)
	}
	if want := fpcheckMessage(addr1, cardOwner, dialed); got[0].Message != want {
		t.Errorf("message = %q, want %q", got[0].Message, want)
	}
}

// TestFingerprintWarningsCapped checks that a directory naming many publishers
// at one address cannot grow the Connect response without bound.
func TestFingerprintWarningsCapped(t *testing.T) {
	const addr = "10.0.0.1:17443"
	dialed := fpcheckFP(1)
	peer := pairing.Peer{
		Fingerprint: dialed,
		Candidates: []pairing.Candidate{
			{Source: pairing.SourcePairing, Addr: addr},
		},
	}
	cards := make([]pairing.Fingerprint, 0, maxFingerprintWarnings+5)
	for i := 0; i < maxFingerprintWarnings+5; i++ {
		cards = append(cards, fpcheckFP(byte(10+i)))
	}

	got := fingerprintWarnings(peer, cardMap{addr: cards})
	if len(got) != maxFingerprintWarnings {
		t.Fatalf("got %d warnings, want the cap of %d", len(got), maxFingerprintWarnings)
	}
}
