package pairing

import (
	"net/netip"
	"time"

	"github.com/wangzw/peers/internal/config"
)

// Audit reasons. Detail carries only the reason; it never carries a key, an
// address or any other secret.
const (
	auditReasonKeyInvalid          = "key invalid"
	auditReasonVerificationFailed  = "verification failed"
	auditReasonTryLater            = "try later"
	auditReasonKeyExpired          = "key expired"
	auditReasonVersionIncompatible = "version incompatible"
	auditReasonTimeout             = "handshake timeout"
	auditReasonNotConnected        = "not connected"
)

// auditPairingSucceeded records a pairing completed on this side. pairing
// writes this event for the acceptor; the broker writes the initiator's event
// with the caller's agent_id after Connect returns.
func auditPairingSucceeded(s settings, now time.Time, fp Fingerprint, alias string) {
	s.Audit(newPairingAuditEvent(config.EventPairingSucceeded, now, fp, alias, "", ""))
}

// auditPairingFailed records a pairing this side rejected or lost. reason is one
// of the auditReason constants. mergeKey is set for a source-level failure that
// a stranger can repeat without limit, either a rate-limited request or one
// whose protocol version does not match; the audit log merges repeated events
// from the same source.
func auditPairingFailed(s settings, now time.Time, fp Fingerprint, alias, reason, mergeKey string) {
	s.Audit(newPairingAuditEvent(config.EventPairingFailed, now, fp, alias, reason, mergeKey))
}

// auditLocalAliasAssigned records that pairing generated a local alias to
// disambiguate a peer whose self-reported alias collided.
func auditLocalAliasAssigned(s settings, now time.Time, fp Fingerprint, alias string) {
	s.Audit(newPairingAuditEvent(config.EventLocalAliasAssigned, now, fp, alias, "", ""))
}

// newPairingAuditEvent builds one pairing-owned event. Time comes from the
// caller's injected clock, never from time.Now, so tests can pin it. pairing
// leaves Actor and Account empty: those identify a management call and belong
// to the broker.
func newPairingAuditEvent(kind config.EventKind, now time.Time, fp Fingerprint, alias, detail, mergeKey string) config.Event {
	e := config.Event{
		Time:     now,
		Event:    kind,
		Detail:   detail,
		MergeKey: mergeKey,
	}
	var zero Fingerprint
	if fp != zero {
		e.Peer = &config.PeerRef{Fingerprint: [32]byte(fp), Alias: alias}
	}
	return e
}

// sourcePrefix returns the network prefix that stands for the source of a
// pairing connection whose remote address is remote, in host:port form: the
// address itself as a /32 for IPv4 and the enclosing /64 for IPv6. An
// IPv4-mapped IPv6 address counts as IPv4 and an IPv6 zone is dropped. It
// returns "" when remote carries no IP address; every such address counts as
// one unknown source.
//
// It is a deliberate copy of the same-named function in internal/link, because
// pairing does not import link; keep the two implementations in step.
func sourcePrefix(remote string) string {
	ap, err := netip.ParseAddrPort(remote)
	if err != nil {
		return ""
	}
	addr := ap.Addr().Unmap()
	if addr.Is4() {
		return netip.PrefixFrom(addr, 32).Masked().String()
	}
	return netip.PrefixFrom(addr.WithZone(""), 64).Masked().String()
}

// auditSourceMergeKey builds the merge key for a source-level pairing failure.
// The key is "source:" plus the source prefix, so repeated attempts from one
// address or one IPv6 /64 collapse in the audit read while the port is
// ignored. An address with no IP yields no merge key, because grouping
// unrelated events under a bare "source:" would merge failures that have no
// source in common.
func auditSourceMergeKey(remoteAddr string) string {
	source := sourcePrefix(remoteAddr)
	if source == "" {
		return ""
	}
	return "source:" + source
}
