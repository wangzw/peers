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

func auditPairingSucceeded(s settings, now time.Time, fp Fingerprint, alias string) {
	s.Audit(newPairingAuditEvent(config.EventPairingSucceeded, now, fp, alias, "", ""))
}

func auditPairingFailed(s settings, now time.Time, fp Fingerprint, alias, reason, mergeKey string) {
	s.Audit(newPairingAuditEvent(config.EventPairingFailed, now, fp, alias, reason, mergeKey))
}

func auditLocalAliasAssigned(s settings, now time.Time, fp Fingerprint, alias string) {
	s.Audit(newPairingAuditEvent(config.EventLocalAliasAssigned, now, fp, alias, "", ""))
}

func newPairingAuditEvent(kind config.EventKind, now time.Time, fp Fingerprint, alias, detail, mergeKey string) config.Event {
	e := config.Event{
		Time:     now,
		Event:    kind,
		Detail:   detail,
		MergeKey: mergeKey,
	}
	if !fp.IsZero() {
		e.Peer = &config.PeerRef{Fingerprint: [32]byte(fp), Alias: alias}
	}
	return e
}

// This is a deliberate copy of the same-named function in internal/link, which
// pairing cannot import; keep the two implementations in step.
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

func auditSourceMergeKey(remoteAddr string) string {
	source := sourcePrefix(remoteAddr)
	if source == "" {
		return ""
	}
	return "source:" + source
}
