package link

import (
	"crypto/tls"
	"log/slog"
	"time"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing"
)

// Peers is the part of the peer table link depends on; *pairing.Pairing
// implements it.
type Peers interface {
	// Fingerprint returns this side's certificate fingerprint.
	Fingerprint() pairing.Fingerprint
	// TLSConfig returns the TLS configuration for dialing and serving, carrying
	// this side's certificate.
	TLSConfig() *tls.Config
	// Peers returns a copy of every peer entry.
	Peers() []pairing.Peer
	// Role returns this side's role for fp, or RoleUnknown when fp is not a peer.
	Role(fp pairing.Fingerprint) pairing.Role
	// EffectiveAlias returns the alias used to address fp, or an empty string
	// when fp is not a peer.
	EffectiveAlias(fp pairing.Fingerprint) string
	// Candidates returns the candidate addresses of a dialer peer in dial order
	// and its last connected address.
	Candidates(fp pairing.Fingerprint) (addresses []string, last string)
	// ObserveAlias records the alias fp reported for itself.
	ObserveAlias(fp pairing.Fingerprint, alias string)
	// ObserveAcceptorAddresses replaces the acceptor-sourced candidate addresses
	// of fp.
	ObserveAcceptorAddresses(fp pairing.Fingerprint, addresses []config.AdvertisedAddr)
	// ObserveConnected records address as fp's last connected address.
	ObserveConnected(fp pairing.Fingerprint, address string)
	// Accept runs the responder side of a pairing handshake over conn and
	// closes conn on failure.
	Accept(conn pairing.PairConn) error
}

// Settings is the part of the configuration link depends on; ConfigSettings
// implements it.
type Settings interface {
	// Alias returns the effective Broker alias.
	Alias() string
	// Advertised returns a copy of this side's advertised address list.
	Advertised() []config.AdvertisedAddr
	// DialTimeout returns link.dial_timeout.
	DialTimeout() time.Duration
	// ReconnectMin returns link.reconnect_min.
	ReconnectMin() time.Duration
	// ReconnectMax returns link.reconnect_max.
	ReconnectMax() time.Duration
	// KeepaliveInterval returns link.keepalive_interval.
	KeepaliveInterval() time.Duration
	// KeepaliveTimeout returns link.keepalive_timeout.
	KeepaliveTimeout() time.Duration
	// Audit writes one configuration audit event.
	Audit(e config.Event)
}

// Clock is the time source link uses; RealClock implements it.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// AfterFunc calls f after d and returns a function that stops the timer and
	// reports whether it prevented the call.
	AfterFunc(d time.Duration, f func()) (stop func() bool)
}

// Options configures New.
type Options struct {
	// Peers is the peer table; required.
	Peers Peers
	// Settings is the configuration view; required.
	Settings Settings
	// Clock is the time source; real time when nil.
	Clock Clock
	// Logger receives diagnostic logs; slog.Default when nil.
	Logger *slog.Logger
	// ProtocolVersion is the link protocol version advertised in Hello and
	// compared on receipt; zero means linkpb.ProtocolVersion.
	ProtocolVersion uint32
}
