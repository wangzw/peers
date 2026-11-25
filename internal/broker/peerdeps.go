package broker

import (
	"context"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing"
)

// PairingOps is the part of *pairing.Pairing the peer RPCs use.
type PairingOps interface {
	// Peers lists the committed peers.
	Peers() []pairing.Peer
	// EffectiveAlias returns the alias used for fp: its local alias when one was
	// assigned, otherwise its self-reported alias; empty when fp is not a peer.
	EffectiveAlias(fp pairing.Fingerprint) string
	// ResolveAlias resolves an effective alias to a committed peer's fingerprint
	// and whether one was found.
	ResolveAlias(alias string) (pairing.Fingerprint, bool)
	// IsPeer reports whether fp is a committed peer.
	IsPeer(fp pairing.Fingerprint) bool
	// Role returns this side's role in the pair with fp.
	Role(fp pairing.Fingerprint) pairing.Role
	// Connect dials address and completes the pairing handshake with the
	// one-time key, returning the committed peer and the host name shown in the
	// address.
	Connect(ctx context.Context, address, key, localAlias string) (pairing.Peer, string, error)
	// CreateOneTimeKey creates a one-time pairing key and returns it with its
	// expiry and the local addresses to show.
	CreateOneTimeKey() (key string, expires time.Time, shown []string, err error)
	// Unpair removes fp from the peer table and closes its link unless a new
	// pairing replaced it.
	Unpair(fp pairing.Fingerprint) error
}

// OwnAliasSync is the alias cache of *pairing.Pairing. AliasConflict takes only
// the pairing lock and may be called while the config lock is held.
type OwnAliasSync interface {
	// SetOwnAlias caches alias as this side's self-reported alias.
	SetOwnAlias(alias string)
	// AliasConflict reports whether alias is already taken by a peer or an
	// in-progress handshake.
	AliasConflict(alias string) bool
}

// PeerStore is the whole pairing surface the broker keeps a reference to.
type PeerStore interface {
	PairingOps
	OwnAliasSync
}

// LinkPhase mirrors link.Phase without importing link.
type LinkPhase uint8

// Link phases reported for a peer.
const (
	// LinkNone reports no link record.
	LinkNone LinkPhase = iota
	// LinkDialing reports that a dial is in progress.
	LinkDialing
	// LinkWaiting reports that the acceptor is waiting for the dialer.
	LinkWaiting
	// LinkHandshaking reports that the handshake is in progress.
	LinkHandshaking
	// LinkEstablished reports that the link is established.
	LinkEstablished
	// LinkBackoff reports that the dialer is waiting before redialing.
	LinkBackoff
	// LinkRejected reports that the link was rejected and redialing has stopped.
	LinkRejected
)

// LinkFailure mirrors the link failures the status detail explains.
type LinkFailure uint8

// Link failures the broker reports.
const (
	// FailureNone reports no failure, or a failure this Broker does not
	// distinguish: only not-a-peer, role mismatch, version incompatible and the
	// inbound limit are reported separately.
	FailureNone LinkFailure = iota
	// FailureNotAPeer means the peer answered Reject "not a peer".
	FailureNotAPeer
	// FailureRoleMismatch means the peer answered Reject "role mismatch".
	FailureRoleMismatch
	// FailureVersionIncompatible means the link protocol versions are incompatible.
	FailureVersionIncompatible
	// FailureInboundLimit means the peer overran this Broker's inbound limit.
	FailureInboundLimit
)

// PeerLinkStatus is the link state of one peer as the broker reports it.
type PeerLinkStatus struct {
	// Phase is the link's current phase.
	Phase LinkPhase
	// Failure is the last failure; a failure of an undistinguished kind reports
	// FailureNone.
	Failure LinkFailure
	// Detail is set only for FailureVersionIncompatible with a known peer version.
	Detail string
}

// PeerLinks is the part of link the peer RPCs and pairing need.
type PeerLinks interface {
	// PairDial dials address and runs the pairing handshake in run; when run
	// succeeds link keeps the connection.
	PairDial(ctx context.Context, address string, run func(pairing.PairConn) error) error
	// Drop closes the link to fp without removing it from the peer table.
	Drop(fp pairing.Fingerprint)
	// Kick ends fp's backoff early so this Broker redials at once when it is
	// the dialer; it does nothing for an accept-side record.
	Kick(fp pairing.Fingerprint)
	// Status returns fp's link state in the broker's own vocabulary.
	Status(fp pairing.Fingerprint) PeerLinkStatus
}

// BrokerCatalog supplies directory knowledge to ListBrokers. nil means no directory.
type BrokerCatalog interface {
	// CardAddresses returns the addresses of known kinds on fp's card, or nil
	// without a card.
	CardAddresses(fp pairing.Fingerprint) []config.AdvertisedAddr
	// NonPeers returns the brokers with a card that are not peers.
	NonPeers() []*localpb.BrokerInfo
}

// ConnectWarner adds warnings to a successful Connect. nil means none.
type ConnectWarner interface {
	// ConnectWarnings returns the warnings to add for peer.
	ConnectWarnings(peer pairing.Peer) []*localpb.Warning
}
