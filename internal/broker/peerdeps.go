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
	Peers() []pairing.Peer
	EffectiveAlias(fp pairing.Fingerprint) string
	ResolveAlias(alias string) (pairing.Fingerprint, bool)
	IsPeer(fp pairing.Fingerprint) bool
	Role(fp pairing.Fingerprint) pairing.Role
	Connect(ctx context.Context, address, key, localAlias string) (pairing.Peer, string, error)
	CreateOneTimeKey() (key string, expires time.Time, shown []string, err error)
	Unpair(fp pairing.Fingerprint) error
}

// OwnAliasSync is the alias cache of *pairing.Pairing. AliasConflict takes only
// the pairing lock and may be called while the config lock is held.
type OwnAliasSync interface {
	SetOwnAlias(alias string)
	AliasConflict(alias string) bool
}

// PeerStore is the whole pairing surface the broker keeps a reference to.
type PeerStore interface {
	PairingOps
	OwnAliasSync
}

// LinkPhase mirrors link.Phase without importing link.
type LinkPhase uint8

const (
	LinkNone LinkPhase = iota // no link record
	LinkDialing
	LinkWaiting
	LinkHandshaking
	LinkEstablished
	LinkBackoff
	LinkRejected
)

// LinkFailure mirrors the link failures the status detail explains.
type LinkFailure uint8

const (
	FailureNone         LinkFailure = iota
	FailureNotAPeer                 // the peer answered Reject "not a peer"
	FailureRoleMismatch             // the peer answered Reject "role mismatch"
	FailureVersionIncompatible
	FailureInboundLimit // the peer overran this Broker's inbound limit
)

// PeerLinkStatus is the link state of one peer as the broker reports it.
type PeerLinkStatus struct {
	Phase   LinkPhase
	Failure LinkFailure
	Detail  string // set only for FailureVersionIncompatible with a known peer version
}

// PeerLinks is the part of link the peer RPCs and pairing need.
type PeerLinks interface {
	PairDial(ctx context.Context, address string, run func(pairing.PairConn) error) error
	Drop(fp pairing.Fingerprint)
	Kick(fp pairing.Fingerprint)
	Status(fp pairing.Fingerprint) PeerLinkStatus
}

// BrokerCatalog supplies directory knowledge to ListBrokers. nil means no directory.
type BrokerCatalog interface {
	CardAddresses(fp pairing.Fingerprint) []config.AdvertisedAddr // known kinds only; nil without a card
	NonPeers() []*localpb.BrokerInfo                              // brokers with a card that are not peers
}

// ConnectWarner adds warnings to a successful Connect. nil means none.
type ConnectWarner interface {
	ConnectWarnings(peer pairing.Peer) []*localpb.Warning
}
