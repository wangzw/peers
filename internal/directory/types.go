package directory

import (
	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing"
)

// Fingerprint identifies a Broker. It is the pairing package's fingerprint, the
// SHA-256 of the certificate's DER SubjectPublicKeyInfo. The zero value means
// no identity.
type Fingerprint = pairing.Fingerprint

// BrokerStatus describes how a Broker appears in the directory's view.
type BrokerStatus uint8

const (
	// StatusConnected means the link to this Broker is established.
	StatusConnected BrokerStatus = iota + 1
	// StatusConnectable means no link is established, but at least one
	// recognized advertised address is known.
	StatusConnectable
	// StatusUnreachable means no link is established and no recognized
	// advertised address is known.
	StatusUnreachable
)

// String returns "connected", "connectable", "not_directly_reachable" or
// "unknown".
func (s BrokerStatus) String() string {
	switch s {
	case StatusConnected:
		return "connected"
	case StatusConnectable:
		return "connectable"
	case StatusUnreachable:
		return "not_directly_reachable"
	default:
		return "unknown"
	}
}

// LinkPhase mirrors the phase of the link to one peer.
type LinkPhase uint8

const (
	// LinkNone means there is no link record (a non-peer).
	LinkNone LinkPhase = iota
	// LinkDialing means a dial is in progress.
	LinkDialing
	// LinkWaiting means the link waits for an incoming connection.
	LinkWaiting
	// LinkHandshaking means the handshake is in progress.
	LinkHandshaking
	// LinkEstablished means the link is usable.
	LinkEstablished
	// LinkBackoff means the link failed and waits before retrying.
	LinkBackoff
	// LinkRejected means the link was rejected and will not retry.
	LinkRejected
)

// LinkFailure is the most recent failure reason of a link.
type LinkFailure uint8

const (
	// FailureNone means no failure was recorded.
	FailureNone LinkFailure = iota
	// FailureNotAPeer means the remote end does not treat this Broker as a peer.
	FailureNotAPeer
	// FailureRoleMismatch means both ends dialed or both ends waited.
	FailureRoleMismatch
	// FailureVersionIncompatible means the link protocol versions do not match.
	FailureVersionIncompatible
)

// LinkView is the directory's view of the link to one peer.
type LinkView struct {
	// Phase is the state of the link to this peer.
	Phase LinkPhase
	// Reason is the most recent failure reason; it is meaningful only when
	// Phase is LinkBackoff or LinkRejected.
	Reason LinkFailure
	// PeerVersion is the peer's link protocol version when Reason is
	// FailureVersionIncompatible, and 0 when unknown.
	PeerVersion uint32
}

// KnownBroker is one entry in the directory's broker view.
type KnownBroker struct {
	// Fingerprint identifies the Broker.
	Fingerprint Fingerprint
	// Alias is the effective alias for a peer and the card alias for a
	// non-peer.
	Alias string
	// Addresses holds the advertised addresses this version recognizes, all of
	// kind "direct"; it may be nil or empty.
	Addresses []config.AdvertisedAddr
	// IsPeer reports whether this Broker is a peer.
	IsPeer bool
	// Role is this side's role in the peer pair; pairing.RoleUnknown for a
	// non-peer.
	Role pairing.Role
	// Status is the Broker's status in the directory view.
	Status BrokerStatus
	// Link is the link phase; LinkNone for a non-peer.
	Link LinkPhase
	// Detail is explanatory text for a non-peer entry; it is empty for a peer.
	Detail string
}

// PublicTopic is one public topic currently declared by a Broker.
type PublicTopic struct {
	// Name is the topic name.
	Name string
	// Description is the description of the chosen declaration.
	Description string
	// Publisher is the fingerprint of the Broker whose declaration supplied
	// Description.
	Publisher Fingerprint
	// PublisherAlias is the effective alias of a peer publisher, the alias on
	// the live card of any other publisher, and empty when neither is known.
	PublisherAlias string
	// Declarers is the number of valid declarations of the name.
	Declarers int
}
