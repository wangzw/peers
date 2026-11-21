package directory

import (
	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing"
)

// Fingerprint identifies a Broker. It is the SHA-256 over the certificate's DER
// SubjectPublicKeyInfo, as defined by the pairing package, so it matches the
// pairing and link layers. The zero value means "no identity".
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
	Phase       LinkPhase
	Reason      LinkFailure // meaningful when Phase is LinkBackoff or LinkRejected
	PeerVersion uint32      // peer's link protocol version when Reason is FailureVersionIncompatible; 0 when unknown
}

// KnownBroker is one entry in the directory's broker view.
type KnownBroker struct {
	Fingerprint Fingerprint
	Alias       string                  // effective alias for a peer; card alias for a non-peer
	Addresses   []config.AdvertisedAddr // only entries with Kind == "direct"; may be nil or an empty slice
	IsPeer      bool
	Role        pairing.Role // pairing.RoleUnknown for a non-peer
	Status      BrokerStatus
	Link        LinkPhase // LinkNone for a non-peer
	Detail      string    // non-peer entries only; empty for peers
}

// PublicTopic is one public topic currently declared by a Broker.
type PublicTopic struct {
	Name string
	// Description is the description of the chosen declaration: the local
	// Broker's when it declares the name, otherwise a peer's, otherwise the
	// highest version among the remaining publishers.
	Description string
	// Publisher is the fingerprint of the Broker whose declaration supplied
	// Description.
	Publisher Fingerprint
	// PublisherAlias is the effective alias of a peer publisher, the alias in
	// the live card of any other publisher, and empty when neither is known.
	PublisherAlias string
	// Declarers is the number of live, well-formed declarations of the name.
	Declarers int
}
