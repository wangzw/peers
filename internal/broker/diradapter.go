package broker

import (
	"errors"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/directory"
	"github.com/wangzw/peers/internal/pairing"
)

// directoryOps is the part of *directory.Directory the adapter uses. It keeps
// the adapter testable with discoveryDir and names the exact surface the
// broker needs from the directory package.
type directoryOps interface {
	SetPublic(public bool) error
	DeclarePublicTopic(name, description string) error
	RetirePublicTopic(name string)
	PublicTopics() []directory.PublicTopic
	Brokers() []directory.KnownBroker
	CardsByAddress(address string) []pairing.Fingerprint
	Reissue()
}

// directoryAdapter adapts *directory.Directory to the broker's directory
// views: PublicTopicDirectory, BrokerCatalog, ConnectWarner and CardIssuer.
// Every method forwards; only the public topic declaration maps errors,
// because broker-local returns them to the client as an *Error.
type directoryAdapter struct {
	d directoryOps
}

// newDirectoryAdapter returns the adapter backed by d.
func newDirectoryAdapter(d directoryOps) *directoryAdapter {
	return &directoryAdapter{d: d}
}

// SetPublic turns the Broker's public flag on or off in the directory.
func (a *directoryAdapter) SetPublic(public bool) error {
	return a.d.SetPublic(public)
}

// DeclarePublicTopic declares a public topic and maps the directory's
// sentinels onto the broker's error codes.
func (a *directoryAdapter) DeclarePublicTopic(name, description string) error {
	err := a.d.DeclarePublicTopic(name, description)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, directory.ErrNotPublic):
		return Errorf(localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
			"Not joined: the Broker is not public; set broker.public to true before declaring a public topic")
	case errors.Is(err, directory.ErrQuotaExceeded):
		return Errorf(localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED,
			"Not joined: this Broker already declares the maximum number of public topics")
	case errors.Is(err, directory.ErrInvalidTopic):
		return Errorf(localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
			"Not joined: invalid topic name")
	case errors.Is(err, directory.ErrInvalidDescription):
		return Errorf(localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
			"Not joined: the description must be valid UTF-8 without control characters and at most 256 bytes")
	default:
		return Errorf(localpb.ErrorCode_ERROR_CODE_INTERNAL,
			"declaring the public topic failed: %v", err)
	}
}

// RetirePublicTopic withdraws a public topic declaration.
func (a *directoryAdapter) RetirePublicTopic(name string) {
	a.d.RetirePublicTopic(name)
}

// Reissue re-signs the Broker's card. It satisfies CardIssuer.
func (a *directoryAdapter) Reissue() {
	a.d.Reissue()
}

// PublicTopics returns the public topics the directory currently holds, in
// the directory's order.
func (a *directoryAdapter) PublicTopics() []PublicTopic {
	topics := a.d.PublicTopics()
	if topics == nil {
		return nil
	}
	out := make([]PublicTopic, 0, len(topics))
	for _, t := range topics {
		out = append(out, PublicTopic{
			Name:           t.Name,
			Description:    t.Description,
			Publisher:      t.Publisher,
			PublisherAlias: t.PublisherAlias,
			Declarers:      t.Declarers,
		})
	}
	return out
}

// CardAddresses returns the addresses of the directory's entry for fp. The
// entry holds only known address kinds; a missing entry yields nil.
func (a *directoryAdapter) CardAddresses(fp pairing.Fingerprint) []config.AdvertisedAddr {
	for _, b := range a.d.Brokers() {
		if b.Fingerprint == fp {
			return b.Addresses
		}
	}
	return nil
}

// NonPeers returns the directory's brokers that are not peers, as local API
// entries. The peer-only fields are left at their zero values; ListBrokers
// forces them again.
func (a *directoryAdapter) NonPeers() []*localpb.BrokerInfo {
	var out []*localpb.BrokerInfo
	for _, b := range a.d.Brokers() {
		if b.IsPeer {
			continue
		}
		info := &localpb.BrokerInfo{
			Alias:        b.Alias,
			Fingerprint:  append([]byte(nil), b.Fingerprint[:]...),
			Status:       brokerStatusToProto(b.Status),
			StatusDetail: b.Detail,
		}
		info.AdvertisedAddresses = make([]*localpb.AdvertisedAddress, 0, len(b.Addresses))
		for _, addr := range b.Addresses {
			info.AdvertisedAddresses = append(info.AdvertisedAddresses, &localpb.AdvertisedAddress{
				Kind: addr.Kind,
				Addr: addr.Addr,
			})
		}
		out = append(out, info)
	}
	return out
}

// ConnectWarnings compares the addresses presented during pairing with the
// directory's cards.
func (a *directoryAdapter) ConnectWarnings(peer pairing.Peer) []*localpb.Warning {
	return fingerprintWarnings(peer, a.d)
}

// install fills the broker's directory hooks with this adapter, including the
// card issuer hook, so an alias or advertised-address change re-signs the card
// (REQ-broker-discovery-016).
func (a *directoryAdapter) install(deps *Deps) {
	deps.Directory = a
	deps.Catalog = a
	deps.Warner = a
	deps.CardIssuer = a
}

var _ CardIssuer = (*directoryAdapter)(nil)

// brokerStatusToProto maps the directory's broker status onto the local API.
// An unknown status maps to UNSPECIFIED.
func brokerStatusToProto(s directory.BrokerStatus) localpb.BrokerStatus {
	switch s {
	case directory.StatusConnected:
		return localpb.BrokerStatus_BROKER_STATUS_CONNECTED
	case directory.StatusConnectable:
		return localpb.BrokerStatus_BROKER_STATUS_CONNECTABLE
	case directory.StatusUnreachable:
		return localpb.BrokerStatus_BROKER_STATUS_NOT_DIRECTLY_REACHABLE
	default:
		return localpb.BrokerStatus_BROKER_STATUS_UNSPECIFIED
	}
}
