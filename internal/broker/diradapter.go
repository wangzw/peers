package broker

import (
	"errors"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/directory"
	"github.com/wangzw/peers/internal/pairing"
)

type directoryOps interface {
	SetPublic(public bool) error
	DeclarePublicTopic(name, description string) error
	RetirePublicTopic(name string)
	PublicTopics() []directory.PublicTopic
	Brokers() []directory.KnownBroker
	CardsByAddress(address string) []pairing.Fingerprint
	Reissue()
}

type directoryAdapter struct {
	d directoryOps
}

func newDirectoryAdapter(d directoryOps) *directoryAdapter {
	return &directoryAdapter{d: d}
}

func (a *directoryAdapter) SetPublic(public bool) error {
	return a.d.SetPublic(public)
}

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

func (a *directoryAdapter) RetirePublicTopic(name string) {
	a.d.RetirePublicTopic(name)
}

func (a *directoryAdapter) Reissue() {
	a.d.Reissue()
}

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

func (a *directoryAdapter) CardAddresses(fp pairing.Fingerprint) []config.AdvertisedAddr {
	for _, b := range a.d.Brokers() {
		if b.Fingerprint == fp {
			return b.Addresses
		}
	}
	return nil
}

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

func (a *directoryAdapter) ConnectWarnings(peer pairing.Peer) []*localpb.Warning {
	return fingerprintWarnings(peer, a.d)
}

func (a *directoryAdapter) install(deps *Deps) {
	deps.Directory = a
	deps.Catalog = a
	deps.Warner = a
	deps.CardIssuer = a
}

var _ CardIssuer = (*directoryAdapter)(nil)

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
