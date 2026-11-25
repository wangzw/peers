package broker

import (
	"bytes"
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/pairing"
)

func (b *Broker) peerInfos() []*localpb.BrokerInfo {
	return peerView{
		p:       b.deps.Pairing,
		l:       b.deps.PeerLinks,
		catalog: func() BrokerCatalog { return b.deps.Catalog },
	}.infos()
}

// ListBrokers returns every Broker this Broker knows: its peers and the
// non-peers the directory holds a card for. It needs no agent identity and no
// permission check.
func (b *Broker) ListBrokers(_ context.Context, _ *localpb.ListBrokersRequest) (*localpb.ListBrokersResponse, error) {
	out := b.peerInfos()
	if b.deps.Catalog != nil {
		for _, info := range b.deps.Catalog.NonPeers() {
			if info == nil || b.knownPeer(info.GetFingerprint(), out) {
				continue
			}
			// Clone before clearing the peer-only fields: the catalog keeps the
			// entry it handed out.
			entry := proto.Clone(info).(*localpb.BrokerInfo)
			entry.IsPeer = false
			entry.Role = localpb.PeerRole_PEER_ROLE_UNSPECIFIED
			entry.LinkState = localpb.LinkState_LINK_STATE_UNSPECIFIED
			out = append(out, entry)
		}
	}
	sortBrokerInfos(out)
	return &localpb.ListBrokersResponse{
		Brokers: out,
		Local: &localpb.LocalBrokerState{
			Degraded: b.degradedState(),
			Offline:  b.offline(),
		},
	}, nil
}

// knownPeer reports whether the fingerprint has an entry, or belongs to a peer
// that appeared after the peer list was built; such a Broker may still be in the
// directory's NonPeers list, so IsPeer is asked again.
func (b *Broker) knownPeer(fingerprint []byte, infos []*localpb.BrokerInfo) bool {
	for _, info := range infos {
		if bytes.Equal(info.GetFingerprint(), fingerprint) {
			return true
		}
	}
	if b.deps.Pairing == nil || len(fingerprint) != len(pairing.Fingerprint{}) {
		return false
	}
	var fp pairing.Fingerprint
	copy(fp[:], fingerprint)
	return b.deps.Pairing.IsPeer(fp)
}
