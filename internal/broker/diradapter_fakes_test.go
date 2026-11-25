package broker

import (
	"sync"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/directory"
	"github.com/wangzw/peers/internal/pairing"
)

// discoveryDir is the directoryHandle test double; it serializes field access
// under mu and records the calls that change state.
type discoveryDir struct {
	mu sync.Mutex

	public       bool
	setPublicErr error

	declareErr map[string]error

	brokers []directory.KnownBroker
	topics  []directory.PublicTopic

	cards map[string]pairing.Fingerprint

	startErr error
	onCard   func(fp pairing.Fingerprint, addrs []config.AdvertisedAddr)

	calls []string
}

func (d *discoveryDir) SetPublic(public bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if public {
		d.calls = append(d.calls, "SetPublic(true)")
	} else {
		d.calls = append(d.calls, "SetPublic(false)")
	}
	if d.setPublicErr != nil {
		return d.setPublicErr
	}
	d.public = public
	return nil
}

func (d *discoveryDir) DeclarePublicTopic(name, description string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, "Declare("+name+","+description+")")
	return d.declareErr[name]
}

func (d *discoveryDir) RetirePublicTopic(name string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, "Retire("+name+")")
}

func (d *discoveryDir) PublicTopics() []directory.PublicTopic {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.topics
}

func (d *discoveryDir) Brokers() []directory.KnownBroker {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.brokers
}

func (d *discoveryDir) CardsByAddress(address string) []pairing.Fingerprint {
	d.mu.Lock()
	defer d.mu.Unlock()
	fp, ok := d.cards[address]
	if !ok {
		return nil
	}
	return []pairing.Fingerprint{fp}
}

func (d *discoveryDir) Reissue() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, "Reissue")
}

func (d *discoveryDir) Start() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, "Start")
	return d.startErr
}

func (d *discoveryDir) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, "Close")
}

func (d *discoveryDir) OnCard(f func(fp pairing.Fingerprint, addrs []config.AdvertisedAddr)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onCard = f
}

func (d *discoveryDir) RetireInactiveTopics() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, "RetireInactive")
}

func (d *discoveryDir) recordedCalls() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.calls...)
}

func (d *discoveryDir) cardCallback() func(fp pairing.Fingerprint, addrs []config.AdvertisedAddr) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.onCard
}
