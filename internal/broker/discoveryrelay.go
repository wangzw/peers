package broker

import (
	"errors"
	"log/slog"
	"sort"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/directory"
	"github.com/wangzw/peers/internal/pairing"
)

type publicSwitch interface {
	SetPublic(public bool) error
	DeclarePublicTopic(name, description string) error
	RetirePublicTopic(name string)
}

type cardPairing interface {
	Role(fp pairing.Fingerprint) pairing.Role
	UpdateCardAddresses(fp pairing.Fingerprint, addrs []config.AdvertisedAddr)
}

// publicChanged snapshots the public topics and declares them, then reads the
// set again; topics that left meanwhile are retired so no declaration remains.
func publicChanged(sw publicSwitch, topics func() map[string]string, public bool, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	if err := sw.SetPublic(public); err != nil {
		logSetPublicFailure(log, public, err)
		return
	}
	if !public {
		return
	}

	snapshot := topics()
	names := make([]string, 0, len(snapshot))
	for name := range snapshot {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := sw.DeclarePublicTopic(name, snapshot[name]); err != nil {
			log.Warn("public_topic_redeclare_failed", "topic", name, "error", err)
		}
	}

	current := topics()
	var retired []string
	for name := range snapshot {
		if _, ok := current[name]; !ok {
			retired = append(retired, name)
		}
	}
	sort.Strings(retired)
	for _, name := range retired {
		sw.RetirePublicTopic(name)
	}
}

// logSetPublicFailure reports a failed SetPublic. ErrClosed means the directory
// was closed during shutdown, which is logged at debug rather than as a failure.
func logSetPublicFailure(log *slog.Logger, public bool, err error) {
	if errors.Is(err, directory.ErrClosed) {
		log.Debug("set_public_closed", "public", public, "error", err)
		return
	}
	log.Warn("set_public_failed", "public", public, "error", err)
}

func relayCard(p cardPairing, fp pairing.Fingerprint, addrs []config.AdvertisedAddr) {
	if p.Role(fp) != pairing.RoleDialer {
		return
	}
	p.UpdateCardAddresses(fp, addrs)
}
