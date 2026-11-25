package broker

import (
	"errors"
	"log/slog"
	"sync/atomic"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/link"
)

type linkControl interface {
	Listen(address string) error
	AnnounceAlias()
	AnnounceAddresses()
}

type agentAddressNotifier interface {
	localAliasChanged()
}

// subscribeConfigChanges registers the callbacks that forward configuration
// changes to link, directory and the local agent table. Config delivers changes
// one at a time in submission order, so these callbacks run serially in that
// order. When stopped is set they drop changes instead of forwarding them to
// closing components. A change touching both alias and advertised addresses
// reissues the card twice: the two callbacks each reissue, and config does not
// say that two notifications belong to one change.
func subscribeConfigChanges(cfg ConfigSource, l linkControl, d CardIssuer, a agentAddressNotifier, log *slog.Logger, stopped *atomic.Bool) {
	cfg.Subscribe("broker.listen", func(_, new config.Value) {
		if stopped.Load() {
			return
		}
		if err := l.Listen(new.String); err != nil {
			logListenRebindFailure(log, new.String, err)
		}
	})

	cfg.Subscribe("broker.alias", func(_, _ config.Value) {
		if stopped.Load() {
			return
		}
		l.AnnounceAlias()
		reissueCard(d)
		a.localAliasChanged()
	})

	cfg.SubscribeAdvertised(func(_, _ []config.AdvertisedAddr) {
		if stopped.Load() {
			return
		}
		l.AnnounceAddresses()
		reissueCard(d)
	})
}

// logListenRebindFailure logs a failed broker.listen rebind. A link closed by
// shutdown answers with link.ErrClosed, the expected outcome of a change
// delivered during shutdown, so that case logs at debug; every other failure
// is an error.
func logListenRebindFailure(log *slog.Logger, address string, err error) {
	if errors.Is(err, link.ErrClosed) {
		log.Debug("listen_rebind_failed", "address", address, "error", err)
		return
	}
	log.Error("listen_rebind_failed", "address", address, "error", err)
}

func reissueCard(d CardIssuer) {
	if d != nil {
		d.Reissue()
	}
}
