package broker

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"sync"
	"time"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/directory"
	"github.com/wangzw/peers/internal/pairing"
)

type directoryHandle interface {
	directoryOps
	Start() error
	Close()
	OnCard(f func(fp pairing.Fingerprint, addrs []config.AdvertisedAddr))
	RetireInactiveTopics()
}

type directoryAssembler struct {
	open        func(directory.Deps) (directoryHandle, error)
	newIdentity func(*pairing.Pairing) (directory.Identity, error)
}

var (
	_ Assembler = (*directoryAssembler)(nil)

	_ directoryHandle = (*directory.Directory)(nil)
)

func (a *directoryAssembler) Name() string { return "directory" }

func (a *directoryAssembler) Assemble(_ context.Context, cfg ConfigSource, deps *Deps, asm *Assembly) ([]Component, error) {
	if asm.Pairing == nil || asm.Link == nil {
		return nil, nil
	}
	newIdentity := a.newIdentity
	if newIdentity == nil {
		newIdentity = directory.NewIdentity
	}
	id, err := newIdentity(asm.Pairing)
	if err != nil {
		return nil, fmt.Errorf("directory identity: %w", err)
	}
	dd := directory.Deps{
		Identity: id,
		Peers:    asm.Pairing,
		Links:    directory.NewLinkTransport(asm.Link),
		Logger:   deps.Logger,
		Now:      wallClock,
	}
	return a.assembleWith(dd, asm.Pairing, cfg, deps)
}

// openDirectory opens the directory; on failure it returns an untyped nil
// handle, never a typed nil pointer.
func openDirectory(d directory.Deps) (directoryHandle, error) {
	dir, err := directory.Open(d)
	if err != nil {
		return nil, err
	}
	return dir, nil
}

func (a *directoryAssembler) assembleWith(dd directory.Deps, cp cardPairing, cfg ConfigSource, deps *Deps) ([]Component, error) {
	open := a.open
	if open == nil {
		open = openDirectory
	}
	d, err := open(dd)
	if err != nil {
		// Only a failure to read directory.json wraps *fs.PathError; a wiring
		// error from Open stays fatal.
		var pathErr *fs.PathError
		if errors.As(err, &pathErr) {
			err = wrapDataUnavailable(dataKindDirectory, err)
		}
		return nil, fmt.Errorf("opening the directory: %w", err)
	}
	d.OnCard(func(fp pairing.Fingerprint, addrs []config.AdvertisedAddr) {
		relayCard(cp, fp, addrs)
	})
	ad := newDirectoryAdapter(d)
	ad.install(deps)
	return []Component{&directoryComponent{d: d, a: ad, cfg: cfg}}, nil
}

type directoryComponent struct {
	d   directoryHandle
	a   *directoryAdapter
	cfg ConfigSource

	afterFunc func(time.Duration, func()) interface{ Stop() bool }

	// publicMu serializes reading and applying broker.public so an older value
	// cannot overwrite a newer one.
	publicMu sync.Mutex

	mu    sync.Mutex
	timer interface{ Stop() bool }
}

var (
	_ Component          = (*directoryComponent)(nil)
	_ PostReadyComponent = (*directoryComponent)(nil)
)

func (c *directoryComponent) Name() string { return "directory" }

// Start starts the directory and wires the public switch before the link
// listens, so its frame handlers and link subscription register in time.
func (c *directoryComponent) Start(_ context.Context, b *Broker) error {
	return c.startWith(b.publicTopicSet, b.Logger())
}

func (c *directoryComponent) startWith(topics func() map[string]string, log *slog.Logger) error {
	if err := c.d.Start(); err != nil {
		return fmt.Errorf("starting the directory: %w", err)
	}
	if log == nil {
		log = slog.Default()
	}
	// The subscriber ignores the notification's value and re-reads under
	// publicMu, so a queued older notification cannot restore a stale value.
	c.cfg.Subscribe("broker.public", func(_, _ config.Value) {
		c.applyCurrentPublic(func(public bool) {
			publicChanged(c.a, topics, public, log)
		})
	})
	// Read the current value once under the same lock: a change between the
	// configuration load and the subscription would not reach the subscriber.
	c.applyCurrentPublic(func(public bool) {
		if err := c.a.SetPublic(public); err != nil {
			logSetPublicFailure(log, public, err)
		}
	})
	return nil
}

func (c *directoryComponent) applyCurrentPublic(apply func(bool)) {
	c.publicMu.Lock()
	defer c.publicMu.Unlock()
	v, err := c.cfg.Get("broker.public")
	if err != nil {
		return
	}
	apply(v.Bool)
}

func (c *directoryComponent) AfterReady(context.Context) error {
	afterFunc := c.afterFunc
	if afterFunc == nil {
		afterFunc = func(d time.Duration, f func()) interface{ Stop() bool } {
			return time.AfterFunc(d, f)
		}
	}
	c.mu.Lock()
	c.timer = afterFunc(2*time.Minute, c.d.RetireInactiveTopics)
	c.mu.Unlock()
	return nil
}

// Stop cancels the retirement timer before closing the directory, so a late
// timer cannot touch a closed directory.
func (c *directoryComponent) Stop() {
	c.mu.Lock()
	timer := c.timer
	c.timer = nil
	c.mu.Unlock()
	if timer != nil {
		timer.Stop()
	}
	c.d.Close()
}
