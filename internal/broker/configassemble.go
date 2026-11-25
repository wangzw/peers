package broker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
)

// configWiring mounts the configuration unit onto the running Broker. It runs
// last, so the directory assembler has filled Deps.CardIssuer and the link
// exists in the Assembly.
type configWiring struct {
	checker accessChecker
}

var _ Assembler = (*configWiring)(nil)

func (*configWiring) Name() string { return "config" }

func (w *configWiring) Assemble(_ context.Context, cfg ConfigSource, deps *Deps, asm *Assembly) ([]Component, error) {
	checker := w.checker
	if checker == nil {
		checker = asm.Access
		if checker == nil {
			if asm.System {
				return nil, errors.New("system mode requires an access checker in the Assembly")
			}
			checker = allowAllAccess{}
		}
	}
	if asm.System && checker != asm.Access {
		return nil, errors.New("system mode must use the Assembly access checker")
	}
	asm.configAssembled = true
	asm.effectiveAccess = checker
	path := func() string { return cfg.Paths().ConfigFile }
	deps.ServerOptions = append(deps.ServerOptions,
		grpc.ChainUnaryInterceptor(accessUnaryInterceptor(checker, path)),
		grpc.ChainStreamInterceptor(accessStreamInterceptor(checker, path)),
	)

	var l linkControl = noopLink{}
	if asm.Link != nil {
		l = asm.Link
	}
	var d CardIssuer = noopIssuer{}
	if deps.CardIssuer != nil {
		d = deps.CardIssuer
	}

	return []Component{&configComponent{cfg: cfg, link: l, cards: d}}, nil
}

// configComponent registers the configuration callbacks. Start runs after New
// and before the link listener and the local socket are bound, so the
// validator and the subscriptions exist before any local call can arrive.
type configComponent struct {
	cfg   ConfigSource
	link  linkControl
	cards CardIssuer
	log   *slog.Logger

	ready      atomic.Bool
	stopped    atomic.Bool
	suppressed atomic.Bool

	// boundMu guards bound and boundKnown, which hold the address the link is
	// listening on; holding it across Listen serializes rebinds.
	boundMu    sync.Mutex
	bound      string
	boundKnown bool
}

var (
	_ Component          = (*configComponent)(nil)
	_ PostReadyComponent = (*configComponent)(nil)
)

func (*configComponent) Name() string { return "config" }

func (c *configComponent) Start(_ context.Context, b *Broker) error {
	c.log = b.Logger()
	c.cfg.Validate("broker.listen", listenProbeCheck(c.cfg.ProbeListen))
	subscribeConfigChanges(c.cfg, preReadyLink{linkControl: c.link, comp: c}, c.cards, b, b.Logger(), &c.stopped)
	return nil
}

// AfterReady enables broker.listen rebinds and applies the current value once,
// reconciling a change delivered after Run's read that preReadyLink dropped.
func (c *configComponent) AfterReady(context.Context) error {
	c.ready.Store(true)
	if !c.isBound() {
		return nil
	}
	v, err := c.cfg.Get("broker.listen")
	if err != nil {
		return err
	}
	if err := c.rebind(v.String); err != nil {
		logListenRebindFailure(c.log, v.String, err)
	}
	return nil
}

func (c *configComponent) listenBound(address string) {
	c.boundMu.Lock()
	c.bound = address
	c.boundKnown = true
	c.boundMu.Unlock()
}

func (c *configComponent) isBound() bool {
	c.boundMu.Lock()
	defer c.boundMu.Unlock()
	return c.boundKnown
}

// rebind binds address unless the link already listens there, so a duplicate
// notification does not fail with EADDRINUSE.
func (c *configComponent) rebind(address string) error {
	if c.suppressed.Load() {
		return nil
	}
	c.boundMu.Lock()
	defer c.boundMu.Unlock()
	if c.boundKnown && c.bound == address {
		return nil
	}
	if err := c.link.Listen(address); err != nil {
		return err
	}
	c.bound = address
	c.boundKnown = true
	return nil
}

func (c *configComponent) suppressListenRebind(suppress bool) {
	c.suppressed.Store(suppress)
}

// Stop stops forwarding configuration changes, so a change delivered while the
// link closes is dropped instead of logging a rebind failure.
func (c *configComponent) Stop() { c.stopped.Store(true) }

// preReadyLink drops broker.listen rebinds until configComponent.AfterReady.
// Run binds the effective value after the components start, so forwarding a
// change delivered before then would make Run's own bind fail with "address
// already in use"; AfterReady applies a change delivered after Run's read.
type preReadyLink struct {
	linkControl
	comp *configComponent
}

func (l preReadyLink) Listen(address string) error {
	if !l.comp.ready.Load() {
		return nil
	}
	return l.comp.rebind(address)
}

type noopLink struct{}

func (noopLink) Listen(string) error { return nil }
func (noopLink) AnnounceAlias()      {}
func (noopLink) AnnounceAddresses()  {}

var _ linkControl = noopLink{}

type noopIssuer struct{}

func (noopIssuer) Reissue() {}

var _ CardIssuer = noopIssuer{}
