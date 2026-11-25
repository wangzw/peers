package broker

import (
	"context"
	"fmt"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/link"
)

// listenOnceRunner is both the link a configuration change reaches and the
// LinkRunner Run drives; afterBind and onClose place a change at a precise point.
type listenOnceRunner struct {
	mu        sync.Mutex
	bound     []string
	listened  map[string]bool
	closed    bool
	afterBind func()
	onClose   func()
}

func (r *listenOnceRunner) Listen(address string) error {
	r.mu.Lock()
	r.bound = append(r.bound, address)
	if r.closed {
		r.mu.Unlock()
		return link.ErrClosed
	}
	if r.listened[address] {
		r.mu.Unlock()
		return fmt.Errorf("listen tcp %s: %w", address, syscall.EADDRINUSE)
	}
	if r.listened == nil {
		r.listened = make(map[string]bool)
	}
	r.listened[address] = true
	hook := r.afterBind
	r.afterBind = nil
	r.mu.Unlock()
	if hook != nil {
		hook()
	}
	return nil
}

func (r *listenOnceRunner) binds() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.bound...)
}

func (*listenOnceRunner) Start() {}

func (r *listenOnceRunner) Close() {
	r.mu.Lock()
	r.closed = true
	hook := r.onClose
	r.mu.Unlock()
	if hook != nil {
		hook()
	}
}

func (*listenOnceRunner) AnnounceAlias()     {}
func (*listenOnceRunner) AnnounceAddresses() {}
func (*listenOnceRunner) Reissue()           {}

type listenChangeComponent struct {
	cfg *fakeConfig
}

func (*listenChangeComponent) Name() string { return "listen-change" }

func (c *listenChangeComponent) Start(context.Context, *Broker) error {
	c.cfg.setValue("broker.listen", config.Value{Kind: config.KindString, String: "127.0.0.1:17444"})
	return nil
}

func (*listenChangeComponent) Stop() {}

// TestRunListenChangeBeforeReadyDoesNotBindTwice checks that a broker.listen
// change delivered before Run's startup bind is not bound twice.
func TestRunListenChangeBeforeReadyDoesNotBindTwice(t *testing.T) {
	runner := &listenOnceRunner{}
	rt := newRunTest(t, func(rt *runTest) {
		rt.cfg.setValue("broker.listen", config.Value{Kind: config.KindString, String: "127.0.0.1:17443"})
		rt.opts.Assemblers = []Assembler{&fakeAssembler{name: "link", fn: func(_ context.Context, _ ConfigSource, _ *Deps, asm *Assembly) ([]Component, error) {
			asm.Runner = runner
			return nil, nil
		}}}
		rt.opts.Components = []Component{
			&configComponent{cfg: rt.cfg, link: runner, cards: runner},
			&listenChangeComponent{cfg: rt.cfg},
		}
	})
	rt.start()

	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q, want %q", got, "ok\n")
	}
	if got, want := runner.binds(), []string{"127.0.0.1:17444"}; !equalStrings(got, want) {
		t.Fatalf("Listen addresses = %v, want %v", got, want)
	}
}

// readyBarrier runs AfterReady after the config component, so a test can observe
// the binds it produced.
type readyBarrier struct{ done chan struct{} }

func (*readyBarrier) Name() string { return "ready-barrier" }

func (*readyBarrier) Start(context.Context, *Broker) error { return nil }

func (*readyBarrier) Stop() {}

func (b *readyBarrier) AfterReady(context.Context) error {
	close(b.done)
	return nil
}

// asyncListenConfig delivers broker.listen notifications from a goroutine after
// release.
type asyncListenConfig struct {
	*fakeConfig
	release chan struct{}
	done    chan struct{}
}

func (c *asyncListenConfig) Subscribe(key string, onChange func(old, new config.Value)) {
	if key != "broker.listen" {
		c.fakeConfig.Subscribe(key, onChange)
		return
	}
	c.fakeConfig.Subscribe(key, func(old, new config.Value) {
		go func() {
			<-c.release
			onChange(old, new)
			close(c.done)
		}()
	})
}

func waitBarrier(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(eventTimeout):
		t.Fatal("the config component's AfterReady did not run")
	}
}

// TestRunListenChangeDuringStartupIsReboundAfterReady checks that a change
// arriving before the config component is ready is applied in AfterReady.
func TestRunListenChangeDuringStartupIsReboundAfterReady(t *testing.T) {
	const (
		startup = "127.0.0.1:17443"
		changed = "127.0.0.1:17444"
	)
	runner := &listenOnceRunner{}
	ready := make(chan struct{})
	rt := newRunTest(t, func(rt *runTest) {
		rt.cfg.setValue("broker.listen", config.Value{Kind: config.KindString, String: startup})
		runner.afterBind = func() {
			rt.cfg.setValue("broker.listen", config.Value{Kind: config.KindString, String: changed})
		}
		rt.opts.Assemblers = []Assembler{&fakeAssembler{name: "link", fn: func(_ context.Context, _ ConfigSource, _ *Deps, asm *Assembly) ([]Component, error) {
			asm.Runner = runner
			return nil, nil
		}}}
		rt.opts.Components = []Component{
			&configComponent{cfg: rt.cfg, link: runner, cards: runner},
			&readyBarrier{done: ready},
		}
	})
	rt.start()

	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q, want %q", got, "ok\n")
	}
	waitBarrier(t, ready)

	binds := runner.binds()
	if len(binds) == 0 || binds[len(binds)-1] != changed {
		t.Fatalf("Listen addresses = %v, want the last one %q", binds, changed)
	}
}

// TestRunLateListenNotificationDoesNotRebindTwice checks that a late notification
// for an already bound address does not rebind or log an error.
func TestRunLateListenNotificationDoesNotRebindTwice(t *testing.T) {
	const (
		startup = "127.0.0.1:17443"
		changed = "127.0.0.1:17444"
	)
	runner := &listenOnceRunner{}
	ready := make(chan struct{})
	var async *asyncListenConfig
	rt := newRunTest(t, func(rt *runTest) {
		async = &asyncListenConfig{fakeConfig: rt.cfg, release: make(chan struct{}), done: make(chan struct{})}
		rt.opts.Deps.Config = async
		rt.cfg.setValue("broker.listen", config.Value{Kind: config.KindString, String: startup})
		runner.afterBind = func() {
			async.setValue("broker.listen", config.Value{Kind: config.KindString, String: changed})
		}
		rt.opts.Assemblers = []Assembler{&fakeAssembler{name: "link", fn: func(_ context.Context, _ ConfigSource, _ *Deps, asm *Assembly) ([]Component, error) {
			asm.Runner = runner
			return nil, nil
		}}}
		rt.opts.Components = []Component{
			&configComponent{cfg: async, link: runner, cards: runner},
			&readyBarrier{done: ready},
		}
	})
	rt.start()

	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q, want %q", got, "ok\n")
	}
	waitBarrier(t, ready)

	if got, want := runner.binds(), []string{startup, changed}; !equalStrings(got, want) {
		t.Fatalf("Listen addresses = %v, want %v", got, want)
	}

	close(async.release)
	select {
	case <-async.done:
	case <-time.After(eventTimeout):
		t.Fatal("the late notification was not delivered")
	}

	for _, rec := range decodeLogLines(t, rt.logs.String()) {
		if rec["event"] == "listen_rebind_failed" && rec["level"] == "ERROR" {
			t.Errorf("late notification logged an error-level listen_rebind_failed: %v", rec)
		}
	}
}

// TestRunListenChangeDuringShutdownLogsNoError checks that a change during
// shutdown logs ErrClosed at debug, not as an error.
func TestRunListenChangeDuringShutdownLogsNoError(t *testing.T) {
	const (
		startup = "127.0.0.1:17443"
		changed = "127.0.0.1:17444"
	)
	runner := &listenOnceRunner{}
	ready := make(chan struct{})
	rt := newRunTest(t, func(rt *runTest) {
		rt.cfg.setValue("broker.listen", config.Value{Kind: config.KindString, String: startup})
		rt.cfg.setValue("log.level", config.Value{Kind: config.KindString, String: "debug"})
		runner.onClose = func() {
			rt.cfg.setValue("broker.listen", config.Value{Kind: config.KindString, String: changed})
		}
		rt.opts.Assemblers = []Assembler{&fakeAssembler{name: "link", fn: func(_ context.Context, _ ConfigSource, _ *Deps, asm *Assembly) ([]Component, error) {
			asm.Runner = runner
			return nil, nil
		}}}
		rt.opts.Components = []Component{
			&configComponent{cfg: rt.cfg, link: runner, cards: runner},
			&readyBarrier{done: ready},
		}
	})
	rt.start()

	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q, want %q", got, "ok\n")
	}
	waitBarrier(t, ready)

	rt.cancel()
	if err := rt.waitErr(); err != nil {
		t.Fatalf("Run error = %v, want nil", err)
	}

	var sawDebug bool
	for _, rec := range decodeLogLines(t, rt.logs.String()) {
		if rec["event"] != "listen_rebind_failed" {
			continue
		}
		if rec["level"] == "ERROR" {
			t.Errorf("shutdown change logged an error-level listen_rebind_failed: %v", rec)
		}
		if rec["level"] == "DEBUG" {
			sawDebug = true
		}
	}
	if !sawDebug {
		t.Error("the shutdown change did not log a debug-level listen_rebind_failed")
	}
}
