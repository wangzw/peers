package broker

import (
	"context"
	"errors"
	"io/fs"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/directory"
	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/pairing"
)

var (
	_ directoryHandle = (*discoveryDir)(nil)
	_ publicSwitch    = (*directoryAdapter)(nil)
	_ Assembler       = (*directoryAssembler)(nil)
)

// TestDirectoryAssemblerFillsDeps checks that Assemble opens the directory with
// the wall clock and installs the adapter in every directory hook.
func TestDirectoryAssemblerFillsDeps(t *testing.T) {
	tb := newTestBroker(t)
	dir := &discoveryDir{}
	assembledNow := time.Date(2001, 2, 3, 4, 5, 6, 789000000, time.UTC)
	previousClock := wallClock
	wallClock = func() time.Time { return assembledNow }
	defer func() { wallClock = previousClock }()

	var gotNow time.Time
	a := &directoryAssembler{
		open: func(dd directory.Deps) (directoryHandle, error) {
			if dd.Now == nil {
				t.Fatal("open received a nil clock")
			}
			gotNow = dd.Now()
			return dir, nil
		},
		newIdentity: func(*pairing.Pairing) (directory.Identity, error) { return nil, nil },
	}

	deps := &Deps{}
	asm := &Assembly{Pairing: &pairing.Pairing{}, Link: &link.Link{}}
	comps, err := a.Assemble(context.Background(), tb.cfg, deps, asm)
	if err != nil {
		t.Fatalf("Assemble: %v", err)
	}
	if !gotNow.Equal(assembledNow) {
		t.Errorf("open received clock reading %v, want the assembled wall clock %v", gotNow, assembledNow)
	}
	if len(comps) != 1 {
		t.Fatalf("Assemble returned %d components, want 1", len(comps))
	}
	ad, ok := deps.Directory.(*directoryAdapter)
	if !ok {
		t.Fatalf("Deps.Directory is %T, want *directoryAdapter", deps.Directory)
	}
	if deps.Catalog != ad || deps.Warner != ad || deps.CardIssuer != CardIssuer(ad) {
		t.Error("install did not fill Directory, Catalog, Warner and CardIssuer with the same adapter")
	}
	if ad.d != dir {
		t.Error("the adapter does not forward to the opened directory")
	}
}

// TestDirectoryAssemblerSkipsWithoutLink checks that Assemble without pairing
// and link installs no directory.
func TestDirectoryAssemblerSkipsWithoutLink(t *testing.T) {
	deps := &Deps{}
	comps, err := (&directoryAssembler{}).Assemble(context.Background(), nil, deps, &Assembly{})
	if err != nil {
		t.Fatalf("Assemble without pairing and link: %v", err)
	}
	if comps != nil {
		t.Fatalf("Assemble without pairing and link returned %d components, want none", len(comps))
	}
	if deps.Directory != nil {
		t.Error("Assemble without pairing and link installed a directory")
	}
}

// TestDirectoryAssemblerIdentityErrorFailsStartup checks that an identity error
// fails assembly before the directory is opened.
func TestDirectoryAssemblerIdentityErrorFailsStartup(t *testing.T) {
	for name, identityErr := range map[string]error{
		"no identity": directory.ErrNoIdentity,
		"other":       errors.New("identity unavailable"),
	} {
		t.Run(name, func(t *testing.T) {
			openCalled := false
			a := &directoryAssembler{
				open: func(directory.Deps) (directoryHandle, error) {
					openCalled = true
					return &discoveryDir{}, nil
				},
				newIdentity: func(*pairing.Pairing) (directory.Identity, error) {
					return nil, identityErr
				},
			}
			deps := &Deps{}
			asm := &Assembly{Pairing: &pairing.Pairing{}, Link: &link.Link{}}

			comps, err := a.Assemble(context.Background(), nil, deps, asm)
			if err == nil {
				t.Fatal("Assemble with an identity error returned nil")
			}
			if !errors.Is(err, identityErr) {
				t.Errorf("error = %v, want it to wrap %v", err, identityErr)
			}
			if !strings.HasPrefix(err.Error(), "directory identity:") {
				t.Errorf("error = %q, want the prefix %q", err, "directory identity:")
			}
			if comps != nil {
				t.Errorf("Assemble returned %d components, want none", len(comps))
			}
			if deps.Directory != nil {
				t.Error("Assemble installed a directory after an identity error")
			}
			if openCalled {
				t.Error("Assemble opened the directory despite the identity error")
			}
		})
	}
}

// TestDirectoryAssemblerOnlyWrapsDataFileErrors checks that only a
// directory.json read failure is marked data-unavailable.
func TestDirectoryAssemblerOnlyWrapsDataFileErrors(t *testing.T) {
	tb := newTestBroker(t)
	asm := &Assembly{Pairing: &pairing.Pairing{}, Link: &link.Link{}}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"unreadable directory file", &fs.PathError{Op: "open", Path: "/data/directory.json", Err: errors.New("permission denied")}, true},
		{"links is required", errors.New("directory: links is required"), false},
		{"no data directory", errors.New("directory: config has no data directory"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := &directoryAssembler{
				open:        func(directory.Deps) (directoryHandle, error) { return nil, tc.err },
				newIdentity: func(*pairing.Pairing) (directory.Identity, error) { return nil, nil },
			}
			_, err := a.Assemble(context.Background(), tb.cfg, &Deps{}, asm)
			if err == nil {
				t.Fatal("Assemble succeeded, want the open failure")
			}
			if got := errors.Is(err, config.ErrDataUnavailable); got != tc.want {
				t.Fatalf("errors.Is(err, ErrDataUnavailable) = %v, want %v (error: %v)", got, tc.want, err)
			}
			if !strings.Contains(err.Error(), "opening the directory") {
				t.Errorf("error %q no longer names the directory open step", err)
			}
		})
	}
}

// TestDirectoryComponentStartOrder checks that Start starts the directory then
// applies broker.public, subscribing to broker.public and not broker.alias.
func TestDirectoryComponentStartOrder(t *testing.T) {
	tb := newTestBroker(t)
	tb.cfg.setValue("broker.public", config.Value{Kind: config.KindBool, Bool: true})
	d := &discoveryDir{}
	c := &directoryComponent{d: d, a: newDirectoryAdapter(d), cfg: tb.cfg}

	if err := c.startWith(func() map[string]string { return nil }, discardLogger()); err != nil {
		t.Fatalf("startWith: %v", err)
	}
	want := []string{"Start", "SetPublic(true)"}
	if got := d.recordedCalls(); !slices.Equal(got, want) {
		t.Fatalf("calls = %v, want %v", got, want)
	}

	tb.cfg.mu.Lock()
	keys := make(map[string]bool, len(tb.cfg.subscribers))
	for key := range tb.cfg.subscribers {
		keys[key] = true
	}
	tb.cfg.mu.Unlock()
	if !keys["broker.public"] {
		t.Error("the component did not subscribe to broker.public")
	}
	if keys["broker.alias"] {
		t.Error("the component subscribed to broker.alias; the configuration relay owns that key")
	}
}

// TestDirectoryComponentPublicChange checks that a broker.public change declares
// the joined topics.
func TestDirectoryComponentPublicChange(t *testing.T) {
	tb := newTestBroker(t)
	d := &discoveryDir{}
	c := &directoryComponent{d: d, a: newDirectoryAdapter(d), cfg: tb.cfg}
	topics := func() map[string]string { return map[string]string{"x": "hx"} }

	if err := c.startWith(topics, discardLogger()); err != nil {
		t.Fatalf("startWith: %v", err)
	}
	tb.cfg.setValue("broker.public", config.Value{Kind: config.KindBool, Bool: true})

	want := []string{"Start", "SetPublic(true)", "Declare(x,hx)"}
	if got := d.recordedCalls(); !slices.Equal(got, want) {
		t.Fatalf("calls = %v, want %v", got, want)
	}
}

// TestDirectoryComponentPublicChangeUsesCurrentValue checks that a queued stale
// notification applies the current broker.public value.
func TestDirectoryComponentPublicChangeUsesCurrentValue(t *testing.T) {
	tb := newTestBroker(t)
	tb.cfg.setValue("broker.public", config.Value{Kind: config.KindBool, Bool: true})
	d := &discoveryDir{}
	c := &directoryComponent{d: d, a: newDirectoryAdapter(d), cfg: tb.cfg}
	if err := c.startWith(func() map[string]string { return nil }, discardLogger()); err != nil {
		t.Fatalf("startWith: %v", err)
	}

	tb.cfg.setValue("broker.public", config.Value{Kind: config.KindBool, Bool: false})
	tb.cfg.fireKey("broker.public",
		config.Value{Kind: config.KindBool, Bool: false},
		config.Value{Kind: config.KindBool, Bool: true})

	if d.public {
		t.Error("the stale notification made the directory public again")
	}
	calls := d.recordedCalls()
	if len(calls) == 0 || calls[len(calls)-1] != "SetPublic(false)" {
		t.Errorf("calls = %v, want the last call to apply the current value SetPublic(false)", calls)
	}
}

// TestDirectoryComponentClosedSetPublicLogsDebug checks that a SetPublic during
// shutdown is logged at debug, not warn.
func TestDirectoryComponentClosedSetPublicLogsDebug(t *testing.T) {
	tb := newTestBroker(t)
	tb.cfg.setValue("broker.public", config.Value{Kind: config.KindBool, Bool: true})
	d := &discoveryDir{setPublicErr: directory.ErrClosed}
	c := &directoryComponent{d: d, a: newDirectoryAdapter(d), cfg: tb.cfg}
	logger, buf := relayTestLogger(t)

	if err := c.startWith(func() map[string]string { return nil }, logger); err != nil {
		t.Fatalf("startWith: %v", err)
	}
	assertDebugNotWarn(t, buf, "set_public_closed")
}

// TestDirectoryComponentOnCardRelays checks that the registered OnCard callback
// relays a card to the pairing.
func TestDirectoryComponentOnCardRelays(t *testing.T) {
	fp := pairing.Fingerprint{1}
	cp := &discoveryPairing{roles: map[pairing.Fingerprint]pairing.Role{fp: pairing.RoleDialer}}
	dir := &discoveryDir{}
	a := &directoryAssembler{open: func(directory.Deps) (directoryHandle, error) { return dir, nil }}

	if _, err := a.assembleWith(directory.Deps{}, cp, nil, &Deps{}); err != nil {
		t.Fatalf("assembleWith: %v", err)
	}
	callback := dir.cardCallback()
	if callback == nil {
		t.Fatal("assembleWith did not register the OnCard callback")
	}
	addrs := []config.AdvertisedAddr{{Kind: "direct", Addr: "192.0.2.1:7000"}}
	callback(fp, addrs)

	if len(cp.updates) != 1 {
		t.Fatalf("the card relay made %d updates, want 1", len(cp.updates))
	}
	got := cp.updates[0]
	if got.FP != fp {
		t.Errorf("update fingerprint = %v, want %v", got.FP, fp)
	}
	if !slices.Equal(got.Addrs, addrs) {
		t.Errorf("update addresses = %v, want %v", got.Addrs, addrs)
	}
}

type stopProbe struct {
	d            *discoveryDir
	stopped      bool
	closedAtStop bool
}

func (p *stopProbe) Stop() bool {
	p.stopped = true
	for _, call := range p.d.recordedCalls() {
		if call == "Close" {
			p.closedAtStop = true
		}
	}
	return true
}

// TestDirectoryComponentRetiresAfterTwoMinutes checks that AfterReady schedules
// retirement after two minutes and Stop cancels it before closing.
func TestDirectoryComponentRetiresAfterTwoMinutes(t *testing.T) {
	d := &discoveryDir{}
	probe := &stopProbe{d: d}
	var delay time.Duration
	var fire func()
	c := &directoryComponent{d: d, a: newDirectoryAdapter(d), cfg: newFakeConfig(t.TempDir())}
	c.afterFunc = func(dur time.Duration, f func()) interface{ Stop() bool } {
		delay = dur
		fire = f
		return probe
	}

	if err := c.AfterReady(context.Background()); err != nil {
		t.Fatalf("AfterReady: %v", err)
	}
	if delay != 2*time.Minute {
		t.Fatalf("retirement delay = %v, want %v", delay, 2*time.Minute)
	}
	if fire == nil {
		t.Fatal("AfterReady did not schedule a callback")
	}
	fire()
	if n := countDirCalls(d.recordedCalls(), "RetireInactive"); n != 1 {
		t.Fatalf("RetireInactiveTopics called %d times, want 1", n)
	}

	c.Stop()
	if !probe.stopped {
		t.Error("Stop did not cancel the retirement timer")
	}
	if probe.closedAtStop {
		t.Error("the retirement timer was stopped after the directory closed")
	}
	calls := d.recordedCalls()
	if len(calls) == 0 || calls[len(calls)-1] != "Close" {
		t.Errorf("calls = %v, want the last call to be Close", calls)
	}
}

// TestDefaultAssemblersPlacesDirectory checks that the directory assembler runs
// right after topic and before delivery and config.
func TestDefaultAssemblersPlacesDirectory(t *testing.T) {
	names := make([]string, 0)
	for _, a := range DefaultAssemblers() {
		names = append(names, a.Name())
	}
	at := slices.Index(names, "directory")
	if at < 0 {
		t.Fatalf("DefaultAssemblers = %v, want a directory assembler", names)
	}
	if at == 0 || names[at-1] != "topic" {
		t.Errorf("DefaultAssemblers = %v, want directory right after topic", names)
	}
	for _, later := range []string{"delivery", "config"} {
		if i := slices.Index(names, later); i >= 0 && i < at {
			t.Errorf("DefaultAssemblers = %v, want directory before %s", names, later)
		}
	}
}

func countDirCalls(calls []string, want string) int {
	n := 0
	for _, call := range calls {
		if call == want {
			n++
		}
	}
	return n
}
