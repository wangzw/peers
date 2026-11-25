package broker

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/pairing"
)

type assembleTest struct {
	t      *testing.T
	cfg    *fakeConfig
	deps   *Deps
	asm    *Assembly
	store  *fakePairing
	links  *fakePeerLinks
	runner *fakeRunner
	order  *orderLog
	a      *peerAssembler

	transport  pairing.Transport // what the assembler handed openPairing
	concrete   *pairing.Pairing
	linkPtr    *link.Link // what newLink returned
	openErr    error
	newLinkRan bool
	onNewLink  func(link.Options)
}

func newAssembleTest(t *testing.T) *assembleTest {
	t.Helper()
	level := new(slog.LevelVar)
	level.Set(slog.LevelInfo)
	cfg := newFakeConfig(t.TempDir())
	order := &orderLog{}
	at := &assembleTest{
		t:        t,
		cfg:      cfg,
		asm:      &Assembly{},
		store:    &fakePairing{},
		links:    &fakePeerLinks{status: map[pairing.Fingerprint]PeerLinkStatus{}},
		runner:   newFakeRunner(order),
		order:    order,
		concrete: new(pairing.Pairing),
		linkPtr:  new(link.Link),
	}
	at.deps = &Deps{Config: cfg, Logger: NewLogger(&syncBuffer{}, level)}
	at.a = &peerAssembler{
		openPairing: func(tr pairing.Transport) (PeerStore, *pairing.Pairing, error) {
			at.transport = tr
			if at.openErr != nil {
				return nil, nil, at.openErr
			}
			return at.store, at.concrete, nil
		},
		newLink: func(opts link.Options) (*link.Link, error) {
			at.newLinkRan = true
			if at.onNewLink != nil {
				at.onNewLink(opts)
			}
			return at.linkPtr, nil
		},
		adapt: func(*link.Link) (PeerLinks, LinkRunner) {
			return at.links, at.runner
		},
	}
	return at
}

func (at *assembleTest) assemble() ([]Component, error) {
	at.t.Helper()
	return at.a.Assemble(context.Background(), at.cfg, at.deps, at.asm)
}

// TestPeerAssemblerInjectsTransportBeforeLinkExists checks that pairing dials
// fail before link is bound and reach link's adapter afterwards.
func TestPeerAssemblerInjectsTransportBeforeLinkExists(t *testing.T) {
	at := newAssembleTest(t)
	at.links.pairDialFn = func(_ context.Context, address string, _ func(pairing.PairConn) error) error {
		at.links.mu.Lock()
		at.links.calls = append(at.links.calls, "PairDial "+address)
		at.links.mu.Unlock()
		return nil
	}
	var (
		beforeSet error
		runCalled bool
	)
	at.onNewLink = func(link.Options) {
		beforeSet = at.transport.PairDial(context.Background(), "h:1", func(pairing.PairConn) error {
			runCalled = true
			return nil
		})
	}

	if _, err := at.assemble(); err != nil {
		t.Fatalf("Assemble: %v", err)
	}
	if !errors.Is(beforeSet, pairing.ErrDialFailed) {
		t.Fatalf("PairDial before link exists = %v, want ErrDialFailed", beforeSet)
	}
	if runCalled {
		t.Fatal("the pairing callback ran before link existed")
	}

	if err := at.transport.PairDial(context.Background(), "h:2", func(pairing.PairConn) error { return nil }); err != nil {
		t.Fatalf("PairDial after link exists: %v", err)
	}
	at.transport.Drop(fp(4))

	got := at.links.calls
	want := []string{"PairDial h:2", "Drop 04040404"}
	if len(got) != len(want) {
		t.Fatalf("link calls = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("link calls = %v, want %v", got, want)
		}
	}
}

// TestPeerAssemblerFillsDepsAndAssembly checks that the assembler fills every
// Deps and Assembly field and starts nothing.
func TestPeerAssemblerFillsDepsAndAssembly(t *testing.T) {
	at := newAssembleTest(t)
	comps, err := at.assemble()
	if err != nil {
		t.Fatalf("Assemble: %v", err)
	}
	if len(comps) != 0 {
		t.Fatalf("components = %d, want none", len(comps))
	}
	if at.deps.Pairing != at.store {
		t.Errorf("Deps.Pairing = %v, want the opened store", at.deps.Pairing)
	}
	if at.deps.PeerLinks != at.links {
		t.Errorf("Deps.PeerLinks = %v, want the adapted links", at.deps.PeerLinks)
	}
	if at.deps.Peers == nil {
		t.Error("Deps.Peers is nil")
	}
	if at.asm.Pairing != at.concrete {
		t.Errorf("Assembly.Pairing = %v, want the opened pairing", at.asm.Pairing)
	}
	if at.asm.Link != at.linkPtr {
		t.Errorf("Assembly.Link = %v, want the link newLink returned", at.asm.Link)
	}
	if at.asm.Runner != at.runner {
		t.Errorf("Assembly.Runner = %v, want the adapted runner", at.asm.Runner)
	}
	if calls := at.order.list(); len(calls) != 0 {
		t.Fatalf("runner was called during assembly: %v", calls)
	}
}

// TestPeerAssemblerAliasWiredBeforeLink checks that broker.alias is cached and
// its conflict check registered before link is built.
func TestPeerAssemblerAliasWiredBeforeLink(t *testing.T) {
	at := newAssembleTest(t)
	var (
		aliasAtLink string
		checks      int
	)
	at.onNewLink = func(link.Options) {
		at.store.mu.Lock()
		aliasAtLink = at.store.ownAlias
		at.store.mu.Unlock()
		at.cfg.mu.Lock()
		checks = len(at.cfg.validators["broker.alias"])
		at.cfg.mu.Unlock()
	}

	if _, err := at.assemble(); err != nil {
		t.Fatalf("Assemble: %v", err)
	}
	if aliasAtLink != "hostA" {
		t.Fatalf("pairing own alias when link was built = %q, want %q", aliasAtLink, "hostA")
	}
	if checks != 1 {
		t.Fatalf("broker.alias validators when link was built = %d, want 1", checks)
	}
}

// TestPeerAssemblerPassesProtocolVersion checks that Run's link protocol version
// reaches link unchanged, including zero.
func TestPeerAssemblerPassesProtocolVersion(t *testing.T) {
	for _, tt := range []struct {
		name    string
		version uint32
	}{
		{"explicit version", 9},
		{"default version", 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			at := newAssembleTest(t)
			at.asm.LinkProtocolVersion = tt.version
			var got link.Options
			at.onNewLink = func(opts link.Options) { got = opts }

			if _, err := at.assemble(); err != nil {
				t.Fatalf("Assemble: %v", err)
			}
			if got.ProtocolVersion != tt.version {
				t.Fatalf("ProtocolVersion = %d, want %d", got.ProtocolVersion, tt.version)
			}
			if got.Peers != at.concrete {
				t.Fatalf("link Peers = %v, want the opened pairing", got.Peers)
			}
			if got.Settings == nil {
				t.Error("link Settings is nil")
			}
			if got.Clock == nil {
				t.Error("link Clock is nil")
			}
			if got.Logger != at.deps.Logger {
				t.Error("link Logger is not the assembler's logger")
			}
		})
	}
}

// TestPeerAssemblerCatalogSetLater checks that the peer view reads the catalog on
// each call, so a later assembler can supply it.
func TestPeerAssemblerCatalogSetLater(t *testing.T) {
	at := newAssembleTest(t)
	at.store.peers = []pairing.Peer{{Fingerprint: fp(2), Alias: "hostB", Role: pairing.RoleAcceptor}}
	if _, err := at.assemble(); err != nil {
		t.Fatalf("Assemble: %v", err)
	}

	at.deps.Catalog = &fakeCatalog{cards: map[pairing.Fingerprint][]config.AdvertisedAddr{
		fp(2): {{Kind: config.AdvertisedKindDirect, Addr: "198.51.100.4:17443"}},
	}}

	infos := at.deps.Peers.Peers(context.Background())
	if len(infos) != 1 {
		t.Fatalf("peers = %d, want 1", len(infos))
	}
	addrs := infos[0].GetAdvertisedAddresses()
	if len(addrs) != 1 || addrs[0].GetAddr() != "198.51.100.4:17443" {
		t.Fatalf("advertised addresses = %v, want the card's direct address", addrs)
	}
}

// TestPeerAssemblerOpenFailure checks that a failure to open pairing names the
// step and stops before link is created.
func TestPeerAssemblerOpenFailure(t *testing.T) {
	at := newAssembleTest(t)
	at.openErr = pairing.ErrIdentity

	_, err := at.assemble()
	if err == nil {
		t.Fatal("Assemble succeeded, want an error")
	}
	if !strings.Contains(err.Error(), "loading identity and peer table") {
		t.Fatalf("error = %q, want it to name loading identity and peer table", err)
	}
	if !errors.Is(err, pairing.ErrIdentity) {
		t.Fatalf("error = %v, want it to wrap ErrIdentity", err)
	}
	if at.newLinkRan {
		t.Fatal("link was created after pairing failed to open")
	}
}

// TestPeerAssemblerAliasSubscribedFirst checks that pairing's broker.alias
// subscription is registered before a later subscriber.
func TestPeerAssemblerAliasSubscribedFirst(t *testing.T) {
	at := newAssembleTest(t)
	if _, err := at.assemble(); err != nil {
		t.Fatalf("Assemble: %v", err)
	}

	var (
		laterRan   bool
		laterAlias string
	)
	at.cfg.Subscribe("broker.alias", func(_, new config.Value) {
		laterRan = true
		at.store.mu.Lock()
		laterAlias = at.store.ownAlias
		at.store.mu.Unlock()
	})

	at.cfg.setValue("broker.alias", config.Value{Kind: config.KindString, String: "hostY"})
	if !laterRan {
		t.Fatal("the later subscriber did not run")
	}
	if laterAlias != "hostY" {
		t.Fatalf("pairing own alias when the later subscriber ran = %q, want %q", laterAlias, "hostY")
	}
	at.store.mu.Lock()
	own := at.store.ownAlias
	at.store.mu.Unlock()
	if own != "hostY" {
		t.Fatalf("pairing own alias = %q, want %q", own, "hostY")
	}
}

// TestDefaultAssemblersStartsWithPeers checks that the peer assembler is the
// first production assembler.
func TestDefaultAssemblersStartsWithPeers(t *testing.T) {
	as := DefaultAssemblers()
	if len(as) == 0 {
		t.Fatal("DefaultAssemblers is empty")
	}
	if got := as[0].Name(); got != "pairing and link" {
		t.Fatalf("first assembler = %q, want %q", got, "pairing and link")
	}
}
