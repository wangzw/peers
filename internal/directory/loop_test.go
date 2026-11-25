package directory

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/directory/directorypb"
)

type loopFixture struct {
	t   *testing.T
	d   *Directory
	ep  *fakeLinks
	clk *fakeClock
	set *fakeSettings
	fp  Fingerprint
	dir string
}

func newLoopFixture(t *testing.T, seed byte, dir string, public bool, net *fakeNet, at time.Time) *loopFixture {
	t.Helper()
	return newLoopFixtureWithOps(t, seed, dir, public, net, at, realFileOps{})
}

func newLoopFixtureWithOps(t *testing.T, seed byte, dir string, public bool, net *fakeNet, at time.Time, ops fileOps) *loopFixture {
	t.Helper()
	id := newTestIdentity(seed)
	set := newFakeSettings(dir)
	set.SetPublic(public)
	set.SetAlias("hostA")
	ep := net.Endpoint(id.Fingerprint())
	logger, _ := newLogBuffer()
	clk := newFakeClock(at)
	d, err := openWith(Deps{
		Identity: id,
		Peers:    newFakePeers(),
		Links:    ep,
		Settings: set,
		Logger:   logger,
	}, clk, ops)
	if err != nil {
		t.Fatalf("openWith: %v", err)
	}
	t.Cleanup(d.Close)
	return &loopFixture{t: t, d: d, ep: ep, clk: clk, set: set, fp: id.Fingerprint(), dir: dir}
}

func (f *loopFixture) start() {
	f.t.Helper()
	if err := f.d.Start(); err != nil {
		f.t.Fatalf("Start: %v", err)
	}
	f.clk.WaitTimers(f.t, 1)
}

func (f *loopFixture) card() *storedRecord {
	f.t.Helper()
	r, ok := loopOwnCard(f)
	if !ok {
		f.t.Fatal("the directory holds no local card")
	}
	return r
}

func loopOwnCard(f *loopFixture) (*storedRecord, bool) {
	f.d.mu.RLock()
	defer f.d.mu.RUnlock()
	return f.d.st.get(recordID{publisher: f.fp, kind: recKindCard})
}

func loopPeerCard(f *loopFixture, fp Fingerprint) (*storedRecord, bool) {
	f.d.mu.RLock()
	defer f.d.mu.RUnlock()
	return f.d.st.get(recordID{publisher: fp, kind: recKindCard})
}

func loopLinked(f *loopFixture, peer Fingerprint) bool {
	f.d.mu.RLock()
	defer f.d.mu.RUnlock()
	return f.d.up[peer]
}

func loopWait(t *testing.T, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for !cond() {
		if !time.Now().Before(deadline) {
			t.Fatalf("timed out waiting for %s", desc)
		}
		time.Sleep(time.Millisecond)
	}
}

func loopWaitMaintained(t *testing.T, ch <-chan struct{}, desc string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(eventTimeout):
		t.Fatalf("timed out waiting for %s", desc)
	}
}

func loopCopyFile(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read %s: %v", src, err)
	}
	if err := os.WriteFile(dst, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", dst, err)
	}
}

type loopFailFirstWriteOps struct {
	realFileOps
	mu     sync.Mutex
	failed bool
}

func (o *loopFailFirstWriteOps) WriteFile(path string, data []byte, mode os.FileMode) error {
	o.mu.Lock()
	first := !o.failed
	if first {
		o.failed = true
	}
	o.mu.Unlock()
	if first {
		return errors.New("injected write failure")
	}
	return o.realFileOps.WriteFile(path, data, mode)
}

// TestStartRegistersAndChecks checks that Start registers the frame handlers and issues the card of a public Broker.
func TestStartRegistersAndChecks(t *testing.T) {
	f := newLoopFixture(t, 1, t.TempDir(), true, newFakeNet(), dirTestNow())
	f.start()

	f.ep.mu.Lock()
	_, hasList := f.ep.stateHandlers[directorypb.KindRecordList]
	_, hasRequest := f.ep.requestHandlers[directorypb.KindRecordRequest]
	f.ep.mu.Unlock()
	if !hasList {
		t.Fatal("Start did not register the RecordList state handler")
	}
	if !hasRequest {
		t.Fatal("Start did not register the RecordRequest request handler")
	}

	if _, ok := loopOwnCard(f); !ok {
		t.Fatal("a public directory has no card after Start")
	}
}

// TestStartTwiceAndAfterClose checks that a second Start changes nothing and Start after Close fails with ErrClosed.
func TestStartTwiceAndAfterClose(t *testing.T) {
	f := newLoopFixture(t, 1, t.TempDir(), true, newFakeNet(), dirTestNow())
	f.start()
	version := f.card().version

	if err := f.d.Start(); err != nil {
		t.Fatalf("second Start = %v, want nil", err)
	}
	if got := f.card().version; got != version {
		t.Fatalf("second Start issued a new card (version %d, want %d)", got, version)
	}

	f.d.Close()
	if err := f.d.Start(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Start after Close = %v, want ErrClosed", err)
	}
}

// TestPeriodicReconcile checks that reconciliation runs once the interval elapses and follows a shortened interval.
func TestPeriodicReconcile(t *testing.T) {
	cases := []struct {
		name     string
		interval time.Duration
		advance  time.Duration
	}{
		{name: "ten minutes", interval: 10 * time.Minute, advance: 10 * time.Minute},
		{name: "thirty seconds", interval: 30 * time.Second, advance: 30 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			net := newFakeNet()
			a := newLoopFixture(t, 1, t.TempDir(), false, net, dirTestNow())
			b := newLoopFixture(t, 2, t.TempDir(), false, net, dirTestNow())
			a.set.SetReconcileInterval(tc.interval)
			b.set.SetReconcileInterval(tc.interval)
			a.start()
			b.start()

			net.Connect(a.fp, b.fp)
			reconcileAwait(t, net, "the link to come up", func() bool {
				return loopLinked(a, b.fp) && loopLinked(b, a.fp)
			})
			clearReconcileSent(a.ep)

			a.clk.Advance(tc.advance)
			reconcileAwait(t, net, "a periodic reconcile", func() bool {
				return reconcileFrames(a.ep, b.fp, directorypb.KindRecordList) >= 1
			})
		})
	}

	t.Run("interval shortened at runtime", func(t *testing.T) {
		net := newFakeNet()
		a := newLoopFixture(t, 1, t.TempDir(), false, net, dirTestNow())
		b := newLoopFixture(t, 2, t.TempDir(), false, net, dirTestNow())
		a.set.SetReconcileInterval(10 * time.Minute)
		b.set.SetReconcileInterval(10 * time.Minute)

		maintained := make(chan struct{}, 1)
		a.d.afterMaintain = func() {
			select {
			case maintained <- struct{}{}:
			default:
			}
		}
		a.start()
		b.start()

		net.Connect(a.fp, b.fp)
		reconcileAwait(t, net, "the link to come up", func() bool {
			return loopLinked(a, b.fp) && loopLinked(b, a.fp)
		})
		clearReconcileSent(a.ep)

		a.clk.Advance(time.Minute)
		loopWaitMaintained(t, maintained, "the first maintenance pass")
		a.clk.WaitTimers(t, 1)
		if got := reconcileFrames(a.ep, b.fp, directorypb.KindRecordList); got != 0 {
			t.Fatalf("A reconciled %d times before the interval elapsed, want 0", got)
		}

		a.set.SetReconcileInterval(30 * time.Second)
		a.clk.Advance(time.Minute)
		loopWaitMaintained(t, maintained, "the second maintenance pass")
		reconcileAwait(t, net, "a reconcile after the interval shrank", func() bool {
			return reconcileFrames(a.ep, b.fp, directorypb.KindRecordList) >= 1
		})
	})
}

// TestPersistTimerIndependentOfReconcileInterval checks that the loop still writes at the persist deadline when the reconcile interval is shorter.
func TestPersistTimerIndependentOfReconcileInterval(t *testing.T) {
	f := newLoopFixture(t, 1, t.TempDir(), false, newFakeNet(), dirTestNow())
	f.set.SetReconcileInterval(50 * time.Second)

	var writes int32
	f.d.persistHook = func() { atomic.AddInt32(&writes, 1) }
	maintained := make(chan struct{}, 4)
	f.d.afterMaintain = func() {
		select {
		case maintained <- struct{}{}:
		default:
		}
	}

	f.d.mu.Lock()
	f.d.dirty = true
	f.d.mu.Unlock()
	f.start()

	f.clk.Advance(50 * time.Second)
	loopWaitMaintained(t, maintained, "the first pass")
	if got := atomic.LoadInt32(&writes); got != 1 {
		t.Fatalf("the first pass wrote %d times, want 1", got)
	}
	f.clk.WaitTimers(t, 1)

	f.d.mu.Lock()
	f.d.dirty = true
	f.d.mu.Unlock()
	f.clk.Advance(50 * time.Second)
	loopWaitMaintained(t, maintained, "the reconcile pass at 100 seconds")
	if got := atomic.LoadInt32(&writes); got != 1 {
		t.Fatalf("the reconcile pass wrote %d times, want no write before the persist deadline", got)
	}
	f.clk.WaitTimers(t, 1)

	f.clk.Advance(10 * time.Second)
	loopWaitMaintained(t, maintained, "the persist pass at 110 seconds")
	if got := atomic.LoadInt32(&writes); got != 2 {
		t.Fatalf("the persist deadline wrote %d times in total, want 2", got)
	}
}

// TestLoopRenewsAndPurges checks that the loop renews the local card, purges an expired record and rewrites the file.
func TestLoopRenewsAndPurges(t *testing.T) {
	net := newFakeNet()
	f := newLoopFixture(t, 1, t.TempDir(), true, net, dirTestNow())
	f.start()
	before := f.card().version

	foreign := mustSignRecord(t, 2, recKindCard, "", 1, dirTestNow().Add(time.Minute), recTestCard("foreign"))
	foreign.source = newTestIdentity(3).Fingerprint()
	f.d.mu.Lock()
	outcome := f.d.st.accept(foreign, f.d.now(), f.d.limits())
	f.d.dirty = true
	f.d.mu.Unlock()
	if outcome != outcomeAccepted {
		t.Fatalf("accept = %v, want outcomeAccepted", outcome)
	}

	f.clk.Advance(24 * time.Hour)
	loopWait(t, "the renewed card and the purged record", func() bool {
		f.d.mu.RLock()
		_, present := f.d.st.get(foreign.id)
		cur, ok := f.d.st.get(recordID{publisher: f.fp, kind: recKindCard})
		f.d.mu.RUnlock()
		return ok && !present && cur.version > before
	})

	loopWait(t, "the directory file to be rewritten", func() bool {
		f.d.mu.RLock()
		cur, ok := f.d.st.get(recordID{publisher: f.fp, kind: recKindCard})
		var raw []byte
		if ok {
			raw = append([]byte(nil), cur.raw...)
		}
		f.d.mu.RUnlock()
		if !ok {
			return false
		}
		recs, _, err := loadFile(realFileOps{}, f.dir)
		return err == nil && len(recs) == 1 && bytes.Equal(recs[0].Raw, raw)
	})
}

// TestCloseStopsLoop checks that Close stops the maintenance loop.
func TestCloseStopsLoop(t *testing.T) {
	f := newLoopFixture(t, 1, t.TempDir(), true, newFakeNet(), dirTestNow())
	f.start()
	version := f.card().version

	f.d.Close()
	select {
	case <-f.d.loopDone:
	default:
		t.Fatal("Close returned before the maintenance loop exited")
	}

	f.clk.Advance(24 * time.Hour)
	if got := f.card().version; got != version {
		t.Fatalf("the loop kept maintaining after Close (version %d, want %d)", got, version)
	}
}

// TestPersistRetryInLoop checks that a failed write is retried by a later maintenance pass.
func TestPersistRetryInLoop(t *testing.T) {
	ops := &loopFailFirstWriteOps{}
	f := newLoopFixtureWithOps(t, 1, t.TempDir(), true, newFakeNet(), dirTestNow(), ops)
	f.start()

	if _, err := os.Stat(filepath.Join(f.dir, fileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("directory file exists after the injected first-write failure (err = %v)", err)
	}

	f.clk.Advance(time.Minute)
	loopWait(t, "the retried write", func() bool {
		recs, _, err := loadFile(realFileOps{}, f.dir)
		return err == nil && len(recs) == 1
	})
}

// TestRestartKeepsDirectoryAndVersion checks that the version keeps rising across restarts and a clock rollback.
func TestRestartKeepsDirectoryAndVersion(t *testing.T) {
	dir := t.TempDir()
	net := newFakeNet()

	first := newLoopFixture(t, 1, dir, true, net, dirTestNow())
	first.start()
	firstVersion := first.card().version
	first.d.Close()

	second := newLoopFixture(t, 1, dir, true, net, dirTestNow())
	second.start()
	secondVersion := second.card().version
	if secondVersion <= firstVersion {
		t.Fatalf("card version after a restart = %d, want above %d", secondVersion, firstVersion)
	}
	second.d.Close()

	third := newLoopFixture(t, 1, dir, true, net, dirTestNow().Add(-24*time.Hour))
	third.start()
	thirdVersion := third.card().version
	if thirdVersion <= secondVersion {
		t.Fatalf("card version after a clock rollback = %d, want above %d", thirdVersion, secondVersion)
	}
}

// TestBackupRestoreRaisesVersion checks that a Broker restored from an older backup raises its version above the one already propagated.
func TestBackupRestoreRaisesVersion(t *testing.T) {
	net := newFakeNet()
	dirA := t.TempDir()
	backupDir := t.TempDir()

	b := newLoopFixture(t, 2, t.TempDir(), false, net, dirTestNow())
	b.start()

	a := newLoopFixture(t, 1, dirA, true, net, dirTestNow())
	a.set.SetAdvertised([]config.AdvertisedAddr{{Kind: "direct", Addr: "10.0.0.1:7000"}})
	a.start()
	a.d.Reissue()
	a.d.Reissue()

	loopCopyFile(t, filepath.Join(dirA, fileName), filepath.Join(backupDir, fileName))
	a.d.Reissue()
	a.d.Reissue()

	net.Connect(a.fp, b.fp)
	reconcileAwait(t, net, "B to receive A's card", func() bool {
		_, ok := loopPeerCard(b, a.fp)
		return ok
	})
	heldByB, ok := loopPeerCard(b, a.fp)
	if !ok || heldByB.version == 0 {
		t.Fatal("B holds no card from A")
	}

	net.Disconnect(a.fp, b.fp)
	a.d.Close()

	loopCopyFile(t, filepath.Join(backupDir, fileName), filepath.Join(dirA, fileName))
	restored := newLoopFixture(t, 1, dirA, true, net, dirTestNow())
	restored.set.SetAdvertised([]config.AdvertisedAddr{{Kind: "direct", Addr: "10.0.0.2:7000"}})
	restored.start()

	b.clk.Advance(directoryReplaceInterval())
	net.Connect(restored.fp, b.fp)
	reconcileAwait(t, net, "A to publish a card above B's version", func() bool {
		r, ok := loopPeerCard(b, restored.fp)
		return ok && r.version > heldByB.version
	})

	got, ok := loopPeerCard(b, restored.fp)
	if !ok {
		t.Fatal("B lost A's card")
	}
	card := got.body.GetBrokerCard()
	if card == nil {
		t.Fatal("B holds a record for A that is not a card")
	}
	addrs := card.GetAddresses()
	if len(addrs) != 1 || addrs[0].GetAddr() != "10.0.0.2:7000" {
		t.Fatalf("B holds A's card with addresses %v, want the current 10.0.0.2:7000", addrs)
	}
	if own, ok := loopOwnCard(restored); !ok || own.version <= heldByB.version {
		t.Fatalf("A did not issue a card above B's version %d", heldByB.version)
	}
}
