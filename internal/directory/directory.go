package directory

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/directory/directorypb"
)

// Directory is the Broker's directory: the signed records it holds, the set of
// peers whose links are established, and the reconcile state. Open returns an
// instance; there is no package-level singleton, so one process can run several
// directories in tests.
type Directory struct {
	mu                  sync.RWMutex
	st                  *store
	up                  map[Fingerprint]bool
	recon               map[Fingerprint]*peerReconcile // requester-side state per peer
	public              bool
	active              map[string]string // public topic name to description
	reissuePending      bool              // a Reissue could not be signed; renewDue retries it
	onCard              []func(Fingerprint, []config.AdvertisedAddr)
	ownHook             func(ids []recordID) // called with own records accepted from a link
	dirty               bool
	lastMaintainPersist time.Time // last maintenance pass allowed to write; guarded by mu
	persistHook         func()    // test seam called between taking the snapshot and writing it
	afterMaintain       func()    // test seam called at the end of each maintenance pass
	refetchIdle         func()    // test seam called when a refetch loop finds no deferred identifier
	started, closed     bool

	persistMu  sync.Mutex
	inflight   sync.WaitGroup  // fetch goroutines
	refetching sync.WaitGroup  // deferred-refetch waiters
	ctx        context.Context // canceled by Close so blocked requests return
	cancel     context.CancelFunc
	stop       chan struct{}
	loopDone   chan struct{}

	id       Identity
	peers    PeerTable
	links    Links
	settings Settings
	clk      clock
	ops      fileOps
	log      *slog.Logger
	dataDir  string
}

// Open returns a Directory backed by deps. It loads directory.json but does not
// contact any link; Start registers the frame handlers and subscriptions.
func Open(deps Deps) (*Directory, error) {
	return openWith(deps, nil, realFileOps{})
}

// openWith is the test entry point. A nil clk means realClock{now: deps.Now}; a
// nil ops means realFileOps{}.
func openWith(deps Deps, clk clock, ops fileOps) (*Directory, error) {
	if deps.Identity == nil {
		return nil, errors.New("cannot open the directory: an identity is required")
	}
	if deps.Peers == nil {
		return nil, errors.New("cannot open the directory: peers are required")
	}
	if deps.Links == nil {
		return nil, errors.New("cannot open the directory: links are required")
	}
	fp := deps.Identity.Fingerprint()
	if fp.IsZero() {
		return nil, ErrNoIdentity
	}

	settings := deps.Settings
	if settings == nil {
		settings = configSettings{}
	}
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if clk == nil {
		clk = realClock{now: deps.Now}
	}
	if ops == nil {
		ops = realFileOps{}
	}

	dataDir, err := settings.DataDir()
	if err != nil {
		return nil, fmt.Errorf("cannot resolve the data directory: %w", err)
	}

	frs, highest, err := loadFile(ops, dataDir)
	if err != nil {
		if !errors.Is(err, errUnreadable) {
			return nil, fmt.Errorf("cannot load the directory file: %w", err)
		}
		logger.Error("directory file cannot be parsed; starting with an empty directory", "error", err)
		frs, highest = nil, 0
	}

	recs, dropped := fromFileRecords(frs)
	ctx, cancel := context.WithCancel(context.Background())
	d := &Directory{
		st:       newStore(fp),
		up:       make(map[Fingerprint]bool),
		recon:    make(map[Fingerprint]*peerReconcile),
		active:   make(map[string]string),
		ctx:      ctx,
		cancel:   cancel,
		stop:     make(chan struct{}),
		loopDone: make(chan struct{}),
		id:       deps.Identity,
		peers:    deps.Peers,
		links:    deps.Links,
		settings: settings,
		clk:      clk,
		ops:      ops,
		log:      logger,
		dataDir:  dataDir,
	}

	dropped += d.st.load(recs, highest, d.now())
	if dropped > 0 {
		logger.Warn("dropped invalid records while loading the directory", "count", dropped)
		d.dirty = true
	}
	return d, nil
}

// Close stops the maintenance loop when it is running, waits for in-flight
// fetch goroutines and deferred-refetch waiters, and persists pending changes.
// Every frame handler and link
// notification returns immediately once Close has been called. Calling Close
// again is a no-op.
func (d *Directory) Close() {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	d.closed = true
	started := d.started
	d.mu.Unlock()

	// Cancel in-flight record requests before waiting for them, so Close does
	// not have to wait for their 30-second deadline.
	d.cancel()

	if started {
		close(d.stop)
		<-d.loopDone
	}
	d.inflight.Wait()
	d.refetching.Wait()
	d.persistIfDirty()
}

// trackLinks subscribes to link notifications: an established link joins the up
// set and receives a full RecordList, a torn-down link leaves the set.
func (d *Directory) trackLinks() {
	d.links.Subscribe(
		func(peer Fingerprint, _ []byte) {
			d.mu.Lock()
			if d.closed {
				d.mu.Unlock()
				return
			}
			d.up[peer] = true
			d.mu.Unlock()
			d.sendFullList(peer)
		},
		func(peer Fingerprint) {
			d.mu.Lock()
			if d.closed {
				d.mu.Unlock()
				return
			}
			delete(d.up, peer)
			// The pending set and the accept window belong to the link: a
			// reconnected peer starts from nothing, and an accept window left
			// behind must not legitimize records on the next link. Waking a
			// deferred-refetch waiter before dropping its state makes it leave
			// now instead of at its deadline, so a reconnected peer never has
			// an old waiter and a new one at the same time.
			if pr := d.recon[peer]; pr != nil && pr.refetchRunning {
				select {
				case pr.refetchWake <- struct{}{}:
				default:
				}
			}
			delete(d.recon, peer)
			d.mu.Unlock()
		},
	)
}

// sendFullList sends every unexpired record's identifier and version to peer.
func (d *Directory) sendFullList(peer Fingerprint) {
	d.mu.RLock()
	if d.closed {
		d.mu.RUnlock()
		return
	}
	vs := d.st.versions(d.now())
	d.mu.RUnlock()

	d.sendList(peer, vs)
}

// broadcast sends an incremental RecordList holding vs to every established
// peer except the source link. A zero except sends to every established peer.
// The up set is snapshotted under the lock; the frames are sent outside it.
func (d *Directory) broadcast(vs []*directorypb.RecordVersion, except Fingerprint) {
	if len(vs) == 0 {
		return
	}

	d.mu.RLock()
	if d.closed {
		d.mu.RUnlock()
		return
	}
	peers := make([]Fingerprint, 0, len(d.up))
	for peer := range d.up {
		if except.IsZero() || peer != except {
			peers = append(peers, peer)
		}
	}
	d.mu.RUnlock()

	sort.Slice(peers, func(i, j int) bool {
		return bytes.Compare(peers[i][:], peers[j][:]) < 0
	})

	payload, ok := d.marshalRecordList(vs)
	if !ok {
		return
	}
	for _, peer := range peers {
		d.sendRecordListPayload(peer, payload)
	}
}

// sendList sends one RecordList frame to peer.
func (d *Directory) sendList(peer Fingerprint, vs []*directorypb.RecordVersion) {
	payload, ok := d.marshalRecordList(vs)
	if !ok {
		return
	}
	d.sendRecordListPayload(peer, payload)
}

// marshalRecordList encodes vs as a RecordList payload. It logs and reports
// false when the frame cannot be encoded.
func (d *Directory) marshalRecordList(vs []*directorypb.RecordVersion) ([]byte, bool) {
	payload, err := proto.Marshal(&directorypb.RecordList{Records: vs})
	if err != nil {
		d.log.Error("encode record list", "error", err)
		return nil, false
	}
	return payload, true
}

// sendRecordListPayload sends an encoded RecordList frame to peer. A send on a
// link that just went down is ignored with a debug log.
func (d *Directory) sendRecordListPayload(peer Fingerprint, payload []byte) {
	if err := d.links.Send(peer, directorypb.KindRecordList, payload); err != nil {
		d.log.Debug("send record list failed", "peer", peer.String(), "error", err)
	}
}

// linkUp reports whether peer's link is established and the directory is open.
func (d *Directory) linkUp(peer Fingerprint) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return !d.closed && d.up[peer]
}

// afterPersistLock is a test seam invoked each time persistIfDirty holds
// persistMu, before it inspects the dirty flag. Tests use it to observe when a
// caller reaches the lock. It is nil in production, which costs one nil check.
var afterPersistLock func()

// persistIfDirty writes the directory file when a change is pending. It takes
// persistMu first, so concurrent writers serialize before either takes a
// snapshot, then takes the snapshot and clears the dirty flag under mu, and
// holds persistMu until saveFile returns. Close therefore waits for an
// in-flight write and then writes any change made while that write ran. The
// lock order is persistMu before mu. A failed write logs an error and leaves
// the dirty flag set so the next change or maintenance pass retries.
func (d *Directory) persistIfDirty() {
	d.persistMu.Lock()
	defer d.persistMu.Unlock()

	if afterPersistLock != nil {
		afterPersistLock()
	}

	d.mu.Lock()
	if !d.dirty {
		d.mu.Unlock()
		return
	}
	recs := toFileRecords(d.st.all())
	highest := d.st.highestIssued
	d.dirty = false
	d.mu.Unlock()

	if d.persistHook != nil {
		d.persistHook()
	}

	if err := saveFile(d.ops, d.dataDir, highest, recs); err != nil {
		d.mu.Lock()
		d.dirty = true
		d.mu.Unlock()
		d.log.Error("persist directory file failed", "error", err)
	}
}

// commitOwn persists a pending change and broadcasts freshly signed local
// versions to every established peer. The caller must have released d.mu.
func (d *Directory) commitOwn(vs []*directorypb.RecordVersion) {
	d.persistIfDirty()
	d.broadcast(vs, Fingerprint{})
}

// persistDue reports whether a maintenance pass may write the directory now. It
// gives the maintenance loop at most one write per maintainInterval even when
// directory.reconcile_interval makes the loop wake more often. A local issue, a
// revocation and Close write without this gate, so a busy local issuer cannot
// starve the write of records accepted from a link.
func (d *Directory) persistDue(now time.Time) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.lastMaintainPersist.IsZero() || now.Sub(d.lastMaintainPersist) >= maintainInterval
}

// noteMaintainPersist records that a maintenance pass used its write slot.
func (d *Directory) noteMaintainPersist(now time.Time) {
	d.mu.Lock()
	if now.After(d.lastMaintainPersist) {
		d.lastMaintainPersist = now
	}
	d.mu.Unlock()
}

// limits reads the three record count limits from the settings.
func (d *Directory) limits() limits {
	return limits{
		maxRecords:            d.settings.MaxRecords(),
		maxPerLink:            d.settings.MaxRecordsPerLink(),
		maxTopicsPerPublisher: d.settings.MaxTopicsPerPublisher(),
	}
}

// now returns the directory's current instant.
func (d *Directory) now() time.Time { return d.clk.Now() }
