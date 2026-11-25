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

// Directory is the Broker's directory: the signed records it holds, the
// established peers, and the per-peer reconciliation state.
type Directory struct {
	mu                  sync.RWMutex
	st                  *store
	up                  map[Fingerprint]bool
	recon               map[Fingerprint]*peerReconcile
	public              bool
	active              map[string]string
	reissuePending      bool
	onCard              []func(Fingerprint, []config.AdvertisedAddr)
	ownHook             func(ids []recordID)
	dirty               bool
	lastMaintainPersist time.Time // last maintenance pass allowed to write; guarded by mu
	persistHook         func()    // test seam between taking the snapshot and writing it
	afterMaintain       func()    // test seam at the end of each maintenance pass
	refetchIdle         func()    // test seam when a refetch loop finds no deferred identifier
	started, closed     bool

	persistMu  sync.Mutex
	inflight   sync.WaitGroup
	refetching sync.WaitGroup
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

// Open returns a Directory backed by deps, loading directory.json. It returns an
// error when a dependency is missing, when the identity fingerprint is zero
// (ErrNoIdentity), or when the data directory or the directory file cannot be
// read; a directory file whose content does not parse or whose format is unknown
// is logged and the Directory starts empty. It registers no frame handler and
// does not contact any link; Start does both.
func Open(deps Deps) (*Directory, error) {
	return openWith(deps, nil, realFileOps{})
}

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

// Close stops the maintenance loop, waits for in-flight fetches and pending
// refetches, and persists pending changes. Frame handlers and link
// notifications return without acting once Close has been called, and calling
// Close again is a no-op.
func (d *Directory) Close() {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	d.closed = true
	started := d.started
	d.mu.Unlock()

	// Cancel before waiting so in-flight requests return now rather than at
	// their 30-second deadline.
	d.cancel()

	if started {
		close(d.stop)
		<-d.loopDone
	}
	d.inflight.Wait()
	d.refetching.Wait()
	d.persistIfDirty()
}

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
			// Wake the refetch waiter before dropping the peer state: it
			// leaves now, so a reconnected peer never runs an old and a new
			// waiter together.
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

func (d *Directory) sendList(peer Fingerprint, vs []*directorypb.RecordVersion) {
	payload, ok := d.marshalRecordList(vs)
	if !ok {
		return
	}
	d.sendRecordListPayload(peer, payload)
}

func (d *Directory) marshalRecordList(vs []*directorypb.RecordVersion) ([]byte, bool) {
	payload, err := proto.Marshal(&directorypb.RecordList{Records: vs})
	if err != nil {
		d.log.Error("encode record list", "error", err)
		return nil, false
	}
	return payload, true
}

func (d *Directory) sendRecordListPayload(peer Fingerprint, payload []byte) {
	if err := d.links.Send(peer, directorypb.KindRecordList, payload); err != nil {
		d.log.Debug("send record list failed", "peer", peer.String(), "error", err)
	}
}

func (d *Directory) linkUp(peer Fingerprint) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return !d.closed && d.up[peer]
}

// afterPersistLock is a test seam run while persistIfDirty holds persistMu,
// before it reads the dirty flag.
var afterPersistLock func()

// persistIfDirty writes the directory file when a change is pending. The lock
// order is persistMu before mu, and persistMu is held until saveFile returns so
// Close waits for an in-flight write; a failed write leaves dirty set to retry.
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

// The caller must have released d.mu.
func (d *Directory) commitOwn(vs []*directorypb.RecordVersion) {
	d.persistIfDirty()
	d.broadcast(vs, Fingerprint{})
}

// At most one maintenance write per maintainInterval, even when the loop wakes
// more often to reconcile.
func (d *Directory) persistDue(now time.Time) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.lastMaintainPersist.IsZero() || now.Sub(d.lastMaintainPersist) >= maintainInterval
}

func (d *Directory) noteMaintainPersist(now time.Time) {
	d.mu.Lock()
	if now.After(d.lastMaintainPersist) {
		d.lastMaintainPersist = now
	}
	d.mu.Unlock()
}

func (d *Directory) limits() limits {
	return limits{
		maxRecords:            d.settings.MaxRecords(),
		maxPerLink:            d.settings.MaxRecordsPerLink(),
		maxTopicsPerPublisher: d.settings.MaxTopicsPerPublisher(),
	}
}

func (d *Directory) now() time.Time { return d.clk.Now() }
