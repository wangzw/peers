package helper

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wangzw/peers/internal/broker/helperpb"
	"github.com/wangzw/peers/internal/inbox"
)

// Process is one started helper child.
type Process struct {
	Stdin  io.WriteCloser
	Stdout io.Reader
	Wait   func() error // reaps the child
	Kill   func() error
}

type PoolOptions struct {
	Exe          string                                                    // required unless Start is set
	Lookup       func(uid uint32) (Account, error)                         // default LookupAccount
	Start        func(ctx context.Context, acct Account) (*Process, error) // default: accountCommand(Exe, []string{"internal", "account-helper"}, acct) with pipes
	ReadyTimeout time.Duration                                             // default 5s
	RespawnMin   time.Duration                                             // default 1s
	RespawnMax   time.Duration                                             // default 30s
	After        func(d time.Duration) <-chan time.Time                    // default time.After
	Logger       *slog.Logger                                              // default slog.Default()
	// Stopping is closed when the process starts to shut down. A helper that
	// exits while it is closed is stopping with the Broker, not failing.
	Stopping <-chan struct{}
}

// helperConn is the parent side of one helper connection; *Client implements it.
type helperConn interface {
	WaitReady(ctx context.Context) (*helperpb.Ready, error)
	Lookup(ctx context.Context, configDir, socketPath string) (inbox.Session, error)
	Reread(ctx context.Context, s inbox.Session) (inbox.Session, error)
	Watch(ctx context.Context, configDir string, onChange func(pid int)) (stop func(), err error)
	Deliver(ctx context.Context, s inbox.Session, sig, text, msgID string, timeout time.Duration) error
	Done() <-chan struct{}
	Close() error
}

var _ helperConn = (*Client)(nil)

// errPoolClosed is returned by Acquire and by every backend call once Close
// has started.
var errPoolClosed = errors.New("the helper pool is closed")

// reapTimeout is how long a child gets to exit after its stdin is closed
// before it is killed.
const reapTimeout = 2 * time.Second

// Pool supervises one account helper process per account.
type Pool struct {
	opts    PoolOptions
	connect func(p *Process) helperConn
	closed  atomic.Bool

	mu          sync.Mutex
	states      map[uint32]*helperState
	closeCh     chan struct{}
	closeCtx    context.Context // cancelled when Close starts, to abort spawns
	closeCancel context.CancelFunc
	children    childGroup // every goroutine that still owns a child process
}

// childGroup makes Close's wait race-free with late additions: add refuses
// once closeAndWait has started, so no child can be left behind.
type childGroup struct {
	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
}

func (g *childGroup) add() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return false
	}
	g.wg.Add(1)
	return true
}

func (g *childGroup) done() { g.wg.Done() }

func (g *childGroup) closeAndWait() {
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
	g.wg.Wait()
}

// liveHelper is one running child together with its connection. wait reaps the
// child exactly once, whichever goroutine gets there first.
type liveHelper struct {
	conn helperConn
	proc *Process

	once sync.Once
	err  error
}

func (l *liveHelper) wait() error {
	l.once.Do(func() {
		if l.proc.Wait != nil {
			l.err = l.proc.Wait()
		}
	})
	return l.err
}

func (l *liveHelper) killAndWait() {
	if l.proc.Kill != nil {
		_ = l.proc.Kill()
	}
	_ = l.wait()
}

// poolWatch is one Backend.Watch subscription. The mutex also gives stop its
// guarantee: fire holds it while calling onChange, so once stop has set
// stopped no callback is running or will start.
type poolWatch struct {
	id        uint64
	configDir string
	onChange  func(pid int)

	mu      sync.Mutex
	stopped bool
	conn    helperConn
	stop    func()
}

func (e *poolWatch) fire(pid int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopped {
		return
	}
	e.onChange(pid)
}

// detach marks the entry stopped and returns the stop of the connection it is
// currently attached to, if any. The caller invokes the returned function
// after releasing the lock.
func (e *poolWatch) detach() func() {
	e.mu.Lock()
	e.stopped = true
	stop := e.stop
	e.stop = nil
	e.conn = nil
	e.mu.Unlock()
	return stop
}

// helperState is everything the pool tracks for one account.
type helperState struct {
	uid uint32

	// spawnMu serialises process creation for this account; it is held across
	// Lookup, Start and the Ready handshake so concurrent first uses share one
	// child.
	spawnMu sync.Mutex

	mu        sync.Mutex
	live      *liveHelper
	refs      int
	inflight  int
	watches   map[uint64]*poolWatch
	nextWatch uint64
	backoff   time.Duration

	// respawn holds the token of the respawn loop that currently owns this
	// account, or nil. Token identity keeps a finishing loop from clearing the
	// token of a newer one.
	respawn *respawnRun
}

type respawnRun struct{}

func NewPool(opts PoolOptions) *Pool {
	if opts.Lookup == nil {
		opts.Lookup = LookupAccount
	}
	if opts.ReadyTimeout <= 0 {
		opts.ReadyTimeout = 5 * time.Second
	}
	if opts.RespawnMin <= 0 {
		opts.RespawnMin = time.Second
	}
	if opts.RespawnMax <= 0 {
		opts.RespawnMax = 30 * time.Second
	}
	if opts.RespawnMax < opts.RespawnMin {
		opts.RespawnMax = opts.RespawnMin
	}
	if opts.After == nil {
		opts.After = time.After
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	log := opts.Logger

	p := &Pool{
		opts:    opts,
		states:  make(map[uint32]*helperState),
		closeCh: make(chan struct{}),
	}
	p.closeCtx, p.closeCancel = context.WithCancel(context.Background())
	if p.opts.Start == nil {
		exe := p.opts.Exe
		p.opts.Start = func(ctx context.Context, acct Account) (*Process, error) {
			if exe == "" {
				return nil, errors.New("cannot start the account helper: no executable is configured")
			}
			return startHelperProcess(exe, acct, log, p.stoppingNow)
		}
	}
	p.connect = func(proc *Process) helperConn {
		return NewClient(proc.Stdout, proc.Stdin)
	}
	return p
}

// stoppingNow reports whether the Broker has begun to shut down, either
// because Close has started or because the process context passed as Stopping
// has ended. A helper that exits or writes a cancellation line while this is
// true is stopping with the Broker, not failing.
func (p *Pool) stoppingNow() bool {
	select {
	case <-p.closeCh:
		return true
	default:
	}
	if p.opts.Stopping != nil {
		select {
		case <-p.opts.Stopping:
			return true
		default:
		}
	}
	return false
}

// startHelperProcess runs exe's hidden account-helper subcommand as acct and
// connects the child's standard streams to pipes.
func startHelperProcess(exe string, acct Account, log *slog.Logger, stopping func() bool) (*Process, error) {
	return startHelperProcessArgs(exe, []string{"internal", "account-helper"}, acct, log, stopping)
}

// The child's standard-stream pipes are variables so tests can exercise the
// pipe-creation failures a normal start cannot trigger.
var (
	helperStdinPipe  = func(cmd *exec.Cmd) (io.WriteCloser, error) { return cmd.StdinPipe() }
	helperStdoutPipe = func(cmd *exec.Cmd) (io.ReadCloser, error) { return cmd.StdoutPipe() }
)

// startHelperProcessArgs is startHelperProcess with the subcommand arguments
// spelled out, so tests can exercise the same standard-stream wiring.
func startHelperProcessArgs(exe string, args []string, acct Account, log *slog.Logger, stopping func() bool) (*Process, error) {
	cmd := accountCommand(exe, args, acct)
	stdin, err := helperStdinPipe(cmd)
	if err != nil {
		return nil, err
	}
	stdout, err := helperStdoutPipe(cmd)
	if err != nil {
		return nil, err
	}
	// The helper's stderr is a pipe read by the Broker, not the Broker's own
	// stderr: a helper must not write into the service log as if it were the
	// Broker, and its output is only useful with the account it belongs to.
	stderr := newHelperStderr(log, acct.UID, stopping)
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &Process{
		Stdin:  stdin,
		Stdout: stdout,
		Wait: func() error {
			err := cmd.Wait()
			stderr.flush()
			return err
		},
		Kill: func() error { return cmd.Process.Kill() },
	}, nil
}

// Acquire takes a reference on uid's helper and returns a backend that keeps
// working across helper restarts. It does not start the child.
func (p *Pool) Acquire(ctx context.Context, uid uint32) (*Backend, func(), error) {
	p.mu.Lock()
	if p.closed.Load() {
		p.mu.Unlock()
		return nil, nil, errPoolClosed
	}
	st := p.states[uid]
	if st == nil {
		st = &helperState{
			uid:     uid,
			watches: make(map[uint64]*poolWatch),
			backoff: p.opts.RespawnMin,
		}
		p.states[uid] = st
	}
	st.mu.Lock()
	st.refs++
	st.mu.Unlock()
	p.mu.Unlock()

	b := &Backend{pool: p, state: st}
	var once sync.Once
	release := func() {
		once.Do(func() {
			st.mu.Lock()
			if st.refs > 0 {
				st.refs--
			}
			p.maybeReclaimLocked(st)
			st.mu.Unlock()
		})
	}
	return b, release, nil
}

// Close stops every helper and waits for the children to be reaped.
func (p *Pool) Close() error {
	p.mu.Lock()
	if p.closed.Load() {
		p.mu.Unlock()
		return nil
	}
	p.closed.Store(true)
	close(p.closeCh)
	p.closeCancel()
	states := make([]*helperState, 0, len(p.states))
	for _, st := range p.states {
		states = append(states, st)
	}
	p.mu.Unlock()

	for _, st := range states {
		st.mu.Lock()
		live := st.live
		st.live = nil
		for _, e := range st.watches {
			e.mu.Lock()
			e.conn = nil
			e.stop = nil
			e.mu.Unlock()
		}
		st.mu.Unlock()
		if live != nil {
			p.reap(live)
		}
	}
	// Spawns and reclamations still in progress own children the state sweep
	// above could not see. Wait for every one of them to be reaped.
	p.children.closeAndWait()
	return nil
}

// reap closes the connection, waits up to two seconds for the child to exit,
// and kills it if it has not.
func (p *Pool) reap(live *liveHelper) {
	done := make(chan struct{})
	go func() {
		_ = live.conn.Close()
		_ = live.wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(reapTimeout):
		if live.proc.Kill != nil {
			_ = live.proc.Kill()
		}
		<-done
	}
}

// maybeReclaimLocked closes the connection of st when nothing uses it any
// more. st.mu must be held.
func (p *Pool) maybeReclaimLocked(st *helperState) {
	if p.closed.Load() || st.live == nil {
		return
	}
	if st.refs > 0 || st.inflight > 0 || len(st.watches) > 0 {
		return
	}
	// Count the reaper before detaching the child: if the pool is closing, the
	// Close sweep must still be able to find the live child, and if it is not,
	// Close must wait for this goroutine's child to be reaped.
	if !p.children.add() {
		return
	}
	live := st.live
	st.live = nil
	go func() {
		defer p.children.done()
		p.reap(live)
	}()
}

// ensureLive returns the account's running child, starting one when there is
// none. Spawn attempt, Ready handshake and identity checks all happen while
// holding st.spawnMu, so concurrent callers observe one child.
func (p *Pool) ensureLive(ctx context.Context, st *helperState) (*liveHelper, error) {
	st.spawnMu.Lock()
	defer st.spawnMu.Unlock()

	st.mu.Lock()
	if st.live != nil {
		live := st.live
		st.mu.Unlock()
		return live, nil
	}
	st.mu.Unlock()

	if p.closed.Load() {
		return nil, errPoolClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, deadlineUnavailable(ctx)
	}
	// Count the child before it exists, not after Ready: Close must wait for a
	// spawn that is still in progress, and it cancels closeCtx so a well-behaved
	// Start or Ready wait gives up instead of holding Close for ReadyTimeout.
	if !p.children.add() {
		return nil, errPoolClosed
	}
	counted := true
	defer func() {
		if counted {
			p.children.done()
		}
	}()

	spawnCtx, cancelSpawn := context.WithCancel(ctx)
	defer cancelSpawn()
	stopClose := context.AfterFunc(p.closeCtx, cancelSpawn)
	defer stopClose()

	acct, err := p.opts.Lookup(st.uid)
	if err != nil {
		// The helper cannot be started without the account, so this is a helper
		// failure for registration, not a session record that is missing.
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	proc, err := p.opts.Start(spawnCtx, acct)
	if err != nil {
		if p.closed.Load() {
			return nil, errPoolClosed
		}
		if ctx.Err() != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				// The deadline bounds the helper, so a child that did not start
				// within it is a helper failure. Keep both errors reachable.
				return nil, fmt.Errorf("%w: %w", ErrUnavailable, errors.Join(ctx.Err(), err))
			}
			// The caller cancelled while the child was being started; that is
			// not the helper failing.
			return nil, err
		}
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	conn := p.connect(proc)
	live := &liveHelper{conn: conn, proc: proc}
	if conn == nil {
		live.killAndWait()
		return nil, fmt.Errorf("%w: cannot start the account helper: the helper returned no connection", ErrUnavailable)
	}

	readyCtx, cancelReady := context.WithTimeout(spawnCtx, p.opts.ReadyTimeout)
	ready, err := conn.WaitReady(readyCtx)
	cancelReady()
	if err == nil {
		switch {
		case ready == nil:
			err = fmt.Errorf("%w: account helper for uid %d sent no Ready", ErrProtocol, st.uid)
		case ready.ProtocolVersion != helperpb.ProtocolVersion:
			err = fmt.Errorf("cannot start the account helper for uid %d: it speaks protocol version %d, want %d",
				st.uid, ready.ProtocolVersion, helperpb.ProtocolVersion)
		case ready.Uid != acct.UID || ready.Gid != acct.GID:
			err = fmt.Errorf("cannot start the account helper for uid %d: it reported uid %d gid %d, want uid %d gid %d",
				st.uid, ready.Uid, ready.Gid, acct.UID, acct.GID)
		}
	}
	if err != nil {
		switch {
		case p.closed.Load():
			err = errPoolClosed
		case ctx.Err() != nil && !errors.Is(ctx.Err(), context.DeadlineExceeded):
			// The caller cancelled while the helper was starting up; that is
			// not the helper failing.
		default:
			// A helper that never became ready, spoke a different protocol or
			// reported the wrong identity could not be put to use. A deadline
			// that ended the wait belongs here too: it bounds the helper, so
			// the helper did not start in time.
			err = fmt.Errorf("%w: %w", ErrUnavailable, err)
		}
		live.killAndWait()
		return nil, err
	}

	st.mu.Lock()
	if p.closed.Load() {
		st.mu.Unlock()
		_ = conn.Close()
		_ = live.wait()
		return nil, errPoolClosed
	}
	st.live = live
	st.backoff = p.opts.RespawnMin
	st.mu.Unlock()

	counted = false
	go p.watchLive(st, live)
	return live, nil
}

// watchLive logs an unexpected child exit, detaches its watches and starts the
// respawn loop when any watch still needs the child.
func (p *Pool) watchLive(st *helperState, live *liveHelper) {
	defer p.children.done()
	<-live.conn.Done()

	watches, ok := detachLive(st, live)
	if !ok {
		// Closed by reclamation or by Pool.Close.
		return
	}

	err := live.wait()
	if p.stoppingNow() {
		// The process is going down; this exit is the stop, not a failure.
		p.opts.Logger.Info("account_helper_exited", "uid", st.uid, "err", err)
		return
	}
	p.opts.Logger.Error("account_helper_exited", "uid", st.uid, "err", err)
	if watches > 0 {
		// Close can begin between the check above and this point. A respawn
		// started now would find the pool closed and do nothing, so check
		// again instead of launching a loop into a closing pool. What remains
		// is the window between the first check and the Error above, where the
		// record is written without a lock shared with Close; overlapping that
		// window would still leave one Error record for a normal exit.
		if p.stoppingNow() {
			return
		}
		p.startRespawn(st)
	}
}

// discardLive drops a helper whose call hit its deadline. The helper is marked
// bad, the watches attached to it are detached and the respawn loop restores
// them; with no watch registered, the next call starts a fresh helper.
func (p *Pool) discardLive(st *helperState, live *liveHelper) {
	watches, ok := detachLive(st, live)
	if !ok {
		return
	}
	_ = live.conn.Close()
	live.killAndWait()
	if watches > 0 {
		p.startRespawn(st)
	}
}

// detachLive removes live from the account's state while it is still the
// current child, and detaches every watch attached to its connection. It
// reports how many watches remain, and false when live was already gone.
func detachLive(st *helperState, live *liveHelper) (watches int, ok bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.live != live {
		return 0, false
	}
	st.live = nil
	for _, e := range st.watches {
		e.mu.Lock()
		if e.conn == live.conn {
			e.conn = nil
			e.stop = nil
		}
		e.mu.Unlock()
	}
	return len(st.watches), true
}

// startRespawn launches the account's respawn loop unless one already runs.
func (p *Pool) startRespawn(st *helperState) {
	p.mu.Lock()
	if p.closed.Load() {
		p.mu.Unlock()
		return
	}
	st.mu.Lock()
	if st.respawn != nil {
		st.mu.Unlock()
		p.mu.Unlock()
		return
	}
	tok := &respawnRun{}
	st.respawn = tok
	if !p.children.add() {
		st.respawn = nil
		st.mu.Unlock()
		p.mu.Unlock()
		return
	}
	st.mu.Unlock()
	p.mu.Unlock()

	go p.respawnLoop(st, tok)
}

// respawnLoop retries the spawn with exponential backoff and, once a child is
// up, restores every watch and asks the Broker to reread with pid 0.
func (p *Pool) respawnLoop(st *helperState, tok *respawnRun) {
	defer p.children.done()
	defer func() {
		st.mu.Lock()
		if st.respawn == tok {
			st.respawn = nil
		}
		st.mu.Unlock()
	}()

	for {
		st.mu.Lock()
		if len(st.watches) == 0 {
			st.mu.Unlock()
			return
		}
		delay := st.backoff
		if delay <= 0 {
			delay = p.opts.RespawnMin
		}
		st.mu.Unlock()

		select {
		case <-p.closeCh:
			return
		case <-p.opts.After(delay):
		}

		st.mu.Lock()
		if len(st.watches) == 0 {
			st.mu.Unlock()
			return
		}
		st.mu.Unlock()

		live, err := p.ensureLive(context.Background(), st)
		if err != nil {
			if errors.Is(err, errPoolClosed) {
				return
			}
			st.mu.Lock()
			next := st.backoff * 2
			if next < st.backoff || next > p.opts.RespawnMax {
				next = p.opts.RespawnMax
			}
			st.backoff = next
			st.mu.Unlock()
			continue
		}

		// Give up ownership before restoring so a crash during the restore
		// can start a fresh loop.
		st.mu.Lock()
		if st.respawn == tok {
			st.respawn = nil
		}
		st.mu.Unlock()
		p.restoreWatches(st, live)
		st.mu.Lock()
		p.maybeReclaimLocked(st)
		st.mu.Unlock()
		return
	}
}

// restoreWatches reattaches the account's watches to a new connection and then
// fires each one with pid 0 so the Broker rereads in full.
func (p *Pool) restoreWatches(st *helperState, live *liveHelper) {
	st.mu.Lock()
	entries := make([]*poolWatch, 0, len(st.watches))
	for _, e := range st.watches {
		entries = append(entries, e)
	}
	st.mu.Unlock()

	var restored []*poolWatch
	for _, e := range entries {
		e.mu.Lock()
		active := !e.stopped
		attached := e.conn == live.conn
		e.mu.Unlock()
		if !active {
			continue
		}
		if attached {
			restored = append(restored, e)
			continue
		}

		stop, err := live.conn.Watch(context.Background(), e.configDir, e.fire)
		if err != nil {
			continue
		}
		e.mu.Lock()
		if e.stopped || e.conn != nil {
			e.mu.Unlock()
			stop()
			continue
		}
		e.conn = live.conn
		e.stop = stop
		e.mu.Unlock()
		restored = append(restored, e)
	}

	for _, e := range restored {
		e.fire(0)
	}
}

// Backend is one account's view of the pool; methods mirror Client and survive helper restarts.
type Backend struct {
	pool  *Pool
	state *helperState
}

// invoke runs fn on the account's current connection, with the in-flight
// counter held across the call, spawning on demand.
func (b *Backend) invoke(ctx context.Context, fn func(helperConn) error) (spawnFailed bool, err error) {
	st := b.state
	st.mu.Lock()
	if b.pool.closed.Load() {
		st.mu.Unlock()
		return false, errPoolClosed
	}
	st.inflight++
	live := st.live
	st.mu.Unlock()

	if live == nil {
		live, err = b.pool.ensureLive(ctx, st)
		if err != nil {
			st.mu.Lock()
			st.inflight--
			b.pool.maybeReclaimLocked(st)
			st.mu.Unlock()
			return true, err
		}
	}

	err = fn(live.conn)

	st.mu.Lock()
	st.inflight--
	b.pool.maybeReclaimLocked(st)
	st.mu.Unlock()

	// The call's deadline can end while the helper is still working with room
	// left in the pipe, and every later call would pay the same timeout. Drop
	// this helper so the next call starts a fresh one.
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		b.pool.discardLive(st, live)
	}
	return false, err
}

func (b *Backend) Lookup(ctx context.Context, configDir, socketPath string) (inbox.Session, error) {
	var s inbox.Session
	_, err := b.invoke(ctx, func(c helperConn) error {
		var err error
		s, err = c.Lookup(ctx, configDir, socketPath)
		return err
	})
	return s, err
}

func (b *Backend) Reread(ctx context.Context, s inbox.Session) (inbox.Session, error) {
	var out inbox.Session
	_, err := b.invoke(ctx, func(c helperConn) error {
		var err error
		out, err = c.Reread(ctx, s)
		return err
	})
	return out, err
}

func (b *Backend) Watch(ctx context.Context, configDir string, onChange func(pid int)) (stop func(), err error) {
	p := b.pool
	st := b.state

	st.mu.Lock()
	if p.closed.Load() {
		st.mu.Unlock()
		return nil, errPoolClosed
	}
	st.inflight++
	st.nextWatch++
	entry := &poolWatch{id: st.nextWatch, configDir: configDir, onChange: onChange}
	st.watches[entry.id] = entry
	st.mu.Unlock()

	live, err := p.ensureLive(ctx, st)
	if err == nil {
		var cstop func()
		cstop, err = live.conn.Watch(ctx, configDir, entry.fire)
		if err == nil {
			entry.mu.Lock()
			if entry.stopped {
				entry.mu.Unlock()
				cstop()
				err = errors.New("the watch stopped while starting")
			} else {
				entry.conn = live.conn
				entry.stop = cstop
				entry.mu.Unlock()
			}
		}
	}
	if err == nil {
		st.mu.Lock()
		st.inflight--
		p.maybeReclaimLocked(st)
		st.mu.Unlock()

		var once sync.Once
		stop = func() {
			once.Do(func() {
				cstop := entry.detach()
				if cstop != nil {
					cstop()
				}
				st.mu.Lock()
				delete(st.watches, entry.id)
				p.maybeReclaimLocked(st)
				st.mu.Unlock()
			})
		}
		return stop, nil
	}

	if cstop := entry.detach(); cstop != nil {
		cstop()
	}
	st.mu.Lock()
	delete(st.watches, entry.id)
	st.inflight--
	p.maybeReclaimLocked(st)
	st.mu.Unlock()
	return nil, err
}

func (b *Backend) Deliver(ctx context.Context, s inbox.Session, sig, text, msgID string, timeout time.Duration) error {
	spawnFailed, err := b.invoke(ctx, func(c helperConn) error {
		return c.Deliver(ctx, s, sig, text, msgID, timeout)
	})
	if spawnFailed && !errors.Is(err, ErrNoAccount) {
		return fmt.Errorf("%w: %v", inbox.ErrNotWritten, err)
	}
	return err
}
