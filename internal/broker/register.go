package broker

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/wangzw/peers/internal/broker/helper"
	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/inbox"
)

// registrationWait is how long a waiting registration stays silent before it
// reports why it has not completed.
const registrationWait = 10 * time.Second

// attemptReadHook, when set by a test, runs inside attempt after it has read
// retry and before it releases attemptMu. It lets a test stop the attempt in
// that window and deliver a registry event while the lock is held. Production
// leaves it nil.
var attemptReadHook atomic.Pointer[func(c *regConn)]

// attemptBusyHook, when set by a test, runs when attempt finds attemptMu held,
// after it has recorded the retry request. It lets a test wait until another
// attempt call has handed its request to the running attempt. Production leaves
// it nil.
var attemptBusyHook atomic.Pointer[func(c *regConn)]

// Register serves one registration stream. The first frame identifies the
// caller's inbox socket and configuration directory; later upstream frames are
// ignored. The stream stays open while the registration waits, and after it
// completes it carries address changes until the connection ends.
func (b *Broker) Register(stream localpb.Local_RegisterServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	ctx := stream.Context()
	caller, ok := CallerFrom(ctx)
	if !ok {
		return toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_INTERNAL,
			"no caller identified on this connection"))
	}
	// Count the stream only after the first frame identified the caller, and
	// before the session backend is acquired, so an account past its limit
	// costs no backend work. The slot is held for the stream's lifetime.
	if b.registers != nil {
		if !b.registers.acquire(caller.UID) {
			return toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED,
				"cannot register: account %s already has %d open registrations",
				callerAccount(caller), b.registers.limit))
		}
		defer b.registers.release(caller.UID)
	}
	backend, release, err := b.deps.Sessions.Acquire(ctx, caller.UID)
	if err != nil {
		return toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_INTERNAL,
			"no session backend for account %d: %v", caller.UID, err))
	}

	c := &regConn{
		caller:    caller,
		rawPath:   first.GetInboxSocketPath(),
		configDir: first.GetClaudeConfigDir(),
		backend:   backend,
		release:   release,
		out:       newEventQueue(),
	}

	// sendCtx stops the sender when this handler ends the stream on overflow.
	// A sender that has not started a Send returns instead of starting one. A
	// Send already in the transport returns when the handler returns, because
	// gRPC ends the RPC then, so the overflow path must not wait for the
	// sender.
	sendCtx, cancelSend := context.WithCancel(ctx)
	defer cancelSend()

	senderDone := make(chan struct{})
	go func() {
		defer close(senderDone)
		for {
			resp, ok := c.out.pop()
			if !ok {
				return
			}
			if sendCtx.Err() != nil {
				return
			}
			if err := stream.Send(resp); err != nil {
				return
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, err := stream.Recv(); err != nil {
				return
			}
		}
	}()

	b.beginRegistration(c)

	select {
	case <-done:
	case <-ctx.Done():
	case <-c.out.stopped:
	}

	b.dropConn(c)
	c.out.close()
	if c.out.overflowInfo() {
		// The client stopped reading and the backlog reached its limit. End the
		// stream instead of growing it: the connection stays open, so the client
		// can register again. Stop the sender and return without waiting for it:
		// it may be blocked in a Send the client is not reading, and returning
		// ends the RPC, which releases that Send.
		cancelSend()
		if release != nil {
			release()
		}
		// c.id names this connection even when the refused event names no agent,
		// so the warning always says which registration overflowed.
		b.log.Warn(EvRegisterEventsOverflow, "uid", c.caller.UID, "agent", shortID(c.id))
		return toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED,
			"cannot send registration events: more than %d are waiting to be read", registrationEventLimit))
	}
	<-senderDone
	if release != nil {
		release()
	}
	return nil
}

// beginRegistration puts a new connection into the waiting set and starts the
// lookup that may complete it.
func (b *Broker) beginRegistration(c *regConn) {
	if c.rawPath == "" {
		b.mu.Lock()
		b.waiting[c] = struct{}{}
		c.waitReason = localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SOCKET_PATH_MISSING
		c.attempted = true
		c.reported = true
		c.out.push(notRegisteredResponse(c.waitReason, ""))
		b.mu.Unlock()
		return
	}

	id, socketPath, err := AgentIDFor(c.rawPath)
	if err != nil {
		// A path that can never name a record waits like a missing record. It
		// needs neither a watch nor a registry lookup.
		b.mu.Lock()
		b.waiting[c] = struct{}{}
		c.waitReason = localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SESSION_RECORD_NOT_FOUND
		c.attempted = true
		c.waitTimer = b.deps.Clock.AfterFunc(registrationWait, func() { b.reportWaiting(c) })
		b.mu.Unlock()
		return
	}

	c.id = id
	c.socketPath = socketPath

	b.mu.Lock()
	// Take the watch before the connection becomes visible to the event loop,
	// so a registry change cannot slip in between.
	if key, ok := b.acquireWatchLocked(c.caller.UID, c.configDir, c.backend); ok {
		c.watchDir = key
		c.watching = true
	}
	b.waiting[c] = struct{}{}
	c.waitTimer = b.deps.Clock.AfterFunc(registrationWait, func() { b.reportWaiting(c) })
	b.mu.Unlock()

	b.attempt(c)
}

// attempt runs a full check of a waiting connection: lookup, socket owner,
// usable name. Checks of one connection are serialized by c.attemptMu. A caller
// that finds an attempt already running asks for one more pass instead of
// waiting; this keeps the registry event goroutine from stalling behind a
// record read and lets the running attempt apply the change it carries.
//
// The request is recorded before the lock is tried, and the attempt checks it
// again after releasing the lock. A request made while the lock is held is
// therefore seen by the run that releases it, and one made after that belongs
// to a later call; in neither case can it fall between the read of retry and
// the unlock and be dropped.
func (b *Broker) attempt(c *regConn) {
	c.retry.Store(true)
	for {
		if !c.attemptMu.TryLock() {
			if hook := attemptBusyHook.Load(); hook != nil {
				(*hook)(c)
			}
			return
		}
		c.retry.Store(false)
		a, attached := b.tryComplete(c)
		retry := c.retry.Load()
		if hook := attemptReadHook.Load(); hook != nil {
			(*hook)(c)
		}
		if attached && retry && a != nil {
			// A registry event arrived between the record read and the
			// attachment. The agent is in the table now, so reread it and apply
			// the change the event carries.
			b.mu.Lock()
			refresh := b.refreshOne
			b.mu.Unlock()
			refresh(a)
		}
		c.attemptMu.Unlock()
		// Run again when a request arrived while the lock was held. The check
		// comes after the unlock so that a request made in the window between
		// the read above and the unlock is not lost.
		if attached || !c.retry.Load() {
			return
		}
	}
}

// tryComplete runs one check and reports whether it attached the connection to
// an agent. attached is false when the connection is still waiting or has left.
func (b *Broker) tryComplete(c *regConn) (a *agent, attached bool) {
	b.mu.Lock()
	_, waiting := b.waiting[c]
	b.mu.Unlock()
	if !waiting {
		return nil, false
	}

	problem, session, name, ok := b.inspect(c)

	b.mu.Lock()
	if ok {
		stops := b.completeLocked(c, session, name)
		a := c.agent
		b.mu.Unlock()
		for _, stop := range stops {
			stop()
		}
		if a == nil {
			return nil, false
		}
		return a, true
	}
	if _, stillWaiting := b.waiting[c]; stillWaiting {
		c.waitReason = problem.reason
		c.waitDetail = problem.detail
		c.attempted = true
		if c.reportDue {
			b.reportWaitingLocked(c)
		}
	}
	b.mu.Unlock()
	return nil, false
}

// registrationProblem is the reason and optional detail a registration attempt
// reports for a waiting connection.
type registrationProblem struct {
	reason localpb.NotRegisteredReason
	detail string
}

// inspect performs the three checks of a registration attempt. ok is true when
// all of them pass.
func (b *Broker) inspect(c *regConn) (registrationProblem, inbox.Session, string, bool) {
	callCtx, cancel := sessionCallContext(b.ctx)
	s, err := c.backend.Lookup(callCtx, c.configDir, c.socketPath)
	cancel()
	if err != nil {
		// A missing or partially written record is the ordinary waiting state
		// and needs no warning. Every other failure does: the wait keeps
		// retrying, so the log is the only place an administrator can see why
		// it has not completed.
		if !errors.Is(err, inbox.ErrNoRecord) && !errors.Is(err, inbox.ErrPartial) {
			b.log.Warn(EvRegistrationWaiting, "error", err, "uid", c.caller.UID)
		}
		if errors.Is(err, helper.ErrUnavailable) {
			// The account's helper could not be started or stopped answering.
			// Report that separately from a record that exists but cannot be
			// read, such as a permission denial, which would send the reader
			// looking in the wrong place.
			return registrationProblem{reason: localpb.NotRegisteredReason_NOT_REGISTERED_REASON_ACCOUNT_HELPER_FAILED},
				inbox.Session{}, "", false
		}
		return registrationProblem{reason: localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SESSION_RECORD_NOT_FOUND},
			inbox.Session{}, "", false
	}
	if !socketOwnedBy(c.socketPath, c.caller.UID) {
		return registrationProblem{reason: localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SOCKET_NOT_OWNED},
			inbox.Session{}, "", false
	}
	name, detail, usable := NameUsableDetail(s.Name)
	if !usable {
		return registrationProblem{
			reason: localpb.NotRegisteredReason_NOT_REGISTERED_REASON_NAME_UNUSABLE,
			detail: detail,
		}, inbox.Session{}, "", false
	}
	return registrationProblem{}, s, name, true
}

// completeLocked attaches a connection to its agent, creating the agent when
// needed, and queues the registration_complete event. It returns the watch
// stops the caller runs outside Broker.mu. Callers must hold Broker.mu.
//
// A new agent's watch is created before the agent enters the table. Watch
// releases Broker.mu while it subscribes, and an agent in the table without its
// ref would make every reader of the address panic, so the insertion, the ref
// assignment and the registration_complete push all happen under one
// uninterrupted hold. The connection stays in the waiting set while the watch
// is created, so a stream that ends in that window is still seen.
//
// When the connection already watches the agent's configuration directory, its
// reference is transferred to the agent: the agent's reference is taken before
// the connection's is dropped, so the subscription never falls to zero. The
// ownership check inside that acquisition releases Broker.mu, but the
// connection's reference keeps the watch in place during the window, so no
// registry event can fall between them. Otherwise the agent's watch is created
// after the connection's is dropped, the window releases Broker.mu, and the
// attempt is asked to reread the agent after the attachment.
func (b *Broker) completeLocked(c *regConn, s inbox.Session, name string) []func() {
	if _, ok := b.waiting[c]; !ok {
		return nil
	}

	var stops []func()
	if _, exists := b.agents[c.id]; !exists {
		agentKey := watchKeyFor(c.caller.UID, s.ConfigDir)
		sameKey := c.watching && c.watchDir == agentKey
		var key watchKey
		var withWatch bool
		if sameKey {
			// The connection already watches the agent's directory. Hand its
			// reference over: take the agent's reference before dropping the
			// connection's. The subscription never falls to zero; the ownership
			// check may release Broker.mu, but the connection's reference keeps
			// the watch in place until the agent's is taken, so no registry event
			// can fall between the connection's watch and the agent's.
			key, withWatch = b.acquireWatchLocked(agentKey.uid, agentKey.dir, c.backend)
			stops = append(stops, b.releaseWatchLocked(c.watchDir, c.watching)...)
		} else {
			// Drop this connection's own watch before taking the agent's. The
			// stop runs outside Broker.mu; until then the old subscription still
			// covers its directory. Clearing the flags before the acquisition
			// keeps a stream that ends during it from releasing the reference a
			// second time.
			stops = append(stops, b.releaseWatchLocked(c.watchDir, c.watching)...)
			c.watching = false
			c.watchDir = watchKey{}
			key, withWatch = b.acquireWatchLocked(agentKey.uid, agentKey.dir, c.backend)
		}
		c.watching = false
		c.watchDir = watchKey{}
		if c.waitTimer != nil {
			c.waitTimer.Stop()
			c.waitTimer = nil
		}
		// Broker.mu is held again after the watch creation.

		if _, still := b.waiting[c]; !still {
			// The stream ended while the watch was being created.
			if withWatch {
				stops = append(stops, b.releaseWatchLocked(key, true)...)
			}
			return stops
		}
		if !sameKey {
			// Creating the watch released Broker.mu while this connection was
			// already out of the watching set. A registry event in that window
			// found the agent still out of the table and was not applied; ask
			// the attempt to reread the agent once after the attachment.
			c.retry.Store(true)
		}
		if other, raced := b.agents[c.id]; raced {
			// Another connection registered this agent meanwhile; the extra
			// watch reference is not needed.
			if withWatch {
				stops = append(stops, b.releaseWatchLocked(key, true)...)
			}
			if other.caller.UID != c.caller.UID {
				b.refuseAttachLocked(c, localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SOCKET_NOT_OWNED)
				return stops
			}
			other.conns[c] = struct{}{}
			c.agent = other
		} else {
			a := &agent{
				id:         c.id,
				socketPath: c.socketPath,
				caller:     c.caller,
				backend:    c.backend,
				session:    s,
				name:       name,
				status:     s.Status,
				public:     make(map[string]string),
				conns:      make(map[*regConn]struct{}),
			}
			b.agents[a.id] = a
			a.conns[c] = struct{}{}
			c.agent = a
			if withWatch {
				a.watchDir = key
				a.watching = true
			}
			b.log.Info(EvAgentRegistered, "agent", shortID(a.id), "name", name, "uid", c.caller.UID)
		}
	} else {
		a := b.agents[c.id]
		if a.caller.UID != c.caller.UID {
			// The socket path now names an agent of another account. Attaching
			// would make that agent's registration stream answer to a caller
			// that does not own its socket.
			stops = append(stops, b.releaseWatchLocked(c.watchDir, c.watching)...)
			c.watching = false
			c.watchDir = watchKey{}
			b.refuseAttachLocked(c, localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SOCKET_NOT_OWNED)
			return stops
		}
		stops = append(stops, b.releaseWatchLocked(c.watchDir, c.watching)...)
		c.watching = false
		c.watchDir = watchKey{}
		a.conns[c] = struct{}{}
		c.agent = a
	}

	delete(b.waiting, c)
	if c.waitTimer != nil {
		c.waitTimer.Stop()
		c.waitTimer = nil
	}
	b.checkSessionProtocolLocked(c.agent)
	c.reported = false
	c.waitDetail = ""

	// Introduce the agent to the topic table before the MCP process can see
	// registration_complete and join its default topics.
	b.recomputeRefsLocked()

	autoJoin := true
	if v, err := b.deps.Config.Get("topics.auto_join"); err != nil {
		b.log.Warn(EvAgentRegistered, "error", err, "key", "topics.auto_join")
	} else {
		autoJoin = v.Bool
	}
	c.out.push(&localpb.RegisterResponse{
		Event: &localpb.RegisterResponse_RegistrationComplete{
			RegistrationComplete: &localpb.RegistrationComplete{
				AgentId:      string(c.agent.id),
				AgentAddress: b.addressOf(c.agent),
				AutoJoin:     autoJoin,
			},
		},
	})
	return stops
}

// reportWaiting is the 10-second timer callback of a waiting connection.
func (b *Broker) reportWaiting(c *regConn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.waiting[c]; !ok {
		return
	}
	if !c.attempted {
		// The first attempt is still running and waitReason is not meaningful
		// yet. Reporting now would leak the zero reason; attempt reports as
		// soon as it has stored one.
		c.reportDue = true
		return
	}
	b.reportWaitingLocked(c)
}

// refuseAttachLocked records why a waiting connection cannot attach to an
// existing agent and lets the wait report it, either now when the wait timer
// has already fired or on the timer that is still running. The connection stays
// in the waiting set. Callers must hold Broker.mu.
func (b *Broker) refuseAttachLocked(c *regConn, reason localpb.NotRegisteredReason) {
	if _, still := b.waiting[c]; !still {
		return
	}
	c.waitReason = reason
	c.attempted = true
	if c.reportDue {
		b.reportWaitingLocked(c)
	}
}

// reportWaitingLocked sends the not_registered event for a waiting connection
// and records that it was sent. Callers must hold Broker.mu.
func (b *Broker) reportWaitingLocked(c *regConn) {
	if c.reported {
		return
	}
	c.out.push(notRegisteredResponse(c.waitReason, c.waitDetail))
	c.reported = true
	b.log.Info(EvRegistrationWaiting, "reason", reasonName(c.waitReason), "uid", c.caller.UID)
}

// dropConn removes a connection from the waiting set or from its agent. The
// last connection of an agent unregisters it.
func (b *Broker) dropConn(c *regConn) {
	var stops []func()
	b.mu.Lock()
	if _, waiting := b.waiting[c]; waiting {
		delete(b.waiting, c)
		if c.waitTimer != nil {
			c.waitTimer.Stop()
			c.waitTimer = nil
		}
		stops = append(stops, b.releaseWatchLocked(c.watchDir, c.watching)...)
		c.watching = false
		c.watchDir = watchKey{}
	} else if a := c.agent; a != nil {
		delete(a.conns, c)
		c.agent = nil
		if len(a.conns) == 0 {
			stops = append(stops, b.unregisterLocked(a)...)
		}
	}
	b.unlockAndNotify(stops)
}

func socketOwnedBy(path string, uid uint32) bool {
	fi, err := os.Lstat(path)
	if err != nil {
		return false
	}
	if fi.Mode()&os.ModeSocket == 0 {
		return false
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return false
	}
	return st.Uid == uid
}

func notRegisteredResponse(reason localpb.NotRegisteredReason, detail string) *localpb.RegisterResponse {
	return &localpb.RegisterResponse{
		Event: &localpb.RegisterResponse_NotRegistered{
			NotRegistered: &localpb.RegistrationProblem{Reason: reason, Detail: detail},
		},
	}
}

// reasonName renders an enum value for the structured log, for example
// "socket_path_missing".
func reasonName(reason localpb.NotRegisteredReason) string {
	return strings.ToLower(strings.TrimPrefix(reason.String(), "NOT_REGISTERED_REASON_"))
}

// registrationEventLimit is how many downstream events one registration stream
// may have waiting for its sender. A client that stops reading would otherwise
// make the Broker keep every event it produces.
const registrationEventLimit = 64

// eventQueue is the bounded, per-connection queue that feeds one sender
// goroutine. Consecutive agent_changed events for one agent are coalesced, and
// a push past the limit stops the queue: the Register handler ends the stream.
// Closing it discards pending events, because by then the registration stream
// is over.
type eventQueue struct {
	mu       sync.Mutex
	items    []*localpb.RegisterResponse
	closed   bool
	overflow bool // an event was refused because the queue was full
	signal   chan struct{}
	stopped  chan struct{} // closed once overflow stops the queue
}

func newEventQueue() *eventQueue {
	return &eventQueue{
		signal:  make(chan struct{}, 1),
		stopped: make(chan struct{}),
	}
}

// push queues resp and reports whether it was accepted. An agent_changed
// replaces a trailing, not yet sent agent_changed for the same agent instead of
// being appended, so an unread stream keeps only the latest address. An event
// that would take the backlog past registrationEventLimit is refused, and the
// queue stops; caller work already in progress is unaffected.
func (q *eventQueue) push(resp *localpb.RegisterResponse) bool {
	q.mu.Lock()
	if q.closed || q.overflow {
		q.mu.Unlock()
		return false
	}
	if changed := resp.GetAgentChanged(); changed != nil && len(q.items) > 0 {
		if last := q.items[len(q.items)-1].GetAgentChanged(); last != nil &&
			last.GetAgentId() == changed.GetAgentId() {
			q.items[len(q.items)-1] = resp
			q.mu.Unlock()
			return true
		}
	}
	if len(q.items) >= registrationEventLimit {
		q.overflow = true
		close(q.stopped)
		q.mu.Unlock()
		return false
	}
	q.items = append(q.items, resp)
	q.mu.Unlock()
	select {
	case q.signal <- struct{}{}:
	default:
	}
	return true
}

// overflowInfo reports whether the queue stopped because an event did not fit.
func (q *eventQueue) overflowInfo() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.overflow
}

func (q *eventQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.items = nil
	q.mu.Unlock()
	select {
	case q.signal <- struct{}{}:
	default:
	}
}

// pop blocks until an event is queued or the queue is closed. It returns
// ok == false once the queue is closed and empty.
func (q *eventQueue) pop() (*localpb.RegisterResponse, bool) {
	for {
		q.mu.Lock()
		if len(q.items) > 0 {
			resp := q.items[0]
			q.items = q.items[1:]
			q.mu.Unlock()
			return resp, true
		}
		if q.closed {
			q.mu.Unlock()
			return nil, false
		}
		q.mu.Unlock()
		<-q.signal
	}
}
