package broker

import (
	"context"
	"path/filepath"
	"sync"
)

// registryEvent is one notification from an inbox.Watch callback; the callback
// only records it and a per-account goroutine does the work, so it never waits
// on Broker.mu and a watch stop cannot deadlock against it.
type registryEvent struct {
	// uid is the account that owns the watch; the same directory string names
	// different directories for different accounts, so an event is never
	// applied across accounts.
	uid uint32
	dir string
	pid int
}

// registryQueue dispatches registry events to one serial goroutine per account,
// created when the account's first event arrives and exiting when the queue is
// empty, so one account's stopped helper cannot delay another account's work.
// Events of one account stay in arrival order. Events merge: one (dir, pid) is
// kept once, and a pid 0 event replaces the pid events of its directory, so the
// queue is bounded by the account's directories and pids, not by how many
// notifications arrive.
type registryQueue struct {
	mu      sync.Mutex
	lanes   map[uint32]*eventLane
	handled uint64
	ctx     context.Context
	handle  func(uid uint32, dir string, pid int)
}

type eventLane struct {
	items   []registryEvent
	running bool
}

func newRegistryQueue() registryQueue {
	return registryQueue{lanes: make(map[uint32]*eventLane)}
}

func (q *registryQueue) start(ctx context.Context, handle func(uid uint32, dir string, pid int)) {
	q.mu.Lock()
	q.ctx = ctx
	q.handle = handle
	q.mu.Unlock()
}

func (q *registryQueue) push(uid uint32, dir string, pid int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	lane := q.lanes[uid]
	if lane == nil {
		lane = &eventLane{}
		q.lanes[uid] = lane
	}
	lane.add(registryEvent{uid: uid, dir: dir, pid: pid})
	if !lane.running {
		lane.running = true
		go q.drain(uid, lane)
	}
}

func (l *eventLane) add(ev registryEvent) {
	for _, queued := range l.items {
		if queued.dir == ev.dir && (queued.pid == 0 || queued.pid == ev.pid) {
			return
		}
	}
	if ev.pid == 0 {
		kept := l.items[:0]
		for _, queued := range l.items {
			if queued.dir != ev.dir {
				kept = append(kept, queued)
			}
		}
		l.items = kept
	}
	l.items = append(l.items, ev)
}

func (q *registryQueue) drain(uid uint32, lane *eventLane) {
	for {
		q.mu.Lock()
		if q.ctx != nil {
			select {
			case <-q.ctx.Done():
				lane.items = nil
				lane.running = false
				delete(q.lanes, uid)
				q.mu.Unlock()
				return
			default:
			}
		}
		if len(lane.items) == 0 {
			lane.running = false
			delete(q.lanes, uid)
			q.mu.Unlock()
			return
		}
		event := lane.items[0]
		lane.items = lane.items[1:]
		q.mu.Unlock()
		q.handle(event.uid, event.dir, event.pid)
	}
}

// markHandled records that a lane has looked at one registry event; tests wait
// on handledCount for an event to reach the agent table.
func (q *registryQueue) markHandled() {
	q.mu.Lock()
	q.handled++
	q.mu.Unlock()
}

func (q *registryQueue) handledCount() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.handled
}

// laneActive reports whether the account's event goroutine is still running;
// tests wait on it for a lane to finish its queue.
func (q *registryQueue) laneActive(uid uint32) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	lane := q.lanes[uid]
	return lane != nil && lane.running
}

// handleRegistryEvent rereads the agents of uid whose records live in dir and
// retries every registration connection of uid waiting on it; a pid 0 event
// cannot be attributed to one record, so it refreshes every agent of that
// directory.
func (b *Broker) handleRegistryEvent(uid uint32, dir string, pid int) {
	b.mu.Lock()
	var agents []*agent
	for _, a := range b.agents {
		if a.watching && a.caller.UID == uid && a.watchDir.dir == dir && (pid == 0 || a.session.PID == pid) {
			agents = append(agents, a)
		}
	}
	var conns []*regConn
	for c := range b.waiting {
		if c.watching && c.caller.UID == uid && c.watchDir.dir == dir && c.socketPath != "" {
			// The connection may be between reading its record and attaching
			// its agent; marking the retry under Broker.mu makes the running
			// attempt reread after it attaches instead of losing this change.
			c.retry.Store(true)
			conns = append(conns, c)
		}
	}
	b.mu.Unlock()
	b.events.markHandled()

	for _, a := range agents {
		b.refreshAgent(a)
	}
	for _, c := range conns {
		b.attempt(c)
	}
}

// watchKey identifies one account's watch on a configuration directory. The uid
// is part of the key because the same string means different directories to
// different accounts, and no path may let one account share or block another
// account's watch. The empty string keeps its own slot; every other value is
// cleaned.
type watchKey struct {
	uid uint32
	dir string
}

func watchKeyFor(uid uint32, configDir string) watchKey {
	if configDir != "" {
		configDir = filepath.Clean(configDir)
	}
	return watchKey{uid: uid, dir: configDir}
}
