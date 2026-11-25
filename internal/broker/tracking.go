package broker

import (
	"errors"
	"path/filepath"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/inbox"
)

// errAgentGone reports that the agent named by an RPC left the table, either
// before or during the reread the RPC asked for.
var errAgentGone = errors.New("agent is no longer registered")

// prevNameRetention is how long a renamed agent keeps advertising its previous
// name.
const prevNameRetention = 10 * time.Minute

// rereadAgent refreshes one agent from its session record and returns the
// snapshot; an agent that is absent or unregistered by the reread yields
// errAgentGone.
func (b *Broker) rereadAgent(id AgentID) (agentSnapshot, error) {
	b.mu.Lock()
	a, ok := b.agents[id]
	b.mu.Unlock()
	if !ok {
		return agentSnapshot{}, errAgentGone
	}

	b.refreshAgent(a)

	snapshot, ok := b.lookupAgent(id)
	if !ok {
		return agentSnapshot{}, errAgentGone
	}
	return snapshot, nil
}

// refreshAgent rereads one agent's session record and applies the change; a
// record that makes the agent unregistrable removes it from the table and sends
// its registration connections back to waiting.
func (b *Broker) refreshAgent(a *agent) {
	// The reread runs under a.refreshMu, outside Broker.mu, so a slow read
	// never blocks the table.
	a.refreshMu.Lock()
	defer a.refreshMu.Unlock()

	b.mu.Lock()
	if b.agents[a.id] != a {
		b.mu.Unlock()
		return
	}
	previous := a.session
	b.mu.Unlock()

	callCtx, cancel := sessionCallContext(b.ctx)
	next, err := a.backend.Reread(callCtx, previous)
	cancel()

	b.mu.Lock()
	if b.agents[a.id] != a {
		// The agent left the table while the record was read; the registration
		// connection owns its state again, so discard the result.
		b.mu.Unlock()
		return
	}

	switch {
	case err == nil:
	case errors.Is(err, inbox.ErrPartial):
		// A half-written record keeps the last good values until the next
		// registry event.
		b.mu.Unlock()
		return
	case errors.Is(err, inbox.ErrNoRecord):
		stops := b.revokeLocked(a, localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SESSION_RECORD_NOT_FOUND, "")
		b.unlockAndNotify(stops)
		return
	default:
		b.log.Warn(EvRecordRereadFailed, "agent", shortID(a.id), "error", err)
		b.mu.Unlock()
		return
	}

	if next.SocketPath == "" || filepath.Clean(next.SocketPath) != a.socketPath {
		// A record naming a different inbox socket, or none, is treated as a
		// vanished record; a session that re-registers under the new path gets
		// a new agent ID.
		b.log.Info(EvAgentSocketChanged, "agent", shortID(a.id),
			"old_socket", a.socketPath, "new_socket", next.SocketPath)
		stops := b.revokeLocked(a, localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SESSION_RECORD_NOT_FOUND, "")
		b.unlockAndNotify(stops)
		return
	}

	name, detail, usable := NameUsableDetail(next.Name)
	if !usable {
		stops := b.revokeLocked(a, localpb.NotRegisteredReason_NOT_REGISTERED_REASON_NAME_UNUSABLE, detail)
		b.unlockAndNotify(stops)
		return
	}

	oldName := a.name
	nameChanged := name != oldName
	statusChanged := next.Status != a.status
	oldAddress := a.lastAddress

	a.session = next
	b.checkSessionProtocolLocked(a)
	if nameChanged {
		a.prevName = oldName
		a.prevNameAt = b.deps.Clock.Now()
		if a.prevTimer != nil {
			a.prevTimer.Stop()
		}
		at := a.prevNameAt
		a.prevTimer = b.deps.Clock.AfterFunc(prevNameRetention, func() { b.expirePrevName(a, at) })
		a.name = name
		b.recordNameOwnerLocked(name, a.caller.UID, a.accountKey, a.prevNameAt)
		b.releaseNameOwnerLocked(oldName, a.caller.UID, a.prevNameAt)
		b.log.Info(EvAgentRenamed, "agent", shortID(a.id), "old_name", oldName, "new_name", name)
	}
	if statusChanged {
		a.status = next.Status
	}

	if nameChanged {
		// A rename can move either name between accounts, changing refs, pins
		// and the need for a ref for this agent and for every other agent
		// carrying either name; recomputeRefsLocked updates and pushes them.
		b.recomputeRefsLocked()
	}
	if nameChanged || statusChanged {
		// The topic table must see the new name, the previous name and the
		// status even when no ref changed.
		b.deps.Topics.AgentChanged(a.id, b.memberLocked(a, a.ref, a.refPinned))
		// A rename whose rendered address is unchanged still moves the previous
		// name, so announce it here.
		if nameChanged && a.lastAddress == oldAddress {
			b.pushAgentChangedLocked(a)
		}
	}
	b.mu.Unlock()
}

// expirePrevName clears the previous name of a once the retention window has
// passed; at is the prevNameAt the timer started with, so a later rename with its
// own timer is left alone.
func (b *Broker) expirePrevName(a *agent, at time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.agents[a.id] != a || a.prevName == "" || !a.prevNameAt.Equal(at) {
		return
	}
	a.prevName = ""
	a.prevTimer = nil
	b.deps.Topics.AgentChanged(a.id, b.memberLocked(a, a.ref, a.refPinned))
	b.pushAgentChangedLocked(a)
}

// revokeLocked unregisters a because its record can no longer describe it; every
// connection goes back to waiting with the reason reported once and its inbox
// watch restored. Callers must hold Broker.mu; the returned stops run outside
// it.
func (b *Broker) revokeLocked(a *agent, reason localpb.NotRegisteredReason, detail string) []func() {
	conns := make([]*regConn, 0, len(a.conns))
	for c := range a.conns {
		conns = append(conns, c)
	}
	a.conns = nil

	stops := b.unregisterLocked(a)

	for _, c := range conns {
		c.agent = nil
		c.waitReason = reason
		c.waitDetail = detail
		c.reported = true
		b.waiting[c] = struct{}{}

		// Take the watch before reporting, so a record recovered right after
		// the report is still observed.
		if key, ok := b.acquireWatchLocked(c.caller.UID, c.configDir, c.backend); ok {
			if _, still := b.waiting[c]; still {
				c.watchDir = key
				c.watching = true
			} else {
				stops = append(stops, b.releaseWatchLocked(key, true)...)
			}
		}
		if _, still := b.waiting[c]; still {
			c.out.push(notRegisteredResponse(reason, detail))
		}
	}
	return stops
}

// pushAgentChangedLocked queues an agent_changed event on every registration
// connection of a and records the pushed address. Callers must hold Broker.mu.
func (b *Broker) pushAgentChangedLocked(a *agent) {
	address := b.addressOf(a)
	a.lastAddress = address
	event := &localpb.RegisterResponse{
		Event: &localpb.RegisterResponse_AgentChanged{
			AgentChanged: &localpb.AgentChangedEvent{
				AgentId:      string(a.id),
				AgentAddress: address,
				Status:       statusToProto(a.status),
				PreviousName: a.prevName,
			},
		},
	}
	for c := range a.conns {
		c.out.push(event)
	}
}
