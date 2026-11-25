package broker

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/inbox"
	"github.com/wangzw/peers/internal/topic"
)

// agent is one local agent identified by its socket path; every registration
// connection at that path attaches to the same agent. All fields except
// refreshMu are guarded by Broker.mu.
type agent struct {
	id           AgentID
	ref          string
	socketPath   string
	caller       Caller
	accountKey   uint32
	backend      SessionBackend
	session      inbox.Session
	name         string
	status       string
	refPinned    bool
	refMandatory bool
	// seq is the registration order; it starts at 1 and never repeats.
	seq         uint64
	prevName    string
	prevNameAt  time.Time
	prevTimer   Timer
	lastAddress string
	public      map[string]string
	conns       map[*regConn]struct{}
	refreshMu   sync.Mutex

	introduced bool
	// watching cannot be derived from watchDir: the key carries the owner uid
	// and may have an empty directory, so root's default directory gives the
	// zero watchKey.
	watchDir watchKey
	watching bool

	warnedPeerProtocol int
}

// regConn is one registration stream. Fields other than attemptMu are guarded
// by Broker.mu; attemptMu serializes the record lookup of this connection.
type regConn struct {
	caller     Caller
	accountKey uint32
	rawPath    string
	id         AgentID
	socketPath string
	configDir  string
	backend    SessionBackend
	release    func()
	agent      *agent
	waitReason localpb.NotRegisteredReason
	waitDetail string
	attempted  bool // an attempt has stored waitReason; the timer must not report before that
	reportDue  bool
	reported   bool
	waitTimer  Timer
	watchDir   watchKey
	watching   bool
	out        *eventQueue

	attemptMu sync.Mutex
	// retry asks the running attempt to reread the agent after it attaches,
	// because a registry event or watch creation may have changed the record
	// since it was read.
	retry atomic.Bool
}

// dirWatch is one shared inbox.Watch registration for a configuration directory;
// creating is true while a Watch call is in flight so another acquirer waits
// instead of starting a second watcher.
type dirWatch struct {
	stop     func()
	refs     int
	creating bool
	wait     chan struct{}
}

type agentSnapshot struct {
	ID               AgentID
	Name             string
	Ref              string
	RefPinned        bool
	RefMandatory     bool
	PrevName         string
	Address          string
	Caller           Caller
	Session          inbox.Session
	Inbox            SessionBackend
	NameOwnerUID     uint32
	NameOwned        bool
	PrevNameOwnerUID uint32
	PrevNameOwned    bool
}

func (b *Broker) lookupAgent(id AgentID) (agentSnapshot, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	a, ok := b.agents[id]
	if !ok {
		return agentSnapshot{}, false
	}
	snap := b.snapshotLocked(a)
	if b.nameOwnerPushPending {
		// A read may have expired a name and moved ownership; push the change
		// and take the snapshot again with the new refs.
		b.recomputeRefsLocked()
		b.nameOwnerPushPending = false
		if a, ok = b.agents[id]; ok {
			snap = b.snapshotLocked(a)
		}
	}
	return snap, true
}

func (b *Broker) allAgents() []agentSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	snapshot := func() []agentSnapshot {
		out := make([]agentSnapshot, 0, len(b.agents))
		for _, a := range b.agents {
			out = append(out, b.snapshotLocked(a))
		}
		sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
		return out
	}
	out := snapshot()
	if b.nameOwnerPushPending {
		// A snapshot may have expired a name and moved ownership; push the
		// change and rebuild so the caller sees the post-change refs.
		b.recomputeRefsLocked()
		b.nameOwnerPushPending = false
		out = snapshot()
	}
	return out
}

// snapshotLocked builds a's snapshot. Callers must hold Broker.mu.
func (b *Broker) snapshotLocked(a *agent) agentSnapshot {
	now := b.deps.Clock.Now()
	ownerUID, owned := b.nameOwnerOfLocked(a.name, now)
	var prevOwnerUID uint32
	var prevOwned bool
	if a.prevName != "" {
		prevOwnerUID, prevOwned = b.nameOwnerOfLocked(a.prevName, now)
	}
	needRef := b.nameNeedsRefLocked(a)
	return agentSnapshot{
		ID:               a.id,
		Name:             a.name,
		Ref:              a.ref,
		RefPinned:        a.refPinned,
		RefMandatory:     needRef,
		PrevName:         a.prevName,
		Address:          FormatAddress(a.name, a.ref, b.localAlias(), needRef),
		Caller:           a.caller,
		Session:          a.session,
		Inbox:            a.backend,
		NameOwnerUID:     ownerUID,
		NameOwned:        owned,
		PrevNameOwnerUID: prevOwnerUID,
		PrevNameOwned:    prevOwned,
	}
}

// onUnregister registers a callback that runs outside Broker.mu whenever an
// agent leaves the table.
func (b *Broker) onUnregister(f func(id AgentID)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.unregHooks = append(b.unregHooks, f)
}

// publicTopicSet returns the public topic declarations as a name-to-description
// map, taking the first description in Agent-ID order.
func (b *Broker) publicTopicSet() map[string]string {
	b.mu.Lock()
	defer b.mu.Unlock()
	ids := make([]AgentID, 0, len(b.agents))
	for id := range b.agents {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := make(map[string]string)
	for _, id := range ids {
		for name, description := range b.agents[id].public {
			if _, ok := out[name]; !ok {
				out[name] = description
			}
		}
	}
	return out
}

// refreshAgents rereads the given agents in order, skipping IDs that left the
// table; the reread runs outside Broker.mu.
func (b *Broker) refreshAgents(ids []AgentID) {
	b.mu.Lock()
	fn := b.refreshOne
	agents := make([]*agent, 0, len(ids))
	for _, id := range ids {
		if a, ok := b.agents[id]; ok {
			agents = append(agents, a)
		}
	}
	b.mu.Unlock()
	for _, a := range agents {
		fn(a)
	}
}

// agentFor resolves the agent named by an RPC for the caller that carries ctx.
func (b *Broker) agentFor(ctx context.Context, id string) (*agent, error) {
	caller, ok := CallerFrom(ctx)
	if !ok {
		return nil, Errorf(localpb.ErrorCode_ERROR_CODE_INTERNAL, "no caller identified on this connection")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	a, ok := b.agents[AgentID(id)]
	if !ok || a.caller.UID != caller.UID {
		return nil, Errorf(localpb.ErrorCode_ERROR_CODE_NOT_REGISTERED,
			"agent %q is not registered for this account", id)
	}
	return a, nil
}

// localAlias returns the effective alias. Once Config.Get fails during shutdown
// it keeps the last valid value so addresses still render. Callers must hold
// Broker.mu.
func (b *Broker) localAlias() string {
	v, err := b.deps.Config.Get("broker.alias")
	if err != nil {
		return b.lastAlias
	}
	if ValidAlias(v.String) {
		b.lastAlias = v.String
	}
	return v.String
}

// addressOf renders a's address. Callers must hold Broker.mu.
func (b *Broker) addressOf(a *agent) string {
	return b.formatAddressLocked(a, b.nameNeedsRefLocked(a))
}

// formatAddressLocked renders a's address with the needRef decision already
// made. Callers must hold Broker.mu.
func (b *Broker) formatAddressLocked(a *agent, needRef bool) string {
	return FormatAddress(a.name, a.ref, b.localAlias(), needRef)
}

// nameNeedsRefLocked reports whether a's address must carry a ref. Callers must
// hold Broker.mu.
func (b *Broker) nameNeedsRefLocked(a *agent) bool {
	_, needRef := b.nameRefFlagsLocked(a, b.deps.Clock.Now())
	return needRef
}

// nameRefFlagsLocked returns whether a's name is owned by another account and
// whether its address must carry a ref. Callers must hold Broker.mu.
func (b *Broker) nameRefFlagsLocked(a *agent, now time.Time) (pinned, needRef bool) {
	owner, ok := b.nameOwnerOfLocked(a.name, now)
	if !ok {
		return false, b.nameCountLocked(a.name) > 1
	}
	if owner != a.caller.UID {
		return true, true
	}
	return false, b.ownerNameCountLocked(a.name, owner) > 1
}

// nameCountLocked returns how many agents carry name. Callers must hold
// Broker.mu.
func (b *Broker) nameCountLocked(name string) int {
	n := 0
	for _, a := range b.agents {
		if a.name == name {
			n++
		}
	}
	return n
}

// ownerNameCountLocked returns how many of uid's agents carry name. Callers
// must hold Broker.mu.
func (b *Broker) ownerNameCountLocked(name string, uid uint32) int {
	n := 0
	for _, a := range b.agents {
		if a.name == name && a.caller.UID == uid {
			n++
		}
	}
	return n
}

// recomputeRefsLocked recalculates every agent's ref and whether it must show
// one, then pushes the change to the topic table and to each registration
// stream; a newly seen agent is introduced with its initial roster entry and no
// agent_changed event. An agent whose name belongs to another account, or that
// cannot fit an address under any alias, keeps the full 12-character prefix of
// its ID. Callers must hold Broker.mu.
func (b *Broker) recomputeRefsLocked() {
	ids := make([]AgentID, 0, len(b.agents))
	for id := range b.agents {
		ids = append(ids, id)
	}
	refs := ComputeRefs(ids)
	now := b.deps.Clock.Now()
	for id, a := range b.agents {
		ref := refs[id]
		pinned, needRef := b.nameRefFlagsLocked(a, now)
		// A shortened name is accepted only with the full ref; use the longest
		// alias so the decision holds for every viewer.
		if pinned || addressNameTruncates(a.name, ref, maxAddressAlias(), needRef) {
			ref = shortID(id)
		}
		member := b.memberLocked(a, ref, pinned)
		a.refPinned = pinned
		mandatoryChanged := a.refMandatory != needRef
		a.refMandatory = needRef
		if !a.introduced {
			a.ref = ref
			a.introduced = true
			a.lastAddress = b.formatAddressLocked(a, needRef)
			b.deps.Topics.AgentChanged(id, member)
			continue
		}
		refChanged := a.ref != ref
		a.ref = ref
		address := b.formatAddressLocked(a, needRef)
		if refChanged || mandatoryChanged {
			b.deps.Topics.AgentChanged(id, member)
		}
		if refChanged || mandatoryChanged || address != a.lastAddress {
			b.pushAgentChangedLocked(a)
		}
	}
}

// memberLocked builds a's roster entry; pinned marks a name that belongs to
// another account. Callers must hold Broker.mu.
func (b *Broker) memberLocked(a *agent, ref string, pinned bool) Member {
	return Member{
		Name:        a.name,
		Ref:         ref,
		Status:      topicStatus(a.status),
		PrevName:    a.prevName,
		Account:     topic.SanitizeAccountName(callerAccount(a.caller)),
		RefRequired: pinned,
	}
}

// unregisterLocked removes a from the table and its topic memberships and
// returns the watch stops to run outside Broker.mu. Callers must hold
// Broker.mu.
func (b *Broker) unregisterLocked(a *agent) []func() {
	delete(b.agents, a.id)
	b.releaseNameOwnerLocked(a.name, a.caller.UID, b.deps.Clock.Now())
	b.deps.Topics.LeaveAll(a.id)

	names := make([]string, 0, len(a.public))
	for name := range a.public {
		names = append(names, name)
	}
	b.retireOrphanedPublicTopicsLocked(a, names)

	var stops []func()
	if a.prevTimer != nil {
		a.prevTimer.Stop()
		a.prevTimer = nil
	}
	stops = append(stops, b.releaseWatchLocked(a.watchDir, a.watching)...)
	a.watching = false
	a.watchDir = watchKey{}

	b.recomputeRefsLocked()
	b.gone = append(b.gone, a.id)
	b.log.Info(EvAgentUnregistered, "agent", shortID(a.id), "name", a.name)
	return stops
}

// retireOrphanedPublicTopicsLocked retires the public topics in names that no
// other agent uses and clears them from a. Callers must hold Broker.mu.
func (b *Broker) retireOrphanedPublicTopicsLocked(a *agent, names []string) {
	for _, name := range names {
		if b.publicTopicInUseLocked(name) {
			continue
		}
		if b.deps.Directory != nil {
			b.deps.Directory.RetirePublicTopic(name)
		}
	}
	for _, name := range names {
		delete(a.public, name)
	}
}

// publicTopicInUseLocked reports whether any agent has declared the public
// topic name. Callers must hold Broker.mu.
func (b *Broker) publicTopicInUseLocked(name string) bool {
	for _, other := range b.agents {
		if _, ok := other.public[name]; ok {
			return true
		}
	}
	return false
}

// unlockAndNotify releases Broker.mu, which the caller must hold, then runs the
// watch stops and the unregistration hooks for agents that left the table.
func (b *Broker) unlockAndNotify(stops []func()) {
	gone := b.gone
	b.gone = nil
	b.mu.Unlock()
	for _, stop := range stops {
		stop()
	}
	for _, id := range gone {
		for _, f := range b.unregHooks {
			f(id)
		}
	}
}

// acquireWatchLocked takes a reference on the inbox watch of configDir for uid,
// creating the watch on the first reference. The caller must hold Broker.mu on
// entry and it returns with Broker.mu held; ok is false when configDir does not
// belong to uid or the watch could not be created. The ownership check and
// Watch run outside Broker.mu.
func (b *Broker) acquireWatchLocked(uid uint32, configDir string, backend SessionBackend) (key watchKey, ok bool) {
	if v, verifies := backend.(configDirVerifier); verifies {
		// The check reads the account database and stats a caller-chosen path,
		// so it runs bounded and outside Broker.mu.
		b.mu.Unlock()
		err := verifyConfigDirBounded(b.ctx, uid, &b.configDirChecks, v, configDir)
		b.mu.Lock()
		if err != nil {
			b.log.Error(EvRegistryWatchFailed, "config_dir", configDir, "uid", uid, "error", err)
			return watchKey{}, false
		}
	}
	key = watchKeyFor(uid, configDir)
	for {
		if w, exists := b.watches[key]; exists {
			if w.creating {
				wait := w.wait
				b.mu.Unlock()
				<-wait
				b.mu.Lock()
				continue
			}
			w.refs++
			return key, true
		}

		w := &dirWatch{refs: 1, creating: true, wait: make(chan struct{})}
		b.watches[key] = w
		b.mu.Unlock()
		callCtx, cancel := sessionCallContext(b.ctx)
		stop, err := backend.Watch(callCtx, key.dir, func(pid int) {
			b.events.push(uid, key.dir, pid)
		})
		cancel()
		b.mu.Lock()
		w.creating = false
		if err != nil {
			delete(b.watches, key)
			close(w.wait)
			b.log.Error(EvRegistryWatchFailed, "config_dir", key.dir, "uid", uid, "error", err)
			return key, false
		}
		w.stop = stop
		close(w.wait)
		return key, true
	}
}

// releaseWatchLocked drops one reference on key and returns the stop to run
// outside Broker.mu when the last reference goes; present reports whether the
// caller holds a reference.
func (b *Broker) releaseWatchLocked(key watchKey, present bool) []func() {
	if !present {
		return nil
	}
	w, ok := b.watches[key]
	if !ok {
		return nil
	}
	w.refs--
	if w.refs > 0 {
		return nil
	}
	delete(b.watches, key)
	if w.stop != nil {
		return []func(){w.stop}
	}
	return nil
}

// topicStatus maps a session registry status to the value the topic table
// carries; unknown values become the empty string.
func topicStatus(s string) string {
	switch s {
	case "idle", "busy", "waiting", "shell":
		return s
	default:
		return ""
	}
}

func statusToProto(s string) localpb.AgentStatus {
	switch s {
	case "idle":
		return localpb.AgentStatus_AGENT_STATUS_IDLE
	case "busy":
		return localpb.AgentStatus_AGENT_STATUS_BUSY
	case "waiting":
		return localpb.AgentStatus_AGENT_STATUS_WAITING
	case "shell":
		return localpb.AgentStatus_AGENT_STATUS_SHELL
	default:
		return localpb.AgentStatus_AGENT_STATUS_UNSPECIFIED
	}
}

// shortID returns the first 12 characters of id, or id itself when shorter.
func shortID(id AgentID) string {
	if len(id) > 12 {
		return string(id[:12])
	}
	return string(id)
}
