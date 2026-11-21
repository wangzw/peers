package mcpserver

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
)

const (
	// topicRPCTimeout bounds the whole of one topic tool call, including every
	// Broker call a nameless call makes for its default topics, so the call
	// returns instead of holding the topic lock while the Broker does not
	// answer.
	topicRPCTimeout = 10 * time.Second
	// topicRestoreTimeout bounds one attempt at joining a remembered topic,
	// including the fallback join made without a public declaration.
	topicRestoreTimeout = 10 * time.Second
	// topicRestoreBackoffInitial is the first wait before a pass retries the
	// topics a quota refused. The Broker's topic rate limiter refills one token
	// every 250ms (4 per second), so this is the shortest wait that can let a
	// join refused for being too frequent through.
	topicRestoreBackoffInitial = 250 * time.Millisecond
	// topicRestoreBackoffMax caps the wait between passes. A quota refusal may
	// be a topic count limit, which clears only when a slot frees, so the retry
	// never stops while the registration stays valid. At this cap a refused
	// topic costs one Broker request every 30 seconds at most, which is
	// negligible.
	topicRestoreBackoffMax = 30 * time.Second
	// topicRestoreWarnAfter is how many consecutive quota refusals of one topic
	// pass before the restore logs one warning. Later refusals of the same topic
	// in the same registration do not log again.
	topicRestoreWarnAfter = 3
)

// topicMemory remembers the topics this process joined, in insertion order. A
// later registration restores the membership from it.
type topicMemory struct {
	// mu protects topics.
	mu sync.Mutex
	// ops is a one-slot semaphore that serializes a Broker membership call with
	// the memory write it causes. The topic tools and the restorer both take
	// it, so a join or leave cannot interleave with the restore of the same
	// topic. A semaphore, not a mutex, lets an acquisition wait on a context:
	// a call stops instead of queueing behind a stuck one.
	ops     chan struct{}
	opsOnce sync.Once
	topics  []rememberedTopic
}

// lockTopic and unlockTopic serialize a Broker membership call and the memory
// write it causes. The topic tools and the restorer hold this lock across
// joining or leaving one topic. lockTopic returns the context's error instead
// of waiting when ctx ends first.
func (m *topicMemory) lockTopic(ctx context.Context) error {
	m.opsOnce.Do(func() { m.ops = make(chan struct{}, 1) })
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case m.ops <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *topicMemory) unlockTopic() { <-m.ops }

// get returns the remembered entry with the given name.
func (m *topicMemory) get(name string) (rememberedTopic, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range m.topics {
		if t.Name == name {
			return t, true
		}
	}
	return rememberedTopic{}, false
}

// Remember replaces the entry with the same name in place, or appends a new one.
func (m *topicMemory) Remember(t rememberedTopic) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.topics {
		if m.topics[i].Name == t.Name {
			m.topics[i] = t
			return
		}
	}
	m.topics = append(m.topics, t)
}

// Forget drops the entry with the given name, if any.
func (m *topicMemory) Forget(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.topics {
		if m.topics[i].Name == name {
			m.topics = append(m.topics[:i], m.topics[i+1:]...)
			return
		}
	}
}

// List returns a copy of the remembered topics in insertion order.
func (m *topicMemory) List() []rememberedTopic {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]rememberedTopic, len(m.topics))
	copy(out, m.topics)
	return out
}

// restorer joins the default topics after the first registration and restores
// the remembered topics after every registration. onComplete matches the
// completeFunc signature; holding mu keeps two completions from interleaving.
type restorer struct {
	mem      *topicMemory
	defaults func(ctx context.Context, brokerAutoJoin bool) defaultLookup
	log      *slog.Logger
	// rpcTimeout bounds one attempt at joining a topic. Zero means
	// topicRestoreTimeout; tests set a short value.
	rpcTimeout time.Duration
	// wait pauses the restore between two passes over the quota-refused topics.
	// A nil value uses a real timer bounded by the registration context; tests
	// substitute one that moves a fake clock.
	wait func(ctx context.Context, d time.Duration) error

	mu          sync.Mutex
	initialized bool
}

// onComplete joins the default topics on the first call, then restores every
// remembered topic. ctx lives as long as the registration the call belongs to,
// not as long as the Register stream: the caller cancels it when that
// registration is revoked or replaced, and the restore then stops without
// dropping an entry.
func (r *restorer) onComplete(ctx context.Context, c localpb.LocalClient, agentID string, autoJoin bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// The first registration computes the defaults once; a later one restores
	// what the memory holds, which already includes them.
	if !r.initialized {
		res := r.defaults(ctx, autoJoin)
		// An interrupted registration may have computed the defaults with a
		// cancelled context, in which case the lookup failed and the memory is
		// incomplete; there is nothing to restore on this connection either.
		if ctx.Err() != nil {
			return
		}
		// Record the topics before joining them, so an interrupted join is
		// restored by the next registration.
		for _, name := range res.topics {
			r.mem.Remember(rememberedTopic{Name: name})
		}
		// A lookup that did not settle, such as a git command that timed out,
		// leaves the defaults unknown. Leave initialized false so the next
		// registration computes them again instead of losing them for the life
		// of the process.
		if res.settled {
			r.initialized = true
		}
	}

	r.restoreRemembered(ctx, c, agentID)
}

// restoreRemembered joins every remembered topic. A topic the Broker refuses
// for quota goes into a deferred set and the pass continues with the topics
// behind it, so one topic stuck at the topic count limit cannot hold up the
// topics that do fit. After a pass the deferred topics are retried as a whole,
// after the same exponential backoff and jitter a single topic used. The whole
// restore is one loop over rounds; a deferred topic waits for its round rather
// than getting a goroutine of its own.
func (r *restorer) restoreRemembered(ctx context.Context, c localpb.LocalClient, agentID string) {
	remembered := r.mem.List()
	pending := make([]string, 0, len(remembered))
	for _, t := range remembered {
		pending = append(pending, t.Name)
	}
	backoff := topicRestoreBackoffInitial
	refusals := make(map[string]int)
	warned := make(map[string]bool)

	for len(pending) > 0 {
		var deferred []string
		for _, name := range pending {
			if ctx.Err() != nil {
				return
			}
			outcome, err := r.restoreOnce(ctx, c, agentID, name)
			switch outcome {
			case restoreStop:
				return
			case restoreQuota:
				deferred = append(deferred, name)
				refusals[name]++
				if refusals[name] == topicRestoreWarnAfter && !warned[name] {
					warned[name] = true
					r.warnKept(name, err)
				}
			}
		}
		if len(deferred) == 0 {
			return
		}
		pending = deferred
		if err := r.waitFor(ctx, topicRestoreJitter(backoff)); err != nil {
			// The connection ended or the registration was revoked; the entries
			// stay for the next registration.
			return
		}
		backoff = nextTopicRestoreBackoff(backoff)
	}
}

// restoreOutcome is what one attempt at joining a remembered topic decided.
type restoreOutcome int

const (
	// restoreFinished means the topic was joined, or rejected for good and
	// forgotten: nothing more to do for it during this registration.
	restoreFinished restoreOutcome = iota
	// restoreQuota means the Broker refused the join with quota_exceeded; the
	// topic is deferred and retried in the next round while the registration
	// stays valid.
	restoreQuota
	// restoreStop means the restore must stop and keep every entry: the
	// connection broke, the registration was revoked, or its context ended.
	restoreStop
)

// restoreOnce makes one attempt at joining one remembered topic and reports
// what the caller should do next. The topic lock is held for the attempt and
// released before any backoff, so a topic tool call can run while the round
// waits, and the memory is read again for every attempt: a tool may have left
// the topic since the last one, in which case it is skipped, or changed its
// public declaration or description, in which case the memory wins.
func (r *restorer) restoreOnce(ctx context.Context, c localpb.LocalClient, agentID, name string) (restoreOutcome, error) {
	if err := r.mem.lockTopic(ctx); err != nil {
		return restoreStop, err
	}
	t, ok := r.mem.get(name)
	if !ok {
		r.mem.unlockTopic()
		return restoreFinished, nil
	}
	outcome, err := r.attempt(ctx, c, agentID, t)
	r.mem.unlockTopic()
	return outcome, err
}

// attempt joins t once. A public declaration rejected for a reason other than a
// quota is retried without it, and the private form replaces the memory entry
// when that join succeeds or is refused only for quota. A rejection that says
// the agent registration no longer exists stops the restore and keeps the entry.
// Any other rejection with an ErrorDetail is permanent, so the entry is dropped.
func (r *restorer) attempt(ctx context.Context, c localpb.LocalClient, agentID string, t rememberedTopic) (restoreOutcome, error) {
	err := r.joinOnce(ctx, c, agentID, t)
	if err == nil {
		return restoreFinished, nil
	}
	d := errorDetail(err)
	if d == nil {
		return restoreStop, err
	}
	if isQuota(d) {
		return restoreQuota, err
	}
	if isRegistrationGone(d) {
		return restoreStop, err
	}
	if !t.Public {
		r.mem.Forget(t.Name)
		r.warnDropped(t.Name, err)
		return restoreFinished, err
	}
	t.Public = false
	err = r.joinOnce(ctx, c, agentID, t)
	if err == nil {
		r.mem.Remember(t)
		return restoreFinished, nil
	}
	d = errorDetail(err)
	if d == nil {
		return restoreStop, err
	}
	if isQuota(d) {
		// The public declaration is permanently rejected, but the private join
		// is refused only by a quota: keep the private form and retry it.
		r.mem.Remember(t)
		return restoreQuota, err
	}
	if isRegistrationGone(d) {
		// The fallback could not run because the registration is gone: keep the
		// original public form for the next registration.
		return restoreStop, err
	}
	r.mem.Forget(t.Name)
	r.warnDropped(t.Name, err)
	return restoreFinished, err
}

// joinOnce makes one JoinTopic call bounded by the per-attempt timeout.
func (r *restorer) joinOnce(ctx context.Context, c localpb.LocalClient, agentID string, t rememberedTopic) error {
	timeout := r.rpcTimeout
	if timeout <= 0 {
		timeout = topicRestoreTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	_, err := c.JoinTopic(ctx, joinTopicRequest(agentID, t))
	return err
}

// nextTopicRestoreBackoff doubles d up to topicRestoreBackoffMax.
func nextTopicRestoreBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > topicRestoreBackoffMax {
		d = topicRestoreBackoffMax
	}
	return d
}

// topicRestoreJitter scales a backoff by a random factor in [0.5, 1.5), so
// sessions refused at the same moment do not retry in lockstep and keep
// competing for the same token. The scaled wait is then clamped to the same
// range every other backoff uses: jitter never pushes a wait past
// topicRestoreBackoffMax or below topicRestoreBackoffInitial.
func topicRestoreJitter(d time.Duration) time.Duration {
	jittered := time.Duration(float64(d) * (0.5 + rand.Float64()))
	if jittered > topicRestoreBackoffMax {
		return topicRestoreBackoffMax
	}
	if jittered < topicRestoreBackoffInitial {
		return topicRestoreBackoffInitial
	}
	return jittered
}

// waitFor waits d, or returns early with the context's error when it ends.
func (r *restorer) waitFor(ctx context.Context, d time.Duration) error {
	if r.wait != nil {
		return r.wait(ctx, d)
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// isQuota reports whether d is the Broker's quota_exceeded refusal.
func isQuota(d *localpb.ErrorDetail) bool {
	return d.GetCode() == localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED
}

// isRegistrationGone reports whether d says the agent this restore joins as no
// longer exists. The Broker returns NOT_REGISTERED once the session record was
// revoked, including when that revocation has not reached the Register stream
// yet. It is not a rejection of the topic: the entry is kept for the next
// registration instead of being forgotten.
func isRegistrationGone(d *localpb.ErrorDetail) bool {
	return d.GetCode() == localpb.ErrorCode_ERROR_CODE_NOT_REGISTERED
}

func (r *restorer) warnKept(name string, err error) {
	if r.log != nil {
		r.log.Warn("mcpserver: a topic is still refused by quota; retrying it with backoff while the connection stays up", "topic", name, "error", err)
	}
}

func (r *restorer) warnDropped(name string, err error) {
	if r.log != nil {
		r.log.Warn("mcpserver: dropping a topic that could not be restored", "topic", name, "error", err)
	}
}

// joinTopicRequest builds the Local request that joins t as agentID.
func joinTopicRequest(agentID string, t rememberedTopic) *localpb.JoinTopicRequest {
	return &localpb.JoinTopicRequest{
		AgentId:     agentID,
		Name:        t.Name,
		IsPublic:    t.Public,
		Description: t.Description,
	}
}
