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
	// topicRPCTimeout bounds every Broker call a topic tool call makes after it
	// takes the topic lock, including the calls a nameless call makes for its
	// default topics, which share one deadline.
	topicRPCTimeout = 10 * time.Second
	// topicRestoreTimeout bounds each Broker JoinTopic call made to restore a
	// remembered topic, including the fallback join made without a public
	// declaration.
	topicRestoreTimeout = 10 * time.Second
	// topicRestoreBackoffInitial is the first wait before retrying topics a
	// quota refused; the Broker's topic rate limiter refills one token every
	// 250ms.
	topicRestoreBackoffInitial = 250 * time.Millisecond
	// topicRestoreBackoffMax caps the wait between retry passes.
	topicRestoreBackoffMax = 30 * time.Second
	// topicRestoreWarnAfter is the number of consecutive quota refusals after
	// which the restore logs one warning for a topic.
	topicRestoreWarnAfter = 3
)

type topicMemory struct {
	// mu protects topics.
	mu sync.Mutex
	// ops serializes a Broker membership call with the memory write it causes,
	// between the topic tools and the restorer. A semaphore, not a mutex, lets
	// an acquisition wait on the context.
	ops     chan struct{}
	opsOnce sync.Once
	topics  []rememberedTopic
}

// lockTopic and unlockTopic serialize a Broker membership call and the memory
// write it causes; lockTopic returns the context's error when ctx ends first.
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

type restorer struct {
	mem      *topicMemory
	defaults func(ctx context.Context, brokerAutoJoin bool) defaultLookup
	log      *slog.Logger
	// rpcTimeout bounds each Broker JoinTopic call of one attempt. Zero means
	// topicRestoreTimeout; tests set a short value.
	rpcTimeout time.Duration
	// wait pauses the restore between two passes over the quota-refused topics.
	// A nil value uses a real timer bounded by the registration context; tests
	// substitute one that moves a fake clock.
	wait func(ctx context.Context, d time.Duration) error

	// mu serializes onComplete calls.
	mu          sync.Mutex
	initialized bool
}

// onComplete joins the default topics on the first call, then restores every
// remembered topic. ctx lives as long as the registration the call belongs to,
// not as long as the Register stream: a cancelled registration stops the
// restore without dropping an entry.
func (r *restorer) onComplete(ctx context.Context, c localpb.LocalClient, agentID string, autoJoin bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.initialized {
		res := r.defaults(ctx, autoJoin)
		// A cancelled registration must not mark the defaults as settled.
		if ctx.Err() != nil {
			return
		}
		// Record the topics before joining them, so an interrupted join is
		// restored by the next registration.
		for _, name := range res.topics {
			r.mem.Remember(rememberedTopic{Name: name})
		}
		// A lookup that did not settle leaves the defaults unknown, so the next
		// registration computes them again.
		if res.settled {
			r.initialized = true
		}
	}

	r.restoreRemembered(ctx, c, agentID)
}

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
			// stay.
			return
		}
		backoff = nextTopicRestoreBackoff(backoff)
	}
}

type restoreOutcome int

// What one attempt at joining a remembered topic decided.
const (
	restoreFinished restoreOutcome = iota
	restoreQuota
	// restoreStop stops the restore and keeps every entry for the next
	// registration.
	restoreStop
)

// restoreOnce makes one attempt at joining one remembered topic. The topic lock
// is held for the attempt and released before any backoff, and the memory is
// read again on every attempt.
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
		r.mem.Remember(t)
		return restoreQuota, err
	}
	if isRegistrationGone(d) {
		// Keep the original public form for the next registration.
		return restoreStop, err
	}
	r.mem.Forget(t.Name)
	r.warnDropped(t.Name, err)
	return restoreFinished, err
}

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

func nextTopicRestoreBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > topicRestoreBackoffMax {
		d = topicRestoreBackoffMax
	}
	return d
}

// topicRestoreJitter scales d by a random factor in [0.5, 1.5) and clamps the
// result to the restore bounds, so sessions refused together do not retry in
// lockstep.
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

func isQuota(d *localpb.ErrorDetail) bool {
	return d.GetCode() == localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED
}

// isRegistrationGone reports whether d says the agent this restore joins as no
// longer exists; the entry is kept for the next registration.
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

func joinTopicRequest(agentID string, t rememberedTopic) *localpb.JoinTopicRequest {
	return &localpb.JoinTopicRequest{
		AgentId:     agentID,
		Name:        t.Name,
		IsPublic:    t.Public,
		Description: t.Description,
	}
}
