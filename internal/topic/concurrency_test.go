package topic

import (
	"fmt"
	"math/rand/v2"
	"runtime"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/topic/topicpb"
)

// TestSendHappensUnderLock checks lock rule three: every frame is handed to the
// transport's send methods while the caller holds Registry.mu. A send hook tries
// to take the lock and fails the test when it succeeds, which happens exactly
// when an event collected frames under the lock and sent them after releasing it.
func TestSendHappensUnderLock(t *testing.T) {
	la := newFakeLink(t, fakeLinkFingerprint(1))
	lb := newFakeLink(t, fakeLinkFingerprint(2))
	ra, _ := newLinkRegistry(t, la)
	rb, _ := newLinkRegistry(t, lb)

	a := AgentID("a")
	ra.AgentChanged(a, Member{Name: "api", Ref: "a10003", Status: "idle"})
	if err := ra.Join(a, "t1"); err != nil {
		t.Fatalf("join t1 for a: %v", err)
	}
	b := AgentID("b")
	rb.AgentChanged(b, Member{Name: "web", Ref: "b10003", Status: "idle"})
	if err := rb.Join(b, "t1"); err != nil {
		t.Fatalf("join t1 for b: %v", err)
	}

	var sends atomic.Int64
	la.setSendHook(func(link.Fingerprint, link.Frame) {
		sends.Add(1)
		if ra.mu.TryLock() {
			ra.mu.Unlock()
			t.Error("send called without holding the registry lock")
		}
	})

	// The initial exchange covers onUp and the inbound TopicDigests path.
	connect(la, lb, []byte("salt"))
	flushAll(t, la, lb)

	// Cover the four local operations, including a topic B also has so that
	// the deltas and rosters are not vacuous.
	ra.AgentChanged(a, Member{Name: "api2", Ref: "a10003", Status: "busy", PrevName: "api"})
	if err := ra.Join(a, "t2"); err != nil {
		t.Fatalf("join t2 for a: %v", err)
	}
	if err := rb.Join(b, "t2"); err != nil {
		t.Fatalf("join t2 for b: %v", err)
	}
	if err := ra.Leave(a, "t1"); err != nil {
		t.Fatalf("leave t1 for a: %v", err)
	}
	if err := ra.Join(a, "t1"); err != nil {
		t.Fatalf("rejoin t1 for a: %v", err)
	}
	ra.LeaveAll(a)
	flushAll(t, la, lb)

	if sends.Load() == 0 {
		t.Fatal("no Send happened; the lock check never ran")
	}
}

// TestCallbacksConcurrentWithOperations checks lock rules two and six: callbacks
// from the link run concurrently with local operations and queries, all of them
// complete, and -race reports no unsynchronized access.
func TestCallbacksConcurrentWithOperations(t *testing.T) {
	la := newFakeLink(t, fakeLinkFingerprint(1))
	lb := newFakeLink(t, fakeLinkFingerprint(2))
	ra, _ := newLinkRegistry(t, la)
	rb, _ := newLinkRegistry(t, lb)

	agentsA := concSeedSide(t, ra, "a")
	concSeedSide(t, rb, "b")
	connect(la, lb, []byte("salt-0"))
	flushAll(t, la, lb)

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w)+1, 7))
			owned := concOwned(agentsA, w)
			for i := 0; i < 200; i++ {
				concRandomOp(rng, ra, owned)
			}
		}(w)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			disconnect(la, lb)
			connect(la, lb, []byte(fmt.Sprintf("salt-%d", i+1)))
		}
	}()

	for q := 0; q < 2; q++ {
		wg.Add(1)
		go func(q int) {
			defer wg.Done()
			local := agentsA[q%len(agentsA)].id
			other := AgentRef{ID: agentsA[(q+1)%len(agentsA)].id}
			remote := AgentRef{Peer: lb.self, Ref: "b00000"}
			for i := 0; i < 3000; i++ {
				ra.TopicsOf(local)
				ra.Visible(local, other)
				ra.Visible(local, remote)
				ra.VisibleAgents(local)
				ra.RemoteMembers(lb.self)
			}
		}(q)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(eventTimeout):
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		t.Fatalf("concurrent operations did not finish within %s:\n%s", eventTimeout, buf[:n])
	}

	flushAll(t, la, lb)
}

// TestConvergence drives both registries with random concurrent operations and
// checks that each side's remote view equals the other side's local membership
// inside the topics they share. The reconnect variant repeats the run while the
// link is torn down and rebuilt with a fresh salt.
func TestConvergence(t *testing.T) {
	for seed := int64(1); seed <= 5; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			t.Logf("seed %d", seed)
			t.Run("steady", func(t *testing.T) {
				concRunConvergence(t, seed, false)
			})
			t.Run("reconnect", func(t *testing.T) {
				concRunConvergence(t, seed, true)
			})
		})
	}
}

// TestOldConnectionFramesDoNotPolluteNewLink blocks B's scheduler on the first
// frame of the old connection while A keeps changing and sending, then tears the
// link down and rebuilds it. Frames queued under the old generation must not
// survive the rebuild into the new table.
func TestOldConnectionFramesDoNotPolluteNewLink(t *testing.T) {
	la := newFakeLink(t, fakeLinkFingerprint(1))
	lb := newFakeLink(t, fakeLinkFingerprint(2))
	ra, _ := newLinkRegistry(t, la)
	rb, _ := newLinkRegistry(t, lb)

	agentsA := concSeedSide(t, ra, "a")
	concSeedSide(t, rb, "b")

	release := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once

	lb.mu.Lock()
	orig := lb.handlers[link.FrameKind(topicpb.KindTopicDigests)]
	lb.mu.Unlock()
	if orig == nil {
		t.Fatal("fake link has no TopicDigests handler after New")
	}
	blocking := func(peer link.Fingerprint, f link.Frame) {
		once.Do(func() {
			close(started)
			<-release
		})
		orig(peer, f)
	}
	lb.mu.Lock()
	lb.handlers[link.FrameKind(topicpb.KindTopicDigests)] = blocking
	lb.mu.Unlock()

	connect(la, lb, []byte("old-salt"))
	select {
	case <-started:
	case <-time.After(eventTimeout):
		t.Fatal("the first frame never reached B's handler")
	}

	// Change A while B's scheduler is stuck on the old connection's first frame.
	// Every frame these produce is queued at B under the old generation.
	for i := 0; i < 5; i++ {
		ag := agentsA[i%len(agentsA)]
		ra.AgentChanged(ag.id, Member{Name: fmt.Sprintf("%s-new%d", ag.base, i), Ref: ag.ref, Status: "busy", PrevName: ag.base})
		_ = ra.Join(ag.id, fmt.Sprintf("t%d", (i+1)%5))
		_ = ra.Leave(ag.id, fmt.Sprintf("t%d", i%5))
	}

	disconnect(la, lb)
	connect(la, lb, []byte("new-salt"))
	close(release)
	flushAll(t, la, lb)

	concAssertConverged(t, ra, rb, lb.self)
	concAssertConverged(t, rb, ra, la.self)
}

// concAgent is one seeded local agent used by the randomized workloads.
type concAgent struct {
	id   AgentID
	ref  string
	base string
}

// concSeedSide introduces six agents and places each in one of the five topics,
// mirroring what the broker does before it calls Join.
func concSeedSide(t *testing.T, r *Registry, prefix string) []concAgent {
	t.Helper()
	agents := make([]concAgent, 6)
	for i := range agents {
		agents[i] = concAgent{
			id:   AgentID(fmt.Sprintf("%s%d", prefix, i)),
			ref:  fmt.Sprintf("%s%05x", prefix, i),
			base: fmt.Sprintf("%s-name%d", prefix, i),
		}
		r.AgentChanged(agents[i].id, Member{Name: agents[i].base, Ref: agents[i].ref, Status: "idle"})
		if err := r.Join(agents[i].id, fmt.Sprintf("t%d", i%5)); err != nil {
			t.Fatalf("seed join t%d for %s: %v", i%5, agents[i].id, err)
		}
	}
	return agents
}

// concOwned splits agents round-robin over four workers so no two workers drive
// the same agent's lifecycle.
func concOwned(agents []concAgent, w int) []concAgent {
	out := make([]concAgent, 0, len(agents)/4+1)
	for i := w; i < len(agents); i += 4 {
		out = append(out, agents[i])
	}
	return out
}

var concStatuses = []string{"idle", "busy", "waiting", "shell"}

// concRandomOp performs one of join, leave, info change, and leave-all followed
// by reintroduction. Errors are ignored on purpose: every agent belongs to one
// worker and the names are valid, so the call shape matches the registry's
// contract.
func concRandomOp(rng *rand.Rand, r *Registry, agents []concAgent) {
	ag := agents[rng.IntN(len(agents))]
	switch rng.IntN(4) {
	case 0:
		_ = r.Join(ag.id, fmt.Sprintf("t%d", rng.IntN(5)))
	case 1:
		_ = r.Leave(ag.id, fmt.Sprintf("t%d", rng.IntN(5)))
	case 2:
		name := ag.base
		if rng.IntN(2) == 0 {
			name = fmt.Sprintf("%s-v%d", ag.base, rng.IntN(1000))
		}
		prev := ""
		if rng.IntN(2) == 0 {
			prev = fmt.Sprintf("%s-old%d", ag.base, rng.IntN(3))
		}
		r.AgentChanged(ag.id, Member{Name: name, Ref: ag.ref, Status: concStatuses[rng.IntN(len(concStatuses))], PrevName: prev})
	case 3:
		r.LeaveAll(ag.id)
		r.AgentChanged(ag.id, Member{Name: ag.base, Ref: ag.ref, Status: concStatuses[rng.IntN(len(concStatuses))]})
	}
}

// concRunConvergence runs one convergence scenario for a seed and checks both
// directions once the links have drained.
func concRunConvergence(t *testing.T, seed int64, reconnecting bool) {
	t.Helper()
	la := newFakeLink(t, fakeLinkFingerprint(1))
	lb := newFakeLink(t, fakeLinkFingerprint(2))
	ra, _ := newLinkRegistry(t, la)
	rb, _ := newLinkRegistry(t, lb)

	agentsA := concSeedSide(t, ra, "a")
	agentsB := concSeedSide(t, rb, "b")
	connect(la, lb, []byte(fmt.Sprintf("seed-%d", seed)))
	flushAll(t, la, lb)

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(seed), uint64(w)+1))
			owned := concOwned(agentsA, w)
			for i := 0; i < 150; i++ {
				concRandomOp(rng, ra, owned)
			}
		}(w)

		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(seed), uint64(w)+100))
			owned := concOwned(agentsB, w)
			for i := 0; i < 150; i++ {
				concRandomOp(rng, rb, owned)
			}
		}(w)
	}

	if reconnecting {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				disconnect(la, lb)
				connect(la, lb, []byte(fmt.Sprintf("seed-%d-re%d", seed, i)))
			}
		}()
	}

	wg.Wait()

	// Leave a known shared membership behind so the convergence check cannot
	// pass on two empty views. Every agent still exists: the leave-all case
	// reintroduces its agent in the same operation.
	ra.AgentChanged(agentsA[0].id, Member{Name: agentsA[0].base, Ref: agentsA[0].ref, Status: "idle"})
	rb.AgentChanged(agentsB[0].id, Member{Name: agentsB[0].base, Ref: agentsB[0].ref, Status: "idle"})
	_ = ra.Join(agentsA[0].id, "t0")
	_ = rb.Join(agentsB[0].id, "t0")

	// The last reconnect iteration leaves the link up; drain the queues that
	// the final exchange produced.
	flushAll(t, la, lb)

	concAssertConverged(t, ra, rb, lb.self)
	concAssertConverged(t, rb, ra, la.self)
}

// concAssertConverged compares viewer's remote view of peer with the membership
// the checks can compute from both local tables.
func concAssertConverged(t *testing.T, viewer, peer *Registry, peerFP link.Fingerprint) {
	t.Helper()

	if got, want := viewer.RemoteMembers(peerFP), concExpectedRemoteMembers(t, viewer, peer); !slices.Equal(got, want) {
		t.Fatalf("RemoteMembers(%x) = %+v, want %+v", peerFP, got, want)
	}

	viewer.mu.RLock()
	ids := make([]AgentID, 0, len(viewer.local.agents))
	for id := range viewer.local.agents {
		ids = append(ids, id)
	}
	viewer.mu.RUnlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	for _, id := range ids {
		all := viewer.VisibleAgents(id)
		got := make([]VisibleAgent, 0, len(all))
		for _, v := range all {
			if v.Agent.Peer != (link.Fingerprint{}) {
				got = append(got, v)
			}
		}
		want := concExpectedVisibleRemote(t, viewer, peer, id, peerFP)
		if !concEqualVisible(got, want) {
			t.Fatalf("VisibleAgents(%q) remote = %+v, want %+v", id, got, want)
		}
	}
}

// concExpectedRemoteMembers returns peer's agents in the topics both sides have,
// deduplicated by Ref and ordered by Ref, taking the entry from the first topic
// by name.
func concExpectedRemoteMembers(t *testing.T, viewer, peer *Registry) []Member {
	t.Helper()

	viewer.mu.RLock()
	shared := viewer.local.topicNames()
	viewer.mu.RUnlock()

	peer.mu.RLock()
	defer peer.mu.RUnlock()

	first := map[string]Member{}
	for _, name := range shared {
		for id := range peer.local.topics[name] {
			a := peer.local.agents[id]
			if a == nil || a.info.Ref == "" {
				continue
			}
			if _, ok := first[a.info.Ref]; !ok {
				first[a.info.Ref] = a.info
			}
		}
	}

	out := make([]Member, 0, len(first))
	for _, m := range first {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ref < out[j].Ref })
	return out
}

// concExpectedVisibleRemote returns the remote half of viewer agent local's
// VisibleAgents, computed from peer's local table.
func concExpectedVisibleRemote(t *testing.T, viewer, peer *Registry, local AgentID, peerFP link.Fingerprint) []VisibleAgent {
	t.Helper()

	viewer.mu.RLock()
	var localTopics []string
	if a := viewer.local.agents[local]; a != nil {
		localTopics = querySortedTopics(a.topics)
	}
	viewer.mu.RUnlock()

	peer.mu.RLock()
	defer peer.mu.RUnlock()

	ids := make([]AgentID, 0, len(peer.local.agents))
	for id := range peer.local.agents {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	out := make([]VisibleAgent, 0)
	for _, id := range ids {
		a := peer.local.agents[id]
		if a.info.Ref == "" {
			continue
		}
		shared := concIntersect(localTopics, querySortedTopics(a.topics))
		if len(shared) == 0 {
			continue
		}
		out = append(out, VisibleAgent{
			Agent:  AgentRef{Peer: peerFP, Ref: a.info.Ref},
			Member: a.info,
			Shared: shared,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Agent.Ref < out[j].Agent.Ref })
	return out
}

// concIntersect returns the common names of two ascending lists, ascending.
func concIntersect(a, b []string) []string {
	var out []string
	for i, j := 0, 0; i < len(a) && j < len(b); {
		switch {
		case a[i] < b[j]:
			i++
		case a[i] > b[j]:
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	return out
}

// concEqualVisible compares VisibleAgent slices field by field.
func concEqualVisible(a, b []VisibleAgent) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Agent != b[i].Agent || a[i].Member != b[i].Member || !slices.Equal(a[i].Shared, b[i].Shared) {
			return false
		}
	}
	return true
}
