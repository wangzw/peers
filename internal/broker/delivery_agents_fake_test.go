package broker

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"sync"
	"testing"

	"github.com/wangzw/peers/internal/inbox"
	"github.com/wangzw/peers/internal/testkit/fakesession"
)

// fakeAgents is a localAgents fake whose Reread mirrors Broker.rereadAgent: an
// unknown or vanished record is errAgentGone, a partial record keeps the last
// snapshot, and a complete record replaces it with the normalized name.
type fakeAgents struct {
	mu   sync.Mutex
	byID map[string]agentView
	// rereadErr forces Reread's result for an ID; nil reports a successful reread
	// returning the current snapshot.
	rereadErr map[string]error
}

func newFakeAgents(views ...agentView) *fakeAgents {
	f := &fakeAgents{byID: make(map[string]agentView, len(views))}
	for _, view := range views {
		f.byID[view.ID] = view
	}
	return f
}

func (f *fakeAgents) Lookup(id string) (agentView, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	view, ok := f.byID[id]
	return view, ok
}

// All returns the snapshots sorted by ID, like Broker.allAgents.
func (f *fakeAgents) All() []agentView {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]agentView, 0, len(f.byID))
	for _, view := range f.byID {
		out = append(out, view)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (f *fakeAgents) Reread(id string) (agentView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	view, ok := f.byID[id]
	if !ok {
		return agentView{}, errAgentGone
	}
	if err, forced := f.rereadErr[id]; forced {
		if err == nil {
			return view, nil
		}
		return f.applyRereadLocked(id, view, inbox.Session{}, err)
	}
	next, err := inbox.Reread(view.Session)
	return f.applyRereadLocked(id, view, next, err)
}

// applyRereadLocked stores the reread result as Broker.refreshAgent does; f.mu
// must be held.
func (f *fakeAgents) applyRereadLocked(id string, view agentView, next inbox.Session, err error) (agentView, error) {
	switch {
	case err == nil:
		view.Session = next
		view.Name = inbox.NormalizeName(next.Name)
		f.byID[id] = view
		return view, nil
	case errors.Is(err, inbox.ErrPartial):
		return view, nil
	case errors.Is(err, inbox.ErrNoRecord):
		delete(f.byID, id)
		return agentView{}, errAgentGone
	default:
		return agentView{}, err
	}
}

func testAgent(name, socketPath string) agentView {
	sum := sha256.Sum256([]byte("session:" + socketPath))
	id := hex.EncodeToString(sum[:])
	return agentView{
		ID:      id,
		Name:    inbox.NormalizeName(name),
		Ref:     id[:6],
		Session: inbox.Session{SocketPath: socketPath},
		Inbox:   inboxPackage{},
	}
}

func liveTestAgent(t *testing.T, fake *fakesession.Fake, name string) agentView {
	t.Helper()
	view := testAgent(name, fake.SocketPath())
	session, err := inbox.Lookup(fake.ConfigDir(), fake.SocketPath())
	if err != nil {
		t.Fatalf("inbox.Lookup: %v", err)
	}
	view.Session = session
	return view
}

var _ localAgents = (*fakeAgents)(nil)

// TestFakeAgentsRereadGone checks that the fake reports a removed record as
// errAgentGone, as the Broker does.
func TestFakeAgentsRereadGone(t *testing.T) {
	fake := fakesession.New(t, fakesession.Options{Name: "api-worker"})
	view := liveTestAgent(t, fake, "api-worker")
	agents := newFakeAgents(view)

	fake.RemoveRecord()

	if _, err := agents.Reread(view.ID); !errors.Is(err, errAgentGone) {
		t.Fatalf("Reread after RemoveRecord = %v, want errAgentGone", err)
	}
}

// TestFakeAgentsRereadRenamed checks that the fake applies a renamed record's new
// name through inbox.NormalizeName.
func TestFakeAgentsRereadRenamed(t *testing.T) {
	fake := fakesession.New(t, fakesession.Options{Name: "api-worker"})
	view := liveTestAgent(t, fake, "api-worker")
	agents := newFakeAgents(view)

	fake.Rename("API Worker")

	got, err := agents.Reread(view.ID)
	if err != nil {
		t.Fatalf("Reread after rename: %v", err)
	}
	if got.Name != "api-worker" {
		t.Fatalf("Name after rename = %q, want %q", got.Name, "api-worker")
	}
	if got.Session.Name != "API Worker" {
		t.Fatalf("Session.Name after rename = %q, want the record value %q", got.Session.Name, "API Worker")
	}
}
