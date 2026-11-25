package broker

import (
	"context"
	"sync"
	"time"

	"github.com/wangzw/peers/internal/inbox"
	"github.com/wangzw/peers/internal/pairing"
)

type deliveryRemoteEntry struct {
	Member remoteMember
	Topics []string
}

type deliveryTopics struct {
	local  map[string][]string
	remote map[peerID][]deliveryRemoteEntry
}

func (t *deliveryTopics) VisibleLocal(a, b string) bool {
	shared := false
	for _, ta := range t.local[a] {
		for _, tb := range t.local[b] {
			if ta == tb {
				shared = true
				break
			}
		}
	}
	return shared
}

func (t *deliveryTopics) VisibleRemote(local string, peer peerID, name, ref string) bool {
	for _, e := range t.remote[peer] {
		if e.Member.Name != name || e.Member.Ref != ref {
			continue
		}
		for _, remoteTopic := range e.Topics {
			for _, localTopic := range t.local[local] {
				if remoteTopic == localTopic {
					return true
				}
			}
		}
	}
	return false
}

func (t *deliveryTopics) RemoteMembers(peer peerID) []remoteMember {
	entries := t.remote[peer]
	members := make([]remoteMember, 0, len(entries))
	for _, e := range entries {
		members = append(members, e.Member)
	}
	return members
}

type deliveryPeers struct {
	aliases   map[string]peerID
	roles     map[peerID]pairing.Role
	effective map[peerID]string
}

func (t *deliveryPeers) ResolveAlias(alias string) (peerID, bool) {
	p, ok := t.aliases[alias]
	return p, ok
}

func (t *deliveryPeers) EffectiveAlias(p peerID) string {
	return t.effective[p]
}

func (t *deliveryPeers) Role(p peerID) pairing.Role {
	if role, ok := t.roles[p]; ok {
		return role
	}
	return pairing.RoleUnknown
}

type fakeSettings struct {
	mu       sync.Mutex
	settings sendSettings
	alias    string
	offline  bool
}

func newFakeSettings(settings sendSettings, alias string) *fakeSettings {
	return &fakeSettings{settings: settings, alias: alias}
}

func (s *fakeSettings) Send() sendSettings {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.settings
}

func (s *fakeSettings) LocalAlias() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.alias
}

func (s *fakeSettings) Offline() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.offline
}

// SetOffline changes the broker.offline value a test sees.
func (s *fakeSettings) SetOffline(offline bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offline = offline
}

// SetSettings replaces the send settings.
func (s *fakeSettings) SetSettings(settings sendSettings) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.settings = settings
}

// SetLocalAlias replaces the local alias.
func (s *fakeSettings) SetLocalAlias(alias string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.alias = alias
}

type inboxCall struct {
	Sig      string
	Text     string
	MsgID    string
	Timeout  time.Duration
	At       time.Time // when the call started
	Deadline time.Time // the deadline carried by the call's context
}

type recordingInbox struct {
	mu      sync.Mutex
	results []error
	next    int
	calls   []inboxCall
}

func newRecordingInbox(results ...error) *recordingInbox {
	return &recordingInbox{results: results}
}

func (r *recordingInbox) Deliver(ctx context.Context, _ inbox.Session, sig, text, msgID string, timeout time.Duration) error {
	// The time and the deadline are read before taking the lock so At marks the
	// call's start and not its wait for an earlier call to finish.
	at := time.Now()
	deadline, _ := ctx.Deadline()

	r.mu.Lock()
	defer r.mu.Unlock()

	r.calls = append(r.calls, inboxCall{
		Sig:      sig,
		Text:     text,
		MsgID:    msgID,
		Timeout:  timeout,
		At:       at,
		Deadline: deadline,
	})

	if len(r.results) == 0 {
		return nil
	}
	result := r.results[r.next]
	if r.next < len(r.results)-1 {
		r.next++
	}
	return result
}

func (r *recordingInbox) Calls() []inboxCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]inboxCall(nil), r.calls...)
}

func (r *recordingInbox) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *recordingInbox) SetResults(results ...error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.results = results
	r.next = 0
}

var (
	_ visibility     = (*deliveryTopics)(nil)
	_ peerTable      = (*deliveryPeers)(nil)
	_ settingsSource = (*fakeSettings)(nil)
	_ inboxWriter    = (*recordingInbox)(nil)

	// The production peer table must keep satisfying the consumer interface.
	_ peerTable = (*pairing.Pairing)(nil)
)
