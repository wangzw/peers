package broker

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// fakeClock is a Clock whose time only moves through Advance. Callbacks run
// synchronously, in deadline order, outside the clock's lock.
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	seq     int
	created int
	timers  []*fakeTimer
}

type fakeTimer struct {
	clock   *fakeClock
	at      time.Time
	seq     int
	f       func()
	stopped bool
	fired   bool
}

func newFakeClock(now time.Time) *fakeClock { return &fakeClock{now: now} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) AfterFunc(d time.Duration, f func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{clock: c, at: c.now.Add(d), seq: c.seq, f: f}
	c.seq++
	c.created++
	c.timers = append(c.timers, t)
	return t
}

func (c *fakeClock) createdTimers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.created
}

func (c *fakeClock) pendingTimers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, t := range c.timers {
		if !t.stopped && !t.fired {
			n++
		}
	}
	return n
}

func (t *fakeTimer) Stop() bool {
	c := t.clock
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.stopped || t.fired {
		return false
	}
	t.stopped = true
	return true
}

// Advance moves the clock forward and runs every due callback, earliest deadline
// first.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	due := make([]*fakeTimer, 0, len(c.timers))
	keep := c.timers[:0]
	for _, t := range c.timers {
		if t.stopped || t.fired {
			continue
		}
		if t.at.After(now) {
			keep = append(keep, t)
			continue
		}
		t.fired = true
		due = append(due, t)
	}
	c.timers = keep
	c.mu.Unlock()

	sort.SliceStable(due, func(i, j int) bool {
		if due[i].at.Equal(due[j].at) {
			return due[i].seq < due[j].seq
		}
		return due[i].at.Before(due[j].at)
	})
	for _, t := range due {
		t.f()
	}
}

type fakeLoadCall struct {
	Mode           config.Mode
	ListenOverride string
}

type setCall struct {
	Actor config.Actor
	Key   string
	Value config.Value
}

// fakeConfig implements ConfigSource without touching the process-wide config
// singleton.
type fakeConfig struct {
	mu              sync.Mutex
	runtime         string
	runtimeFallback *config.RuntimeDirFallback
	lockPath        string
	lockCandidates  []string

	values      map[string]config.Value
	secretKeys  map[string]bool
	validators  map[string][]func(old, new config.Value) error
	subscribers map[string][]func(old, new config.Value)
	advertised  []func(old, new []config.AdvertisedAddr)
	audits      []config.Event
	setActors   []config.Actor
	setCalls    []setCall
	parseCalls  int

	subscribedKeys []string
	configFile     string

	loads        []fakeLoadCall
	loadErr      error
	probeErr     error
	lowPortErr   error
	setErr       error
	parseErr     error
	readAuditErr error
	closeCount   int
}

var _ ConfigSource = (*fakeConfig)(nil)

func newFakeConfig(runtimeDir string) *fakeConfig {
	f := &fakeConfig{
		runtime:     runtimeDir,
		lockPath:    runtimeDir + "/broker.lock",
		values:      make(map[string]config.Value),
		secretKeys:  make(map[string]bool),
		validators:  make(map[string][]func(old, new config.Value) error),
		subscribers: make(map[string][]func(old, new config.Value)),
		configFile:  "/fake/config.yaml",
	}
	f.values["broker.alias"] = config.Value{Kind: config.KindString, String: "hostA"}
	f.values["broker.offline"] = config.Value{Kind: config.KindBool, Bool: false}
	f.values["broker.listen"] = config.Value{Kind: config.KindString}
	f.values["topics.auto_join"] = config.Value{Kind: config.KindBool, Bool: true}
	f.values["log.level"] = config.Value{Kind: config.KindString, String: "info"}
	return f
}

func (f *fakeConfig) setValue(key string, v config.Value) {
	f.mu.Lock()
	old := f.values[key]
	f.values[key] = v
	callbacks := append([]func(old, new config.Value){}, f.subscribers[key]...)
	f.mu.Unlock()
	for _, cb := range callbacks {
		cb(old, v)
	}
}

func (f *fakeConfig) setLoadErr(err error) {
	f.mu.Lock()
	f.loadErr = err
	f.mu.Unlock()
}

func (f *fakeConfig) LoadCalls() []fakeLoadCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeLoadCall(nil), f.loads...)
}

func (f *fakeConfig) CloseCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closeCount
}

func (f *fakeConfig) CheckRuntimeDir() error { return nil }

func (f *fakeConfig) AccountLock() string { return f.lockPath }

func (f *fakeConfig) AccountLockCandidates() []string {
	if len(f.lockCandidates) > 0 {
		return f.lockCandidates
	}
	return []string{f.lockPath}
}

func (f *fakeConfig) Load(mode config.Mode, listenOverride string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loads = append(f.loads, fakeLoadCall{Mode: mode, ListenOverride: listenOverride})
	return f.loadErr
}

func (f *fakeConfig) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCount++
}

func (f *fakeConfig) Paths() config.PathSet {
	f.mu.Lock()
	defer f.mu.Unlock()
	return config.PathSet{
		RuntimeDir:         f.runtime,
		RuntimeDirFallback: f.runtimeFallback,
		ConfigFile:         f.configFile,
	}
}

func (f *fakeConfig) Get(key string) (config.Value, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.secretKeys[key] {
		return config.Value{Kind: config.KindSecret, SecretIsSet: f.values[key].String != ""}, nil
	}
	v, ok := f.values[key]
	if !ok {
		return config.Value{}, fmt.Errorf("%w: %s", config.ErrUnknownKey, key)
	}
	return copyValue(v), nil
}

func (f *fakeConfig) All() []config.Entry {
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, 0, len(f.values))
	for k := range f.values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]config.Entry, 0, len(keys))
	for _, k := range keys {
		out = append(out, config.Entry{Key: k, Value: copyValue(f.values[k])})
	}
	return out
}

func (f *fakeConfig) Set(actor config.Actor, key string, v config.Value) (config.Value, error) {
	f.mu.Lock()
	f.setCalls = append(f.setCalls, setCall{Actor: actor, Key: key, Value: copyValue(v)})
	if f.setErr != nil {
		err := f.setErr
		f.mu.Unlock()
		return config.Value{}, err
	}
	old, ok := f.values[key]
	if !ok {
		f.mu.Unlock()
		return config.Value{}, fmt.Errorf("%w: %s", config.ErrUnknownKey, key)
	}
	for _, check := range f.validators[key] {
		if err := check(old, v); err != nil {
			f.mu.Unlock()
			return config.Value{}, fmt.Errorf("%w: %v", config.ErrInvalidValue, err)
		}
	}
	f.values[key] = v
	f.setActors = append(f.setActors, actor)
	callbacks := append([]func(old, new config.Value){}, f.subscribers[key]...)
	f.mu.Unlock()

	for _, cb := range callbacks {
		cb(old, v)
	}
	return old, nil
}

func (f *fakeConfig) ParseValue(key, text string) (config.Value, error) {
	f.mu.Lock()
	f.parseCalls++
	err := f.parseErr
	f.mu.Unlock()
	if err != nil {
		return config.Value{}, err
	}
	return config.ParseValue(key, text)
}

func (f *fakeConfig) Validate(key string, check func(old, new config.Value) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.validators[key] = append(f.validators[key], check)
}

func (f *fakeConfig) ProbeListen(string) error { return f.probeErr }

func (f *fakeConfig) CheckLowPort() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lowPortErr
}

func (f *fakeConfig) Subscribe(key string, onChange func(old, new config.Value)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subscribedKeys = append(f.subscribedKeys, key)
	f.subscribers[key] = append(f.subscribers[key], onChange)
}

func (f *fakeConfig) setValueQuietly(key string, v config.Value) {
	f.mu.Lock()
	f.values[key] = v
	f.mu.Unlock()
}

func (f *fakeConfig) fireKey(key string, old, new config.Value) {
	f.mu.Lock()
	callbacks := append([]func(old, new config.Value){}, f.subscribers[key]...)
	f.mu.Unlock()
	for _, cb := range callbacks {
		cb(old, new)
	}
}

func (f *fakeConfig) fireAdvertised(old, new []config.AdvertisedAddr) {
	f.mu.Lock()
	callbacks := append([]func(old, new []config.AdvertisedAddr){}, f.advertised...)
	f.mu.Unlock()
	for _, cb := range callbacks {
		cb(old, new)
	}
}

func (f *fakeConfig) SubscribeAdvertised(onChange func(old, new []config.AdvertisedAddr)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.advertised = append(f.advertised, onChange)
}

func (f *fakeConfig) Audit(e config.Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.audits = append(f.audits, e)
}

func (f *fakeConfig) ReadAudit(since time.Time) ([]config.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.readAuditErr != nil {
		return nil, f.readAuditErr
	}
	var out []config.Event
	for _, e := range f.audits {
		if !e.Time.Before(since) {
			out = append(out, e)
		}
	}
	return out, nil
}

func copyValue(v config.Value) config.Value {
	if v.Strings != nil {
		v.Strings = append([]string(nil), v.Strings...)
	}
	return v
}

type fakeSessions struct {
	mu           sync.Mutex
	acquiredUIDs []uint32
	releases     int

	noWatch    bool
	acquireErr error
}

var _ SessionRouter = (*fakeSessions)(nil)

func (f *fakeSessions) Acquire(ctx context.Context, uid uint32) (SessionBackend, func(), error) {
	f.mu.Lock()
	f.acquiredUIDs = append(f.acquiredUIDs, uid)
	err := f.acquireErr
	noWatch := f.noWatch
	f.mu.Unlock()
	if err != nil {
		return nil, nil, err
	}

	base, _, err := UserRouter().Acquire(ctx, uid)
	if err != nil {
		return nil, nil, err
	}
	if noWatch {
		base = noWatchBackend{SessionBackend: base}
	}
	release := func() {
		f.mu.Lock()
		f.releases++
		f.mu.Unlock()
	}
	return base, release, nil
}

func (f *fakeSessions) AcquiredUIDs() []uint32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uint32(nil), f.acquiredUIDs...)
}

func (f *fakeSessions) ReleaseCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.releases
}

type noWatchBackend struct{ SessionBackend }

func (noWatchBackend) Watch(context.Context, string, func(int)) (func(), error) {
	return func() {}, nil
}

type topicCall struct {
	Method   string // "join", "leave", "leave_all" or "agent_changed"
	Agent    AgentID
	Name     string
	PrevName string
	Ref      string
	Status   string
}

type fakeTopics struct {
	mu           sync.Mutex
	members      map[AgentID]map[string]bool
	introduced   map[AgentID]bool
	roster       map[AgentID]Member
	remote       map[[32]byte][]Member
	remoteShared map[[32]byte]map[string][]string
	joinErrs     map[string]error
	leaveErrs    map[string]error
	calls        []topicCall
	notify       chan struct{}
}

var _ TopicTable = (*fakeTopics)(nil)

func newFakeTopics() *fakeTopics {
	return &fakeTopics{
		members:      make(map[AgentID]map[string]bool),
		introduced:   make(map[AgentID]bool),
		roster:       make(map[AgentID]Member),
		remote:       make(map[[32]byte][]Member),
		remoteShared: make(map[[32]byte]map[string][]string),
		joinErrs:     make(map[string]error),
		leaveErrs:    make(map[string]error),
		notify:       make(chan struct{}, 64),
	}
}

func (f *fakeTopics) record(c topicCall) {
	f.calls = append(f.calls, c)
	select {
	case f.notify <- struct{}{}:
	default:
	}
}

func (f *fakeTopics) Join(agent AgentID, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record(topicCall{Method: "join", Agent: agent, Name: name})
	if name == "" {
		return ErrInvalidTopicName
	}
	if err, ok := f.joinErrs[name]; ok {
		return err
	}
	if !f.introduced[agent] {
		return ErrUnknownAgent
	}
	if f.members[agent] == nil {
		f.members[agent] = make(map[string]bool)
	}
	f.members[agent][name] = true
	return nil
}

func (f *fakeTopics) Leave(agent AgentID, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record(topicCall{Method: "leave", Agent: agent, Name: name})
	if name == "" {
		return ErrInvalidTopicName
	}
	if err, ok := f.leaveErrs[name]; ok {
		return err
	}
	delete(f.members[agent], name)
	return nil
}

func (f *fakeTopics) LeaveAll(agent AgentID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record(topicCall{Method: "leave_all", Agent: agent})
	delete(f.members, agent)
	delete(f.introduced, agent)
}

func (f *fakeTopics) TopicsOf(agent AgentID) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.topicsOfLocked(agent)
}

func (f *fakeTopics) topicsOfLocked(agent AgentID) []string {
	topics := make([]string, 0, len(f.members[agent]))
	for name := range f.members[agent] {
		topics = append(topics, name)
	}
	sort.Strings(topics)
	return topics
}

func (f *fakeTopics) AgentChanged(agent AgentID, m Member) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.introduced[agent] = true
	f.roster[agent] = m
	f.record(topicCall{Method: "agent_changed", Agent: agent, Name: m.Name, PrevName: m.PrevName, Ref: m.Ref, Status: m.Status})
}

func (f *fakeTopics) Visible(local AgentID, other VisibleAgent) ([]string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if other.Local != "" {
		shared := intersect(f.topicsOfLocked(local), f.topicsOfLocked(other.Local))
		return shared, len(shared) > 0
	}
	shared := append([]string(nil), f.remoteShared[other.Peer][other.Member.Name]...)
	sort.Strings(shared)
	return shared, len(shared) > 0
}

func (f *fakeTopics) VisibleAgents(local AgentID) []VisibleAgent {
	f.mu.Lock()
	defer f.mu.Unlock()

	ids := make([]AgentID, 0, len(f.members))
	for id := range f.members {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	var out []VisibleAgent
	for _, id := range ids {
		if id == local {
			continue
		}
		shared := intersect(f.topicsOfLocked(local), f.topicsOfLocked(id))
		if len(shared) == 0 {
			continue
		}
		out = append(out, VisibleAgent{Local: id, Member: f.roster[id], Shared: shared})
	}

	peers := make([][32]byte, 0, len(f.remote))
	for fp := range f.remote {
		peers = append(peers, fp)
	}
	sort.Slice(peers, func(i, j int) bool {
		return bytes.Compare(peers[i][:], peers[j][:]) < 0
	})
	for _, fp := range peers {
		for _, m := range f.remote[fp] {
			shared := append([]string(nil), f.remoteShared[fp][m.Name]...)
			sort.Strings(shared)
			out = append(out, VisibleAgent{Peer: fp, Member: m, Shared: shared})
		}
	}
	return out
}

func (f *fakeTopics) RemoteMembers(peer [32]byte) []Member {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Member(nil), f.remote[peer]...)
}

func (f *fakeTopics) SetRemote(peer [32]byte, members []Member, shared map[string][]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.remote[peer] = append([]Member(nil), members...)
	f.remoteShared[peer] = make(map[string][]string, len(shared))
	for name, topics := range shared {
		f.remoteShared[peer][name] = append([]string(nil), topics...)
	}
}

func (f *fakeTopics) FailJoin(name string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.joinErrs[name] = err
}

func (f *fakeTopics) FailLeave(name string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leaveErrs[name] = err
}

func (f *fakeTopics) Calls() []topicCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]topicCall(nil), f.calls...)
}

func (f *fakeTopics) callsOf(method string) []topicCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []topicCall
	for _, c := range f.calls {
		if c.Method == method {
			out = append(out, c)
		}
	}
	return out
}

func (f *fakeTopics) JoinCalls() []topicCall         { return f.callsOf("join") }
func (f *fakeTopics) LeaveCalls() []topicCall        { return f.callsOf("leave") }
func (f *fakeTopics) LeaveAllCalls() []topicCall     { return f.callsOf("leave_all") }
func (f *fakeTopics) AgentChangedCalls() []topicCall { return f.callsOf("agent_changed") }

func (f *fakeTopics) Roster(agent AgentID) (Member, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, ok := f.roster[agent]
	return m, ok
}

func (f *fakeTopics) LastAgentChanged(agent AgentID) (topicCall, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.calls) - 1; i >= 0; i-- {
		c := f.calls[i]
		if c.Method == "agent_changed" && c.Agent == agent {
			return c, true
		}
	}
	return topicCall{}, false
}

func (f *fakeTopics) ResetCalls() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = nil
}

func intersect(a, b []string) []string {
	inB := make(map[string]bool, len(b))
	for _, s := range b {
		inB[s] = true
	}
	shared := make([]string, 0, len(a))
	for _, s := range a {
		if inB[s] {
			shared = append(shared, s)
		}
	}
	sort.Strings(shared)
	return shared
}

type directoryCall struct {
	Method      string // "declare" or "retire"
	Name        string
	Description string
}

type fakeDirectory struct {
	mu     sync.Mutex
	public bool
	known  []PublicTopic
	calls  []directoryCall
	notify chan struct{}
}

var _ PublicTopicDirectory = (*fakeDirectory)(nil)

func newFakeDirectory() *fakeDirectory {
	return &fakeDirectory{notify: make(chan struct{}, 64)}
}

func (f *fakeDirectory) record(c directoryCall) {
	f.calls = append(f.calls, c)
	select {
	case f.notify <- struct{}{}:
	default:
	}
}

func (f *fakeDirectory) DeclarePublicTopic(name, description string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record(directoryCall{Method: "declare", Name: name, Description: description})
	if !f.public {
		return Errorf(localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
			"this Broker is not public; set broker.public to true first")
	}
	return nil
}

func (f *fakeDirectory) RetirePublicTopic(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record(directoryCall{Method: "retire", Name: name})
}

func (f *fakeDirectory) PublicTopics() []PublicTopic {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]PublicTopic(nil), f.known...)
}

func (f *fakeDirectory) Calls() []directoryCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]directoryCall(nil), f.calls...)
}

func (f *fakeDirectory) ResetCalls() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = nil
}

type fakePeers struct {
	aliases map[[32]byte]string
	infos   []*localpb.BrokerInfo
}

var _ PeerView = (*fakePeers)(nil)

func newFakePeers() *fakePeers {
	return &fakePeers{aliases: make(map[[32]byte]string)}
}

func (f *fakePeers) EffectiveAlias(fp [32]byte) (string, bool) {
	alias, ok := f.aliases[fp]
	return alias, ok
}

func (f *fakePeers) Peers(context.Context) []*localpb.BrokerInfo {
	return f.infos
}
