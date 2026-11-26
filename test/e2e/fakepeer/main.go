//go:build e2e

// Command fakepeer is an end-to-end test peer: it joins a real Broker's link
// through pairing and serves directory records the test supplies. Commands
// arrive as one JSON object per line on stdin and events go out one JSON object
// per line on stdout.
package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/directory/directorypb"
	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/link/linkpb"
	"github.com/wangzw/peers/internal/pairing"
	"github.com/wangzw/peers/internal/topic/topicpb"
)

const commandTimeout = 10 * time.Second

var _ pairing.Transport = (*lateTransport)(nil)

// lateTransport lets pairing.Open run before the Link exists; pairing never
// dials during Open.
type lateTransport struct {
	mu sync.Mutex
	l  *link.Link
}

func (t *lateTransport) PairDial(ctx context.Context, address string, run func(pairing.PairConn) error) error {
	t.mu.Lock()
	l := t.l
	t.mu.Unlock()
	if l == nil {
		return pairing.ErrDialFailed
	}
	return l.PairDial(ctx, address, run)
}

func (t *lateTransport) Drop(fp pairing.Fingerprint) {
	t.mu.Lock()
	l := t.l
	t.mu.Unlock()
	if l != nil {
		l.Drop(fp)
	}
}

// emitter serializes event writes from link callbacks and the command loop.
type emitter struct {
	mu  sync.Mutex
	enc *json.Encoder
}

func newEmitter(w io.Writer) *emitter { return &emitter{enc: json.NewEncoder(w)} }

func (e *emitter) emit(v any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	_ = e.enc.Encode(v)
}

type readyEvent struct {
	Event       string `json:"event"`
	Fingerprint string `json:"fingerprint"`
}

type linkedEvent struct {
	Event string `json:"event"`
	Peer  string `json:"peer"`
}

type pairedEvent struct {
	Event       string `json:"event"`
	Fingerprint string `json:"fingerprint"`
}

type recordsSetEvent struct {
	Event string `json:"event"`
	Count int    `json:"count"`
}

type wireRecordListEntry struct {
	Publisher string `json:"publisher"`
	Kind      string `json:"kind"`
	Object    string `json:"object"`
	Version   uint64 `json:"version"`
}

type recordListEvent struct {
	Event   string                `json:"event"`
	Peer    string                `json:"peer"`
	Records []wireRecordListEntry `json:"records"`
}

type closedEvent struct {
	Event  string `json:"event"`
	Reason string `json:"reason"`
}

type errorEvent struct {
	Event   string `json:"event"`
	Message string `json:"message"`
}

type echoedEvent struct {
	Event string `json:"event"`
	Peer  string `json:"peer"`
	Count int    `json:"count"`
}

// Sent holds the digests computed under the fake peer's fingerprint; Want
// holds, per topic, the digest the Broker uses when it sends a Roster.
type digestsSentEvent struct {
	Event  string   `json:"event"`
	Peer   string   `json:"peer"`
	Sent   []string `json:"sent"`
	Want   []string `json:"want"`
	Topics []string `json:"topics"`
}

// The count uses its own key because a fakePeerWire's members key holds an array.
type rosterSentEvent struct {
	Event  string `json:"event"`
	Peer   string `json:"peer"`
	Digest string `json:"digest"`
	Count  int    `json:"count"`
}

// Count is the number of added entries, generated plus extra.
type rosterDeltaSentEvent struct {
	Event  string `json:"event"`
	Peer   string `json:"peer"`
	Digest string `json:"digest"`
	Count  int    `json:"count"`
}

type topicDigestsEvent struct {
	Event   string   `json:"event"`
	Peer    string   `json:"peer"`
	Digests []string `json:"digests"`
}

type topicRosterEvent struct {
	Event   string       `json:"event"`
	Peer    string       `json:"peer"`
	Digest  string       `json:"digest"`
	Members []wireMember `json:"members"`
}

type topicRosterDeltaEvent struct {
	Event  string `json:"event"`
	Peer   string `json:"peer"`
	Digest string `json:"digest"`
}

type peerSet struct {
	mu    sync.Mutex
	peers map[pairing.Fingerprint]struct{}
}

func newPeerSet() *peerSet { return &peerSet{peers: make(map[pairing.Fingerprint]struct{})} }

func (s *peerSet) add(fp pairing.Fingerprint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.peers[fp] = struct{}{}
}

func (s *peerSet) remove(fp pairing.Fingerprint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.peers, fp)
}

func (s *peerSet) snapshot() []pairing.Fingerprint {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]pairing.Fingerprint, 0, len(s.peers))
	for fp := range s.peers {
		out = append(out, fp)
	}
	return out
}

// topicDigest is computed independently of product code, so a digest change
// breaks the test.
func topicDigest(salt []byte, sender pairing.Fingerprint, name string) [32]byte {
	h := sha256.New()
	h.Write(salt)
	h.Write(sender[:])
	h.Write([]byte(name))
	var d [32]byte
	copy(d[:], h.Sum(nil))
	return d
}

type topicFrames struct {
	mu      sync.Mutex
	salts   map[pairing.Fingerprint][]byte
	last    pairing.Fingerprint
	payload []byte
	have    bool
}

func newTopicFrames() *topicFrames {
	return &topicFrames{salts: map[pairing.Fingerprint][]byte{}}
}

func (t *topicFrames) up(peer pairing.Fingerprint, salt []byte) {
	t.mu.Lock()
	t.salts[peer] = append([]byte(nil), salt...)
	t.mu.Unlock()
}

func (t *topicFrames) down(peer pairing.Fingerprint) {
	t.mu.Lock()
	delete(t.salts, peer)
	t.mu.Unlock()
}

func (t *topicFrames) recordDigests(peer pairing.Fingerprint, payload []byte) {
	t.mu.Lock()
	t.last = peer
	t.payload = append([]byte(nil), payload...)
	t.have = true
	t.mu.Unlock()
}

func (t *topicFrames) peer() (pairing.Fingerprint, []byte, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.have {
		return pairing.Fingerprint{}, nil, false
	}
	salt, ok := t.salts[t.last]
	if !ok {
		return pairing.Fingerprint{}, nil, false
	}
	return t.last, append([]byte(nil), salt...), true
}

func (t *topicFrames) echo(l *link.Link, e *emitter) {
	t.mu.Lock()
	peer, payload, ok := t.last, append([]byte(nil), t.payload...), t.have
	t.mu.Unlock()
	if !ok {
		e.emit(errorEvent{Event: "error", Message: "echo_digests: no TopicDigests received"})
		return
	}
	if err := l.Send(peer, link.Frame{Kind: topicpb.KindTopicDigests, Payload: payload}); err != nil {
		e.emit(errorEvent{Event: "error", Message: "echo_digests: " + err.Error()})
		return
	}
	var msg topicpb.TopicDigests
	_ = proto.Unmarshal(payload, &msg)
	e.emit(echoedEvent{Event: "echoed", Peer: peer.String(), Count: len(msg.Digests)})
}

func (t *topicFrames) sendDigests(l *link.Link, self pairing.Fingerprint, names []string, e *emitter) {
	peer, salt, ok := t.peer()
	if !ok {
		e.emit(errorEvent{Event: "error", Message: "digests: no link with a salt"})
		return
	}
	sent := make([][32]byte, len(names))
	want := make([][32]byte, len(names))
	msg := &topicpb.TopicDigests{Digests: make([][]byte, len(names))}
	for i, name := range names {
		sent[i] = topicDigest(salt, self, name)
		want[i] = topicDigest(salt, peer, name)
		b := make([]byte, len(sent[i]))
		copy(b, sent[i][:])
		msg.Digests[i] = b
	}
	payload, err := proto.Marshal(msg)
	if err != nil {
		e.emit(errorEvent{Event: "error", Message: "digests: " + err.Error()})
		return
	}
	if err := l.Send(peer, link.Frame{Kind: topicpb.KindTopicDigests, Payload: payload}); err != nil {
		e.emit(errorEvent{Event: "error", Message: "digests: " + err.Error()})
		return
	}
	e.emit(digestsSentEvent{
		Event:  "digests_sent",
		Peer:   peer.String(),
		Sent:   hexDigests(sent),
		Want:   hexDigests(want),
		Topics: append([]string(nil), names...),
	})
}

func (t *topicFrames) sendRoster(l *link.Link, digest string, members []wireMember, e *emitter) {
	peer, _, ok := t.peer()
	if !ok {
		e.emit(errorEvent{Event: "error", Message: "roster: no link with a salt"})
		return
	}
	d, err := hex.DecodeString(digest)
	if err != nil || len(d) != 32 {
		e.emit(errorEvent{Event: "error", Message: "roster: digest must be 32 bytes of hex"})
		return
	}
	msg := &topicpb.Roster{Digest: d, Members: make([]*topicpb.Member, len(members))}
	for i, m := range members {
		msg.Members[i] = &topicpb.Member{Name: m.Name, Ref: m.Ref, Status: statusFromName(m.Status)}
	}
	payload, err := proto.Marshal(msg)
	if err != nil {
		e.emit(errorEvent{Event: "error", Message: "roster: " + err.Error()})
		return
	}
	if err := l.Send(peer, link.Frame{Kind: topicpb.KindRoster, Payload: payload}); err != nil {
		e.emit(errorEvent{Event: "error", Message: "roster: " + err.Error()})
		return
	}
	e.emit(rosterSentEvent{Event: "roster_sent", Peer: peer.String(), Digest: digest, Count: len(members)})
}

// sendRosterDelta uses the digest bound to the fake peer's fingerprint, so the
// test must send its TopicDigests for the topic first or the Broker ignores it.
func (t *topicFrames) sendRosterDelta(l *link.Link, self pairing.Fingerprint, cmd command, e *emitter) {
	if len(cmd.Topics) != 1 {
		e.emit(errorEvent{Event: "error", Message: "roster_delta: exactly one topic name is required"})
		return
	}
	if cmd.Count < 0 {
		e.emit(errorEvent{Event: "error", Message: "roster_delta: count must not be negative"})
		return
	}
	peer, salt, ok := t.peer()
	if !ok {
		e.emit(errorEvent{Event: "error", Message: "roster_delta: no link with a salt"})
		return
	}
	d := topicDigest(salt, self, cmd.Topics[0])
	added := make([]*topicpb.Member, 0, cmd.Count+len(cmd.Extra))
	for i := 0; i < cmd.Count; i++ {
		ref := fmt.Sprintf("%06x", cmd.RefBase+i)
		added = append(added, &topicpb.Member{Name: cmd.NamePrefix + ref, Ref: ref})
	}
	for _, m := range cmd.Extra {
		added = append(added, &topicpb.Member{Name: m.Name, Ref: m.Ref, Status: statusFromName(m.Status)})
	}
	msg := &topicpb.RosterDelta{Digest: d[:], Added: added}
	payload, err := proto.Marshal(msg)
	if err != nil {
		e.emit(errorEvent{Event: "error", Message: "roster_delta: " + err.Error()})
		return
	}
	if err := l.Send(peer, link.Frame{Kind: topicpb.KindRosterDelta, Payload: payload}); err != nil {
		e.emit(errorEvent{Event: "error", Message: "roster_delta: " + err.Error()})
		return
	}
	e.emit(rosterDeltaSentEvent{Event: "roster_delta_sent", Peer: peer.String(), Digest: hex.EncodeToString(d[:]), Count: len(added)})
}

func hexDigests(ds [][32]byte) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = hex.EncodeToString(d[:])
	}
	return out
}

func statusFromName(s string) topicpb.AgentStatus {
	switch s {
	case "idle":
		return topicpb.AgentStatus_AGENT_STATUS_IDLE
	case "busy":
		return topicpb.AgentStatus_AGENT_STATUS_BUSY
	case "waiting":
		return topicpb.AgentStatus_AGENT_STATUS_WAITING
	case "shell":
		return topicpb.AgentStatus_AGENT_STATUS_SHELL
	default:
		return topicpb.AgentStatus_AGENT_STATUS_UNSPECIFIED
	}
}

type command struct {
	Cmd        string       `json:"cmd"`
	Address    string       `json:"address"`
	Key        string       `json:"key"`
	Records    []wireRecord `json:"records"`
	Topics     []string     `json:"topics"`
	Digest     string       `json:"digest"`
	Members    []wireMember `json:"members"`
	Count      int          `json:"count"`
	NamePrefix string       `json:"name_prefix"`
	RefBase    int          `json:"ref_base"`
	Extra      []wireMember `json:"extra"`
}

type wireRecord struct {
	Raw       string `json:"raw"`
	Signature string `json:"signature"`
}

type wireMember struct {
	Name   string `json:"name"`
	Ref    string `json:"ref"`
	Status string `json:"status"`
}

func main() {
	if err := run(); err != nil {
		newEmitter(os.Stdout).emit(errorEvent{Event: "error", Message: err.Error()})
		os.Exit(1)
	}
}

func run() error {
	if err := prepareHome(); err != nil {
		return err
	}
	if err := config.Load(config.ModeUser, ""); err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	defer config.Close()

	lt := &lateTransport{}
	p, err := pairing.Open(lt)
	if err != nil {
		return fmt.Errorf("open pairing: %w", err)
	}

	l, err := link.New(link.Options{Peers: p, Settings: link.ConfigSettings()})
	if err != nil {
		return fmt.Errorf("open link: %w", err)
	}
	defer l.Close()
	lt.mu.Lock()
	lt.l = l
	lt.mu.Unlock()

	e := newEmitter(os.Stdout)
	set := &recordSet{}
	established := newPeerSet()
	topics := newTopicFrames()

	l.HandleState(directorypb.KindRecordList, func(peer link.Fingerprint, f link.Frame) {
		_ = l.Send(peer, link.Frame{Kind: directorypb.KindRecordList, Payload: marshal(set.list())})
		var list directorypb.RecordList
		entries := []wireRecordListEntry{}
		if proto.Unmarshal(f.Payload, &list) == nil {
			for _, rv := range list.GetRecords() {
				entries = append(entries, wireRecordListEntry{
					Publisher: hex.EncodeToString(rv.GetId().GetPublisher()),
					Kind:      rv.GetId().GetKind().String(),
					Object:    rv.GetId().GetObject(),
					Version:   rv.GetVersion(),
				})
			}
		}
		e.emit(recordListEvent{Event: "record_list", Peer: peer.String(), Records: entries})
	})
	l.HandleRequest(directorypb.KindRecordRequest, func(_ context.Context, _ link.Fingerprint, f link.Frame) link.Frame {
		req := new(directorypb.RecordRequest)
		if err := proto.Unmarshal(f.Payload, req); err != nil {
			req = new(directorypb.RecordRequest)
		}
		return link.Frame{Kind: directorypb.KindRecords, Payload: marshal(set.answer(req))}
	})

	l.HandleState(topicpb.KindTopicDigests, func(peer link.Fingerprint, f link.Frame) {
		topics.recordDigests(peer, f.Payload)
		var msg topicpb.TopicDigests
		digests := []string{}
		if proto.Unmarshal(f.Payload, &msg) == nil {
			digests = make([]string, len(msg.Digests))
			for i, d := range msg.Digests {
				digests[i] = hex.EncodeToString(d)
			}
		}
		e.emit(topicDigestsEvent{Event: "topic_digests", Peer: peer.String(), Digests: digests})
	})
	l.HandleState(topicpb.KindRoster, func(peer link.Fingerprint, f link.Frame) {
		var msg topicpb.Roster
		_ = proto.Unmarshal(f.Payload, &msg)
		members := make([]wireMember, len(msg.Members))
		for i, m := range msg.Members {
			members[i] = wireMember{Name: m.Name, Ref: m.Ref}
		}
		e.emit(topicRosterEvent{Event: "topic_roster", Peer: peer.String(), Digest: hex.EncodeToString(msg.Digest), Members: members})
	})
	l.HandleState(topicpb.KindRosterDelta, func(peer link.Fingerprint, f link.Frame) {
		var msg topicpb.RosterDelta
		_ = proto.Unmarshal(f.Payload, &msg)
		e.emit(topicRosterDeltaEvent{Event: "topic_roster_delta", Peer: peer.String(), Digest: hex.EncodeToString(msg.Digest)})
	})

	l.Subscribe(func(peer link.Fingerprint, salt []byte) {
		established.add(peer)
		topics.up(peer, salt)
		e.emit(linkedEvent{Event: "linked", Peer: peer.String()})
		_ = l.Send(peer, link.Frame{Kind: directorypb.KindRecordList, Payload: marshal(set.list())})
	}, func(peer link.Fingerprint) {
		established.remove(peer)
		topics.down(peer)
	})
	l.Start()

	e.emit(readyEvent{Event: "ready", Fingerprint: p.Fingerprint().String()})

	// A 64 KiB record is about 87 KiB in base64, above the scanner's default limit.
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for scanner.Scan() {
		handleCommand(scanner.Bytes(), p, l, set, established, topics, e)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read commands: %w", err)
	}
	return nil
}

// prepareHome creates the data directory and, when absent, a config file with
// an empty listen address so the fake peer never binds the default port.
func prepareHome() error {
	home := os.Getenv("PEERS_TEST_HOME")
	if home == "" {
		return errors.New("PEERS_TEST_HOME is not set")
	}
	dir := filepath.Join(home, ".peers")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create data directory: %w", err)
	}
	path := filepath.Join(dir, "config.yaml")
	switch _, err := os.Stat(path); {
	case errors.Is(err, fs.ErrNotExist):
		if err := os.WriteFile(path, []byte("broker:\n  listen: \"\"\n"), 0o600); err != nil {
			return fmt.Errorf("write config file: %w", err)
		}
	case err != nil:
		return fmt.Errorf("stat config file: %w", err)
	}
	return nil
}

func handleCommand(line []byte, p *pairing.Pairing, l *link.Link, set *recordSet, established *peerSet, topics *topicFrames, e *emitter) {
	var cmd command
	if err := json.Unmarshal(line, &cmd); err != nil {
		e.emit(errorEvent{Event: "error", Message: "decode command: " + err.Error()})
		return
	}

	switch cmd.Cmd {
	case "pair":
		ctx, cancel := context.WithTimeout(context.Background(), commandTimeout)
		defer cancel()
		peer, _, err := p.Connect(ctx, cmd.Address, cmd.Key, "")
		if err != nil {
			e.emit(errorEvent{Event: "error", Message: err.Error()})
			return
		}
		e.emit(pairedEvent{Event: "paired", Fingerprint: peer.Fingerprint.String()})
	case "records":
		recs, err := decodeRecords(cmd.Records)
		if err != nil {
			e.emit(errorEvent{Event: "error", Message: err.Error()})
			return
		}
		set.set(recs)
		payload := marshal(set.list())
		for _, peer := range established.snapshot() {
			_ = l.Send(peer, link.Frame{Kind: directorypb.KindRecordList, Payload: payload})
		}
		e.emit(recordsSetEvent{Event: "records_set", Count: len(recs)})
	case "dial_unpaired":
		ctx, cancel := context.WithTimeout(context.Background(), commandTimeout)
		defer cancel()
		var conn *grpc.ClientConn
		open := func(ctx context.Context) (linkpb.Link_ConnectClient, error) {
			tc := p.TLSConfig().Clone()
			c, err := grpc.NewClient(cmd.Address, grpc.WithTransportCredentials(credentials.NewTLS(tc)))
			if err != nil {
				return nil, err
			}
			conn = c
			return linkpb.NewLinkClient(c).Connect(ctx)
		}
		reason := dialUnpaired(ctx, open, set)
		if conn != nil {
			_ = conn.Close()
		}
		e.emit(closedEvent{Event: "closed", Reason: reason})
	case "echo_digests":
		topics.echo(l, e)
	case "digests":
		topics.sendDigests(l, p.Fingerprint(), cmd.Topics, e)
	case "roster":
		topics.sendRoster(l, cmd.Digest, cmd.Members, e)
	case "roster_delta":
		topics.sendRosterDelta(l, p.Fingerprint(), cmd, e)
	default:
		e.emit(errorEvent{Event: "error", Message: "unknown command"})
	}
}

func decodeRecords(wire []wireRecord) ([]*directorypb.Record, error) {
	recs := make([]*directorypb.Record, 0, len(wire))
	for i, w := range wire {
		raw, err := base64.StdEncoding.DecodeString(w.Raw)
		if err != nil {
			return nil, fmt.Errorf("record %d: decode raw: %w", i, err)
		}
		sig, err := base64.StdEncoding.DecodeString(w.Signature)
		if err != nil {
			return nil, fmt.Errorf("record %d: decode signature: %w", i, err)
		}
		recs = append(recs, &directorypb.Record{Raw: raw, Signature: sig})
	}
	return recs, nil
}
