//go:build e2e

// Command fakepeer is an end-to-end test peer. It joins a real Broker's link
// through pairing and then serves directory records supplied by the test, so a
// test can present cards and announcements the Broker could not produce
// itself.
//
// Commands arrive as one JSON object per line on stdin. Events go out as one
// JSON object per line on stdout.
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

// commandTimeout bounds one pairing or dial command.
const commandTimeout = 10 * time.Second

var _ pairing.Transport = (*lateTransport)(nil)

// lateTransport lets pairing.Open run before the Link exists. Pairing never
// dials during Open, so the Link is installed before the first command.
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

// emitter serializes event writes: link callbacks and the command loop emit
// from different goroutines.
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

type closedEvent struct {
	Event  string `json:"event"`
	Reason string `json:"reason"`
}

type errorEvent struct {
	Event   string `json:"event"`
	Message string `json:"message"`
}

// echoedEvent reports the digest count an echo_digests command resent.
type echoedEvent struct {
	Event string `json:"event"`
	Peer  string `json:"peer"`
	Count int    `json:"count"`
}

// digestsSentEvent reports a TopicDigests the fake peer sent. Sent holds the
// digests computed under the fake peer's fingerprint; want holds, per topic
// name, the digest the Broker uses when it sends a Roster for that name.
type digestsSentEvent struct {
	Event  string   `json:"event"`
	Peer   string   `json:"peer"`
	Sent   []string `json:"sent"`
	Want   []string `json:"want"`
	Topics []string `json:"topics"`
}

// rosterSentEvent reports a Roster the fake peer sent. The member count uses
// its own key so the line still decodes as a fakePeerWire, whose members key
// holds an array.
type rosterSentEvent struct {
	Event  string `json:"event"`
	Peer   string `json:"peer"`
	Digest string `json:"digest"`
	Count  int    `json:"count"`
}

// topicDigestsEvent reports a TopicDigests the fake peer received.
type topicDigestsEvent struct {
	Event   string   `json:"event"`
	Peer    string   `json:"peer"`
	Digests []string `json:"digests"`
}

// topicRosterEvent reports a Roster the fake peer received.
type topicRosterEvent struct {
	Event   string       `json:"event"`
	Peer    string       `json:"peer"`
	Digest  string       `json:"digest"`
	Members []wireMember `json:"members"`
}

// topicRosterDeltaEvent reports a RosterDelta the fake peer received.
type topicRosterDeltaEvent struct {
	Event  string `json:"event"`
	Peer   string `json:"peer"`
	Digest string `json:"digest"`
}

// peerSet is the set of peers whose link is up.
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

// topicDigest is SHA-256 over the salt, the sender's fingerprint and the topic
// name, the value docs/adr/adr-salted-topic-digest.md defines. The fake peer
// derives it from the ADR text rather than from product code, so a change to
// the digest the ADR does not describe breaks the test.
func topicDigest(salt []byte, sender pairing.Fingerprint, name string) [32]byte {
	h := sha256.New()
	h.Write(salt)
	h.Write(sender[:])
	h.Write([]byte(name))
	var d [32]byte
	copy(d[:], h.Sum(nil))
	return d
}

// topicFrames records the topic protocol state of each link: the handshake salt
// and the last TopicDigests received. The commands use it to echo a set
// verbatim or to compute one direction's digests.
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

// peer returns the peer that sent the latest TopicDigests and its salt.
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

// echo resends the latest TopicDigests verbatim.
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

// sendDigests sends a TopicDigests holding the fake peer's direction for names.
// An empty names list sends an empty set, which leaves every topic.
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

// sendRoster sends a Roster under digest (lowercase hex) with members.
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

// hexDigests renders digests as lowercase hex.
func hexDigests(ds [][32]byte) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = hex.EncodeToString(d[:])
	}
	return out
}

// statusFromName maps a status name to its wire value; an unknown name is
// AGENT_STATUS_UNSPECIFIED.
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

// command is one stdin line.
type command struct {
	Cmd     string       `json:"cmd"`
	Address string       `json:"address"`
	Key     string       `json:"key"`
	Records []wireRecord `json:"records"`
	Topics  []string     `json:"topics"`
	Digest  string       `json:"digest"`
	Members []wireMember `json:"members"`
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

	l.HandleState(directorypb.KindRecordList, func(peer link.Fingerprint, _ link.Frame) {
		_ = l.Send(peer, link.Frame{Kind: directorypb.KindRecordList, Payload: marshal(set.list())})
	})
	l.HandleRequest(directorypb.KindRecordRequest, func(_ context.Context, _ link.Fingerprint, f link.Frame) link.Frame {
		req := new(directorypb.RecordRequest)
		if err := proto.Unmarshal(f.Payload, req); err != nil {
			req = new(directorypb.RecordRequest)
		}
		return link.Frame{Kind: directorypb.KindRecords, Payload: marshal(set.answer(req))}
	})

	// The topic handlers record every topic frame and report it as an event, so
	// a test can observe what the Broker sent and in which order.
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

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		handleCommand(scanner.Bytes(), p, l, set, established, topics, e)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read commands: %w", err)
	}
	return nil
}

// prepareHome creates the data directory and, when absent, a config file that
// keeps the fake peer from listening on the default port.
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
	default:
		e.emit(errorEvent{Event: "error", Message: "unknown command"})
	}
}

// decodeRecords turns the stdin form of a record into the wire message. Both
// fields are standard base64.
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
