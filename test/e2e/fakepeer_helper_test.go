//go:build e2e

package e2e

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/directory/directorypb"
	"github.com/wangzw/peers/test/e2e/harness"
)

// fakePeerCommandBound bounds one pairing or dial command the fake peer runs.
// The fake peer's own command timeout is 10 seconds, so this bound is strictly
// larger by the project-wide three seconds.
const fakePeerCommandBound = 13 * time.Second

// fakePeerWire is one JSON line the fake peer writes to stdout.
type fakePeerWire struct {
	Event       string               `json:"event"`
	Fingerprint string               `json:"fingerprint"`
	Peer        string               `json:"peer"`
	Count       int                  `json:"count"`
	Reason      string               `json:"reason"`
	Message     string               `json:"message"`
	Digests     []string             `json:"digests"`
	Sent        []string             `json:"sent"`
	Want        []string             `json:"want"`
	Topics      []string             `json:"topics"`
	Digest      string               `json:"digest"`
	Members     []fakePeerWireMember `json:"members"`
	Records     []fakePeerWireRecord `json:"records"`
}

// fakePeerWireRecord is one identifier and version a RecordList carried.
type fakePeerWireRecord struct {
	Publisher string `json:"publisher"`
	Kind      string `json:"kind"`
	Object    string `json:"object"`
	Version   uint64 `json:"version"`
}

// fakePeerWireMember is one member entry in a Roster the fake peer sends or
// receives.
type fakePeerWireMember struct {
	Name   string `json:"name"`
	Ref    string `json:"ref"`
	Status string `json:"status"`
}

// fakePeerEvent is one stdout line with the bookkeeping of which waiter has
// already consumed it.
type fakePeerEvent struct {
	fakePeerWire
	consumed bool
}

// fakePeer drives one fakepeer process over its stdin/stdout protocol.
type fakePeer struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stderr lockedBuffer

	mu     sync.Mutex
	events []*fakePeerEvent
}

// lockedBuffer collects the fake peer's standard error without racing the
// reader that copies it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// startFakePeer builds the fake peer, starts it with only the test home, the
// runtime root and PATH in its environment, and waits for its ready event.
func startFakePeer(t *testing.T) *fakePeer {
	t.Helper()

	bin := harness.BuildPackage(t, "./test/e2e/fakepeer", runtime.GOOS, runtime.GOARCH, "e2e", "peers_testenv")
	home := t.TempDir()
	// The runtime root must stay short; the fake peer itself binds no socket
	// but the product code derives paths from it.
	root, err := os.MkdirTemp("/tmp", "apfp")
	if err != nil {
		t.Fatalf("creating the fake peer's runtime root: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })

	cmd := exec.Command(bin)
	cmd.Env = []string{
		"PEERS_TEST_HOME=" + home,
		"PEERS_TEST_RUNTIME_ROOT=" + root,
		"PATH=" + os.Getenv("PATH"),
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("opening the fake peer's stdin: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("opening the fake peer's stdout: %v", err)
	}
	f := &fakePeer{cmd: cmd, stdin: stdin}
	cmd.Stderr = &f.stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting the fake peer: %v", err)
	}
	go f.read(stdout)
	t.Cleanup(f.close)

	ready := f.waitEvent(t, "ready", 3*time.Second)
	if ready.Fingerprint == "" {
		t.Fatal("the fake peer's ready event carries no fingerprint")
	}
	return f
}

// read copies the fake peer's stdout events into the event log.
func (f *fakePeer) read(r io.Reader) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		var wire fakePeerWire
		if err := json.Unmarshal(scanner.Bytes(), &wire); err != nil {
			continue
		}
		f.mu.Lock()
		f.events = append(f.events, &fakePeerEvent{fakePeerWire: wire})
		f.mu.Unlock()
	}
}

// waitEvent waits for the first unconsumed event of kind. An error event fails
// the test at once.
func (f *fakePeer) waitEvent(t *testing.T, kind string, bound time.Duration) *fakePeerEvent {
	t.Helper()
	deadline := time.Now().Add(bound)
	for {
		f.mu.Lock()
		for _, event := range f.events {
			if event.consumed {
				continue
			}
			if event.Event == "error" {
				event.consumed = true
				message := event.Message
				f.mu.Unlock()
				t.Fatalf("the fake peer reported an error: %s", message)
				return nil
			}
			if event.Event == kind {
				event.consumed = true
				f.mu.Unlock()
				return event
			}
		}
		f.mu.Unlock()

		if !time.Now().Before(deadline) {
			f.mu.Lock()
			seen := make([]string, 0, len(f.events))
			for _, event := range f.events {
				seen = append(seen, event.Event)
			}
			f.mu.Unlock()
			t.Fatalf("the fake peer sent no %q event within %s (events: %v; stderr: %s)", kind, bound, seen, f.stderr.String())
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// send writes one command line.
func (f *fakePeer) send(t *testing.T, command any) {
	t.Helper()
	data, err := json.Marshal(command)
	if err != nil {
		t.Fatalf("encoding a fake peer command: %v", err)
	}
	if _, err := f.stdin.Write(append(data, '\n')); err != nil {
		t.Fatalf("writing a fake peer command: %v", err)
	}
}

// Pair dials a Broker with the one-time key it created and returns the paired
// fingerprint.
func (f *fakePeer) Pair(t *testing.T, address, key string) string {
	t.Helper()
	f.send(t, map[string]any{"cmd": "pair", "address": address, "key": key})
	event := f.waitEvent(t, "paired", fakePeerCommandBound)
	if event.Fingerprint == "" {
		t.Fatal("the fake peer's paired event carries no fingerprint")
	}
	return event.Fingerprint
}

// WaitLinked waits for the first link the fake peer establishes.
func (f *fakePeer) WaitLinked(t *testing.T, bound time.Duration) string {
	t.Helper()
	event := f.waitEvent(t, "linked", bound)
	if event.Peer == "" {
		t.Fatal("the fake peer's linked event carries no peer")
	}
	return event.Peer
}

// Records replaces the record set the fake peer serves and pushes the new list
// to every linked peer.
func (f *fakePeer) Records(t *testing.T, records ...*directorypb.Record) {
	t.Helper()
	wire := make([]map[string]string, 0, len(records))
	for _, record := range records {
		wire = append(wire, map[string]string{
			"raw":       base64.StdEncoding.EncodeToString(record.GetRaw()),
			"signature": base64.StdEncoding.EncodeToString(record.GetSignature()),
		})
	}
	f.send(t, map[string]any{"cmd": "records", "records": wire})
	event := f.waitEvent(t, "records_set", 3*time.Second)
	if event.Count != len(records) {
		t.Fatalf("the fake peer set %d records, want %d", event.Count, len(records))
	}
}

// DialUnpaired opens a link stream without pairing, offers the current record
// set, and returns the stream's closing reason.
func (f *fakePeer) DialUnpaired(t *testing.T, address string) string {
	t.Helper()
	f.send(t, map[string]any{"cmd": "dial_unpaired", "address": address})
	return f.waitEvent(t, "closed", fakePeerCommandBound).Reason
}

// TopicDigests waits for a TopicDigests the fake peer received.
func (f *fakePeer) TopicDigests(t *testing.T) *fakePeerEvent {
	t.Helper()
	return f.waitEvent(t, "topic_digests", fakePeerCommandBound)
}

// EchoDigests resends the most recent TopicDigests verbatim.
func (f *fakePeer) EchoDigests(t *testing.T) *fakePeerEvent {
	t.Helper()
	f.send(t, map[string]any{"cmd": "echo_digests"})
	return f.waitEvent(t, "echoed", fakePeerCommandBound)
}

// SendTopicDigests sends the fake peer's own digests for names. The returned
// event lists in Want the digests the Broker uses when it sends a Roster for
// the same names.
func (f *fakePeer) SendTopicDigests(t *testing.T, names ...string) *fakePeerEvent {
	t.Helper()
	f.send(t, map[string]any{"cmd": "digests", "topics": names})
	return f.waitEvent(t, "digests_sent", fakePeerCommandBound)
}

// SendRoster sends a Roster under digest (lowercase hex) carrying members.
func (f *fakePeer) SendRoster(t *testing.T, digest string, members ...fakePeerWireMember) *fakePeerEvent {
	t.Helper()
	f.send(t, map[string]any{"cmd": "roster", "digest": digest, "members": members})
	return f.waitEvent(t, "roster_sent", fakePeerCommandBound)
}

// SendRosterDeltaBatch sends one RosterDelta under the digest the fake peer
// itself uses for topic. It carries count generated added members whose name
// is namePrefix followed by the member's six-hex Ref, the first Ref being
// refBase, then extra members. SendTopicDigests must have named topic first, or
// the Broker ignores the frame as outside the peer's latest digest set.
func (f *fakePeer) SendRosterDeltaBatch(t *testing.T, topic string, count, refBase int, namePrefix string, extra ...fakePeerWireMember) *fakePeerEvent {
	t.Helper()
	f.send(t, map[string]any{
		"cmd":         "roster_delta",
		"topics":      []string{topic},
		"count":       count,
		"ref_base":    refBase,
		"name_prefix": namePrefix,
		"extra":       extra,
	})
	return f.waitEvent(t, "roster_delta_sent", fakePeerCommandBound)
}

// WaitTopicRosterFence waits until a Roster under fence arrives and requires
// every Roster received before it to be under allowed. The fence digest is one
// the echoed set never carried, so the fence Roster can only come from the
// peer's own digest set; frames are ordered, so every Roster the echo could
// have produced has arrived by then. A Roster under any other digest before
// the fence is the echoed set matching a topic A alone holds, which is exactly
// the leak REQ-topics-014 forbids. Checking only the first Roster would pass
// whenever a leaked Roster happens to sort after the allowed one, so the whole
// run before the fence is checked.
func (f *fakePeer) WaitTopicRosterFence(t *testing.T, fence, allowed string, bound time.Duration) *fakePeerWire {
	t.Helper()
	deadline := time.Now().Add(bound)
	for {
		f.mu.Lock()
		var fenceWire, leaked *fakePeerWire
		for _, event := range f.events {
			if event.Event == "error" {
				message := event.Message
				f.mu.Unlock()
				t.Fatalf("the fake peer reported an error: %s", message)
				return nil
			}
			if event.Event != "topic_roster" {
				continue
			}
			wire := event.fakePeerWire
			if wire.Digest == fence {
				fenceWire = &wire
				break
			}
			if wire.Digest != allowed {
				leaked = &wire
				break
			}
		}
		f.mu.Unlock()
		if leaked != nil {
			t.Fatalf("the peer received a Roster under %s before the fence %s: the echoed set matched a topic A alone holds",
				leaked.Digest, fence)
			return nil
		}
		if fenceWire != nil {
			return fenceWire
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("the peer received no Roster under the fence %s within %s (stderr: %s)", fence, bound, f.stderr.String())
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// recordListVersions returns every version of the identifier seen in the
// RecordList frames the fake peer received. kind is the protobuf enum name,
// such as RECORD_KIND_BROKER_CARD. An error the fake peer reported fails the
// test, so a silent failure cannot pass the negative checks.
func (f *fakePeer) recordListVersions(t *testing.T, publisher, kind, object string) []uint64 {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []uint64
	for _, event := range f.events {
		if event.Event == "error" {
			t.Fatalf("the fake peer reported an error: %s", event.Message)
		}
		if event.Event != "record_list" {
			continue
		}
		for _, record := range event.Records {
			if record.Publisher == publisher && record.Kind == kind && record.Object == object {
				out = append(out, record.Version)
			}
		}
	}
	return out
}

// close shuts the fake peer down by closing its stdin and waits for it to
// exit.
func (f *fakePeer) close() {
	_ = f.stdin.Close()
	done := make(chan struct{})
	go func() {
		_ = f.cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		if f.cmd.Process != nil {
			_ = f.cmd.Process.Kill()
		}
		<-done
	}
}
