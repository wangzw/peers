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

// fakePeerCommandBound is strictly larger than the fake peer's own 10-second
// command timeout.
const fakePeerCommandBound = 13 * time.Second

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

type fakePeerWireRecord struct {
	Publisher string `json:"publisher"`
	Kind      string `json:"kind"`
	Object    string `json:"object"`
	Version   uint64 `json:"version"`
}

type fakePeerWireMember struct {
	Name   string `json:"name"`
	Ref    string `json:"ref"`
	Status string `json:"status"`
}

type fakePeerEvent struct {
	fakePeerWire
	consumed bool
}

type fakePeer struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stderr lockedBuffer

	mu     sync.Mutex
	events []*fakePeerEvent
}

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

func startFakePeer(t *testing.T) *fakePeer {
	t.Helper()

	bin := harness.BuildPackage(t, "./test/e2e/fakepeer", runtime.GOOS, runtime.GOARCH, "e2e", "peers_testenv")
	home := t.TempDir()
	// The runtime root must stay short: the product code derives paths from it.
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

	ready := f.waitEvent(t, "ready", harness.EventTimeout)
	if ready.Fingerprint == "" {
		t.Fatal("the fake peer's ready event carries no fingerprint")
	}
	return f
}

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

func (f *fakePeer) Pair(t *testing.T, address, key string) string {
	t.Helper()
	f.send(t, map[string]any{"cmd": "pair", "address": address, "key": key})
	event := f.waitEvent(t, "paired", fakePeerCommandBound)
	if event.Fingerprint == "" {
		t.Fatal("the fake peer's paired event carries no fingerprint")
	}
	return event.Fingerprint
}

func (f *fakePeer) WaitLinked(t *testing.T, bound time.Duration) string {
	t.Helper()
	event := f.waitEvent(t, "linked", bound)
	if event.Peer == "" {
		t.Fatal("the fake peer's linked event carries no peer")
	}
	return event.Peer
}

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
	event := f.waitEvent(t, "records_set", harness.EventTimeout)
	if event.Count != len(records) {
		t.Fatalf("the fake peer set %d records, want %d", event.Count, len(records))
	}
}

func (f *fakePeer) DialUnpaired(t *testing.T, address string) string {
	t.Helper()
	f.send(t, map[string]any{"cmd": "dial_unpaired", "address": address})
	return f.waitEvent(t, "closed", fakePeerCommandBound).Reason
}

func (f *fakePeer) TopicDigests(t *testing.T) *fakePeerEvent {
	t.Helper()
	return f.waitEvent(t, "topic_digests", fakePeerCommandBound)
}

func (f *fakePeer) EchoDigests(t *testing.T) *fakePeerEvent {
	t.Helper()
	f.send(t, map[string]any{"cmd": "echo_digests"})
	return f.waitEvent(t, "echoed", fakePeerCommandBound)
}

func (f *fakePeer) SendTopicDigests(t *testing.T, names ...string) *fakePeerEvent {
	t.Helper()
	f.send(t, map[string]any{"cmd": "digests", "topics": names})
	return f.waitEvent(t, "digests_sent", fakePeerCommandBound)
}

func (f *fakePeer) SendRoster(t *testing.T, digest string, members ...fakePeerWireMember) *fakePeerEvent {
	t.Helper()
	f.send(t, map[string]any{"cmd": "roster", "digest": digest, "members": members})
	return f.waitEvent(t, "roster_sent", fakePeerCommandBound)
}

// SendTopicDigests must have named topic first, or the Broker ignores the frame
// as outside the peer's latest digest set.
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

// WaitTopicRosterFence waits for a Roster under fence. Every Roster received
// before the fence must be under allowed, and any other digest fails. Frames
// are ordered, so every Roster the echo could have produced has arrived by the
// time the fence does, and checking only the first Roster would pass whenever a
// leaked one happens to sort after the allowed one, so the whole run before the
// fence is checked.
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
