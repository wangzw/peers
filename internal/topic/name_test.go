package topic

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/topic/topicpb"
)

// TestValidateName checks the rules for a legal topic name.
// REQ-topics-001: a topic is joined by name, so the name must be legal first.
func TestValidateName(t *testing.T) {
	valid := []string{
		"release",
		"github.com/org/repo",
		"a b",
		"名字",
		strings.Repeat("a", 256),
		strings.Repeat("名", 85), // 255 bytes
		"Release",
	}
	for _, name := range valid {
		if err := ValidateName(name); err != nil {
			t.Errorf("ValidateName(%q) = %v, want nil", name, err)
		}
	}

	invalid := []string{
		"",
		strings.Repeat("a", 257),
		strings.Repeat("名", 86), // 258 bytes
		"a\x00b",
		"a\nb",
		"\x7f",
		"a\u0085b",
		"\xff",
	}
	for _, name := range invalid {
		if err := ValidateName(name); !errors.Is(err, ErrInvalidTopic) {
			t.Errorf("ValidateName(%q) = %v, want ErrInvalidTopic", name, err)
		}
	}
}

func TestDigestBindsSender(t *testing.T) {
	salt := []byte("salt")
	var f1, f2 link.Fingerprint
	f1[0], f2[0] = 1, 2

	// Two directions of one topic under one salt, against fixed values.
	for _, tc := range []struct {
		fp   link.Fingerprint
		want string
	}{
		{f1, "4f07fe1fb1ac90392a249ad6535b7a8e714bc917f3882cb07c5b10c041a6bb2b"},
		{f2, "266f506a71f852b8eef5fa24ab5a65d4edd06d8cc2d4035eef1bdeee09c64517"},
	} {
		if got := digest(salt, tc.fp, "topic"); hex.EncodeToString(got[:]) != tc.want {
			t.Errorf("digest(salt, %x, topic) = %s, want %s", tc.fp[:4], hex.EncodeToString(got[:]), tc.want)
		}
	}
	if digest(salt, f1, "topic") == digest(salt, f2, "topic") {
		t.Error("the digest ignores the sender")
	}
	if digest([]byte("s1"), f1, "t") == digest([]byte("s2"), f1, "t") {
		t.Error("the digest ignores the salt")
	}

	// SHA-256("ab" || zero fingerprint || "c") proves salt first, sender
	// second, name third.
	var zero link.Fingerprint
	if got, want := digest([]byte("ab"), zero, "c"), "311d16069bf00c1d50ef0f78ebcd407e88b193f5a737020ace49c1e0ccac89de"; hex.EncodeToString(got[:]) != want {
		t.Errorf("digest(\"ab\", zero, \"c\") = %s, want %s", hex.EncodeToString(got[:]), want)
	}

	// A digest must not write into the caller's salt, not even into its spare
	// capacity.
	tight := make([]byte, 3, 16)
	copy(tight, "s1x")
	before := append([]byte(nil), tight[:cap(tight)]...)
	digest(tight, f1, "t")
	if !bytes.Equal(tight[:cap(tight)], before) {
		t.Errorf("digest modified the caller's salt: %q -> %q", before, tight[:cap(tight)])
	}
}

func TestNewPeerStateCopiesSalt(t *testing.T) {
	salt := []byte("s1")
	self, peer := link.Fingerprint{1}, link.Fingerprint{2}
	ps := newPeerState(salt, self, peer, []string{"t1", "t2"})
	salt[0] = 'X'
	if string(ps.salt) != "s1" {
		t.Errorf("ps.salt = %q, want %q (the salt was not copied)", ps.salt, "s1")
	}

	wantSend := digest([]byte("s1"), self, "t1")
	if got := ps.send["t1"]; got != wantSend {
		t.Errorf("ps.send[\"t1\"] = %x, want %x", got, wantSend)
	}
	wantRecv := digest([]byte("s1"), peer, "t1")
	if got := ps.recv["t1"]; got != wantRecv {
		t.Errorf("ps.recv[\"t1\"] = %x, want %x", got, wantRecv)
	}
	if got := ps.recvName[wantRecv]; got != "t1" {
		t.Errorf("ps.recvName[recv(\"t1\")] = %q, want %q", got, "t1")
	}

	if ps.remote == nil || len(ps.remote) != 0 {
		t.Errorf("ps.remote = %v, want an allocated empty map", ps.remote)
	}
	if ps.shared == nil || len(ps.shared) != 0 {
		t.Errorf("ps.shared = %v, want an allocated empty map", ps.shared)
	}
	if ps.rosters == nil || len(ps.rosters) != 0 {
		t.Errorf("ps.rosters = %v, want an allocated empty map", ps.rosters)
	}
}

// sendFailTransport is the smallest Transport implementation: SendLatest always
// fails, the other methods do nothing.
type sendFailTransport struct {
	err error
}

func (s *sendFailTransport) SendLatest(link.Fingerprint, link.Frame) error { return s.err }

func (s *sendFailTransport) SendLatestKey(link.Fingerprint, string, link.Frame) (bool, error) {
	return false, s.err
}

func (s *sendFailTransport) HandleState(link.FrameKind, func(link.Fingerprint, link.Frame)) {}

func (s *sendFailTransport) Subscribe(func(link.Fingerprint, []byte), func(link.Fingerprint)) {
}

func TestSendLatestLockedLogsFailure(t *testing.T) {
	var buf bytes.Buffer
	r := &Registry{
		t:   &sendFailTransport{err: errors.New("link is down")},
		log: slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	r.sendLatestLocked(link.Fingerprint{}, topicpb.KindTopicDigests, &topicpb.TopicDigests{})

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("got %d log records, want 1: %q", len(lines), buf.String())
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("log record is not JSON: %v", err)
	}
	if got := rec["msg"]; got != "topic.send_failed" {
		t.Errorf("log msg = %v, want topic.send_failed", got)
	}
	if _, ok := rec["kind"]; !ok {
		t.Errorf("log record has no kind field: %s", lines[0])
	}
}
