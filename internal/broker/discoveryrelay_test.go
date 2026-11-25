package broker

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"slices"
	"testing"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/directory"
	"github.com/wangzw/peers/internal/pairing"
)

var (
	_ publicSwitch = (*relaySwitch)(nil)
	_ cardPairing  = (*discoveryPairing)(nil)
	_ cardPairing  = (*pairing.Pairing)(nil)
)

type relaySwitch struct {
	setPublicErr error
	declareErr   map[string]error
	calls        []string
}

func (s *relaySwitch) SetPublic(public bool) error {
	if public {
		s.calls = append(s.calls, "SetPublic(true)")
	} else {
		s.calls = append(s.calls, "SetPublic(false)")
	}
	return s.setPublicErr
}

func (s *relaySwitch) DeclarePublicTopic(name, description string) error {
	s.calls = append(s.calls, "Declare("+name+","+description+")")
	return s.declareErr[name]
}

func (s *relaySwitch) RetirePublicTopic(name string) {
	s.calls = append(s.calls, "Retire("+name+")")
}

type cardUpdate struct {
	FP    pairing.Fingerprint
	Addrs []config.AdvertisedAddr
}

type discoveryPairing struct {
	roles   map[pairing.Fingerprint]pairing.Role
	updates []cardUpdate
}

func (p *discoveryPairing) Role(fp pairing.Fingerprint) pairing.Role {
	return p.roles[fp]
}

func (p *discoveryPairing) UpdateCardAddresses(fp pairing.Fingerprint, addrs []config.AdvertisedAddr) {
	p.updates = append(p.updates, cardUpdate{FP: fp, Addrs: addrs})
}

func relayTestLogger(t *testing.T) (*slog.Logger, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	return slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})), &buf
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func logRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var records []map[string]any
	dec := json.NewDecoder(buf)
	for {
		var rec map[string]any
		if err := dec.Decode(&rec); err != nil {
			if errors.Is(err, io.EOF) {
				return records
			}
			t.Fatalf("decoding a log record: %v", err)
		}
		records = append(records, rec)
	}
}

func findLogRecord(t *testing.T, records []map[string]any, want string) map[string]any {
	t.Helper()
	for _, rec := range records {
		if rec["msg"] == want {
			return rec
		}
	}
	t.Fatalf("no log record with msg %q in %v", want, records)
	return nil
}

func assertDebugNotWarn(t *testing.T, buf *bytes.Buffer, msg string) {
	t.Helper()
	records := logRecords(t, buf)
	for _, rec := range records {
		if rec["level"] == "WARN" {
			t.Errorf("a closed-directory SetPublic was logged at WARN: %v", rec)
		}
	}
	rec := findLogRecord(t, records, msg)
	if rec["level"] != "DEBUG" {
		t.Errorf("log level for %q = %v, want DEBUG", msg, rec["level"])
	}
}

func relayTestFP(b byte) pairing.Fingerprint {
	var fp pairing.Fingerprint
	fp[0] = b
	return fp
}

// TestPublicChangedTrueRedeclares checks that becoming public sets the flag and
// re-declares the topics in name order.
func TestPublicChangedTrueRedeclares(t *testing.T) {
	sw := &relaySwitch{}
	topics := func() map[string]string {
		return map[string]string{"y": "", "x": "hx"}
	}

	publicChanged(sw, topics, true, discardLogger())

	want := []string{"SetPublic(true)", "Declare(x,hx)", "Declare(y,)"}
	if !slices.Equal(sw.calls, want) {
		t.Fatalf("calls = %v, want %v", sw.calls, want)
	}
}

// TestPublicChangedFalseOnlyRevokes checks that becoming private clears the flag
// without reading the topics.
func TestPublicChangedFalseOnlyRevokes(t *testing.T) {
	sw := &relaySwitch{}
	topicsCalled := false
	topics := func() map[string]string {
		topicsCalled = true
		return map[string]string{"x": "hx"}
	}

	publicChanged(sw, topics, false, discardLogger())

	want := []string{"SetPublic(false)"}
	if !slices.Equal(sw.calls, want) {
		t.Fatalf("calls = %v, want %v", sw.calls, want)
	}
	if topicsCalled {
		t.Error("publicChanged read the local public topics when becoming private")
	}
}

// TestPublicChangedSetPublicErrorStops checks that a failed SetPublic stops the
// relay and logs set_public_failed.
func TestPublicChangedSetPublicErrorStops(t *testing.T) {
	sw := &relaySwitch{setPublicErr: errors.New("directory closed")}
	logger, buf := relayTestLogger(t)

	publicChanged(sw, func() map[string]string { return map[string]string{"x": "hx"} }, true, logger)

	want := []string{"SetPublic(true)"}
	if !slices.Equal(sw.calls, want) {
		t.Fatalf("calls = %v, want %v", sw.calls, want)
	}
	rec := findLogRecord(t, logRecords(t, buf), "set_public_failed")
	if errText, _ := rec["error"].(string); errText != "directory closed" {
		t.Errorf("logged error = %v, want %q", rec["error"], "directory closed")
	}
}

// TestPublicChangedClosedDirectoryLogsDebug checks that a closed directory
// during shutdown logs at debug, not as a warning.
func TestPublicChangedClosedDirectoryLogsDebug(t *testing.T) {
	sw := &relaySwitch{setPublicErr: directory.ErrClosed}
	logger, buf := relayTestLogger(t)

	publicChanged(sw, func() map[string]string { return map[string]string{"x": "hx"} }, true, logger)

	assertDebugNotWarn(t, buf, "set_public_closed")
}

// TestPublicChangedOneDeclareFailureContinues checks that one failed declaration
// logs and does not stop the rest.
func TestPublicChangedOneDeclareFailureContinues(t *testing.T) {
	sw := &relaySwitch{declareErr: map[string]error{"x": errors.New("quota exceeded")}}
	logger, buf := relayTestLogger(t)
	topics := func() map[string]string {
		return map[string]string{"x": "hx", "y": ""}
	}

	publicChanged(sw, topics, true, logger)

	want := []string{"SetPublic(true)", "Declare(x,hx)", "Declare(y,)"}
	if !slices.Equal(sw.calls, want) {
		t.Fatalf("calls = %v, want %v", sw.calls, want)
	}
	rec := findLogRecord(t, logRecords(t, buf), "public_topic_redeclare_failed")
	if rec["topic"] != "x" {
		t.Errorf("logged topic = %v, want %q", rec["topic"], "x")
	}
	if errText, _ := rec["error"].(string); errText != "quota exceeded" {
		t.Errorf("logged error = %v, want %q", rec["error"], "quota exceeded")
	}
}

// TestPublicChangedRetiresTopicsLeftMeanwhile checks that topics dropped between
// the two snapshots are retired.
func TestPublicChangedRetiresTopicsLeftMeanwhile(t *testing.T) {
	sw := &relaySwitch{}
	calls := 0
	topics := func() map[string]string {
		calls++
		if calls == 1 {
			return map[string]string{"x": "", "y": ""}
		}
		return map[string]string{"y": ""}
	}

	publicChanged(sw, topics, true, discardLogger())

	want := []string{"SetPublic(true)", "Declare(x,)", "Declare(y,)", "Retire(x)"}
	if !slices.Equal(sw.calls, want) {
		t.Fatalf("calls = %v, want %v", sw.calls, want)
	}
}

// TestRelayCardOnlyDialerPeers checks that only a dialer peer's card addresses
// are recorded.
func TestRelayCardOnlyDialerPeers(t *testing.T) {
	dialer := relayTestFP(1)
	acceptor := relayTestFP(2)
	unknown := relayTestFP(3)
	p := &discoveryPairing{roles: map[pairing.Fingerprint]pairing.Role{
		dialer:   pairing.RoleDialer,
		acceptor: pairing.RoleAcceptor,
	}}
	addrs := []config.AdvertisedAddr{
		{Kind: config.AdvertisedKindDirect, Addr: "10.0.0.1:17443"},
		{Kind: config.AdvertisedKindDirect, Addr: "10.0.0.2:17443"},
	}

	relayCard(p, dialer, addrs)
	relayCard(p, acceptor, []config.AdvertisedAddr{{Kind: config.AdvertisedKindDirect, Addr: "10.0.0.3:17443"}})
	relayCard(p, unknown, []config.AdvertisedAddr{{Kind: config.AdvertisedKindDirect, Addr: "10.0.0.4:17443"}})

	if len(p.updates) != 1 {
		t.Fatalf("updates = %v, want exactly the dialer peer's card", p.updates)
	}
	if p.updates[0].FP != dialer {
		t.Errorf("updated fingerprint = %v, want the dialer %v", p.updates[0].FP, dialer)
	}
	if !slices.Equal(p.updates[0].Addrs, addrs) {
		t.Errorf("updated addresses = %v, want %v", p.updates[0].Addrs, addrs)
	}
}
