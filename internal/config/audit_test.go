package config

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

const auditTestClockText = "2026-11-18T12:00:00.123456789Z"

const auditTestSecretKey = "pairing.static_key"

func auditTestClock(t *testing.T) time.Time {
	t.Helper()
	when, err := time.Parse(time.RFC3339Nano, auditTestClockText)
	if err != nil {
		t.Fatalf("parse test clock: %v", err)
	}
	return when
}

func newAuditTestLog(t *testing.T, dirMode os.FileMode) (*auditLog, string, *bytes.Buffer) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "data", "audit.log")
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	clock := auditTestClock(t)
	log, err := openAuditWithDirMode(path, dirMode, func() time.Time { return clock }, logger)
	if err != nil {
		t.Fatalf("open audit log: %v", err)
	}
	t.Cleanup(func() {
		if err := log.close(); err != nil {
			t.Errorf("close audit log: %v", err)
		}
	})
	return log, path, &logs
}

func testFingerprint(seed byte) [32]byte {
	var fingerprint [32]byte
	for i := range fingerprint {
		fingerprint[i] = seed + byte(i)
	}
	return fingerprint
}

// TestREQBrokerConfig006_SetWritesAudit checks that a config_set event
// round-trips through the file with its time, actor, account, key and typed old
// and new values.
func TestREQBrokerConfig006_SetWritesAudit(t *testing.T) {
	log, path, _ := newAuditTestLog(t, 0o700)
	when := auditTestClock(t)
	oldValue := Value{Kind: KindString, String: "old"}
	newValue := Value{Kind: KindString, String: "new"}

	err := log.append(Event{
		Time:    when,
		Event:   EventConfigSet,
		Actor:   "buildbox.ana/ana",
		Account: "ana",
		Key:     "broker.alias",
		Old:     &oldValue,
		New:     &newValue,
	})
	if err != nil {
		t.Fatalf("append config_set: %v", err)
	}

	events, err := log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	got := events[0]
	if !got.Time.Equal(when) {
		t.Errorf("time = %s, want %s", got.Time, when)
	}
	if got.Event != EventConfigSet {
		t.Errorf("event = %q, want %q", got.Event, EventConfigSet)
	}
	if got.Actor != "buildbox.ana/ana" || got.Account != "ana" {
		t.Errorf("actor/account = %q/%q", got.Actor, got.Account)
	}
	if got.Key != "broker.alias" {
		t.Errorf("key = %q", got.Key)
	}
	if got.Old == nil || !reflect.DeepEqual(*got.Old, oldValue) {
		t.Errorf("old = %#v, want %#v", got.Old, oldValue)
	}
	if got.New == nil || !reflect.DeepEqual(*got.New, newValue) {
		t.Errorf("new = %#v, want %#v", got.New, newValue)
	}
	if got.Count != 0 {
		t.Errorf("count = %d, want 0 for an unmerged record", got.Count)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if !strings.Contains(string(data), `"event":"config_set"`) {
		t.Errorf("raw record does not contain the event name: %s", data)
	}
	if !strings.Contains(string(data), `"kind":"string"`) {
		t.Errorf("raw record does not carry the value kind: %s", data)
	}
}

// TestREQBrokerConfig006_SecretAuditOnlyKey checks that a secret key's
// config_set and config_file_changed records keep only the key and never carry
// old or new values.
func TestREQBrokerConfig006_SecretAuditOnlyKey(t *testing.T) {
	log, path, _ := newAuditTestLog(t, 0o700)
	const sentinel = "SENTINEL-secret-value-0123456789"
	oldValue := Value{Kind: KindString, String: sentinel}
	newValue := Value{Kind: KindString, String: sentinel}
	for _, kind := range []EventKind{EventConfigSet, EventConfigFileChanged} {
		if err := log.append(Event{
			Time:  auditTestClock(t).Add(time.Duration(len(kind)) * time.Second),
			Event: kind,
			Key:   auditTestSecretKey,
			Old:   &oldValue,
			New:   &newValue,
		}); err != nil {
			t.Fatalf("append %s: %v", kind, err)
		}
	}

	events, err := log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	for _, event := range events {
		if event.Key != auditTestSecretKey {
			t.Errorf("key = %q, want %q", event.Key, auditTestSecretKey)
		}
		if event.Old != nil || event.New != nil {
			t.Errorf("secret record carried old/new: %#v %#v", event.Old, event.New)
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if bytes.Contains(data, []byte(sentinel)) {
		t.Errorf("secret value was written to the audit log")
	}
}

// TestREQBrokerConfig007_AllEventKinds checks that every event kind is written
// and read back in order.
func TestREQBrokerConfig007_AllEventKinds(t *testing.T) {
	log, _, _ := newAuditTestLog(t, 0o700)
	kinds := []EventKind{
		EventConfigSet,
		EventConfigFileChanged,
		EventPairingKeyCreated,
		EventPairingSucceeded,
		EventPairingFailed,
		EventLinkUp,
		EventLinkDown,
		EventUnpaired,
		EventConnectionRejected,
		EventRejectedByPeer,
		EventLocalAliasAssigned,
		EventConfigRejected,
		EventUpdateApplied,
		EventUpdateFailed,
	}
	base := auditTestClock(t)
	for index, kind := range kinds {
		if err := log.append(Event{
			Time:  base.Add(time.Duration(index) * time.Second),
			Event: kind,
		}); err != nil {
			t.Fatalf("append %q: %v", kind, err)
		}
	}

	events, err := log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if len(events) != len(kinds) {
		t.Fatalf("got %d events, want %d", len(events), len(kinds))
	}
	for index, kind := range kinds {
		if events[index].Event != kind {
			t.Errorf("event[%d] = %q, want %q", index, events[index].Event, kind)
		}
	}
}

// TestREQBrokerConfig007_ActorAccountPeer checks that the actor, account and
// peer fields round-trip, with an empty alias for a rejected connection.
func TestREQBrokerConfig007_ActorAccountPeer(t *testing.T) {
	log, _, _ := newAuditTestLog(t, 0o700)
	fingerprint := testFingerprint(0x10)
	when := auditTestClock(t)
	if err := log.append(Event{
		Time:    when,
		Event:   EventPairingSucceeded,
		Actor:   "buildbox.ana/ana",
		Account: "ana",
		Peer:    &PeerRef{Fingerprint: fingerprint, Alias: "peer-17443"},
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := log.append(Event{
		Time:   when.Add(time.Second),
		Event:  EventConnectionRejected,
		Peer:   &PeerRef{Fingerprint: testFingerprint(0x40)},
		Detail: "unknown fingerprint",
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	events, err := log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	if events[0].Actor != "buildbox.ana/ana" || events[0].Account != "ana" {
		t.Errorf("actor/account = %q/%q", events[0].Actor, events[0].Account)
	}
	if events[0].Peer == nil {
		t.Fatal("peer is nil")
	}
	if events[0].Peer.Fingerprint != fingerprint {
		t.Errorf("fingerprint = %x, want %x", events[0].Peer.Fingerprint, fingerprint)
	}
	if events[0].Peer.Alias != "peer-17443" {
		t.Errorf("alias = %q", events[0].Peer.Alias)
	}
	if events[1].Peer == nil || events[1].Peer.Alias != "" {
		t.Errorf("rejected connection peer = %#v, want an empty alias", events[1].Peer)
	}
	if events[1].Detail != "unknown fingerprint" {
		t.Errorf("detail = %q", events[1].Detail)
	}
}

// TestREQBrokerConfig008_SecretNeverPersisted checks that a secret value never
// reaches the audit file or the run log.
func TestREQBrokerConfig008_SecretNeverPersisted(t *testing.T) {
	log, path, logs := newAuditTestLog(t, 0o700)
	const sentinel = "PAIRING-KEY-SENTINEL-0123456789abcdef"
	when := auditTestClock(t)

	if err := log.append(Event{
		Time:  when,
		Event: EventConfigSet,
		Key:   auditTestSecretKey,
		Old:   &Value{Kind: KindString, String: sentinel},
		New:   &Value{Kind: KindString, String: sentinel},
	}); err != nil {
		t.Fatalf("append config_set: %v", err)
	}
	if err := log.append(Event{
		Time:  when.Add(time.Second),
		Event: EventConfigFileChanged,
		Key:   auditTestSecretKey,
		Old:   &Value{Kind: KindSecret, SecretIsSet: true},
		New:   &Value{Kind: KindSecret, SecretIsSet: true},
	}); err != nil {
		t.Fatalf("append config_file_changed: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if bytes.Contains(data, []byte(sentinel)) {
		t.Fatalf("sentinel secret appears in the audit file")
	}
	if bytes.Contains(logs.Bytes(), []byte(sentinel)) {
		t.Fatalf("sentinel secret appears in the run log")
	}
	events, err := log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	for _, event := range events {
		if event.Old != nil || event.New != nil {
			t.Errorf("secret event carries old/new: %#v %#v", event.Old, event.New)
		}
	}
}

// TestREQBrokerConfig008_NoMessageBodyField checks that a record carries only
// the allowed fields and no message body.
func TestREQBrokerConfig008_NoMessageBodyField(t *testing.T) {
	log, path, _ := newAuditTestLog(t, 0o700)
	if err := log.append(Event{
		Time:   auditTestClock(t),
		Event:  EventLinkUp,
		Detail: "connection reset",
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := log.append(Event{
		Time:  auditTestClock(t).Add(time.Second),
		Event: EventLinkDown,
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	allowed := map[string]bool{
		"time": true, "event": true, "actor": true, "account": true,
		"peer": true, "key": true, "old": true, "new": true,
		"count": true, "detail": true,
	}
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte{'\n'}) {
		var record map[string]json.RawMessage
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("decode record: %v", err)
		}
		for field := range record {
			if !allowed[field] {
				t.Errorf("audit record has unexpected field %q", field)
			}
		}
		for _, forbidden := range []string{"body", "message", "payload", "content"} {
			if _, ok := record[forbidden]; ok {
				t.Errorf("audit record has forbidden field %q", forbidden)
			}
		}
	}
}

// TestREQBrokerConfig009_ReadAuditSinceAndOrder checks that read sorts by time,
// filters at or after since, and keeps file order for equal timestamps.
func TestREQBrokerConfig009_ReadAuditSinceAndOrder(t *testing.T) {
	log, _, _ := newAuditTestLog(t, 0o700)
	base := auditTestClock(t)
	for _, offset := range []time.Duration{2 * time.Minute, 0, time.Minute} {
		if err := log.append(Event{
			Time:   base.Add(offset),
			Event:  EventLinkUp,
			Detail: offset.String(),
		}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	events, err := log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("got %d events, want 3", len(events))
	}
	for index := 1; index < len(events); index++ {
		if events[index].Time.Before(events[index-1].Time) {
			t.Fatalf("events are not in ascending time order: %s before %s",
				events[index].Time, events[index-1].Time)
		}
	}

	since := base.Add(time.Minute)
	filtered, err := log.read(since)
	if err != nil {
		t.Fatalf("read audit log with since: %v", err)
	}
	if len(filtered) != 2 {
		t.Fatalf("got %d events at or after since, want 2", len(filtered))
	}
	if filtered[0].Time.Before(since) {
		t.Errorf("first filtered time %s is before since %s", filtered[0].Time, since)
	}

	if err := log.append(Event{Time: base, Event: EventLinkDown, Detail: "first"}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := log.append(Event{Time: base, Event: EventLinkDown, Detail: "second"}); err != nil {
		t.Fatalf("append: %v", err)
	}
	events, err = log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	var sameTime []string
	for _, event := range events {
		if event.Time.Equal(base) && event.Event == EventLinkDown {
			sameTime = append(sameTime, event.Detail)
		}
	}
	if !reflect.DeepEqual(sameTime, []string{"first", "second"}) {
		t.Errorf("equal-time order = %v, want [first second]", sameTime)
	}
}

// TestConfigAuditMergeCount checks that rejected connections from one
// fingerprint merge into one counted record while a later event or another
// fingerprint stays separate.
func TestConfigAuditMergeCount(t *testing.T) {
	log, _, _ := newAuditTestLog(t, 0o700)
	base := auditTestClock(t)
	fingerprint := testFingerprint(0x20)
	peer := &PeerRef{Fingerprint: fingerprint}
	for _, offset := range []time.Duration{0, 5 * time.Minute, 9*time.Minute + 59*time.Second} {
		if err := log.append(Event{Time: base.Add(offset), Event: EventConnectionRejected, Peer: peer}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := log.append(Event{Time: base.Add(21 * time.Minute), Event: EventConnectionRejected, Peer: peer}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := log.append(Event{
		Time:  base.Add(time.Minute),
		Event: EventConnectionRejected,
		Peer:  &PeerRef{Fingerprint: testFingerprint(0x60)},
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	events, err := log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("got %d events, want 3 (one merged group, one later record, one other peer)", len(events))
	}
	if events[0].Count != 3 {
		t.Errorf("first group count = %d, want 3", events[0].Count)
	}
	if events[1].Count != 0 {
		t.Errorf("later record count = %d, want 0", events[1].Count)
	}
	if events[2].Count != 0 {
		t.Errorf("other peer count = %d, want 0", events[2].Count)
	}
}

// TestConfigAuditMergePairingFailed checks that pairing failures with the same
// merge key merge, while an empty key or a different source stays separate.
func TestConfigAuditMergePairingFailed(t *testing.T) {
	log, _, _ := newAuditTestLog(t, 0o700)
	base := auditTestClock(t)
	for _, offset := range []time.Duration{0, time.Minute, 2 * time.Minute} {
		if err := log.append(Event{
			Time:     base.Add(offset),
			Event:    EventPairingFailed,
			MergeKey: "fp:abc",
			Detail:   "too many attempts",
		}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := log.append(Event{
		Time:   base.Add(3 * time.Minute),
		Event:  EventPairingFailed,
		Detail: "bad key",
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := log.append(Event{
		Time:     base.Add(4 * time.Minute),
		Event:    EventPairingFailed,
		MergeKey: "fp:def",
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	events, err := log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("got %d events, want 3", len(events))
	}
	if events[0].Count != 3 {
		t.Errorf("merged count = %d, want 3", events[0].Count)
	}
	if events[1].Count != 0 || events[2].Count != 0 {
		t.Errorf("unmerged records have counts %d and %d", events[1].Count, events[2].Count)
	}
}

// TestConfigAuditMergePairingFailedByReason checks that two pairing failures
// from one source with different reasons keep separate counted records.
func TestConfigAuditMergePairingFailedByReason(t *testing.T) {
	log, _, _ := newAuditTestLog(t, 0o700)
	base := auditTestClock(t)
	details := []string{"try later", "version incompatible", "try later", "version incompatible"}
	for i, detail := range details {
		if err := log.append(Event{
			Time:     base.Add(time.Duration(i) * time.Minute),
			Event:    EventPairingFailed,
			MergeKey: "source:192.0.2.10/32",
			Detail:   detail,
		}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	events, err := log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2 (one merged record per reason)", len(events))
	}
	counts := make(map[string]uint64, len(events))
	for _, event := range events {
		counts[event.Detail] = event.Count
	}
	for _, detail := range []string{"try later", "version incompatible"} {
		if got := counts[detail]; got != 2 {
			t.Errorf("merged count for %q = %d, want 2; events = %+v", detail, got, events)
		}
	}
}

// TestConfigAuditMergeKeyNotPersisted checks that the merge key reaches neither
// the audit file nor a read record.
func TestConfigAuditMergeKeyNotPersisted(t *testing.T) {
	log, path, _ := newAuditTestLog(t, 0o700)
	base := auditTestClock(t)
	for _, offset := range []time.Duration{0, auditMergeWindow + time.Minute} {
		if err := log.append(Event{
			Time:     base.Add(offset),
			Event:    EventPairingFailed,
			MergeKey: "source:192.0.2.10",
		}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if bytes.Contains(data, []byte("merge")) || bytes.Contains(data, []byte("192.0.2.10")) {
		t.Fatalf("merge key was persisted to the audit file: %s", data)
	}

	events, err := log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	for _, event := range events {
		if event.MergeKey != "" {
			t.Errorf("ReadAudit returned the internal merge key %q", event.MergeKey)
		}
	}
}

// TestConfigAuditConnectionRejectedBySource checks that rejected connections
// sharing a source merge key become one record counting all three.
func TestConfigAuditConnectionRejectedBySource(t *testing.T) {
	log, _, _ := newAuditTestLog(t, 0o700)
	base := auditTestClock(t)
	for index := 0; index < 3; index++ {
		if err := log.append(Event{
			Time:     base.Add(time.Duration(index) * time.Minute),
			Event:    EventConnectionRejected,
			MergeKey: "source:192.0.2.10",
		}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	events, err := log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if len(events) != 1 || events[0].Count != 3 {
		t.Fatalf("got %d events with count %d, want 1 event with count 3",
			len(events), events[0].Count)
	}
}

// TestConfigAuditMergeClockRollback checks that an event whose time is earlier
// than the open window starts a new record.
func TestConfigAuditMergeClockRollback(t *testing.T) {
	log, _, _ := newAuditTestLog(t, 0o700)
	base := auditTestClock(t)
	peer := &PeerRef{Fingerprint: testFingerprint(0x30)}
	if err := log.append(Event{Time: base, Event: EventConnectionRejected, Peer: peer}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := log.append(Event{Time: base.Add(-time.Minute), Event: EventConnectionRejected, Peer: peer}); err != nil {
		t.Fatalf("append: %v", err)
	}
	events, err := log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2 (clock rollback starts a new record)", len(events))
	}
}

func auditFileRecords(t *testing.T, path string) []Event {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read audit file: %v", err)
	}
	var events []Event
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		event, err := decodeEvent(line)
		if err != nil {
			t.Fatalf("decode audit record %q: %v", line, err)
		}
		events = append(events, event)
	}
	return events
}

// TestConfigAuditMergeHoldsEventsInMemory checks that a window's events stay in
// memory until it closes while read still returns the pending record.
func TestConfigAuditMergeHoldsEventsInMemory(t *testing.T) {
	log, path, _ := newAuditTestLog(t, 0o700)
	base := auditTestClock(t)
	peer := &PeerRef{Fingerprint: testFingerprint(0x70)}
	for _, offset := range []time.Duration{0, time.Minute, 2 * time.Minute} {
		if err := log.append(Event{Time: base.Add(offset), Event: EventConnectionRejected, Peer: peer}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	if records := auditFileRecords(t, path); len(records) != 0 {
		t.Fatalf("file has %d records before the window closed, want 0", len(records))
	}

	events, err := log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1 pending merged record", len(events))
	}
	if events[0].Count != 3 {
		t.Errorf("pending record count = %d, want 3", events[0].Count)
	}
	if !events[0].Time.Equal(base) {
		t.Errorf("pending record time = %s, want the first event's time %s", events[0].Time, base)
	}
}

// TestConfigAuditMergeWindowStartsAtFirstEvent checks that the window is
// anchored at the first event, not chained from each adjacent event.
func TestConfigAuditMergeWindowStartsAtFirstEvent(t *testing.T) {
	log, path, _ := newAuditTestLog(t, 0o700)
	base := auditTestClock(t)
	peer := &PeerRef{Fingerprint: testFingerprint(0x71)}
	for _, offset := range []time.Duration{0, 6 * time.Minute, 12 * time.Minute} {
		if err := log.append(Event{Time: base.Add(offset), Event: EventConnectionRejected, Peer: peer}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	records := auditFileRecords(t, path)
	if len(records) != 1 {
		t.Fatalf("file has %d records, want the expired window's one record", len(records))
	}
	if records[0].Count != 2 {
		t.Errorf("expired window count = %d, want 2", records[0].Count)
	}
	if !records[0].Time.Equal(base) {
		t.Errorf("expired window time = %s, want %s", records[0].Time, base)
	}

	events, err := log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want the expired window and the new pending window", len(events))
	}
	if events[0].Count != 2 || !events[0].Time.Equal(base) {
		t.Errorf("first event = time %s count %d, want time %s count 2", events[0].Time, events[0].Count, base)
	}
	if events[1].Count != 0 || !events[1].Time.Equal(base.Add(12*time.Minute)) {
		t.Errorf("second event = time %s count %d, want time %s count 0",
			events[1].Time, events[1].Count, base.Add(12*time.Minute))
	}
}

// TestConfigAuditMergeWindowIncludesBoundary checks that events exactly one
// merge window apart share the window.
func TestConfigAuditMergeWindowIncludesBoundary(t *testing.T) {
	log, path, _ := newAuditTestLog(t, 0o700)
	base := auditTestClock(t)
	peer := &PeerRef{Fingerprint: testFingerprint(0x72)}
	for _, offset := range []time.Duration{0, auditMergeWindow} {
		if err := log.append(Event{Time: base.Add(offset), Event: EventConnectionRejected, Peer: peer}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if records := auditFileRecords(t, path); len(records) != 0 {
		t.Fatalf("file has %d records, want both events held in one open window", len(records))
	}
	events, err := log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if len(events) != 1 || events[0].Count != 2 {
		t.Fatalf("got %d events with counts %v, want one record with count 2", len(events), auditCounts(events))
	}
}

// TestConfigAuditCloseFlushesPendingCount checks that Close writes the
// accumulated count to the file.
func TestConfigAuditCloseFlushesPendingCount(t *testing.T) {
	log, path, _ := newAuditTestLog(t, 0o700)
	base := auditTestClock(t)
	peer := &PeerRef{Fingerprint: testFingerprint(0x73)}
	for _, offset := range []time.Duration{0, time.Minute} {
		if err := log.append(Event{Time: base.Add(offset), Event: EventConnectionRejected, Peer: peer}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if records := auditFileRecords(t, path); len(records) != 0 {
		t.Fatalf("file has %d records before close, want 0", len(records))
	}
	if err := log.close(); err != nil {
		t.Fatalf("close audit log: %v", err)
	}

	records := auditFileRecords(t, path)
	if len(records) != 1 {
		t.Fatalf("file has %d records after close, want 1", len(records))
	}
	if records[0].Count != 2 {
		t.Errorf("flushed record count = %d, want 2", records[0].Count)
	}
	if !records[0].Time.Equal(base) {
		t.Errorf("flushed record time = %s, want %s", records[0].Time, base)
	}
}

type auditTestClockControl struct {
	mu  sync.Mutex
	now time.Time
}

func (c *auditTestClockControl) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *auditTestClockControl) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

type auditTestTimers struct {
	mu        sync.Mutex
	durations []time.Duration
	callbacks []func()
}

func (t *auditTestTimers) afterFunc(d time.Duration, f func()) auditTimer {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.durations = append(t.durations, d)
	t.callbacks = append(t.callbacks, f)
	return auditTestTimer{}
}

type auditTestTimer struct{}

func (auditTestTimer) Stop() {}

func (t *auditTestTimers) fireAll() {
	t.mu.Lock()
	callbacks := append([]func(){}, t.callbacks...)
	t.callbacks = nil
	t.mu.Unlock()
	for _, callback := range callbacks {
		callback()
	}
}

// TestConfigAuditWindowExpiryFlushesThroughTimer checks that the timer seam
// fires the expiry callback and appends the accumulated count.
func TestConfigAuditWindowExpiryFlushesThroughTimer(t *testing.T) {
	base := auditTestClock(t)
	clock := &auditTestClockControl{now: base}
	timers := &auditTestTimers{}
	path := filepath.Join(t.TempDir(), "data", "audit.log")
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	log, err := openAudit(path, clock.Now, logger)
	if err != nil {
		t.Fatalf("open audit log: %v", err)
	}
	log.afterFunc = timers.afterFunc
	t.Cleanup(func() { _ = log.close() })

	peer := &PeerRef{Fingerprint: testFingerprint(0x75)}
	for _, offset := range []time.Duration{0, time.Minute} {
		if err := log.append(Event{Time: base.Add(offset), Event: EventConnectionRejected, Peer: peer}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if records := auditFileRecords(t, path); len(records) != 0 {
		t.Fatalf("file has %d records before the window expired, want 0", len(records))
	}

	timers.mu.Lock()
	armed := append([]time.Duration{}, timers.durations...)
	timers.mu.Unlock()
	if len(armed) != 1 {
		t.Fatalf("window scheduled %d timers, want 1", len(armed))
	}
	if armed[0] != auditMergeWindow {
		t.Errorf("timer delay = %s, want %s", armed[0], auditMergeWindow)
	}

	clock.Advance(auditMergeWindow)
	timers.fireAll()

	records := auditFileRecords(t, path)
	if len(records) != 1 {
		t.Fatalf("file has %d records after the window expired, want 1", len(records))
	}
	if records[0].Count != 2 {
		t.Errorf("flushed record count = %d, want 2", records[0].Count)
	}
	if !records[0].Time.Equal(base) {
		t.Errorf("flushed record time = %s, want %s", records[0].Time, base)
	}
	events, err := log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if len(events) != 1 || events[0].Count != 2 {
		t.Fatalf("read returned %d events with counts %v, want one record with count 2",
			len(events), auditCounts(events))
	}
}

// TestConfigAuditTimerEarlyFireRearmsUntilDeadline checks that an early firing
// arms a timer for the remaining time and the window is still written without a
// later event.
func TestConfigAuditTimerEarlyFireRearmsUntilDeadline(t *testing.T) {
	log, path, clock, timers, _ := newAuditTestLogAt(t)
	base := clock.Now()
	if err := log.append(Event{
		Time:     base,
		Event:    EventConnectionRejected,
		MergeKey: "source:203.0.113.7/32",
	}); err != nil {
		t.Fatalf("append connection_rejected: %v", err)
	}

	clock.Advance(auditMergeWindow - time.Nanosecond)
	timers.fireAll()
	if records := auditFileRecords(t, path); len(records) != 0 {
		t.Fatalf("file has %d records before the deadline, want 0", len(records))
	}

	timers.mu.Lock()
	armed := append([]time.Duration{}, timers.durations...)
	timers.mu.Unlock()
	if len(armed) != 2 {
		t.Fatalf("early firing armed %d timers in total, want 2", len(armed))
	}
	if armed[1] != time.Nanosecond {
		t.Errorf("re-armed timer delay = %s, want the remaining %s", armed[1], time.Nanosecond)
	}

	clock.Advance(time.Nanosecond)
	timers.fireAll()
	records := auditFileRecords(t, path)
	if len(records) != 1 {
		t.Fatalf("file has %d records after the deadline with no further events, want 1", len(records))
	}
	if !records[0].Time.Equal(base) {
		t.Errorf("flushed record time = %s, want %s", records[0].Time, base)
	}
}

// TestConfigAuditTimerStaleCallbackIgnoredAfterReschedule checks that a stale
// timer callback does nothing and only the current callback re-arms.
func TestConfigAuditTimerStaleCallbackIgnoredAfterReschedule(t *testing.T) {
	log, path, clock, timers, _ := newAuditTestLogAt(t)
	base := clock.Now()

	if err := log.append(Event{
		Time:     base.Add(time.Minute),
		Event:    EventConnectionRejected,
		MergeKey: "source:203.0.113.10/32",
	}); err != nil {
		t.Fatalf("append the later window: %v", err)
	}
	if err := log.append(Event{
		Time:     base,
		Event:    EventConnectionRejected,
		MergeKey: "source:203.0.113.11/32",
	}); err != nil {
		t.Fatalf("append the earlier window: %v", err)
	}

	timers.mu.Lock()
	armed := append([]time.Duration{}, timers.durations...)
	timers.mu.Unlock()
	if len(armed) != 2 {
		t.Fatalf("scheduling armed %d timers, want 2", len(armed))
	}
	if armed[0] != auditMergeWindow+time.Minute {
		t.Errorf("first timer delay = %s, want %s", armed[0], auditMergeWindow+time.Minute)
	}
	if armed[1] != auditMergeWindow {
		t.Errorf("replacement timer delay = %s, want %s", armed[1], auditMergeWindow)
	}

	timers.fireAll()

	if records := auditFileRecords(t, path); len(records) != 0 {
		t.Fatalf("stale callback flushed %d records before the deadline, want 0", len(records))
	}
	timers.mu.Lock()
	armed = append([]time.Duration{}, timers.durations...)
	timers.mu.Unlock()
	if len(armed) != 3 {
		t.Fatalf("firing the stale and current callbacks armed %d timers in total, want 3 (one re-arm)",
			len(armed))
	}
	if armed[2] != auditMergeWindow {
		t.Errorf("re-armed timer delay = %s, want %s", armed[2], auditMergeWindow)
	}
	log.mu.Lock()
	due := log.timerDue
	hasTimer := log.timer != nil
	log.mu.Unlock()
	if !hasTimer || !due.Equal(base.Add(auditMergeWindow)) {
		t.Fatalf("after firing, timer due = %s (armed %v), want %s", due, hasTimer, base.Add(auditMergeWindow))
	}
}

// TestConfigAuditReadMergesFileAndPendingInTimeOrder checks that a pending
// window opened before a written record still comes first.
func TestConfigAuditReadMergesFileAndPendingInTimeOrder(t *testing.T) {
	log, _, _ := newAuditTestLog(t, 0o700)
	base := auditTestClock(t)
	peer := &PeerRef{Fingerprint: testFingerprint(0x74)}
	if err := log.append(Event{Time: base.Add(5 * time.Minute), Event: EventLinkUp}); err != nil {
		t.Fatalf("append link_up: %v", err)
	}
	for _, offset := range []time.Duration{0, time.Minute} {
		if err := log.append(Event{Time: base.Add(offset), Event: EventConnectionRejected, Peer: peer}); err != nil {
			t.Fatalf("append connection_rejected: %v", err)
		}
	}

	events, err := log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	if !events[0].Time.Equal(base) || events[0].Count != 2 {
		t.Errorf("first event = time %s count %d, want time %s count 2",
			events[0].Time, events[0].Count, base)
	}
	if !events[1].Time.Equal(base.Add(5*time.Minute)) || events[1].Event != EventLinkUp {
		t.Errorf("second event = time %s kind %q, want the written link_up at %s",
			events[1].Time, events[1].Event, base.Add(5*time.Minute))
	}
}

func auditCounts(events []Event) []uint64 {
	counts := make([]uint64, len(events))
	for index, event := range events {
		counts[index] = event.Count
	}
	return counts
}

// TestConfigAuditAppendOnlyFile checks that flushing a merged window only
// appends and leaves every existing byte in place.
func TestConfigAuditAppendOnlyFile(t *testing.T) {
	log, path, _ := newAuditTestLog(t, 0o700)
	base := auditTestClock(t)
	if err := log.append(Event{Time: base, Event: EventLinkUp}); err != nil {
		t.Fatalf("append: %v", err)
	}
	first, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if len(first) == 0 {
		t.Fatal("first append produced an empty file")
	}

	peer := &PeerRef{Fingerprint: testFingerprint(0x50)}
	for _, offset := range []time.Duration{time.Minute, 2 * time.Minute, 20 * time.Minute} {
		if err := log.append(Event{
			Time:  base.Add(offset),
			Event: EventConnectionRejected,
			Peer:  peer,
		}); err != nil {
			t.Fatalf("append: %v", err)
		}
		if _, err := log.read(time.Time{}); err != nil {
			t.Fatalf("read: %v", err)
		}
	}
	if err := log.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if !bytes.HasPrefix(after, first) {
		t.Fatalf("flushing or reading rewrote the existing audit bytes")
	}
	if len(after) <= len(first) {
		t.Fatalf("audit file did not grow: %d then %d bytes", len(first), len(after))
	}
	records := auditFileRecords(t, path)
	if len(records) != 3 {
		t.Fatalf("file has %d records, want link_up, one merged window and one later record", len(records))
	}
	if records[1].Count != 2 || !records[1].Time.Equal(base.Add(time.Minute)) {
		t.Errorf("merged record = time %s count %d, want time %s count 2",
			records[1].Time, records[1].Count, base.Add(time.Minute))
	}
}

// TestConfigAuditFileMode checks that a new audit file is created with mode
// 0600 and its directory with 0700.
func TestConfigAuditFileMode(t *testing.T) {
	log, path, _ := newAuditTestLog(t, 0o700)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat audit log: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Errorf("audit file mode = %04o, want 0600", got)
	}
	dirInfo, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatalf("stat audit directory: %v", err)
	}
	if got := dirInfo.Mode().Perm(); got != 0o700 {
		t.Errorf("audit directory mode = %04o, want 0700", got)
	}
	if err := log.append(Event{Time: auditTestClock(t), Event: EventLinkUp}); err != nil {
		t.Fatalf("append: %v", err)
	}
}

// TestConfigAuditPreservesExistingFileMode checks that opening an existing
// audit file keeps its mode.
func TestConfigAuditPreservesExistingFileMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.log")
	if err := os.WriteFile(path, nil, 0o640); err != nil {
		t.Fatalf("seed audit log: %v", err)
	}
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatalf("chmod audit log: %v", err)
	}
	log, err := openAudit(path, time.Now, slog.Default())
	if err != nil {
		t.Fatalf("open audit log: %v", err)
	}
	defer func() { _ = log.close() }()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat audit log: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o640 {
		t.Errorf("existing audit file mode = %04o, want 0640", got)
	}
}

// TestConfigAuditMalformedLineSkipped checks that undecodable lines are skipped
// with an error log entry and the readable records survive.
func TestConfigAuditMalformedLineSkipped(t *testing.T) {
	log, path, logs := newAuditTestLog(t, 0o700)
	base := auditTestClock(t)
	if err := log.append(Event{Time: base, Event: EventLinkUp}); err != nil {
		t.Fatalf("append: %v", err)
	}

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open for raw append: %v", err)
	}
	if _, err := file.WriteString("{\"time\":\"not-a-time\",\"event\":\"link_up\"}\n"); err != nil {
		t.Fatalf("write bad line: %v", err)
	}
	if _, err := file.WriteString("truncated-json\n"); err != nil {
		t.Fatalf("write bad line: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close raw append: %v", err)
	}

	if err := log.append(Event{Time: base.Add(time.Second), Event: EventLinkDown}); err != nil {
		t.Fatalf("append: %v", err)
	}
	events, err := log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	if !strings.Contains(logs.String(), "skipping unreadable audit record") {
		t.Errorf("bad lines were skipped without an error log entry: %s", logs.String())
	}
}

// TestConfigAuditUnknownEventKindDropped checks that an unknown event kind is
// refused, nothing is written and the drop is logged.
func TestConfigAuditUnknownEventKindDropped(t *testing.T) {
	log, path, logs := newAuditTestLog(t, 0o700)
	if err := log.append(Event{Time: auditTestClock(t), Event: EventKind("bogus")}); err == nil {
		t.Fatal("append of an unknown event kind returned no error")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if len(bytes.TrimSpace(data)) != 0 {
		t.Errorf("unknown event kind was written: %s", data)
	}
	if !strings.Contains(logs.String(), "config_audit_unknown_kind") {
		t.Errorf("drop was not logged: %s", logs.String())
	}
}

// TestConfigAuditZeroTimeUsesClock checks that an event without a time takes
// the injected clock's time.
func TestConfigAuditZeroTimeUsesClock(t *testing.T) {
	log, _, _ := newAuditTestLog(t, 0o700)
	if err := log.append(Event{Event: EventLinkUp}); err != nil {
		t.Fatalf("append: %v", err)
	}
	events, err := log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if !events[0].Time.Equal(auditTestClock(t)) {
		t.Errorf("time = %s, want the injected clock %s", events[0].Time, auditTestClock(t))
	}
}

// TestConfigAuditValueKindsRoundTrip checks that every value kind survives
// encoding and decoding unchanged.
func TestConfigAuditValueKindsRoundTrip(t *testing.T) {
	log, _, _ := newAuditTestLog(t, 0o700)
	cases := []Value{
		{Kind: KindString, String: "hello"},
		{Kind: KindBool, Bool: true},
		{Kind: KindBool, Bool: false},
		{Kind: KindInt, Int: -42},
		{Kind: KindDuration, Duration: 90 * time.Second},
		{Kind: KindStringList, Strings: []string{"a", "b"}},
	}
	base := auditTestClock(t)
	for index, value := range cases {
		value := value
		if err := log.append(Event{
			Time:  base.Add(time.Duration(index) * time.Second),
			Event: EventConfigSet,
			Key:   "broker.alias",
			New:   &value,
		}); err != nil {
			t.Fatalf("append %#v: %v", value, err)
		}
	}
	events, err := log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if len(events) != len(cases) {
		t.Fatalf("got %d events, want %d", len(events), len(cases))
	}
	for index, want := range cases {
		if events[index].New == nil {
			t.Fatalf("event %d has no new value", index)
		}
		if !reflect.DeepEqual(*events[index].New, want) {
			t.Errorf("event %d value = %#v, want %#v", index, *events[index].New, want)
		}
	}
}

// TestConfigAuditValueKindSetExcludesUnset checks that the unset kind is
// refused by both encoding and decoding.
func TestConfigAuditValueKindSetExcludesUnset(t *testing.T) {
	if _, err := encodeValue(Value{Kind: KindUnset}); err == nil {
		t.Error("encodeValue accepted KindUnset")
	}
	if _, err := decodeValue(wireValue{Kind: "unset"}); err == nil {
		t.Error("decodeValue accepted the unset kind")
	}
}

// TestConfigAuditCloseIsIdempotent checks that a second Close succeeds and a
// later append or read returns an error.
func TestConfigAuditCloseIsIdempotent(t *testing.T) {
	log, _, _ := newAuditTestLog(t, 0o700)
	if err := log.close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := log.close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if err := log.append(Event{Time: auditTestClock(t), Event: EventLinkUp}); err == nil {
		t.Fatal("append after close returned no error")
	}
	if _, err := log.read(time.Time{}); err == nil {
		t.Fatal("read after close returned no error")
	}
}

// TestConfigAuditCloseConcurrentWithAudit checks that Audit and ReadAudit may
// run while Close clears the audit pointer, and a later read returns
// ErrNotLoaded.
func TestConfigAuditCloseConcurrentWithAudit(t *testing.T) {
	c, _ := newWatchCore(t, "broker:\n  alias: old\n", false, nil)
	var logs bytes.Buffer
	c.logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	when := auditTestClock(t)

	const workers = 8
	const perWorker = 200
	var wait sync.WaitGroup
	start := make(chan struct{})
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			for index := 0; index < perWorker; index++ {
				c.auditAppend(Event{Time: when, Event: EventLinkUp})
				_, _ = c.readAudit(time.Time{})
			}
		}()
	}
	wait.Add(1)
	go func() {
		defer wait.Done()
		<-start
		c.close()
	}()
	close(start)
	wait.Wait()

	if _, err := c.readAudit(time.Time{}); err != ErrNotLoaded {
		t.Fatalf("ReadAudit after Close = %v, want ErrNotLoaded", err)
	}
}

// TestConfigAuditConcurrentAppendAndRead checks that concurrent appends and
// reads keep every record on its own line and readable.
func TestConfigAuditConcurrentAppendAndRead(t *testing.T) {
	log, path, _ := newAuditTestLog(t, 0o700)
	const writers = 6
	const perWriter = 40
	base := auditTestClock(t)

	var wait sync.WaitGroup
	errs := make(chan error, writers)
	for writer := 0; writer < writers; writer++ {
		writer := writer
		wait.Add(1)
		go func() {
			defer wait.Done()
			for index := 0; index < perWriter; index++ {
				when := base.Add(time.Duration(writer*perWriter+index) * time.Second)
				if err := log.append(Event{Time: when, Event: EventLinkUp}); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	stop := make(chan struct{})
	var readers sync.WaitGroup
	for reader := 0; reader < 2; reader++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := log.read(time.Time{}); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wait.Wait()
	close(stop)
	readers.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent operation: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if got := bytes.Count(data, []byte{'\n'}); got != writers*perWriter {
		t.Fatalf("file has %d lines, want %d", got, writers*perWriter)
	}
	events, err := log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if len(events) != writers*perWriter {
		t.Fatalf("read %d events, want %d", len(events), writers*perWriter)
	}
}

// TestConfigAuditRepairsMissingTrailingNewline checks that a partial final line
// gets a newline so a later record lands on its own line.
func TestConfigAuditRepairsMissingTrailingNewline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.log")
	when := auditTestClock(t)
	first, err := encodeEvent(Event{Time: when, Event: EventLinkUp})
	if err != nil {
		t.Fatalf("encode first record: %v", err)
	}
	if len(first) == 0 || first[len(first)-1] != '\n' {
		t.Fatalf("encoded record does not end with a newline: %q", first)
	}
	partial := []byte(`{"time":"2026-11-18T12:00:01.000000000Z","event":"link_`)
	if err := os.WriteFile(path, append(append([]byte(nil), first...), partial...), 0o600); err != nil {
		t.Fatal(err)
	}

	log, err := openAudit(path, func() time.Time { return when }, testLogger())
	if err != nil {
		t.Fatalf("open audit log: %v", err)
	}
	second := Event{Time: when.Add(time.Second), Event: EventLinkDown}
	if err := log.append(second); err != nil {
		t.Fatalf("append after the partial line: %v", err)
	}
	if err := log.close(); err != nil {
		t.Fatalf("close audit log: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(data, []byte{'\n'})
	if len(lines) != 4 || len(lines[3]) != 0 {
		t.Fatalf("file has %d line segments %q, want first record, partial tail, second record and a final newline", len(lines), data)
	}
	if !bytes.Equal(lines[1], partial) {
		t.Errorf("partial tail changed from %q to %q; the log must stay append-only", partial, lines[1])
	}
	event, err := decodeEvent(lines[2])
	if err != nil {
		t.Fatalf("the appended record is not on its own decodable line: %v (%q)", err, lines[2])
	}
	if event.Event != EventLinkDown || !event.Time.Equal(second.Time) {
		t.Errorf("appended record = %+v, want %+v", event, second)
	}
}

// TestConfigAuditKeepsTerminatedFileUnchanged checks that opening a log that
// already ends with a newline leaves it unchanged.
func TestConfigAuditKeepsTerminatedFileUnchanged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.log")
	when := auditTestClock(t)
	first, err := encodeEvent(Event{Time: when, Event: EventLinkUp})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, first, 0o600); err != nil {
		t.Fatal(err)
	}
	log, err := openAudit(path, func() time.Time { return when }, testLogger())
	if err != nil {
		t.Fatalf("open audit log: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, first) {
		t.Errorf("opening a terminated log changed it:\nbefore %q\nafter  %q", first, data)
	}
	if err := log.close(); err != nil {
		t.Fatal(err)
	}
}

// TestConfigAuditAfterCloseIsLogged checks that an event produced after Close
// is reported to the run log.
func TestConfigAuditAfterCloseIsLogged(t *testing.T) {
	c, _ := newNCCore(t, "broker:\n  alias: old\n", nil)
	var logs bytes.Buffer
	c.logger = slog.New(slog.NewTextHandler(&logs, nil))

	c.auditMu.RLock()
	log := c.audit
	c.auditMu.RUnlock()
	if log == nil {
		t.Fatal("the test core has no audit log")
	}
	if err := log.close(); err != nil {
		t.Fatalf("close audit log: %v", err)
	}

	c.auditAppend(Event{Event: EventLinkUp})
	if got := logs.String(); !strings.Contains(got, "config_audit_closed") {
		t.Fatalf("an event after Close was dropped silently; log = %q", got)
	}
}

// TestAppendAuditWritesWithoutLoading checks that AppendAudit writes one record
// from a process that did not load the configuration.
func TestAppendAuditWritesWithoutLoading(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.log")
	event := Event{
		Event:  EventUpdateFailed,
		Detail: "reloading the service definition failed: bootstrap failed",
	}
	if err := AppendAudit(path, event); err != nil {
		t.Fatalf("AppendAudit: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	for _, want := range []string{
		`"event":"update_failed"`,
		`"detail":"reloading the service definition failed: bootstrap failed"`,
	} {
		if !strings.Contains(string(data), want) {
			t.Errorf("audit log = %q, want it to contain %q", data, want)
		}
	}
	if bytes.Count(data, []byte("\n")) != 1 {
		t.Errorf("audit log = %q, want exactly one record", data)
	}
}

// TestAppendAuditRejectsAnEmptyPath checks that AppendAudit refuses an empty
// path.
func TestAppendAuditRejectsAnEmptyPath(t *testing.T) {
	if err := AppendAudit("", Event{Event: EventUpdateFailed}); err == nil {
		t.Fatal("AppendAudit accepted an empty path")
	}
}

func newAuditTestLogAt(t *testing.T) (*auditLog, string, *auditTestClockControl, *auditTestTimers, *bytes.Buffer) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "data", "audit.log")
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	clock := &auditTestClockControl{now: auditTestClock(t)}
	timers := &auditTestTimers{}
	log, err := openAudit(path, clock.Now, logger)
	if err != nil {
		t.Fatalf("open audit log: %v", err)
	}
	log.afterFunc = timers.afterFunc
	t.Cleanup(func() { _ = log.close() })
	return log, path, clock, timers, &logs
}

// TestConfigAuditConnectionRejectedMergesBySourceBeforeFingerprint checks that
// rejections naming a source share one window even when each carries a
// different fingerprint.
func TestConfigAuditConnectionRejectedMergesBySourceBeforeFingerprint(t *testing.T) {
	log, path, clock, timers, _ := newAuditTestLogAt(t)
	base := auditTestClock(t)
	source := "source:203.0.113.7/32"
	first := &PeerRef{Fingerprint: testFingerprint(0x80)}
	second := &PeerRef{Fingerprint: testFingerprint(0x90)}
	for index, peer := range []*PeerRef{first, second} {
		if err := log.append(Event{
			Time:     base.Add(time.Duration(index) * time.Minute),
			Event:    EventConnectionRejected,
			Peer:     peer,
			MergeKey: source,
			Detail:   "not a peer from 203.0.113.7/32",
		}); err != nil {
			t.Fatalf("append connection_rejected: %v", err)
		}
	}
	clock.Advance(auditMergeWindow + 2*time.Minute)
	timers.fireAll()

	records := auditFileRecords(t, path)
	if len(records) != 1 {
		t.Fatalf("file has %d rejection records, want 1 merged record", len(records))
	}
	got := records[0]
	if got.Count != 2 {
		t.Errorf("merged count = %d, want 2", got.Count)
	}
	if got.Peer == nil || got.Peer.Fingerprint != first.Fingerprint {
		t.Errorf("merged peer = %+v, want the first event's fingerprint %x", got.Peer, first.Fingerprint)
	}
}

// TestConfigAuditAppendDoesNotScanEveryWindow checks that appending to an open
// window examines at most the heap root, not every open window.
func TestConfigAuditAppendDoesNotScanEveryWindow(t *testing.T) {
	log, _, _ := newAuditTestLog(t, 0o700)
	base := auditTestClock(t)
	const windows = 1000
	firstKey := "source:198.51.100.0/32"
	for index := 0; index < windows; index++ {
		key := fmt.Sprintf("source:198.51.100.%d/32", index)
		if err := log.append(Event{
			Time:     base,
			Event:    EventConnectionRejected,
			MergeKey: key,
		}); err != nil {
			t.Fatalf("append window %d: %v", index, err)
		}
	}

	log.mu.Lock()
	before := log.scannedWindows
	log.mu.Unlock()
	if err := log.append(Event{
		Time:     base.Add(time.Minute),
		Event:    EventConnectionRejected,
		MergeKey: firstKey,
	}); err != nil {
		t.Fatalf("append to an open window: %v", err)
	}
	log.mu.Lock()
	scanned := log.scannedWindows - before
	log.mu.Unlock()
	if scanned > 2 {
		t.Fatalf("appending to an open window examined %d pending windows, want at most 2", scanned)
	}
}

// TestConfigAuditExpiryOrdersEqualDeadlinesByKey checks that windows expiring
// at the same instant are written in merge-key order.
func TestConfigAuditExpiryOrdersEqualDeadlinesByKey(t *testing.T) {
	log, path, clock, timers, _ := newAuditTestLogAt(t)
	base := auditTestClock(t)
	later := "source:203.0.113.9/32"
	earlier := "source:203.0.113.1/32"
	for _, key := range []string{later, earlier} {
		if err := log.append(Event{
			Time:     base,
			Event:    EventConnectionRejected,
			MergeKey: key,
			Detail:   key,
		}); err != nil {
			t.Fatalf("append %s: %v", key, err)
		}
	}
	clock.Advance(auditMergeWindow)
	timers.fireAll()

	records := auditFileRecords(t, path)
	if len(records) != 2 {
		t.Fatalf("file has %d records, want 2", len(records))
	}
	if records[0].Detail != earlier || records[1].Detail != later {
		t.Fatalf("equal-deadline order = [%s %s], want [%s %s]",
			records[0].Detail, records[1].Detail, earlier, later)
	}
}

// TestConfigAuditPendingWindowCapAndSummary checks that after 256 ordinary
// windows further sources fold into one counted summary record.
func TestConfigAuditPendingWindowCapAndSummary(t *testing.T) {
	log, path, clock, timers, logs := newAuditTestLogAt(t)
	base := auditTestClock(t)
	const total = 300
	for index := 0; index < total; index++ {
		if err := log.append(Event{
			Time:     base,
			Event:    EventConnectionRejected,
			MergeKey: fmt.Sprintf("source:198.51.100.%d/32", index),
			Detail:   fmt.Sprintf("not a peer from 198.51.100.%d/32", index),
		}); err != nil {
			t.Fatalf("append window %d: %v", index, err)
		}
	}
	clock.Advance(auditMergeWindow)
	timers.fireAll()

	records := auditFileRecords(t, path)
	if len(records) != 257 {
		t.Fatalf("file has %d records, want 256 ordinary windows and one summary", len(records))
	}
	last := records[len(records)-1]
	if last.Event != EventConnectionRejected {
		t.Errorf("summary kind = %q, want connection_rejected", last.Event)
	}
	if last.Peer != nil {
		t.Errorf("summary peer = %+v, want no peer", last.Peer)
	}
	if last.Detail != "other sources" {
		t.Errorf("summary detail = %q, want %q", last.Detail, "other sources")
	}
	if last.Count != 44 {
		t.Errorf("summary count = %d, want 44", last.Count)
	}
	if got := strings.Count(logs.String(), "config_audit_merge_overflow"); got != 1 {
		t.Errorf("overflow logged %d times, want 1", got)
	}
}

// TestConfigAuditFullTableStillMergesExistingWindow checks that a full table
// still merges a hit on an existing window and opens no summary.
func TestConfigAuditFullTableStillMergesExistingWindow(t *testing.T) {
	log, path, clock, timers, _ := newAuditTestLogAt(t)
	base := auditTestClock(t)
	for index := 0; index < 256; index++ {
		if err := log.append(Event{
			Time:     base,
			Event:    EventConnectionRejected,
			MergeKey: fmt.Sprintf("source:203.0.113.%d/32", index),
		}); err != nil {
			t.Fatalf("append window %d: %v", index, err)
		}
	}
	if err := log.append(Event{
		Time:     base.Add(time.Minute),
		Event:    EventConnectionRejected,
		MergeKey: "source:203.0.113.5/32",
	}); err != nil {
		t.Fatalf("append to an existing window: %v", err)
	}
	clock.Advance(auditMergeWindow)
	timers.fireAll()

	records := auditFileRecords(t, path)
	if len(records) != 256 {
		t.Fatalf("file has %d records, want 256 ordinary windows and no summary", len(records))
	}
	merged := 0
	for _, record := range records {
		if record.Detail == "other sources" {
			t.Fatalf("a hit on an existing window opened a summary: %+v", record)
		}
		if record.Count == 2 {
			merged++
		}
	}
	if merged != 1 {
		t.Errorf("%d windows counted two events, want the one existing window that was hit", merged)
	}
}

// TestConfigAuditCapAppliesToPairingFailed checks that the ordinary-window cap
// is shared across mergeable kinds and keeps one summary per kind.
func TestConfigAuditCapAppliesToPairingFailed(t *testing.T) {
	log, _, _, _, _ := newAuditTestLogAt(t)
	base := auditTestClock(t)
	for index := 0; index < 256; index++ {
		if err := log.append(Event{
			Time:     base,
			Event:    EventConnectionRejected,
			MergeKey: fmt.Sprintf("source:203.0.113.%d/32", index),
		}); err != nil {
			t.Fatalf("append window %d: %v", index, err)
		}
	}
	for index := 0; index < 2; index++ {
		if err := log.append(Event{
			Time:     base,
			Event:    EventPairingFailed,
			MergeKey: fmt.Sprintf("source:198.51.100.%d/32", index),
			Detail:   "too many attempts",
		}); err != nil {
			t.Fatalf("append pairing_failed %d: %v", index, err)
		}
	}
	events, err := log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	summaries := 0
	for _, event := range events {
		if event.Event == EventPairingFailed && event.Detail == "other sources" {
			summaries++
			if event.Count != 2 {
				t.Errorf("pairing_failed summary count = %d, want 2", event.Count)
			}
		}
	}
	if summaries != 1 {
		t.Errorf("pairing_failed summaries = %d, want 1", summaries)
	}
}

// TestConfigAuditCloseFlushesSummary checks that Close writes an open summary
// window like any other window.
func TestConfigAuditCloseFlushesSummary(t *testing.T) {
	log, path, _, _, _ := newAuditTestLogAt(t)
	base := auditTestClock(t)
	for index := 0; index < 258; index++ {
		if err := log.append(Event{
			Time:     base,
			Event:    EventConnectionRejected,
			MergeKey: fmt.Sprintf("source:198.51.100.%d/32", index),
		}); err != nil {
			t.Fatalf("append window %d: %v", index, err)
		}
	}
	if err := log.close(); err != nil {
		t.Fatalf("close audit log: %v", err)
	}
	records := auditFileRecords(t, path)
	if len(records) != 257 {
		t.Fatalf("file has %d records after close, want 257", len(records))
	}
	last := records[len(records)-1]
	if last.Detail != "other sources" || last.Peer != nil || last.Count != 2 {
		t.Errorf("summary after close = %+v, want detail %q, no peer and count 2",
			last, "other sources")
	}
}

// TestOpenAuditUsesRealClockAndReportsUnknownKind checks that an event without
// a time takes the real clock and an unknown kind is refused and reported.
func TestOpenAuditUsesRealClockAndReportsUnknownKind(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	path := filepath.Join(t.TempDir(), "audit.log")
	log, err := openAuditWithDirMode(path, auditDirMode, time.Now, logger)
	if err != nil {
		t.Fatalf("open audit log: %v", err)
	}
	t.Cleanup(func() { _ = log.close() })

	if err := log.append(Event{Event: EventKind("bogus")}); err == nil {
		t.Fatal("append of an unknown kind returned no error")
	}
	if !strings.Contains(logs.String(), "config_audit_unknown_kind") {
		t.Errorf("the logger did not receive the unknown-kind report: %q", logs.String())
	}

	if err := log.append(Event{Event: EventLinkUp}); err != nil {
		t.Fatalf("append link_up: %v", err)
	}
	events, err := log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if len(events) != 1 || events[0].Time.IsZero() {
		t.Fatalf("events = %+v, want one record carrying the clock's time", events)
	}
	if age := time.Since(events[0].Time); age < 0 || age > time.Minute {
		t.Errorf("the clock produced %s, which is %s from now", events[0].Time, age)
	}
}

// TestOpenAuditReportsFilesystemFailures checks that an uncreatable parent
// directory and a refused filename both make opening fail with the matching
// error.
func TestOpenAuditReportsFilesystemFailures(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, nil, 0o600); err != nil {
		t.Fatalf("seed blocker file: %v", err)
	}
	if _, err := openAudit(filepath.Join(blocker, "audit.log"), time.Now, testLogger()); err == nil {
		t.Fatal("opening an audit log under a regular file succeeded")
	} else if !strings.Contains(err.Error(), "cannot create the audit log directory") {
		t.Errorf("parent-directory error = %v", err)
	}

	tooLong := filepath.Join(dir, strings.Repeat("a", 300))
	if _, err := openAudit(tooLong, time.Now, testLogger()); err == nil {
		t.Fatal("opening an audit log with an over-long name succeeded")
	} else if !strings.Contains(err.Error(), "cannot open the audit log") {
		t.Errorf("open error = %v", err)
	}
}

// TestAuditAppendStripsSecretKindValues checks that a secret-kind value is
// removed from an event that did not go through the key table.
func TestAuditAppendStripsSecretKindValues(t *testing.T) {
	log, path, _ := newAuditTestLog(t, 0o700)
	if err := log.append(Event{
		Time:  auditTestClock(t),
		Event: EventLinkUp,
		Old:   &Value{Kind: KindSecret, SecretIsSet: true},
		New:   &Value{Kind: KindSecret, SecretIsSet: true},
	}); err != nil {
		t.Fatalf("append link_up: %v", err)
	}
	events, err := log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if events[0].Old != nil || events[0].New != nil {
		t.Fatalf("record carries a secret-kind value: old %#v new %#v", events[0].Old, events[0].New)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if bytes.Contains(data, []byte(`"secret"`)) {
		t.Errorf("secret kind reached the audit file: %s", data)
	}
}

// TestAuditAppendWriteFailureIsLogged checks that a write failure is returned
// and logged and a later read reports its seek failure.
func TestAuditAppendWriteFailureIsLogged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.log")
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	log, err := openAudit(path, func() time.Time { return auditTestClock(t) }, logger)
	if err != nil {
		t.Fatalf("open audit log: %v", err)
	}
	t.Cleanup(func() { _ = log.close() })
	if err := log.file.Close(); err != nil {
		t.Fatalf("close the underlying file: %v", err)
	}

	if err := log.append(Event{Time: auditTestClock(t), Event: EventLinkUp}); err == nil {
		t.Fatal("append to a closed file returned no error")
	}
	if !strings.Contains(logs.String(), "cannot append audit record") {
		t.Errorf("write failure was not logged: %q", logs.String())
	}
	if _, err := log.read(time.Time{}); err == nil {
		t.Fatal("read from a closed file returned no error")
	} else if !strings.Contains(err.Error(), "cannot seek in the audit log") {
		t.Errorf("read error = %v", err)
	}
}

// TestAuditReadSkipsBlankAndUnterminatedLines checks that a blank line adds no
// record and an unterminated tail is skipped and logged once.
func TestAuditReadSkipsBlankAndUnterminatedLines(t *testing.T) {
	log, path, logs := newAuditTestLog(t, 0o700)
	if err := log.append(Event{Time: auditTestClock(t), Event: EventLinkUp}); err != nil {
		t.Fatalf("append link_up: %v", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open for raw append: %v", err)
	}
	if _, err := file.WriteString("\n" + `{"time":"2026-11-18T12:00:01`); err != nil {
		t.Fatalf("write raw tail: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close raw append: %v", err)
	}

	events, err := log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if len(events) != 1 || events[0].Event != EventLinkUp {
		t.Fatalf("got %+v, want only the written link_up record", events)
	}
	if got := strings.Count(logs.String(), "skipping unreadable audit record"); got != 1 {
		t.Errorf("unreadable-line reports = %d, want 1 (the blank line must not be reported): %q",
			got, logs.String())
	}
}

// TestAuditReadSinceFiltersPendingWindow checks that a pending window whose
// first event predates since is omitted.
func TestAuditReadSinceFiltersPendingWindow(t *testing.T) {
	log, _, clock, _, _ := newAuditTestLogAt(t)
	base := clock.Now()
	if err := log.append(Event{
		Time:     base,
		Event:    EventConnectionRejected,
		MergeKey: "source:203.0.113.20/32",
	}); err != nil {
		t.Fatalf("append connection_rejected: %v", err)
	}

	all, err := log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("got %d events without a filter, want the pending window", len(all))
	}
	filtered, err := log.read(base.Add(time.Minute))
	if err != nil {
		t.Fatalf("read audit log with since: %v", err)
	}
	if len(filtered) != 0 {
		t.Fatalf("got %d events after since, want the pending window omitted: %+v", len(filtered), filtered)
	}
}

// TestDecodeEventRejectsMalformedRecords checks that a missing time, unknown
// kind, non-hex fingerprint and wrong-length fingerprint are all refused.
func TestDecodeEventRejectsMalformedRecords(t *testing.T) {
	cases := []struct {
		name  string
		line  string
		want  string
		check func(*testing.T, error)
	}{
		{"no time", `{"event":"link_up"}`, "no time", nil},
		{"unknown kind", fmt.Sprintf(`{"time":%q,"event":"bogus"}`, auditTestClockText), "unknown audit event kind", nil},
		{"bad fingerprint hex", fmt.Sprintf(`{"time":%q,"event":"link_up","peer":{"fingerprint":"zz"}}`, auditTestClockText), "invalid byte", func(t *testing.T, err error) {
			var invalid hex.InvalidByteError
			if !errors.As(err, &invalid) {
				t.Fatalf("error = %v, want hex.InvalidByteError", err)
			}
			if strings.Contains(err.Error(), "bytes") {
				t.Fatalf("error = %v, want the hex failure, not the fingerprint length", err)
			}
		}},
		{"short fingerprint", fmt.Sprintf(`{"time":%q,"event":"link_up","peer":{"fingerprint":"00"}}`, auditTestClockText), "1 bytes", nil},
	}
	for _, tc := range cases {
		_, err := decodeEvent([]byte(tc.line))
		if err == nil {
			t.Errorf("%s: decodeEvent accepted the record", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error = %v, want it to mention %q", tc.name, err, tc.want)
		}
		if tc.check != nil {
			tc.check(t, err)
		}
	}
}

// TestDecodeEventRejectsMalformedValues checks that a malformed old or new
// value makes the record unreadable.
func TestDecodeEventRejectsMalformedValues(t *testing.T) {
	for field, value := range map[string]string{
		"old": `"old":{"kind":"string","value":5}`,
		"new": `"new":{"kind":"string","value":5}`,
	} {
		line := fmt.Sprintf(`{"time":%q,"event":"link_up",%s}`, auditTestClockText, value)
		_, err := decodeEvent([]byte(line))
		if err == nil {
			t.Errorf("%s: decodeEvent accepted a malformed value", field)
			continue
		}
		var typeErr *json.UnmarshalTypeError
		if !errors.As(err, &typeErr) {
			t.Errorf("%s: error = %v, want a *json.UnmarshalTypeError", field, err)
		}
	}
}

// TestDecodeValueRejectsMalformedPayloads checks that each value kind reports
// its own malformed payload.
func TestDecodeValueRejectsMalformedPayloads(t *testing.T) {
	typeMismatch := func(err error) bool {
		var typeErr *json.UnmarshalTypeError
		return errors.As(err, &typeErr)
	}
	cases := []struct {
		name  string
		wire  wireValue
		check func(error) bool
		want  string
	}{
		{"string", wireValue{Kind: "string", Value: json.RawMessage(`5`)}, typeMismatch, ""},
		{"bool", wireValue{Kind: "bool", Value: json.RawMessage(`"x"`)}, typeMismatch, ""},
		{"int", wireValue{Kind: "int", Value: json.RawMessage(`"x"`)}, typeMismatch, ""},
		{"duration payload", wireValue{Kind: "duration", Value: json.RawMessage(`5`)}, typeMismatch, ""},
		{"duration text", wireValue{Kind: "duration", Value: json.RawMessage(`"not-a-duration"`)}, nil, "invalid duration"},
		{"string list", wireValue{Kind: "string_list", Value: json.RawMessage(`5`)}, typeMismatch, ""},
		{"secret", wireValue{Kind: "secret", Value: json.RawMessage(`true`)}, nil, "audit record contains a secret value"},
		{"unknown kind", wireValue{Kind: "bogus", Value: json.RawMessage(`null`)}, nil, `unknown audit value kind "bogus"`},
	}
	for _, tc := range cases {
		_, err := decodeValue(tc.wire)
		if err == nil {
			t.Errorf("%s: decodeValue accepted the payload", tc.name)
			continue
		}
		if tc.check != nil && !tc.check(err) {
			t.Errorf("%s: error = %v, want a *json.UnmarshalTypeError", tc.name, err)
		}
		if tc.want != "" && !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error = %v, want it to mention %q", tc.name, err, tc.want)
		}
	}
}

// TestAuditRescheduleClampsNegativeDelay checks that a window already past its
// deadline is armed with a zero delay.
func TestAuditRescheduleClampsNegativeDelay(t *testing.T) {
	log, _, clock, timers, _ := newAuditTestLogAt(t)
	base := clock.Now()
	if err := log.append(Event{
		Time:     base.Add(-20 * time.Minute),
		Event:    EventConnectionRejected,
		MergeKey: "source:203.0.113.30/32",
	}); err != nil {
		t.Fatalf("append connection_rejected: %v", err)
	}

	timers.mu.Lock()
	armed := append([]time.Duration{}, timers.durations...)
	timers.mu.Unlock()
	if len(armed) != 1 || armed[0] != 0 {
		t.Fatalf("armed delays = %v, want one timer with delay 0", armed)
	}
	log.mu.Lock()
	due := log.timerDue
	log.mu.Unlock()
	if want := base.Add(-10 * time.Minute); !due.Equal(want) {
		t.Errorf("timer due = %s, want the deadline %s", due, want)
	}
}

// TestAuditSummaryWindowClosesOnExpiredEvent checks that an overflow event
// after the summary expired writes the old summary and opens a new one.
func TestAuditSummaryWindowClosesOnExpiredEvent(t *testing.T) {
	log, path, clock, _, _ := newAuditTestLogAt(t)
	base := clock.Now()
	for index := 0; index < auditMaxPendingWindows; index++ {
		if err := log.append(Event{
			Time:     base,
			Event:    EventConnectionRejected,
			MergeKey: fmt.Sprintf("source:198.51.100.%d/32", index),
		}); err != nil {
			t.Fatalf("append window %d: %v", index, err)
		}
	}
	for _, offset := range []time.Duration{0, 2 * auditMergeWindow} {
		if err := log.append(Event{
			Time:     base.Add(offset),
			Event:    EventConnectionRejected,
			MergeKey: fmt.Sprintf("source:192.0.2.%d/32", 200+int(offset/time.Minute)),
		}); err != nil {
			t.Fatalf("append overflow event at %s: %v", offset, err)
		}
	}

	records := auditFileRecords(t, path)
	if len(records) != 1 {
		t.Fatalf("file has %d records, want the expired summary only", len(records))
	}
	if records[0].Detail != auditOverflowDetail || records[0].Count != 0 {
		t.Errorf("expired summary = %+v, want detail %q and no count", records[0], auditOverflowDetail)
	}
	if !records[0].Time.Equal(base) {
		t.Errorf("expired summary time = %s, want %s", records[0].Time, base)
	}

	events, err := log.read(time.Time{})
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	var summaries []Event
	for _, event := range events {
		if event.Detail == auditOverflowDetail {
			summaries = append(summaries, event)
		}
	}
	if len(summaries) != 2 {
		t.Fatalf("got %d summary records, want the written one and the new one: %+v", len(summaries), summaries)
	}
	if !summaries[0].Time.Equal(base) || !summaries[1].Time.Equal(base.Add(2*auditMergeWindow)) {
		t.Errorf("summary times = [%s %s], want [%s %s]",
			summaries[0].Time, summaries[1].Time, base, base.Add(2*auditMergeWindow))
	}
}

// TestCoreAuditAppendWithoutOpenLogReportsToCoreLogger checks that an event
// reported with no open audit log reaches the core's run log.
func TestCoreAuditAppendWithoutOpenLogReportsToCoreLogger(t *testing.T) {
	var logs bytes.Buffer
	previous := defaultLogger
	defaultLogger = func() *slog.Logger {
		return slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	t.Cleanup(func() { defaultLogger = previous })

	c := newCore()
	c.auditAppend(Event{Event: EventLinkUp})
	if !strings.Contains(logs.String(), "config_audit_not_open") {
		t.Fatalf("the core logger did not receive the report: %q", logs.String())
	}
}

// TestOpenAuditReportsRepairFailures checks that a stat failure and an
// unreadable tail both make opening fail with the repair error.
func TestOpenAuditReportsRepairFailures(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.log")
	previous := auditCreateFile
	t.Cleanup(func() { auditCreateFile = previous })

	closedFile, err := os.OpenFile(filepath.Join(dir, "closed.tmp"), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatalf("create scratch file: %v", err)
	}
	if err := closedFile.Close(); err != nil {
		t.Fatalf("close scratch file: %v", err)
	}
	auditCreateFile = func(string) (*os.File, error) { return closedFile, nil }
	if _, err := openAudit(path, time.Now, testLogger()); err == nil {
		t.Fatal("opening an audit log whose file cannot be statted succeeded")
	} else if !strings.Contains(err.Error(), "cannot repair the audit log") {
		t.Errorf("stat failure error = %v", err)
	}

	seed := filepath.Join(dir, "writeonly.tmp")
	if err := os.WriteFile(seed, []byte("x\n"), 0o600); err != nil {
		t.Fatalf("seed scratch file: %v", err)
	}
	writeOnly, err := os.OpenFile(seed, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open scratch file write-only: %v", err)
	}
	t.Cleanup(func() { _ = writeOnly.Close() })
	auditCreateFile = func(string) (*os.File, error) { return writeOnly, nil }
	if _, err := openAudit(path, time.Now, testLogger()); err == nil {
		t.Fatal("opening an audit log whose tail cannot be read succeeded")
	} else if !strings.Contains(err.Error(), "cannot repair the audit log") {
		t.Errorf("read failure error = %v", err)
	}
}

// TestCreateOrOpenAuditFileReportsChmodFailure checks that a chmod failure is
// returned and stays distinguishable through the open error.
func TestCreateOrOpenAuditFileReportsChmodFailure(t *testing.T) {
	sentinel := errors.New("chmod denied")
	previous := auditChmodFile
	auditChmodFile = func(*os.File, os.FileMode) error { return sentinel }
	t.Cleanup(func() { auditChmodFile = previous })

	path := filepath.Join(t.TempDir(), "audit.log")
	if _, err := openAudit(path, time.Now, testLogger()); !errors.Is(err, sentinel) {
		t.Fatalf("openAudit error = %v, want it to wrap the chmod failure", err)
	}
}

// TestAuditReadReportsReadFailure checks that a file that can be seeked but not
// read reports the read error.
func TestAuditReadReportsReadFailure(t *testing.T) {
	dir := t.TempDir()
	seed := filepath.Join(dir, "writeonly.tmp")
	if err := os.WriteFile(seed, nil, 0o600); err != nil {
		t.Fatalf("seed scratch file: %v", err)
	}
	writeOnly, err := os.OpenFile(seed, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open scratch file write-only: %v", err)
	}
	t.Cleanup(func() { _ = writeOnly.Close() })

	previous := auditCreateFile
	auditCreateFile = func(string) (*os.File, error) { return writeOnly, nil }
	t.Cleanup(func() { auditCreateFile = previous })

	log, err := openAudit(filepath.Join(dir, "audit.log"), time.Now, testLogger())
	if err != nil {
		t.Fatalf("open audit log over an empty write-only file: %v", err)
	}
	t.Cleanup(func() { _ = log.close() })
	if _, err := log.read(time.Time{}); err == nil {
		t.Fatal("read from a write-only file returned no error")
	} else if !strings.Contains(err.Error(), "cannot read the audit log") {
		t.Errorf("read error = %v", err)
	}
}
