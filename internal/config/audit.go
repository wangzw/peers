package config

import (
	"bytes"
	"container/heap"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const (
	auditFileMode os.FileMode = 0o600
	// auditDirMode is only the fallback for the audit log's parent directory;
	// Load creates that directory with the mode the deployment needs, so this
	// value matters only when the directory disappears in between.
	auditDirMode os.FileMode = 0o700

	auditMergeWindow       = 10 * time.Minute
	auditMaxPendingWindows = 256
	auditOverflowDetail    = "other sources"
	auditTimeLayout        = "2006-01-02T15:04:05.000000000Z"
)

var errAuditClosed = errors.New("the audit log is closed")

type auditTimer interface {
	Stop()
}

type realAuditTimer struct{ timer *time.Timer }

func (t realAuditTimer) Stop() { t.timer.Stop() }

func newRealAuditTimer(d time.Duration, f func()) auditTimer {
	return realAuditTimer{timer: time.AfterFunc(d, f)}
}

type pendingAudit struct {
	key         string
	event       Event
	started     time.Time
	occurrences uint64
	summary     bool
	index       int
}

func (p *pendingAudit) deadline() time.Time {
	return p.started.Add(auditMergeWindow)
}

func (p *pendingAudit) withinMergeWindow(t time.Time) bool {
	return !t.Before(p.started) && t.Sub(p.started) <= auditMergeWindow
}

type auditExpiryHeap []*pendingAudit

func (h auditExpiryHeap) Len() int { return len(h) }

func (h auditExpiryHeap) Less(i, j int) bool {
	di, dj := h[i].deadline(), h[j].deadline()
	if !di.Equal(dj) {
		return di.Before(dj)
	}
	if h[i].summary != h[j].summary {
		return !h[i].summary
	}
	return h[i].key < h[j].key
}

func (h auditExpiryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *auditExpiryHeap) Push(x any) {
	window := x.(*pendingAudit)
	window.index = len(*h)
	*h = append(*h, window)
}

func (h *auditExpiryHeap) Pop() any {
	old := *h
	n := len(old)
	window := old[n-1]
	old[n-1] = nil
	window.index = -1
	*h = old[:n-1]
	return window
}

// auditLog serializes access to the audit file with its own mutex, independent
// of core.mu because Audit may run while a configuration change holds core.mu.
type auditLog struct {
	mu     sync.Mutex
	file   *os.File
	path   string
	now    func() time.Time
	logger *slog.Logger
	closed bool

	// afterFunc schedules the window-expiry callback; tests replace it to fire
	// the callback on command.
	afterFunc func(time.Duration, func()) auditTimer

	pending  map[string]*pendingAudit
	summary  map[EventKind]*pendingAudit
	expiry   auditExpiryHeap
	timer    auditTimer
	timerDue time.Time
	// timerGen identifies the current timer, so a callback from a stopped or
	// replaced timer does nothing.
	timerGen uint64

	// scannedWindows counts the windows the expiry code examines; it is guarded
	// by mu and lets a test prove an append does not scan every open window.
	scannedWindows uint64
}

func openAudit(path string, now func() time.Time, logger *slog.Logger) (*auditLog, error) {
	return openAuditWithDirMode(path, auditDirMode, now, logger)
}

func openAuditWithDirMode(path string, dirMode os.FileMode, now func() time.Time, logger *slog.Logger) (*auditLog, error) {
	if path == "" {
		return nil, errors.New("the audit log path is empty")
	}
	if now == nil {
		now = time.Now
	}
	if logger == nil {
		logger = slog.Default()
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, dirMode); err != nil {
			return nil, fmt.Errorf("cannot create the audit log directory %s: %w", dir, err)
		}
	}
	file, err := auditCreateFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot open the audit log %s: %w", path, err)
	}
	if err := terminatePartialLine(file); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("cannot repair the audit log %s: %w", path, err)
	}
	return &auditLog{
		file:      file,
		path:      path,
		now:       now,
		logger:    logger,
		afterFunc: newRealAuditTimer,
		pending:   make(map[string]*pendingAudit),
		summary:   make(map[EventKind]*pendingAudit),
	}, nil
}

var auditCreateFile = createOrOpenAuditFile

var auditChmodFile = func(file *os.File, mode os.FileMode) error { return file.Chmod(mode) }

// The explicit chmod corrects a permissive umask; an existing file keeps its
// own mode, owner and group.
func createOrOpenAuditFile(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL|os.O_APPEND, auditFileMode)
	if err == nil {
		if chmodErr := auditChmodFile(file, auditFileMode); chmodErr != nil {
			_ = file.Close()
			return nil, chmodErr
		}
		return file, nil
	}
	if !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	return os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0)
}

func terminatePartialLine(file *os.File) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		return nil
	}
	last := make([]byte, 1)
	if _, err := file.ReadAt(last, info.Size()-1); err != nil {
		return err
	}
	if last[0] == '\n' {
		return nil
	}
	_, err = file.Write([]byte{'\n'})
	return err
}

func (a *auditLog) append(e Event) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return errAuditClosed
	}
	if !validEventKind(e.Event) {
		a.logger.Error("config_audit_unknown_kind", "audit_event", string(e.Event))
		return fmt.Errorf("unknown audit event kind %q", e.Event)
	}
	if e.Time.IsZero() {
		e.Time = a.now()
	}
	e.Count = 0
	if e.Event == EventConfigSet || e.Event == EventConfigFileChanged {
		if spec, ok := lookupKey(e.Key); ok && spec.Secret {
			e.Old = nil
			e.New = nil
		}
	}
	if e.Old != nil && e.Old.Kind == KindSecret {
		e.Old = nil
	}
	if e.New != nil && e.New.Kind == KindSecret {
		e.New = nil
	}
	if key, mergeable := auditMergeKey(e); mergeable {
		a.mergeLocked(key, e)
		return nil
	}
	return a.writeEventLocked(e)
}

func (a *auditLog) mergeLocked(key string, e Event) {
	a.flushExpiredLocked(a.now())
	if pending, ok := a.pending[key]; ok {
		if pending.withinMergeWindow(e.Time) {
			pending.occurrences++
			return
		}
		a.closeWindowLocked(pending)
	}
	if len(a.pending) < auditMaxPendingWindows {
		a.openWindowLocked(&pendingAudit{key: key, event: e, started: e.Time, occurrences: 1})
		a.rescheduleLocked()
		return
	}
	a.mergeSummaryLocked(e.Event, e)
}

func (a *auditLog) mergeSummaryLocked(kind EventKind, e Event) {
	if pending, ok := a.summary[kind]; ok {
		if pending.withinMergeWindow(e.Time) {
			pending.occurrences++
			return
		}
		a.closeWindowLocked(pending)
	}
	event := e
	event.Peer = nil
	event.Detail = auditOverflowDetail
	event.MergeKey = ""
	a.openWindowLocked(&pendingAudit{
		key:         string(kind),
		event:       event,
		started:     e.Time,
		occurrences: 1,
		summary:     true,
	})
	a.logger.Warn("config_audit_merge_overflow",
		"audit_event", string(kind), "limit", auditMaxPendingWindows)
	a.rescheduleLocked()
}

func (a *auditLog) openWindowLocked(window *pendingAudit) {
	if window.summary {
		a.summary[EventKind(window.key)] = window
	} else {
		a.pending[window.key] = window
	}
	heap.Push(&a.expiry, window)
}

func (a *auditLog) closeWindowLocked(window *pendingAudit) {
	a.forgetWindowLocked(window)
	a.writePendingLocked(window)
}

func (a *auditLog) forgetWindowLocked(window *pendingAudit) {
	a.deleteWindowKeyLocked(window)
	if window.index >= 0 {
		heap.Remove(&a.expiry, window.index)
	}
}

// deleteWindowKeyLocked removes a window from its table without touching the
// heap, for callers that already popped its entry.
func (a *auditLog) deleteWindowKeyLocked(window *pendingAudit) {
	if window.summary {
		delete(a.summary, EventKind(window.key))
	} else {
		delete(a.pending, window.key)
	}
}

func (a *auditLog) flushExpiredLocked(now time.Time) {
	for len(a.expiry) > 0 {
		window := a.expiry[0]
		a.scannedWindows++
		if now.Before(window.deadline()) {
			break
		}
		heap.Pop(&a.expiry)
		a.deleteWindowKeyLocked(window)
		a.writePendingLocked(window)
	}
	a.rescheduleLocked()
}

func (a *auditLog) writePendingLocked(pending *pendingAudit) {
	event := pending.event
	event.MergeKey = ""
	if pending.occurrences > 1 {
		event.Count = pending.occurrences
	}
	_ = a.writeEventLocked(event)
}

func (a *auditLog) writeEventLocked(e Event) error {
	e.MergeKey = ""
	line, err := encodeEvent(e)
	if err != nil {
		a.logger.Error("config_audit_encode_failed", "audit_event", string(e.Event), "err", err)
		return err
	}
	if _, err := a.file.Write(line); err != nil {
		a.logger.Error("config: cannot append audit record", "path", a.path, "err", err)
		return err
	}
	return nil
}

func (a *auditLog) rescheduleLocked() {
	if a.closed || len(a.expiry) == 0 {
		a.stopTimerLocked()
		return
	}
	// The heap root is the only window this examines; count it so the scan test
	// sees a fallback to a full-table scan.
	a.scannedWindows++
	earliest := a.expiry[0].deadline()
	if a.timer != nil && a.timerDue.Equal(earliest) {
		return
	}
	a.stopTimerLocked()
	delay := earliest.Sub(a.now())
	if delay < 0 {
		delay = 0
	}
	gen := a.timerGen
	a.timer = a.afterFunc(delay, func() { a.expire(gen) })
	a.timerDue = earliest
}

func (a *auditLog) stopTimerLocked() {
	if a.timer != nil {
		a.timer.Stop()
	}
	a.timer = nil
	a.timerDue = time.Time{}
	a.timerGen++
}

// expire clears the fired timer before flushing: an early firing can find no
// window due, and rescheduleLocked must then see no timer and arm one for the
// remaining time. A callback from a stopped or replaced timer carries a stale
// generation and returns.
func (a *auditLog) expire(gen uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || gen != a.timerGen {
		return
	}
	a.timer = nil
	a.timerDue = time.Time{}
	a.flushExpiredLocked(a.now())
}

func (a *auditLog) orderedPendingLocked() []*pendingAudit {
	keys := make([]string, 0, len(a.pending))
	for key := range a.pending {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	windows := make([]*pendingAudit, 0, len(a.pending)+len(a.summary))
	for _, key := range keys {
		windows = append(windows, a.pending[key])
	}
	kinds := make([]string, 0, len(a.summary))
	for kind := range a.summary {
		kinds = append(kinds, string(kind))
	}
	sort.Strings(kinds)
	for _, kind := range kinds {
		windows = append(windows, a.summary[EventKind(kind)])
	}
	return windows
}

// Filtering compares a merged window's first event time, so a window that
// opened before since is omitted even if a later event in it falls after since.
func (a *auditLog) read(since time.Time) ([]Event, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil, errAuditClosed
	}
	if _, err := a.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("cannot seek in the audit log %s: %w", a.path, err)
	}
	data, err := io.ReadAll(a.file)
	if err != nil {
		return nil, fmt.Errorf("cannot read the audit log %s: %w", a.path, err)
	}
	events := make([]Event, 0, bytes.Count(data, []byte{'\n'}))
	for len(data) > 0 {
		var line []byte
		if i := bytes.IndexByte(data, '\n'); i >= 0 {
			line, data = data[:i], data[i+1:]
		} else {
			line, data = data, nil
		}
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		event, err := decodeEvent(line)
		if err != nil {
			a.logger.Error("config: skipping unreadable audit record", "path", a.path, "err", err)
			continue
		}
		if !since.IsZero() && event.Time.Before(since) {
			continue
		}
		events = append(events, event)
	}
	for _, pending := range a.orderedPendingLocked() {
		event := pending.event
		if pending.occurrences > 1 {
			event.Count = pending.occurrences
		}
		if !since.IsZero() && event.Time.Before(since) {
			continue
		}
		events = append(events, event)
	}
	sort.SliceStable(events, func(i, j int) bool {
		return events[i].Time.Before(events[j].Time)
	})
	// MergeKey is an in-memory grouping aid, never part of a returned record.
	for index := range events {
		events[index].MergeKey = ""
	}
	return events, nil
}

func (a *auditLog) close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	a.closed = true
	a.stopTimerLocked()
	for len(a.expiry) > 0 {
		window := heap.Pop(&a.expiry).(*pendingAudit)
		a.deleteWindowKeyLocked(window)
		a.writePendingLocked(window)
	}
	return a.file.Close()
}

func auditMergeKey(e Event) (string, bool) {
	switch e.Event {
	case EventConnectionRejected:
		// Prefer the caller's source over the fingerprint: a stranger can change
		// its certificate on every connection and defeat the merge.
		if e.MergeKey != "" {
			return "connection_rejected:" + e.MergeKey, true
		}
		if e.Peer != nil {
			var zero [32]byte
			if e.Peer.Fingerprint != zero {
				return "connection_rejected:" + hex.EncodeToString(e.Peer.Fingerprint[:]), true
			}
		}
		return "", false
	case EventPairingFailed:
		if e.MergeKey == "" {
			return "", false
		}
		return "pairing_failed:" + e.MergeKey + ":" + e.Detail, true
	default:
		return "", false
	}
}

func validEventKind(kind EventKind) bool {
	return allEventKinds[kind]
}

type wireEvent struct {
	Time    string     `json:"time"`
	Event   EventKind  `json:"event"`
	Actor   string     `json:"actor,omitempty"`
	Account string     `json:"account,omitempty"`
	Peer    *wirePeer  `json:"peer,omitempty"`
	Key     string     `json:"key,omitempty"`
	Old     *wireValue `json:"old,omitempty"`
	New     *wireValue `json:"new,omitempty"`
	Count   uint64     `json:"count,omitempty"`
	Detail  string     `json:"detail,omitempty"`
}

type wirePeer struct {
	Fingerprint string `json:"fingerprint"`
	Alias       string `json:"alias,omitempty"`
}

type wireValue struct {
	Kind  string          `json:"kind"`
	Value json.RawMessage `json:"value,omitempty"`
}

func encodeEvent(e Event) ([]byte, error) {
	wire := wireEvent{
		Time:    e.Time.UTC().Format(auditTimeLayout),
		Event:   e.Event,
		Actor:   e.Actor,
		Account: e.Account,
		Key:     e.Key,
		Count:   e.Count,
		Detail:  e.Detail,
	}
	if e.Peer != nil {
		wire.Peer = &wirePeer{
			Fingerprint: hex.EncodeToString(e.Peer.Fingerprint[:]),
			Alias:       e.Peer.Alias,
		}
	}
	if e.Old != nil {
		value, err := encodeValue(*e.Old)
		if err != nil {
			return nil, err
		}
		wire.Old = value
	}
	if e.New != nil {
		value, err := encodeValue(*e.New)
		if err != nil {
			return nil, err
		}
		wire.New = value
	}
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(wire); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func encodeValue(v Value) (*wireValue, error) {
	switch v.Kind {
	case KindString:
		return marshalWireValue("string", v.String)
	case KindBool:
		return marshalWireValue("bool", v.Bool)
	case KindInt:
		return marshalWireValue("int", v.Int)
	case KindDuration:
		return marshalWireValue("duration", v.Duration.String())
	case KindStringList:
		values := v.Strings
		if values == nil {
			values = []string{}
		}
		return marshalWireValue("string_list", values)
	case KindSecret:
		return nil, errors.New("a secret value cannot be written to the audit log")
	default:
		return nil, fmt.Errorf("unknown value kind %d", v.Kind)
	}
}

func marshalWireValue(kind string, plain any) (*wireValue, error) {
	data, err := json.Marshal(plain)
	if err != nil {
		return nil, err
	}
	return &wireValue{Kind: kind, Value: data}, nil
}

func decodeEvent(line []byte) (Event, error) {
	var wire wireEvent
	if err := json.Unmarshal(line, &wire); err != nil {
		return Event{}, err
	}
	if wire.Time == "" {
		return Event{}, errors.New("audit record has no time")
	}
	when, err := time.Parse(time.RFC3339Nano, wire.Time)
	if err != nil {
		return Event{}, err
	}
	if !validEventKind(wire.Event) {
		return Event{}, fmt.Errorf("unknown audit event kind %q", wire.Event)
	}
	event := Event{
		Time:    when,
		Event:   wire.Event,
		Actor:   wire.Actor,
		Account: wire.Account,
		Key:     wire.Key,
		Count:   wire.Count,
		Detail:  wire.Detail,
	}
	if wire.Peer != nil {
		fingerprint, err := hex.DecodeString(wire.Peer.Fingerprint)
		if err != nil {
			return Event{}, err
		}
		if len(fingerprint) != len(PeerRef{}.Fingerprint) {
			return Event{}, fmt.Errorf("audit peer fingerprint has %d bytes", len(fingerprint))
		}
		peer := PeerRef{Alias: wire.Peer.Alias}
		copy(peer.Fingerprint[:], fingerprint)
		event.Peer = &peer
	}
	if wire.Old != nil {
		value, err := decodeValue(*wire.Old)
		if err != nil {
			return Event{}, err
		}
		event.Old = &value
	}
	if wire.New != nil {
		value, err := decodeValue(*wire.New)
		if err != nil {
			return Event{}, err
		}
		event.New = &value
	}
	return event, nil
}

func decodeValue(wire wireValue) (Value, error) {
	switch wire.Kind {
	case "string":
		var value string
		if err := json.Unmarshal(wire.Value, &value); err != nil {
			return Value{}, err
		}
		return Value{Kind: KindString, String: value}, nil
	case "bool":
		var value bool
		if err := json.Unmarshal(wire.Value, &value); err != nil {
			return Value{}, err
		}
		return Value{Kind: KindBool, Bool: value}, nil
	case "int":
		var value int64
		if err := json.Unmarshal(wire.Value, &value); err != nil {
			return Value{}, err
		}
		return Value{Kind: KindInt, Int: value}, nil
	case "duration":
		var text string
		if err := json.Unmarshal(wire.Value, &text); err != nil {
			return Value{}, err
		}
		duration, err := time.ParseDuration(text)
		if err != nil {
			return Value{}, err
		}
		return Value{Kind: KindDuration, Duration: duration}, nil
	case "string_list":
		var values []string
		if err := json.Unmarshal(wire.Value, &values); err != nil {
			return Value{}, err
		}
		return Value{Kind: KindStringList, Strings: values}, nil
	case "secret":
		return Value{}, errors.New("audit record contains a secret value")
	default:
		return Value{}, fmt.Errorf("unknown audit value kind %q", wire.Kind)
	}
}

// auditAppend may run while a configuration change holds core.mu, so it reads
// the audit pointer under auditMu; Close clears that pointer under the same
// lock.
func (c *core) auditAppend(e Event) {
	c.auditMu.RLock()
	log := c.audit
	c.auditMu.RUnlock()
	if log == nil {
		c.loggerOr().Error("config_audit_not_open", "audit_event", string(e.Event))
		return
	}
	if err := log.append(e); errors.Is(err, errAuditClosed) {
		c.loggerOr().Error("config_audit_closed", "audit_event", string(e.Event))
	}
}

func (c *core) loggerOr() *slog.Logger {
	if c.logger != nil {
		return c.logger
	}
	return defaultLogger()
}

func (c *core) readAudit(since time.Time) ([]Event, error) {
	c.auditMu.RLock()
	log := c.audit
	c.auditMu.RUnlock()
	if log == nil {
		return nil, ErrNotLoaded
	}
	return log.read(since)
}
