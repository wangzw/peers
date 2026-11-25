package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

func decodeLogLines(t *testing.T, raw string) []map[string]any {
	t.Helper()
	raw = strings.TrimSuffix(raw, "\n")
	if raw == "" {
		return nil
	}
	lines := strings.Split(raw, "\n")
	records := make([]map[string]any, len(lines))
	for i, line := range lines {
		if err := json.Unmarshal([]byte(line), &records[i]); err != nil {
			t.Fatalf("line %d is not a JSON object: %v\nline: %s", i, err, line)
		}
	}
	return records
}

// TestRunLogLinesAreJSON checks that each run log record is one JSON object
// with a UTC time, a level and an event, including a nested attribute group.
func TestRunLogLinesAreJSON(t *testing.T) {
	var buf bytes.Buffer
	var level slog.LevelVar
	logger := NewLogger(&buf, &level)

	logger.Info(EvBrokerStarted, "pid", 42)
	logger.Warn(EvConnectionRejected, "uid", 1000)
	logger.Error(EvStartupFailed,
		slog.Group("cause",
			slog.String("phase", "listen"),
			slog.Int("port", 9000),
		),
	)

	records := decodeLogLines(t, buf.String())
	if len(records) != 3 {
		t.Fatalf("records = %d, want 3", len(records))
	}
	wantEvents := []string{EvBrokerStarted, EvConnectionRejected, EvStartupFailed}
	wantLevels := []string{"INFO", "WARN", "ERROR"}
	for i, rec := range records {
		if _, ok := rec["msg"]; ok {
			t.Errorf("record %d has a msg key, want event: %v", i, rec)
		}
		if _, ok := rec["time"]; !ok {
			t.Errorf("record %d has no time key: %v", i, rec)
		}
		if _, ok := rec["level"]; !ok {
			t.Errorf("record %d has no level key: %v", i, rec)
		}
		if _, ok := rec["event"]; !ok {
			t.Errorf("record %d has no event key: %v", i, rec)
		}

		ts, ok := rec["time"].(string)
		if !ok {
			t.Fatalf("record %d time is %T, want string", i, rec["time"])
		}
		if _, err := time.Parse(time.RFC3339Nano, ts); err != nil {
			t.Errorf("record %d time %q is not RFC 3339 with nanoseconds: %v", i, ts, err)
		}
		if !strings.HasSuffix(ts, "Z") {
			t.Errorf("record %d time %q is not UTC", i, ts)
		}

		if lvl, ok := rec["level"].(string); !ok || lvl != wantLevels[i] {
			t.Errorf("record %d level = %v, want %q", i, rec["level"], wantLevels[i])
		}
		if ev, ok := rec["event"].(string); !ok || ev != wantEvents[i] {
			t.Errorf("record %d event = %v, want %q", i, rec["event"], wantEvents[i])
		}
	}

	cause, ok := records[2]["cause"].(map[string]any)
	if !ok {
		t.Fatalf("record 2 cause = %T, want a JSON object", records[2]["cause"])
	}
	if got, want := cause["phase"], "listen"; got != want {
		t.Errorf("nested attribute phase = %v, want %q", got, want)
	}
}

func countLogKeys(t *testing.T, line string) map[string]int {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(line))
	tok, err := dec.Token()
	if err != nil {
		t.Fatalf("decode first token: %v", err)
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '{' {
		t.Fatalf("first token = %v, want %q", tok, '{')
	}
	keys := map[string]int{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			t.Fatalf("decode key: %v", err)
		}
		key, ok := tok.(string)
		if !ok {
			t.Fatalf("key token = %v, want a string", tok)
		}
		keys[key]++
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			t.Fatalf("decode value for %q: %v", key, err)
		}
	}
	if _, err := dec.Token(); err != nil {
		t.Fatalf("decode closing token: %v", err)
	}
	return keys
}

func decodeEventAndMessage(t *testing.T, line string) (event, message string) {
	t.Helper()
	var rec struct {
		Event   string `json:"event"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("decode line: %v", err)
	}
	return rec.Event, rec.Message
}

// TestNewLoggerKeepsEventKeyUnique checks that a caller's "event" attribute
// supplies the event name instead of producing a second "event" key.
func TestNewLoggerKeepsEventKeyUnique(t *testing.T) {
	var buf bytes.Buffer
	var level slog.LevelVar
	logger := NewLogger(&buf, &level)

	logger.Warn("free text", "event", "snake_event", "uid", 1001)

	line := strings.TrimSuffix(buf.String(), "\n")
	if got := countLogKeys(t, line)["event"]; got != 1 {
		t.Fatalf("event key appears %d times in %s, want 1", got, line)
	}
	event, message := decodeEventAndMessage(t, line)
	if event != "snake_event" {
		t.Errorf("event = %q, want %q", event, "snake_event")
	}
	if message != "free text" {
		t.Errorf("message = %q, want %q", message, "free text")
	}
}

// TestNewLoggerWithEventAttrKeepsEventKeyUnique checks the same rule for an
// event supplied through logger.With.
func TestNewLoggerWithEventAttrKeepsEventKeyUnique(t *testing.T) {
	var buf bytes.Buffer
	var level slog.LevelVar
	logger := NewLogger(&buf, &level)

	logger.With("event", "with_event", "uid", 1001).Info("free text")

	line := strings.TrimSuffix(buf.String(), "\n")
	if got := countLogKeys(t, line)["event"]; got != 1 {
		t.Fatalf("event key appears %d times in %s, want 1", got, line)
	}
	event, message := decodeEventAndMessage(t, line)
	if event != "with_event" {
		t.Errorf("event = %q, want %q", event, "with_event")
	}
	if message != "free text" {
		t.Errorf("message = %q, want %q", message, "free text")
	}
}

// TestNewLoggerEmptyGroupEventIsEventName checks that an event inside an
// empty-name group is flattened and becomes the record's event name.
func TestNewLoggerEmptyGroupEventIsEventName(t *testing.T) {
	t.Run("record", func(t *testing.T) {
		var buf bytes.Buffer
		var level slog.LevelVar
		logger := NewLogger(&buf, &level)

		logger.Info("free text", slog.Group("", "event", "group_event", "uid", 1001))

		line := strings.TrimSuffix(buf.String(), "\n")
		if got := countLogKeys(t, line)["event"]; got != 1 {
			t.Fatalf("event key appears %d times in %s, want 1", got, line)
		}
		event, message := decodeEventAndMessage(t, line)
		if event != "group_event" {
			t.Errorf("event = %q, want %q", event, "group_event")
		}
		if message != "free text" {
			t.Errorf("message = %q, want %q", message, "free text")
		}
	})

	t.Run("with", func(t *testing.T) {
		var buf bytes.Buffer
		var level slog.LevelVar
		logger := NewLogger(&buf, &level)

		logger.With(slog.Group("", "event", "with_group_event", "uid", 1001)).Info("free text")

		line := strings.TrimSuffix(buf.String(), "\n")
		if got := countLogKeys(t, line)["event"]; got != 1 {
			t.Fatalf("event key appears %d times in %s, want 1", got, line)
		}
		event, message := decodeEventAndMessage(t, line)
		if event != "with_group_event" {
			t.Errorf("event = %q, want %q", event, "with_group_event")
		}
		if message != "free text" {
			t.Errorf("message = %q, want %q", message, "free text")
		}
	})
}

// TestNewLoggerEventInsideGroupStaysNested checks that an "event" attribute
// written after WithGroup stays inside the group and does not name the record.
func TestNewLoggerEventInsideGroupStaysNested(t *testing.T) {
	var buf bytes.Buffer
	var level slog.LevelVar
	logger := NewLogger(&buf, &level)

	logger.WithGroup("g").Info("outer_event", "event", "nested_event", "uid", 1001)

	line := strings.TrimSuffix(buf.String(), "\n")
	if got := countLogKeys(t, line)["event"]; got != 1 {
		t.Fatalf("top-level event key appears %d times in %s, want 1", got, line)
	}
	event, message := decodeEventAndMessage(t, line)
	if event != "outer_event" {
		t.Errorf("event = %q, want %q", event, "outer_event")
	}
	if message != "" {
		t.Errorf("message = %q, want no message", message)
	}
	var rec struct {
		Group map[string]any `json:"g"`
	}
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("decode line: %v", err)
	}
	if got := rec.Group["event"]; got != "nested_event" {
		t.Errorf("g.event = %v, want %q", got, "nested_event")
	}
}

// TestNewLoggerKeepsCallerMessage checks that an explicit message attribute is
// not duplicated by the message the record already carries.
func TestNewLoggerKeepsCallerMessage(t *testing.T) {
	var buf bytes.Buffer
	var level slog.LevelVar
	logger := NewLogger(&buf, &level)

	logger.Info("log message", "event", "caller_message_event", "message", "caller text")

	line := strings.TrimSuffix(buf.String(), "\n")
	keys := countLogKeys(t, line)
	if got := keys["event"]; got != 1 {
		t.Fatalf("event key appears %d times in %s, want 1", got, line)
	}
	if got := keys["message"]; got != 1 {
		t.Fatalf("message key appears %d times in %s, want 1", got, line)
	}
	event, message := decodeEventAndMessage(t, line)
	if event != "caller_message_event" {
		t.Errorf("event = %q, want %q", event, "caller_message_event")
	}
	if message != "caller text" {
		t.Errorf("message = %q, want the caller's %q", message, "caller text")
	}
}

// TestNewLoggerGroupUnderEventKeyKeepsItsData checks that a group named "event"
// moves to "event_details" and the message still names the record.
func TestNewLoggerGroupUnderEventKeyKeepsItsData(t *testing.T) {
	t.Run("record", func(t *testing.T) {
		var buf bytes.Buffer
		var level slog.LevelVar
		logger := NewLogger(&buf, &level)

		logger.Info("group_event_message", slog.Group("event", "phase", "listen"))

		line := strings.TrimSuffix(buf.String(), "\n")
		if got := countLogKeys(t, line)["event"]; got != 1 {
			t.Fatalf("event key appears %d times in %s, want 1", got, line)
		}
		event, _ := decodeEventAndMessage(t, line)
		if event != "group_event_message" {
			t.Errorf("event = %q, want %q", event, "group_event_message")
		}
		var rec struct {
			Details map[string]any `json:"event_details"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("decode line: %v", err)
		}
		if got := rec.Details["phase"]; got != "listen" {
			t.Errorf("event_details.phase = %v, want %q", got, "listen")
		}
	})

	t.Run("with", func(t *testing.T) {
		var buf bytes.Buffer
		var level slog.LevelVar
		logger := NewLogger(&buf, &level)

		logger.With(slog.Group("event", "phase", "listen")).Info("group_event_message")

		line := strings.TrimSuffix(buf.String(), "\n")
		if got := countLogKeys(t, line)["event"]; got != 1 {
			t.Fatalf("event key appears %d times in %s, want 1", got, line)
		}
		event, _ := decodeEventAndMessage(t, line)
		if event != "group_event_message" {
			t.Errorf("event = %q, want %q", event, "group_event_message")
		}
		var rec struct {
			Details map[string]any `json:"event_details"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("decode line: %v", err)
		}
		if got := rec.Details["phase"]; got != "listen" {
			t.Errorf("event_details.phase = %v, want %q", got, "listen")
		}
	})
}

// TestLogTimeFormat checks that a timestamp in a non-UTC zone is rendered in
// UTC with nanosecond precision.
func TestLogTimeFormat(t *testing.T) {
	var buf bytes.Buffer
	var level slog.LevelVar
	logger := NewLogger(&buf, &level)

	zone := time.FixedZone("UTC+2", 2*60*60)
	record := slog.NewRecord(time.Date(2026, 1, 2, 4, 4, 5, 6, zone), slog.LevelInfo, "time_format", 0)
	if err := logger.Handler().Handle(context.Background(), record); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	const want = `"time":"2026-01-02T02:04:05.000000006Z"`
	if got := buf.String(); !strings.Contains(got, want) {
		t.Fatalf("log line %q does not contain %s", got, want)
	}
}

// TestLogLevelVarFilters checks that changing the level variable filters later
// records immediately.
func TestLogLevelVarFilters(t *testing.T) {
	var buf bytes.Buffer
	var level slog.LevelVar
	level.Set(slog.LevelWarn)
	logger := NewLogger(&buf, &level)

	logger.Info("info_event")
	logger.Warn("warn_event")
	records := decodeLogLines(t, buf.String())
	if len(records) != 1 {
		t.Fatalf("records at warn level = %d, want 1 (info filtered)", len(records))
	}
	if got := records[0]["event"]; got != "warn_event" {
		t.Errorf("event = %v, want %q", got, "warn_event")
	}

	level.Set(slog.LevelDebug)
	logger.Debug("debug_event")
	records = decodeLogLines(t, buf.String())
	if len(records) != 2 {
		t.Fatalf("records after raising verbosity = %d, want 2", len(records))
	}
	if got := records[1]["event"]; got != "debug_event" {
		t.Errorf("event = %v, want %q", got, "debug_event")
	}
}

// TestLoggerConcurrentWrites checks that concurrent writers produce one intact
// line per record.
func TestLoggerConcurrentWrites(t *testing.T) {
	var buf bytes.Buffer
	var level slog.LevelVar
	logger := NewLogger(&buf, &level)

	const goroutines = 16
	const perGoroutine = 100
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				logger.Info("concurrent_write", "goroutine", g, "iteration", i)
			}
		}(g)
	}
	wg.Wait()

	records := decodeLogLines(t, buf.String())
	if want := goroutines * perGoroutine; len(records) != want {
		t.Fatalf("records = %d, want %d", len(records), want)
	}
}

// TestParseLogLevel checks the accepted level names and that anything else is
// an error.
func TestParseLogLevel(t *testing.T) {
	valid := map[string]slog.Level{
		"debug": slog.LevelDebug,
		"info":  slog.LevelInfo,
		"warn":  slog.LevelWarn,
		"error": slog.LevelError,
	}
	for in, want := range valid {
		got, err := ParseLogLevel(in)
		if err != nil {
			t.Errorf("ParseLogLevel(%q) returned error: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("ParseLogLevel(%q) = %v, want %v", in, got, want)
		}
	}

	for _, in := range []string{"INFO", "", "trace", "Debug", " warning "} {
		if got, err := ParseLogLevel(in); err == nil {
			t.Errorf("ParseLogLevel(%q) = %v, want an error", in, got)
		}
	}
}
