package helper

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
)

// maxHelperStderrLine bounds the bytes held for one unterminated stderr line so a helper that never writes a newline cannot grow the Broker's memory.
const maxHelperStderrLine = 64 << 10

type helperStderr struct {
	log      *slog.Logger
	uid      uint32
	stopping func() bool

	mu  sync.Mutex
	buf []byte
}

func newHelperStderr(log *slog.Logger, uid uint32, stopping func() bool) *helperStderr {
	return &helperStderr{log: log, uid: uid, stopping: stopping}
}

func (w *helperStderr) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	n := len(p)
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}
		w.logLine(string(bytes.TrimSuffix(w.buf[:i], []byte{'\r'})))
		w.buf = w.buf[i+1:]
	}
	if len(w.buf) > maxHelperStderrLine {
		w.logLine(string(w.buf[:maxHelperStderrLine]) + " (truncated)")
		w.buf = w.buf[:0]
	}
	return n, nil
}

func (w *helperStderr) flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.buf) > 0 {
		w.logLine(string(bytes.TrimSuffix(w.buf, []byte{'\r'})))
		w.buf = nil
	}
}

func (w *helperStderr) logLine(line string) {
	level := slog.LevelWarn
	if w.stopping != nil && w.stopping() && strings.Contains(line, "context canceled") {
		// The helper's context ended with the Broker; this is the stop, not a failure.
		level = slog.LevelDebug
	}
	w.log.Log(context.Background(), level, "account_helper_stderr",
		"uid", w.uid,
		"line", line)
}
