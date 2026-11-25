package config

import (
	"log/slog"
	"sync"
)

type notifier interface {
	enqueue(fn func())
	close()
}

type asyncNotifier struct {
	mu     sync.Mutex
	cond   *sync.Cond
	queue  []func()
	closed bool
	done   chan struct{}
	logger *slog.Logger
	// closeMarked runs after close sets closed and before it waits for the queue
	// to drain; tests use it to release a blocked callback only once the queue
	// is provably behind close.
	closeMarked func()
}

func newNotifier(logger *slog.Logger) notifier {
	n := &asyncNotifier{done: make(chan struct{}), logger: logger}
	n.cond = sync.NewCond(&n.mu)
	go n.run()
	return n
}

func (n *asyncNotifier) enqueue(fn func()) {
	if fn == nil {
		return
	}
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		n.logger.Error("config: dropping a notification after close")
		return
	}
	n.queue = append(n.queue, fn)
	n.mu.Unlock()
	n.cond.Signal()
}

func (n *asyncNotifier) close() {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return
	}
	n.closed = true
	marked := n.closeMarked
	n.mu.Unlock()
	if marked != nil {
		marked()
	}
	n.cond.Broadcast()
	<-n.done
}

func (n *asyncNotifier) run() {
	defer close(n.done)
	for {
		n.mu.Lock()
		for len(n.queue) == 0 && !n.closed {
			n.cond.Wait()
		}
		if len(n.queue) == 0 && n.closed {
			n.mu.Unlock()
			return
		}
		fn := n.queue[0]
		n.queue = n.queue[1:]
		n.mu.Unlock()
		callSafely(fn, n.logger)
	}
}

func callSafely(fn func(), logger *slog.Logger) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("config: notification callback panicked", "panic", r)
		}
	}()
	fn()
}
