package helper

import (
	"context"
	"sync"
	"time"

	"github.com/wangzw/peers/internal/inbox"
)

type deliverCall struct {
	s       inbox.Session
	sig     string
	text    string
	msgID   string
	timeout time.Duration
}

// fakeOps is the SessionOps test double: it serves sessions keyed by socket path, records deliveries, and lets tests fire watch callbacks. All field access is protected by mu; callbacks run outside the lock.
type fakeOps struct {
	mu          sync.Mutex
	sessions    map[string]inbox.Session
	lookupErr   error
	deliverFn   func(ctx context.Context, s inbox.Session, sig, text, msgID string, timeout time.Duration) error
	watches     map[string][]func(int)
	stopped     map[string]int
	delivered   []deliverCall
	fireOnWatch int
	// watchErr makes Watch fail; onWatch runs after the callback is registered and before Watch returns.
	watchErr error
	onWatch  func()
}

func (f *fakeOps) Lookup(configDir, socketPath string) (inbox.Session, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lookupErr != nil {
		return inbox.Session{}, f.lookupErr
	}
	s, ok := f.sessions[socketPath]
	if !ok {
		return inbox.Session{}, inbox.ErrNoRecord
	}
	return s, nil
}

func (f *fakeOps) Reread(s inbox.Session) (inbox.Session, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lookupErr != nil {
		return inbox.Session{}, f.lookupErr
	}
	got, ok := f.sessions[s.SocketPath]
	if !ok {
		return inbox.Session{}, inbox.ErrNoRecord
	}
	return got, nil
}

func (f *fakeOps) Watch(configDir string, onChange func(int)) (func(), error) {
	f.mu.Lock()
	if f.watches == nil {
		f.watches = make(map[string][]func(int))
	}
	f.watches[configDir] = append(f.watches[configDir], onChange)
	firePID := f.fireOnWatch
	hook := f.onWatch
	err := f.watchErr
	f.mu.Unlock()

	if hook != nil {
		hook()
	}
	if err != nil {
		return nil, err
	}

	var once sync.Once
	stop := func() {
		once.Do(func() {
			f.mu.Lock()
			if f.stopped == nil {
				f.stopped = make(map[string]int)
			}
			f.stopped[configDir]++
			f.mu.Unlock()
		})
	}
	if firePID != 0 {
		onChange(firePID)
	}
	return stop, nil
}

func (f *fakeOps) Deliver(ctx context.Context, s inbox.Session, sig, text, msgID string, timeout time.Duration) error {
	f.mu.Lock()
	f.delivered = append(f.delivered, deliverCall{s: s, sig: sig, text: text, msgID: msgID, timeout: timeout})
	fn := f.deliverFn
	f.mu.Unlock()
	if fn == nil {
		return nil
	}
	return fn(ctx, s, sig, text, msgID, timeout)
}

func (f *fakeOps) fire(configDir string, pid int) {
	f.mu.Lock()
	cbs := append([]func(int){}, f.watches[configDir]...)
	f.mu.Unlock()
	for _, cb := range cbs {
		cb(pid)
	}
}
