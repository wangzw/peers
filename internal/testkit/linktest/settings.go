package linktest

import (
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
)

// Default durations FakeSettings reports until a setter changes them; they
// match the configuration defaults.
const (
	defaultDialTimeout       = 5 * time.Second
	defaultReconnectMin      = time.Second
	defaultReconnectMax      = 60 * time.Second
	defaultKeepaliveInterval = 15 * time.Second
	defaultKeepaliveTimeout  = 10 * time.Second
)

// FakeSettings is a mutable configuration view that implements the method set
// link requires. All methods are safe for concurrent use.
type FakeSettings struct {
	mu                sync.Mutex
	alias             string
	advertised        []config.AdvertisedAddr
	dialTimeout       time.Duration
	reconnectMin      time.Duration
	reconnectMax      time.Duration
	keepaliveInterval time.Duration
	keepaliveTimeout  time.Duration
	events            []config.Event
	changed           chan struct{}
}

// NewFakeSettings returns settings with the production default durations, an
// empty advertised address list and the given alias.
func NewFakeSettings(alias string) *FakeSettings {
	return &FakeSettings{
		alias:             alias,
		dialTimeout:       defaultDialTimeout,
		reconnectMin:      defaultReconnectMin,
		reconnectMax:      defaultReconnectMax,
		keepaliveInterval: defaultKeepaliveInterval,
		keepaliveTimeout:  defaultKeepaliveTimeout,
		changed:           make(chan struct{}),
	}
}

// SetAlias replaces the effective alias.
func (s *FakeSettings) SetAlias(alias string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.alias = alias
	s.notifyLocked()
}

// SetAdvertised replaces the advertised address list.
func (s *FakeSettings) SetAdvertised(addrs []config.AdvertisedAddr) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advertised = append([]config.AdvertisedAddr(nil), addrs...)
	s.notifyLocked()
}

// SetDialTimeout replaces the dial timeout.
func (s *FakeSettings) SetDialTimeout(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dialTimeout = d
	s.notifyLocked()
}

// SetReconnect sets the minimum and maximum reconnect backoff.
func (s *FakeSettings) SetReconnect(min, max time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reconnectMin = min
	s.reconnectMax = max
	s.notifyLocked()
}

// SetKeepalive sets the keepalive interval and timeout.
func (s *FakeSettings) SetKeepalive(interval, timeout time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keepaliveInterval = interval
	s.keepaliveTimeout = timeout
	s.notifyLocked()
}

// Alias implements Settings.
func (s *FakeSettings) Alias() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.alias
}

// Advertised implements Settings and returns a copy.
func (s *FakeSettings) Advertised() []config.AdvertisedAddr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]config.AdvertisedAddr(nil), s.advertised...)
}

// DialTimeout implements Settings.
func (s *FakeSettings) DialTimeout() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dialTimeout
}

// ReconnectMin implements Settings.
func (s *FakeSettings) ReconnectMin() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reconnectMin
}

// ReconnectMax implements Settings.
func (s *FakeSettings) ReconnectMax() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reconnectMax
}

// KeepaliveInterval implements Settings.
func (s *FakeSettings) KeepaliveInterval() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.keepaliveInterval
}

// KeepaliveTimeout implements Settings.
func (s *FakeSettings) KeepaliveTimeout() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.keepaliveTimeout
}

// Audit records e.
func (s *FakeSettings) Audit(e config.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
	s.notifyLocked()
}

// Events returns a copy of every recorded audit event in order.
func (s *FakeSettings) Events() []config.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]config.Event(nil), s.events...)
}

// WaitEvent blocks until the first event of kind has been recorded and returns
// it, failing the test once eventTimeout passes.
func (s *FakeSettings) WaitEvent(t testing.TB, kind config.EventKind) config.Event {
	t.Helper()
	waitUntil(t, "audit event "+string(kind), s.changedCh, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.hasEventLocked(kind)
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.events {
		if e.Event == kind {
			return e
		}
	}
	return config.Event{}
}

func (s *FakeSettings) hasEventLocked(kind config.EventKind) bool {
	for _, e := range s.events {
		if e.Event == kind {
			return true
		}
	}
	return false
}

func (s *FakeSettings) changedCh() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.changed
}

// notifyLocked wakes every waiter. The caller must hold s.mu.
func (s *FakeSettings) notifyLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}
