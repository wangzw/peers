package config

import (
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type ncNoRoutes struct{}

func (ncNoRoutes) DefaultRouteAddresses() ([]netip.Addr, error) { return nil, nil }

type ncRoutes struct {
	mu    sync.Mutex
	addrs []netip.Addr
}

func (r *ncRoutes) DefaultRouteAddresses() ([]netip.Addr, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]netip.Addr(nil), r.addrs...), nil
}

func (r *ncRoutes) set(addrs ...netip.Addr) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.addrs = append([]netip.Addr(nil), addrs...)
}

func ncAddr(t *testing.T, s string) netip.Addr {
	t.Helper()
	addr, err := netip.ParseAddr(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return addr
}

func newNCCore(t *testing.T, body string, routes addressSource) (*core, string) {
	t.Helper()
	home := t.TempDir()
	c := newCore()
	c.skipDataCheck = true
	c.noBackground = true
	c.checkRuntime = func() (string, error) { return filepath.Join(home, "run"), nil }
	c.homeDir = func() (string, error) { return home, nil }
	c.hostname = func() (string, error) { return "hostA", nil }
	c.accountName = func() (string, error) { return "tester", nil }
	c.now = func() time.Time { return time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC) }
	c.routesSrc = ncNoRoutes{}
	if routes != nil {
		c.routesSrc = routes
	}
	dir := filepath.Join(home, ".peers")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := c.load(ModeUser, ""); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.close)
	return c, path
}

type ncKeyEvent struct {
	old Value
	new Value
}

type ncAdvEvent struct {
	old []AdvertisedAddr
	new []AdvertisedAddr
}

func ncSubscribeKey(c *core, key string, ch chan<- ncKeyEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subs[key] = append(c.subs[key], func(old, new Value) {
		ch <- ncKeyEvent{old: old, new: new}
	})
}

func ncSubscribeAdvertised(c *core, ch chan<- ncAdvEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.advSubs = append(c.advSubs, func(old, new []AdvertisedAddr) {
		ch <- ncAdvEvent{old: old, new: new}
	})
}

// ncDrain enqueues a barrier callback and waits for it, so everything enqueued
// before the barrier has finished when it returns.
func ncDrain(t *testing.T, c *core) {
	t.Helper()
	done := make(chan struct{})
	c.notify.enqueue(func() { close(done) })
	select {
	case <-done:
	case <-time.After(eventTimeout):
		t.Fatal("the notification queue did not drain")
	}
}

func ncWaitKey(t *testing.T, ch <-chan ncKeyEvent) ncKeyEvent {
	t.Helper()
	select {
	case event := <-ch:
		return event
	case <-time.After(eventTimeout):
		t.Fatal("no key notification arrived")
		return ncKeyEvent{}
	}
}

func ncWaitAdvertised(t *testing.T, ch <-chan ncAdvEvent) ncAdvEvent {
	t.Helper()
	select {
	case event := <-ch:
		return event
	case <-time.After(eventTimeout):
		t.Fatal("no advertised-address notification arrived")
		return ncAdvEvent{}
	}
}

func ncAddrs(list []AdvertisedAddr) []string {
	out := make([]string, 0, len(list))
	for _, addr := range list {
		out = append(out, addr.Addr)
	}
	return out
}

func ncRequireAddrList(t *testing.T, got, want []AdvertisedAddr, what string) {
	t.Helper()
	gotAddrs := ncAddrs(got)
	if len(gotAddrs) != len(want) {
		t.Fatalf("%s = %v, want %v", what, gotAddrs, ncAddrs(want))
	}
	for i := range want {
		if gotAddrs[i] != want[i].Addr {
			t.Fatalf("%s = %v, want %v", what, gotAddrs, ncAddrs(want))
		}
	}
}

func ncSet(t *testing.T, c *core, key string, v Value) {
	t.Helper()
	c.mu.Lock()
	_, err := c.setLocked(Actor{}, key, v)
	c.mu.Unlock()
	if err != nil {
		t.Fatalf("Set(%s) error = %v", key, err)
	}
}

func ncWriteConfig(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestConfigNotificationSetAdvertised checks that a Set on a listen-group key
// notifies the advertised-address subscribers with the old and new lists.
func TestConfigNotificationSetAdvertised(t *testing.T) {
	c, _ := newNCCore(t, "broker:\n  listen: 127.0.0.1:7777\n", nil)
	changes := make(chan ncAdvEvent, 4)
	ncSubscribeAdvertised(c, changes)

	ncSet(t, c, "broker.listen", stringValue("127.0.0.1:8888"))

	event := ncWaitAdvertised(t, changes)
	ncRequireAddrList(t, event.old, []AdvertisedAddr{{Kind: AdvertisedKindDirect, Addr: "127.0.0.1:7777"}}, "notification old list")
	ncRequireAddrList(t, event.new, []AdvertisedAddr{{Kind: AdvertisedKindDirect, Addr: "127.0.0.1:8888"}}, "notification new list")
}

// TestConfigNotificationSetUnrelatedKeyDoesNotNotifyAdvertised checks that a Set
// outside the address group sends no advertised-address notification.
func TestConfigNotificationSetUnrelatedKeyDoesNotNotifyAdvertised(t *testing.T) {
	c, _ := newNCCore(t, "broker:\n  listen: 127.0.0.1:7777\n", nil)
	changes := make(chan ncAdvEvent, 4)
	ncSubscribeAdvertised(c, changes)

	ncSet(t, c, "broker.public", boolValue(true))
	ncDrain(t, c)

	select {
	case event := <-changes:
		t.Fatalf("Set(broker.public) notified the advertised list: %+v", event)
	default:
	}
}

// TestConfigNotificationSetListenNotifiesDefaultAlias checks that a listen
// change sends no alias notification for a derived or an explicit alias.
func TestConfigNotificationSetListenNotifiesDefaultAlias(t *testing.T) {
	t.Run("default alias does not follow the listen port", func(t *testing.T) {
		c, _ := newNCCore(t, "broker:\n  listen: 127.0.0.1:7777\n", nil)
		alias := make(chan ncKeyEvent, 4)
		advertised := make(chan ncAdvEvent, 4)
		ncSubscribeKey(c, "broker.alias", alias)
		ncSubscribeAdvertised(c, advertised)

		ncSet(t, c, "broker.listen", stringValue("127.0.0.1:8888"))

		// The advertised notification is enqueued after the alias one, so
		// receiving it proves a stray alias notification would already have
		// been delivered.
		ncWaitAdvertised(t, advertised)
		select {
		case event := <-alias:
			t.Fatalf("a listener-independent alias was notified: old=%q new=%q", event.old.String, event.new.String)
		default:
		}
	})

	t.Run("explicit alias is not notified", func(t *testing.T) {
		c, _ := newNCCore(t, "broker:\n  alias: myalias\n  listen: 127.0.0.1:7777\n", nil)
		alias := make(chan ncKeyEvent, 4)
		advertised := make(chan ncAdvEvent, 4)
		ncSubscribeKey(c, "broker.alias", alias)
		ncSubscribeAdvertised(c, advertised)

		ncSet(t, c, "broker.listen", stringValue("127.0.0.1:8888"))

		// The advertised notification is enqueued after the alias one, so
		// receiving it proves a stray alias notification would already have
		// been delivered.
		ncWaitAdvertised(t, advertised)
		select {
		case event := <-alias:
			t.Fatalf("an unchanged explicit alias was notified: old=%q new=%q", event.old.String, event.new.String)
		default:
		}
	})
}

// TestConfigNotificationReloadAdvertised checks that an adopted reload of a
// listen-group key notifies the advertised-address subscribers.
func TestConfigNotificationReloadAdvertised(t *testing.T) {
	c, path := newNCCore(t, "broker:\n  listen: 127.0.0.1:7777\n", nil)
	changes := make(chan ncAdvEvent, 4)
	ncSubscribeAdvertised(c, changes)

	ncWriteConfig(t, path, "broker:\n  listen: 127.0.0.1:8888\n")
	c.rescanOnce()

	event := ncWaitAdvertised(t, changes)
	ncRequireAddrList(t, event.old, []AdvertisedAddr{{Kind: AdvertisedKindDirect, Addr: "127.0.0.1:7777"}}, "notification old list")
	ncRequireAddrList(t, event.new, []AdvertisedAddr{{Kind: AdvertisedKindDirect, Addr: "127.0.0.1:8888"}}, "notification new list")
}

// TestConfigNotificationReloadKey checks that an adopted reloaded value notifies
// its key subscribers.
func TestConfigNotificationReloadKey(t *testing.T) {
	c, path := newNCCore(t, "broker:\n  listen: 127.0.0.1:7777\n  public: false\n", nil)
	public := make(chan ncKeyEvent, 4)
	ncSubscribeKey(c, "broker.public", public)

	ncWriteConfig(t, path, "broker:\n  listen: 127.0.0.1:7777\n  public: true\n")
	c.rescanOnce()

	event := ncWaitKey(t, public)
	if event.old.Bool || !event.new.Bool {
		t.Fatalf("public notification old/new = %v/%v, want false/true", event.old.Bool, event.new.Bool)
	}
}

// TestConfigNotificationReloadUnchangedIsSilent checks that a reload changing no
// value notifies neither keys nor advertised addresses.
func TestConfigNotificationReloadUnchangedIsSilent(t *testing.T) {
	c, path := newNCCore(t, "broker:\n  listen: 127.0.0.1:7777\n  public: false\n", nil)
	public := make(chan ncKeyEvent, 4)
	advertised := make(chan ncAdvEvent, 4)
	ncSubscribeKey(c, "broker.public", public)
	ncSubscribeAdvertised(c, advertised)

	ncWriteConfig(t, path, "# no value changed\nbroker:\n  listen: 127.0.0.1:7777\n  public: false\n")
	c.rescanOnce()
	ncDrain(t, c)

	select {
	case event := <-public:
		t.Fatalf("a reload with no change notified broker.public: old=%+v new=%+v", event.old, event.new)
	default:
	}
	select {
	case event := <-advertised:
		t.Fatalf("a reload with an unchanged address list notified the subscribers: %+v", event)
	default:
	}
}

// TestConfigNotificationReevaluateAdvertised checks that periodic
// re-enumeration notifies a changed list and not an unchanged one.
func TestConfigNotificationReevaluateAdvertised(t *testing.T) {
	routes := &ncRoutes{}
	routes.set(ncAddr(t, "192.0.2.10"))
	c, _ := newNCCore(t, "broker:\n  listen: :17443\n", routes)
	changes := make(chan ncAdvEvent, 4)
	ncSubscribeAdvertised(c, changes)

	c.reevaluateAdvertised()
	ncDrain(t, c)
	select {
	case event := <-changes:
		t.Fatalf("an unchanged re-enumerated list was notified: %+v", event)
	default:
	}

	routes.set(ncAddr(t, "192.0.2.20"))
	c.reevaluateAdvertised()
	event := ncWaitAdvertised(t, changes)
	ncRequireAddrList(t, event.old, []AdvertisedAddr{{Kind: AdvertisedKindDirect, Addr: "192.0.2.10:17443"}}, "notification old list")
	ncRequireAddrList(t, event.new, []AdvertisedAddr{{Kind: AdvertisedKindDirect, Addr: "192.0.2.20:17443"}}, "notification new list")
}
