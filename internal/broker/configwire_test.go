package broker

import (
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/wangzw/peers/internal/config"
)

type wireRecorder struct {
	calls     []string
	addresses []string
	listenErr error
}

func (r *wireRecorder) Listen(address string) error {
	r.calls = append(r.calls, "Listen")
	r.addresses = append(r.addresses, address)
	return r.listenErr
}

func (r *wireRecorder) AnnounceAlias()     { r.calls = append(r.calls, "AnnounceAlias") }
func (r *wireRecorder) AnnounceAddresses() { r.calls = append(r.calls, "AnnounceAddresses") }
func (r *wireRecorder) Reissue()           { r.calls = append(r.calls, "Reissue") }
func (r *wireRecorder) localAliasChanged() { r.calls = append(r.calls, "localAliasChanged") }

var (
	_ linkControl          = (*wireRecorder)(nil)
	_ CardIssuer           = (*wireRecorder)(nil)
	_ agentAddressNotifier = (*wireRecorder)(nil)
)

func newWireLog() (*slog.Logger, *syncBuffer) {
	buf := &syncBuffer{}
	return NewLogger(buf, &slog.LevelVar{}), buf
}

// TestAliasChangeAnnouncesAndReissues checks that an alias change announces
// the alias, reissues the card and updates agent addresses, tolerating a nil
// card issuer.
func TestAliasChangeAnnouncesAndReissues(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		f := newFakeConfig(t.TempDir())
		rec := &wireRecorder{}
		log, _ := newWireLog()
		subscribeConfigChanges(f, rec, rec, rec, log, new(atomic.Bool))

		f.fireKey("broker.alias", stringValue("hostA"), stringValue("hostB"))

		want := []string{"AnnounceAlias", "Reissue", "localAliasChanged"}
		if !reflect.DeepEqual(rec.calls, want) {
			t.Errorf("calls = %v, want %v", rec.calls, want)
		}
	})

	t.Run("nil card issuer", func(t *testing.T) {
		f := newFakeConfig(t.TempDir())
		rec := &wireRecorder{}
		log, _ := newWireLog()
		subscribeConfigChanges(f, rec, nil, rec, log, new(atomic.Bool))

		f.fireKey("broker.alias", stringValue("hostA"), stringValue("hostB"))

		want := []string{"AnnounceAlias", "localAliasChanged"}
		if !reflect.DeepEqual(rec.calls, want) {
			t.Errorf("calls = %v, want %v", rec.calls, want)
		}
	})
}

// TestAdvertisedChangeAnnouncesAndReissues checks that a changed advertised
// address list is announced and the card reissued.
func TestAdvertisedChangeAnnouncesAndReissues(t *testing.T) {
	f := newFakeConfig(t.TempDir())
	rec := &wireRecorder{}
	log, _ := newWireLog()
	subscribeConfigChanges(f, rec, rec, rec, log, new(atomic.Bool))

	f.fireAdvertised(nil, []config.AdvertisedAddr{{Kind: config.AdvertisedKindDirect, Addr: "10.0.0.1:7000"}})

	want := []string{"AnnounceAddresses", "Reissue"}
	if !reflect.DeepEqual(rec.calls, want) {
		t.Errorf("calls = %v, want %v", rec.calls, want)
	}
}

// TestListenChangeRebinds checks that a listen change binds the new address,
// and a failed rebind is logged as an error without stopping the Broker.
func TestListenChangeRebinds(t *testing.T) {
	t.Run("rebinds", func(t *testing.T) {
		f := newFakeConfig(t.TempDir())
		rec := &wireRecorder{}
		log, _ := newWireLog()
		subscribeConfigChanges(f, rec, rec, rec, log, new(atomic.Bool))

		f.fireKey("broker.listen", stringValue("127.0.0.1:7000"), stringValue("127.0.0.1:7001"))

		if want := []string{"Listen"}; !reflect.DeepEqual(rec.calls, want) {
			t.Fatalf("calls = %v, want %v", rec.calls, want)
		}
		if want := []string{"127.0.0.1:7001"}; !reflect.DeepEqual(rec.addresses, want) {
			t.Errorf("Listen addresses = %v, want %v", rec.addresses, want)
		}
	})

	t.Run("logs failure", func(t *testing.T) {
		f := newFakeConfig(t.TempDir())
		rec := &wireRecorder{listenErr: errors.New("address already in use")}
		log, buf := newWireLog()
		subscribeConfigChanges(f, rec, rec, rec, log, new(atomic.Bool))

		f.fireKey("broker.listen", stringValue("127.0.0.1:7000"), stringValue("127.0.0.1:7001"))

		lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
		if len(lines) != 1 || lines[0] == "" {
			t.Fatalf("log has %d lines, want exactly 1: %q", len(lines), buf.String())
		}
		var got struct {
			Level   string `json:"level"`
			Event   string `json:"event"`
			Address string `json:"address"`
			Error   string `json:"error"`
		}
		if err := json.Unmarshal([]byte(lines[0]), &got); err != nil {
			t.Fatalf("parsing log line: %v\n%s", err, lines[0])
		}
		if got.Level != "ERROR" {
			t.Errorf("level = %q, want ERROR", got.Level)
		}
		if got.Event != "listen_rebind_failed" {
			t.Errorf("event = %q, want listen_rebind_failed", got.Event)
		}
		if got.Address != "127.0.0.1:7001" {
			t.Errorf("address = %q, want 127.0.0.1:7001", got.Address)
		}
		if !strings.Contains(got.Error, "address already in use") {
			t.Errorf("error = %q, want it to contain the Listen error", got.Error)
		}
	})
}

// TestSubscribesExactlyThree checks that exactly three subscriptions are
// registered, two keys and the advertised list, and never broker.public.
func TestSubscribesExactlyThree(t *testing.T) {
	f := newFakeConfig(t.TempDir())
	rec := &wireRecorder{}
	log, _ := newWireLog()
	subscribeConfigChanges(f, rec, rec, rec, log, new(atomic.Bool))

	f.mu.Lock()
	keys := append([]string(nil), f.subscribedKeys...)
	advertised := len(f.advertised)
	f.mu.Unlock()

	wantKeys := []string{"broker.listen", "broker.alias"}
	if !reflect.DeepEqual(keys, wantKeys) {
		t.Errorf("subscribed keys = %v, want %v", keys, wantKeys)
	}
	if advertised != 1 {
		t.Errorf("advertised subscriptions = %d, want 1", advertised)
	}
	for _, key := range keys {
		if key == "broker.public" {
			t.Error("subscribed to broker.public, which the directory unit owns")
		}
	}
}
