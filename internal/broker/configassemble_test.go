package broker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
)

var (
	_ Assembler   = (*configWiring)(nil)
	_ Component   = (*configComponent)(nil)
	_ CardIssuer  = noopIssuer{}
	_ linkControl = noopLink{}
)

// wiringChecker is an accessChecker safe for the gRPC server goroutine. The
// package's fakeAccessChecker records without a lock, which is fine for direct
// interceptor calls but not for a call arriving over the network.
type wiringChecker struct {
	mu      sync.Mutex
	granted bool
	err     error
	calls   []accessCheckCall
}

func (c *wiringChecker) Check(_ context.Context, who Caller, path string, need Access) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, accessCheckCall{who: who, path: path, need: need})
	return c.granted, c.err
}

// wiringIssuer records card reissues.
type wiringIssuer struct {
	mu sync.Mutex
	n  int
}

func (i *wiringIssuer) Reissue() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.n++
}

func (i *wiringIssuer) count() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.n
}

// newWiringComponents runs configWiring.Assemble for cfg without going through
// Run, and starts nothing.
func newWiringComponents(t *testing.T, cfg ConfigSource, deps *Deps, asm *Assembly, checker accessChecker) []Component {
	t.Helper()
	w := &configWiring{checker: checker}
	comps, err := w.Assemble(context.Background(), cfg, deps, asm)
	if err != nil {
		t.Fatalf("configWiring.Assemble: %v", err)
	}
	return comps
}

func TestConfigWiringAddsInterceptors(t *testing.T) {
	checker := &wiringChecker{}
	var before, after int
	tb := newTestBroker(t, func(d *Deps) {
		before = len(d.ServerOptions)
		comps := newWiringComponents(t, d.Config, d, &Assembly{}, checker)
		if len(comps) != 1 {
			t.Fatalf("Assemble returned %d components, want 1", len(comps))
		}
		after = len(d.ServerOptions)
	})
	if got := after - before; got != 2 {
		t.Fatalf("Assemble added %d server options, want 2", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	_, err := tb.client.SetConfig(ctx, &localpb.SetConfigRequest{Key: "broker.alias", Value: "build-box"})
	wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED)

	checker.mu.Lock()
	calls := append([]accessCheckCall(nil), checker.calls...)
	checker.mu.Unlock()
	if len(calls) != 1 {
		t.Fatalf("checker called %d times, want 1", len(calls))
	}
	if calls[0].path != accessTestConfigPath {
		t.Errorf("checker saw path %q, want %q", calls[0].path, accessTestConfigPath)
	}

	tb.cfg.mu.Lock()
	setCalls := len(tb.cfg.setCalls)
	tb.cfg.mu.Unlock()
	if setCalls != 0 {
		t.Errorf("Set was called %d times despite the denial, want 0", setCalls)
	}

	if _, err := callTestHello(t, tb.client, localpb.ProtocolVersion); err != nil {
		t.Fatalf("Hello with the interceptor installed: %v", err)
	}
}

func TestConfigWiringDefaultsToAllowAll(t *testing.T) {
	tb := newTestBroker(t, func(d *Deps) {
		newWiringComponents(t, d.Config, d, &Assembly{}, nil)
	})

	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()

	// Status only filters on denial, so the default checker must be exercised
	// through calls the interceptor rejects outright.
	resp, err := tb.client.GetConfig(ctx, &localpb.GetConfigRequest{Key: "broker.alias"})
	if err != nil {
		t.Fatalf("GetConfig with the default checker: %v", err)
	}
	if entries := resp.GetEntries(); len(entries) != 1 || entries[0].GetKey() != "broker.alias" {
		t.Fatalf("GetConfig entries = %v, want one broker.alias entry", entries)
	} else if got := entries[0].GetValue().GetStringValue(); got != "hostA" {
		t.Errorf("GetConfig broker.alias = %q, want hostA", got)
	}

	setResp, err := tb.client.SetConfig(ctx, &localpb.SetConfigRequest{Key: "broker.alias", Value: "build-box"})
	if err != nil {
		t.Fatalf("SetConfig with the default checker: %v", err)
	}
	if got := setResp.GetNewValue().GetStringValue(); got != "build-box" {
		t.Errorf("SetConfig new value = %q, want build-box", got)
	}
	tb.cfg.mu.Lock()
	setCalls := len(tb.cfg.setCalls)
	tb.cfg.mu.Unlock()
	if setCalls != 1 {
		t.Errorf("Set calls = %d, want 1", setCalls)
	}
}

func TestConfigWiringUsesCardIssuer(t *testing.T) {
	tb := newTestBroker(t)
	issuer := &wiringIssuer{}
	deps := &Deps{Config: tb.cfg, CardIssuer: issuer}
	comps := newWiringComponents(t, tb.cfg, deps, &Assembly{}, nil)

	if err := comps[0].Start(context.Background(), tb.b); err != nil {
		t.Fatalf("component Start: %v", err)
	}
	tb.cfg.fireKey("broker.alias", stringValue("hostA"), stringValue("hostB"))

	if got := issuer.count(); got != 1 {
		t.Errorf("CardIssuer.Reissue called %d times, want 1", got)
	}
}

func TestConfigComponentRegistersListenValidator(t *testing.T) {
	tb := newTestBroker(t)
	deps := &Deps{Config: tb.cfg}
	comps := newWiringComponents(t, tb.cfg, deps, &Assembly{}, nil)

	if err := comps[0].Start(context.Background(), tb.b); err != nil {
		t.Fatalf("component Start: %v", err)
	}

	tb.cfg.mu.Lock()
	validatorCount := len(tb.cfg.validators)
	_, hasListen := tb.cfg.validators["broker.listen"]
	_, hasAlias := tb.cfg.validators["broker.alias"]
	keys := append([]string(nil), tb.cfg.subscribedKeys...)
	tb.cfg.mu.Unlock()

	if validatorCount != 1 || !hasListen {
		t.Errorf("validators = %d keys (broker.listen: %v), want only broker.listen", validatorCount, hasListen)
	}
	if hasAlias {
		t.Error("registered a broker.alias validator, which belongs to the pairing unit")
	}
	if want := []string{"broker.listen", "broker.alias"}; !reflect.DeepEqual(keys, want) {
		t.Errorf("subscribed keys = %v, want %v", keys, want)
	}
}

func TestConfigComponentListenValidatorProbesNewAddress(t *testing.T) {
	tb := newTestBroker(t)
	deps := &Deps{Config: tb.cfg}
	comps := newWiringComponents(t, tb.cfg, deps, &Assembly{}, nil)
	if err := comps[0].Start(context.Background(), tb.b); err != nil {
		t.Fatalf("component Start: %v", err)
	}

	tb.cfg.mu.Lock()
	checks := append([]func(old, new config.Value) error(nil), tb.cfg.validators["broker.listen"]...)
	tb.cfg.probeErr = errors.New("bind: permission denied")
	tb.cfg.mu.Unlock()
	if len(checks) != 1 {
		t.Fatalf("broker.listen validators = %d, want 1", len(checks))
	}

	err := checks[0](config.Value{Kind: config.KindString}, config.Value{Kind: config.KindString, String: "127.0.0.1:1"})
	if err == nil {
		t.Fatal("validator accepted an address the probe refused")
	}
	if !strings.Contains(err.Error(), "is unavailable") {
		t.Errorf("validator error = %q, want it to contain %q", err, "is unavailable")
	}
}

func TestConfigWiringWithoutLinkAndDirectory(t *testing.T) {
	tb := newTestBroker(t)
	deps := &Deps{Config: tb.cfg}
	comps := newWiringComponents(t, tb.cfg, deps, &Assembly{}, nil)

	if err := comps[0].Start(context.Background(), tb.b); err != nil {
		t.Fatalf("component Start: %v", err)
	}
	// Every callback must tolerate the no-op link and issuer.
	tb.cfg.fireKey("broker.alias", stringValue("hostA"), stringValue("hostB"))
	tb.cfg.fireKey("broker.listen", stringValue(""), stringValue("127.0.0.1:7001"))
	tb.cfg.fireAdvertised(nil, []config.AdvertisedAddr{{Kind: config.AdvertisedKindDirect, Addr: "10.0.0.1:7000"}})
}

func TestConfigComponentStopStopsForwarding(t *testing.T) {
	tb := newTestBroker(t)
	rec := &wireRecorder{}
	comp := &configComponent{cfg: tb.cfg, link: rec, cards: rec}
	if err := comp.Start(context.Background(), tb.b); err != nil {
		t.Fatalf("component Start: %v", err)
	}
	if err := comp.AfterReady(context.Background()); err != nil {
		t.Fatalf("component AfterReady: %v", err)
	}

	tb.cfg.fireKey("broker.listen", stringValue("127.0.0.1:7000"), stringValue("127.0.0.1:7001"))
	if want := []string{"Listen"}; !reflect.DeepEqual(rec.calls, want) {
		t.Fatalf("calls before Stop = %v, want %v", rec.calls, want)
	}
	rec.calls = nil

	comp.Stop()
	tb.cfg.fireKey("broker.listen", stringValue("127.0.0.1:7001"), stringValue("127.0.0.1:7002"))
	tb.cfg.fireKey("broker.alias", stringValue("hostA"), stringValue("hostB"))

	if len(rec.calls) != 0 {
		t.Errorf("calls after Stop = %v, want none", rec.calls)
	}
}

func TestDefaultAssemblersEndsWithConfig(t *testing.T) {
	assemblers := DefaultAssemblers()
	if len(assemblers) == 0 {
		t.Fatal("DefaultAssemblers returned no assemblers")
	}
	last := assemblers[len(assemblers)-1]
	if got := last.Name(); got != "config" {
		t.Errorf("last assembler = %q, want config", got)
	}
}

// newWiringServer builds a Broker whose server options come from configWiring
// and serves it on an in-memory listener. The peer credentials are read from a
// stub, because a bufconn connection carries no SO_PEERCRED.
func newWiringServer(t *testing.T, checker accessChecker) (*fakeConfig, localpb.LocalClient) {
	t.Helper()

	cfg := newFakeConfig(t.TempDir())
	logs := &syncBuffer{}
	level := new(slog.LevelVar)
	level.Set(slog.LevelDebug)
	deps := Deps{
		Config:    cfg,
		Sessions:  &fakeSessions{},
		Topics:    newFakeTopics(),
		Logger:    NewLogger(logs, level),
		InSession: func() bool { return false },
		Geteuid:   os.Geteuid,
		ReadPeerCred: func(net.Conn) (Caller, error) {
			return Caller{UID: uint32(os.Geteuid()), Username: "alice"}, nil
		},
	}
	if _, err := (&configWiring{checker: checker}).Assemble(context.Background(), cfg, &deps, &Assembly{}); err != nil {
		t.Fatalf("Assemble: %v", err)
	}

	b, err := New(deps)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	lis := bufconn.Listen(1 << 20)
	go func() { _ = b.Serve(lis) }()
	t.Cleanup(func() {
		b.Stop()
		_ = lis.Close()
	})

	cc, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return cfg, localpb.NewLocalClient(cc)
}

// TestLocalServerEndToEndOverBufconn covers REQ-broker-config-002 and
// REQ-broker-config-021: over a real gRPC server, a permitted caller changes
// the configuration and reads the audit log, and a denied caller reaches
// neither.
func TestLocalServerEndToEndOverBufconn(t *testing.T) {
	t.Run("allowed", func(t *testing.T) {
		checker := &wiringChecker{granted: true}
		cfg, client := newWiringServer(t, checker)
		cfg.Audit(config.Event{Event: config.EventConfigSet, Key: "broker.alias", Actor: "w@hostA"})

		ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
		defer cancel()

		resp, err := client.SetConfig(ctx, &localpb.SetConfigRequest{Key: "broker.alias", Value: "build-box"})
		if err != nil {
			t.Fatalf("SetConfig: %v", err)
		}
		if got := resp.GetNewValue().GetStringValue(); got != "build-box" {
			t.Errorf("new value = %q, want build-box", got)
		}
		v, err := cfg.Get("broker.alias")
		if err != nil {
			t.Fatalf("Get after SetConfig: %v", err)
		}
		if v.String != "build-box" {
			t.Errorf("stored alias = %q, want build-box", v.String)
		}

		stream, err := client.Audit(ctx, &localpb.AuditRequest{})
		if err != nil {
			t.Fatalf("Audit: %v", err)
		}
		ev, err := stream.Recv()
		if err != nil {
			t.Fatalf("Audit Recv: %v", err)
		}
		if got := ev.GetEvent(); got != localpb.AuditEventKind_AUDIT_EVENT_KIND_CONFIG_SET {
			t.Errorf("audit event = %v, want config_set", got)
		}
		if _, err := stream.Recv(); err != io.EOF {
			t.Errorf("second Audit Recv error = %v, want EOF", err)
		}
	})

	t.Run("denied", func(t *testing.T) {
		checker := &wiringChecker{}
		cfg, client := newWiringServer(t, checker)
		cfg.Audit(config.Event{Event: config.EventConfigSet, Key: "broker.alias"})

		ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
		defer cancel()

		_, err := client.SetConfig(ctx, &localpb.SetConfigRequest{Key: "broker.alias", Value: "build-box"})
		wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED)
		cfg.mu.Lock()
		setCalls := len(cfg.setCalls)
		alias := cfg.values["broker.alias"].String
		cfg.mu.Unlock()
		if setCalls != 0 || alias != "hostA" {
			t.Errorf("denied SetConfig touched the fake: setCalls=%d alias=%q", setCalls, alias)
		}

		stream, err := client.Audit(ctx, &localpb.AuditRequest{})
		if err == nil {
			_, err = stream.Recv()
		}
		wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED)
	})
}
