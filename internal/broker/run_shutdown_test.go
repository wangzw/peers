package broker

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/testkit/socktest"
)

// orderedSessions records when a registration handler releases its session
// backend and closes released the first time it happens.
type orderedSessions struct {
	*fakeSessions
	order    *orderLog
	released chan struct{}
	once     sync.Once
}

func (s *orderedSessions) Acquire(ctx context.Context, uid uint32) (SessionBackend, func(), error) {
	base, release, err := s.fakeSessions.Acquire(ctx, uid)
	if err != nil {
		return nil, nil, err
	}
	return base, func() {
		s.order.add("release")
		s.once.Do(func() { close(s.released) })
		release()
	}, nil
}

// waitingRunner is a LinkRunner whose Close waits for a frame handler, bounded
// by closeWait.
type waitingRunner struct {
	order       *orderLog
	handlerDone <-chan struct{}
}

// closeWait is the package fallback, so a stuck shutdown fails the
// elapsed-time assertion instead of hanging.
const closeWait = eventTimeout

func (*waitingRunner) Listen(string) error { return nil }
func (r *waitingRunner) Start()            { r.order.add("Start") }
func (r *waitingRunner) Close() {
	r.order.add("Close")
	select {
	case <-r.handlerDone:
	case <-time.After(closeWait):
		r.order.add("CloseTimedOut")
	}
}

// TestRunShutdownStopsBlockedDelivery checks that shutdown returns without
// waiting for a delivery handler blocked in an inbox write.
func TestRunShutdownStopsBlockedDelivery(t *testing.T) {
	peer := fakenetPeer(20)
	sender := remoteMember{Name: "api-worker", Ref: "1a2b3c"}
	target := testAgent("frontend", "/tmp/ap-run-shutdown-delivery.sock")
	view := target
	block := newBlockingInbox()
	view.Inbox = block

	agents := newFakeAgents(view)
	agents.mu.Lock()
	agents.rereadErr = map[string]error{target.ID: nil}
	agents.mu.Unlock()

	topics := &deliveryTopics{
		local:  map[string][]string{target.ID: {"t1"}},
		remote: map[peerID][]deliveryRemoteEntry{peer: {{Member: sender, Topics: []string{"t1"}}}},
	}
	peers := &deliveryPeers{
		aliases:   map[string]peerID{},
		effective: map[peerID]string{peer: "hostA"},
	}
	settings := newFakeSettings(sendSettings{
		RequestTimeout: time.Hour,
		InboxBackoff:   time.Millisecond,
		InboxTimeout:   time.Hour,
		DedupeWindow:   time.Minute,
		InboxRetries:   0,
	}, "hostA")
	d := newDelivery(deliveryDeps{
		Agents:   agents,
		Topics:   topics,
		Peers:    peers,
		Link:     newFakeNet(t).end(fakenetPeer(10)),
		Settings: settings,
		Log:      quietLogger(),
	})

	frame, err := encodeDeliverRequest(deliverRequest{
		MessageID:  receiveMsgID,
		SenderName: sender.Name,
		SenderRef:  sender.Ref,
		TargetName: target.Name,
		TargetRef:  target.Ref,
		Remaining:  time.Hour,
		Text:       "hello",
	})
	if err != nil {
		t.Fatalf("encodeDeliverRequest: %v", err)
	}

	handlerDone := make(chan struct{})
	runner := &waitingRunner{order: &orderLog{}, handlerDone: handlerDone}
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.Assemblers = []Assembler{&fakeAssembler{
			name: "delivery-test",
			fn: func(_ context.Context, _ ConfigSource, deps *Deps, asm *Assembly) ([]Component, error) {
				deps.Delivery = d
				asm.Runner = runner
				return nil, nil
			},
		}}
	})
	rt.start()
	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q, want %q", got, "ok\n")
	}

	replies := make(chan link.Frame, 1)
	go func() {
		replies <- d.handleDeliver(context.Background(), peer, frame)
		close(handlerDone)
	}()
	select {
	case <-block.entered:
	case <-time.After(eventTimeout):
		t.Fatal("the delivery handler never reached the inbox write")
	}
	t.Cleanup(func() { close(block.release) })

	// The handler is released only by cleanup, so this timeout turns a shutdown
	// that waits for it into a reported failure; a correct shutdown never spends it.
	rt.cancel()
	done := make(chan error, 1)
	go func() { done <- rt.waitErr() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run error = %v, want nil", err)
		}
	case <-time.After(eventTimeout):
		t.Fatal("Run did not return after cancel: shutdown waited for the inbox write")
	}

	// A cancelled write does not prove nothing reached the session, so the
	// request must report delivery_unknown.
	select {
	case reply := <-replies:
		wantReceiveError(t, reply, localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN, "Delivery unknown:")
	case <-time.After(eventTimeout):
		t.Fatal("the delivery handler did not return after shutdown")
	}
}

func dialRunLocal(t *testing.T, sock string) localpb.LocalClient {
	t.Helper()
	cc, err := grpc.NewClient("unix://"+sock, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return localpb.NewLocalClient(cc)
}

// TestRunShutdownWaitsForRegistrationHandlers checks that Run returns only
// after an in-flight Register handler has released its backend and left no
// watch.
func TestRunShutdownWaitsForRegistrationHandlers(t *testing.T) {
	sessions := &fakeSessions{}
	order := &orderLog{}
	released := make(chan struct{})
	captured := make(chan *Broker, 1)
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.Deps.Sessions = &orderedSessions{fakeSessions: sessions, order: order, released: released}
		rt.opts.Components = []Component{&orderComponent{
			name:     "capture",
			order:    order,
			captured: captured,
		}}
	})
	rt.start()
	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q", got)
	}

	var b *Broker
	select {
	case b = <-captured:
	case <-time.After(eventTimeout):
		t.Fatal("the component never received the Broker")
	}

	// A valid absolute path with no session record keeps the handler on its watch.
	client := dialRunLocal(t, rt.socketPath())
	configDir := t.TempDir()
	startRegistrationAt(t, client, configDir, socktest.Path(t, "s.sock"))

	deadline := time.Now().Add(eventTimeout)
	for {
		b.mu.Lock()
		n := len(b.watches)
		b.mu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the registration handler did not take a watch; watches = %d", n)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Holding Broker.mu keeps the handler inside dropConn; cancelling and
	// releasing it lets the handler finish and release its backend.
	b.mu.Lock()
	rt.cancel()
	b.mu.Unlock()

	select {
	case <-released:
	case <-time.After(eventTimeout):
		t.Fatal("the registration handler did not release its backend")
	}

	if err := rt.waitErr(); err != nil {
		t.Fatalf("Run error = %v, want nil", err)
	}

	entries := order.list()
	releasedAt, stoppedAt := -1, -1
	for i, entry := range entries {
		switch entry {
		case "release":
			releasedAt = i
		case "capture.Stop":
			stoppedAt = i
		}
	}
	if releasedAt < 0 || stoppedAt < 0 || stoppedAt < releasedAt {
		t.Fatalf("shutdown order = %v, want the handler's release before capture.Stop", entries)
	}

	b.mu.Lock()
	watches := len(b.watches)
	b.mu.Unlock()
	if watches != 0 {
		t.Errorf("b.watches has %d entries after Run returned, want 0", watches)
	}
	if got, want := sessions.ReleaseCount(), len(sessions.AcquiredUIDs()); got != want {
		t.Errorf("session backend releases = %d, Acquire calls = %d", got, want)
	}
}
