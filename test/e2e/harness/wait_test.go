package harness

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakelocal"
	"github.com/wangzw/peers/test/e2e/node"
)

// recordTB captures Fatalf calls and panics with fatalStop so the code under
// test unwinds the way it would under testing.T.
type recordTB struct {
	testing.TB // always nil; embedded for the unexported method only

	mu     sync.Mutex
	fatals []string
	logs   []string
}

type fatalStop struct{}

func (r *recordTB) Helper() {}

func (r *recordTB) Logf(format string, args ...any) {
	r.mu.Lock()
	r.logs = append(r.logs, fmt.Sprintf(format, args...))
	r.mu.Unlock()
}

func (r *recordTB) Fatalf(format string, args ...any) {
	r.mu.Lock()
	r.fatals = append(r.fatals, fmt.Sprintf(format, args...))
	r.mu.Unlock()
	panic(fatalStop{})
}

func (r *recordTB) lastFatal() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.fatals) == 0 {
		return ""
	}
	return r.fatals[len(r.fatals)-1]
}

// captureFatal returns the message of the Fatalf fn raised and re-raises any
// other panic.
func captureFatal(t *testing.T, tb *recordTB, fn func()) string {
	t.Helper()
	var msg string
	func() {
		defer func() {
			r := recover()
			if r == nil {
				return
			}
			if _, ok := r.(fatalStop); !ok {
				panic(r)
			}
			msg = tb.lastFatal()
		}()
		fn()
	}()
	return msg
}

// TestEventuallyReportsLastState checks that a failed wait reports the last
// state observed before the deadline, not the state of the final post-deadline
// call.
func TestEventuallyReportsLastState(t *testing.T) {
	tb := &recordTB{}
	bound := 120 * time.Millisecond
	deadline := time.Now().Add(bound)

	var realCalls, lateCalls int
	start := time.Now()
	msg := captureFatal(t, tb, func() {
		Eventually(tb, bound, "example condition", func() (bool, string) {
			if time.Now().Before(deadline) {
				realCalls++
				return false, fmt.Sprintf("state-%d", realCalls)
			}
			lateCalls++
			return false, "no time left in the wait"
		})
	})
	elapsed := time.Since(start)

	if msg == "" {
		t.Fatal("Eventually returned without reporting a failure")
	}
	if lateCalls != 1 {
		t.Errorf("the condition ran %d times after the deadline, want the final observation call", lateCalls)
	}
	last := fmt.Sprintf("state-%d", realCalls)
	if !strings.Contains(msg, last) {
		t.Fatalf("failure message %q does not contain the last state %q", msg, last)
	}
	if strings.Contains(msg, "no time left") {
		t.Errorf("failure message %q reports the post-deadline state", msg)
	}
	if elapsed < bound {
		t.Fatalf("Eventually returned after %s, want at least the bound %s", elapsed, bound)
	}
}

// TestEventuallyObservesSuccessJustBeforeTheDeadline checks that a condition
// that first succeeds a few milliseconds before the deadline is still seen.
func TestEventuallyObservesSuccessJustBeforeTheDeadline(t *testing.T) {
	tb := &recordTB{}
	bound := 120 * time.Millisecond
	target := time.Now().Add(bound - 5*time.Millisecond)

	msg := captureFatal(t, tb, func() {
		Eventually(tb, bound, "example condition", func() (bool, string) {
			if !time.Now().Before(target) {
				return true, ""
			}
			return false, "not yet"
		})
	})

	if msg != "" {
		t.Fatalf("Eventually reported %q, want the final observation to succeed", msg)
	}
}

// TestEventuallyPollObservesSuccessJustBeforeTheDeadline checks that the shared
// wait loop keeps its final observation when a condition first succeeds just
// before the deadline.
func TestEventuallyPollObservesSuccessJustBeforeTheDeadline(t *testing.T) {
	tb := &recordTB{}
	bound := 120 * time.Millisecond
	deadline := time.Now().Add(bound)
	target := deadline.Add(-5 * time.Millisecond)

	msg := captureFatal(t, tb, func() {
		// Call eventuallyUntil directly: EventuallyPoll lifts a short bound to
		// ToolCallWaitBound, so the target would sit well inside the wait and
		// the last-observation boundary would never be reached.
		eventuallyUntil(tb, deadline, bound, "example condition", func(deadline time.Time) (bool, string) {
			if !time.Now().Before(target) {
				return true, ""
			}
			return false, "not yet"
		})
	})

	if msg != "" {
		t.Fatalf("EventuallyPoll reported %q, want the final observation to succeed", msg)
	}
}

// TestEventuallyReportsStateFromLastCallWithTimeLeft checks that a post-deadline
// call's "no time left" state does not replace the last state observed while
// the wait had time.
func TestEventuallyReportsStateFromLastCallWithTimeLeft(t *testing.T) {
	tb := &recordTB{}
	bound := 120 * time.Millisecond
	deadline := time.Now().Add(bound)

	var realCalls, lateCalls int
	msg := captureFatal(t, tb, func() {
		eventuallyUntil(tb, deadline, bound, "example condition", func(deadline time.Time) (bool, string) {
			if time.Now().Before(deadline) {
				realCalls++
				return false, fmt.Sprintf("observed-%d", realCalls)
			}
			lateCalls++
			return false, "no time left in the wait"
		})
	})

	if msg == "" {
		t.Fatal("Eventually returned without reporting a failure")
	}
	if lateCalls != 1 {
		t.Errorf("the condition ran %d times after the deadline, want the final observation call", lateCalls)
	}
	last := fmt.Sprintf("observed-%d", realCalls)
	if !strings.Contains(msg, last) {
		t.Errorf("failure message %q does not contain the last state %q", msg, last)
	}
	if strings.Contains(msg, "no time left") {
		t.Errorf("failure message %q reports the post-deadline state", msg)
	}
}

// TestEventuallyPollLiftsBoundAndPassesDeadline checks that the wait bound is
// lifted to ToolCallWaitBound and every call receives the same deadline.
func TestEventuallyPollLiftsBoundAndPassesDeadline(t *testing.T) {
	tb := &recordTB{}
	var deadlines []time.Time
	EventuallyPoll(tb, time.Second, "example condition", func(deadline time.Time) (bool, string) {
		deadlines = append(deadlines, deadline)
		if len(deadlines) < 2 {
			return false, "state-1"
		}
		return true, ""
	})

	if len(deadlines) != 2 {
		t.Fatalf("condition ran %d times, want two", len(deadlines))
	}
	if !deadlines[0].Equal(deadlines[1]) {
		t.Fatalf("condition received deadlines %s then %s, want one deadline", deadlines[0], deadlines[1])
	}
	if left := time.Until(deadlines[0]); left < ToolCallWaitBound-time.Second || left > ToolCallWaitBound {
		t.Fatalf("the deadline leaves %s, want the lifted bound %s", left, ToolCallWaitBound)
	}
	if len(tb.fatals) != 0 {
		t.Fatalf("EventuallyPoll failed: %v", tb.fatals)
	}
}

// TestEventuallyPollKeepsLongerBound checks that a caller bound longer than
// ToolCallWaitBound is kept.
func TestEventuallyPollKeepsLongerBound(t *testing.T) {
	tb := &recordTB{}
	bound := ToolCallWaitBound + 2*time.Second
	var deadline time.Time
	EventuallyPoll(tb, bound, "example condition", func(d time.Time) (bool, string) {
		deadline = d
		return true, ""
	})

	if left := time.Until(deadline); left < bound-time.Second || left > bound {
		t.Fatalf("the deadline leaves %s, want the caller's bound %s", left, bound)
	}
	if len(tb.fatals) != 0 {
		t.Fatalf("EventuallyPoll failed: %v", tb.fatals)
	}
}

// TestConsistentlyFailsOnFirstViolation checks that a violation ends the window
// at once with the state that failed.
func TestConsistentlyFailsOnFirstViolation(t *testing.T) {
	tb := &recordTB{}
	window := 2 * time.Second

	calls := 0
	start := time.Now()
	msg := captureFatal(t, tb, func() {
		Consistently(tb, window, "steady condition", func() (bool, string) {
			calls++
			return calls != 3, fmt.Sprintf("state-%d", calls)
		})
	})
	elapsed := time.Since(start)

	if msg == "" {
		t.Fatal("Consistently returned without reporting a failure")
	}
	if calls != 3 {
		t.Fatalf("condition ran %d times, want it to stop at the third violation", calls)
	}
	if !strings.Contains(msg, "state-3") {
		t.Fatalf("failure message %q does not contain the state at the violation", msg)
	}
	if elapsed >= window {
		t.Fatalf("Consistently returned after %s, want less than the window %s", elapsed, window)
	}
}

type blockingBackend struct {
	mu    sync.Mutex
	calls int
}

func (b *blockingBackend) callErrWithin(bound time.Duration, tool string, args map[string]any) (Result, error) {
	b.mu.Lock()
	b.calls++
	b.mu.Unlock()
	if bound > 0 {
		time.Sleep(bound)
	}
	return Result{}, context.DeadlineExceeded
}

func (b *blockingBackend) callCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

func (b *blockingBackend) mcpLogTail(int) (string, bool)      { return "", false }
func (b *blockingBackend) logBrokerStack(testing.TB) string   { return "" }
func (b *blockingBackend) tools(t testing.TB) []node.ToolInfo { return nil }
func (b *blockingBackend) stopMCP(t testing.TB, sig syscall.Signal) int {
	return 0
}
func (b *blockingBackend) restartMCP(t testing.TB)             {}
func (b *blockingBackend) startExtraMCP(t testing.TB)          {}
func (b *blockingBackend) inboxConnections(t testing.TB) int   { return 0 }
func (b *blockingBackend) fake() FakeSession                   { return nil }
func (b *blockingBackend) takeFakeErrors(t testing.TB) []error { return nil }
func (b *blockingBackend) end() error                          { return nil }

// TestConsistentlyPollFailsWhenASampleIsCutByItsCallBound checks that a sample
// cut off by its own call bound, shorter than pollBound, is reported as a stall
// and ends the window at once.
func TestConsistentlyPollFailsWhenASampleIsCutByItsCallBound(t *testing.T) {
	backend := &blockingBackend{}
	session := &Session{name: "stalled", impl: backend}
	tb := &recordTB{}
	const (
		window    = 3 * time.Second
		callBound = 50 * time.Millisecond
		pollBound = time.Second
	)

	start := time.Now()
	msg := captureFatal(t, tb, func() {
		ConsistentlyPoll(tb, window, pollBound, "the stalled window", nil, func(deadline time.Time) Observation {
			_, err := session.PollCallWithin(tb, deadline, callBound, "list_topics", nil)
			return Unobservable(err.Error())
		})
	})
	elapsed := time.Since(start)

	if msg == "" {
		t.Fatal("ConsistentlyPoll returned without reporting the stalled sample")
	}
	for _, want := range []string{"could not be observed", "1 sample stalled", "first stall", "no stall hook was given"} {
		if !strings.Contains(msg, want) {
			t.Errorf("failure %q does not contain %q", msg, want)
		}
	}
	if elapsed > time.Second {
		t.Fatalf("the window ran for %s after the stalled sample; it must fail at once", elapsed)
	}
	if calls := backend.callCount(); calls != 1 {
		t.Fatalf("the window made %d calls, want one; it must stop at the first stall", calls)
	}
	if logged := strings.Join(tb.logs, "\n"); !strings.Contains(logged, "=== harness STALL: the stalled window") {
		t.Errorf("the STALL marker was not logged:\n%s", logged)
	}
}

// TestTopicTimeoutStateMarksTopicRPCError checks that the product's topic RPC
// timeout text is reported as a stalled sample and an unrelated error is not.
func TestTopicTimeoutStateMarksTopicRPCError(t *testing.T) {
	const text = "list_topics failed: the Broker did not answer: context deadline exceeded (code: internal)"
	state := TopicTimeoutState(text)
	if state == "" {
		t.Fatalf("TopicTimeoutState(%q) returned no state", text)
	}
	if obs := Unobservable(state); !obs.Stalled {
		t.Fatalf("Unobservable(%q) did not mark the sample stalled: %+v", state, obs)
	}
	if other := TopicTimeoutState("list_agents failed: not_registered"); other != "" {
		t.Fatalf("TopicTimeoutState marked an unrelated error: %q", other)
	}
}

// TestConfigYAMLMerge checks that ConfigYAML merges a later fragment over the
// profile and is deterministic.
func TestConfigYAMLMerge(t *testing.T) {
	merged := ConfigYAML(FastTimers, "send:\n  request_timeout: 10s\n")

	var cfg struct {
		Link struct {
			ReconnectMin string `yaml:"reconnect_min"`
			ReconnectMax string `yaml:"reconnect_max"`
			DialTimeout  string `yaml:"dial_timeout"`
		} `yaml:"link"`
		Send struct {
			LinkBackoff    string `yaml:"link_backoff"`
			RequestTimeout string `yaml:"request_timeout"`
		} `yaml:"send"`
		Broker struct {
			AddressCheckInterval string `yaml:"address_check_interval"`
		} `yaml:"broker"`
	}
	if err := yaml.Unmarshal([]byte(merged), &cfg); err != nil {
		t.Fatalf("merged configuration is not valid YAML: %v\n%s", err, merged)
	}

	if cfg.Send.RequestTimeout != "10s" {
		t.Fatalf("send.request_timeout = %q, want the later fragment to win with %q", cfg.Send.RequestTimeout, "10s")
	}
	if cfg.Link.ReconnectMin != "100ms" {
		t.Fatalf("link.reconnect_min = %q, want %q", cfg.Link.ReconnectMin, "100ms")
	}
	if cfg.Link.ReconnectMax != "400ms" {
		t.Fatalf("link.reconnect_max = %q, want %q", cfg.Link.ReconnectMax, "400ms")
	}
	if cfg.Link.DialTimeout != FastDialTimeout.String() {
		t.Fatalf("link.dial_timeout = %q, want %q", cfg.Link.DialTimeout, FastDialTimeout)
	}

	var profile struct {
		Send struct {
			RequestTimeout string `yaml:"request_timeout"`
		} `yaml:"send"`
	}
	if err := yaml.Unmarshal([]byte(ConfigYAML(FastTimers)), &profile); err != nil {
		t.Fatalf("FastTimers is not valid YAML: %v\n%s", err, FastTimers)
	}
	if profile.Send.RequestTimeout != FastRequestTimeout.String() {
		t.Fatalf("FastTimers send.request_timeout = %q, want %q", profile.Send.RequestTimeout, FastRequestTimeout)
	}
	if cfg.Send.LinkBackoff != "100ms" {
		t.Fatalf("send.link_backoff = %q, want %q", cfg.Send.LinkBackoff, "100ms")
	}
	if cfg.Broker.AddressCheckInterval != "500ms" {
		t.Fatalf("broker.address_check_interval = %q, want %q", cfg.Broker.AddressCheckInterval, "500ms")
	}

	if merged != ConfigYAML(FastTimers, "send:\n  request_timeout: 10s\n") {
		t.Fatalf("ConfigYAML is not deterministic:\n%s", merged)
	}
}

type startupBackend struct {
	startingCalls int
	calls         []string
	transportErr  error
}

func (b *startupBackend) callErrWithin(_ time.Duration, tool string, args map[string]any) (Result, error) {
	b.calls = append(b.calls, tool)
	if b.transportErr != nil {
		return Result{}, b.transportErr
	}
	if b.startingCalls > 0 {
		b.startingCalls--
		return Result{ToolResult: node.ToolResult{
			IsError: true,
			Text:    fmt.Sprintf("%s failed: the connection to the Broker is still being set up; retry shortly (code: internal)", tool),
		}}, nil
	}
	return Result{}, nil
}

func (b *startupBackend) mcpLogTail(int) (string, bool) { return "", false }

func (b *startupBackend) logBrokerStack(testing.TB) string { return "" }

func (b *startupBackend) tools(t testing.TB) []node.ToolInfo { return nil }

func (b *startupBackend) stopMCP(t testing.TB, sig syscall.Signal) int { return 0 }

func (b *startupBackend) restartMCP(t testing.TB) {}

func (b *startupBackend) startExtraMCP(t testing.TB) {}

func (b *startupBackend) inboxConnections(t testing.TB) int { return 0 }

func (b *startupBackend) fake() FakeSession { return nil }

func (b *startupBackend) takeFakeErrors(t testing.TB) []error { return nil }

func (b *startupBackend) end() error { return nil }

// TestWaitConnectedWaitsOutStarting checks that WaitConnected polls past the MCP
// server's starting error until a management call succeeds.
func TestWaitConnectedWaitsOutStarting(t *testing.T) {
	backend := &startupBackend{startingCalls: 3}
	session := &Session{name: "operator", impl: backend}

	first := session.Call(t, "create_pairing_key", nil)
	if !first.IsError || !strings.Contains(first.Text, "still being set up") {
		t.Fatalf("the first management call did not reproduce the starting race: %+v", first)
	}

	WaitConnected(t, session, time.Second)

	for _, tool := range backend.calls[1:] {
		if tool != "list_brokers" {
			t.Fatalf("WaitConnected called %s, want only the identity-free list_brokers", tool)
		}
	}

	if got := session.Call(t, "create_pairing_key", nil); got.IsError {
		t.Fatalf("create_pairing_key still failed after WaitConnected: %s", got.Text)
	}
}

// TestWaitConnectedAgainstStartingMCPServer reproduces the starting window with
// the real MCP server and checks that WaitConnected waits until Hello completes.
func TestWaitConnectedAgainstStartingMCPServer(t *testing.T) {
	release := make(chan struct{})
	socket := fakelocal.SocketPath(t)
	fakelocal.Start(t, socket, fakelocal.Handlers{
		Hello: func(ctx context.Context, req *localpb.HelloRequest) (*localpb.HelloResponse, error) {
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return &localpb.HelloResponse{ProtocolVersion: localpb.ProtocolVersion}, nil
		},
		ListBrokers: func(context.Context, *localpb.ListBrokersRequest) (*localpb.ListBrokersResponse, error) {
			return &localpb.ListBrokersResponse{Local: &localpb.LocalBrokerState{}}, nil
		},
		CreatePairingKey: func(context.Context, *localpb.CreatePairingKeyRequest) (*localpb.CreatePairingKeyResponse, error) {
			return &localpb.CreatePairingKeyResponse{Key: "test-key"}, nil
		},
	})

	host := NewLocalHost(t, LocalOptions{Name: "starting"})
	session := host.StartSession(t, SessionSpec{
		Name:        "operator",
		StartMCP:    true,
		NoSocketEnv: true,
		Env:         map[string]string{"PEERS_TEST_SOCKET": socket},
	})
	defer session.End(t)

	first := session.Call(t, "create_pairing_key", nil)
	if !first.IsError || !strings.Contains(first.Text, "still being set up") {
		t.Fatalf("the first management call did not reproduce the starting race: %+v", first)
	}

	close(release)
	WaitConnected(t, session, 3*time.Second)

	if got := session.Call(t, "create_pairing_key", nil); got.IsError {
		t.Fatalf("create_pairing_key still failed after WaitConnected: %s", got.Text)
	}
}

// TestCallUntilOKDoesNotRetryTransportError checks that a transport error fails
// the test after exactly one attempt.
func TestCallUntilOKDoesNotRetryTransportError(t *testing.T) {
	backend := &startupBackend{transportErr: errors.New("connection reset")}
	session := &Session{name: "side-effect", impl: backend}
	tb := &recordTB{}

	msg := captureFatal(t, tb, func() {
		session.CallUntilOK(tb, 500*time.Millisecond, "send_message", nil)
	})

	if msg == "" {
		t.Fatal("CallUntilOK returned without reporting the transport error")
	}
	if !strings.Contains(msg, "connection reset") {
		t.Fatalf("failure %q does not name the transport error", msg)
	}
	if len(backend.calls) != 1 {
		t.Fatalf("made %d calls, want exactly one; a transport error must not be retried", len(backend.calls))
	}
}

// TestCallUntilOKRetriesToolError checks that a tool error result is retried
// until the tool succeeds.
func TestCallUntilOKRetriesToolError(t *testing.T) {
	backend := &startupBackend{startingCalls: 2}
	session := &Session{name: "side-effect", impl: backend}

	result := session.CallUntilOK(t, 2*time.Second, "create_pairing_key", nil)
	if result.IsError {
		t.Fatalf("CallUntilOK gave up on a retryable tool error: %s", result.Text)
	}
	if len(backend.calls) != 3 {
		t.Fatalf("made %d calls, want three (two tool errors then success)", len(backend.calls))
	}
}
