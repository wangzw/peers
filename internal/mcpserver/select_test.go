package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/testkit/fakelocal"
	"github.com/wangzw/peers/internal/testkit/socktest"
)

func recordingAfter() (func(time.Duration) <-chan time.Time, func() []time.Duration) {
	var waits []time.Duration
	after := func(d time.Duration) <-chan time.Time {
		waits = append(waits, d)
		ch := make(chan time.Time)
		close(ch)
		return ch
	}
	return after, func() []time.Duration {
		return append([]time.Duration(nil), waits...)
	}
}

func wantWaits(t *testing.T, got []time.Duration, want ...time.Duration) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("waits = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("waits = %v, want %v", got, want)
		}
	}
}

// TestSelectIgnoresSocketEnvironment checks that AGENT_PEER_SOCKET does not
// influence selection and the account's user Broker wins.
func TestSelectIgnoresSocketEnvironment(t *testing.T) {
	envPath := fakelocal.SocketPath(t)
	fakelocal.Start(t, envPath, fakelocal.Handlers{})
	userPath := fakelocal.SocketPath(t)
	fakelocal.Start(t, userPath, fakelocal.Handlers{})
	systemPath := fakelocal.SocketPath(t)
	fakelocal.Start(t, systemPath, fakelocal.Handlers{})
	t.Setenv("AGENT_PEER_SOCKET", envPath)

	s := &selector{
		userSocket:   userPath,
		systemSocket: systemPath,
		probe:        inspectSocket,
	}

	got, err := s.Find(context.Background())
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if got.Kind != kindUser || got.Path != userPath {
		t.Fatalf("Find = %+v, want {%s kindUser} despite AGENT_PEER_SOCKET naming %s", got, userPath, envPath)
	}
}

// TestSelectUserBeforeSystem checks that the user Broker wins over the system
// one.
func TestSelectUserBeforeSystem(t *testing.T) {
	userPath := fakelocal.SocketPath(t)
	fakelocal.Start(t, userPath, fakelocal.Handlers{})
	systemPath := fakelocal.SocketPath(t)
	fakelocal.Start(t, systemPath, fakelocal.Handlers{})

	s := &selector{
		getenv:       func(string) string { return "" },
		userSocket:   userPath,
		systemSocket: systemPath,
		probe:        inspectSocket,
	}

	got, err := s.Find(context.Background())
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if got.Kind != kindUser || got.Path != userPath {
		t.Fatalf("Find = %+v, want {%s kindUser}", got, userPath)
	}
}

// TestSelectSystemOnly checks that the system Broker is selected when no user
// Broker answers.
func TestSelectSystemOnly(t *testing.T) {
	systemPath := fakelocal.SocketPath(t)
	fakelocal.Start(t, systemPath, fakelocal.Handlers{})

	old := peerUID
	peerUID = func(net.Conn) (uint32, error) { return 0, nil }
	t.Cleanup(func() { peerUID = old })

	s := &selector{
		getenv:       func(string) string { return "" },
		userSocket:   socktest.Path(t, "broker.sock"),
		systemSocket: systemPath,
		probe:        inspectSocket,
	}

	got, err := s.Find(context.Background())
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if got.Kind != kindSystem || got.Path != systemPath {
		t.Fatalf("Find = %+v, want {%s kindSystem}", got, systemPath)
	}
}

// TestFindSystemDeniedReturnsNoBroker checks that a system socket this account
// may not use is not retried and selection finds no Broker.
func TestFindSystemDeniedReturnsNoBroker(t *testing.T) {
	userPath := socktest.Path(t, "user.sock")
	systemPath := socktest.Path(t, "system.sock")
	after, waits := recordingAfter()

	s := &selector{
		getenv:       func(string) string { return "" },
		userSocket:   userPath,
		systemSocket: systemPath,
		probe: func(_ context.Context, path string) probeInfo {
			if path == systemPath {
				return probeInfo{Outcome: probeDenied}
			}
			return probeInfo{Outcome: probeMissing}
		},
		after: after,
	}

	_, err := s.Find(context.Background())
	if !errors.Is(err, errNoBroker) {
		t.Fatalf("Find error = %v, want errNoBroker", err)
	}
	if w := waits(); len(w) != 0 {
		t.Fatalf("waits = %v, want none for a denied system socket", w)
	}
}

// TestSelectRejectsForeignUserSocket checks that a user socket served by another
// account is not selected.
func TestSelectRejectsForeignUserSocket(t *testing.T) {
	userPath := fakelocal.SocketPath(t)
	fakelocal.Start(t, userPath, fakelocal.Handlers{})

	foreign := uint32(os.Geteuid()) + 1
	old := peerUID
	peerUID = func(net.Conn) (uint32, error) { return foreign, nil }
	t.Cleanup(func() { peerUID = old })

	s := &selector{
		getenv:       func(string) string { return "" },
		userSocket:   userPath,
		systemSocket: socktest.Path(t, "system.sock"),
		probe:        inspectSocket,
	}

	got, err := s.Find(context.Background())
	if err == nil {
		t.Fatalf("Find = %+v, want an error for a socket served by uid %d", got, foreign)
	}
	if got.Kind == kindUser {
		t.Fatalf("Find = %+v, want not kindUser", got)
	}
	wantUID := strconv.FormatUint(uint64(foreign), 10)
	if !strings.Contains(err.Error(), userPath) || !strings.Contains(err.Error(), wantUID) {
		t.Fatalf("error = %q, want it to name %s and uid %s", err, userPath, wantUID)
	}
}

// TestSelectRejectsRootServedUserSocket checks that a user socket served by
// root, rather than by the account, is rejected.
func TestSelectRejectsRootServedUserSocket(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root's own user socket is served by root")
	}
	userPath := fakelocal.SocketPath(t)
	fakelocal.Start(t, userPath, fakelocal.Handlers{})

	old := peerUID
	peerUID = func(net.Conn) (uint32, error) { return 0, nil }
	t.Cleanup(func() { peerUID = old })

	s := &selector{
		getenv:       func(string) string { return "" },
		userSocket:   userPath,
		systemSocket: socktest.Path(t, "system.sock"),
		probe:        inspectSocket,
	}

	_, err := s.Find(context.Background())
	if err == nil {
		t.Fatal("Find accepted a user socket served by root")
	}
	if !strings.Contains(err.Error(), userPath) || !strings.Contains(err.Error(), "uid 0") {
		t.Fatalf("error = %q, want it to name %s and uid 0", err, userPath)
	}
}

// TestFindNonRootSystemSocketReportsMismatch checks that a system socket served
// by a non-root account reports the peer mismatch rather than no Broker.
func TestFindNonRootSystemSocketReportsMismatch(t *testing.T) {
	userPath := fakelocal.SocketPath(t)
	systemPath := socktest.Path(t, "system.sock")

	s := &selector{
		getenv:       func(string) string { return "" },
		userSocket:   userPath,
		systemSocket: systemPath,
		probe: func(ctx context.Context, path string) probeInfo {
			if path == systemPath {
				return probeInfo{Outcome: probeOK, PeerUID: 1, PeerKnown: true}
			}
			return inspectSocket(ctx, path)
		},
	}

	_, err := s.Find(context.Background())
	if err == nil {
		t.Fatal("Find accepted a system socket served by a non-root account")
	}
	want := fmt.Sprintf("the system Broker socket %s is served by uid 1, not root", systemPath)
	if err.Error() != want {
		t.Fatalf("error = %q, want %q", err, want)
	}
	if errors.Is(err, errNoBroker) {
		t.Fatalf("error = %v, want the peer mismatch rather than errNoBroker", err)
	}
}

// TestFindNoBroker checks that no Broker at all is an error whose text names
// both ways to start one.
func TestFindNoBroker(t *testing.T) {
	s := &selector{
		getenv:       func(string) string { return "" },
		userSocket:   socktest.Path(t, "user.sock"),
		systemSocket: socktest.Path(t, "system.sock"),
		probe:        inspectSocket,
	}

	_, err := s.Find(context.Background())
	if !errors.Is(err, errNoBroker) {
		t.Fatalf("Find error = %v, want errNoBroker", err)
	}
	for _, want := range []string{"ask the administrator", "peers service install", "peers service run", "code 6"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %q, want it to contain %q", err, want)
		}
	}
}

// TestSelectRootSharedSocketPath checks that when the user and system sockets
// are one path only one probe runs and the target is the user socket.
func TestSelectRootSharedSocketPath(t *testing.T) {
	same := socktest.Path(t, "broker.sock")
	probes := 0

	s := &selector{
		getenv:       func(string) string { return "" },
		userSocket:   same,
		systemSocket: same,
		probe: func(context.Context, string) probeInfo {
			probes++
			return probeInfo{Outcome: probeOK}
		},
	}

	got, err := s.Find(context.Background())
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if got.Kind != kindUser || got.Path != same {
		t.Fatalf("Find = %+v, want {%s kindUser}", got, same)
	}
	if probes != 1 {
		t.Fatalf("probed %d times, want 1", probes)
	}
}

// TestFindSystemRefusedRetriesThenNoBroker checks that a refused system socket
// is retried for the full deadline and then reported as no Broker.
func TestFindSystemRefusedRetriesThenNoBroker(t *testing.T) {
	userPath := socktest.Path(t, "user.sock")
	systemPath := socktest.Path(t, "system.sock")
	after, waits := recordingAfter()

	s := &selector{
		getenv:       func(string) string { return "" },
		userSocket:   userPath,
		systemSocket: systemPath,
		probe: func(_ context.Context, path string) probeInfo {
			if path == systemPath {
				return probeInfo{Outcome: probeRefused}
			}
			return probeInfo{Outcome: probeMissing}
		},
		after: after,
	}

	_, err := s.Find(context.Background())
	if !errors.Is(err, errNoBroker) {
		t.Fatalf("Find error = %v, want errNoBroker", err)
	}
	w := waits()
	wantWaits(t, w,
		250*time.Millisecond,
		500*time.Millisecond,
		time.Second,
		2*time.Second,
		4*time.Second,
		2250*time.Millisecond,
	)
	var total time.Duration
	for _, d := range w {
		total += d
	}
	if total != SystemRetryDeadline {
		t.Fatalf("waits total %v, want %v", total, SystemRetryDeadline)
	}
}

// TestSelectSystemRefusedThenUp checks that a system socket refusing connections
// is selected once it starts answering.
func TestSelectSystemRefusedThenUp(t *testing.T) {
	userPath := socktest.Path(t, "user.sock")
	systemPath := socktest.Path(t, "system.sock")
	systemProbes := 0
	after, waits := recordingAfter()

	s := &selector{
		getenv:       func(string) string { return "" },
		userSocket:   userPath,
		systemSocket: systemPath,
		probe: func(_ context.Context, path string) probeInfo {
			if path != systemPath {
				return probeInfo{Outcome: probeMissing}
			}
			systemProbes++
			if systemProbes <= 2 {
				return probeInfo{Outcome: probeRefused}
			}
			return probeInfo{Outcome: probeOK}
		},
		after: after,
	}

	got, err := s.Find(context.Background())
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if got.Kind != kindSystem || got.Path != systemPath {
		t.Fatalf("Find = %+v, want {%s kindSystem}", got, systemPath)
	}
	wantWaits(t, waits(), 250*time.Millisecond, 500*time.Millisecond)
}

// TestFindSystemRefusedRetries checks that a refused system socket is retried
// with the backoff schedule plus the remaining wait, and then reported as no
// Broker.
func TestFindSystemRefusedRetries(t *testing.T) {
	systemPath := socktest.Path(t, "system.sock")
	after, waits := recordingAfter()

	s := &selector{
		getenv:       func(string) string { return "" },
		userSocket:   socktest.Path(t, "user.sock"),
		systemSocket: systemPath,
		probe: func(_ context.Context, path string) probeInfo {
			if path == systemPath {
				return probeInfo{Outcome: probeRefused}
			}
			return probeInfo{Outcome: probeMissing}
		},
		after: after,
	}

	_, err := s.Find(context.Background())
	if !errors.Is(err, errNoBroker) {
		t.Fatalf("Find error = %v, want errNoBroker", err)
	}
	wantWaits(t, waits(),
		250*time.Millisecond,
		500*time.Millisecond,
		time.Second,
		2*time.Second,
		4*time.Second,
		2250*time.Millisecond,
	)
}

// TestFindSystemRefusedThenWrongPeerReportsMismatch checks that a peer mismatch
// during the retry ends it at once with the peer mismatch error.
func TestFindSystemRefusedThenWrongPeerReportsMismatch(t *testing.T) {
	userPath := socktest.Path(t, "user.sock")
	systemPath := socktest.Path(t, "system.sock")
	const foreign = uint32(4242)
	after, waits := recordingAfter()

	systemProbes := 0
	s := &selector{
		getenv:       func(string) string { return "" },
		userSocket:   userPath,
		systemSocket: systemPath,
		probe: func(_ context.Context, path string) probeInfo {
			if path != systemPath {
				return probeInfo{Outcome: probeMissing}
			}
			systemProbes++
			if systemProbes <= 2 {
				return probeInfo{Outcome: probeRefused}
			}
			return probeInfo{Outcome: probeWrongPeer, PeerUID: foreign, PeerKnown: true}
		},
		after: after,
	}

	_, err := s.findForLevel(context.Background(), true, true)
	if err == nil {
		t.Fatal("findForLevel returned nil error for a system socket served by another account")
	}
	want := fmt.Sprintf("the system Broker socket %s is served by uid %d, not root", systemPath, foreign)
	if err.Error() != want {
		t.Fatalf("error = %q, want %q", err, want)
	}
	if errors.Is(err, errNoSystemBroker) {
		t.Fatalf("error = %v, want the peer mismatch rather than a missing system Broker", err)
	}
	wantWaits(t, waits(), 250*time.Millisecond)
	if systemProbes != 3 {
		t.Fatalf("system socket probed %d times, want 3 (level, retry start, one retry)", systemProbes)
	}
}

// TestFindAtLevelSelectsOnlyThatLevel checks that each explicit level picks its
// own socket even when both Brokers answer.
func TestFindAtLevelSelectsOnlyThatLevel(t *testing.T) {
	userPath := socktest.Path(t, "user.sock")
	systemPath := socktest.Path(t, "system.sock")

	s := &selector{
		getenv:       func(string) string { return "" },
		userSocket:   userPath,
		systemSocket: systemPath,
		probe: func(_ context.Context, path string) probeInfo {
			return probeInfo{Outcome: probeOK}
		},
	}

	userTarget, found, err := s.findAtLevel(context.Background(), false)
	if err != nil || !found {
		t.Fatalf("findAtLevel(user) = (%+v, %v, %v), want the user socket", userTarget, found, err)
	}
	if userTarget.Kind != kindUser || userTarget.Path != userPath {
		t.Fatalf("findAtLevel(user) = %+v, want {%s kindUser}", userTarget, userPath)
	}

	systemTarget, found, err := s.findAtLevel(context.Background(), true)
	if err != nil || !found {
		t.Fatalf("findAtLevel(system) = (%+v, %v, %v), want the system socket", systemTarget, found, err)
	}
	if systemTarget.Kind != kindSystem || systemTarget.Path != systemPath {
		t.Fatalf("findAtLevel(system) = %+v, want {%s kindSystem}", systemTarget, systemPath)
	}
}

// TestFindAtLevelUserDoesNotFallThrough checks that --user does not fall through
// to a running system Broker.
func TestFindAtLevelUserDoesNotFallThrough(t *testing.T) {
	userPath := socktest.Path(t, "user.sock")
	systemPath := socktest.Path(t, "system.sock")

	s := &selector{
		getenv:       func(string) string { return "" },
		userSocket:   userPath,
		systemSocket: systemPath,
		probe: func(_ context.Context, path string) probeInfo {
			if path == systemPath {
				return probeInfo{Outcome: probeOK}
			}
			return probeInfo{Outcome: probeMissing}
		},
	}

	got, found, err := s.findAtLevel(context.Background(), false)
	if err != nil {
		t.Fatalf("findAtLevel(user): %v", err)
	}
	if found {
		t.Fatalf("findAtLevel(user) = %+v, want no target despite the running system Broker", got)
	}
}

// TestFindForLevelExplainsAnEmptySelection checks the error text an empty
// selection returns for each level.
func TestFindForLevelExplainsAnEmptySelection(t *testing.T) {
	userPath := socktest.Path(t, "user.sock")
	systemPath := socktest.Path(t, "system.sock")

	cases := []struct {
		name          string
		system        bool
		explicit      bool
		root          bool
		userOutcome   probeResult
		systemOutcome probeResult
		want          error
	}{
		{
			name: "named system", system: true, explicit: true,
			userOutcome: probeMissing, systemOutcome: probeMissing,
			want: errNoSystemBroker,
		},
		{
			name: "root default level", system: true, explicit: false, root: true,
			userOutcome: probeMissing, systemOutcome: probeMissing,
			want: errNoSystemBroker,
		},
		{
			name: "default level with the system Broker running", system: false, explicit: false,
			userOutcome: probeMissing, systemOutcome: probeOK,
			want: errNoUserBrokerSystemRunning,
		},
		{
			name: "named user", system: false, explicit: true,
			userOutcome: probeMissing, systemOutcome: probeOK,
			want: errNoBroker,
		},
		{
			name: "default level with nothing running", system: false, explicit: false,
			userOutcome: probeMissing, systemOutcome: probeMissing,
			want: errNoBroker,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			userSocket := userPath
			if tc.root {
				userSocket = systemPath
			}
			s := &selector{
				userSocket:   userSocket,
				systemSocket: systemPath,
				probe: func(_ context.Context, path string) probeInfo {
					if path == userSocket {
						return probeInfo{Outcome: tc.userOutcome}
					}
					return probeInfo{Outcome: tc.systemOutcome}
				},
			}
			_, err := s.findForLevel(context.Background(), tc.system, tc.explicit)
			if !errors.Is(err, tc.want) {
				t.Fatalf("findForLevel error = %v, want %v", err, tc.want)
			}
		})
	}
}

// TestFindForLevelReturnsTheSelectedSocket checks that the level-aware finder
// returns the selected socket.
func TestFindForLevelReturnsTheSelectedSocket(t *testing.T) {
	userPath := socktest.Path(t, "user.sock")
	systemPath := socktest.Path(t, "system.sock")

	s := &selector{
		userSocket:   userPath,
		systemSocket: systemPath,
		probe: func(_ context.Context, path string) probeInfo {
			if path == systemPath {
				return probeInfo{Outcome: probeOK}
			}
			return probeInfo{Outcome: probeMissing}
		},
	}
	got, err := s.findForLevel(context.Background(), true, true)
	if err != nil {
		t.Fatalf("findForLevel: %v", err)
	}
	if got != systemPath {
		t.Errorf("findForLevel = %q, want %q", got, systemPath)
	}
}

// TestFindAtLevelSystemDoesNotFallThrough checks that --system does not fall
// through to a running user Broker.
func TestFindAtLevelSystemDoesNotFallThrough(t *testing.T) {
	userPath := socktest.Path(t, "user.sock")
	systemPath := socktest.Path(t, "system.sock")

	s := &selector{
		getenv:       func(string) string { return "" },
		userSocket:   userPath,
		systemSocket: systemPath,
		probe: func(_ context.Context, path string) probeInfo {
			if path == userPath {
				return probeInfo{Outcome: probeOK}
			}
			return probeInfo{Outcome: probeMissing}
		},
	}

	got, found, err := s.findAtLevel(context.Background(), true)
	if err != nil {
		t.Fatalf("findAtLevel(system): %v", err)
	}
	if found {
		t.Fatalf("findAtLevel(system) = %+v, want no target despite the running user Broker", got)
	}
}
