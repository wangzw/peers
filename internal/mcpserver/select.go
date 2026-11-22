package mcpserver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// targetKind names which step of the selection order chose the Broker.
type targetKind int

const (
	// kindTest means the test build hook named the socket. A release build
	// never produces this kind.
	kindTest targetKind = iota
	// kindUser means the user Broker of this account was chosen.
	kindUser
	// kindSystem means the system Broker was chosen.
	kindSystem
)

// target is a Broker socket together with the rule that selected it.
type target struct {
	Path string
	Kind targetKind
}

// SystemRetryDeadline is how long step two keeps retrying a system
// socket that exists and may be connected to but has no process answering. It
// is exported so end-to-end tests can derive their wait bounds from it rather
// than repeat the number.
const SystemRetryDeadline = 10 * time.Second

// systemRetryWaits are the waits between probes of a refused system
// socket. The remaining time up to SystemRetryDeadline is waited last, so the
// total is exactly the deadline.
var systemRetryWaits = []time.Duration{
	250 * time.Millisecond,
	500 * time.Millisecond,
	1 * time.Second,
	2 * time.Second,
	4 * time.Second,
}

// selector applies the Broker selection order. The fields are seams for tests
// and for FindBroker, which supplies the production values.
type selector struct {
	getenv        func(string) string
	userSocket    string
	altUserSocket string
	systemSocket  string
	probe         func(ctx context.Context, path string) probeInfo
	after         func(time.Duration) <-chan time.Time
	// lockHeld reports whether a runtime directory's account lock is held by a
	// running Broker. It is a seam for tests; nil means accountLockHeld.
	lockHeld func(runtimeDir string) bool
}

// Find returns the first target of the selection order that holds. It never
// starts a Broker: when no socket answers it returns errNoBroker, whose text
// tells the model how to start one.
func (s *selector) Find(ctx context.Context) (target, error) {
	t, found, err := s.find(ctx)
	if err != nil {
		return target{}, err
	}
	if !found {
		return target{}, errNoBroker
	}
	return t, nil
}

// find runs the selection order. A test build may name a socket with its hook;
// a release build has no such step and always selects by account.
func (s *selector) find(ctx context.Context) (target, bool, error) {
	if t, found, err := s.findNamedSocket(ctx); err != nil || found {
		return t, found, err
	}

	// The user step may find the Broker at either runtime directory. A socket
	// served by another account is unusable, so it is neither selected nor
	// allowed to fall through to the system step.
	if t, found, err := s.findUserSocket(ctx); err != nil || found {
		return t, found, err
	}

	// For root the user and system sockets share one path, so the
	// probe above already covered the system Broker.
	if s.systemSocket != s.userSocket {
		ok, err := s.probeSystemWithRetry(ctx)
		if err != nil {
			return target{}, false, err
		}
		if ok {
			return target{Path: s.systemSocket, Kind: kindSystem}, true, nil
		}
	}
	return target{}, false, nil
}

// userSocketPaths returns every user socket the selection probes: the location
// the rule selects first, then the other candidate when it differs.
func (s *selector) userSocketPaths() []string {
	paths := []string{s.userSocket}
	if s.altUserSocket != "" && s.altUserSocket != s.userSocket {
		paths = append(paths, s.altUserSocket)
	}
	return paths
}

// findUserSocket probes every candidate user socket and returns the one
// selection uses. Only a socket that passes the peer check is usable. When more
// than one passes, the account lock says which one is the running Broker;
// otherwise the candidate the location rule named first is kept. A socket
// served by another account is reported when no candidate is usable, matching
// the error for a single taken path.
func (s *selector) findUserSocket(ctx context.Context) (target, bool, error) {
	var usable []string
	var wrongPath string
	var wrongUID uint32
	for _, path := range s.userSocketPaths() {
		r := s.probePath(ctx, path, kindUser)
		switch r.Outcome {
		case probeOK:
			usable = append(usable, path)
		case probeWrongPeer:
			if wrongPath == "" {
				wrongPath, wrongUID = path, r.PeerUID
			}
		}
	}
	if len(usable) == 0 {
		if wrongPath != "" {
			return target{}, false, peerMismatchError(wrongPath, kindUser, wrongUID)
		}
		return target{}, false, nil
	}
	path := usable[0]
	if len(usable) > 1 {
		path = s.liveUserSocket(usable)
	}
	return target{Path: path, Kind: kindUser}, true, nil
}

// liveUserSocket picks the socket whose runtime directory holds the account
// lock, when exactly one does. Two held locks should not happen, and no held
// lock leaves the first usable socket, which is the one the location rule
// selected.
func (s *selector) liveUserSocket(paths []string) string {
	held := make([]string, 0, len(paths))
	for _, path := range paths {
		if s.userLockHeld(filepath.Dir(path)) {
			held = append(held, path)
		}
	}
	if len(held) == 1 {
		return held[0]
	}
	return paths[0]
}

func (s *selector) userLockHeld(runtimeDir string) bool {
	if s.lockHeld != nil {
		return s.lockHeld(runtimeDir)
	}
	return accountLockHeld(runtimeDir)
}

// findAtLevel runs the selection order narrowed to one level. A test build may
// still name a socket with its hook; otherwise only that level's socket is
// probed, so --system and --user cannot fall through to each other. Commands
// that take --system or --user use it, because the design makes the selected
// level decide which Broker they talk to.
func (s *selector) findAtLevel(ctx context.Context, system bool) (target, bool, error) {
	if t, found, err := s.findNamedSocket(ctx); err != nil || found {
		return t, found, err
	}
	if !system {
		return s.findUserSocket(ctx)
	}

	// For root the user and system sockets share one path, so the
	// system probe is the only one needed.
	if s.systemSocket == s.userSocket {
		switch r := s.probePath(ctx, s.systemSocket, kindSystem); r.Outcome {
		case probeOK:
			return target{Path: s.systemSocket, Kind: kindSystem}, true, nil
		case probeWrongPeer:
			return target{}, false, peerMismatchError(s.systemSocket, kindSystem, r.PeerUID)
		}
		return target{}, false, nil
	}

	switch r := s.probePath(ctx, s.systemSocket, kindSystem); r.Outcome {
	case probeOK:
		return target{Path: s.systemSocket, Kind: kindSystem}, true, nil
	case probeWrongPeer:
		return target{}, false, peerMismatchError(s.systemSocket, kindSystem, r.PeerUID)
	case probeRefused:
		// A service manager may be restarting the Broker, so the refused
		// system socket is retried before selection gives up.
		ok, err := s.probeSystemWithRetry(ctx)
		if err != nil {
			return target{}, false, err
		}
		if ok {
			return target{Path: s.systemSocket, Kind: kindSystem}, true, nil
		}
	}
	return target{}, false, nil
}

// probeSystemWithRetry probes the system socket. A refused connection is
// retried for SystemRetryDeadline, because a service manager may be restarting
// the Broker; a socket the account may not use, a missing socket and any other
// failure are not retried. A socket answered by an account that may not serve
// the system Broker is the same peer mismatch the first probe reports: a
// restart is exactly when another account can take the socket, so the retry
// ends at once instead of running out the deadline and reporting a missing
// system Broker.
func (s *selector) probeSystemWithRetry(ctx context.Context) (bool, error) {
	switch r := s.probePath(ctx, s.systemSocket, kindSystem); r.Outcome {
	case probeOK:
		return true, nil
	case probeWrongPeer:
		return false, peerMismatchError(s.systemSocket, kindSystem, r.PeerUID)
	case probeRefused:
	default:
		return false, nil
	}

	var waited time.Duration
	for _, d := range systemRetryWaits {
		if err := s.wait(ctx, d); err != nil {
			return false, err
		}
		waited += d
		if ok, mismatch := s.probeSystemRetryStep(ctx); mismatch != nil || ok {
			return ok, mismatch
		}
	}
	if rest := SystemRetryDeadline - waited; rest > 0 {
		if err := s.wait(ctx, rest); err != nil {
			return false, err
		}
		if ok, mismatch := s.probeSystemRetryStep(ctx); mismatch != nil || ok {
			return ok, mismatch
		}
	}
	return false, nil
}

// probeSystemRetryStep probes the system socket once during a retry. A usable
// socket is reported as ok; a socket answered by another account ends the
// retry with the peer mismatch, which names the path and the answering
// account; a socket still refusing connections, and every other outcome, keep
// the retry going, as before.
func (s *selector) probeSystemRetryStep(ctx context.Context) (ok bool, mismatch error) {
	r := s.probePath(ctx, s.systemSocket, kindSystem)
	switch r.Outcome {
	case probeOK:
		return true, nil
	case probeWrongPeer:
		return false, peerMismatchError(s.systemSocket, kindSystem, r.PeerUID)
	}
	return false, nil
}

func (s *selector) wait(ctx context.Context, d time.Duration) error {
	select {
	case <-s.afterFunc()(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// probePath probes path and applies the peer rule of the selection step. The
// raw probe already rejects a peer that may not serve a Broker at all; this
// narrows it to the account that may serve this particular socket.
func (s *selector) probePath(ctx context.Context, path string, kind targetKind) probeInfo {
	var info probeInfo
	if s.probe == nil {
		info = inspectSocket(ctx, path)
	} else {
		info = s.probe(ctx, path)
	}
	if info.Outcome == probeOK && info.PeerKnown && !peerAllowed(kind, info.PeerUID) {
		info.Outcome = probeWrongPeer
	}
	return info
}

// peerAllowed reports whether uid may serve the Broker socket of kind: root for
// the system Broker, this account for the user one, and either for
// the socket named by the test build hook.
func peerAllowed(kind targetKind, uid uint32) bool {
	switch kind {
	case kindSystem:
		return uid == 0
	case kindTest:
		return peerMayServe(uid)
	default:
		return uid == uint32(os.Geteuid())
	}
}

// peerMismatch is the error for a socket served by an account that may not
// serve the Broker at its path. It is a type so that the connection loop can
// show the sentence as it stands: the text is already what the model should
// read, unlike the other selection errors that the loop wraps in its
// unavailable sentence.
type peerMismatch struct {
	path string
	kind targetKind
	uid  uint32
}

func (e *peerMismatch) Error() string {
	switch e.kind {
	case kindSystem:
		return fmt.Sprintf("the system Broker socket %s is served by uid %d, not root", e.path, e.uid)
	default:
		return fmt.Sprintf("the user Broker socket %s is served by uid %d, not this account (%d)",
			e.path, e.uid, os.Geteuid())
	}
}

// peerMismatchError names a user or system socket and the account
// that answered it. The text is shown to the model, so it must carry both
// facts. A test-named socket builds its own message in the test build.
func peerMismatchError(path string, kind targetKind, uid uint32) error {
	return &peerMismatch{path: path, kind: kind, uid: uid}
}

func (s *selector) afterFunc() func(time.Duration) <-chan time.Time {
	if s.after == nil {
		return time.After
	}
	return s.after
}

// productionSelector builds the selector with the paths and probes of the
// running process.
func productionSelector() *selector {
	sockets := userSocketCandidates(os.Geteuid())
	s := &selector{
		getenv:       os.Getenv,
		userSocket:   sockets[0],
		systemSocket: systemSocketPath(),
		probe:        inspectSocket,
		after:        time.After,
		lockHeld:     accountLockHeld,
	}
	if len(sockets) > 1 {
		s.altUserSocket = sockets[1]
	}
	return s
}

// FindBroker applies the Broker selection order and returns the socket path of
// the first step that holds. It never starts a Broker. The error text is
// suitable for a terminal user.
func FindBroker(ctx context.Context) (string, error) {
	t, err := productionSelector().Find(ctx)
	if err != nil {
		return "", err
	}
	return t.Path, nil
}

// FindBrokerForLevel returns the socket path of the running Broker at the
// selected level. It never starts a Broker and never falls through to the other
// level, so a command that names a level talks to that level or fails. explicit
// reports whether a level option was given rather than resolved from the
// calling account; it tells a defaulted user selection, which names --system when
// that Broker answers, from a named one.
func FindBrokerForLevel(ctx context.Context, system, explicit bool) (string, error) {
	return productionSelector().findForLevel(ctx, system, explicit)
}

// findForLevel selects a Broker at one level and builds the error for an empty
// selection. The system installation always reports the system Broker as
// unavailable, whether --system named it or root resolved it from the account.
// The default user selection falls back to the account's user Broker; when
// only the system Broker is running, the error names --system as the
// option that reaches it.
func (s *selector) findForLevel(ctx context.Context, system, explicit bool) (string, error) {
	t, found, err := s.findAtLevel(ctx, system)
	if err != nil {
		return "", err
	}
	if found {
		return t.Path, nil
	}
	if system {
		return "", errNoSystemBroker
	}
	if !explicit && s.systemSocket != s.userSocket {
		if s.probePath(ctx, s.systemSocket, kindSystem).Outcome == probeOK {
			return "", errNoUserBrokerSystemRunning
		}
	}
	return "", errNoBroker
}

// UserSocketPath returns the socket path of the calling account's user
// Broker. For root it is the same path as the system socket.
func UserSocketPath() string {
	return userSocketPath(os.Geteuid())
}

// FindBrokerWithDial is FindBroker together with a function that opens a
// connection to the selected socket. The function checks the peer of every
// underlying connection with peerUID before gRPC sends a byte, so a socket
// taken over by another account after selection is never used.
func FindBrokerWithDial(ctx context.Context) (string, func() (*grpc.ClientConn, localpb.LocalClient, error), error) {
	path, err := FindBroker(ctx)
	if err != nil {
		return "", nil, err
	}
	return path, func() (*grpc.ClientConn, localpb.LocalClient, error) {
		return dialLocal(path)
	}, nil
}

// FindBrokerWithDialForLevel is FindBrokerForLevel together with a function
// that opens a connection to the selected socket. As with FindBrokerWithDial,
// the dialer checks the peer of every underlying connection.
func FindBrokerWithDialForLevel(ctx context.Context, system, explicit bool) (string, func() (*grpc.ClientConn, localpb.LocalClient, error), error) {
	path, err := FindBrokerForLevel(ctx, system, explicit)
	if err != nil {
		return "", nil, err
	}
	return path, func() (*grpc.ClientConn, localpb.LocalClient, error) {
		return dialLocal(path)
	}, nil
}
