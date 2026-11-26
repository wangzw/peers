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

type targetKind int

// Selection steps in trial order: test hook, user Broker, system Broker.
const (
	kindTest targetKind = iota
	kindUser
	kindSystem
)

type target struct {
	Path string
	Kind targetKind
}

// SystemRetryDeadline is how long selection keeps retrying a system socket
// that exists and may be connected to but has no process answering.
const SystemRetryDeadline = 10 * time.Second

var systemRetryWaits = []time.Duration{
	250 * time.Millisecond,
	500 * time.Millisecond,
	1 * time.Second,
	2 * time.Second,
	4 * time.Second,
}

// selector applies the Broker selection order; its fields are seams for tests,
// and FindBroker supplies the production values.
type selector struct {
	getenv        func(string) string
	userSocket    string
	altUserSocket string
	systemSocket  string
	probe         func(ctx context.Context, path string) probeInfo
	after         func(time.Duration) <-chan time.Time
	lockHeld      func(runtimeDir string) bool
}

// Find returns the first target of the selection order that holds. It never
// starts a Broker; when no socket answers it returns errNoBroker.
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

func (s *selector) find(ctx context.Context) (target, bool, error) {
	if t, found, err := s.findNamedSocket(ctx); err != nil || found {
		return t, found, err
	}

	if t, found, err := s.findUserSocket(ctx); err != nil || found {
		return t, found, err
	}

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

func (s *selector) userSocketPaths() []string {
	paths := []string{s.userSocket}
	if s.altUserSocket != "" && s.altUserSocket != s.userSocket {
		paths = append(paths, s.altUserSocket)
	}
	return paths
}

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

// With several usable sockets the account lock says which Broker is running;
// with no held lock the first usable socket is kept, which is the one the
// location rule named first; two held locks must not happen.
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

func (s *selector) findAtLevel(ctx context.Context, system bool) (target, bool, error) {
	if t, found, err := s.findNamedSocket(ctx); err != nil || found {
		return t, found, err
	}
	if !system {
		return s.findUserSocket(ctx)
	}

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

// A refused connection is retried for SystemRetryDeadline; a peer mismatch ends
// the retry at once rather than running out the deadline.
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
// serve the Broker at its path; its text is shown to the model as it stands,
// unlike the other selection errors the connection loop wraps.
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

func peerMismatchError(path string, kind targetKind, uid uint32) error {
	return &peerMismatch{path: path, kind: kind, uid: uid}
}

func (s *selector) afterFunc() func(time.Duration) <-chan time.Time {
	if s.after == nil {
		return time.After
	}
	return s.after
}

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

// FindBroker returns the socket path of the first step of the Broker selection
// order that holds. It never starts a Broker. The error text is suitable for a
// terminal user.
func FindBroker(ctx context.Context) (string, error) {
	t, err := productionSelector().Find(ctx)
	if err != nil {
		return "", err
	}
	return t.Path, nil
}

// FindBrokerForLevel returns the socket path of the running Broker at the
// selected level. It never starts a Broker and never falls through to the other
// level. explicit reports whether a level option was given rather than resolved
// from the calling account. When no Broker is found, system reports the system
// Broker unavailable, a defaulted user selection over a running system Broker
// reports that only the system Broker runs and names --system, and any other
// selection reports errNoBroker.
func FindBrokerForLevel(ctx context.Context, system, explicit bool) (string, error) {
	return productionSelector().findForLevel(ctx, system, explicit)
}

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
