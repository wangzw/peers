package link

import (
	"context"
	"crypto/rand"
	"errors"
	"log/slog"
	"time"

	"github.com/wangzw/peers/internal/link/linkpb"
)

// New creates a Link that has not started working; it starts no goroutine,
// binds no port and reads no peer table, and work begins with Listen or Start.
// It returns an error and a nil *Link when Options.Peers or Options.Settings is
// nil.
func New(opts Options) (*Link, error) {
	if opts.Peers == nil {
		return nil, errors.New("cannot create the Link: Options.Peers is nil")
	}
	if opts.Settings == nil {
		return nil, errors.New("cannot create the Link: Options.Settings is nil")
	}

	clock := opts.Clock
	if clock == nil {
		clock = RealClock()
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	version := opts.ProtocolVersion
	if version == 0 {
		version = linkpb.ProtocolVersion
	}

	rootCtx, cancelRoot := context.WithCancel(context.Background())
	l := &Link{
		peers:    opts.Peers,
		settings: opts.Settings,
		clock:    clock,
		logger:   logger,

		protocolVersion: version,

		sendQueueSize:               1024,
		firstFrameTimeout:           10 * time.Second,
		helloTimeout:                10 * time.Second,
		rejectLinger:                time.Second,
		maxUnauthenticated:          64,
		maxUnauthenticatedPerSource: 8,
		maxStateFrames:              defaultMaxStateFrames,
		maxStateBytes:               defaultMaxStateBytes,
		enforcementMinTime:          5 * time.Second,
		localNets:                   nil,
		onDial:                      nil,
		randRead:                    rand.Read,

		rootCtx:    rootCtx,
		cancelRoot: cancelRoot,

		// records and link up/down notifications
		records:  make(map[Fingerprint]*record),
		dropGen:  make(map[Fingerprint]uint64),
		dropping: make(map[Fingerprint]chan struct{}),
	}
	l.notifier = newNotifier(l)

	// TLS dialing, server credentials and gRPC streams
	l.unauth = newUnauthRegistry(l.maxUnauthenticated, l.maxUnauthenticatedPerSource, l.firstFrameTimeout, l.logger, l.clock.AfterFunc)

	// frame dispatch and send API
	l.stateHandlers = make(map[FrameKind]func(peer Fingerprint, f Frame))
	l.requestHandlers = make(map[FrameKind]func(ctx context.Context, peer Fingerprint, f Frame) Frame)
	return l, nil
}
