package link

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
)

// Link maintains the authenticated frame channel to each peer.
type Link struct {
	peers    Peers
	settings Settings
	clock    Clock
	logger   *slog.Logger

	protocolVersion uint32 // Options.ProtocolVersion, or linkpb.ProtocolVersion when 0; not a test knob

	// knobs are fixed after New; tests change them with SetTestKnobs before
	// Listen or Start.
	sendQueueSize               int
	firstFrameTimeout           time.Duration
	helloTimeout                time.Duration
	rejectLinger                time.Duration
	maxUnauthenticated          int
	maxUnauthenticatedPerSource int
	maxStateFrames              int
	maxStateBytes               int
	enforcementMinTime          time.Duration
	localNets                   func() ([]*net.IPNet, error)
	onDial                      func(address string)
	randRead                    func([]byte) (int, error)
	dialContext                 func(ctx context.Context, network, address string) (net.Conn, error)
	afterHelloSent              func()

	rootCtx    context.Context
	cancelRoot context.CancelFunc
	wg         sync.WaitGroup

	mu     sync.Mutex // guards closed, records, dropGen, dropping, slots, server, lis and started
	closed bool

	// --- records and link up/down notifications ---
	records  map[Fingerprint]*record
	dropGen  map[Fingerprint]uint64
	dropping map[Fingerprint]chan struct{}
	subMu    sync.RWMutex
	subs     []subscription
	notifier *notifier

	// --- TLS dialing, server credentials and gRPC streams ---
	unauth          *unauthRegistry
	onConnect       func()
	closeClientConn func(*grpc.ClientConn) error

	// --- frame dispatch and send API ---
	regMu           sync.RWMutex
	stateHandlers   map[FrameKind]func(peer Fingerprint, f Frame)
	requestHandlers map[FrameKind]func(ctx context.Context, peer Fingerprint, f Frame) Frame

	// slots holds the request semaphore of each paired peer, shared by its
	// successive connections; unpairing a peer removes its entry once no handler
	// holds a slot. Guarded by mu.
	slots map[Fingerprint]chan struct{}

	// --- listening and closing ---
	server *grpc.Server
	lis    *trackingListener

	// --- dial loop, Start, rejection and PairDial ---
	started bool

	// beforeDialLoop, when non-nil, runs at the start of the goroutine
	// startDialLoop launches, before that goroutine runs the connection install
	// accepted or enters the dial loop; tests use it to hold the window where
	// Drop can cancel the goroutine's dial-loop context.
	beforeDialLoop func()
	// onRejectLingerExit, when non-nil, runs when the goroutine rejectAndLinger
	// started exits; tests use it to observe that exit.
	onRejectLingerExit func()
}
