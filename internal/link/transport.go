package link

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"sync/atomic"

	"github.com/wangzw/peers/internal/link/linkpb"
	"github.com/wangzw/peers/internal/pairing"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// MaxMessageSize is the largest frame either side accepts or sends. It applies
// to the encoded gRPC message, so a frame's payload is a little smaller.
const MaxMessageSize = 4 << 20

// maxHeaderListSize bounds what a pre-authentication connection can make the
// server parse; link uses no custom metadata.
const maxHeaderListSize = 16 << 10

// frameExceedsLimit measures the encoded message: proto.Size is the length gRPC
// compares with MaxSendMsgSize, and no compressor is configured on either side.
func frameExceedsLimit(f *linkpb.Frame) bool {
	return proto.Size(f) > MaxMessageSize
}

// checkFrameSize runs before the transport because a close racing the
// transport's own Send can replace ResourceExhausted with a generic
// closed-connection error.
func checkFrameSize(f *linkpb.Frame) error {
	if n := proto.Size(f); n > MaxMessageSize {
		return status.Errorf(codes.ResourceExhausted, "link: frame is %d bytes, above the %d-byte message limit", n, MaxMessageSize)
	}
	return nil
}

// dialTLS does not check the fingerprint; the caller does that before anything
// is sent.
func (l *Link) dialTLS(ctx context.Context, address string) (*tls.Conn, peerIdentity, error) {
	if l.onDial != nil {
		l.onDial(address)
	}

	dialCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if stop := l.clock.AfterFunc(l.settings.DialTimeout(), cancel); stop != nil {
		defer stop()
	}

	dial := l.dialContext
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	raw, err := dial(dialCtx, "tcp", address)
	if err != nil {
		return nil, peerIdentity{}, err
	}
	cfg := l.peers.TLSConfig()
	if cfg == nil {
		_ = raw.Close()
		return nil, peerIdentity{}, errors.New("no TLS configuration is available")
	}
	tc := tls.Client(raw, cfg)
	if err := tc.HandshakeContext(dialCtx); err != nil {
		_ = raw.Close()
		return nil, peerIdentity{}, err
	}
	id, err := identityFromState(tc.ConnectionState(), connectionRemote(raw))
	if err != nil {
		_ = tc.Close()
		return nil, peerIdentity{}, err
	}
	return tc, id, nil
}

// clientKeepalive returns the gRPC keepalive parameters for dialed links; gRPC
// raises a probe interval below ten seconds to its own minimum.
func (l *Link) clientKeepalive() keepalive.ClientParameters {
	return keepalive.ClientParameters{
		Time:                l.settings.KeepaliveInterval(),
		Timeout:             l.settings.KeepaliveTimeout(),
		PermitWithoutStream: true,
	}
}

// openClientStream hands an authenticated TLS connection to gRPC and opens the
// single Connect stream on it. abort does not wait and is safe to call
// repeatedly.
func (l *Link) openClientStream(tc *tls.Conn, id peerIdentity, address string) (*conn, error) {
	streamCtx, cancel := context.WithCancel(l.rootCtx)
	var (
		cc        *grpc.ClientConn
		closeOnce sync.Once
	)
	abort := func() {
		closeOnce.Do(func() {
			cancel()
			if tc != nil {
				_ = tc.Close()
			}
			if cc != nil {
				closeConn := cc.Close
				if l.closeClientConn != nil {
					closeConn = func() error { return l.closeClientConn(cc) }
				}
				// ClientConn.Close can block briefly; callers may hold locks.
				go func() { _ = closeConn() }()
			}
		})
	}

	var err error
	cc, err = l.newGRPCClientConn(tc, id, address)
	if err != nil {
		abort()
		return nil, err
	}
	stream, err := linkpb.NewLinkClient(cc).Connect(streamCtx)
	if err != nil {
		abort()
		return nil, err
	}
	c := newConn(stream, abort, id, l.sendQueueSize)
	c.start()
	return c, nil
}

// newGRPCClientConn builds the gRPC client connection around an already
// handshaked TLS connection; the dialer returns that connection exactly once so
// gRPC cannot open a second transport.
func (l *Link) newGRPCClientConn(tc *tls.Conn, id peerIdentity, address string) (*grpc.ClientConn, error) {
	var used atomic.Bool
	dial := func(context.Context, string) (net.Conn, error) {
		if !used.CompareAndSwap(false, true) {
			return nil, errors.New("the transport was already used")
		}
		return tc, nil
	}
	return grpc.NewClient("passthrough:///"+address,
		grpc.WithContextDialer(dial),
		grpc.WithTransportCredentials(clientCreds{id: id}),
		grpc.WithKeepaliveParams(l.clientKeepalive()),
		grpc.WithNoProxy(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(MaxMessageSize),
			grpc.MaxCallSendMsgSize(MaxMessageSize),
		),
	)
}

func (l *Link) isKnownPeer(fp Fingerprint) bool {
	return l.peers.Role(fp) != pairing.RoleUnknown
}

// serverOptions returns the gRPC server options for accepted links; the
// keepalive parameters are read when the server is created, so a configuration
// change applies to the next server.
func (l *Link) serverOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.Creds(serverCreds{cfg: l.peers.TLSConfig, known: l.isKnownPeer}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    l.settings.KeepaliveInterval(),
			Timeout: l.settings.KeepaliveTimeout(),
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             l.enforcementMinTime,
			PermitWithoutStream: true,
		}),
		grpc.MaxConcurrentStreams(1),
		grpc.MaxHeaderListSize(maxHeaderListSize),
		grpc.ConnectionTimeout(l.firstFrameTimeout),
		grpc.MaxRecvMsgSize(MaxMessageSize),
		grpc.MaxSendMsgSize(MaxMessageSize),
	}
}

func (l *Link) newGRPCServer(onStream func(c *conn, t *trackedConn)) *grpc.Server {
	srv := grpc.NewServer(l.serverOptions()...)
	linkpb.RegisterLinkServer(srv, &linkServer{link: l, onStream: onStream})
	return srv
}

type linkServer struct {
	linkpb.UnimplementedLinkServer
	link     *Link
	onStream func(c *conn, t *trackedConn)
}

func (s *linkServer) Connect(stream grpc.BidiStreamingServer[linkpb.Frame, linkpb.Frame]) error {
	if s.link.onConnect != nil {
		s.link.onConnect()
	}
	p, ok := peer.FromContext(stream.Context())
	if !ok || p.AuthInfo == nil {
		return status.Error(codes.Internal, "link: stream has no peer identity")
	}
	info, ok := p.AuthInfo.(serverAuthInfo)
	if !ok {
		return status.Error(codes.Internal, "link: stream has unexpected credentials")
	}
	if info.tracked == nil {
		// Without the tracked wrapper the first-frame deadline and the
		// unauthenticated connection cap cannot be enforced; refuse to guess.
		return status.Error(codes.Internal, "link: stream has no tracked connection")
	}
	if !info.tracked.claimStream() {
		return status.Error(codes.ResourceExhausted, "one stream per connection")
	}
	abort := func() { _ = info.tracked.Close() }

	// Join the link WaitGroup under the same mutex Close uses, so a stream that
	// arrives during Close either joins before Close waits or is refused here.
	s.link.mu.Lock()
	if s.link.closed {
		s.link.mu.Unlock()
		_ = info.tracked.Close()
		return status.Error(codes.Unavailable, "link: closed")
	}
	s.link.wg.Add(1)
	s.link.mu.Unlock()
	defer s.link.wg.Done()

	c := newConn(stream, abort, info.id, s.link.sendQueueSize)
	c.start()
	s.onStream(c, info.tracked)
	<-c.done()
	return nil
}
