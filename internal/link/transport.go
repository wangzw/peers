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

// maxMessageSize is the largest frame either side accepts or sends. It applies
// to the gRPC message, so a frame's payload is a little smaller. The same
// constant is passed to grpc.MaxCallSendMsgSize and grpc.MaxSendMsgSize, and
// checkFrameSize enforces it before a frame reaches the transport.
const maxMessageSize = 4 << 20

// maxHeaderListSize is the largest HTTP/2 header list an inbound connection may
// send. Link uses no custom metadata, so the limit does not affect an
// established link; it only bounds what a pre-authentication connection can
// make the server parse.
const maxHeaderListSize = 16 << 10

// checkFrameSize rejects a frame the transport would refuse. The limit applies
// to the encoded message, so proto.Size is the matching measure: it is the
// serialized length gRPC compares with MaxSendMsgSize, and no compressor is
// configured on either side. The check runs before the frame reaches the
// transport because a close that races with the transport's own Send can
// replace ResourceExhausted with a generic closed-connection error.
func checkFrameSize(f *linkpb.Frame) error {
	if n := proto.Size(f); n > maxMessageSize {
		return status.Errorf(codes.ResourceExhausted, "link: frame is %d bytes, above the %d-byte message limit", n, maxMessageSize)
	}
	return nil
}

// dialTLS opens a TCP connection and completes a TLS 1.3 handshake with the
// peer's own TLS configuration. It does not check the fingerprint; the caller
// does that before anything is sent. The dial timeout from settings bounds the
// whole operation through a context canceled by the injected clock.
func (l *Link) dialTLS(ctx context.Context, address string) (*tls.Conn, peerIdentity, error) {
	if l.onDial != nil {
		l.onDial(address)
	}

	dialCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if stop := l.clock.AfterFunc(l.settings.DialTimeout(), cancel); stop != nil {
		defer stop()
	}

	raw, err := (&net.Dialer{}).DialContext(dialCtx, "tcp", address)
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

// clientKeepalive returns the gRPC keepalive parameters for dialed links. The
// probe interval is read from settings; gRPC raises intervals below ten
// seconds to its own minimum.
func (l *Link) clientKeepalive() keepalive.ClientParameters {
	return keepalive.ClientParameters{
		Time:                l.settings.KeepaliveInterval(),
		Timeout:             l.settings.KeepaliveTimeout(),
		PermitWithoutStream: true,
	}
}

// openClientStream hands an already authenticated TLS connection to gRPC and
// opens the single Connect stream on it. The abort function stops the stream,
// closes the transport and closes the client connection without waiting; it is
// safe to call repeatedly.
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
// handshaked TLS connection. The dialer returns that connection exactly once;
// later calls fail, so gRPC can never open a second transport behind link's
// back.
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
			grpc.MaxCallRecvMsgSize(maxMessageSize),
			grpc.MaxCallSendMsgSize(maxMessageSize),
		),
	)
}

// isKnownPeer reports whether fp is in the peer table. serverCreds calls it
// after the TLS handshake to decide whether the connection leaves the
// stranger-source pool.
func (l *Link) isKnownPeer(fp Fingerprint) bool {
	return l.peers.Role(fp) != pairing.RoleUnknown
}

// serverOptions returns the gRPC server options for accepted links. The
// keepalive parameters are read when the server is created, so a later
// configuration change applies to the next server.
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
		grpc.MaxRecvMsgSize(maxMessageSize),
		grpc.MaxSendMsgSize(maxMessageSize),
	}
}

// newGRPCServer creates the gRPC server used by Listen and registers the
// Connect handler on it. Each accepted connection is handed to onStream once,
// as a conn, and the handler stays alive until that conn closes.
func (l *Link) newGRPCServer(onStream func(c *conn, t *trackedConn)) *grpc.Server {
	srv := grpc.NewServer(l.serverOptions()...)
	linkpb.RegisterLinkServer(srv, &linkServer{link: l, onStream: onStream})
	return srv
}

// linkServer is the linkpb.Link service. Its single Connect method is the only
// stream a connection may carry.
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
