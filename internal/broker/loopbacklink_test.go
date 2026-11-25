package broker

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/link/linkpb"
)

// maxLoopbackMessageSize must match the link implementation's 4 MiB message
// limit, so a frame that passes too_large has room on the wire.
const maxLoopbackMessageSize = 4 << 20

// loopbackLink implements deliveryLink over a real linkpb.Link gRPC stream on
// 127.0.0.1; both ends share one object, so a test can use it as both sender and
// receiver link. It is test-only; production uses *link.Link.
type loopbackLink struct {
	linkpb.UnimplementedLinkServer

	t      *testing.T
	remote peerID // identity attributed to requests arriving on the server

	mu       sync.Mutex
	handlers map[link.FrameKind]func(context.Context, peerID, link.Frame) link.Frame
	ups      []func(peerID, []byte)

	// stream is the current client stream, nil while the stream is down.
	stream       linkpb.Link_ConnectClient
	streamCancel context.CancelFunc
	streamDown   chan struct{}
	up           bool
	pending      map[uint64]chan *linkpb.Frame

	// sendMu serializes Send on both streams: gRPC allows one sender per stream,
	// and handler goroutines reply concurrently.
	sendMu sync.Mutex
	nextID atomic.Uint64

	lis  net.Listener
	srv  *grpc.Server
	conn *grpc.ClientConn
}

var _ deliveryLink = (*loopbackLink)(nil)

func newLoopbackLink(t *testing.T, remote peerID) *loopbackLink {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("loopback: listen: %v", err)
	}

	l := &loopbackLink{
		t:        t,
		remote:   remote,
		handlers: make(map[link.FrameKind]func(context.Context, peerID, link.Frame) link.Frame),
		pending:  make(map[uint64]chan *linkpb.Frame),
		lis:      lis,
	}
	l.srv = grpc.NewServer(
		grpc.MaxRecvMsgSize(maxLoopbackMessageSize),
		grpc.MaxSendMsgSize(maxLoopbackMessageSize),
	)
	linkpb.RegisterLinkServer(l.srv, l)
	go func() { _ = l.srv.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxLoopbackMessageSize),
			grpc.MaxCallSendMsgSize(maxLoopbackMessageSize),
		),
	)
	if err != nil {
		l.srv.Stop()
		t.Fatalf("loopback: dial: %v", err)
	}
	l.conn = conn

	t.Cleanup(func() {
		l.closeStream()
		_ = conn.Close()
		l.srv.Stop()
	})

	l.reopen()
	return l
}

// Connect runs the server side of the stream, handing each request frame to its
// handler and answering an unregistered kind as unsupported.
func (l *loopbackLink) Connect(stream linkpb.Link_ConnectServer) error {
	for {
		f, err := stream.Recv()
		if err != nil {
			return nil
		}
		if f.GetIsReply() {
			continue
		}
		l.dispatch(stream, f)
	}
}

func (l *loopbackLink) dispatch(stream linkpb.Link_ConnectServer, f *linkpb.Frame) {
	handler := l.handler(link.FrameKind(f.GetKind()))
	if handler == nil {
		l.reply(stream, unsupportedLoopbackFrame(f))
		return
	}
	ctx := stream.Context()
	go func() {
		reply := handler(ctx, l.remote, link.Frame{
			Kind:    link.FrameKind(f.GetKind()),
			Payload: f.GetPayload(),
		})
		l.reply(stream, &linkpb.Frame{
			Kind:      reply.Kind,
			RequestId: f.GetRequestId(),
			IsReply:   true,
			Payload:   reply.Payload,
		})
	}()
}

func unsupportedLoopbackFrame(req *linkpb.Frame) *linkpb.Frame {
	payload, err := proto.Marshal(&linkpb.Unsupported{RequestKind: req.GetKind()})
	if err != nil {
		payload = nil
	}
	return &linkpb.Frame{
		Kind:      linkpb.KindUnsupported,
		RequestId: req.GetRequestId(),
		IsReply:   true,
		Payload:   payload,
	}
}

// reply sends f on the server stream; a stream the test closed makes Send fail,
// which the test relies on, so the error is ignored.
func (l *loopbackLink) reply(stream linkpb.Link_ConnectServer, f *linkpb.Frame) {
	l.sendMu.Lock()
	defer l.sendMu.Unlock()
	_ = stream.Send(f)
}

// Request sends a kind-400 frame and waits for its reply; the sent flag is false
// before the write and true after, and a stream ending after the write yields
// ErrLinkDown.
func (l *loopbackLink) Request(ctx context.Context, _ peerID, f link.Frame) (link.Frame, bool, error) {
	l.mu.Lock()
	stream := l.stream
	down := l.streamDown
	up := l.up
	if !up || stream == nil {
		l.mu.Unlock()
		return link.Frame{}, false, fmt.Errorf("%w: loopback stream is closed", link.ErrNotEstablished)
	}
	id := l.nextID.Add(1)
	ch := make(chan *linkpb.Frame, 1)
	l.pending[id] = ch
	l.mu.Unlock()

	defer l.forget(id)

	l.sendMu.Lock()
	err := stream.Send(&linkpb.Frame{Kind: f.Kind, RequestId: id, Payload: f.Payload})
	l.sendMu.Unlock()
	if err != nil {
		return link.Frame{}, false, fmt.Errorf("%w: %v", link.ErrNotEstablished, err)
	}

	select {
	case reply := <-ch:
		if reply == nil {
			return link.Frame{}, true, link.ErrLinkDown
		}
		return loopbackReply(f.Kind, reply)
	case <-down:
		// A reply may have arrived just before the stream ended; prefer it.
		select {
		case reply := <-ch:
			if reply != nil {
				return loopbackReply(f.Kind, reply)
			}
		default:
		}
		return link.Frame{}, true, link.ErrLinkDown
	case <-ctx.Done():
		return link.Frame{}, true, ctx.Err()
	}
}

func loopbackReply(kind link.FrameKind, reply *linkpb.Frame) (link.Frame, bool, error) {
	if reply.GetKind() == linkpb.KindUnsupported {
		refused := &linkpb.Unsupported{}
		if err := proto.Unmarshal(reply.GetPayload(), refused); err != nil {
			return link.Frame{}, true, &link.UnsupportedError{Kind: kind}
		}
		return link.Frame{}, true, &link.UnsupportedError{Kind: link.FrameKind(refused.GetRequestKind())}
	}
	return link.Frame{Kind: reply.GetKind(), Payload: reply.GetPayload()}, true, nil
}

// forget drops the pending entry of id; deleting a missing key is harmless.
func (l *loopbackLink) forget(id uint64) {
	l.mu.Lock()
	delete(l.pending, id)
	l.mu.Unlock()
}

func (l *loopbackLink) recv(stream linkpb.Link_ConnectClient, down chan struct{}) {
	for {
		f, err := stream.Recv()
		if err != nil {
			l.endStream(down)
			return
		}
		if !f.GetIsReply() {
			continue
		}
		l.mu.Lock()
		ch := l.pending[f.GetRequestId()]
		delete(l.pending, f.GetRequestId())
		l.mu.Unlock()
		if ch != nil {
			select {
			case ch <- f:
			default:
			}
		}
	}
}

func (l *loopbackLink) endStream(down chan struct{}) {
	l.mu.Lock()
	if l.streamDown != down {
		l.mu.Unlock()
		return
	}
	l.up = false
	l.stream = nil
	l.streamDown = nil
	cancel := l.streamCancel
	l.streamCancel = nil
	pending := l.pending
	l.pending = make(map[uint64]chan *linkpb.Frame)
	close(down)
	l.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	for _, ch := range pending {
		close(ch)
	}
}

func (l *loopbackLink) closeStream() {
	l.mu.Lock()
	down := l.streamDown
	l.mu.Unlock()
	if down != nil {
		l.endStream(down)
	}
}

// reopen builds a new client stream and fires the onUp callbacks; it is the
// dialer's redial, also used by Kick.
func (l *loopbackLink) reopen() {
	l.mu.Lock()
	if l.up {
		l.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := linkpb.NewLinkClient(l.conn).Connect(ctx)
	if err != nil {
		l.mu.Unlock()
		cancel()
		l.t.Errorf("loopback: reopening the stream: %v", err)
		return
	}
	down := make(chan struct{})
	l.stream = stream
	l.streamCancel = cancel
	l.streamDown = down
	l.up = true
	l.pending = make(map[uint64]chan *linkpb.Frame)
	ups := append([]func(peerID, []byte){}, l.ups...)
	l.mu.Unlock()

	go l.recv(stream, down)
	for _, f := range ups {
		f(l.remote, nil)
	}
}

// HandleRequest registers the handler for kind.
func (l *loopbackLink) HandleRequest(kind link.FrameKind, h func(context.Context, peerID, link.Frame) link.Frame) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.handlers[kind] = h
}

func (l *loopbackLink) handler(kind link.FrameKind) func(context.Context, peerID, link.Frame) link.Frame {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.handlers[kind]
}

// Subscribe records the up callbacks; down callbacks are ignored because tests
// wait for an explicit reopen.
func (l *loopbackLink) Subscribe(onUp func(peerID, []byte), onDown func(peerID)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if onUp != nil {
		l.ups = append(l.ups, onUp)
	}
	_ = onDown
}

// Established reports whether a client stream is open.
func (l *loopbackLink) Established(peerID) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.up
}

// Kick redials: it reopens the stream and fires onUp, like the link's dialer.
func (l *loopbackLink) Kick(peerID) {
	l.reopen()
}
