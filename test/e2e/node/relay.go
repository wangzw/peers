package node

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	"google.golang.org/grpc"

	"github.com/wangzw/peers/internal/link/linkpb"
)

// Relay is a man in the middle on the Link protocol: it terminates the client's
// TLS connection and opens its own to upstream, so it copies every frame while
// holding none of the pairing keys. Forwarding the client's certificate instead
// would prove nothing about a MITM.
type Relay struct {
	// UnimplementedLinkServer keeps Relay satisfying LinkServer when the service
	// gains a method the relay does not forward.
	linkpb.UnimplementedLinkServer

	upstream     string
	listener     net.Listener
	server       *grpc.Server
	upstreamCert tls.Certificate

	mu         sync.Mutex
	streams    int
	framesUp   int
	framesDown int

	closeOnce sync.Once
}

// StartRelay serves the Link service on listen with a fresh certificate and,
// for every accepted stream, opens a stream to upstream with a second fresh
// certificate. Frames are copied both ways unchanged.
func StartRelay(listen, upstream string) (*Relay, error) {
	serverCert, err := NewSelfSignedCert()
	if err != nil {
		return nil, err
	}
	upstreamCert, err := NewSelfSignedCert()
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, fmt.Errorf("node: relay: listen on %s: %w", listen, err)
	}
	r := &Relay{
		upstream:     upstream,
		listener:     listener,
		upstreamCert: upstreamCert,
	}
	r.server = grpc.NewServer(grpc.Creds(serverTLS(serverCert)))
	linkpb.RegisterLinkServer(r.server, r)
	go func() { _ = r.server.Serve(listener) }()
	return r, nil
}

// Stats reports how many streams the relay accepted and how many frames it
// forwarded in each direction.
func (r *Relay) Stats() (streams, framesUp, framesDown int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.streams, r.framesUp, r.framesDown
}

// Close stops the relay and every stream it is carrying.
func (r *Relay) Close() error {
	r.closeOnce.Do(func() {
		r.server.Stop()
		_ = r.listener.Close()
	})
	return nil
}

// Connect relays one accepted stream to upstream. It returns when either
// direction ends; the remaining copy exits as soon as the streams are torn
// down, so the handler does not wait for a client that keeps its side open.
func (r *Relay) Connect(clientStream grpc.BidiStreamingServer[linkpb.Frame, linkpb.Frame]) error {
	r.mu.Lock()
	r.streams++
	r.mu.Unlock()

	conn, err := grpc.NewClient(r.upstream, grpc.WithTransportCredentials(clientTLS(r.upstreamCert)))
	if err != nil {
		return fmt.Errorf("node: relay: dial upstream %s: %w", r.upstream, err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithCancel(clientStream.Context())
	defer cancel()

	upstreamStream, err := linkpb.NewLinkClient(conn).Connect(ctx)
	if err != nil {
		return fmt.Errorf("node: relay: open upstream stream: %w", err)
	}

	done := make(chan struct{}, 2)
	go func() {
		defer func() { done <- struct{}{} }()
		r.copyFrames(clientStream, upstreamStream, true)
	}()
	go func() {
		defer func() { done <- struct{}{} }()
		r.copyFrames(upstreamStream, clientStream, false)
	}()

	<-done
	cancel()
	return nil
}

type frameStream interface {
	Recv() (*linkpb.Frame, error)
	Send(*linkpb.Frame) error
}

// copyFrames forwards frames from one stream to the other until a read or a
// write fails. up says which direction the frames travel, for Stats.
func (r *Relay) copyFrames(from, to frameStream, up bool) {
	for {
		frame, err := from.Recv()
		if err != nil {
			return
		}
		if err := to.Send(frame); err != nil {
			return
		}
		r.mu.Lock()
		if up {
			r.framesUp++
		} else {
			r.framesDown++
		}
		r.mu.Unlock()
	}
}
