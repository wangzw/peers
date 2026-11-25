// Package fakelocal provides a scriptable fake of the Broker's Local gRPC
// service for tests. A Server listens on a Unix socket, answers each RPC from a
// Handlers value, records every unary request and the first frame of each
// Register stream, and gives the test control of each Register stream.
package fakelocal

import (
	"bytes"
	"context"
	"net"
	"os"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/socktest"
)

// SocketPath returns a short socket path in a fresh directory removed at
// cleanup; the path stays below the Unix socket limit of 108 bytes on Linux and
// 104 bytes on macOS.
func SocketPath(t testing.TB) string {
	t.Helper()
	return socktest.Path(t, "broker.sock")
}

// Selector returns a termcmd-compatible selector for a Server at path. The
// returned value is the unnamed function type the selector interface uses, so
// a test package that may not import localpb can assign it directly.
func Selector(path string) func(context.Context) (string, func() (*grpc.ClientConn, localpb.LocalClient, error), error) {
	return func(context.Context) (string, func() (*grpc.ClientConn, localpb.LocalClient, error), error) {
		return path, func() (*grpc.ClientConn, localpb.LocalClient, error) {
			cc, err := grpc.NewClient("unix://"+path, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return nil, nil, err
			}
			return cc, localpb.NewLocalClient(cc), nil
		}, nil
	}
}

// Handlers holds optional per-RPC behaviour. A nil field answers
// codes.Unimplemented, except Hello, which answers localpb.ProtocolVersion.
type Handlers struct {
	// Hello answers Hello; nil answers localpb.ProtocolVersion.
	Hello func(context.Context, *localpb.HelloRequest) (*localpb.HelloResponse, error)
	// ListAgents answers ListAgents.
	ListAgents func(context.Context, *localpb.ListAgentsRequest) (*localpb.ListAgentsResponse, error)
	// Send answers Send.
	Send func(context.Context, *localpb.SendRequest) (*localpb.SendResponse, error)
	// JoinTopic answers JoinTopic.
	JoinTopic func(context.Context, *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error)
	// LeaveTopic answers LeaveTopic.
	LeaveTopic func(context.Context, *localpb.LeaveTopicRequest) (*localpb.LeaveTopicResponse, error)
	// ListTopics answers ListTopics.
	ListTopics func(context.Context, *localpb.ListTopicsRequest) (*localpb.ListTopicsResponse, error)
	// ListBrokers answers ListBrokers.
	ListBrokers func(context.Context, *localpb.ListBrokersRequest) (*localpb.ListBrokersResponse, error)
	// Connect answers Connect.
	Connect func(context.Context, *localpb.ConnectRequest) (*localpb.ConnectResponse, error)
	// Disconnect answers Disconnect.
	Disconnect func(context.Context, *localpb.DisconnectRequest) (*localpb.DisconnectResponse, error)
	// CreatePairingKey answers CreatePairingKey.
	CreatePairingKey func(context.Context, *localpb.CreatePairingKeyRequest) (*localpb.CreatePairingKeyResponse, error)
	// GetConfig answers GetConfig.
	GetConfig func(context.Context, *localpb.GetConfigRequest) (*localpb.GetConfigResponse, error)
	// SetConfig answers SetConfig.
	SetConfig func(context.Context, *localpb.SetConfigRequest) (*localpb.SetConfigResponse, error)
	// Status answers Status.
	Status func(context.Context, *localpb.StatusRequest) (*localpb.StatusResponse, error)
	// UpdateStatus answers UpdateStatus.
	UpdateStatus func(context.Context, *localpb.UpdateStatusRequest) (*localpb.UpdateStatusResponse, error)
	// ApplyUpdate answers ApplyUpdate.
	ApplyUpdate func(context.Context, *localpb.ApplyUpdateRequest) (*localpb.ApplyUpdateResponse, error)
	// Audit serves Audit.
	Audit func(*localpb.AuditRequest, localpb.Local_AuditServer) error
}

// AdminHandlers returns handlers for the six Broker administration RPCs, each
// answering a sample whose formatted output is distinct: a config entry, a
// change line, a key, the empty broker listing, a paired line and a
// disconnected line.
func AdminHandlers() Handlers {
	return Handlers{
		GetConfig: func(context.Context, *localpb.GetConfigRequest) (*localpb.GetConfigResponse, error) {
			return &localpb.GetConfigResponse{Entries: []*localpb.ConfigEntry{
				{Key: "broker.alias", Value: &localpb.ConfigValue{Value: &localpb.ConfigValue_StringValue{StringValue: "hostA"}}},
			}}, nil
		},
		SetConfig: func(context.Context, *localpb.SetConfigRequest) (*localpb.SetConfigResponse, error) {
			return &localpb.SetConfigResponse{
				OldValue: &localpb.ConfigValue{Value: &localpb.ConfigValue_StringValue{StringValue: "old"}},
				NewValue: &localpb.ConfigValue{Value: &localpb.ConfigValue_StringValue{StringValue: "new"}},
			}, nil
		},
		CreatePairingKey: func(context.Context, *localpb.CreatePairingKeyRequest) (*localpb.CreatePairingKeyResponse, error) {
			return &localpb.CreatePairingKeyResponse{Key: "one-time-key"}, nil
		},
		ListBrokers: func(context.Context, *localpb.ListBrokersRequest) (*localpb.ListBrokersResponse, error) {
			return &localpb.ListBrokersResponse{Local: &localpb.LocalBrokerState{}}, nil
		},
		Connect: func(context.Context, *localpb.ConnectRequest) (*localpb.ConnectResponse, error) {
			return &localpb.ConnectResponse{Alias: "hostB", Fingerprint: bytes.Repeat([]byte{0xcd}, 32)}, nil
		},
		Disconnect: func(context.Context, *localpb.DisconnectRequest) (*localpb.DisconnectResponse, error) {
			return &localpb.DisconnectResponse{}, nil
		},
	}
}

// ApplyUpdateAnswer returns an ApplyUpdate handler that reports installing to
// from toVersion. An empty toVersion means the Broker is up to date.
func ApplyUpdateAnswer(from, toVersion string, restarting bool) func(context.Context, *localpb.ApplyUpdateRequest) (*localpb.ApplyUpdateResponse, error) {
	return func(context.Context, *localpb.ApplyUpdateRequest) (*localpb.ApplyUpdateResponse, error) {
		return &localpb.ApplyUpdateResponse{FromVersion: from, ToVersion: toVersion, Restarting: restarting}, nil
	}
}

// ApplyUpdateFailure returns an ApplyUpdate handler that fails with code and
// message.
func ApplyUpdateFailure(code codes.Code, message string) func(context.Context, *localpb.ApplyUpdateRequest) (*localpb.ApplyUpdateResponse, error) {
	return func(context.Context, *localpb.ApplyUpdateRequest) (*localpb.ApplyUpdateResponse, error) {
		return nil, status.Error(code, message)
	}
}

// Call is one recorded request, either a unary request or the first frame of a
// Register stream (Method "Register"), in arrival order.
type Call struct {
	// Method is the RPC name.
	Method string
	// Request is the request message as received.
	Request proto.Message
}

// Server is a fake Local service listening on a Unix socket.
type Server struct {
	t        testing.TB
	path     string
	handlers Handlers

	mu      sync.Mutex
	calls   []Call
	ready   []*RegisterConn
	streams map[*RegisterConn]struct{}
	notify  chan struct{}
	closed  chan struct{}
	stopped bool
	lis     net.Listener
	gs      *grpc.Server
	regWG   sync.WaitGroup
}

// Start listens on path and serves until Stop or test cleanup. When h.Hello is
// nil, Hello answers localpb.ProtocolVersion.
func Start(t testing.TB, path string, h Handlers) *Server {
	t.Helper()
	s := &Server{
		t:        t,
		path:     path,
		handlers: h,
		streams:  make(map[*RegisterConn]struct{}),
		notify:   make(chan struct{}, 1),
	}
	s.start()
	t.Cleanup(s.Stop)
	return s
}

func (s *Server) start() {
	lis, err := net.Listen("unix", s.path)
	if err != nil {
		s.t.Fatalf("fakelocal: listening on %s: %v", s.path, err)
	}
	gs := grpc.NewServer()
	localpb.RegisterLocalServer(gs, &service{s: s})

	s.mu.Lock()
	s.lis = lis
	s.gs = gs
	s.closed = make(chan struct{})
	s.stopped = false
	s.ready = nil
	s.mu.Unlock()

	go func() { _ = gs.Serve(lis) }()
}

// Path returns the socket path the server listens on.
func (s *Server) Path() string { return s.path }

// Stop closes the listener, ends every open Register stream with
// codes.Unavailable, stops the gRPC server and removes the socket file. It is
// idempotent.
func (s *Server) Stop() { s.stop() }

func (s *Server) stop() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	lis, gs := s.lis, s.gs
	streams := make([]*RegisterConn, 0, len(s.streams))
	for c := range s.streams {
		streams = append(streams, c)
	}
	close(s.closed)
	s.ready = nil
	s.mu.Unlock()

	if lis != nil {
		_ = lis.Close()
	}
	for _, c := range streams {
		c.End()
	}
	s.regWG.Wait()
	if gs != nil {
		gs.Stop()
	}
	_ = os.Remove(s.path)
}

// Restart stops the server like Stop and listens again on the same path with
// the same Handlers. Calls is not cleared. Used to simulate a Broker restart.
func (s *Server) Restart() {
	s.stop()
	s.start()
}

// NextRegister waits for the next Register stream whose first frame has
// arrived.
func (s *Server) NextRegister(ctx context.Context) (*RegisterConn, error) {
	for {
		s.mu.Lock()
		if len(s.ready) > 0 {
			c := s.ready[0]
			s.ready = s.ready[1:]
			s.mu.Unlock()
			return c, nil
		}
		closed := s.closed
		notify := s.notify
		s.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-closed:
			return nil, status.Error(codes.Unavailable, "fakelocal: server stopped")
		case <-notify:
		}
	}
}

// Calls returns every unary request and the first frame of each Register stream
// received so far, in arrival order.
func (s *Server) Calls() []Call {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Call, len(s.calls))
	copy(out, s.calls)
	return out
}

func (s *Server) record(method string, req proto.Message) {
	s.mu.Lock()
	s.calls = append(s.calls, Call{Method: method, Request: req})
	s.mu.Unlock()
}

func (s *Server) signal() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// RegisterConn is one open Register stream, controlled by the test.
type RegisterConn struct {
	// First is the first RegisterRequest the client sent on this stream, as
	// received by the server.
	First *localpb.RegisterRequest

	s      *Server
	stream localpb.Local_RegisterServer
	done   chan struct{}
	end    chan struct{}
	endOne sync.Once
	sendMu sync.Mutex
}

// Send writes one response on the stream.
func (c *RegisterConn) Send(r *localpb.RegisterResponse) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	select {
	case <-c.done:
		return status.Error(codes.Unavailable, "fakelocal: register stream ended")
	default:
	}
	return c.stream.Send(r)
}

// Complete sends a registration_complete event.
func (c *RegisterConn) Complete(agentID, address string, autoJoin bool) error {
	return c.Send(&localpb.RegisterResponse{
		Event: &localpb.RegisterResponse_RegistrationComplete{
			RegistrationComplete: &localpb.RegistrationComplete{
				AgentId:      agentID,
				AgentAddress: address,
				AutoJoin:     autoJoin,
			},
		},
	})
}

// NotRegistered sends a not_registered event carrying reason.
func (c *RegisterConn) NotRegistered(r localpb.NotRegisteredReason) error {
	return c.Send(&localpb.RegisterResponse{
		Event: &localpb.RegisterResponse_NotRegistered{
			NotRegistered: &localpb.RegistrationProblem{Reason: r},
		},
	})
}

// End makes the server end the stream with codes.Unavailable. It is safe to
// call more than once.
func (c *RegisterConn) End() {
	c.endOne.Do(func() { close(c.end) })
}

// Done is closed when the stream ends for any reason, the client ending it
// included.
func (c *RegisterConn) Done() <-chan struct{} { return c.done }

// ErrorStatus builds a gRPC status error with code carrying exactly one
// ErrorDetail.
func ErrorStatus(code codes.Code, detail *localpb.ErrorDetail) error {
	st := status.New(code, detail.GetMessage())
	st, err := st.WithDetails(detail)
	if err != nil {
		return status.Error(code, detail.GetMessage())
	}
	return st.Err()
}

type service struct {
	localpb.UnimplementedLocalServer
	s *Server
}

func unimplemented(method string) error {
	return status.Errorf(codes.Unimplemented, "fakelocal: %s not implemented", method)
}

func (svc *service) Register(stream localpb.Local_RegisterServer) error {
	s := svc.s

	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return status.Error(codes.Unavailable, "fakelocal: server stopped")
	}
	s.regWG.Add(1)
	c := &RegisterConn{
		s:      s,
		stream: stream,
		done:   make(chan struct{}),
		end:    make(chan struct{}),
	}
	s.streams[c] = struct{}{}
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.streams, c)
		s.mu.Unlock()
		// Wait for an in-flight Send before closing Done. Send holds sendMu for
		// the whole write, and grpc-go forbids using a server stream after its
		// handler has returned.
		c.sendMu.Lock()
		close(c.done)
		c.sendMu.Unlock()
		s.regWG.Done()
	}()

	type recvResult struct {
		req *localpb.RegisterRequest
		err error
	}
	first := make(chan recvResult, 1)
	go func() {
		req, err := stream.Recv()
		first <- recvResult{req: req, err: err}
	}()

	select {
	case <-c.end:
		return status.Error(codes.Unavailable, "fakelocal: server ended the register stream")
	case <-stream.Context().Done():
		return status.Error(codes.Unavailable, "fakelocal: client ended the register stream")
	case r := <-first:
		if r.err != nil {
			return nil
		}
		c.First = r.req
		s.record("Register", r.req)
	}

	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return status.Error(codes.Unavailable, "fakelocal: server stopped")
	}
	s.ready = append(s.ready, c)
	s.mu.Unlock()
	s.signal()

	// The Broker accepts only the first frame; every later frame is read and
	// discarded until the stream ends.
	drained := make(chan struct{}, 1)
	go func() {
		for {
			if _, err := stream.Recv(); err != nil {
				drained <- struct{}{}
				return
			}
		}
	}()

	select {
	case <-c.end:
		return status.Error(codes.Unavailable, "fakelocal: server ended the register stream")
	case <-stream.Context().Done():
		return status.Error(codes.Unavailable, "fakelocal: client ended the register stream")
	case <-drained:
		return nil
	}
}

func (svc *service) Hello(ctx context.Context, req *localpb.HelloRequest) (*localpb.HelloResponse, error) {
	svc.s.record("Hello", req)
	if h := svc.s.handlers.Hello; h != nil {
		return h(ctx, req)
	}
	return &localpb.HelloResponse{ProtocolVersion: localpb.ProtocolVersion}, nil
}

func (svc *service) ListAgents(ctx context.Context, req *localpb.ListAgentsRequest) (*localpb.ListAgentsResponse, error) {
	svc.s.record("ListAgents", req)
	if h := svc.s.handlers.ListAgents; h != nil {
		return h(ctx, req)
	}
	return nil, unimplemented("ListAgents")
}

func (svc *service) Send(ctx context.Context, req *localpb.SendRequest) (*localpb.SendResponse, error) {
	svc.s.record("Send", req)
	if h := svc.s.handlers.Send; h != nil {
		return h(ctx, req)
	}
	return nil, unimplemented("Send")
}

func (svc *service) JoinTopic(ctx context.Context, req *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
	svc.s.record("JoinTopic", req)
	if h := svc.s.handlers.JoinTopic; h != nil {
		return h(ctx, req)
	}
	return nil, unimplemented("JoinTopic")
}

func (svc *service) LeaveTopic(ctx context.Context, req *localpb.LeaveTopicRequest) (*localpb.LeaveTopicResponse, error) {
	svc.s.record("LeaveTopic", req)
	if h := svc.s.handlers.LeaveTopic; h != nil {
		return h(ctx, req)
	}
	return nil, unimplemented("LeaveTopic")
}

func (svc *service) ListTopics(ctx context.Context, req *localpb.ListTopicsRequest) (*localpb.ListTopicsResponse, error) {
	svc.s.record("ListTopics", req)
	if h := svc.s.handlers.ListTopics; h != nil {
		return h(ctx, req)
	}
	return nil, unimplemented("ListTopics")
}

func (svc *service) ListBrokers(ctx context.Context, req *localpb.ListBrokersRequest) (*localpb.ListBrokersResponse, error) {
	svc.s.record("ListBrokers", req)
	if h := svc.s.handlers.ListBrokers; h != nil {
		return h(ctx, req)
	}
	return nil, unimplemented("ListBrokers")
}

func (svc *service) Connect(ctx context.Context, req *localpb.ConnectRequest) (*localpb.ConnectResponse, error) {
	svc.s.record("Connect", req)
	if h := svc.s.handlers.Connect; h != nil {
		return h(ctx, req)
	}
	return nil, unimplemented("Connect")
}

func (svc *service) Disconnect(ctx context.Context, req *localpb.DisconnectRequest) (*localpb.DisconnectResponse, error) {
	svc.s.record("Disconnect", req)
	if h := svc.s.handlers.Disconnect; h != nil {
		return h(ctx, req)
	}
	return nil, unimplemented("Disconnect")
}

func (svc *service) CreatePairingKey(ctx context.Context, req *localpb.CreatePairingKeyRequest) (*localpb.CreatePairingKeyResponse, error) {
	svc.s.record("CreatePairingKey", req)
	if h := svc.s.handlers.CreatePairingKey; h != nil {
		return h(ctx, req)
	}
	return nil, unimplemented("CreatePairingKey")
}

func (svc *service) GetConfig(ctx context.Context, req *localpb.GetConfigRequest) (*localpb.GetConfigResponse, error) {
	svc.s.record("GetConfig", req)
	if h := svc.s.handlers.GetConfig; h != nil {
		return h(ctx, req)
	}
	return nil, unimplemented("GetConfig")
}

func (svc *service) SetConfig(ctx context.Context, req *localpb.SetConfigRequest) (*localpb.SetConfigResponse, error) {
	svc.s.record("SetConfig", req)
	if h := svc.s.handlers.SetConfig; h != nil {
		return h(ctx, req)
	}
	return nil, unimplemented("SetConfig")
}

func (svc *service) Status(ctx context.Context, req *localpb.StatusRequest) (*localpb.StatusResponse, error) {
	svc.s.record("Status", req)
	if h := svc.s.handlers.Status; h != nil {
		return h(ctx, req)
	}
	return nil, unimplemented("Status")
}

func (svc *service) UpdateStatus(ctx context.Context, req *localpb.UpdateStatusRequest) (*localpb.UpdateStatusResponse, error) {
	svc.s.record("UpdateStatus", req)
	if h := svc.s.handlers.UpdateStatus; h != nil {
		return h(ctx, req)
	}
	return nil, unimplemented("UpdateStatus")
}

func (svc *service) ApplyUpdate(ctx context.Context, req *localpb.ApplyUpdateRequest) (*localpb.ApplyUpdateResponse, error) {
	svc.s.record("ApplyUpdate", req)
	if h := svc.s.handlers.ApplyUpdate; h != nil {
		return h(ctx, req)
	}
	return nil, unimplemented("ApplyUpdate")
}

func (svc *service) Audit(req *localpb.AuditRequest, stream localpb.Local_AuditServer) error {
	if h := svc.s.handlers.Audit; h != nil {
		return h(req, stream)
	}
	return unimplemented("Audit")
}
