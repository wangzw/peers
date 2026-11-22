package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"syscall"

	"google.golang.org/grpc"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// degradedService answers the local interface while the Broker runs
// diagnostics only. It serves a duplicate of the local socket listener, so
// stopping it closes every connection it accepted and returns while the
// original listener stays open for the full service.
type degradedService struct {
	localpb.UnimplementedLocalServer

	state    func() *localpb.DegradedState
	version  string
	protocol uint32
	log      *slog.Logger

	// registers bounds the Register streams one account may hold while the
	// Broker runs diagnostics only, with the same limit the full service uses.
	registers *accountLimit

	srv  *grpc.Server
	ln   net.Listener
	done chan struct{}
	once sync.Once
}

// startDegradedService serves the diagnostics-only interface on ln. The
// returned service's Stop closes its connections and its duplicate listener;
// ln itself remains bound and accepting.
func startDegradedService(
	ln net.Listener,
	state func() *localpb.DegradedState,
	version string,
	protocol uint32,
	readPeerCred func(net.Conn) (Caller, error),
	allow func(Caller) bool,
	log *slog.Logger,
) (*degradedService, error) {
	dup, err := duplicateListener(ln)
	if err != nil {
		return nil, fmt.Errorf("duplicate the local socket listener: %w", err)
	}
	if log == nil {
		log = slog.Default()
	}
	if readPeerCred == nil {
		readPeerCred = ReadPeerCred
	}
	conns := newAccountLimit(localConnLimit, EvLocalConnectionLimitReached, log)
	conns.totalLimit = localTotalConnLimit
	registers := newAccountLimit(localRegisterLimit, EvRegisterLimitReached, log)
	creds := newPeerCredCreds(readPeerCred, allow, func(c Caller) {
		log.Warn(EvConnectionRejected, "uid", c.UID, "pid", c.PID)
	}, conns)

	// The version interceptors must run before the degraded ones, so a
	// connection already refused by Hello sees version_mismatch rather than
	// broker_degraded.
	opts := []grpc.ServerOption{
		grpc.Creds(creds),
		grpc.ConnectionTimeout(localHandshakeTimeout),
		// The diagnostics-only service carries the same per-connection stream
		// limit as the full one, so a client cannot use it to open streams
		// without bound while the Broker is degraded.
		grpc.MaxConcurrentStreams(uint32(localStreamLimit)),
		grpc.WaitForHandlers(true),
	}
	opts = append(opts, versionServerOptions()...)
	opts = append(opts,
		grpc.ChainUnaryInterceptor(degradedUnaryInterceptor(state)),
		grpc.ChainStreamInterceptor(degradedStreamInterceptor(state)),
	)

	s := &degradedService{
		state:     state,
		version:   version,
		protocol:  protocol,
		log:       log,
		registers: registers,
		srv:       grpc.NewServer(opts...),
		ln:        dup,
		done:      make(chan struct{}),
	}
	localpb.RegisterLocalServer(s.srv, s)
	go func() {
		defer close(s.done)
		if err := s.srv.Serve(dup); err != nil &&
			!errors.Is(err, grpc.ErrServerStopped) &&
			!errors.Is(err, net.ErrClosed) {
			log.Warn(evLocalServeFailed, "error", err)
		}
	}()
	return s, nil
}

// Stop closes every connection the degraded service accepted and stops using
// its duplicate listener. The original listener is unaffected.
func (s *degradedService) Stop() {
	if s == nil {
		return
	}
	s.once.Do(func() {
		s.srv.Stop()
		<-s.done
	})
}

// duplicateListener returns a second listener over the same bound UNIX socket.
// The original listener keeps its pollable file descriptor, because the test
// below never puts it into blocking mode.
func duplicateListener(ln net.Listener) (net.Listener, error) {
	ul, ok := ln.(*net.UnixListener)
	if !ok {
		return nil, fmt.Errorf("local socket listener is %T, not a UNIX listener", ln)
	}
	rc, err := ul.SyscallConn()
	if err != nil {
		return nil, err
	}
	var dupFd int
	var dupErr error
	if err := rc.Control(func(fd uintptr) {
		d, errno := syscall.Dup(int(fd))
		if errno != nil {
			dupErr = errno
			return
		}
		syscall.CloseOnExec(d)
		dupFd = d
	}); err != nil {
		return nil, err
	}
	if dupErr != nil {
		return nil, os.NewSyscallError("dup", dupErr)
	}
	f := os.NewFile(uintptr(dupFd), "broker.sock")
	defer func() { _ = f.Close() }()
	return net.FileListener(f)
}

// Hello performs the local protocol version handshake on a diagnostics-only
// Broker. Its behavior matches the full Broker's Hello.
func (s *degradedService) Hello(ctx context.Context, req *localpb.HelloRequest) (*localpb.HelloResponse, error) {
	v := s.protocol
	if v == 0 {
		v = localpb.ProtocolVersion
	}
	return answerHello(ctx, v, req.GetProtocolVersion(), s.log)
}

// Status reports only the current version and the failure while diagnostics
// only.
func (s *degradedService) Status(context.Context, *localpb.StatusRequest) (*localpb.StatusResponse, error) {
	return &localpb.StatusResponse{
		CurrentVersion: s.version,
		Degraded:       s.currentState(),
	}, nil
}

// Register answers a registration with not_registered/broker_degraded and
// keeps the stream open until the client goes away.
func (s *degradedService) Register(stream localpb.Local_RegisterServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	caller, ok := CallerFrom(stream.Context())
	if !ok {
		return toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_INTERNAL,
			"no caller identified on this connection"))
	}
	if s.registers != nil {
		key, err := accountKeyFor(caller.UID)
		if err != nil {
			return toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_INTERNAL,
				"cannot register: looking up account %d: %v", caller.UID, err))
		}
		if !s.registers.acquire(key, caller.UID) {
			return toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED,
				"cannot register: account %s already has %d open registrations",
				callerAccount(caller), s.registers.limit))
		}
		defer s.registers.release(key)
	}
	detail := ""
	if d := s.currentState(); d != nil {
		detail = d.Reason
	}
	if err := stream.Send(&localpb.RegisterResponse{
		Event: &localpb.RegisterResponse_NotRegistered{
			NotRegistered: &localpb.RegistrationProblem{
				Reason: localpb.NotRegisteredReason_NOT_REGISTERED_REASON_BROKER_DEGRADED,
				Detail: detail,
			},
		},
	}); err != nil {
		return err
	}
	for {
		if _, err := stream.Recv(); err != nil {
			return nil
		}
	}
}

func (s *degradedService) currentState() *localpb.DegradedState {
	if s.state == nil {
		return nil
	}
	return s.state()
}

func degradedUnaryInterceptor(state func() *localpb.DegradedState) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		switch info.FullMethod {
		case localpb.Local_Hello_FullMethodName, localpb.Local_Status_FullMethodName:
			return handler(ctx, req)
		default:
			return nil, degradedCallError(state)
		}
	}
}

func degradedStreamInterceptor(state func() *localpb.DegradedState) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if info.FullMethod == localpb.Local_Register_FullMethodName {
			return handler(srv, ss)
		}
		return degradedCallError(state)
	}
}

func degradedCallError(state func() *localpb.DegradedState) error {
	reason := "the Broker is degraded"
	if state != nil {
		if d := state(); d != nil && d.Reason != "" {
			reason = d.Reason
		}
	}
	return toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_BROKER_DEGRADED, "%s", reason))
}
