package helper

import (
	"context"
	"errors"
	"io"
	"os"
	"slices"
	"sync"

	"github.com/wangzw/peers/internal/broker/helperpb"
	"github.com/wangzw/peers/internal/inbox"
)

// Identity is the credential the helper process runs with, reported in Ready.
type Identity struct {
	// UID is the process's real user id.
	UID uint32
	// GID is the process's real group id.
	GID uint32
	// Groups is the process's sorted, unique supplementary group list.
	Groups []uint32
}

// CurrentIdentity returns the credential of the current process.
func CurrentIdentity() Identity {
	groups, _ := os.Getgroups()
	ids := make([]uint32, 0, len(groups))
	for _, g := range groups {
		ids = append(ids, uint32(g))
	}
	slices.Sort(ids)
	ids = slices.Compact(ids)
	return Identity{
		UID:    uint32(os.Getuid()),
		GID:    uint32(os.Getgid()),
		Groups: ids,
	}
}

type serveWatch struct {
	configDir string
	stop      func()
	// started is set once WatchStarted has been written; until then events queue in pending because the Broker drops a watch id it has not learned yet.
	started bool
	pending []int64
}

type server struct {
	ctx    context.Context
	cancel context.CancelFunc
	w      *msgWriter
	ops    SessionOps
	self   Identity

	mu        sync.Mutex
	closed    bool
	inflight  map[uint64]struct{}
	cancels   map[uint64]context.CancelFunc
	watches   map[uint64]*serveWatch
	nextWatch uint64

	wg       sync.WaitGroup
	failOnce sync.Once
	failCh   chan error
}

func newServer(ctx context.Context, w io.Writer, ops SessionOps, self Identity) *server {
	runCtx, cancel := context.WithCancel(ctx)
	return &server{
		ctx:      runCtx,
		cancel:   cancel,
		w:        newMsgWriter(w),
		ops:      ops,
		self:     self,
		inflight: make(map[uint64]struct{}),
		cancels:  make(map[uint64]context.CancelFunc),
		watches:  make(map[uint64]*serveWatch),
		failCh:   make(chan error, 1),
	}
}

// Serve runs the helper protocol until r reaches EOF, ctx ends, or writing to w fails.
// It returns nil after a clean EOF and an error otherwise.
func Serve(ctx context.Context, r io.Reader, w io.Writer, ops SessionOps, self Identity) error {
	s := newServer(ctx, w, ops, self)
	if err := s.writeReady(); err != nil {
		s.cleanup()
		return err
	}

	readDone := make(chan error, 1)
	go func() { readDone <- s.readLoop(r) }()

	select {
	case err := <-readDone:
		s.cleanup()
		return err
	case err := <-s.failCh:
		s.cleanup()
		return err
	case <-s.ctx.Done():
		s.cleanup()
		return s.ctx.Err()
	}
}

func (s *server) writeReady() error {
	return s.w.write(&helperpb.HelperMessage{
		Body: &helperpb.HelperMessage_Ready{Ready: &helperpb.Ready{
			ProtocolVersion: helperpb.ProtocolVersion,
			Uid:             s.self.UID,
			Gid:             s.self.GID,
			Groups:          s.self.Groups,
		}},
	})
}

func (s *server) readLoop(r io.Reader) error {
	rd := newMsgReader(r)
	for {
		var req helperpb.HelperRequest
		if err := rd.read(&req); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := s.handle(&req); err != nil {
			return err
		}
	}
}

func (s *server) handle(req *helperpb.HelperRequest) error {
	if !hasOperation(req) {
		s.respondError(req.GetId(), helperpb.HelperErrorCode_HELPER_ERROR_CODE_BAD_REQUEST,
			errors.New("request has no operation"))
		return nil
	}
	id := req.GetId()
	if id == 0 {
		s.respondError(0, helperpb.HelperErrorCode_HELPER_ERROR_CODE_BAD_REQUEST,
			errors.New("request id is zero"))
		return nil
	}

	switch op := req.GetOp().(type) {
	case *helperpb.HelperRequest_Lookup:
		if !s.startRequest(id, true) {
			return nil
		}
		go func() {
			defer s.wg.Done()
			s.serveLookup(id, op.Lookup)
		}()
	case *helperpb.HelperRequest_Reread:
		if !s.startRequest(id, true) {
			return nil
		}
		go func() {
			defer s.wg.Done()
			s.serveReread(id, op.Reread)
		}()
	case *helperpb.HelperRequest_Deliver:
		d := op.Deliver
		if d.GetTimeout() == nil || d.GetTimeout().AsDuration() <= 0 {
			s.respondError(id, helperpb.HelperErrorCode_HELPER_ERROR_CODE_BAD_REQUEST,
				errors.New("deliver timeout is missing or not positive"))
			return nil
		}
		if !s.startRequest(id, true) {
			return nil
		}
		go func() {
			defer s.wg.Done()
			s.serveDeliver(id, d)
		}()
	case *helperpb.HelperRequest_Watch:
		if !s.startRequest(id, false) {
			return nil
		}
		s.serveWatch(id, op.Watch)
	case *helperpb.HelperRequest_Unwatch:
		if !s.startRequest(id, false) {
			return nil
		}
		s.serveUnwatch(id, op.Unwatch)
	case *helperpb.HelperRequest_Cancel:
		if !s.startRequest(id, false) {
			return nil
		}
		// Cancel carries no response of its own; release the id before acting.
		s.endRequest(id)
		s.serveCancel(op.Cancel)
	}
	return nil
}

func (s *server) startRequest(id uint64, async bool) bool {
	switch s.beginRequest(id, async) {
	case requestClosed:
		return false
	case requestDuplicate:
		s.respondDuplicate(id)
		return false
	}
	return true
}

func hasOperation(req *helperpb.HelperRequest) bool {
	switch req.GetOp().(type) {
	case *helperpb.HelperRequest_Lookup,
		*helperpb.HelperRequest_Reread,
		*helperpb.HelperRequest_Watch,
		*helperpb.HelperRequest_Unwatch,
		*helperpb.HelperRequest_Deliver,
		*helperpb.HelperRequest_Cancel:
		return true
	}
	return false
}

type requestStart int

// Outcomes of beginRequest.
const (
	requestStarted requestStart = iota
	requestDuplicate
	requestClosed
)

// beginRequest records id as in flight; the closed and duplicate checks and the WaitGroup increment all happen under mu so cleanup cannot race a new request.
func (s *server) beginRequest(id uint64, async bool) requestStart {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return requestClosed
	}
	if _, ok := s.inflight[id]; ok {
		return requestDuplicate
	}
	s.inflight[id] = struct{}{}
	if async {
		s.wg.Add(1)
	}
	return requestStarted
}

func (s *server) endRequest(id uint64) {
	s.mu.Lock()
	delete(s.inflight, id)
	s.mu.Unlock()
}

func (s *server) respondDuplicate(id uint64) {
	s.respondError(id, helperpb.HelperErrorCode_HELPER_ERROR_CODE_BAD_REQUEST,
		errors.New("duplicate request id"))
}

func (s *server) serveLookup(id uint64, req *helperpb.LookupRequest) {
	defer s.endRequest(id)
	sess, err := s.ops.Lookup(req.GetConfigDir(), req.GetSocketPath())
	if err != nil {
		s.respondError(id, errorCode(err), err)
		return
	}
	s.respondSession(id, sess)
}

func (s *server) serveReread(id uint64, req *helperpb.RereadRequest) {
	defer s.endRequest(id)
	sess, err := s.ops.Reread(fromProtoSession(req.GetSession()))
	if err != nil {
		s.respondError(id, errorCode(err), err)
		return
	}
	s.respondSession(id, sess)
}

func (s *server) serveDeliver(id uint64, req *helperpb.DeliverRequest) {
	ctx, cancel := context.WithCancel(s.ctx)
	s.mu.Lock()
	s.cancels[id] = cancel
	s.mu.Unlock()

	defer func() {
		cancel()
		s.mu.Lock()
		delete(s.cancels, id)
		s.mu.Unlock()
		s.endRequest(id)
	}()

	err := s.ops.Deliver(ctx, fromProtoSession(req.GetSession()), req.GetSignature(),
		req.GetText(), req.GetMessageId(), req.GetTimeout().AsDuration())
	if err != nil {
		s.respondError(id, errorCode(err), err)
		return
	}
	s.respondDone(id)
}

func (s *server) serveWatch(id uint64, req *helperpb.WatchRequest) {
	defer s.endRequest(id)
	configDir := req.GetConfigDir()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.nextWatch++
	watchID := s.nextWatch
	entry := &serveWatch{configDir: configDir}
	s.watches[watchID] = entry
	s.mu.Unlock()

	onChange := func(pid int) {
		s.mu.Lock()
		defer s.mu.Unlock()
		e, ok := s.watches[watchID]
		if !ok {
			return
		}
		if !e.started {
			e.pending = append(e.pending, int64(pid))
			return
		}
		s.respondWatchEvent(watchID, int64(pid))
	}

	stop, err := s.ops.Watch(configDir, onChange)
	if err != nil {
		s.mu.Lock()
		delete(s.watches, watchID)
		s.mu.Unlock()
		s.respondError(id, helperpb.HelperErrorCode_HELPER_ERROR_CODE_FAILED, err)
		return
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		if stop != nil {
			stop()
		}
		return
	}
	entry.stop = stop
	entry.started = true
	// Write WatchStarted under mu so no event reaches the Broker before it knows the watch id; events queued during ops.Watch go out after it.
	s.respondWatchStarted(id, watchID)
	pending := entry.pending
	entry.pending = nil
	for _, pid := range pending {
		s.respondWatchEvent(watchID, pid)
	}
	s.mu.Unlock()
}

func (s *server) serveUnwatch(id uint64, req *helperpb.UnwatchRequest) {
	defer s.endRequest(id)

	s.mu.Lock()
	entry, ok := s.watches[req.GetWatchId()]
	if !ok {
		s.mu.Unlock()
		s.respondError(id, helperpb.HelperErrorCode_HELPER_ERROR_CODE_BAD_REQUEST,
			errors.New("unknown watch id"))
		return
	}
	delete(s.watches, req.GetWatchId())
	s.mu.Unlock()

	if entry.stop != nil {
		entry.stop()
	}
	s.respondDone(id)
}

func (s *server) serveCancel(req *helperpb.CancelRequest) {
	s.mu.Lock()
	cancel := s.cancels[req.GetRequestId()]
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *server) cleanup() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.wg.Wait()
		return
	}
	s.closed = true
	watches := make([]*serveWatch, 0, len(s.watches))
	for id, entry := range s.watches {
		watches = append(watches, entry)
		delete(s.watches, id)
	}
	s.mu.Unlock()

	s.cancel()
	for _, entry := range watches {
		if entry.stop != nil {
			entry.stop()
		}
	}
	s.wg.Wait()
}

func (s *server) respondDone(id uint64) {
	s.respond(&helperpb.HelperResponse{
		Id:     id,
		Result: &helperpb.HelperResponse_Done{Done: &helperpb.Done{}},
	})
}

func (s *server) respondSession(id uint64, sess inbox.Session) {
	s.respond(&helperpb.HelperResponse{
		Id:     id,
		Result: &helperpb.HelperResponse_Session{Session: toProtoSession(sess)},
	})
}

func (s *server) respondWatchStarted(id, watchID uint64) {
	s.respond(&helperpb.HelperResponse{
		Id:     id,
		Result: &helperpb.HelperResponse_WatchStarted{WatchStarted: &helperpb.WatchStarted{WatchId: watchID}},
	})
}

func (s *server) respondError(id uint64, code helperpb.HelperErrorCode, err error) {
	s.respond(&helperpb.HelperResponse{
		Id: id,
		Result: &helperpb.HelperResponse_Error{Error: &helperpb.HelperError{
			Code:    code,
			Message: err.Error(),
		}},
	})
}

func (s *server) respondWatchEvent(watchID uint64, pid int64) {
	s.writeMessage(&helperpb.HelperMessage{
		Body: &helperpb.HelperMessage_WatchEvent{WatchEvent: &helperpb.WatchEvent{
			WatchId: watchID,
			Pid:     pid,
		}},
	})
}

func (s *server) respond(resp *helperpb.HelperResponse) {
	s.writeMessage(&helperpb.HelperMessage{
		Body: &helperpb.HelperMessage_Response{Response: resp},
	})
}

func (s *server) writeMessage(msg *helperpb.HelperMessage) {
	if err := s.w.write(msg); err != nil {
		s.noteWriteError(err)
	}
}

func (s *server) noteWriteError(err error) {
	s.failOnce.Do(func() {
		s.failCh <- err
	})
}

// errorCode maps an inbox outcome to the wire error code, using errors.Is so wrapped sentinels keep their meaning.
func errorCode(err error) helperpb.HelperErrorCode {
	switch {
	case errors.Is(err, inbox.ErrNoRecord):
		return helperpb.HelperErrorCode_HELPER_ERROR_CODE_NO_RECORD
	case errors.Is(err, inbox.ErrPartial):
		return helperpb.HelperErrorCode_HELPER_ERROR_CODE_PARTIAL
	case errors.Is(err, inbox.ErrBadSignature):
		return helperpb.HelperErrorCode_HELPER_ERROR_CODE_BAD_SIGNATURE
	case errors.Is(err, inbox.ErrNotWritten):
		return helperpb.HelperErrorCode_HELPER_ERROR_CODE_NOT_WRITTEN
	case errors.Is(err, inbox.ErrUncertain):
		return helperpb.HelperErrorCode_HELPER_ERROR_CODE_UNCERTAIN
	case errors.Is(err, context.Canceled):
		return helperpb.HelperErrorCode_HELPER_ERROR_CODE_CANCELED
	default:
		return helperpb.HelperErrorCode_HELPER_ERROR_CODE_FAILED
	}
}

func fromProtoSession(s *helperpb.Session) inbox.Session {
	if s == nil {
		return inbox.Session{}
	}
	return inbox.Session{
		ConfigDir:  s.GetConfigDir(),
		PID:        int(s.GetPid()),
		Name:       s.GetName(),
		Status:     s.GetStatus(),
		SocketPath: s.GetSocketPath(),
	}
}

func toProtoSession(s inbox.Session) *helperpb.Session {
	return &helperpb.Session{
		ConfigDir:  s.ConfigDir,
		Pid:        int64(s.PID),
		Name:       s.Name,
		Status:     s.Status,
		SocketPath: s.SocketPath,
	}
}

// RunAccountHelper is the body of `peers internal account-helper`.
func RunAccountHelper(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	// Switch identity before serving and before CurrentIdentity reads it; a failed switch exits nonzero without serving as root.
	if err := enterHelperAccount(); err != nil {
		return err
	}
	return Serve(ctx, stdin, stdout, InboxOps{}, CurrentIdentity())
}
