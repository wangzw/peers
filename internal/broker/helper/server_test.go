package helper

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/wangzw/peers/internal/broker/helperpb"
	"github.com/wangzw/peers/internal/inbox"
)

type serveSide struct {
	t      *testing.T
	ops    *fakeOps
	reqW   *io.PipeWriter
	req    *msgWriter
	respR  *io.PipeReader
	resp   *msgReader
	cancel context.CancelFunc
	done   chan error
}

func newServeSide(t *testing.T, ops *fakeOps, self Identity) *serveSide {
	t.Helper()
	reqR, reqW := io.Pipe()
	respR, respW := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	side := &serveSide{
		t:      t,
		ops:    ops,
		reqW:   reqW,
		req:    newMsgWriter(reqW),
		respR:  respR,
		resp:   newMsgReader(respR),
		cancel: cancel,
		done:   make(chan error, 1),
	}
	go func() {
		err := Serve(ctx, reqR, respW, ops, self)
		_ = respW.Close()
		side.done <- err
	}()
	t.Cleanup(func() {
		cancel()
		_ = reqW.Close()
		_ = respR.Close()
	})
	return side
}

func (s *serveSide) readReady() *helperpb.Ready {
	s.t.Helper()
	m := s.recv()
	ready := m.GetReady()
	if ready == nil {
		s.t.Fatalf("first message body is %T, want Ready", m.GetBody())
	}
	return ready
}

func (s *serveSide) send(req *helperpb.HelperRequest) {
	s.t.Helper()
	if err := s.req.write(req); err != nil {
		s.t.Fatalf("writing request: %v", err)
	}
}

func (s *serveSide) recv() *helperpb.HelperMessage {
	s.t.Helper()
	type result struct {
		msg *helperpb.HelperMessage
		err error
	}
	ch := make(chan result, 1)
	go func() {
		var m helperpb.HelperMessage
		err := s.resp.read(&m)
		ch <- result{msg: &m, err: err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			s.t.Fatalf("reading message: %v", r.err)
		}
		return r.msg
	case <-time.After(processTimeout):
		s.t.Fatal("timed out waiting for a helper message")
		return nil
	}
}

func (s *serveSide) waitDone() error {
	s.t.Helper()
	select {
	case err := <-s.done:
		return err
	case <-time.After(processTimeout):
		s.t.Fatal("Serve did not return")
		return nil
	}
}

func responseOf(t *testing.T, m *helperpb.HelperMessage) *helperpb.HelperResponse {
	t.Helper()
	resp := m.GetResponse()
	if resp == nil {
		t.Fatalf("message body is %T, want a response", m.GetBody())
	}
	return resp
}

func watchEventOf(t *testing.T, m *helperpb.HelperMessage) *helperpb.WatchEvent {
	t.Helper()
	event := m.GetWatchEvent()
	if event == nil {
		t.Fatalf("message body is %T, want a watch event", m.GetBody())
	}
	return event
}

func watchRequest(id uint64, configDir string) *helperpb.HelperRequest {
	return &helperpb.HelperRequest{
		Id: id,
		Op: &helperpb.HelperRequest_Watch{Watch: &helperpb.WatchRequest{ConfigDir: configDir}},
	}
}

func unwatchRequest(id, watchID uint64) *helperpb.HelperRequest {
	return &helperpb.HelperRequest{
		Id: id,
		Op: &helperpb.HelperRequest_Unwatch{Unwatch: &helperpb.UnwatchRequest{WatchId: watchID}},
	}
}

func lookupRequest(id uint64, configDir, socketPath string) *helperpb.HelperRequest {
	return &helperpb.HelperRequest{
		Id: id,
		Op: &helperpb.HelperRequest_Lookup{Lookup: &helperpb.LookupRequest{
			ConfigDir:  configDir,
			SocketPath: socketPath,
		}},
	}
}

func deliverRequest(id uint64, msgID string, timeout time.Duration) *helperpb.HelperRequest {
	return &helperpb.HelperRequest{
		Id: id,
		Op: &helperpb.HelperRequest_Deliver{Deliver: &helperpb.DeliverRequest{
			Session:   &helperpb.Session{ConfigDir: "/c", Pid: 4242, SocketPath: "/s"},
			Signature: "sig",
			Text:      "text",
			MessageId: msgID,
			Timeout:   durationpb.New(timeout),
		}},
	}
}

func cancelRequest(id, requestID uint64) *helperpb.HelperRequest {
	return &helperpb.HelperRequest{
		Id: id,
		Op: &helperpb.HelperRequest_Cancel{Cancel: &helperpb.CancelRequest{RequestId: requestID}},
	}
}

func deliveredCount(f *fakeOps) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.delivered)
}

func stoppedCount(f *fakeOps, configDir string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stopped[configDir]
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(processTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestCurrentIdentityMatchesProcess checks that CurrentIdentity reports this process's ids and a sorted unique group list.
func TestCurrentIdentityMatchesProcess(t *testing.T) {
	got := CurrentIdentity()
	if got.UID != uint32(os.Getuid()) {
		t.Errorf("UID = %d, want %d", got.UID, os.Getuid())
	}
	if got.GID != uint32(os.Getgid()) {
		t.Errorf("GID = %d, want %d", got.GID, os.Getgid())
	}
	for i := 1; i < len(got.Groups); i++ {
		if got.Groups[i-1] >= got.Groups[i] {
			t.Fatalf("Groups = %v, want strictly ascending and unique", got.Groups)
		}
	}
}

// TestServeSendsReadyFirst checks that Serve writes the protocol version and the helper's identity as its first message.
func TestServeSendsReadyFirst(t *testing.T) {
	self := Identity{UID: 1234, GID: 5678, Groups: []uint32{3, 9}}
	side := newServeSide(t, &fakeOps{}, self)

	ready := side.readReady()
	if ready.GetProtocolVersion() != helperpb.ProtocolVersion {
		t.Errorf("ProtocolVersion = %d, want %d", ready.GetProtocolVersion(), helperpb.ProtocolVersion)
	}
	if ready.GetUid() != self.UID {
		t.Errorf("Uid = %d, want %d", ready.GetUid(), self.UID)
	}
	if ready.GetGid() != self.GID {
		t.Errorf("Gid = %d, want %d", ready.GetGid(), self.GID)
	}
}

// TestServeLookupMapsNoRecord checks that a lookup with no record answers with the NO_RECORD error code.
func TestServeLookupMapsNoRecord(t *testing.T) {
	side := newServeSide(t, &fakeOps{}, Identity{UID: 1})
	side.readReady()

	side.send(lookupRequest(3, "/c", "/s"))
	resp := responseOf(t, side.recv())
	if resp.GetId() != 3 {
		t.Errorf("response id = %d, want 3", resp.GetId())
	}
	e := resp.GetError()
	if e == nil || e.GetCode() != helperpb.HelperErrorCode_HELPER_ERROR_CODE_NO_RECORD {
		t.Fatalf("response = %v, want NO_RECORD", resp)
	}
}

// TestServeLookupReturnsSession checks that a lookup answers with the session record's fields.
func TestServeLookupReturnsSession(t *testing.T) {
	ops := &fakeOps{sessions: map[string]inbox.Session{
		"/s": {ConfigDir: "/c", PID: 4242, Name: "a", Status: "busy", SocketPath: "/s"},
	}}
	side := newServeSide(t, ops, Identity{UID: 1})
	side.readReady()

	side.send(lookupRequest(4, "/c", "/s"))
	resp := responseOf(t, side.recv())
	sess := resp.GetSession()
	if sess == nil {
		t.Fatalf("response = %v, want a session", resp)
	}
	if sess.GetConfigDir() != "/c" || sess.GetPid() != 4242 || sess.GetName() != "a" ||
		sess.GetStatus() != "busy" || sess.GetSocketPath() != "/s" {
		t.Fatalf("session = %v, want the fakeOps record", sess)
	}
}

// TestServeDeliverErrorCodes checks that each delivery outcome maps to its wire error code, with Done for success.
func TestServeDeliverErrorCodes(t *testing.T) {
	errs := []error{nil, inbox.ErrNotWritten, inbox.ErrUncertain, inbox.ErrBadSignature, fmt.Errorf("x: %w", inbox.ErrUncertain)}
	var mu sync.Mutex
	next := 0
	ops := &fakeOps{deliverFn: func(context.Context, inbox.Session, string, string, string, time.Duration) error {
		mu.Lock()
		defer mu.Unlock()
		err := errs[next]
		next++
		return err
	}}
	side := newServeSide(t, ops, Identity{UID: 1})
	side.readReady()

	want := []helperpb.HelperErrorCode{
		0, // Done
		helperpb.HelperErrorCode_HELPER_ERROR_CODE_NOT_WRITTEN,
		helperpb.HelperErrorCode_HELPER_ERROR_CODE_UNCERTAIN,
		helperpb.HelperErrorCode_HELPER_ERROR_CODE_BAD_SIGNATURE,
		helperpb.HelperErrorCode_HELPER_ERROR_CODE_UNCERTAIN,
	}
	for i := range errs {
		id := uint64(i + 1)
		side.send(deliverRequest(id, fmt.Sprintf("m%d", i), time.Second))
		resp := responseOf(t, side.recv())
		if resp.GetId() != id {
			t.Fatalf("case %d: response id = %d, want %d", i, resp.GetId(), id)
		}
		if want[i] == 0 {
			if resp.GetDone() == nil {
				t.Fatalf("case %d: response = %v, want Done", i, resp)
			}
			continue
		}
		e := resp.GetError()
		if e == nil || e.GetCode() != want[i] {
			t.Fatalf("case %d: response = %v, want code %v", i, resp, want[i])
		}
	}
}

// TestServeConcurrentRequestsAnsweredById checks that a fast request is answered while a slow one is still running.
func TestServeConcurrentRequestsAnsweredById(t *testing.T) {
	release := make(chan struct{})
	ops := &fakeOps{deliverFn: func(_ context.Context, _ inbox.Session, _, _, msgID string, _ time.Duration) error {
		if msgID == "slow" {
			<-release
		}
		return nil
	}}
	side := newServeSide(t, ops, Identity{UID: 1})
	side.readReady()

	side.send(deliverRequest(1, "slow", time.Second))
	waitUntil(t, "the slow delivery to start", func() bool { return deliveredCount(ops) == 1 })
	side.send(deliverRequest(2, "fast", time.Second))

	resp := responseOf(t, side.recv())
	if resp.GetId() != 2 || resp.GetDone() == nil {
		t.Fatalf("first response = %v, want Done for id 2", resp)
	}

	close(release)
	resp = responseOf(t, side.recv())
	if resp.GetId() != 1 || resp.GetDone() == nil {
		t.Fatalf("second response = %v, want Done for id 1", resp)
	}
}

// TestServeCancelStopsDeliver checks that a cancel request ends the delivery with CANCELED and produces no second answer.
func TestServeCancelStopsDeliver(t *testing.T) {
	ops := &fakeOps{deliverFn: func(ctx context.Context, _ inbox.Session, _, _, _ string, _ time.Duration) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	side := newServeSide(t, ops, Identity{UID: 1})
	side.readReady()

	side.send(deliverRequest(5, "m", time.Second))
	waitUntil(t, "the delivery to start", func() bool { return deliveredCount(ops) == 1 })
	side.send(cancelRequest(6, 5))

	resp := responseOf(t, side.recv())
	if resp.GetId() != 5 {
		t.Fatalf("response id = %d, want 5", resp.GetId())
	}
	if e := resp.GetError(); e == nil || e.GetCode() != helperpb.HelperErrorCode_HELPER_ERROR_CODE_CANCELED {
		t.Fatalf("response = %v, want CANCELED", resp)
	}

	side.send(lookupRequest(7, "/c", "/s"))
	resp = responseOf(t, side.recv())
	if resp.GetId() != 7 {
		t.Fatalf("follow-up response id = %d, want 7", resp.GetId())
	}
}

// TestServeWatchEventsFollowStarted checks that WatchStarted is written before any event queued during the watch setup.
func TestServeWatchEventsFollowStarted(t *testing.T) {
	ops := &fakeOps{fireOnWatch: 42}
	side := newServeSide(t, ops, Identity{UID: 1})
	side.readReady()

	side.send(watchRequest(1, "/c"))

	resp := responseOf(t, side.recv())
	started := resp.GetWatchStarted()
	if resp.GetId() != 1 || started == nil || started.GetWatchId() != 1 {
		t.Fatalf("first response = %v, want WatchStarted{1}", resp)
	}

	event := watchEventOf(t, side.recv())
	if event.GetWatchId() != 1 || event.GetPid() != 42 {
		t.Fatalf("event = %v, want {watch 1, pid 42}", event)
	}
}

// TestServeUnwatchStops checks that unwatch stops the callback, drops late events and refuses an unknown watch id.
func TestServeUnwatchStops(t *testing.T) {
	ops := &fakeOps{}
	side := newServeSide(t, ops, Identity{UID: 1})
	side.readReady()

	side.send(watchRequest(1, "/c"))
	if resp := responseOf(t, side.recv()); resp.GetWatchStarted() == nil {
		t.Fatalf("response = %v, want WatchStarted", resp)
	}

	side.send(unwatchRequest(2, 1))
	if resp := responseOf(t, side.recv()); resp.GetDone() == nil {
		t.Fatalf("response = %v, want Done", resp)
	}
	if got := stoppedCount(ops, "/c"); got != 1 {
		t.Fatalf("stop count = %d, want 1", got)
	}

	ops.fire("/c", 7)
	side.send(lookupRequest(3, "/c", "/s"))
	if resp := responseOf(t, side.recv()); resp.GetId() != 3 {
		t.Fatalf("response id = %d, want 3 (an event leaked after unwatch)", resp.GetId())
	}

	side.send(unwatchRequest(4, 99))
	resp := responseOf(t, side.recv())
	if e := resp.GetError(); e == nil || e.GetCode() != helperpb.HelperErrorCode_HELPER_ERROR_CODE_BAD_REQUEST {
		t.Fatalf("response = %v, want BAD_REQUEST for an unknown watch", resp)
	}
}

// TestServeDuplicateIdRejected checks that a request repeating an in-flight id is answered with BAD_REQUEST.
func TestServeDuplicateIdRejected(t *testing.T) {
	release := make(chan struct{})
	ops := &fakeOps{deliverFn: func(_ context.Context, _ inbox.Session, _, _, msgID string, _ time.Duration) error {
		if msgID == "first" {
			<-release
		}
		return nil
	}}
	side := newServeSide(t, ops, Identity{UID: 1})
	side.readReady()

	side.send(deliverRequest(1, "first", time.Second))
	waitUntil(t, "the first delivery to start", func() bool { return deliveredCount(ops) == 1 })
	side.send(deliverRequest(1, "second", time.Second))

	resp := responseOf(t, side.recv())
	if e := resp.GetError(); e == nil || e.GetCode() != helperpb.HelperErrorCode_HELPER_ERROR_CODE_BAD_REQUEST {
		t.Fatalf("response = %v, want BAD_REQUEST for the duplicate id", resp)
	}

	close(release)
	resp = responseOf(t, side.recv())
	if resp.GetId() != 1 || resp.GetDone() == nil {
		t.Fatalf("response = %v, want Done for the first request", resp)
	}
}

// TestServeEOFCleansUp checks that a clean EOF cancels in-flight deliveries, stops watches and returns nil.
func TestServeEOFCleansUp(t *testing.T) {
	canceled := make(chan struct{}, 1)
	ops := &fakeOps{deliverFn: func(ctx context.Context, _ inbox.Session, _, _, _ string, _ time.Duration) error {
		<-ctx.Done()
		canceled <- struct{}{}
		return ctx.Err()
	}}
	side := newServeSide(t, ops, Identity{UID: 1})
	side.readReady()

	side.send(deliverRequest(1, "m", time.Second))
	waitUntil(t, "the delivery to start", func() bool { return deliveredCount(ops) == 1 })
	side.send(watchRequest(2, "/c"))
	if resp := responseOf(t, side.recv()); resp.GetWatchStarted() == nil {
		t.Fatalf("response = %v, want WatchStarted", resp)
	}

	_ = side.respR.Close()
	_ = side.reqW.Close()

	if err := side.waitDone(); err != nil {
		t.Fatalf("Serve returned %v after a clean EOF", err)
	}
	select {
	case <-canceled:
	case <-time.After(processTimeout):
		t.Fatal("in-flight delivery was not canceled")
	}
	if got := stoppedCount(ops, "/c"); got != 1 {
		t.Fatalf("stop count = %d, want 1", got)
	}
}

// TestServeTruncatedInputIsError checks that a message cut in half ends Serve with io.ErrUnexpectedEOF.
func TestServeTruncatedInputIsError(t *testing.T) {
	side := newServeSide(t, &fakeOps{}, Identity{UID: 1})
	side.readReady()

	var buf bytes.Buffer
	req := lookupRequest(1, "/c", "/s")
	if _, err := protodelim.MarshalTo(&buf, req); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	half := buf.Bytes()[:buf.Len()/2]
	if _, err := side.reqW.Write(half); err != nil {
		t.Fatalf("writing half a message: %v", err)
	}
	_ = side.reqW.Close()

	if err := side.waitDone(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Serve returned %v, want io.ErrUnexpectedEOF", err)
	}
}
