package mcpserver

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"

	"google.golang.org/grpc"

	"github.com/wangzw/peers/internal/broker/localpb"
)

type fakeRegisterClient struct {
	localpb.LocalClient
	openErr error
	stream  *fakeRegisterStream
}

func (c *fakeRegisterClient) Register(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[localpb.RegisterRequest, localpb.RegisterResponse], error) {
	if c.openErr != nil {
		return nil, c.openErr
	}
	return c.stream, nil
}

type fakeRegisterStream struct {
	grpc.BidiStreamingClient[localpb.RegisterRequest, localpb.RegisterResponse]
	responses <-chan *localpb.RegisterResponse
	sendErr   error
	closed    bool
}

func (s *fakeRegisterStream) Send(*localpb.RegisterRequest) error {
	return s.sendErr
}

func (s *fakeRegisterStream) Recv() (*localpb.RegisterResponse, error) {
	resp, ok := <-s.responses
	if !ok {
		return nil, io.EOF
	}
	return resp, nil
}

func (s *fakeRegisterStream) CloseSend() error {
	s.closed = true
	return nil
}

// TestRunRegistrationReportsOpenError returns the error that refused the Register
// stream.
func TestRunRegistrationReportsOpenError(t *testing.T) {
	st := &statusStore{}
	err := runRegistration(t.Context(), &fakeRegisterClient{openErr: errors.New("stream refused")},
		&localpb.RegisterRequest{}, st, nil, nil)
	if err == nil || err.Error() != "stream refused" {
		t.Fatalf("runRegistration error = %v, want the open failure", err)
	}
}

// TestRunRegistrationReportsSendError returns the first send's error and closes
// the send side.
func TestRunRegistrationReportsSendError(t *testing.T) {
	stream := &fakeRegisterStream{
		responses: func() <-chan *localpb.RegisterResponse {
			ch := make(chan *localpb.RegisterResponse)
			close(ch)
			return ch
		}(),
		sendErr: errors.New("send failed"),
	}
	st := &statusStore{}
	err := runRegistration(t.Context(), &fakeRegisterClient{stream: stream},
		&localpb.RegisterRequest{}, st, nil, nil)
	if err == nil || err.Error() != "send failed" {
		t.Fatalf("runRegistration error = %v, want the send failure", err)
	}
	if !stream.closed {
		t.Fatal("runRegistration did not close the send side after a failed send")
	}
}

type recordHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, r)
	h.mu.Unlock()
	return nil
}
func (h *recordHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordHandler) messages() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, 0, len(h.records))
	for _, r := range h.records {
		out = append(out, r.Message)
	}
	return out
}

// TestRunRegistrationLogsUnknownEvent logs an unknown event and ends with the
// stream's EOF.
func TestRunRegistrationLogsUnknownEvent(t *testing.T) {
	ch := make(chan *localpb.RegisterResponse, 1)
	ch <- &localpb.RegisterResponse{}
	close(ch)
	stream := &fakeRegisterStream{responses: ch}
	handler := &recordHandler{}
	log := slog.New(handler)

	err := runRegistration(t.Context(), &fakeRegisterClient{stream: stream},
		&localpb.RegisterRequest{}, &statusStore{}, nil, log)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("runRegistration error = %v, want io.EOF", err)
	}
	want := "ignoring an unknown Register event"
	for _, message := range handler.messages() {
		if message == want {
			return
		}
	}
	t.Fatalf("log messages = %v, want %q", handler.messages(), want)
}
