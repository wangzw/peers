package termcmd

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakelocal"
)

// stubLocalClient answers only the calls a case overrides. The embedded nil
// client makes an unexpected call panic instead of returning a zero answer.
type stubLocalClient struct {
	localpb.LocalClient
	status    *localpb.StatusResponse
	statusErr error
	auditErr  error
	list      *localpb.ListBrokersResponse
	listErr   error
	update    *localpb.UpdateStatusResponse
	updateErr error
}

func (c *stubLocalClient) Hello(context.Context, *localpb.HelloRequest, ...grpc.CallOption) (*localpb.HelloResponse, error) {
	return &localpb.HelloResponse{ProtocolVersion: localpb.ProtocolVersion}, nil
}

func (c *stubLocalClient) Status(context.Context, *localpb.StatusRequest, ...grpc.CallOption) (*localpb.StatusResponse, error) {
	return c.status, c.statusErr
}

func (c *stubLocalClient) Audit(context.Context, *localpb.AuditRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[localpb.AuditEvent], error) {
	return nil, c.auditErr
}

func (c *stubLocalClient) ListBrokers(context.Context, *localpb.ListBrokersRequest, ...grpc.CallOption) (*localpb.ListBrokersResponse, error) {
	return c.list, c.listErr
}

func (c *stubLocalClient) UpdateStatus(context.Context, *localpb.UpdateStatusRequest, ...grpc.CallOption) (*localpb.UpdateStatusResponse, error) {
	return c.update, c.updateErr
}

// stubSelector returns a selector whose dialer hands out client over a real
// ClientConn, so the caller's deferred Close has something to close.
func stubSelector(t *testing.T, client localpb.LocalClient) Selector {
	t.Helper()
	return func(context.Context) (string, Dialer, error) {
		return "stub.sock", func() (*grpc.ClientConn, localpb.LocalClient, error) {
			cc, err := grpc.NewClient("unix://"+fakelocal.SocketPath(t),
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return nil, nil, err
			}
			return cc, client, nil
		}, nil
	}
}

// TestRunCommandsSelectorError reports a selector failure with the shared prefix
// from every command and writes nothing.
func TestRunCommandsSelectorError(t *testing.T) {
	const noBroker = "no Broker is available for this account"
	sel := func(context.Context) (string, Dialer, error) {
		return "", nil, errors.New(noBroker)
	}
	want := "cannot reach the Broker: " + noBroker

	tests := []struct {
		name string
		run  func(w *bytes.Buffer) error
	}{
		{"RunStatus", func(w *bytes.Buffer) error { return RunStatus(t.Context(), sel, w) }},
		{"RunAudit", func(w *bytes.Buffer) error { return RunAudit(t.Context(), sel, 0, w) }},
		{"RunConfigGet", func(w *bytes.Buffer) error { return RunConfigGet(t.Context(), sel, "k", w) }},
		{"RunConfigSet", func(w *bytes.Buffer) error { return RunConfigSet(t.Context(), sel, "k", "v", w) }},
		{"RunSetOnline", func(w *bytes.Buffer) error { return RunSetOnline(t.Context(), sel, true, w) }},
		{"RunPairingKeyCreate", func(w *bytes.Buffer) error { return RunPairingKeyCreate(t.Context(), sel, w) }},
		{"RunPeerList", func(w *bytes.Buffer) error { return RunPeerList(t.Context(), sel, w) }},
		{"RunPeerConnect", func(w *bytes.Buffer) error {
			return RunPeerConnect(t.Context(), sel, "a", "k", "", w, w)
		}},
		{"RunPeerDisconnect", func(w *bytes.Buffer) error { return RunPeerDisconnect(t.Context(), sel, "b", w) }},
		{"PeerNames", func(*bytes.Buffer) error {
			_, err := PeerNames(t.Context(), sel)
			return err
		}},
		{"RunApplyUpdate", func(*bytes.Buffer) error {
			_, err := RunApplyUpdate(t.Context(), sel)
			return err
		}},
		{"RunUpdateStatus", func(*bytes.Buffer) error {
			_, _, err := RunUpdateStatus(t.Context(), sel)
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			err := tt.run(&out)
			if err == nil || err.Error() != want {
				t.Fatalf("%s error = %v, want %q", tt.name, err, want)
			}
			if out.Len() != 0 {
				t.Fatalf("%s wrote %q, want nothing", tt.name, out.String())
			}
		})
	}
}

// TestRunStatusReturnsWriteError returns the writer's error.
func TestRunStatusReturnsWriteError(t *testing.T) {
	client := &stubLocalClient{status: &localpb.StatusResponse{}}
	err := RunStatus(t.Context(), stubSelector(t, client), &failWriter{allow: 0})
	if !errors.Is(err, errWriter) {
		t.Fatalf("RunStatus error = %v, want %v", err, errWriter)
	}
}

// TestRunAuditReturnsStreamStartError returns the Broker's error that refused the
// audit stream and writes nothing.
func TestRunAuditReturnsStreamStartError(t *testing.T) {
	client := &stubLocalClient{auditErr: status.Error(codes.Unavailable, "audit unavailable")}
	var out bytes.Buffer
	err := RunAudit(t.Context(), stubSelector(t, client), 0, &out)
	if err == nil || !strings.Contains(err.Error(), "audit unavailable") {
		t.Fatalf("RunAudit error = %v, want the Broker's message", err)
	}
	if out.Len() != 0 {
		t.Fatalf("RunAudit wrote %q, want nothing", out.String())
	}
}

// TestRunAuditReturnsWriteError returns the writer's error for an audit event.
func TestRunAuditReturnsWriteError(t *testing.T) {
	path := fakelocal.SocketPath(t)
	event := sampleAuditEvents()[0]
	fakelocal.Start(t, path, fakelocal.Handlers{
		Audit: func(_ *localpb.AuditRequest, stream localpb.Local_AuditServer) error {
			return stream.Send(event)
		},
	})
	err := RunAudit(t.Context(), fixedSelector(path), 0, &failWriter{allow: 0})
	if !errors.Is(err, errWriter) {
		t.Fatalf("RunAudit error = %v, want %v", err, errWriter)
	}
}

// TestRunPeerConnectReturnsWriteError returns the writer's error for the result
// line.
func TestRunPeerConnectReturnsWriteError(t *testing.T) {
	path := fakelocal.SocketPath(t)
	fakelocal.Start(t, path, fakelocal.AdminHandlers())
	err := RunPeerConnect(t.Context(), fixedSelector(path), "peer", "key", "", &failWriter{allow: 0}, &bytes.Buffer{})
	if !errors.Is(err, errWriter) {
		t.Fatalf("RunPeerConnect error = %v, want %v", err, errWriter)
	}
}

// TestPeerNamesReturnsListError returns the Broker's list failure and no names.
func TestPeerNamesReturnsListError(t *testing.T) {
	client := &stubLocalClient{listErr: status.Error(codes.Unavailable, "list unavailable")}
	names, err := PeerNames(t.Context(), stubSelector(t, client))
	if err == nil || !strings.Contains(err.Error(), "list unavailable") {
		t.Fatalf("PeerNames error = %v, want the Broker's message", err)
	}
	if names != nil {
		t.Fatalf("PeerNames = %v, want nil on error", names)
	}
}

// TestRunUpdateStatusReturnsBrokerError returns the Broker's failure and empty
// versions.
func TestRunUpdateStatusReturnsBrokerError(t *testing.T) {
	client := &stubLocalClient{updateErr: status.Error(codes.PermissionDenied, "not allowed")}
	current, available, err := RunUpdateStatus(t.Context(), stubSelector(t, client))
	if err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("RunUpdateStatus error = %v, want the Broker's message", err)
	}
	if current != "" || available != "" {
		t.Fatalf("RunUpdateStatus = (%q, %q), want empty strings on error", current, available)
	}
}

// TestRunStatusReturnsDialerOpenError returns the dialer's error and writes
// nothing.
func TestRunStatusReturnsDialerOpenError(t *testing.T) {
	sel := func(context.Context) (string, Dialer, error) {
		return "stub.sock", func() (*grpc.ClientConn, localpb.LocalClient, error) {
			return nil, nil, errors.New("the dialer refused")
		}, nil
	}
	var out bytes.Buffer
	err := RunStatus(t.Context(), sel, &out)
	if err == nil || !strings.Contains(err.Error(), "the dialer refused") {
		t.Fatalf("RunStatus error = %v, want the dialer's error", err)
	}
	if out.Len() != 0 {
		t.Fatalf("RunStatus wrote %q, want nothing", out.String())
	}
}
