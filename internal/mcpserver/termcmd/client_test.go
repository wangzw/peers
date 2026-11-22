package termcmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakelocal"
)

// fixedSelector returns path and an unchecked dialer from every call.
func fixedSelector(path string) Selector {
	return func(context.Context) (string, Dialer, error) {
		return path, func() (*grpc.ClientConn, localpb.LocalClient, error) {
			cc, err := grpc.NewClient("unix://"+path, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return nil, nil, err
			}
			return cc, localpb.NewLocalClient(cc), nil
		}, nil
	}
}

// methodCalls counts the unary calls srv received for method.
func methodCalls(srv *fakelocal.Server, method string) int {
	n := 0
	for _, c := range srv.Calls() {
		if c.Method == method {
			n++
		}
	}
	return n
}

// within runs fn and fails the test if it has not returned within d.
func within(t *testing.T, d time.Duration, fn func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return err
	case <-time.After(d):
		t.Fatalf("the call did not return within %s", d)
		return nil
	}
}

func TestRunStatusUsesSelectedSocket(t *testing.T) {
	path := fakelocal.SocketPath(t)
	resp := &localpb.StatusResponse{
		Agents: []*localpb.AgentStatusEntry{{
			Address: "alice@host-a",
			Account: "wangzw",
			Status:  localpb.AgentStatus_AGENT_STATUS_IDLE,
			Topics:  []string{"team-a"},
		}},
	}
	srv := fakelocal.Start(t, path, fakelocal.Handlers{
		Status: func(context.Context, *localpb.StatusRequest) (*localpb.StatusResponse, error) {
			return resp, nil
		},
	})

	var got bytes.Buffer
	if err := RunStatus(t.Context(), fixedSelector(path), &got); err != nil {
		t.Fatalf("RunStatus: %v", err)
	}

	var want bytes.Buffer
	if err := WriteStatus(&want, resp); err != nil {
		t.Fatalf("WriteStatus: %v", err)
	}
	if got.String() != want.String() {
		t.Errorf("RunStatus output = %q, want %q", got.String(), want.String())
	}
	if n := methodCalls(srv, "Hello"); n != 1 {
		t.Errorf("Hello calls = %d, want 1", n)
	}
	if n := methodCalls(srv, "Status"); n != 1 {
		t.Errorf("Status calls = %d, want 1", n)
	}
}

// TestRunStatusDiagnosticsOnly checks that the status lines are written and
// the sentinel comes back, so the command can exit with 1 without repeating the
// reason on standard error.
func TestRunStatusDiagnosticsOnly(t *testing.T) {
	path := fakelocal.SocketPath(t)
	const reason = "config parse failed"
	const fix = "fix the config"
	srv := fakelocal.Start(t, path, fakelocal.Handlers{
		Status: func(context.Context, *localpb.StatusRequest) (*localpb.StatusResponse, error) {
			return &localpb.StatusResponse{
				CurrentVersion: "0.2.0",
				Degraded: &localpb.DegradedState{
					Level:  localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY,
					Reason: reason,
					Fix:    fix,
				},
			}, nil
		},
	})

	var got bytes.Buffer
	err := RunStatus(t.Context(), fixedSelector(path), &got)
	if !errors.Is(err, ErrDiagnosticsOnly) {
		t.Fatalf("RunStatus error = %v, want ErrDiagnosticsOnly", err)
	}
	want := "Broker: degraded (diagnostics only): " + reason + "\nFix: " + fix + "\n"
	if got.String() != want {
		t.Errorf("RunStatus output = %q, want %q", got.String(), want)
	}
	if n := methodCalls(srv, "Status"); n != 1 {
		t.Errorf("Status calls = %d, want 1", n)
	}
}

func TestRunStatusSelectorError(t *testing.T) {
	const noBroker = "no Broker is available for this account; if a system Broker is installed on this host, ask the administrator to start it. Otherwise start a user Broker with `peers service install`, or run `peers service run` in a terminal outside the Claude Code session (a Broker started inside the session exits with code 6)"
	want := "cannot reach the Broker: " + noBroker
	var got bytes.Buffer
	err := RunStatus(t.Context(), func(context.Context) (string, Dialer, error) {
		return "", nil, errors.New(noBroker)
	}, &got)
	if err == nil || err.Error() != want {
		t.Fatalf("RunStatus error = %v, want %q", err, want)
	}
	if got.Len() != 0 {
		t.Errorf("RunStatus wrote %q, want nothing", got.String())
	}
}

func TestRunStatusUnreachable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.sock")
	var got bytes.Buffer
	err := within(t, eventTimeout, func() error {
		return RunStatus(t.Context(), fixedSelector(path), &got)
	})
	if err == nil {
		t.Fatal("RunStatus returned nil, want an error")
	}
	if prefix := "cannot reach the Broker at " + path; !strings.HasPrefix(err.Error(), prefix) {
		t.Errorf("RunStatus error = %q, want it to start with %q", err, prefix)
	}
	if got.Len() != 0 {
		t.Errorf("RunStatus wrote %q, want nothing", got.String())
	}
}

// TestRunStatusHelloUnavailableNeutralizesMessage checks that the Broker's text
// on a failed Hello handshake is escaped before it reaches the terminal, since
// the peer process chose it.
func TestRunStatusHelloUnavailableNeutralizesMessage(t *testing.T) {
	path := fakelocal.SocketPath(t)
	const raw = "starting up\x1b[31m"
	fakelocal.Start(t, path, fakelocal.Handlers{
		Hello: func(context.Context, *localpb.HelloRequest) (*localpb.HelloResponse, error) {
			return nil, status.Error(codes.Unavailable, raw)
		},
	})

	var got bytes.Buffer
	err := RunStatus(t.Context(), fixedSelector(path), &got)
	if err == nil {
		t.Fatal("RunStatus returned nil, want an error")
	}
	if strings.ContainsRune(err.Error(), '\x1b') {
		t.Errorf("RunStatus error carries a raw ESC: %q", err.Error())
	}
	if want := "starting up" + `\x1b` + "[31m"; !strings.Contains(err.Error(), want) {
		t.Errorf("RunStatus error = %q, want it to contain %q", err.Error(), want)
	}
	if got.Len() != 0 {
		t.Errorf("RunStatus wrote %q, want nothing", got.String())
	}
}

func TestRunStatusVersionMismatch(t *testing.T) {
	path := fakelocal.SocketPath(t)
	srv := fakelocal.Start(t, path, fakelocal.Handlers{
		Hello: func(context.Context, *localpb.HelloRequest) (*localpb.HelloResponse, error) {
			return &localpb.HelloResponse{ProtocolVersion: 2}, nil
		},
	})

	var got bytes.Buffer
	err := RunStatus(t.Context(), fixedSelector(path), &got)
	var vm *VersionMismatchError
	if !errors.As(err, &vm) {
		t.Fatalf("RunStatus error = %v, want *VersionMismatchError", err)
	}
	if vm.Broker != 2 || vm.Client != localpb.ProtocolVersion {
		t.Errorf("VersionMismatchError = %+v, want Broker 2 and Client %d", vm, localpb.ProtocolVersion)
	}
	if n := methodCalls(srv, "Status"); n != 0 {
		t.Errorf("Status calls = %d, want 0", n)
	}
}

func TestRunStatusHelloVersionMismatchError(t *testing.T) {
	path := fakelocal.SocketPath(t)
	srv := fakelocal.Start(t, path, fakelocal.Handlers{
		Hello: func(context.Context, *localpb.HelloRequest) (*localpb.HelloResponse, error) {
			return nil, fakelocal.ErrorStatus(codes.FailedPrecondition, &localpb.ErrorDetail{
				Code:    localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH,
				Message: "local protocol version 1 is not supported by this Broker (version 2); restart the Broker",
			})
		},
	})

	var got bytes.Buffer
	err := RunStatus(t.Context(), fixedSelector(path), &got)
	if err == nil {
		t.Fatal("RunStatus returned nil, want an error")
	}
	if !strings.HasPrefix(err.Error(), "version_mismatch: ") {
		t.Errorf("RunStatus error = %q, want it to start with %q", err, "version_mismatch: ")
	}
	if !strings.Contains(err.Error(), "with this binary") {
		t.Errorf("RunStatus error = %q, want it to say to restart the Broker with this binary", err)
	}
	if n := methodCalls(srv, "Status"); n != 0 {
		t.Errorf("Status calls = %d, want 0", n)
	}
}

func TestRunStatusPermissionDenied(t *testing.T) {
	path := fakelocal.SocketPath(t)
	fakelocal.Start(t, path, fakelocal.Handlers{
		Status: func(context.Context, *localpb.StatusRequest) (*localpb.StatusResponse, error) {
			return nil, fakelocal.ErrorStatus(codes.PermissionDenied, &localpb.ErrorDetail{
				Code:    localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED,
				Message: "read permission on the audit log is required",
			})
		},
	})

	var got bytes.Buffer
	err := RunStatus(t.Context(), fixedSelector(path), &got)
	if err == nil {
		t.Fatal("RunStatus returned nil, want an error")
	}
	if !strings.HasPrefix(err.Error(), "permission_denied: ") {
		t.Errorf("RunStatus error = %q, want it to start with %q", err, "permission_denied: ")
	}
}

// sampleAuditEvents builds five events in arrival order.
func sampleAuditEvents() []*localpb.AuditEvent {
	const base = "2026-11-18T10:00:00Z"
	start, err := time.Parse(time.RFC3339, base)
	if err != nil {
		panic(err)
	}
	events := make([]*localpb.AuditEvent, 5)
	for i := range events {
		events[i] = &localpb.AuditEvent{
			Time:  timestamppb.New(start.Add(time.Duration(i) * time.Minute)),
			Event: localpb.AuditEventKind_AUDIT_EVENT_KIND_CONFIG_SET,
			Actor: fmt.Sprintf("actor%d", i),
		}
	}
	return events
}

// auditRecorder records the Limit of each Audit request in a concurrency-safe
// way.
type auditRecorder struct {
	mu   sync.Mutex
	reqs []uint32
}

func (r *auditRecorder) record(limit uint32) {
	r.mu.Lock()
	r.reqs = append(r.reqs, limit)
	r.mu.Unlock()
}

func (r *auditRecorder) limits() []uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint32(nil), r.reqs...)
}

func TestRunAuditStreamsWithLimit(t *testing.T) {
	events := sampleAuditEvents()

	tests := []struct {
		name  string
		limit uint32
		want  []*localpb.AuditEvent
	}{
		{name: "limit", limit: 2, want: events[len(events)-2:]},
		{name: "no limit", limit: 0, want: events},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := fakelocal.SocketPath(t)
			rec := &auditRecorder{}
			fakelocal.Start(t, path, fakelocal.Handlers{
				Audit: func(req *localpb.AuditRequest, stream localpb.Local_AuditServer) error {
					rec.record(req.GetLimit())
					evs := events
					if n := int(req.GetLimit()); n > 0 && n < len(evs) {
						evs = evs[len(evs)-n:]
					}
					for _, ev := range evs {
						if err := stream.Send(ev); err != nil {
							return err
						}
					}
					return nil
				},
			})

			var got bytes.Buffer
			if err := RunAudit(t.Context(), fixedSelector(path), tt.limit, &got); err != nil {
				t.Fatalf("RunAudit: %v", err)
			}

			var want bytes.Buffer
			for _, ev := range tt.want {
				if err := WriteAuditEvent(&want, ev); err != nil {
					t.Fatalf("WriteAuditEvent: %v", err)
				}
			}
			if got.String() != want.String() {
				t.Errorf("RunAudit output = %q, want %q", got.String(), want.String())
			}
			if lines := strings.Count(got.String(), "\n"); lines != len(tt.want) {
				t.Errorf("RunAudit wrote %d lines, want %d", lines, len(tt.want))
			}
			if got := rec.limits(); len(got) != 1 || got[0] != tt.limit {
				t.Errorf("Audit request limits = %v, want [%d]", got, tt.limit)
			}
		})
	}
}

func TestRunAuditPermissionDenied(t *testing.T) {
	path := fakelocal.SocketPath(t)
	fakelocal.Start(t, path, fakelocal.Handlers{
		Audit: func(*localpb.AuditRequest, localpb.Local_AuditServer) error {
			return fakelocal.ErrorStatus(codes.PermissionDenied, &localpb.ErrorDetail{
				Code:    localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED,
				Message: "read permission on the audit log is required",
			})
		},
	})

	var got bytes.Buffer
	err := RunAudit(t.Context(), fixedSelector(path), 0, &got)
	if err == nil {
		t.Fatal("RunAudit returned nil, want an error")
	}
	if !strings.HasPrefix(err.Error(), "permission_denied: ") {
		t.Errorf("RunAudit error = %q, want it to start with %q", err, "permission_denied: ")
	}
	if got.Len() != 0 {
		t.Errorf("RunAudit wrote %q, want nothing", got.String())
	}
}

func TestRunAuditErrorAfterEvents(t *testing.T) {
	path := fakelocal.SocketPath(t)
	events := sampleAuditEvents()[:2]
	fakelocal.Start(t, path, fakelocal.Handlers{
		Audit: func(_ *localpb.AuditRequest, stream localpb.Local_AuditServer) error {
			for _, ev := range events {
				if err := stream.Send(ev); err != nil {
					return err
				}
			}
			return status.Error(codes.Internal, "x")
		},
	})

	var got bytes.Buffer
	err := RunAudit(t.Context(), fixedSelector(path), 0, &got)
	if err == nil {
		t.Fatal("RunAudit returned nil, want an error")
	}

	var want bytes.Buffer
	for _, ev := range events {
		if err := WriteAuditEvent(&want, ev); err != nil {
			t.Fatalf("WriteAuditEvent: %v", err)
		}
	}
	if got.String() != want.String() {
		t.Errorf("RunAudit output = %q, want the two lines written before the error %q", got.String(), want.String())
	}
}

// requestOf returns the request of the first call to method.
func requestOf[T any](t *testing.T, srv *fakelocal.Server, method string) T {
	t.Helper()
	for _, c := range srv.Calls() {
		if c.Method != method {
			continue
		}
		if req, ok := c.Request.(T); ok {
			return req
		}
	}
	t.Fatalf("no %s call was recorded", method)
	var zero T
	return zero
}

func TestRunConfigGetAll(t *testing.T) {
	path := fakelocal.SocketPath(t)
	resp := &localpb.GetConfigResponse{Entries: []*localpb.ConfigEntry{
		{Key: "topics.auto_join", Value: &localpb.ConfigValue{Value: &localpb.ConfigValue_BoolValue{BoolValue: true}}},
		{Key: "pairing.static_key", Value: &localpb.ConfigValue{Value: &localpb.ConfigValue_SecretIsSet{SecretIsSet: true}}},
		{Key: "broker.alias", Value: &localpb.ConfigValue{Value: &localpb.ConfigValue_StringValue{StringValue: "hostA"}}},
	}}
	srv := fakelocal.Start(t, path, fakelocal.Handlers{
		GetConfig: func(context.Context, *localpb.GetConfigRequest) (*localpb.GetConfigResponse, error) {
			return resp, nil
		},
	})

	var got bytes.Buffer
	if err := RunConfigGet(t.Context(), fixedSelector(path), "", &got); err != nil {
		t.Fatalf("RunConfigGet: %v", err)
	}

	var want bytes.Buffer
	if err := WriteConfigEntries(&want, resp.GetEntries()); err != nil {
		t.Fatalf("WriteConfigEntries: %v", err)
	}
	if got.String() != want.String() {
		t.Errorf("RunConfigGet output = %q, want %q", got.String(), want.String())
	}
	if !strings.Contains(got.String(), "pairing.static_key = set") {
		t.Errorf("RunConfigGet output = %q, want the secret entry marked set", got.String())
	}
	if req := requestOf[*localpb.GetConfigRequest](t, srv, "GetConfig"); req.GetKey() != "" {
		t.Errorf("GetConfig key = %q, want empty for every entry", req.GetKey())
	}
}

func TestRunConfigGetOneKey(t *testing.T) {
	path := fakelocal.SocketPath(t)
	resp := &localpb.GetConfigResponse{Entries: []*localpb.ConfigEntry{
		{Key: "broker.alias", Value: &localpb.ConfigValue{Value: &localpb.ConfigValue_StringValue{StringValue: "hostA"}}},
	}}
	srv := fakelocal.Start(t, path, fakelocal.Handlers{
		GetConfig: func(context.Context, *localpb.GetConfigRequest) (*localpb.GetConfigResponse, error) {
			return resp, nil
		},
	})

	var got bytes.Buffer
	if err := RunConfigGet(t.Context(), fixedSelector(path), "broker.alias", &got); err != nil {
		t.Fatalf("RunConfigGet: %v", err)
	}
	if want := "broker.alias = hostA\n"; got.String() != want {
		t.Errorf("RunConfigGet output = %q, want %q", got.String(), want)
	}
	if req := requestOf[*localpb.GetConfigRequest](t, srv, "GetConfig"); req.GetKey() != "broker.alias" {
		t.Errorf("GetConfig key = %q, want %q", req.GetKey(), "broker.alias")
	}
}

func TestRunConfigGetEmpty(t *testing.T) {
	path := fakelocal.SocketPath(t)
	fakelocal.Start(t, path, fakelocal.Handlers{
		GetConfig: func(context.Context, *localpb.GetConfigRequest) (*localpb.GetConfigResponse, error) {
			return &localpb.GetConfigResponse{}, nil
		},
	})

	var got bytes.Buffer
	if err := RunConfigGet(t.Context(), fixedSelector(path), "", &got); err != nil {
		t.Fatalf("RunConfigGet: %v", err)
	}
	if got.Len() != 0 {
		t.Errorf("RunConfigGet wrote %q, want nothing", got.String())
	}
}

func TestRunConfigSet(t *testing.T) {
	path := fakelocal.SocketPath(t)
	srv := fakelocal.Start(t, path, fakelocal.Handlers{
		SetConfig: func(context.Context, *localpb.SetConfigRequest) (*localpb.SetConfigResponse, error) {
			return &localpb.SetConfigResponse{
				OldValue: &localpb.ConfigValue{Value: &localpb.ConfigValue_BoolValue{BoolValue: false}},
				NewValue: &localpb.ConfigValue{Value: &localpb.ConfigValue_BoolValue{BoolValue: true}},
			}, nil
		},
	})

	var got bytes.Buffer
	if err := RunConfigSet(t.Context(), fixedSelector(path), "topics.auto_join", "true", &got); err != nil {
		t.Fatalf("RunConfigSet: %v", err)
	}
	if want := "topics.auto_join: false -> true\n"; got.String() != want {
		t.Errorf("RunConfigSet output = %q, want %q", got.String(), want)
	}
	req := requestOf[*localpb.SetConfigRequest](t, srv, "SetConfig")
	if req.GetKey() != "topics.auto_join" || req.GetValue() != "true" {
		t.Errorf("SetConfig request = (%q, %q), want (%q, %q)", req.GetKey(), req.GetValue(), "topics.auto_join", "true")
	}
}

func TestRunConfigSetPermissionDeniedIncludesPath(t *testing.T) {
	const configFile = "/etc/peers/config.yaml"
	path := fakelocal.SocketPath(t)
	fakelocal.Start(t, path, fakelocal.Handlers{
		SetConfig: func(context.Context, *localpb.SetConfigRequest) (*localpb.SetConfigResponse, error) {
			return nil, fakelocal.PermissionDenied("changing the configuration requires write permission on " + configFile)
		},
	})

	var got bytes.Buffer
	err := RunConfigSet(t.Context(), fixedSelector(path), "topics.auto_join", "true", &got)
	if err == nil {
		t.Fatal("RunConfigSet returned nil, want an error")
	}
	if !strings.HasPrefix(err.Error(), "permission_denied: ") {
		t.Errorf("RunConfigSet error = %q, want it to start with %q", err, "permission_denied: ")
	}
	if !strings.Contains(err.Error(), configFile) {
		t.Errorf("RunConfigSet error = %q, want it to name the configuration file %q", err, configFile)
	}
	if got.Len() != 0 {
		t.Errorf("RunConfigSet wrote %q, want nothing", got.String())
	}
}

// TestRunSetOnline covers both directions: the stored broker.offline value is
// the opposite of the requested state, and the success line reports the state
// (REQ-broker-config-066).
func TestRunSetOnline(t *testing.T) {
	cases := []struct {
		name     string
		online   bool
		wantVal  string
		wantLine string
	}{
		{"offline", false, "true", "The Broker is now offline.\n"},
		{"online", true, "false", "The Broker is now online.\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := fakelocal.SocketPath(t)
			srv := fakelocal.Start(t, path, fakelocal.Handlers{
				SetConfig: func(context.Context, *localpb.SetConfigRequest) (*localpb.SetConfigResponse, error) {
					return &localpb.SetConfigResponse{
						OldValue: &localpb.ConfigValue{Value: &localpb.ConfigValue_BoolValue{BoolValue: !tc.online}},
						NewValue: &localpb.ConfigValue{Value: &localpb.ConfigValue_BoolValue{BoolValue: !tc.online}},
					}, nil
				},
			})

			var got bytes.Buffer
			if err := RunSetOnline(t.Context(), fixedSelector(path), tc.online, &got); err != nil {
				t.Fatalf("RunSetOnline: %v", err)
			}
			if got.String() != tc.wantLine {
				t.Errorf("RunSetOnline output = %q, want %q", got.String(), tc.wantLine)
			}
			req := requestOf[*localpb.SetConfigRequest](t, srv, "SetConfig")
			if req.GetKey() != "broker.offline" || req.GetValue() != tc.wantVal {
				t.Errorf("SetConfig request = (%q, %q), want (%q, %q)", req.GetKey(), req.GetValue(), "broker.offline", tc.wantVal)
			}
		})
	}
}

// TestRunSetOnlinePermissionDenied checks that a denied write is returned as a
// permission error and nothing is printed.
func TestRunSetOnlinePermissionDenied(t *testing.T) {
	path := fakelocal.SocketPath(t)
	fakelocal.Start(t, path, fakelocal.Handlers{
		SetConfig: func(context.Context, *localpb.SetConfigRequest) (*localpb.SetConfigResponse, error) {
			return nil, fakelocal.PermissionDenied("changing the configuration requires write permission on /etc/peers/config.yaml")
		},
	})

	var got bytes.Buffer
	err := RunSetOnline(t.Context(), fixedSelector(path), false, &got)
	if err == nil {
		t.Fatal("RunSetOnline returned nil, want an error")
	}
	if !strings.HasPrefix(err.Error(), "permission_denied: ") {
		t.Errorf("RunSetOnline error = %q, want it to start with %q", err, "permission_denied: ")
	}
	if got.Len() != 0 {
		t.Errorf("RunSetOnline wrote %q, want nothing", got.String())
	}
}

func TestRunPairingKeyCreate(t *testing.T) {
	path := fakelocal.SocketPath(t)
	resp := &localpb.CreatePairingKeyResponse{
		Key:            "one-time-key",
		ExpiresAt:      timestamppb.New(time.Date(2026, 11, 18, 10, 15, 0, 0, time.UTC)),
		ShownAddresses: []string{"hostA:17443"},
	}
	srv := fakelocal.Start(t, path, fakelocal.Handlers{
		CreatePairingKey: func(context.Context, *localpb.CreatePairingKeyRequest) (*localpb.CreatePairingKeyResponse, error) {
			return resp, nil
		},
	})

	var got bytes.Buffer
	if err := RunPairingKeyCreate(t.Context(), fixedSelector(path), &got); err != nil {
		t.Fatalf("RunPairingKeyCreate: %v", err)
	}
	var want bytes.Buffer
	if err := WritePairingKey(&want, resp); err != nil {
		t.Fatalf("WritePairingKey: %v", err)
	}
	if got.String() != want.String() {
		t.Errorf("RunPairingKeyCreate output = %q, want %q", got.String(), want.String())
	}
	if n := methodCalls(srv, "CreatePairingKey"); n != 1 {
		t.Errorf("CreatePairingKey calls = %d, want 1", n)
	}
}

// TestRunPairingKeyCreateNotListening checks that the terminal tells the user
// how to pair from this side, in the command-line form rather than the MCP tool
// name that the Broker's own message no longer carries.
func TestRunPairingKeyCreateNotListening(t *testing.T) {
	path := fakelocal.SocketPath(t)
	fakelocal.Start(t, path, fakelocal.Handlers{
		CreatePairingKey: func(context.Context, *localpb.CreatePairingKeyRequest) (*localpb.CreatePairingKeyResponse, error) {
			return nil, fakelocal.ErrorStatus(codes.InvalidArgument, &localpb.ErrorDetail{
				Code:    localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
				Message: "this Broker is not listening (broker.listen is empty), so the other Broker cannot dial it",
			})
		},
	})

	var got bytes.Buffer
	err := RunPairingKeyCreate(t.Context(), fixedSelector(path), &got)
	if err == nil {
		t.Fatal("RunPairingKeyCreate returned nil, want an error")
	}
	if !strings.Contains(err.Error(), "`peers broker connect`") {
		t.Errorf("error = %q, want the pairing command", err)
	}
	if got.Len() != 0 {
		t.Errorf("RunPairingKeyCreate wrote %q, want nothing", got.String())
	}
}

func TestRunPeerList(t *testing.T) {
	path := fakelocal.SocketPath(t)
	resp := &localpb.ListBrokersResponse{Local: &localpb.LocalBrokerState{}, Brokers: []*localpb.BrokerInfo{
		{
			Alias:       "hostB",
			Fingerprint: bytes.Repeat([]byte{0xcd}, 32),
			IsPeer:      true,
			Status:      localpb.BrokerStatus_BROKER_STATUS_CONNECTED,
			Role:        localpb.PeerRole_PEER_ROLE_ACCEPTOR,
			AdvertisedAddresses: []*localpb.AdvertisedAddress{
				{Kind: "direct", Addr: "hostb.example:17443"},
			},
		},
		{
			Alias:       "hostA",
			Fingerprint: bytes.Repeat([]byte{0xab}, 32),
			IsPeer:      true,
			Status:      localpb.BrokerStatus_BROKER_STATUS_CONNECTED,
			Role:        localpb.PeerRole_PEER_ROLE_DIALER,
		},
	}}
	srv := fakelocal.Start(t, path, fakelocal.Handlers{
		ListBrokers: func(context.Context, *localpb.ListBrokersRequest) (*localpb.ListBrokersResponse, error) {
			return resp, nil
		},
	})

	var got bytes.Buffer
	if err := RunPeerList(t.Context(), fixedSelector(path), &got); err != nil {
		t.Fatalf("RunPeerList: %v", err)
	}
	var want bytes.Buffer
	if err := WriteBrokerList(&want, resp.GetBrokers(), resp.GetLocal()); err != nil {
		t.Fatalf("WriteBrokerList: %v", err)
	}
	if got.String() != want.String() {
		t.Errorf("RunPeerList output = %q, want %q", got.String(), want.String())
	}
	if n := methodCalls(srv, "ListBrokers"); n != 1 {
		t.Errorf("ListBrokers calls = %d, want 1", n)
	}
}

func TestRunPeerListEmpty(t *testing.T) {
	path := fakelocal.SocketPath(t)
	fakelocal.Start(t, path, fakelocal.Handlers{
		ListBrokers: func(context.Context, *localpb.ListBrokersRequest) (*localpb.ListBrokersResponse, error) {
			return &localpb.ListBrokersResponse{Local: &localpb.LocalBrokerState{}}, nil
		},
	})

	var got bytes.Buffer
	if err := RunPeerList(t.Context(), fixedSelector(path), &got); err != nil {
		t.Fatalf("RunPeerList: %v", err)
	}
	if want := "Brokers: none\n"; got.String() != want {
		t.Errorf("RunPeerList output = %q, want %q", got.String(), want)
	}
}

// TestRunPeerListShowsLocalState checks that broker list prints the state the
// Broker reports for itself, without calling Status.
func TestRunPeerListShowsLocalState(t *testing.T) {
	path := fakelocal.SocketPath(t)
	srv := fakelocal.Start(t, path, fakelocal.Handlers{
		ListBrokers: func(context.Context, *localpb.ListBrokersRequest) (*localpb.ListBrokersResponse, error) {
			return &localpb.ListBrokersResponse{Local: &localpb.LocalBrokerState{
				Offline: true,
				Degraded: &localpb.DegradedState{
					Level:  localpb.DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY,
					Reason: "listen address :17443 is unavailable",
					Fix:    "peers config set broker.listen <free address>",
				},
			}}, nil
		},
	})

	var got bytes.Buffer
	if err := RunPeerList(t.Context(), fixedSelector(path), &got); err != nil {
		t.Fatalf("RunPeerList: %v", err)
	}
	want := "Broker: degraded (local only): listen address :17443 is unavailable\n" +
		"Fix: peers config set broker.listen <free address>\n" +
		"Broker: offline; messages from other hosts are refused. Run peers service online to accept them.\n" +
		"Brokers: none\n"
	if got.String() != want {
		t.Errorf("RunPeerList output = %q, want %q", got.String(), want)
	}
	if n := methodCalls(srv, "Status"); n != 0 {
		t.Errorf("Status calls = %d, want 0", n)
	}
}

// TestPeerNamesCollectsAliasesAndFingerprints checks the disconnect completion
// candidates: a paired Broker contributes its alias and fingerprint, one
// without an alias only the fingerprint, and a non-peer nothing.
func TestPeerNamesCollectsAliasesAndFingerprints(t *testing.T) {
	path := fakelocal.SocketPath(t)
	fakelocal.Start(t, path, fakelocal.Handlers{
		ListBrokers: func(context.Context, *localpb.ListBrokersRequest) (*localpb.ListBrokersResponse, error) {
			return &localpb.ListBrokersResponse{Brokers: []*localpb.BrokerInfo{
				{Alias: "web", Fingerprint: []byte{0xab, 0xcd}, IsPeer: true},
				{Fingerprint: []byte{0x01, 0x02}, IsPeer: true},
				{Alias: "card", Fingerprint: []byte{0x03, 0x04}, IsPeer: false},
			}}, nil
		},
	})

	got, err := PeerNames(t.Context(), fixedSelector(path))
	if err != nil {
		t.Fatalf("PeerNames: %v", err)
	}
	want := []string{"web", "abcd", "0102"}
	if !slices.Equal(got, want) {
		t.Fatalf("PeerNames = %q, want %q", got, want)
	}
}

// TestRunPeerListBrokerDegraded checks the diagnostics-only path: the Broker
// refuses ListBrokers, and the command reports the described error.
func TestRunPeerListBrokerDegraded(t *testing.T) {
	path := fakelocal.SocketPath(t)
	fail, err := status.New(codes.FailedPrecondition, "config parse failed").WithDetails(&localpb.ErrorDetail{
		Code:    localpb.ErrorCode_ERROR_CODE_BROKER_DEGRADED,
		Message: "config parse failed",
	})
	if err != nil {
		t.Fatalf("building the error detail: %v", err)
	}
	fakelocal.Start(t, path, fakelocal.Handlers{
		ListBrokers: func(context.Context, *localpb.ListBrokersRequest) (*localpb.ListBrokersResponse, error) {
			return nil, fail.Err()
		},
	})

	var got bytes.Buffer
	err = RunPeerList(t.Context(), fixedSelector(path), &got)
	if err == nil || err.Error() != "broker_degraded: config parse failed" {
		t.Fatalf("RunPeerList error = %v, want %q", err, "broker_degraded: config parse failed")
	}
	if got.Len() != 0 {
		t.Errorf("RunPeerList wrote %q, want nothing", got.String())
	}
}

func TestRunPeerConnect(t *testing.T) {
	path := fakelocal.SocketPath(t)
	fingerprint := bytes.Repeat([]byte{0xcd}, 32)
	srv := fakelocal.Start(t, path, fakelocal.Handlers{
		Connect: func(context.Context, *localpb.ConnectRequest) (*localpb.ConnectResponse, error) {
			return &localpb.ConnectResponse{
				Alias:       "hostB",
				Fingerprint: fingerprint,
				Warnings: []*localpb.Warning{
					{Message: "first warning"},
					{Message: "second warning"},
				},
			}, nil
		},
	})

	var got, warned bytes.Buffer
	err := RunPeerConnect(t.Context(), fixedSelector(path), "hostb.example:17443", "one-time-key", "lab-b", &got, &warned)
	if err != nil {
		t.Fatalf("RunPeerConnect: %v", err)
	}

	wantLine := "Paired with hostB (" + strings.Repeat("cd", 32) + ")\n"
	if got.String() != wantLine {
		t.Errorf("RunPeerConnect output = %q, want %q", got.String(), wantLine)
	}
	wantWarnings := "warning: first warning\nwarning: second warning\n"
	if warned.String() != wantWarnings {
		t.Errorf("RunPeerConnect warnings = %q, want %q", warned.String(), wantWarnings)
	}

	req := requestOf[*localpb.ConnectRequest](t, srv, "Connect")
	if req.GetAddress() != "hostb.example:17443" || req.GetKey() != "one-time-key" || req.GetLocalAlias() != "lab-b" {
		t.Errorf("Connect request = (%q, %q, %q), want (%q, %q, %q)",
			req.GetAddress(), req.GetKey(), req.GetLocalAlias(),
			"hostb.example:17443", "one-time-key", "lab-b")
	}
}

func TestRunPeerConnectNoWarnings(t *testing.T) {
	path := fakelocal.SocketPath(t)
	fakelocal.Start(t, path, fakelocal.Handlers{
		Connect: func(context.Context, *localpb.ConnectRequest) (*localpb.ConnectResponse, error) {
			return &localpb.ConnectResponse{
				Alias:       "hostB",
				Fingerprint: bytes.Repeat([]byte{0xcd}, 32),
			}, nil
		},
	})

	var got, warned bytes.Buffer
	if err := RunPeerConnect(t.Context(), fixedSelector(path), "hostb.example:17443", "key", "", &got, &warned); err != nil {
		t.Fatalf("RunPeerConnect: %v", err)
	}
	if warned.Len() != 0 {
		t.Errorf("RunPeerConnect warnings = %q, want nothing", warned.String())
	}
}

// TestRunPeerConnectNeedsLocalAlias checks that the terminal asks for --alias,
// the command's own form of the local_alias parameter.
func TestRunPeerConnectNeedsLocalAlias(t *testing.T) {
	path := fakelocal.SocketPath(t)
	fakelocal.Start(t, path, fakelocal.Handlers{
		Connect: func(context.Context, *localpb.ConnectRequest) (*localpb.ConnectResponse, error) {
			return nil, fakelocal.ErrorStatus(codes.FailedPrecondition, &localpb.ErrorDetail{
				Code:    localpb.ErrorCode_ERROR_CODE_NEEDS_LOCAL_ALIAS,
				Message: "the other Broker's alias is already this Broker's alias or a peer's alias",
			})
		},
	})

	var got, warned bytes.Buffer
	err := RunPeerConnect(t.Context(), fixedSelector(path), "hostb.example:17443", "key", "", &got, &warned)
	if err == nil {
		t.Fatal("RunPeerConnect returned nil, want an error")
	}
	if !strings.Contains(err.Error(), "--alias") {
		t.Errorf("error = %q, want the --alias hint", err)
	}
	if strings.Contains(err.Error(), "retry with local_alias") {
		t.Errorf("error = %q, want no MCP parameter name", err)
	}
	if got.Len() != 0 || warned.Len() != 0 {
		t.Errorf("RunPeerConnect wrote %q and %q, want nothing", got.String(), warned.String())
	}
}

func TestRunPeerDisconnect(t *testing.T) {
	path := fakelocal.SocketPath(t)
	srv := fakelocal.Start(t, path, fakelocal.Handlers{
		Disconnect: func(context.Context, *localpb.DisconnectRequest) (*localpb.DisconnectResponse, error) {
			return &localpb.DisconnectResponse{}, nil
		},
	})

	var got bytes.Buffer
	if err := RunPeerDisconnect(t.Context(), fixedSelector(path), "hostB", &got); err != nil {
		t.Fatalf("RunPeerDisconnect: %v", err)
	}
	if want := "Disconnected hostB\n"; got.String() != want {
		t.Errorf("RunPeerDisconnect output = %q, want %q", got.String(), want)
	}
	if req := requestOf[*localpb.DisconnectRequest](t, srv, "Disconnect"); req.GetBroker() != "hostB" {
		t.Errorf("Disconnect broker = %q, want %q", req.GetBroker(), "hostB")
	}
}

func TestRunPeerDisconnectError(t *testing.T) {
	path := fakelocal.SocketPath(t)
	fakelocal.Start(t, path, fakelocal.Handlers{
		Disconnect: func(context.Context, *localpb.DisconnectRequest) (*localpb.DisconnectResponse, error) {
			return nil, fakelocal.ErrorStatus(codes.NotFound, &localpb.ErrorDetail{
				Code:    localpb.ErrorCode_ERROR_CODE_NOT_FOUND,
				Message: "no peer Broker has the alias or fingerprint \"hostZ\"",
			})
		},
	})

	var got bytes.Buffer
	err := RunPeerDisconnect(t.Context(), fixedSelector(path), "hostZ", &got)
	if err == nil {
		t.Fatal("RunPeerDisconnect returned nil, want an error")
	}
	if !strings.HasPrefix(err.Error(), "not_found: ") {
		t.Errorf("RunPeerDisconnect error = %q, want it to start with %q", err, "not_found: ")
	}
	if !strings.Contains(err.Error(), "`peers broker list`") {
		t.Errorf("RunPeerDisconnect error = %q, want the list command", err)
	}
	if got.Len() != 0 {
		t.Errorf("RunPeerDisconnect wrote %q, want nothing", got.String())
	}
}

func TestRunPeerListSelectorError(t *testing.T) {
	const noBroker = "no Broker is available for this account"
	var got bytes.Buffer
	err := RunPeerList(t.Context(), func(context.Context) (string, Dialer, error) {
		return "", nil, errors.New(noBroker)
	}, &got)
	if want := "cannot reach the Broker: " + noBroker; err == nil || err.Error() != want {
		t.Fatalf("RunPeerList error = %v, want %q", err, want)
	}
	if got.Len() != 0 {
		t.Errorf("RunPeerList wrote %q, want nothing", got.String())
	}
}

func TestRunAuditCancel(t *testing.T) {
	path := fakelocal.SocketPath(t)
	sent := make(chan struct{})
	events := sampleAuditEvents()[:1]
	fakelocal.Start(t, path, fakelocal.Handlers{
		Audit: func(_ *localpb.AuditRequest, stream localpb.Local_AuditServer) error {
			if err := stream.Send(events[0]); err != nil {
				return err
			}
			close(sent)
			<-stream.Context().Done()
			return nil
		},
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var got bytes.Buffer
	done := make(chan error, 1)
	go func() { done <- RunAudit(ctx, fixedSelector(path), 0, &got) }()

	select {
	case <-sent:
	case <-time.After(eventTimeout):
		t.Fatalf("the Audit handler did not send its event within %s", eventTimeout)
	}

	cancel()
	select {
	case err := <-done:
		// The cancellation is the point of the test; the error itself is
		// the reported context error and is not asserted here.
		_ = err
	case <-time.After(eventTimeout):
		t.Fatalf("RunAudit did not return within %s after the context was cancelled", eventTimeout)
	}
}

func TestRunApplyUpdateUsesSelectedSocket(t *testing.T) {
	path := fakelocal.SocketPath(t)
	h := fakelocal.AdminHandlers()
	h.ApplyUpdate = func(context.Context, *localpb.ApplyUpdateRequest) (*localpb.ApplyUpdateResponse, error) {
		return &localpb.ApplyUpdateResponse{FromVersion: "0.1.1", ToVersion: "0.1.2", Restarting: true}, nil
	}
	srv := fakelocal.Start(t, path, h)

	got, err := RunApplyUpdate(t.Context(), fixedSelector(path))
	if err != nil {
		t.Fatalf("RunApplyUpdate: %v", err)
	}
	if got.FromVersion != "0.1.1" || got.ToVersion != "0.1.2" || !got.Restarting {
		t.Fatalf("RunApplyUpdate = %+v, want 0.1.1 -> 0.1.2 restarting", got)
	}
	if n := methodCalls(srv, "ApplyUpdate"); n != 1 {
		t.Fatalf("ApplyUpdate calls = %d, want 1", n)
	}
}

func TestRunApplyUpdateDescribesBrokerErrors(t *testing.T) {
	path := fakelocal.SocketPath(t)
	h := fakelocal.AdminHandlers()
	h.ApplyUpdate = func(context.Context, *localpb.ApplyUpdateRequest) (*localpb.ApplyUpdateResponse, error) {
		return nil, fakelocal.ErrorStatus(codes.PermissionDenied, &localpb.ErrorDetail{
			Code:    localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED,
			Message: "the account cannot write the Broker configuration",
		})
	}
	fakelocal.Start(t, path, h)

	_, err := RunApplyUpdate(t.Context(), fixedSelector(path))
	if err == nil {
		t.Fatal("RunApplyUpdate returned nil, want the Broker's permission error")
	}
	if want := "permission_denied: the account cannot write the Broker configuration"; err.Error() != want {
		t.Fatalf("RunApplyUpdate error = %q, want %q", err, want)
	}
	if IsFailedPrecondition(err) {
		t.Fatal("IsFailedPrecondition = true for a permission failure")
	}
}

func TestRunApplyUpdateRecognizesFailedPrecondition(t *testing.T) {
	path := fakelocal.SocketPath(t)
	h := fakelocal.AdminHandlers()
	h.ApplyUpdate = func(context.Context, *localpb.ApplyUpdateRequest) (*localpb.ApplyUpdateResponse, error) {
		return nil, status.Error(codes.FailedPrecondition, "this Broker cannot replace its binary")
	}
	fakelocal.Start(t, path, h)

	_, err := RunApplyUpdate(t.Context(), fixedSelector(path))
	if err == nil {
		t.Fatal("RunApplyUpdate returned nil, want the Broker's refusal")
	}
	if !IsFailedPrecondition(err) {
		t.Fatalf("IsFailedPrecondition = false for %v", err)
	}
}

func TestRunUpdateStatusUsesSelectedSocket(t *testing.T) {
	path := fakelocal.SocketPath(t)
	h := fakelocal.AdminHandlers()
	h.UpdateStatus = func(context.Context, *localpb.UpdateStatusRequest) (*localpb.UpdateStatusResponse, error) {
		return &localpb.UpdateStatusResponse{CurrentVersion: "0.1.2", AvailableVersion: ""}, nil
	}
	srv := fakelocal.Start(t, path, h)

	current, available, err := RunUpdateStatus(t.Context(), fixedSelector(path))
	if err != nil {
		t.Fatalf("RunUpdateStatus: %v", err)
	}
	if current != "0.1.2" || available != "" {
		t.Fatalf("RunUpdateStatus = (%q, %q), want (0.1.2, empty)", current, available)
	}
	if n := methodCalls(srv, "UpdateStatus"); n != 1 {
		t.Fatalf("UpdateStatus calls = %d, want 1", n)
	}
}
