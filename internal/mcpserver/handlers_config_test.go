package mcpserver

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakelocal"
)

// newConfigTestHandler starts a fake Local service, connects to it, and returns
// handlers whose status is connected for the agent "agent-1".
func newConfigTestHandler(t *testing.T, hs fakelocal.Handlers) (*handlers, *fakelocal.Server) {
	t.Helper()
	path := fakelocal.SocketPath(t)
	s := fakelocal.Start(t, path, hs)
	cc, err := grpc.NewClient("unix://"+path, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dialing the fake Local service: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	h := newHandlers(Status{Phase: PhaseConnected, Client: localpb.NewLocalClient(cc), AgentID: "agent-1"})
	return h, s
}

// TestRenderValue covers every ConfigValue variant and the units and quoting
// rules of the rendered text.
func TestRenderValue(t *testing.T) {
	t.Parallel()

	str := func(v string) *localpb.ConfigValue {
		return &localpb.ConfigValue{Value: &localpb.ConfigValue_StringValue{StringValue: v}}
	}
	boolean := func(v bool) *localpb.ConfigValue {
		return &localpb.ConfigValue{Value: &localpb.ConfigValue_BoolValue{BoolValue: v}}
	}
	integer := func(v int64) *localpb.ConfigValue {
		return &localpb.ConfigValue{Value: &localpb.ConfigValue_IntValue{IntValue: v}}
	}
	dur := func(d time.Duration) *localpb.ConfigValue {
		return &localpb.ConfigValue{Value: &localpb.ConfigValue_DurationValue{DurationValue: durationpb.New(d)}}
	}
	list := func(vs ...string) *localpb.ConfigValue {
		return &localpb.ConfigValue{Value: &localpb.ConfigValue_StringListValue{
			StringListValue: &localpb.StringList{Values: vs},
		}}
	}
	secret := func(v bool) *localpb.ConfigValue {
		return &localpb.ConfigValue{Value: &localpb.ConfigValue_SecretIsSet{SecretIsSet: v}}
	}

	cases := []struct {
		name string
		in   *localpb.ConfigValue
		want string
	}{
		{"nil value", nil, ""},
		{"unset oneof", &localpb.ConfigValue{}, ""},
		{"string", str("alpha"), "alpha"},
		{"empty string", str(""), ""},
		{"bool true", boolean(true), "true"},
		{"bool false", boolean(false), "false"},
		{"integer", integer(42), "42"},
		{"negative integer", integer(-7), "-7"},
		{"duration ten minutes", dur(10 * time.Minute), "10m"},
		{"duration one hour", dur(time.Hour), "1h"},
		{"duration one and a half hours", dur(90 * time.Minute), "1h30m"},
		{"duration half a second", dur(500 * time.Millisecond), "500ms"},
		{"duration zero", dur(0), "0s"},
		{"list", list("a", "b"), "[a, b]"},
		{"list with quoting", list("a", "b c", ""), `[a, "b c", ""]`},
		{"empty list", list(), "[]"},
		{"list element with comma", list("a,b"), `["a,b"]`},
		{"list element with bracket", list("[x]"), `["[x]"]`},
		{"secret is set", secret(true), "(set)"},
		{"secret is not set", secret(false), "(not set)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := renderValue(tc.in); got != tc.want {
				t.Errorf("renderValue(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestGetConfig covers forwarding, value rendering, the secret marker and the
// read-permission error text. REQ-broker-config-001, REQ-broker-config-022,
// REQ-broker-pairing-030.
func TestGetConfig(t *testing.T) {
	t.Parallel()

	t.Run("without key and with a secret", func(t *testing.T) {
		t.Parallel()
		h, s := newConfigTestHandler(t, fakelocal.Handlers{
			GetConfig: func(context.Context, *localpb.GetConfigRequest) (*localpb.GetConfigResponse, error) {
				return &localpb.GetConfigResponse{Entries: []*localpb.ConfigEntry{
					{Key: "broker.alias", Value: &localpb.ConfigValue{Value: &localpb.ConfigValue_StringValue{StringValue: "alpha"}}},
					{Key: "pairing.static_key", Value: &localpb.ConfigValue{Value: &localpb.ConfigValue_SecretIsSet{SecretIsSet: true}}},
				}}, nil
			},
		})

		ctx, cancel := context.WithTimeout(t.Context(), eventTimeout)
		defer cancel()
		out, err := h.GetConfig(ctx, GetConfigIn{})
		if err != nil {
			t.Fatalf("GetConfig: %v", err)
		}

		calls := s.Calls()
		if len(calls) != 1 || calls[0].Method != "GetConfig" {
			t.Fatalf("calls = %v, want one GetConfig", calls)
		}
		if key := calls[0].Request.(*localpb.GetConfigRequest).GetKey(); key != "" {
			t.Errorf("request key = %q, want empty", key)
		}

		if len(out.Entries) != 2 {
			t.Fatalf("entries = %v, want two", out.Entries)
		}
		if out.Entries[0] != (ConfigEntryOut{Key: "broker.alias", Value: "alpha"}) {
			t.Errorf("first entry = %+v, want broker.alias=alpha", out.Entries[0])
		}
		if out.Entries[1] != (ConfigEntryOut{Key: "pairing.static_key", Value: "(set)"}) {
			t.Errorf("second entry = %+v, want pairing.static_key=(set)", out.Entries[1])
		}

		// A secret entry exposes only the marker, no value alongside it.
		b, err := json.Marshal(out.Entries[1])
		if err != nil {
			t.Fatalf("marshaling the secret entry: %v", err)
		}
		if want := `{"key":"pairing.static_key","value":"(set)"}`; string(b) != want {
			t.Errorf("secret entry JSON = %s, want %s", b, want)
		}
	})

	t.Run("permission denied", func(t *testing.T) {
		t.Parallel()
		h, _ := newConfigTestHandler(t, fakelocal.Handlers{
			GetConfig: func(context.Context, *localpb.GetConfigRequest) (*localpb.GetConfigResponse, error) {
				return nil, fakelocal.ErrorStatus(codes.PermissionDenied, &localpb.ErrorDetail{
					Code:    localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED,
					Message: "read denied",
				})
			},
		})

		ctx, cancel := context.WithTimeout(t.Context(), eventTimeout)
		defer cancel()
		_, err := h.GetConfig(ctx, GetConfigIn{Key: "broker.alias"})
		if err == nil {
			t.Fatal("GetConfig succeeded, want a permission error")
		}
		if !strings.HasPrefix(err.Error(), "get_config failed:") {
			t.Errorf("error = %q, want a get_config failed prefix", err)
		}
		if !strings.Contains(err.Error(), "read permission") {
			t.Errorf("error = %q, want it to mention read permission", err)
		}
	})
}

// TestSetBrokerOnline covers both directions, the changed flag taken from the
// Broker's old and new values, and the denied write.
// REQ-broker-config-067.
func TestSetBrokerOnline(t *testing.T) {
	t.Parallel()

	boolean := func(v bool) *localpb.ConfigValue {
		return &localpb.ConfigValue{Value: &localpb.ConfigValue_BoolValue{BoolValue: v}}
	}
	cases := []struct {
		name        string
		online      bool
		oldOffline  bool
		newOffline  bool
		wantValue   string
		wantChanged bool
	}{
		{"go offline", false, false, true, "true", true},
		{"go online", true, true, false, "false", true},
		{"already offline", false, true, true, "true", false},
		{"already online", true, false, false, "false", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, s := newConfigTestHandler(t, fakelocal.Handlers{
				SetConfig: func(context.Context, *localpb.SetConfigRequest) (*localpb.SetConfigResponse, error) {
					return &localpb.SetConfigResponse{
						OldValue: boolean(tc.oldOffline),
						NewValue: boolean(tc.newOffline),
					}, nil
				},
			})

			ctx, cancel := context.WithTimeout(t.Context(), eventTimeout)
			defer cancel()
			out, err := h.SetBrokerOnline(ctx, SetBrokerOnlineIn{Online: tc.online})
			if err != nil {
				t.Fatalf("SetBrokerOnline: %v", err)
			}
			if out.Online != tc.online || out.Changed != tc.wantChanged {
				t.Errorf("result = %+v, want online %v and changed %v", out, tc.online, tc.wantChanged)
			}

			calls := s.Calls()
			if len(calls) != 1 || calls[0].Method != "SetConfig" {
				t.Fatalf("calls = %v, want one SetConfig", calls)
			}
			req := calls[0].Request.(*localpb.SetConfigRequest)
			if req.GetKey() != "broker.offline" || req.GetValue() != tc.wantValue {
				t.Errorf("request = (%q, %q), want (broker.offline, %q)", req.GetKey(), req.GetValue(), tc.wantValue)
			}
			if req.GetAgentId() != "agent-1" {
				t.Errorf("request agent_id = %q, want agent-1 for the audit record", req.GetAgentId())
			}
		})
	}

	t.Run("permission denied", func(t *testing.T) {
		t.Parallel()
		h, _ := newConfigTestHandler(t, fakelocal.Handlers{
			SetConfig: func(context.Context, *localpb.SetConfigRequest) (*localpb.SetConfigResponse, error) {
				return nil, fakelocal.ErrorStatus(codes.PermissionDenied, &localpb.ErrorDetail{
					Code:    localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED,
					Message: "write denied",
				})
			},
		})

		ctx, cancel := context.WithTimeout(t.Context(), eventTimeout)
		defer cancel()
		_, err := h.SetBrokerOnline(ctx, SetBrokerOnlineIn{Online: false})
		if err == nil {
			t.Fatal("SetBrokerOnline succeeded, want a permission error")
		}
		if !strings.HasPrefix(err.Error(), "set_broker_online failed:") {
			t.Errorf("error = %q, want a set_broker_online failed prefix", err)
		}
		if !strings.Contains(err.Error(), "write permission") {
			t.Errorf("error = %q, want it to mention write permission", err)
		}
	})
}

// TestSetConfig covers forwarding the raw value, rendering the old and new
// values, and keeping the Broker's reason for a rejected change.
// REQ-broker-config-002, REQ-broker-config-021, REQ-broker-config-029,
// REQ-broker-config-032, REQ-broker-pairing-031.
func TestSetConfig(t *testing.T) {
	t.Parallel()

	t.Run("forwards the raw value", func(t *testing.T) {
		t.Parallel()
		h, s := newConfigTestHandler(t, fakelocal.Handlers{
			SetConfig: func(context.Context, *localpb.SetConfigRequest) (*localpb.SetConfigResponse, error) {
				return &localpb.SetConfigResponse{
					OldValue: &localpb.ConfigValue{Value: &localpb.ConfigValue_StringValue{StringValue: "10s"}},
					NewValue: &localpb.ConfigValue{Value: &localpb.ConfigValue_DurationValue{DurationValue: durationpb.New(15 * time.Second)}},
				}, nil
			},
		})

		ctx, cancel := context.WithTimeout(t.Context(), eventTimeout)
		defer cancel()
		out, err := h.SetConfig(ctx, SetConfigIn{Key: "link.keepalive_interval", Value: "15s"})
		if err != nil {
			t.Fatalf("SetConfig: %v", err)
		}

		calls := s.Calls()
		if len(calls) != 1 || calls[0].Method != "SetConfig" {
			t.Fatalf("calls = %v, want one SetConfig", calls)
		}
		req := calls[0].Request.(*localpb.SetConfigRequest)
		if req.GetKey() != "link.keepalive_interval" || req.GetValue() != "15s" {
			t.Errorf("request = %+v, want key link.keepalive_interval and value 15s verbatim", req)
		}
		if req.GetAgentId() != "agent-1" {
			t.Errorf("request agent_id = %q, want agent-1 for the audit record", req.GetAgentId())
		}

		want := SetConfigOut{Key: "link.keepalive_interval", Old: "10s", New: "15s"}
		if out != want {
			t.Errorf("result = %+v, want %+v", out, want)
		}
	})

	rejections := []struct {
		name    string
		message string
	}{
		{
			name:    "listen address fixed by the flag",
			message: "broker.listen is set by the --listen flag for this run; the config file was not changed",
		},
		{
			name:    "static key set only in the file",
			message: "pairing.static_key can only be set in the config file; the config file was not changed",
		},
		{
			name:    "invalid combination",
			message: "broker.listen conflicts with broker.advertised_addresses; the config file was not changed",
		},
	}
	for _, tc := range rejections {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newConfigTestHandler(t, fakelocal.Handlers{
				SetConfig: func(context.Context, *localpb.SetConfigRequest) (*localpb.SetConfigResponse, error) {
					return nil, fakelocal.ErrorStatus(codes.InvalidArgument, &localpb.ErrorDetail{
						Code:    localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
						Message: tc.message,
					})
				},
			})

			ctx, cancel := context.WithTimeout(t.Context(), eventTimeout)
			defer cancel()
			_, err := h.SetConfig(ctx, SetConfigIn{Key: "k", Value: "v"})
			if err == nil {
				t.Fatal("SetConfig succeeded, want an invalid_argument error")
			}
			if !strings.HasPrefix(err.Error(), "set_config failed:") {
				t.Errorf("error = %q, want a set_config failed prefix", err)
			}
			if !strings.Contains(err.Error(), tc.message) {
				t.Errorf("error = %q, want the Broker's reason verbatim", err)
			}
			if !strings.Contains(err.Error(), "(code: invalid_argument)") {
				t.Errorf("error = %q, want code invalid_argument", err)
			}
		})
	}

	t.Run("permission denied", func(t *testing.T) {
		t.Parallel()
		h, _ := newConfigTestHandler(t, fakelocal.Handlers{
			SetConfig: func(context.Context, *localpb.SetConfigRequest) (*localpb.SetConfigResponse, error) {
				return nil, fakelocal.ErrorStatus(codes.PermissionDenied, &localpb.ErrorDetail{
					Code:    localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED,
					Message: "write denied",
				})
			},
		})

		ctx, cancel := context.WithTimeout(t.Context(), eventTimeout)
		defer cancel()
		_, err := h.SetConfig(ctx, SetConfigIn{Key: "broker.alias", Value: "b"})
		if err == nil {
			t.Fatal("SetConfig succeeded, want a permission error")
		}
		if !strings.Contains(err.Error(), "write permission") {
			t.Errorf("error = %q, want it to mention write permission", err)
		}
	})
}

// TestConfigToolCallTimeouts covers get_config, set_config and
// set_broker_online: each returns a tool error instead of waiting for a Broker
// that never answers. The seam is short; the backstop in waitBoundedCall is the
// test's own bound and fires on a handler that never returns.
func TestConfigToolCallTimeouts(t *testing.T) {
	t.Parallel()

	blocked := func(ctx context.Context) error {
		<-ctx.Done()
		return status.Error(codes.DeadlineExceeded, "the Broker did not answer")
	}

	t.Run("get_config", func(t *testing.T) {
		t.Parallel()
		h, _ := newConfigTestHandler(t, fakelocal.Handlers{
			GetConfig: func(ctx context.Context, _ *localpb.GetConfigRequest) (*localpb.GetConfigResponse, error) {
				return nil, blocked(ctx)
			},
		})
		h.rpcTimeout = handlerTimeoutSeam

		elapsed, err := waitBoundedCall(t, "GetConfig", func() error {
			_, err := h.GetConfig(context.Background(), GetConfigIn{})
			return err
		})
		checkBoundedError(t, "GetConfig", err, elapsed, "get_config failed", "(code: internal)")
	})

	t.Run("set_config", func(t *testing.T) {
		t.Parallel()
		h, _ := newConfigTestHandler(t, fakelocal.Handlers{
			SetConfig: func(ctx context.Context, _ *localpb.SetConfigRequest) (*localpb.SetConfigResponse, error) {
				return nil, blocked(ctx)
			},
		})
		h.rpcTimeout = handlerTimeoutSeam

		elapsed, err := waitBoundedCall(t, "SetConfig", func() error {
			_, err := h.SetConfig(context.Background(), SetConfigIn{Key: "broker.alias", Value: "b"})
			return err
		})
		checkBoundedError(t, "SetConfig", err, elapsed, "set_config failed", "(code: internal)")
	})

	t.Run("set_broker_online", func(t *testing.T) {
		t.Parallel()
		h, _ := newConfigTestHandler(t, fakelocal.Handlers{
			SetConfig: func(ctx context.Context, _ *localpb.SetConfigRequest) (*localpb.SetConfigResponse, error) {
				return nil, blocked(ctx)
			},
		})
		h.rpcTimeout = handlerTimeoutSeam

		elapsed, err := waitBoundedCall(t, "SetBrokerOnline", func() error {
			_, err := h.SetBrokerOnline(context.Background(), SetBrokerOnlineIn{Online: true})
			return err
		})
		checkBoundedError(t, "SetBrokerOnline", err, elapsed, "set_broker_online failed", "(code: internal)")
	})
}
