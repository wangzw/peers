package termcmd

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakelocal"
)

// TestRunCommandsReturnBrokerErrorsWithoutOutput checks that the config, pairing
// key, broker list, connect and disconnect commands report a permission or
// unavailable Broker error and write nothing.
func TestRunCommandsReturnBrokerErrorsWithoutOutput(t *testing.T) {
	kinds := []struct {
		name       string
		brokerErr  func() error
		wantPrefix string
	}{
		{
			name: "permission_denied",
			brokerErr: func() error {
				return fakelocal.ErrorStatus(codes.PermissionDenied, &localpb.ErrorDetail{
					Code:    localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED,
					Message: "the test account may not perform this operation",
				})
			},
			wantPrefix: "permission_denied: ",
		},
		{
			name:       "unavailable",
			brokerErr:  func() error { return status.Error(codes.Unavailable, "the Broker is going away") },
			wantPrefix: "cannot reach the Broker: ",
		},
	}

	commands := []struct {
		name     string
		handlers func(err error) fakelocal.Handlers
		run      func(ctx context.Context, sel Selector, w, warn io.Writer) error
	}{
		{
			name: "config get",
			handlers: func(err error) fakelocal.Handlers {
				return fakelocal.Handlers{GetConfig: func(context.Context, *localpb.GetConfigRequest) (*localpb.GetConfigResponse, error) {
					return nil, err
				}}
			},
			run: func(ctx context.Context, sel Selector, w, _ io.Writer) error {
				return RunConfigGet(ctx, sel, "directory.max_records", w)
			},
		},
		{
			name: "config set",
			handlers: func(err error) fakelocal.Handlers {
				return fakelocal.Handlers{SetConfig: func(context.Context, *localpb.SetConfigRequest) (*localpb.SetConfigResponse, error) {
					return nil, err
				}}
			},
			run: func(ctx context.Context, sel Selector, w, _ io.Writer) error {
				return RunConfigSet(ctx, sel, "directory.max_records", "900", w)
			},
		},
		{
			name: "key create",
			handlers: func(err error) fakelocal.Handlers {
				return fakelocal.Handlers{CreatePairingKey: func(context.Context, *localpb.CreatePairingKeyRequest) (*localpb.CreatePairingKeyResponse, error) {
					return nil, err
				}}
			},
			run: func(ctx context.Context, sel Selector, w, _ io.Writer) error {
				return RunPairingKeyCreate(ctx, sel, w)
			},
		},
		{
			name: "broker list",
			handlers: func(err error) fakelocal.Handlers {
				return fakelocal.Handlers{ListBrokers: func(context.Context, *localpb.ListBrokersRequest) (*localpb.ListBrokersResponse, error) {
					return nil, err
				}}
			},
			run: func(ctx context.Context, sel Selector, w, _ io.Writer) error {
				return RunPeerList(ctx, sel, w)
			},
		},
		{
			name: "broker connect",
			handlers: func(err error) fakelocal.Handlers {
				return fakelocal.Handlers{Connect: func(context.Context, *localpb.ConnectRequest) (*localpb.ConnectResponse, error) {
					return nil, err
				}}
			},
			run: func(ctx context.Context, sel Selector, w, warn io.Writer) error {
				return RunPeerConnect(ctx, sel, "hostb.example:17443", "one-time-key", "", w, warn)
			},
		},
		{
			name: "peer disconnect",
			handlers: func(err error) fakelocal.Handlers {
				return fakelocal.Handlers{Disconnect: func(context.Context, *localpb.DisconnectRequest) (*localpb.DisconnectResponse, error) {
					return nil, err
				}}
			},
			run: func(ctx context.Context, sel Selector, w, _ io.Writer) error {
				return RunPeerDisconnect(ctx, sel, "hostB", w)
			},
		},
	}

	for _, cmd := range commands {
		for _, kind := range kinds {
			t.Run(cmd.name+"/"+kind.name, func(t *testing.T) {
				path := fakelocal.SocketPath(t)
				fakelocal.Start(t, path, cmd.handlers(kind.brokerErr()))

				var out, warn bytes.Buffer
				err := cmd.run(t.Context(), fixedSelector(path), &out, &warn)
				if err == nil {
					t.Fatalf("%s returned nil for %s, want an error", cmd.name, kind.name)
				}
				if !strings.HasPrefix(err.Error(), kind.wantPrefix) {
					t.Errorf("%s error = %q, want it to start with %q", cmd.name, err, kind.wantPrefix)
				}
				if out.Len() != 0 {
					t.Errorf("%s wrote %q to the output writer, want nothing", cmd.name, out.String())
				}
				if warn.Len() != 0 {
					t.Errorf("%s wrote %q to the warning writer, want nothing", cmd.name, warn.String())
				}
			})
		}
	}
}
