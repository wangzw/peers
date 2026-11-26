package mcpserver

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// fakeUpdateClient answers ApplyUpdate and counts the calls; its embedded
// LocalClient is nil, so a call to any other RPC fails loudly.
type fakeUpdateClient struct {
	localpb.LocalClient
	resp  *localpb.ApplyUpdateResponse
	err   error
	block bool
	calls int
}

func (f *fakeUpdateClient) ApplyUpdate(ctx context.Context, _ *localpb.ApplyUpdateRequest, _ ...grpc.CallOption) (*localpb.ApplyUpdateResponse, error) {
	f.calls++
	if f.block {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return f.resp, f.err
}

func newUpdateTestHandler(c *fakeUpdateClient) *handlers {
	return newHandlers(Status{Phase: PhaseConnected, Client: c, AgentID: "agent-1"})
}

// TestUpdateBroker checks the forwarded reply, the two result sentences and the
// failure outcomes.
func TestUpdateBroker(t *testing.T) {
	t.Parallel()

	t.Run("upgrades and restarts", func(t *testing.T) {
		t.Parallel()
		c := &fakeUpdateClient{resp: &localpb.ApplyUpdateResponse{
			FromVersion: "v0.1.0",
			ToVersion:   "v0.1.1",
			Restarting:  true,
		}}

		out, err := newUpdateTestHandler(c).UpdateBroker(t.Context(), NoArgs{})
		if err != nil {
			t.Fatalf("UpdateBroker: %v", err)
		}
		if c.calls != 1 {
			t.Errorf("ApplyUpdate calls = %d, want 1", c.calls)
		}
		if out.FromVersion != "v0.1.0" || out.ToVersion != "v0.1.1" || !out.Restarting {
			t.Errorf("result = %+v, want v0.1.0 -> v0.1.1 restarting", out)
		}
		want := "Upgrading the Broker from v0.1.0 to v0.1.1; the Broker will restart and the connection will recover automatically. " +
			"Reopen this session to use the new MCP server."
		if out.Result != want {
			t.Errorf("Result = %q, want %q", out.Result, want)
		}
	})

	t.Run("already up to date", func(t *testing.T) {
		t.Parallel()
		c := &fakeUpdateClient{resp: &localpb.ApplyUpdateResponse{FromVersion: "v0.1.1"}}

		out, err := newUpdateTestHandler(c).UpdateBroker(t.Context(), NoArgs{})
		if err != nil {
			t.Fatalf("UpdateBroker: %v", err)
		}
		if out.ToVersion != "" || out.Restarting {
			t.Errorf("result = %+v, want no target version and no restart", out)
		}
		if want := "The Broker is already up to date (v0.1.1)."; out.Result != want {
			t.Errorf("Result = %q, want %q", out.Result, want)
		}
	})

	t.Run("permission denied keeps the Broker's reason and the config path", func(t *testing.T) {
		t.Parallel()
		const message = "account 2001 cannot write /etc/peers/config.yaml"
		c := &fakeUpdateClient{err: status.Error(codes.PermissionDenied, message)}

		_, err := newUpdateTestHandler(c).UpdateBroker(t.Context(), NoArgs{})
		if err == nil {
			t.Fatal("UpdateBroker succeeded, want a permission error")
		}
		want := "Upgrade failed: " + message + " (code: permission_denied). " + textNeedWrite
		if err.Error() != want {
			t.Errorf("error = %q, want %q", err, want)
		}
	})

	t.Run("failed precondition names the terminal alternative", func(t *testing.T) {
		t.Parallel()
		const message = "the Broker is not running under a service manager"
		c := &fakeUpdateClient{err: status.Error(codes.FailedPrecondition, message)}

		_, err := newUpdateTestHandler(c).UpdateBroker(t.Context(), NoArgs{})
		if err == nil {
			t.Fatal("UpdateBroker succeeded, want a failed_precondition error")
		}
		want := "Upgrade failed: " + message +
			" (code: failed_precondition). Run `peers update` in a terminal instead."
		if err.Error() != want {
			t.Errorf("error = %q, want %q", err, want)
		}
	})

	t.Run("aborted says an update is already in progress", func(t *testing.T) {
		t.Parallel()
		c := &fakeUpdateClient{err: status.Error(codes.Aborted, textUpdateAborted)}

		_, err := newUpdateTestHandler(c).UpdateBroker(t.Context(), NoArgs{})
		if err == nil {
			t.Fatal("UpdateBroker succeeded, want an aborted error")
		}
		want := "Upgrade failed: an update is already in progress (code: aborted)"
		if err.Error() != want {
			t.Errorf("error = %q, want %q", err, want)
		}
	})
}

// TestUpdateBrokerTimeout checks the dedicated text returned when the Broker
// does not finish the update.
func TestUpdateBrokerTimeout(t *testing.T) {
	t.Parallel()

	const want = "Upgrade failed: the Broker did not complete the upgrade within 4 minutes (code: internal). The upgrade was either abandoned, leaving the running version unchanged (a replacement that had started is rolled back), or completed, leaving the Broker restarting with the new version. To check the result, run `peers --version`, run `peers service status`, or run `peers update` in a terminal."

	h := newUpdateTestHandler(&fakeUpdateClient{block: true})
	h.updateTimeout = handlerTimeoutSeam

	elapsed, err := waitBoundedCall(t, "UpdateBroker", func() error {
		_, err := h.UpdateBroker(context.Background(), NoArgs{})
		return err
	})
	if err == nil {
		t.Fatal("UpdateBroker succeeded although the Broker never answered")
	}
	if elapsed < handlerTimeoutSeam/2 {
		t.Errorf("UpdateBroker returned after %s, want about %s", elapsed, handlerTimeoutSeam)
	}
	if err.Error() != want {
		t.Errorf("error = %q, want %q", err, want)
	}
}
