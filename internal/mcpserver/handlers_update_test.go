package mcpserver

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// fakeUpdateClient answers ApplyUpdate and counts the calls. The embedded
// LocalClient is nil: a test for one tool never reaches the other RPCs, and a
// call to one of them fails loudly instead of returning a zero answer.
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

// TestUpdateBroker covers the forwarded reply, the two result sentences and the
// three failure outcomes (REQ-update-008).
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
		// The sentence is the wording of design-mcpserver.
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
		// The sentence is the wording of design-mcpserver.
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

// TestUpdateBrokerTimeout covers a Broker that does not finish the update: the
// tool returns the dedicated timeout text instead of waiting for Claude Code's
// own tool timeout. The text is asserted verbatim.
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
