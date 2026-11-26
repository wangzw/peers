package mcpserver

import (
	"bytes"
	"context"
	"net"
	"os"
	"strings"
	"testing"

	"google.golang.org/grpc"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/mcpserver/termcmd"
	"github.com/wangzw/peers/internal/testkit/fakelocal"
)

// TestRunStatusRefusesForeignPeer checks that the status command refuses a
// Broker socket taken over by another account before Hello is sent.
func TestRunStatusRefusesForeignPeer(t *testing.T) {
	path := fakelocal.SocketPath(t)
	srv := fakelocal.Start(t, path, fakelocal.Handlers{
		Status: func(context.Context, *localpb.StatusRequest) (*localpb.StatusResponse, error) {
			return &localpb.StatusResponse{}, nil
		},
	})

	old := peerUID
	peerUID = func(net.Conn) (uint32, error) { return uint32(os.Geteuid()) + 1, nil }
	t.Cleanup(func() { peerUID = old })

	sel := func(context.Context) (string, termcmd.Dialer, error) {
		return path, func() (*grpc.ClientConn, localpb.LocalClient, error) {
			return dialLocal(path)
		}, nil
	}

	var got bytes.Buffer
	err := termcmd.RunStatus(context.Background(), sel, &got)
	if err == nil {
		t.Fatal("RunStatus connected to a Broker socket served by another account")
	}
	if !strings.Contains(err.Error(), "served by uid") {
		t.Errorf("RunStatus error = %q, want it to name the peer uid", err)
	}
	for _, c := range srv.Calls() {
		if c.Method == "Hello" {
			t.Fatal("the fake Broker received Hello over a connection served by another account")
		}
	}
	if got.Len() != 0 {
		t.Errorf("RunStatus wrote %q, want nothing", got.String())
	}
}
