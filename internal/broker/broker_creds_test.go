package broker

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestNewRejectsServerOptionsCreds checks that New refuses Deps.ServerOptions
// that replace the process-credential handshake, while accepting other options.
func TestNewRejectsServerOptionsCreds(t *testing.T) {
	for name, opt := range map[string]grpc.ServerOption{
		"replacement": grpc.Creds(insecure.NewCredentials()),
		"nil":         grpc.Creds(nil),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := New(Deps{
				Config:        newFakeConfig(t.TempDir()),
				Topics:        newFakeTopics(),
				ServerOptions: []grpc.ServerOption{opt},
			})
			if err == nil {
				t.Fatal("New accepted Deps.ServerOptions containing grpc.Creds")
			}
			if !strings.Contains(err.Error(), "credentials") {
				t.Errorf("New error = %q, want it to name the credentials option", err)
			}
		})
	}

	// Other options must still be accepted; the rejection targets credentials
	// only.
	b, err := New(Deps{
		Config: newFakeConfig(t.TempDir()),
		Topics: newFakeTopics(),
		ServerOptions: []grpc.ServerOption{
			grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
				return handler(ctx, req)
			}),
		},
	})
	if err != nil {
		t.Fatalf("New rejected a non-credentials ServerOption: %v", err)
	}
	b.Stop()
}
