package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"testing"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/directory"
	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/pairing"
)

// TestPeerAssemblerMarksIdentityAndPeerTableFailures checks that identity and
// peer-table open failures enter the data-unavailable class with the right kind,
// while an unrelated wiring failure does not.
func TestPeerAssemblerMarksIdentityAndPeerTableFailures(t *testing.T) {
	cases := []struct {
		name string
		err  error
		kind dataKind
	}{
		{"identity", fmt.Errorf("%w: open identity.key: permission denied", pairing.ErrIdentity), dataKindIdentity},
		{"peer table", fmt.Errorf("%w: read peers.json: permission denied", pairing.ErrStore), dataKindPeers},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := &peerAssembler{openPairing: func(pairing.Transport) (PeerStore, *pairing.Pairing, error) {
				return nil, nil, tc.err
			}}
			deps := Deps{Logger: NewLogger(io.Discard, new(slog.LevelVar))}
			_, err := a.Assemble(context.Background(), newFakeConfig(t.TempDir()), &deps, &Assembly{})
			if !errors.Is(err, config.ErrDataUnavailable) {
				t.Fatalf("error = %v, want the data-unavailable class", err)
			}
			kind, _, ok := classifyDataUnavailable(err)
			if !ok || kind != tc.kind {
				t.Fatalf("kind = %v (ok %v), want %v", kind, ok, tc.kind)
			}
		})
	}

	a := &peerAssembler{openPairing: func(pairing.Transport) (PeerStore, *pairing.Pairing, error) {
		return nil, nil, errors.New("wiring exploded")
	}}
	deps := Deps{Logger: NewLogger(io.Discard, new(slog.LevelVar))}
	_, err := a.Assemble(context.Background(), newFakeConfig(t.TempDir()), &deps, &Assembly{})
	if errors.Is(err, config.ErrDataUnavailable) {
		t.Fatalf("a non-data failure was marked data unavailable: %v", err)
	}
}

type pathsConfig struct {
	*fakeConfig
	paths config.PathSet
}

func (c pathsConfig) Paths() config.PathSet { return c.paths }

type zeroIdentity struct{}

func (zeroIdentity) Fingerprint() directory.Fingerprint { return directory.Fingerprint{} }
func (zeroIdentity) PublicKey() []byte                  { return nil }
func (zeroIdentity) Sign([]byte) []byte                 { return nil }

// TestDirectoryAssemblerMarksReadFailures checks that a failed directory read is
// marked as a directory data-unavailable failure, while a missing identity is
// not.
func TestDirectoryAssemblerMarksReadFailures(t *testing.T) {
	cfg := pathsConfig{
		fakeConfig: newFakeConfig(t.TempDir()),
		paths:      config.PathSet{DataDir: "/home/u/.peers"},
	}
	asm := &Assembly{Pairing: &pairing.Pairing{}, Link: &link.Link{}}
	a := &directoryAssembler{
		open: func(directory.Deps) (directoryHandle, error) {
			return nil, fmt.Errorf("directory: read %s: %w", "/home/u/.peers/directory.json",
				&fs.PathError{Op: "open", Path: "/home/u/.peers/directory.json", Err: errors.New("permission denied")})
		},
		newIdentity: func(*pairing.Pairing) (directory.Identity, error) { return zeroIdentity{}, nil },
	}
	deps := Deps{Logger: NewLogger(io.Discard, new(slog.LevelVar))}
	_, err := a.Assemble(context.Background(), cfg, &deps, asm)
	if !errors.Is(err, config.ErrDataUnavailable) {
		t.Fatalf("error = %v, want the data-unavailable class", err)
	}
	if kind, _, _ := classifyDataUnavailable(err); kind != dataKindDirectory {
		t.Fatalf("kind = %v, want directory", kind)
	}

	a.open = func(directory.Deps) (directoryHandle, error) {
		return nil, directory.ErrNoIdentity
	}
	_, err = a.Assemble(context.Background(), cfg, &deps, asm)
	if errors.Is(err, config.ErrDataUnavailable) {
		t.Fatalf("a wiring failure was marked data unavailable: %v", err)
	}
}
