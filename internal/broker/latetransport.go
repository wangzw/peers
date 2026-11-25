package broker

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/wangzw/peers/internal/pairing"
)

type transportTarget interface {
	PairDial(ctx context.Context, address string, run func(pairing.PairConn) error) error
	Drop(fp pairing.Fingerprint)
}

// Assembly opens pairing before link exists, so the target is bound only after
// link is built.
type lateTransport struct {
	target atomic.Pointer[transportTarget]
}

// set binds the target once; a second call panics.
func (t *lateTransport) set(target transportTarget) {
	if !t.target.CompareAndSwap(nil, &target) {
		panic("broker: transport target set twice")
	}
}

// PairDial reports ErrDialFailed while no target is bound so pairing treats the attempt as an unreachable peer.
func (t *lateTransport) PairDial(ctx context.Context, address string, run func(pairing.PairConn) error) error {
	target := t.target.Load()
	if target == nil {
		return fmt.Errorf("%w: link is not started", pairing.ErrDialFailed)
	}
	return (*target).PairDial(ctx, address, run)
}

func (t *lateTransport) Drop(fp pairing.Fingerprint) {
	if target := t.target.Load(); target != nil {
		(*target).Drop(fp)
	}
}

var _ pairing.Transport = (*lateTransport)(nil)
